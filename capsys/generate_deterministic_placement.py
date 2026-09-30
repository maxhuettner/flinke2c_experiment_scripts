#!/usr/bin/env python3
"""Generate deterministic CAPSYS schedulercfg placement files.

Fixed, reproducible operator->node assignment per (query, topology).

Assignment: each operator gets its own hashed node preference order -
sha256(topology + label + task_id + node_address) - and takes the first
preferred node that still has a free slot (capped at task_slots). Operators
can land on the same node, up to its slot count; nothing forces an even
spread. `topology` has to be in the hash: cloud/edge-heavy/edge-to-cloud
declare identical node ids and addresses, so nothing about the node data
itself can tell them apart otherwise.

Output: capsys/deterministic/schedulercfg_<query>_<topology>.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CAPSYS_DIR = REPO_ROOT / "capsys"
DETERMINISTIC_DIR = CAPSYS_DIR / "deterministic"
OPERATOR_GRAPHS_DIR = CAPSYS_DIR / "operator_graphs"
TOPOLOGIES_DIR = REPO_ROOT / "config" / "topologies"
QUERY_CONFIG_FILE = (
    REPO_ROOT / "exp_management" / "configs" / "flink" / "query_config.yml"
)

_OPERATOR_RE = re.compile(r"^(?P<label>.+)\[(?P<task_id>\d+)\]$")


def operator_graph_path(query: str, graphs_dir: Path = OPERATOR_GRAPHS_DIR) -> Path:
    return graphs_dir / f"{query}.json"


def save_operator_graph(
    query: str,
    operators: list[tuple[str, str]],
    source: str,
    graphs_dir: Path = OPERATOR_GRAPHS_DIR,
    edges: list[list[str]] | None = None,
) -> Path:
    path = operator_graph_path(query, graphs_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "query": query,
        "source": source,
        "operators": [f"{label}[{task_id}]" for label, task_id in operators],
    }
    if edges:
        data["edges"] = edges  # [parent, child] pairs, by "Label[task_id]"
    path.write_text(json.dumps(data, indent=2) + "\n")
    return path


def load_operator_graph(
    query: str, graphs_dir: Path = OPERATOR_GRAPHS_DIR
) -> list[tuple[str, str]] | None:
    path = operator_graph_path(query, graphs_dir)
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    operators = []
    for value in data["operators"]:
        m = _OPERATOR_RE.match(value)
        if not m:
            raise ValueError(f"{path}: unparseable operator entry: {value!r}")
        operators.append((m.group("label"), m.group("task_id")))
    return operators


def load_operator_graph_edges(
    query: str, graphs_dir: Path = OPERATOR_GRAPHS_DIR
) -> list[list[str]] | None:
    path = operator_graph_path(query, graphs_dir)
    if not path.exists():
        return None
    return json.loads(path.read_text()).get("edges") or None


def default_task_slots(query: str, query_config_file: Path = QUERY_CONFIG_FILE) -> int:
    """Per-query slot count from query_config.yml (default 1)."""
    import yaml

    qcfg = yaml.safe_load(query_config_file.read_text()) or {}
    return qcfg.get(query, {}).get("num_task_slots", 1)


_LINE_RE = re.compile(r"^(?P<label>.+)\[(?P<task_id>\d+)\]; (?P<address>\S+)\s*$")


def _parse_schedulercfg(path: Path) -> list[tuple[str, str]]:
    """Return [(label, task_id), ...] in file order (node address dropped)."""
    operators = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        m = _LINE_RE.match(line)
        if not m:
            raise ValueError(f"{path}: unparseable line: {line!r}")
        operators.append((m.group("label"), m.group("task_id")))
    return operators


def load_canonical_operators(
    query: str, reference_topology: str, reference_dir: Path = CAPSYS_DIR
) -> list[tuple[str, str]]:
    """Operator list from whichever profiled rep a majority agree on."""
    rep_paths = sorted(
        reference_dir.glob(f"schedulercfg_{query}_{reference_topology}_*")
    )
    if not rep_paths:
        raise FileNotFoundError(
            f"No reference schedulercfg files found for query={query!r} "
            f"topology={reference_topology!r} under {reference_dir}. "
            "For a topology with no profiled data (e.g. wan), run "
            "'sim profile' once first to capture a job plan."
        )

    parsed = {p: _parse_schedulercfg(p) for p in rep_paths}
    signatures = {p: tuple(sorted(Counter(ops).items())) for p, ops in parsed.items()}
    majority_sig, _count = Counter(signatures.values()).most_common(1)[0]

    for p in rep_paths:  # sorted() above -> lowest-numbered rep wins
        if signatures[p] == majority_sig:
            return parsed[p]
    raise AssertionError("unreachable: majority signature must match some rep")


def load_operators_from_mapping_json(
    path: Path,
) -> tuple[list[tuple[str, str]], list[list[str]]]:
    """Operators + edges from a capsys/expjson/local_sql.json file."""
    config = json.loads(path.read_text())
    operators = []
    for value in config["mapping"].values():
        m = _OPERATOR_RE.match(value)
        if not m:
            raise ValueError(f"{path}: unparseable mapping value: {value!r}")
        operators.append((m.group("label"), m.group("task_id")))
    return operators, config.get("edges", [])


def load_compute_nodes(topology: str) -> list[dict]:
    topo_path = TOPOLOGIES_DIR / f"{topology}.json"
    nodes = json.loads(topo_path.read_text())["nodes"]
    compute_nodes = [n for n in nodes if n["node_type"] == "compute"]
    if not compute_nodes:
        raise ValueError(f"{topo_path}: no node_type=='compute' nodes found")
    return sorted(compute_nodes, key=lambda n: n["id"])


def bfs_from_sink_order(
    operators: list[tuple[str, str]], edges: list[list[str]]
) -> list[tuple[str, str]]:
    """Reorder `operators` by BFS distance from the sink, walking edges
    backwards (child -> parent), instead of Flink's plan-enumeration order."""
    parents: dict[str, list[str]] = {}
    for parent, child in edges:
        parents.setdefault(child, []).append(parent)

    by_key = {f"{label}[{tid}]": (label, tid) for label, tid in operators}
    sinks = [k for k in by_key if k.startswith("Sink")]
    if not sinks:
        raise ValueError("no Sink operator found - can't BFS from sink")

    seen = set(sinks)
    order = list(sinks)
    frontier = list(sinks)
    while frontier:
        next_frontier = []
        for key in frontier:
            for parent in parents.get(key, []):
                if parent not in seen:
                    seen.add(parent)
                    order.append(parent)
                    next_frontier.append(parent)
        frontier = next_frontier

    order += [k for k in by_key if k not in seen]  # unreachable - keep, at the end
    return [by_key[k] for k in order]


def assign_deterministic_placement(
    topology: str,
    operators: list[tuple[str, str]],
    compute_nodes: list[dict],
    task_slots: int,
) -> list[tuple[str, str, str]]:
    """Per-operator hashed node preference, first free slot wins."""
    capacity = len(compute_nodes) * task_slots
    if len(operators) > capacity:
        raise ValueError(
            f"infeasible: {len(operators)} operators > capacity "
            f"{capacity} ({len(compute_nodes)} compute nodes x {task_slots} slots)"
        )
    load = {n["id"]: 0 for n in compute_nodes}
    assignment = []
    for label, task_id in operators:
        def node_key(n: dict) -> str:
            return hashlib.sha256(f"{topology}:{label}:{task_id}:{n['address']}".encode()).hexdigest()

        for n in sorted(compute_nodes, key=node_key):
            if load[n["id"]] < task_slots:
                load[n["id"]] += 1
                assignment.append((label, task_id, n["address"]))
                break
    return assignment


def write_schedulercfg(path: Path, assignment: list[tuple[str, str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [f"{label}[{task_id}]; {address}" for label, task_id, address in assignment]
    path.write_text("\n".join(lines) + "\n")


def generate(
    query: str,
    topology: str,
    *,
    reference_topology: str | None = None,
    reference_dir: Path = CAPSYS_DIR,
    from_mapping_json: Path | None = None,
    task_slots: int | None = None,
    output_dir: Path = DETERMINISTIC_DIR,
    graphs_dir: Path = OPERATOR_GRAPHS_DIR,
    order: str = "mapping",
) -> Path:
    if task_slots is None:
        task_slots = default_task_slots(query)

    if from_mapping_json is not None:
        # explicit override - always refreshes the cache
        operators, edges = load_operators_from_mapping_json(from_mapping_json)
        save_operator_graph(
            query,
            operators,
            source=f"mapping-json:{from_mapping_json}",
            graphs_dir=graphs_dir,
            edges=edges,
        )
    else:
        operators = load_operator_graph(query, graphs_dir)
        if operators is None:
            operators = load_canonical_operators(
                query, reference_topology or topology, reference_dir=reference_dir
            )
            save_operator_graph(
                query,
                operators,
                source=f"reference-schedulercfg:{reference_topology or topology}",
                graphs_dir=graphs_dir,
            )

    if order == "bfs-sink":
        edges = load_operator_graph_edges(query, graphs_dir)
        if not edges:
            raise ValueError(
                f"no edges cached for {query!r} - recapture with "
                "--from-mapping-json against a live job to get bfs-sink order"
            )
        operators = bfs_from_sink_order(operators, edges)

    compute_nodes = load_compute_nodes(topology)
    assignment = assign_deterministic_placement(
        topology, operators, compute_nodes, task_slots
    )
    out_path = output_dir / f"schedulercfg_{query}_{topology}"
    write_schedulercfg(out_path, assignment)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--topology",
        "--topologies",
        dest="topologies",
        nargs="+",
        required=True,
        help="Topology/topologies to place onto, e.g. edge-to-cloud cloud edge-heavy on-prem wan",
    )
    parser.add_argument(
        "--reference-topology",
        default=None,
        help="Topology whose profiled schedulercfg files supply the canonical operator "
        "list (default: same as --topology)",
    )
    parser.add_argument(
        "--reference-dir",
        type=Path,
        default=CAPSYS_DIR,
        help="Directory holding profiled schedulercfg_* files",
    )
    parser.add_argument(
        "--from-mapping-json",
        type=Path,
        default=None,
        help='Use a capsys/expjson/local_sql.json-shaped file\'s "mapping" dict as the '
        "canonical operator list instead of a profiled schedulercfg reference (for a query "
        "CAPSYS's DFS can never plan, e.g. q5/q7 - see this script's module docstring, "
        "option 3). Only usable with a single --queries value.",
    )
    parser.add_argument(
        "--queries", nargs="+", required=True, help="Query names, e.g. q1 q2 q3 q4 q8"
    )
    parser.add_argument(
        "--task-slots",
        type=int,
        default=None,
        help="TaskManager slots per compute node (default: per-query value from query_config.yml)",
    )
    parser.add_argument("--output-dir", type=Path, default=DETERMINISTIC_DIR)
    parser.add_argument("--graphs-dir", type=Path, default=OPERATOR_GRAPHS_DIR)
    parser.add_argument(
        "--order",
        choices=["mapping", "bfs-sink"],
        default="mapping",
        help="Operator processing order (affects who wins a contested slot): "
        "Flink's plan order (default), or BFS distance from the sink "
        "(needs cached edges - see --from-mapping-json)",
    )
    args = parser.parse_args()

    if args.from_mapping_json is not None and (
        len(args.queries) > 1 or len(args.topologies) > 1
    ):
        parser.error(
            "--from-mapping-json only makes sense with a single --queries and --topology value"
        )

    for query in args.queries:
        for topology in args.topologies:
            try:
                out_path = generate(
                    query,
                    topology,
                    reference_topology=args.reference_topology,
                    reference_dir=args.reference_dir,
                    from_mapping_json=args.from_mapping_json,
                    task_slots=args.task_slots,
                    output_dir=args.output_dir,
                    graphs_dir=args.graphs_dir,
                    order=args.order,
                )
            except ValueError as exc:  # infeasible - skip, keep going
                print(f"{query}/{topology}: SKIPPED ({exc})")
                continue
            try:
                display_path = out_path.relative_to(REPO_ROOT)
            except ValueError:
                display_path = out_path
            print(f"{query}/{topology}: wrote {display_path}")


if __name__ == "__main__":
    sys.exit(main())
