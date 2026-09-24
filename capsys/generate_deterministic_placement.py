#!/usr/bin/env python3
"""Generate deterministic CAPSYS schedulercfg placement files.

Unlike the real CAPSYS pipeline (`runds2placement.py`'s DFS/microbenchmark
search over profiled compute/network costs, which is expensive and can
produce a different plan - or no plan at all - per repetition), this
generates a single, reproducible operator->node assignment per (query,
topology): the same operator always lands on the same node.

Operator identity (label + per-operator subtask index, e.g. "Calc[3]") is not
statically derivable from the SQL query text - it only exists once Flink has
compiled the query into a job graph. Crucially, that graph is per-query, not
per-topology (verified by diffing q4's operator set across cloud/edge-heavy/
edge-to-cloud's profiled files - identical): the same query with the same
num_task_slots always compiles to the same operators, regardless of which
topology it eventually gets placed onto. So this script caches the
canonical operator list for each query exactly once, in
capsys/operator_graphs/<query>.json, the first time it's derived (from
whichever source below) - every later --topology just re-reads that cached
graph and round-robins it across that topology's own node list. This script
does not talk to a live Flink cluster itself. Three ways to originally
populate a query's cached graph:

  1. Reference an already-profiled topology (--reference-dir capsys, the
     default): reuses the operator set embedded in existing
     capsys/schedulercfg_<query>_<topology>_<rep> files (see
     --reference-topology). This is how the e2c files were generated
     (edge-to-cloud already has 10 profiled repetitions per query).

  2. For a topology with no profiled files at all (e.g. wan, where the DFS
     search never finds a feasible plan - see experiments.wan.capsys.yml),
     the operator list has to come from one live "sim profile" run first
     (it deploys the query and reads the job plan via
     generate_local_sql_config.py's REST introspection, same as CAPSYS
     itself does) - deterministic placement only replaces the DFS search
     step, not operator discovery.

  3. --from-mapping-json <path>: for a query CAPSYS's DFS can never plan for
     on ANY topology (q5/q7 - see the "multiple downstream operators" exit
     in dfs.py/dfsMultiProcess.py: their operator graph branches, which the
     DFS's network-cost calculation can't handle, so no profiled
     schedulercfg reference ever exists), operator discovery and DFS
     planning are two independent steps - discovery happens first and
     always succeeds regardless of graph shape. So it's enough to run just
     capsys/generate_local_sql_config.py against a live (deployed, RUNNING)
     job once - no profiling wait, no DFS/"plan" step, no per-repetition
     cost - to get capsys/expjson/local_sql.json with a "mapping" dict whose
     values are already the exact "Label[task_id]" strings this script
     needs. Point --from-mapping-json at that file instead of
     --reference-topology/--reference-dir.

Assignment algorithm, given the canonical operator list and a topology:
  1. Eligible nodes = topology nodes with node_type == "compute" only.
     "source"/"sink" nodes never run a Flink TaskManager
     (see worker_nodes filtering in cli/experiment.py) and are excluded.
  2. Capacity check: len(operators) <= len(eligible_nodes) * task_slots,
     otherwise the placement is infeasible (mirrors CAPSYS's own
     "no bound found" failure, just as an explicit upfront check).
  3. Round-robin the operators, in the canonical list's order, across the
     node list (sorted by node id): node = nodes[i % len(nodes)]. This
     fills every node once before doubling up on any node, which is the
     natural way to spread operators out while staying deterministic. Only
     once every node holds `task_slots` operators does round-robin start
     stacking additional operators per node.

Output: capsys/deterministic/schedulercfg_<query>_<topology> (no repetition
suffix - the whole point is that there's exactly one deterministic plan).
Format is identical to the real schedulercfg files:
    <OperatorLabel>[<task_id>]; <node_address>

Cached graph: capsys/operator_graphs/<query>.json, a flat list of
"Label[task_id]" strings in canonical order plus a `source` note recording
where they came from. Delete a query's file there to force it to be
re-derived (e.g. --reference-topology/--from-mapping-json again) next run.
"""

from __future__ import annotations

import argparse
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
QUERY_CONFIG_FILE = REPO_ROOT / "exp_management" / "configs" / "flink" / "query_config.yml"

_OPERATOR_RE = re.compile(r"^(?P<label>.+)\[(?P<task_id>\d+)\]$")


def operator_graph_path(query: str, graphs_dir: Path = OPERATOR_GRAPHS_DIR) -> Path:
    return graphs_dir / f"{query}.json"


def save_operator_graph(
    query: str, operators: list[tuple[str, str]], source: str, graphs_dir: Path = OPERATOR_GRAPHS_DIR
) -> Path:
    path = operator_graph_path(query, graphs_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "query": query,
                "source": source,
                "operators": [f"{label}[{task_id}]" for label, task_id in operators],
            },
            indent=2,
        )
        + "\n"
    )
    return path


def load_operator_graph(query: str, graphs_dir: Path = OPERATOR_GRAPHS_DIR) -> list[tuple[str, str]] | None:
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


def default_task_slots(query: str, query_config_file: Path = QUERY_CONFIG_FILE) -> int:
    """Per-query TaskManager slot count, same precedence cli/experiment.py uses
    for `sim experiment`/`sim profile` (ExperimentSpec.num_task_slots, read from
    the experiment YAML, overrides this default of 1). A slot here isn't "one
    operator" - Flink's default slot sharing lets many distinct operators
    co-locate within a single slot - it's how many parallel pipeline-instances
    a query needs on one TaskManager, e.g. q3/q4/q8 need >1 to fit all their
    operators without spilling onto more nodes than exist."""
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
    """Pick the canonical (label, task_id) list for `query` on
    `reference_topology` out of its profiled repetitions.

    Individual repetitions can carry one-off glitches from the profiling run
    (e.g. schedulercfg_q4_edge-to-cloud_0 has a duplicate "GroupAggregate[15]"
    line that reps 1-9 don't), so this takes the operator-label multiset that
    a majority of the available repetitions agree on, and returns the first
    (lowest-numbered) repetition matching it - preserving that file's
    original operator ordering.
    """
    rep_paths = sorted(reference_dir.glob(f"schedulercfg_{query}_{reference_topology}_*"))
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


def load_operators_from_mapping_json(path: Path) -> list[tuple[str, str]]:
    """Parse a capsys/expjson/local_sql.json-shaped file's "mapping" dict
    into a canonical (label, task_id) operator list, in the insertion order
    Flink's job plan returned them (build_mapping() in
    generate_local_sql_config.py iterates plan_nodes in that order, and JSON
    object order round-trips through json.loads). Values are already
    "Label[task_id]" strings straight from the Flink REST job plan - no
    node address, since this file never went through DFS placement."""
    config = json.loads(path.read_text())
    operators = []
    for value in config["mapping"].values():
        m = _OPERATOR_RE.match(value)
        if not m:
            raise ValueError(f"{path}: unparseable mapping value: {value!r}")
        operators.append((m.group("label"), m.group("task_id")))
    return operators


def load_compute_nodes(topology: str) -> list[dict]:
    topo_path = TOPOLOGIES_DIR / f"{topology}.json"
    nodes = json.loads(topo_path.read_text())["nodes"]
    compute_nodes = [n for n in nodes if n["node_type"] == "compute"]
    if not compute_nodes:
        raise ValueError(f"{topo_path}: no node_type=='compute' nodes found")
    return sorted(compute_nodes, key=lambda n: n["id"])


def assign_deterministic_placement(
    operators: list[tuple[str, str]], compute_nodes: list[dict], task_slots: int
) -> list[tuple[str, str, str]]:
    """Round-robin `operators` across `compute_nodes`. Returns
    [(label, task_id, address), ...] in the same order as `operators`."""
    capacity = len(compute_nodes) * task_slots
    if len(operators) > capacity:
        raise ValueError(
            f"infeasible: {len(operators)} operators > capacity "
            f"{capacity} ({len(compute_nodes)} compute nodes x {task_slots} slots)"
        )
    return [
        (label, task_id, compute_nodes[i % len(compute_nodes)]["address"])
        for i, (label, task_id) in enumerate(operators)
    ]


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
) -> Path:
    if task_slots is None:
        task_slots = default_task_slots(query)

    if from_mapping_json is not None:
        # Explicit override always re-derives and refreshes the cache, even
        # if a cached graph already exists (e.g. re-capturing after a query
        # change).
        operators = load_operators_from_mapping_json(from_mapping_json)
        save_operator_graph(query, operators, source=f"mapping-json:{from_mapping_json}", graphs_dir=graphs_dir)
    else:
        operators = load_operator_graph(query, graphs_dir)
        if operators is None:
            operators = load_canonical_operators(
                query, reference_topology or topology, reference_dir=reference_dir
            )
            save_operator_graph(
                query, operators, source=f"reference-schedulercfg:{reference_topology or topology}", graphs_dir=graphs_dir
            )

    compute_nodes = load_compute_nodes(topology)
    assignment = assign_deterministic_placement(operators, compute_nodes, task_slots)
    out_path = output_dir / f"schedulercfg_{query}_{topology}"
    write_schedulercfg(out_path, assignment)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--topology", "--topologies", dest="topologies", nargs="+", required=True,
        help="Topology/topologies to place onto, e.g. edge-to-cloud cloud edge-heavy on-prem wan",
    )
    parser.add_argument(
        "--reference-topology",
        default=None,
        help="Topology whose profiled schedulercfg files supply the canonical operator "
        "list (default: same as --topology)",
    )
    parser.add_argument(
        "--reference-dir", type=Path, default=CAPSYS_DIR, help="Directory holding profiled schedulercfg_* files"
    )
    parser.add_argument(
        "--from-mapping-json",
        type=Path,
        default=None,
        help="Use a capsys/expjson/local_sql.json-shaped file's \"mapping\" dict as the "
        "canonical operator list instead of a profiled schedulercfg reference (for a query "
        "CAPSYS's DFS can never plan, e.g. q5/q7 - see this script's module docstring, "
        "option 3). Only usable with a single --queries value.",
    )
    parser.add_argument("--queries", nargs="+", required=True, help="Query names, e.g. q1 q2 q3 q4 q8")
    parser.add_argument(
        "--task-slots",
        type=int,
        default=None,
        help="TaskManager slots per compute node (default: per-query value from query_config.yml)",
    )
    parser.add_argument("--output-dir", type=Path, default=DETERMINISTIC_DIR)
    parser.add_argument("--graphs-dir", type=Path, default=OPERATOR_GRAPHS_DIR)
    args = parser.parse_args()

    if args.from_mapping_json is not None and (len(args.queries) > 1 or len(args.topologies) > 1):
        parser.error("--from-mapping-json only makes sense with a single --queries and --topology value")

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
                )
            except ValueError as exc:  # infeasible capacity - keep going for the rest of the matrix
                print(f"{query}/{topology}: SKIPPED ({exc})")
                continue
            try:
                display_path = out_path.relative_to(REPO_ROOT)
            except ValueError:
                display_path = out_path
            print(f"{query}/{topology}: wrote {display_path}")


if __name__ == "__main__":
    sys.exit(main())
