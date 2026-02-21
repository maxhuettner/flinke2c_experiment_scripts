#!/usr/bin/env python3
"""Convert network-sim topology JSON files to Flink GraphML files.

Reads a topology JSON (or every *.json in a directory) and writes a
topology.graphml-compatible file to exp_management/configs/flink/coordinator/.
The output filename matches the topology stem, e.g.:
    config/topologies/cloud.json -> exp_management/configs/flink/coordinator/cloud.graphml

The generated file is used by the flinke2c scheduler when placement_method is
set (e.g. TOP_DOWN). It encodes node types and overlay IP addresses so Flink
can map operators to the physical network topology.

Usage:
    # Single file
    python3 tools/topology_to_graphml.py config/topologies/cloud.json

    # All topologies in a directory
    python3 tools/topology_to_graphml.py config/topologies/

    # Override task slots per compute node (default: 1)
    python3 tools/topology_to_graphml.py config/topologies/cloud.json --slots 3

    # Custom output directory
    python3 tools/topology_to_graphml.py config/topologies/ --output-dir /tmp/graphml/
"""

import argparse
import json
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# GraphML generation (self-contained, no imports from cli/)
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path("exp_management/configs/flink/coordinator")


def _load_topology(path: Path) -> dict:
    with open(path) as f:
        return json.load(f)


def _all_to_all_edges(raw_nodes: list[dict]) -> list[tuple[str, str]]:
    """Generate directed all-to-all edges for a topology with no explicit edges.

    Edges go from every non-sink node to every non-source node, covering
    the full mesh while avoiding sink → source back-edges.
    """
    non_sink = [nd["id"] for nd in raw_nodes
                if nd.get("node_type", "compute").lower() != "sink"]
    non_src  = [nd["id"] for nd in raw_nodes
                if nd.get("node_type", "compute").lower() != "source"]
    return [(u, v) for u in non_sink for v in non_src if u != v]


def topology_to_graphml(topo: dict, slots: int) -> str:
    """Convert a parsed topology dict to GraphML XML string."""
    raw_nodes = topo["nodes"]
    raw_edges = topo.get("edges", [])

    # Identify source node (needed only to detect the no-edges case)
    has_source = any(nd.get("node_type", "compute").lower() == "source" for nd in raw_nodes)

    raw_edge_pairs = [(e["source"], e["target"]) for e in raw_edges]

    if raw_edge_pairs:
        # Edges are already correctly directed in the topology JSON — use as-is
        directed_edges = raw_edge_pairs
    elif has_source:
        # No edges defined (e.g. pure-cloud flat topology): fully connected mesh
        directed_edges = _all_to_all_edges(raw_nodes)
    else:
        directed_edges = []

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<graphml xmlns="http://graphml.graphdrawing.org/xmlns"',
        '         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        '         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns'
        ' http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">',
        '    <key id="type"    for="node" attr.name="type"              attr.type="string"/>',
        '    <key id="id"      for="node" attr.name="id"                attr.type="string"/>',
        '    <key id="compute" for="node" attr.name="computeCapability" attr.type="double"/>',
        '    <key id="memory"  for="node" attr.name="memoryCapability"  attr.type="double"/>',
        '    <key id="slots"   for="node" attr.name="slots"             attr.type="int"/>',
        '',
        '    <graph id="processing-topology" edgedefault="directed">',
    ]

    for nd in raw_nodes:
        nid = nd["id"]
        nt  = nd.get("node_type", "compute").lower()
        addr = nd.get("address", nid)

        if nt == "source":
            lines += [
                f'        <node id="{nid}">',
                '            <data key="type">source</data>',
                '            <data key="id">Source1</data>',
                '        </node>',
                '',
            ]
        elif nt == "sink":
            lines += [
                f'        <node id="{nid}">',
                '            <data key="type">sink</data>',
                '            <data key="id">Sink1</data>',
                '        </node>',
                '',
            ]
        else:  # compute (or anything else)
            lines += [
                f'        <node id="{nid}">',
                '            <data key="type">compute</data>',
                f'            <data key="id">{addr}</data>',
                '            <data key="compute">1.0</data>',
                '            <data key="memory">1.0</data>',
                f'            <data key="slots">{slots}</data>',
                '        </node>',
                '',
            ]

    for u, v in directed_edges:
        lines.append(f'        <edge id="{u}_{v}" source="{u}" target="{v}"/>')

    if directed_edges:
        lines.append('')

    lines += ['    </graph>', '</graphml>']
    return '\n'.join(lines) + '\n'


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def convert_file(topology_path: Path, slots: int, output_dir: Path) -> None:
    topo = _load_topology(topology_path)
    graphml = topology_to_graphml(topo, slots)
    output_path = output_dir / (topology_path.stem + ".graphml")
    output_path.write_text(graphml)
    print(f"  {topology_path}  →  {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "topology",
        help="Topology JSON file, or directory containing *.json topology files",
    )
    parser.add_argument(
        "--slots",
        type=int,
        default=1,
        help="Task slots per compute node written into <data key='slots'> (default: 1)",
    )
    parser.add_argument(
        "--output-dir",
        default=str(OUTPUT_DIR),
        help=f"Output directory for .graphml files (default: {OUTPUT_DIR})",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    target = Path(args.topology)

    if target.is_dir():
        files = sorted(target.glob("*.json"))
        if not files:
            print(f"No *.json files found in {target}", file=sys.stderr)
            sys.exit(1)
        print(f"Converting {len(files)} topology file(s) with {args.slots} slot(s)...")
        for f in files:
            convert_file(f, args.slots, output_dir)
    elif target.is_file():
        print(f"Converting with {args.slots} slot(s)...")
        convert_file(target, args.slots, output_dir)
    else:
        print(f"Error: '{args.topology}' is not a file or directory", file=sys.stderr)
        sys.exit(1)

    print("Done.")


if __name__ == "__main__":
    main()
