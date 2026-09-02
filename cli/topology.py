"""Load a JSON topology file into a networkx Graph."""

import json
from dataclasses import dataclass, field
from typing import Optional

import networkx as nx


@dataclass
class TopoNode:
    id: str
    node_type: str  # "Compute", "Source", "Sink"
    address: str
    speed: Optional[int] = None
    # "auto" means derive provisioning from location (cloud => provision).
    provision: str = "auto"
    # "cloud" (default) or "on-prem".
    location: str = "cloud"
    instance_type: str = "t3.micro"
    ami: Optional[str] = None
    extra: dict = field(default_factory=dict)

    def is_on_prem(self) -> bool:
        loc = (self.location or "").strip().lower().replace("_", "-")
        return loc in ("onprem", "on-prem")

    def cloud_region(self, default_region: str) -> Optional[str]:
        if self.is_on_prem():
            return None

        loc = (self.location or "").strip()
        normalized = loc.lower().replace("_", "-")
        if normalized in ("", "cloud", "aws", "ec2"):
            return default_region
        return loc

    def should_provision(self) -> bool:
        value = (self.provision or "").strip().lower()
        if value in ("existing", "static", "skip", "false", "onprem", "on-prem"):
            return False
        if value in ("true", "create", "cloud", "aws", "ec2", "new"):
            return True
        # "auto" (or any unrecognised value) falls back to location.
        return not self.is_on_prem()


def load_topology(path: str) -> nx.Graph:
    """Return an undirected Graph where each node carries a TopoNode as 'data'
    and each edge carries a 'speed' attribute from the weight block."""
    with open(path) as f:
        raw = json.load(f)

    g: nx.Graph = nx.Graph()

    for nd in raw["nodes"]:
        known = {
            "id", "node_type", "address", "speed", "provision", "location",
            "instance_type", "cloud_instance_type", "ami",
        }
        location = nd.get("location", "cloud")
        is_on_prem = str(location).strip().lower().replace("_", "-") in ("onprem", "on-prem")
        # On-prem nodes are real cluster hardware, not an AWS instance - don't
        # default them to "t3.micro" or they'll silently be sized (for Flink
        # memory purposes, see AWS_INSTANCE_MEMORY_MB) as a 1GiB cloud
        # instance. Fall through to the on-prem fallback size instead.
        default_instance_type = "on-prem" if is_on_prem else "t3.micro"
        node = TopoNode(
            id=nd["id"],
            node_type=nd.get("node_type", "Compute"),
            address=nd.get("address", ""),
            speed=int(nd["speed"]) if nd.get("speed") is not None else None,
            provision=nd.get("provision", "auto"),
            location=location,
            # Backward-compatible alias: cloud_instance_type.
            instance_type=nd.get("instance_type", nd.get("cloud_instance_type", default_instance_type)),
            ami=nd.get("ami"),
            extra={k: v for k, v in nd.items() if k not in known},
        )
        g.add_node(node.id, data=node)

    for ed in raw.get("edges", []):
        g.add_edge(
            ed["source"],
            ed["target"],
            speed=ed.get("speed") or ed.get("weight", {}).get("speed"),
        )

    _add_implicit_all_to_all_edges(g)

    return g


def _add_implicit_all_to_all_edges(graph: nx.Graph) -> None:
    """Expand same-location ``network_type=all-to-all`` groups into edges.

    This keeps the JSON concise for flat cloud regions while still giving the
    routing and provisioning code an explicit graph to work with.
    """
    groups: dict[str, list[str]] = {}

    for node_id in graph.nodes():
        node: TopoNode = graph.nodes[node_id]["data"]
        network_type = str(node.extra.get("network_type", "")).strip().lower()
        if network_type != "all-to-all" or node.is_on_prem():
            continue

        location_key = (node.location or "cloud").strip().lower().replace("_", "-")
        groups.setdefault(location_key, []).append(node_id)

    for node_ids in groups.values():
        ordered = sorted(node_ids)
        for index, source in enumerate(ordered):
            for target in ordered[index + 1:]:
                graph.add_edge(source, target)
