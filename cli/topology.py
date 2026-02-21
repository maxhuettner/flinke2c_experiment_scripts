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
        node = TopoNode(
            id=nd["id"],
            node_type=nd.get("node_type", "Compute"),
            address=nd.get("address", ""),
            speed=nd.get("speed"),
            provision=nd.get("provision", "auto"),
            location=nd.get("location", "cloud"),
            # Backward-compatible alias: cloud_instance_type.
            instance_type=nd.get("instance_type", nd.get("cloud_instance_type", "t3.micro")),
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

    return g
