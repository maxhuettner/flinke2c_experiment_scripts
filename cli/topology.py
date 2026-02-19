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
    # "cloud" provisions an EC2 instance; anything else skips provisioning
    provision: str = "cloud"
    instance_type: str = "t3.micro"
    ami: Optional[str] = None
    extra: dict = field(default_factory=dict)

    def should_provision(self) -> bool:
        return self.provision.lower() not in (
            "existing", "static", "skip", "false", "onprem", "on-prem",
        )


def load_topology(path: str) -> nx.Graph:
    """Return an undirected Graph where each node carries a TopoNode as 'data'
    and each edge carries a 'speed' attribute from the weight block."""
    with open(path) as f:
        raw = json.load(f)

    g: nx.Graph = nx.Graph()

    for nd in raw["nodes"]:
        known = {"id", "node_type", "address", "speed", "provision", "instance_type", "ami"}
        node = TopoNode(
            id=nd["id"],
            node_type=nd.get("node_type", "Compute"),
            address=nd.get("address", ""),
            speed=nd.get("speed"),
            provision=nd.get("provision", "cloud"),
            instance_type=nd.get("instance_type", "t3.micro"),
            ami=nd.get("ami"),
            extra={k: v for k, v in nd.items() if k not in known},
        )
        g.add_node(node.id, data=node)

    for ed in raw.get("edges", []):
        g.add_edge(
            ed["source"],
            ed["target"],
            speed=ed.get("weight", {}).get("speed"),
        )

    return g
