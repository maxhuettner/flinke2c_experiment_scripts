"""Build and write Terraform variable files from the topology graph."""

import json
from pathlib import Path
from typing import Optional

import networkx as nx

from cli.topology import TopoNode

_DEFAULTS = dict(
    aws_region="eu-central-1",
    vpc_cidr="10.42.0.0/16",
    public_subnet_cidr="10.42.0.0/20",
    # Allow SSH from anywhere (lab usage).
    ssh_ingress_cidrs=["0.0.0.0/0"],
    # Allow WireGuard from anywhere so nodes can reach each other.
    # Without this the Terraform security group has no WireGuard ingress rule
    # and inter-node tunnels fail silently.
    wireguard_ingress_cidrs=["0.0.0.0/0"],
    wireguard_udp_port=51820,
    ssh_key_pair_name="network-sim-cli",
    default_instance_type="t3.micro",
)


def build_tfvars(
    graph: nx.Graph,
    ssh_public_key: Optional[str] = None,
    aws_region: str = _DEFAULTS["aws_region"],
    vpc_cidr: str = _DEFAULTS["vpc_cidr"],
    public_subnet_cidr: str = _DEFAULTS["public_subnet_cidr"],
    ssh_ingress_cidrs: Optional[list[str]] = None,
    wireguard_ingress_cidrs: Optional[list[str]] = None,
    wireguard_udp_port: int = _DEFAULTS["wireguard_udp_port"],
    ssh_key_pair_name: str = _DEFAULTS["ssh_key_pair_name"],
    default_instance_type: str = _DEFAULTS["default_instance_type"],
) -> dict:
    """Build the dict that will be serialised as generated.auto.tfvars.json."""
    instances: dict = {}
    for node_id in graph.nodes():
        node: TopoNode = graph.nodes[node_id]["data"]
        if not node.should_provision():
            continue

        tags: dict[str, str] = {
            "Name": node_id,
            "graph_node_id": node_id,
            "graph_node_type": node.node_type.lower(),
        }
        if node.speed is not None:
            tags["speed"] = str(node.speed)
        if node.address:
            tags["address"] = node.address

        instance: dict = {
            "instance_type": node.instance_type or default_instance_type,
            "tags": tags,
        }
        if node.ami:
            instance["ami"] = node.ami

        instances[node_id] = instance

    tfvars: dict = {
        "aws_region": aws_region,
        "vpc_cidr": vpc_cidr,
        "public_subnet_cidr": public_subnet_cidr,
        "ssh_ingress_cidrs": ssh_ingress_cidrs or _DEFAULTS["ssh_ingress_cidrs"],
        "wireguard_ingress_cidrs": wireguard_ingress_cidrs or _DEFAULTS["wireguard_ingress_cidrs"],
        "wireguard_udp_port": wireguard_udp_port,
        "ssh_key_pair_name": ssh_key_pair_name,
        "ec2_instances": instances,
    }
    if ssh_public_key:
        tfvars["ssh_public_key"] = ssh_public_key

    return tfvars


def write_tfvars(path: Path, tfvars: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(tfvars, f, indent=2)
        f.write("\n")
