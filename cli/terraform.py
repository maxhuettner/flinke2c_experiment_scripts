"""Build and write Terraform variable files from the topology graph."""

import ipaddress
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
    wireguard_udp_port_max=51999,
    ssh_key_pair_name="network-sim-cli",
    default_instance_type="t3.micro",
)


def _cloud_transit_nodes(graph: nx.Graph) -> set[str]:
    """Return provisioned cloud nodes that sit on cloud<->on-prem paths.

    These nodes act as transit routers for sink/source or other cloud nodes
    reaching on-prem nodes over the mixed topology, so EC2 source/dest check
    must be disabled on them.
    """
    provisioned_cloud_nodes = [
        node_id
        for node_id in graph.nodes()
        if graph.nodes[node_id]["data"].should_provision()
        and not graph.nodes[node_id]["data"].is_on_prem()
    ]
    on_prem_nodes = [
        node_id for node_id in graph.nodes() if graph.nodes[node_id]["data"].is_on_prem()
    ]

    transit_nodes: set[str] = set()
    for src in provisioned_cloud_nodes:
        for dst in on_prem_nodes:
            try:
                for path in nx.all_shortest_paths(graph, src, dst):
                    for node_id in path[1:-1]:
                        node: TopoNode = graph.nodes[node_id]["data"]
                        if node.should_provision() and not node.is_on_prem():
                            transit_nodes.add(node_id)
            except nx.NetworkXNoPath:
                continue
    return transit_nodes


def _infer_aws_subnet_for_ips(ips: list[ipaddress.IPv4Address]) -> Optional[ipaddress.IPv4Network]:
    """Return an AWS-valid subnet (/24.. /16) containing all IPs.

    We intentionally avoid very small inferred subnets like /28 because
    topology node addresses may use low host IPs (for example .15), which can
    accidentally become the subnet broadcast address.
    """
    if not ips:
        return None
    first = min(ips)
    for prefix in range(24, 15, -1):
        network = ipaddress.IPv4Network((first, prefix), strict=False)
        if all(ip in network for ip in ips):
            return network
    return None


def _aws_reserved_addresses(subnet: ipaddress.IPv4Network) -> set[ipaddress.IPv4Address]:
    base = int(subnet.network_address)
    return {
        ipaddress.IPv4Address(base + 0),
        ipaddress.IPv4Address(base + 1),
        ipaddress.IPv4Address(base + 2),
        ipaddress.IPv4Address(base + 3),
        ipaddress.IPv4Address(base + subnet.num_addresses - 1),
    }


def build_tfvars(
    graph: nx.Graph,
    ssh_public_key: Optional[str] = None,
    aws_region: str = _DEFAULTS["aws_region"],
    vpc_cidr: str = _DEFAULTS["vpc_cidr"],
    public_subnet_cidr: str = _DEFAULTS["public_subnet_cidr"],
    ssh_ingress_cidrs: Optional[list[str]] = None,
    wireguard_ingress_cidrs: Optional[list[str]] = None,
    wireguard_udp_port: int = _DEFAULTS["wireguard_udp_port"],
    wireguard_udp_port_max: int = _DEFAULTS["wireguard_udp_port_max"],
    ssh_key_pair_name: str = _DEFAULTS["ssh_key_pair_name"],
    default_instance_type: str = _DEFAULTS["default_instance_type"],
) -> dict:
    """Build the dict that will be serialised as generated.auto.tfvars.json."""
    instances: dict = {}
    cloud_private_ips: list[ipaddress.IPv4Address] = []
    cloud_private_ip_by_node: dict[str, ipaddress.IPv4Address] = {}
    cloud_transit_nodes = _cloud_transit_nodes(graph)
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
        if node_id in cloud_transit_nodes:
            instance["source_dest_check"] = False
        # Cloud nodes use their topology address as the EC2 private IP so
        # cloud-to-cloud links can use native VPC routing without WireGuard.
        if node.address and not node.is_on_prem():
            try:
                node_ip = ipaddress.IPv4Address(node.address)
                instance["private_ip"] = node.address
                cloud_private_ips.append(node_ip)
                cloud_private_ip_by_node[node_id] = node_ip
            except ValueError:
                print(
                    f"  [warn] node '{node_id}' has non-IPv4 address '{node.address}', "
                    "skipping explicit EC2 private_ip assignment"
                )
        if node.ami:
            instance["ami"] = node.ami

        instances[node_id] = instance

    effective_vpc_cidr = vpc_cidr
    effective_public_subnet_cidr = public_subnet_cidr
    try:
        current_subnet = ipaddress.IPv4Network(public_subnet_cidr, strict=False)
        current_vpc = ipaddress.IPv4Network(vpc_cidr, strict=False)
        if cloud_private_ips and not all(ip in current_subnet for ip in cloud_private_ips):
            inferred_subnet = _infer_aws_subnet_for_ips(cloud_private_ips)
            if inferred_subnet:
                effective_public_subnet_cidr = str(inferred_subnet)
                if not inferred_subnet.subnet_of(current_vpc):
                    effective_vpc_cidr = str(inferred_subnet)
                print(
                    f"  [info] adjusted VPC/subnet CIDR to fit topology node addresses: "
                    f"vpc={effective_vpc_cidr}, subnet={effective_public_subnet_cidr}"
                )
            else:
                print(
                    "  [warn] could not infer an AWS-valid subnet (/28.. /16) covering all "
                    "topology node addresses; keeping configured CIDRs"
                )
    except ValueError:
        print(
            f"  [warn] invalid VPC/subnet CIDR(s): vpc='{vpc_cidr}', subnet='{public_subnet_cidr}'. "
            "Terraform may fail to apply"
        )

    # AWS reserves the first 4 and last IP in every subnet, so those addresses
    # can never be assigned to instances. Fail fast with an actionable error.
    final_subnet = ipaddress.IPv4Network(effective_public_subnet_cidr, strict=False)
    reserved = _aws_reserved_addresses(final_subnet)
    invalid_assignments: list[str] = []
    for node_id, node_ip in sorted(cloud_private_ip_by_node.items()):
        if node_ip in reserved:
            invalid_assignments.append(f"{node_id}={node_ip}")
    if invalid_assignments:
        raise ValueError(
            "Topology uses AWS-reserved private IPs for cloud nodes in subnet "
            f"{final_subnet}: {', '.join(invalid_assignments)}. "
            "Use addresses not in the first four or last subnet IP (for example 10.10.0.8+)."
        )

    tfvars: dict = {
        "aws_region": aws_region,
        "vpc_cidr": effective_vpc_cidr,
        "public_subnet_cidr": effective_public_subnet_cidr,
        "ssh_ingress_cidrs": ssh_ingress_cidrs or _DEFAULTS["ssh_ingress_cidrs"],
        "wireguard_ingress_cidrs": wireguard_ingress_cidrs or _DEFAULTS["wireguard_ingress_cidrs"],
        "wireguard_udp_port": wireguard_udp_port,
        "wireguard_udp_port_max": wireguard_udp_port_max,
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
