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


def _default_tfvars(
    aws_region: str,
    vpc_cidr: str,
    public_subnet_cidr: str,
    ssh_ingress_cidrs: Optional[list[str]],
    wireguard_ingress_cidrs: Optional[list[str]],
    wireguard_udp_port: int,
    wireguard_udp_port_max: int,
    ssh_key_pair_name: str,
    ssh_public_key: Optional[str],
) -> dict:
    tfvars: dict = {
        "aws_region": aws_region,
        "vpc_cidr": vpc_cidr,
        "public_subnet_cidr": public_subnet_cidr,
        "ssh_ingress_cidrs": ssh_ingress_cidrs or _DEFAULTS["ssh_ingress_cidrs"],
        "wireguard_ingress_cidrs": wireguard_ingress_cidrs or _DEFAULTS["wireguard_ingress_cidrs"],
        "wireguard_udp_port": wireguard_udp_port,
        "wireguard_udp_port_max": wireguard_udp_port_max,
        "ssh_key_pair_name": ssh_key_pair_name,
        "ec2_instances": {},
    }
    if ssh_public_key:
        tfvars["ssh_public_key"] = ssh_public_key
    return tfvars


def _cloud_nodes_by_region(
    graph: nx.Graph,
    default_region: str,
) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for node_id in graph.nodes():
        node: TopoNode = graph.nodes[node_id]["data"]
        if not node.should_provision():
            continue

        region = node.cloud_region(default_region)
        if region is None:
            continue
        groups.setdefault(region, []).append(node_id)

    return {region: sorted(node_ids) for region, node_ids in sorted(groups.items())}


def _cloud_transit_nodes(graph: nx.Graph) -> set[str]:
    """Return provisioned cloud nodes that forward traffic for other nodes.

    Any cloud node that appears as an interior hop on a shortest path between
    two topology nodes may need to route packets that are neither sourced from
    nor destined to itself. Those EC2 instances must have source/dest check
    disabled or AWS will drop the forwarded traffic.
    """
    node_ids = sorted(graph.nodes())
    transit_nodes: set[str] = set()

    for index, src in enumerate(node_ids):
        for dst in node_ids[index + 1:]:
            paths = nx.all_shortest_paths(graph, src, dst)

            for path in paths:
                for node_id in path[1:-1]:
                    node: TopoNode = graph.nodes[node_id]["data"]
                    if node.should_provision() and not node.is_on_prem():
                        transit_nodes.add(node_id)

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
    """Build single-region tfvars.

    Multi-region topologies must use ``build_tfvars_by_region``.
    """
    tfvars_by_region = build_tfvars_by_region(
        graph,
        ssh_public_key=ssh_public_key,
        aws_region=aws_region,
        vpc_cidr=vpc_cidr,
        public_subnet_cidr=public_subnet_cidr,
        ssh_ingress_cidrs=ssh_ingress_cidrs,
        wireguard_ingress_cidrs=wireguard_ingress_cidrs,
        wireguard_udp_port=wireguard_udp_port,
        wireguard_udp_port_max=wireguard_udp_port_max,
        ssh_key_pair_name=ssh_key_pair_name,
        default_instance_type=default_instance_type,
    )
    if not tfvars_by_region:
        return _default_tfvars(
            aws_region=aws_region,
            vpc_cidr=vpc_cidr,
            public_subnet_cidr=public_subnet_cidr,
            ssh_ingress_cidrs=ssh_ingress_cidrs,
            wireguard_ingress_cidrs=wireguard_ingress_cidrs,
            wireguard_udp_port=wireguard_udp_port,
            wireguard_udp_port_max=wireguard_udp_port_max,
            ssh_key_pair_name=ssh_key_pair_name,
            ssh_public_key=ssh_public_key,
        )
    if len(tfvars_by_region) > 1:
        raise ValueError(
            "Topology spans multiple AWS regions. Use build_tfvars_by_region() "
            "and provision each region separately."
        )
    return next(iter(tfvars_by_region.values()))


def build_tfvars_by_region(
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
) -> dict[str, dict]:
    """Build one Terraform tfvars document per AWS region in the topology."""
    tfvars_by_region: dict[str, dict] = {}
    cloud_nodes_by_region = _cloud_nodes_by_region(graph, aws_region)

    if not cloud_nodes_by_region:
        return {}

    cloud_transit_nodes = _cloud_transit_nodes(graph)
    for region, region_node_ids in cloud_nodes_by_region.items():
        tfvars_by_region[region] = _build_region_tfvars(
            graph=graph,
            node_ids=region_node_ids,
            transit_nodes=cloud_transit_nodes,
            ssh_public_key=ssh_public_key,
            aws_region=region,
            vpc_cidr=vpc_cidr,
            public_subnet_cidr=public_subnet_cidr,
            ssh_ingress_cidrs=ssh_ingress_cidrs,
            wireguard_ingress_cidrs=wireguard_ingress_cidrs,
            wireguard_udp_port=wireguard_udp_port,
            wireguard_udp_port_max=wireguard_udp_port_max,
            ssh_key_pair_name=ssh_key_pair_name,
            default_instance_type=default_instance_type,
        )

    return tfvars_by_region


def _build_region_tfvars(
    graph: nx.Graph,
    node_ids: list[str],
    transit_nodes: set[str],
    ssh_public_key: Optional[str],
    aws_region: str,
    vpc_cidr: str,
    public_subnet_cidr: str,
    ssh_ingress_cidrs: Optional[list[str]],
    wireguard_ingress_cidrs: Optional[list[str]],
    wireguard_udp_port: int,
    wireguard_udp_port_max: int,
    ssh_key_pair_name: str,
    default_instance_type: str,
) -> dict:
    """Build the tfvars dict for one Terraform region/state."""
    instances: dict = {}
    cloud_private_ips: list[ipaddress.IPv4Address] = []
    cloud_private_ip_by_node: dict[str, ipaddress.IPv4Address] = {}

    for node_id in node_ids:
        node: TopoNode = graph.nodes[node_id]["data"]

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
        if node_id in transit_nodes:
            instance["source_dest_check"] = False
        # Cloud nodes use their topology address as the EC2 private IP within
        # their region-local VPC so same-region links can use native routing.
        if node.address:
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

    tfvars = _default_tfvars(
        aws_region=aws_region,
        vpc_cidr=effective_vpc_cidr,
        public_subnet_cidr=effective_public_subnet_cidr,
        ssh_ingress_cidrs=ssh_ingress_cidrs,
        wireguard_ingress_cidrs=wireguard_ingress_cidrs,
        wireguard_udp_port=wireguard_udp_port,
        wireguard_udp_port_max=wireguard_udp_port_max,
        ssh_key_pair_name=ssh_key_pair_name,
        ssh_public_key=ssh_public_key,
    )
    tfvars["ec2_instances"] = instances
    return tfvars


def write_tfvars(path: Path, tfvars: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(tfvars, f, indent=2)
        f.write("\n")
