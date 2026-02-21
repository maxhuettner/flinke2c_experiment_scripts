"""Generate the Ansible inventory YAML with embedded WireGuard config."""

import ipaddress
from pathlib import Path
from typing import Optional

import networkx as nx
import yaml

from cli.wireguard import (
    DEFAULT_KEY_SALT,
    DEFAULT_LISTEN_PORT,
    build_wireguard_ecmp_config,
)


def _load_onprem(path: Optional[Path]) -> dict:
    """Return the on-prem host map from an existing inventory file, or {}."""
    if path and path.exists():
        with open(path) as f:
            doc = yaml.safe_load(f) or {}
        hosts = (
            doc.get("all", {})
            .get("children", {})
            .get("onprem", {})
            .get("hosts", {})
        ) or {}
        if isinstance(hosts, list):
            return {str(host): {} for host in hosts}
        if isinstance(hosts, dict):
            return hosts
        print(f"  [warn] unsupported onprem hosts format in {path}: {type(hosts).__name__}")
        return {}
    return {}


def _is_on_prem_node(graph: nx.Graph, node_id: str) -> bool:
    node = graph.nodes[node_id]["data"]
    return node.is_on_prem()


def _build_cross_location_graph(graph: nx.Graph) -> nx.Graph:
    """Return a graph that only keeps cloud<->on-prem edges."""
    wg_graph: nx.Graph = nx.Graph()
    wg_graph.add_nodes_from(graph.nodes(data=True))

    for u, v, attrs in graph.edges(data=True):
        if _is_on_prem_node(graph, u) == _is_on_prem_node(graph, v):
            continue
        wg_graph.add_edge(u, v, **attrs)

    return wg_graph


def _assign_interface_addresses(
    node_ids: list[str],
    base: str = "10.255.0.0",
    prefix: int = 32,
) -> dict[str, str]:
    base_int = int(ipaddress.IPv4Address(base))
    return {
        nid: f"{ipaddress.IPv4Address(base_int + i + 1)}/{prefix}"
        for i, nid in enumerate(sorted(node_ids))
    }


def write_inventory(
    graph: nx.Graph,
    instance_map: dict[str, dict],
    ansible_user: str,
    ansible_key: Optional[str],
    output_path: Path,
    onprem_path: Optional[Path] = None,
    wg_listen_port: int = DEFAULT_LISTEN_PORT,
    wg_salt: str = DEFAULT_KEY_SALT,
) -> None:
    """Write the generated Ansible inventory to *output_path*.

    Cloud hosts are sourced from *instance_map* (keyed by node id, values
    contain ``public_ip`` / ``private_ip`` as returned by ``terraform output
    -json``).  On-prem hosts are copied verbatim from *onprem_path* so
    manually managed static inventory entries are preserved.

    WireGuard ECMP configuration is generated only for cloud<->on-prem links.
    Cloud<->cloud edges rely on native VPC routing and do not create WireGuard
    interfaces.
    """
    onprem_hosts = _load_onprem(onprem_path)
    topology_nodes = set(graph.nodes())
    filtered_onprem_hosts: dict = {}
    for node_id, vars_ in onprem_hosts.items():
        if node_id not in topology_nodes:
            print(f"  [info] on-prem host '{node_id}' not present in selected topology, skipping")
            continue
        if not _is_on_prem_node(graph, node_id):
            print(f"  [warn] host '{node_id}' is listed in onprem inventory but topology marks it as cloud; skipping")
            continue
        filtered_onprem_hosts[node_id] = vars_
    onprem_hosts = filtered_onprem_hosts

    # Cloud endpoints (Terraform output).
    cloud_host_ips: dict[str, str] = {}
    for node_id, meta in instance_map.items():
        ip = meta.get("public_ip") or meta.get("private_ip")
        if ip:
            cloud_host_ips[node_id] = ip

    # On-prem endpoints (static inventory).
    onprem_host_ips: dict[str, str] = {}
    for node_id, vars_ in onprem_hosts.items():
        ip = vars_.get("wireguard_endpoint") or vars_.get("ansible_host")
        if ip:
            onprem_host_ips[node_id] = ip

    # Build WireGuard graph from mixed-location edges only.
    wg_graph = _build_cross_location_graph(graph)
    wg_nodes = sorted([nid for nid, deg in wg_graph.degree() if deg > 0])

    # Endpoint map for peers that can actually form tunnels.
    wg_host_ips = {**cloud_host_ips, **onprem_host_ips}
    wg_node_ids = [nid for nid in wg_nodes if nid in wg_host_ips]

    missing_wg_endpoints = [nid for nid in wg_nodes if nid not in wg_host_ips]
    for nid in missing_wg_endpoints:
        print(
            f"  [warn] no endpoint IP for '{nid}' (expected in Terraform output or onprem inventory), "
            "skipping WireGuard edges for this node"
        )

    # Route targets use topology node addresses. Cloud WireGuard interfaces
    # receive dedicated tunnel IPs to avoid clashing with EC2 private IPs.
    route_address_map = {
        nid: graph.nodes[nid]["data"].address
        for nid in wg_node_ids
    }
    cloud_wg_nodes = [nid for nid in wg_node_ids if nid in cloud_host_ips]
    interface_address_map = _assign_interface_addresses(cloud_wg_nodes)
    for nid in wg_node_ids:
        if nid in interface_address_map:
            continue
        interface_address_map[nid] = route_address_map[nid]

    wg_configs = build_wireguard_ecmp_config(
        graph=wg_graph,
        host_public_ips=wg_host_ips,
        node_ids=wg_node_ids,
        listen_port=wg_listen_port,
        salt=wg_salt,
        route_address_map=route_address_map,
        interface_address_map=interface_address_map,
    )

    cloud_hosts: dict = {}
    for node_id, meta in instance_map.items():
        ip = meta.get("public_ip") or meta.get("private_ip")
        if not ip:
            print(f"  [warn] {node_id} has no reachable IP, skipping inventory entry")
            continue

        node_type = None
        if node_id in graph:
            node_type = graph.nodes[node_id]["data"].node_type.lower()

        entry: dict = {
            "ansible_host": ip,
            "ansible_user": ansible_user,
            "node_class": "cloud",
        }
        if node_type:
            entry["node_type"] = node_type
        if ansible_key:
            entry["ansible_ssh_private_key_file"] = ansible_key
        if node_id in wg_configs and wg_configs[node_id]["wireguard_interfaces"]:
            entry["wireguard_interfaces"] = wg_configs[node_id]["wireguard_interfaces"]
            entry["wireguard_ecmp_routes"] = wg_configs[node_id].get("wireguard_ecmp_routes", [])

        cloud_hosts[node_id] = entry

    # Populate on-prem WireGuard metadata when host IDs match topology node IDs.
    for node_id, entry in onprem_hosts.items():
        if node_id in graph:
            entry.setdefault("node_type", graph.nodes[node_id]["data"].node_type.lower())
        if node_id in wg_configs and wg_configs[node_id]["wireguard_interfaces"]:
            if "wireguard_interfaces" not in entry:
                entry["wireguard_interfaces"] = wg_configs[node_id]["wireguard_interfaces"]
            if "wireguard_ecmp_routes" not in entry:
                entry["wireguard_ecmp_routes"] = wg_configs[node_id].get("wireguard_ecmp_routes", [])
            entry.setdefault("node_class", "onprem")

    inventory: dict = {
        "all": {
            "children": {
                "cloud": {"hosts": cloud_hosts},
            }
        }
    }
    if onprem_hosts:
        inventory["all"]["children"]["onprem"] = {"hosts": onprem_hosts}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        yaml.dump(inventory, f, default_flow_style=False, sort_keys=True)
