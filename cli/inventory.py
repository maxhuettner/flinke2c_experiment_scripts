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


def _cloud_region(graph: nx.Graph, node_id: str, default_cloud_region: str) -> Optional[str]:
    node = graph.nodes[node_id]["data"]
    return node.cloud_region(default_cloud_region)


def _uses_native_cloud_mesh(
    graph: nx.Graph,
    node_id: str,
    default_cloud_region: str,
) -> bool:
    node = graph.nodes[node_id]["data"]
    if node.is_on_prem():
        return False
    network_type = str(node.extra.get("network_type", "")).strip().lower()
    if network_type != "all-to-all":
        return False
    return _cloud_region(graph, node_id, default_cloud_region) is not None


def _is_native_cloud_edge(
    graph: nx.Graph,
    source: str,
    target: str,
    default_cloud_region: str,
) -> bool:
    if _is_on_prem_node(graph, source) or _is_on_prem_node(graph, target):
        return False
    if _cloud_region(graph, source, default_cloud_region) != _cloud_region(
        graph, target, default_cloud_region
    ):
        return False
    return _uses_native_cloud_mesh(graph, source, default_cloud_region) and _uses_native_cloud_mesh(
        graph, target, default_cloud_region
    )


def _build_wireguard_graph(
    graph: nx.Graph,
    default_cloud_region: str,
) -> nx.Graph:
    """Return the graph edges that require WireGuard.

    Same-region cloud nodes only bypass WireGuard when both endpoints are part
    of an explicit ``network_type=all-to-all`` mesh. All other graph edges get
    WireGuard tunnels so that the overlay matches the topology.
    """
    wg_graph: nx.Graph = nx.Graph()
    wg_graph.add_nodes_from(graph.nodes(data=True))

    for u, v, attrs in graph.edges(data=True):
        if _is_native_cloud_edge(graph, u, v, default_cloud_region):
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


# Internal keys written by earlier tool versions; strip them from the output.
# Keep wireguard_endpoint so WireGuard peer endpoint derivation can use it.
_ONPREM_STRIP_KEYS = {"topology_node_id"}


def _build_onprem_hosts(
    graph: nx.Graph,
    onprem_file_vars: dict,
) -> tuple[dict, dict[str, str]]:
    """Build on-prem host entries and hostname→topo-node-id map from the graph.

    The topology's ``on-prem-id`` field is the authoritative source of which
    physical hosts participate and how they map to logical topology nodes.
    Supplementary vars from *onprem_file_vars* (e.g. ansible_user) are merged
    in, but internal-only keys are stripped.  When two topology nodes share an
    on-prem-id (same machine acting as multiple logical nodes) the first one in
    document order wins — typically the node with cross-location WireGuard edges.
    """
    onprem_id_to_topo: dict[str, str] = {}
    onprem_hosts: dict = {}

    for nid in graph.nodes():
        topo_node = graph.nodes[nid]["data"]
        if not topo_node.is_on_prem():
            continue
        opid = topo_node.extra.get("on-prem-id")
        hostname = opid or nid
        if opid and opid not in onprem_id_to_topo:
            onprem_id_to_topo[opid] = nid
        if hostname not in onprem_hosts:
            file_vars = {
                k: v for k, v in (onprem_file_vars.get(hostname) or {}).items()
                if k not in _ONPREM_STRIP_KEYS
            }
            # Topology values are authoritative defaults; inventory file vars
            # can override them when present.
            file_vars.setdefault("ansible_host", topo_node.address)
            file_vars.setdefault("wireguard_endpoint", topo_node.address)
            file_vars.setdefault("ansible_user", "ubuntu")
            file_vars.setdefault("node_class", "onprem")
            file_vars.setdefault("node_type", topo_node.node_type.lower())
            onprem_hosts[hostname] = file_vars

    return onprem_hosts, onprem_id_to_topo


def _build_wg_configs(
    graph: nx.Graph,
    cloud_host_ips: dict[str, str],
    onprem_hosts: dict,
    onprem_id_to_topo: dict[str, str],
    listen_port: int,
    salt: str,
    default_cloud_region: str,
) -> dict:
    """Compute per-node WireGuard ECMP config for all nodes in the overlay.

    Same-region cloud<->cloud edges are excluded only when both endpoints use
    ``network_type=all-to-all``. Cross-region cloud edges, explicit same-region
    cloud edges, cloud<->on-prem edges, and on-prem<->on-prem edges get
    WireGuard tunnels. On-prem nodes use their topology ``address`` as the LAN
    endpoint; an explicit ``ansible_host`` in onprem_hosts overrides that if set.
    """
    # On-prem-only topology: no cloud nodes are present/provisioned, so skip
    # WireGuard entirely and use native cluster networking.
    if not cloud_host_ips:
        return {}

    onprem_host_ips: dict[str, str] = {}
    for hostname, vars_ in onprem_hosts.items():
        ip = vars_.get("wireguard_endpoint") or vars_.get("ansible_host")
        if ip:
            onprem_host_ips[onprem_id_to_topo.get(hostname, hostname)] = ip
        else:
            topo_id = onprem_id_to_topo.get(hostname, hostname)
            print(
                f"  [warn] on-prem host '{hostname}' (topology node '{topo_id}') has no "
                "wireguard_endpoint/ansible_host in onprem.yml; falling back to topology "
                "address for peer endpoints, which may be unreachable from other hosts"
            )

    wg_graph = _build_wireguard_graph(graph, default_cloud_region)
    wg_nodes = sorted(nid for nid, deg in wg_graph.degree() if deg > 0)
    wg_host_ips = {**cloud_host_ips, **onprem_host_ips}

    wg_node_ids = []
    for nid in wg_nodes:
        if nid in wg_host_ips or _is_on_prem_node(graph, nid):
            wg_node_ids.append(nid)
        else:
            print(
                f"  [warn] no endpoint IP for cloud node '{nid}' "
                "(expected in Terraform output), skipping WireGuard edges for this node"
            )

    # route_address_map covers ALL topology nodes so that cloud-only nodes
    # (e.g. snk with no WireGuard interfaces) appear in AllowedIPs when
    # on-prem nodes can reach them transitively through a cloud gateway.
    route_address_map = {nid: graph.nodes[nid]["data"].address for nid in graph.nodes()}
    cloud_wg_nodes = [nid for nid in wg_node_ids if nid in cloud_host_ips]
    interface_address_map = _assign_interface_addresses(cloud_wg_nodes)
    for nid in wg_node_ids:
        if nid not in interface_address_map:
            interface_address_map[nid] = route_address_map[nid]

    on_prem_nodes = {nid for nid in wg_node_ids if _is_on_prem_node(graph, nid)}

    # On-prem nodes are reachable at their topology address on the local LAN.
    # ansible_host (from onprem_hosts) overrides this if a different IP is set.
    onprem_lan_ips = {nid: graph.nodes[nid]["data"].address for nid in on_prem_nodes}
    host_public_ips = {**cloud_host_ips, **onprem_lan_ips, **onprem_host_ips}

    return build_wireguard_ecmp_config(
        graph=wg_graph,
        host_public_ips=host_public_ips,
        node_ids=wg_node_ids,
        listen_port=listen_port,
        salt=salt,
        route_address_map=route_address_map,
        interface_address_map=interface_address_map,
        on_prem_nodes=on_prem_nodes,
        routing_graph=graph,
    )


def _cloud_transit_route_commands(
    graph: nx.Graph,
    src: str,
    default_cloud_region: str,
) -> list[str]:
    """Return static ECMP routes for a cloud node without direct WG interfaces."""
    src_addr = graph.nodes[src]["data"].address
    src_region = _cloud_region(graph, src, default_cloud_region)
    cloud_neighbors = [
        nbr for nbr in sorted(graph.neighbors(src))
        if _is_native_cloud_edge(graph, src, nbr, default_cloud_region)
    ]
    if not cloud_neighbors:
        return []

    commands: list[str] = []
    for dest in sorted(graph.nodes()):
        if dest == src:
            continue
        if (
            not _is_on_prem_node(graph, dest)
            and _cloud_region(graph, dest, default_cloud_region) == src_region
            and _uses_native_cloud_mesh(graph, dest, default_cloud_region)
        ):
            continue
        try:
            paths = list(nx.all_shortest_paths(graph, src, dest))
        except nx.NetworkXNoPath:
            continue

        first_hops = sorted({
            path[1]
            for path in paths
            if len(path) >= 2 and path[1] in cloud_neighbors
        })
        if not first_hops:
            continue

        dest_ip = graph.nodes[dest]["data"].address
        nexthops = " ".join(
            f"nexthop via {graph.nodes[hop]['data'].address}"
            for hop in first_hops
        )
        commands.append(f"ip route replace {dest_ip}/32 src {src_addr} {nexthops}")

    return commands


def _augment_cloud_transit_routes(
    graph: nx.Graph,
    cloud_host_ips: dict[str, str],
    wg_configs: dict,
    default_cloud_region: str,
) -> dict:
    """Add route-only configs for cloud nodes that need transit to on-prem nodes."""
    augmented = dict(wg_configs)
    for node_id in sorted(cloud_host_ips):
        existing = augmented.get(node_id)
        if existing and existing.get("wireguard_interfaces"):
            continue

        route_cmds = _cloud_transit_route_commands(
            graph,
            node_id,
            default_cloud_region,
        )
        if not route_cmds:
            continue

        augmented[node_id] = {
            "wireguard_interfaces": [],
            "wireguard_ecmp_routes": route_cmds,
        }
    return augmented


def _build_cloud_entry(
    node_id: str,
    ip: str,
    graph: nx.Graph,
    ansible_user: str,
    ansible_key: Optional[str],
    wg_configs: dict,
) -> dict:
    entry: dict = {"ansible_host": ip, "ansible_user": ansible_user, "node_class": "cloud"}
    if node_id in graph:
        entry["node_type"] = graph.nodes[node_id]["data"].node_type.lower()
    if ansible_key:
        entry["ansible_ssh_private_key_file"] = ansible_key
    if node_id in wg_configs:
        interfaces = wg_configs[node_id].get("wireguard_interfaces", [])
        routes = wg_configs[node_id].get("wireguard_ecmp_routes", [])
        if interfaces or routes:
            entry["wireguard_interfaces"] = interfaces
            entry["wireguard_ecmp_routes"] = routes
    return entry


def _apply_onprem_wg(
    onprem_hosts: dict,
    onprem_id_to_topo: dict[str, str],
    graph: nx.Graph,
    wg_configs: dict,
) -> None:
    """Stamp WireGuard config and metadata onto on-prem host entries in-place."""
    for hostname, entry in onprem_hosts.items():
        topo_nid = onprem_id_to_topo.get(hostname, hostname)
        if topo_nid in graph:
            entry.setdefault("node_type", graph.nodes[topo_nid]["data"].node_type.lower())
        entry.setdefault("node_class", "onprem")
        cfg = wg_configs.get(topo_nid, {})
        if cfg.get("wireguard_interfaces"):
            entry.setdefault("wireguard_interfaces", cfg["wireguard_interfaces"])
            entry.setdefault("wireguard_ecmp_routes", cfg.get("wireguard_ecmp_routes", []))


def write_inventory(
    graph: nx.Graph,
    instance_map: dict[str, dict],
    ansible_user: str,
    ansible_key: Optional[str],
    output_path: Path,
    onprem_path: Optional[Path] = None,
    wg_listen_port: int = DEFAULT_LISTEN_PORT,
    wg_salt: str = DEFAULT_KEY_SALT,
    default_cloud_region: str = "eu-central-1",
) -> None:
    """Write the generated Ansible inventory to *output_path*.

    Cloud hosts are sourced from *instance_map* (keyed by node id, values
    contain ``public_ip`` / ``private_ip`` as returned by ``terraform output
    -json``).  On-prem hosts are derived from the topology's ``on-prem-id``
    fields — every on-prem node that carries one gets an inventory entry
    automatically.  *onprem_path* is optional and provides supplementary vars
    (ansible_user, …) that are merged in for matching hosts.

    Same-region cloud<->cloud edges rely on native VPC routing only for
    ``network_type=all-to-all`` meshes. Explicit cloud edges outside that mesh,
    cross-region cloud edges, and any edge touching on-prem create WireGuard
    interfaces.
    """
    onprem_file_vars = _load_onprem(onprem_path)
    onprem_hosts, onprem_id_to_topo = _build_onprem_hosts(graph, onprem_file_vars)

    cloud_host_ips: dict[str, str] = {}
    for node_id, meta in instance_map.items():
        ip = meta.get("public_ip") or meta.get("private_ip")
        if ip:
            cloud_host_ips[node_id] = ip

    wg_configs = _build_wg_configs(
        graph=graph,
        cloud_host_ips=cloud_host_ips,
        onprem_hosts=onprem_hosts,
        onprem_id_to_topo=onprem_id_to_topo,
        listen_port=wg_listen_port,
        salt=wg_salt,
        default_cloud_region=default_cloud_region,
    )
    wg_configs = _augment_cloud_transit_routes(
        graph,
        cloud_host_ips,
        wg_configs,
        default_cloud_region,
    )

    cloud_hosts: dict = {}
    for node_id, meta in instance_map.items():
        ip = meta.get("public_ip") or meta.get("private_ip")
        if not ip:
            print(f"  [warn] {node_id} has no reachable IP, skipping inventory entry")
            continue
        cloud_hosts[node_id] = _build_cloud_entry(
            node_id, ip, graph, ansible_user, ansible_key, wg_configs
        )

    _apply_onprem_wg(onprem_hosts, onprem_id_to_topo, graph, wg_configs)

    inventory: dict = {"all": {"children": {"cloud": {"hosts": cloud_hosts}}}}
    if onprem_hosts:
        inventory["all"]["children"]["onprem"] = {"hosts": onprem_hosts}

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        yaml.dump(inventory, f, default_flow_style=False, sort_keys=True)
