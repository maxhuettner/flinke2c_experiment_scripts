"""Compute per-node WireGuard configuration from a topology graph.

Multi-hop forwarding needs, per intermediate node: IP forwarding enabled and
FORWARD iptables rules (both handled by Ansible), plus AllowedIPs entries for
every destination whose shortest path starts with that peer's tunnel, not
just the peer's own address. This module computes that third part.

Keypairs are derived deterministically from SHA-256(salt || node_id), so
reruns don't drift, byte-compatible with the prior Rust x25519-dalek impl.
"""

import base64
import hashlib
import ipaddress
from typing import Optional

import networkx as nx
from cryptography.hazmat.primitives.asymmetric.x25519 import X25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

DEFAULT_NETWORK_BASE = "10.10.10.0"
DEFAULT_PREFIX = 32
DEFAULT_LISTEN_PORT = 51820
DEFAULT_INTERFACE = "wg0"
DEFAULT_KEY_SALT = "network-sim-wireguard"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _generate_keypair(node_id: str, salt: str) -> tuple[str, str]:
    """Return (private_key_b64, public_key_b64) deterministically."""
    # SHA-256(salt_bytes || node_id_bytes) — matches the Rust implementation.
    digest = hashlib.sha256(salt.encode() + node_id.encode()).digest()
    priv = X25519PrivateKey.from_private_bytes(digest)
    pub = priv.public_key()
    priv_b64 = base64.b64encode(
        priv.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    ).decode()
    pub_b64 = base64.b64encode(
        pub.public_bytes(Encoding.Raw, PublicFormat.Raw)
    ).decode()
    return priv_b64, pub_b64


def _assign_addresses(
    node_ids: list[str],
    base: str = DEFAULT_NETWORK_BASE,
    prefix: int = DEFAULT_PREFIX,
) -> dict[str, str]:
    """Assign sequential /prefix WireGuard addresses (sorted ids → stable mapping)."""
    base_int = int(ipaddress.IPv4Address(base))
    return {
        nid: f"{ipaddress.IPv4Address(base_int + i + 1)}/{prefix}"
        for i, nid in enumerate(sorted(node_ids))
    }


def _compute_next_hops(
    graph: nx.Graph,
    participants: list[str],
) -> dict[str, dict[str, str]]:
    """Return {source: {destination: next_hop}} via BFS on the participant sub-graph."""
    sub = graph.subgraph(participants)
    tables: dict[str, dict[str, str]] = {}
    for src in participants:
        routes: dict[str, str] = {}
        paths = nx.single_source_shortest_path(sub, src)
        for dest, path in paths.items():
            if dest == src or len(path) < 2:
                continue
            routes[dest] = path[1]  # first hop after src
        tables[src] = routes
    return tables


def _compute_all_nexthops(
    graph: nx.Graph,
    participants: list[str],
    routing_graph: Optional[nx.Graph] = None,
) -> dict[str, dict[str, list[str]]]:
    """Return {source: {destination: [nexthop, ...]}}, all equal-cost next-hops (for ECMP).

    *routing_graph*, if given, is used for path-finding (so cloud-only nodes
    reachable via VPC appear as destinations); *graph* still determines valid
    first hops, restricted to direct WireGuard neighbours.
    """
    sub = graph.subgraph(participants)
    rg = routing_graph if routing_graph is not None else sub
    all_dests = sorted(rg.nodes())
    tables: dict[str, dict[str, list[str]]] = {}
    for src in participants:
        wg_nbrs = set(sub.neighbors(src))
        routes: dict[str, list[str]] = {}
        for dest in all_dests:
            if dest == src:
                continue
            try:
                paths = list(nx.all_shortest_paths(rg, src, dest))
            except nx.NetworkXNoPath:
                continue
            nexthops = sorted({p[1] for p in paths if len(p) >= 2 and p[1] in wg_nbrs})
            if nexthops:
                routes[dest] = nexthops
        tables[src] = routes
    return tables


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_wireguard_config(
    graph: nx.Graph,
    host_public_ips: dict[str, str],
    node_ids: Optional[list[str]] = None,
    interface: str = DEFAULT_INTERFACE,
    listen_port: int = DEFAULT_LISTEN_PORT,
    salt: str = DEFAULT_KEY_SALT,
    network_base: str = DEFAULT_NETWORK_BASE,
    prefix: int = DEFAULT_PREFIX,
) -> dict[str, dict]:
    """Return a per-node WireGuard config dict, keyed by node id, for the Ansible ``wireguard:`` var.

    Only nodes present in *host_public_ips* participate (others, e.g. on-prem
    nodes with manually managed configs, are skipped). For peer P on node S,
    AllowedIPs covers every destination whose shortest path from S starts
    with S→P, enabling multi-hop forwarding without a full tunnel mesh.
    """
    participants = sorted(
        nid for nid in (node_ids or list(graph.nodes())) if nid in host_public_ips
    )

    address_map = _assign_addresses(participants, network_base, prefix)
    key_map = {nid: _generate_keypair(nid, salt) for nid in participants}
    next_hop_tables = _compute_next_hops(graph, participants)

    result: dict[str, dict] = {}
    for src in participants:
        routes = next_hop_tables.get(src, {})

        # AllowedIPs for peer X = address of every destination routed via X.
        allowed_by_peer: dict[str, list[str]] = {}
        for dest, next_hop in routes.items():
            wg_ip = address_map[dest].split("/")[0]
            allowed_by_peer.setdefault(next_hop, []).append(f"{wg_ip}/32")

        peers = []
        for peer_id in sorted(allowed_by_peer):
            peers.append({
                "name": peer_id,
                "public_key": key_map[peer_id][1],
                "endpoint": f"{host_public_ips[peer_id]}:{listen_port}",
                "allowed_ips": sorted(allowed_by_peer[peer_id]),
                "persistent_keepalive": 25,
            })

        result[src] = {
            "interface": interface,
            "listen_port": listen_port,
            "private_key": key_map[src][0],
            "address": address_map[src],
            "endpoint": f"{host_public_ips[src]}:{listen_port}",
            "peers": peers,
        }

    return result


def _ensure_cidr(ip: str) -> str:
    return ip if "/" in ip else f"{ip}/32"


def _build_route_map(
    nodes: list[str],
    graph: nx.Graph,
    route_address_map: Optional[dict[str, str]],
) -> dict[str, str]:
    """Build IP map for *nodes* (may include WG-less cloud nodes, e.g. a sink,
    so their addresses can still appear in AllowedIPs via a cloud gateway)."""
    if route_address_map is None:
        return {nid: f"{graph.nodes[nid]['data'].address}/32" for nid in nodes if nid in graph}
    return {
        nid: _ensure_cidr(route_address_map[nid])
        for nid in nodes
        if nid in route_address_map
    }


def _build_iface_map(
    participants: list[str],
    route_map: dict[str, str],
    interface_address_map: Optional[dict[str, str]],
) -> dict[str, str]:
    if interface_address_map is None:
        return {nid: route_map[nid] for nid in participants if nid in route_map}
    return {
        nid: _ensure_cidr(interface_address_map.get(nid, route_map[nid]))
        for nid in participants
    }


def _peer_endpoint(
    src: str,
    nbr: str,
    port: int,
    host_public_ips: dict[str, str],
    on_prem_nodes: Optional[set[str]],
) -> Optional[str]:
    """Return the WireGuard endpoint for *nbr* as seen from *src*, or None for
    cloud→on-prem (on-prem sits behind NAT; cloud learns it from the handshake)."""
    src_is_cloud = on_prem_nodes is None or src not in on_prem_nodes
    nbr_is_on_prem = on_prem_nodes is not None and nbr in on_prem_nodes
    if src_is_cloud and nbr_is_on_prem:
        return None
    return f"{host_public_ips[nbr]}:{port}"


def _peer_allowed_ips(
    nbr: str,
    nbr_route_ip: str,
    routes: dict[str, list[str]],
    route_map: dict[str, str],
    nbr_iface_ip: Optional[str] = None,
) -> list[str]:
    """AllowedIPs: neighbor's own IPs (route + interface address, which can
    differ on cloud nodes) plus every destination routed through it."""
    allowed: set[str] = {nbr_route_ip}
    if nbr_iface_ip and nbr_iface_ip != nbr_route_ip:
        allowed.add(nbr_iface_ip)
    for dest, nexthops in routes.items():
        if nbr in nexthops:
            dest_ip = route_map[dest].split("/")[0] + "/32"
            allowed.add(dest_ip)
    return sorted(allowed)


def _peer_direct_routes(
    nbr: str,
    nbr_route_ip: str,
    routes: dict[str, list[str]],
    route_map: dict[str, str],
) -> list[str]:
    """Direct routes: neighbor's IP + destinations exclusively reached via this neighbor."""
    direct = [nbr_route_ip]
    for dest, nexthops in sorted(routes.items()):
        if nexthops == [nbr]:
            dest_ip = route_map[dest].split("/")[0] + "/32"
            if dest_ip != nbr_route_ip:
                direct.append(dest_ip)
    return direct


def _ecmp_route_commands(
    routes: dict[str, list[str]],
    route_map: dict[str, str],
    src_address: Optional[str] = None,
) -> list[str]:
    """ECMP ip-route commands for destinations with multiple equal-cost next-hops."""
    commands = []
    src_hint = f" src {src_address}" if src_address else ""
    for dest, nexthops in sorted(routes.items()):
        if len(nexthops) > 1:
            nexthop_args = " ".join(f"nexthop dev wg_{nh}" for nh in nexthops)
            dest_ip = route_map[dest].split("/")[0] + "/32"
            commands.append(f"ip route replace {dest_ip}{src_hint} {nexthop_args}")
    return commands


def build_wireguard_ecmp_config(
    graph: nx.Graph,
    host_public_ips: dict[str, str],
    node_ids: Optional[list[str]] = None,
    listen_port: int = DEFAULT_LISTEN_PORT,
    salt: str = DEFAULT_KEY_SALT,
    route_address_map: Optional[dict[str, str]] = None,
    interface_address_map: Optional[dict[str, str]] = None,
    on_prem_nodes: Optional[set[str]] = None,
    routing_graph: Optional[nx.Graph] = None,
) -> dict[str, dict]:
    """Return per-node ECMP WireGuard config: one ``wg_{neighbor}`` interface per
    direct graph-neighbor, each with a ``wireguard_interfaces`` entry and a
    ``wireguard_ecmp_routes`` list of ``ip route replace ... nexthop ...``
    commands for destinations with more than one equal-cost next-hop.

    *routing_graph*, if given (e.g. the full topology including cloud-only
    edges), extends nexthop computation so WG-less nodes like a sink still
    appear in AllowedIPs via a cloud gateway. Ports are assigned per edge,
    stable-sorted by (min(u,v), max(u,v)) so both ends agree; each edge
    endpoint gets its own keypair from ``edge_{min}_{max}_{node}``.
    """
    participants = sorted(
        nid for nid in (node_ids or list(graph.nodes()))
        if nid in host_public_ips or (on_prem_nodes and nid in on_prem_nodes)
    )
    sub = graph.subgraph(participants)

    # Also covers routing-graph-only nodes so e.g. snk appears in AllowedIPs.
    rg = routing_graph if routing_graph is not None else graph
    routing_nodes = sorted(set(participants) | set(rg.nodes()))
    route_map = _build_route_map(routing_nodes, rg, route_address_map)
    iface_map = _build_iface_map(participants, route_map, interface_address_map)

    edges = sorted({(min(u, v), max(u, v)) for u, v in sub.edges()})
    edge_ports = {edge: listen_port + i for i, edge in enumerate(edges)}
    edge_keys: dict[tuple[str, str], dict[str, tuple[str, str]]] = {
        (u, v): {
            u: _generate_keypair(f"edge_{u}_{v}_{u}", salt),
            v: _generate_keypair(f"edge_{u}_{v}_{v}", salt),
        }
        for (u, v) in edges
    }
    all_nexthops = _compute_all_nexthops(graph, participants, routing_graph=rg)

    result: dict[str, dict] = {}
    for src in participants:
        routes = all_nexthops.get(src, {})
        src_address = route_map[src].split("/")[0] if src in route_map else None
        interfaces = []
        for nbr in sorted(sub.neighbors(src)):
            edge_key = (min(src, nbr), max(src, nbr))
            port = edge_ports[edge_key]
            nbr_route_ip = route_map[nbr].split("/")[0] + "/32"
            nbr_iface_ip = iface_map[nbr].split("/")[0] + "/32" if nbr in iface_map else None
            peer: dict = {
                "public_key": edge_keys[edge_key][nbr][1],
                "allowed_ips": _peer_allowed_ips(nbr, nbr_route_ip, routes, route_map, nbr_iface_ip),
                "persistent_keepalive": 25,
                "direct_routes": _peer_direct_routes(nbr, nbr_route_ip, routes, route_map),
            }
            endpoint = _peer_endpoint(src, nbr, port, host_public_ips, on_prem_nodes)
            if endpoint is not None:
                peer["endpoint"] = endpoint
            interfaces.append({
                "interface": f"wg_{nbr}",
                "listen_port": port,
                "private_key": edge_keys[edge_key][src][0],
                "address": iface_map[src],
                "node_address": src_address,
                "speed": sub[src][nbr].get("speed"),
                "peers": [peer],
            })
        result[src] = {
            "wireguard_interfaces": interfaces,
            "wireguard_ecmp_routes": _ecmp_route_commands(
                routes,
                route_map,
                src_address=src_address,
            ),
        }
    return result
