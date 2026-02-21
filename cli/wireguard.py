"""Compute per-node WireGuard configuration from a topology graph.

Routing strategy
----------------
WireGuard's AllowedIPs acts as a per-peer routing table entry.  For a packet
on node A destined for node E, we need A to send it to whichever direct
neighbour lies on the shortest path to E.  That neighbour then forwards it on,
and so on, until it reaches E.

For this forwarding chain to work every intermediate node must:

  1. Have IP-forwarding enabled  (net.ipv4.ip_forward = 1, set by Ansible).
  2. Have iptables FORWARD rules that permit traffic in/out of wg0 (set via
     PostUp/PreDown in the WireGuard config template).
  3. Have AllowedIPs entries for every destination whose shortest path begins
     with the direct tunnel to that peer — not just the peer's own WireGuard
     IP.

This module covers point 3.  Points 1 and 2 are handled in the Ansible role.

Key generation
--------------
Keypairs are derived deterministically from (salt || node_id) via SHA-256 so
that re-running the tool produces the same keys (no drift in deployed configs).
The algorithm is byte-compatible with the previous Rust x25519-dalek
implementation:  SHA-256(salt_bytes || node_id_bytes) → raw scalar → X25519.
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
) -> dict[str, dict[str, list[str]]]:
    """Return {source: {destination: [nexthop, ...]}} with all ECMP next-hops.

    Uses nx.all_shortest_paths so that destinations with multiple equal-cost
    paths contribute multiple next-hops, enabling kernel ECMP routes.
    """
    sub = graph.subgraph(participants)
    tables: dict[str, dict[str, list[str]]] = {}
    for src in participants:
        routes: dict[str, list[str]] = {}
        for dest in participants:
            if dest == src:
                continue
            try:
                paths = list(nx.all_shortest_paths(sub, src, dest))
            except nx.NetworkXNoPath:
                continue
            nexthops = sorted({p[1] for p in paths if len(p) >= 2})
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
    """Return a per-node WireGuard config dict keyed by node id.

    *host_public_ips* maps node_id → public IP string.  Only nodes present
    in that dict participate in the overlay; others are silently skipped (so
    on-prem nodes with manually managed WireGuard configs are not touched).

    Each value is a dict suitable for embedding under the ``wireguard:`` key
    of an Ansible inventory host entry.

    AllowedIPs explained
    --------------------
    For peer P on source node S, AllowedIPs contains the WireGuard address of
    every destination D where the BFS shortest path from S to D begins with
    the hop S→P.  This lets the kernel route traffic for non-adjacent nodes
    through the correct tunnel, enabling multi-hop forwarding without a full
    mesh of direct WireGuard sessions.
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

        # Group every reachable destination by its next-hop peer.
        # AllowedIPs for peer X = WireGuard address of every D where next_hop == X.
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


def build_wireguard_ecmp_config(
    graph: nx.Graph,
    host_public_ips: dict[str, str],
    node_ids: Optional[list[str]] = None,
    listen_port: int = DEFAULT_LISTEN_PORT,
    salt: str = DEFAULT_KEY_SALT,
    route_address_map: Optional[dict[str, str]] = None,
    interface_address_map: Optional[dict[str, str]] = None,
) -> dict[str, dict]:
    """Return per-node ECMP WireGuard config with one interface per direct neighbor.

    Each node gets a dict with:
      wireguard_interfaces  – list of per-neighbor wg interface configs
      wireguard_ecmp_routes – list of ``ip route replace … nexthop …`` commands

    One WireGuard interface is created per direct graph-neighbor (named
    ``wg_{neighbor}``).  AllowedIPs on each interface covers all destinations
    reachable via that neighbor.  Exclusively-via-one-interface destinations get
    a direct ``ip route replace`` in PostUp; destinations with multiple equal-cost
    next-hops get a multi-nexthop ECMP route installed by a separate service.

    Port assignment
    ---------------
    Each undirected edge in the graph gets a unique UDP port starting at
    ``listen_port``.  Edges are sorted by ``(min(u,v), max(u,v))`` for a stable
    assignment, so both endpoints of an edge use the same port.

    Key generation
    --------------
    Each edge-endpoint gets its own keypair derived from
    ``edge_{min}_{max}_{node}`` so that per-interface keys are independent.
    """
    participants = sorted(
        nid for nid in (node_ids or list(graph.nodes())) if nid in host_public_ips
    )
    sub = graph.subgraph(participants)

    # Routed destination addresses (AllowedIPs / ip route targets).
    if route_address_map is None:
        route_map = {
            nid: f"{graph.nodes[nid]['data'].address}/32"
            for nid in participants
        }
    else:
        route_map = {
            nid: route_address_map.get(nid, graph.nodes[nid]["data"].address)
            for nid in participants
        }
        route_map = {
            nid: ip if "/" in ip else f"{ip}/32"
            for nid, ip in route_map.items()
        }

    # Interface addresses can differ from routed destination addresses to avoid
    # conflicts with existing host NIC IPs (for example EC2 private IPs).
    if interface_address_map is None:
        iface_map = route_map
    else:
        iface_map = {
            nid: interface_address_map.get(nid, route_map[nid])
            for nid in participants
        }
        iface_map = {
            nid: ip if "/" in ip else f"{ip}/32"
            for nid, ip in iface_map.items()
        }

    # One port per undirected edge, sorted for stable assignment.
    edges = sorted({(min(u, v), max(u, v)) for u, v in sub.edges()})
    edge_ports = {edge: listen_port + i for i, edge in enumerate(edges)}

    # One keypair per edge-endpoint: (edge, node) → (priv_b64, pub_b64).
    edge_keys: dict[tuple[str, str], dict[str, tuple[str, str]]] = {}
    for (u, v) in edges:
        edge_keys[(u, v)] = {
            u: _generate_keypair(f"edge_{u}_{v}_{u}", salt),
            v: _generate_keypair(f"edge_{u}_{v}_{v}", salt),
        }

    all_nexthops = _compute_all_nexthops(graph, participants)

    result: dict[str, dict] = {}
    for src in participants:
        routes = all_nexthops.get(src, {})
        direct_neighbors = sorted(sub.neighbors(src))

        interfaces = []
        for nbr in direct_neighbors:
            edge_key = (min(src, nbr), max(src, nbr))
            port = edge_ports[edge_key]
            iface_name = f"wg_{nbr}"

            nbr_route_ip = route_map[nbr].split("/")[0] + "/32"

            # AllowedIPs: neighbor's own WG IP + every dest where nbr is a valid nexthop.
            allowed: set[str] = {nbr_route_ip}
            for dest, nexthops in routes.items():
                if nbr in nexthops:
                    allowed.add(route_map[dest].split("/")[0] + "/32")

            # Direct routes for PostUp: neighbor's own IP + dests exclusively via this nbr.
            direct: list[str] = [nbr_route_ip]
            for dest, nexthops in sorted(routes.items()):
                if nexthops == [nbr]:
                    dest_ip = route_map[dest].split("/")[0] + "/32"
                    if dest_ip != nbr_route_ip:
                        direct.append(dest_ip)

            interfaces.append({
                "interface": iface_name,
                "listen_port": port,
                "private_key": edge_keys[edge_key][src][0],
                "address": iface_map[src],
                "speed": sub[src][nbr].get("speed"),
                "peers": [{
                    "public_key": edge_keys[edge_key][nbr][1],
                    "endpoint": f"{host_public_ips[nbr]}:{port}",
                    "allowed_ips": sorted(allowed),
                    "persistent_keepalive": 25,
                    "direct_routes": sorted(direct),
                }],
            })

        # ECMP routes: destinations reached via more than one equal-cost next-hop.
        ecmp_routes: list[str] = []
        for dest, nexthops in sorted(routes.items()):
            if len(nexthops) > 1:
                nexthop_args = " ".join(f"nexthop dev wg_{nh}" for nh in nexthops)
                dest_ip = route_map[dest].split("/")[0] + "/32"
                ecmp_routes.append(f"ip route replace {dest_ip} {nexthop_args}")

        result[src] = {
            "wireguard_interfaces": interfaces,
            "wireguard_ecmp_routes": ecmp_routes,
        }

    return result
