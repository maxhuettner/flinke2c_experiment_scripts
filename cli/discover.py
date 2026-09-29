"""Probe live node-to-node connectivity to derive a topology's edges."""

import asyncio
import json
import sys
from typing import Optional

import asyncssh

_PROBE_TEMPLATE = """python3 - <<'PY'
import json, socket
targets = {targets}
port = {port}
timeout = {timeout}
reachable = []
for node_id, ip in targets.items():
    try:
        with socket.create_connection((ip, port), timeout=timeout):
            reachable.append(node_id)
    except OSError:
        pass
print(json.dumps(reachable))
PY"""


def _conn_kwargs(host: str, username: str, key_path: str, passphrase: Optional[str], port: int) -> dict:
    kwargs: dict = {
        "host": host,
        "port": port,
        "username": username,
        "client_keys": [key_path],
        "known_hosts": None,
        "config": [],
        "agent_path": None,
        "preferred_auth": ["publickey"],
        "public_key_auth": True,
        "kbdint_auth": False,
        "password_auth": False,
        "gss_kex": False,
        "gss_auth": False,
        "connect_timeout": 10,
        "login_timeout": 15,
    }
    if passphrase:
        kwargs["passphrase"] = passphrase
    return kwargs


async def _probe_from_node(
    node_id: str,
    host: str,
    username: str,
    key_path: str,
    passphrase: Optional[str],
    ssh_port: int,
    targets: dict[str, str],
    probe_port: int,
    timeout: int,
) -> tuple[str, list[str]]:
    probe_targets = {tid: ip for tid, ip in targets.items() if tid != node_id}
    if not probe_targets:
        return node_id, []

    command = _PROBE_TEMPLATE.format(
        targets=json.dumps(probe_targets), port=probe_port, timeout=timeout,
    )
    try:
        async with asyncssh.connect(**_conn_kwargs(host, username, key_path, passphrase, ssh_port)) as conn:
            result = await conn.run(command, check=False)
            if result.exit_status != 0:
                print(f"[{node_id}] probe failed: {result.stderr}", file=sys.stderr)
                return node_id, []
            return node_id, json.loads((result.stdout or "[]").strip())
    except (asyncssh.Error, OSError, json.JSONDecodeError) as exc:
        print(f"[{node_id}] probe error: {exc}", file=sys.stderr)
        return node_id, []


_DIRECT_PROBE_TEMPLATE = """python3 - <<'PY'
import json, subprocess

def one_hop_away(addr, timeout):
    # TTL=1: only a direct neighbor can reply before a router drops it.
    try:
        result = subprocess.run(
            ["ping", "-c", "1", "-W", str(timeout), "-t", "1", addr],
            capture_output=True, text=True, timeout=timeout + 2,
        )
        return result.returncode == 0
    except Exception:
        return False

targets = {targets}
timeout = {timeout}
direct = [nid for nid, addr in targets.items() if one_hop_away(addr, timeout)]
print(json.dumps(sorted(direct)))
PY"""


async def _direct_probe_from_node(
    node_id: str,
    host: str,
    username: str,
    key_path: str,
    passphrase: Optional[str],
    ssh_port: int,
    targets: dict[str, str],
    timeout: int,
) -> tuple[str, list[str]]:
    probe_targets = {tid: addr for tid, addr in targets.items() if tid != node_id}
    if not probe_targets:
        return node_id, []

    command = _DIRECT_PROBE_TEMPLATE.format(targets=json.dumps(probe_targets), timeout=timeout)
    try:
        async with asyncssh.connect(**_conn_kwargs(host, username, key_path, passphrase, ssh_port)) as conn:
            result = await conn.run(command, check=False)
            if result.exit_status != 0:
                print(f"[{node_id}] direct-neighbor probe failed: {result.stderr}", file=sys.stderr)
                return node_id, []
            return node_id, json.loads((result.stdout or "[]").strip())
    except (asyncssh.Error, OSError, json.JSONDecodeError) as exc:
        print(f"[{node_id}] direct-neighbor probe error: {exc}", file=sys.stderr)
        return node_id, []


async def discover_direct_edges(
    hosts: dict[str, dict],      # node_id -> {"host": ssh_ip, "user": user}
    addresses: dict[str, str],   # node_id -> overlay/topology address (e.g. the WAN 10.x address)
    key_path: str,
    passphrase: Optional[str] = None,
    ssh_port: int = 22,
    timeout: int = 3,
) -> list[tuple[str, str]]:
    """Return edges between single-hop node pairs only, via TTL=1 ping."""
    probe_nodes = sorted(set(hosts) & set(addresses))
    tasks = [
        _direct_probe_from_node(
            nid, hosts[nid]["host"], hosts[nid]["user"], key_path, passphrase, ssh_port,
            addresses, timeout,
        )
        for nid in probe_nodes
    ]
    results = await asyncio.gather(*tasks)
    direct_from = dict(results)

    edges: set[tuple[str, str]] = set()
    for source, neighbors in direct_from.items():
        for neighbor in neighbors:
            edges.add(tuple(sorted((source, neighbor))))
    return sorted(edges)


async def discover_edges(
    hosts: dict[str, dict],  # node_id -> {"host": ip, "user": user}
    key_path: str,
    passphrase: Optional[str] = None,
    ssh_port: int = 22,
    probe_port: int = 22,
    timeout: int = 3,
) -> list[tuple[str, str]]:
    """Return edges for every node pair reachable by TCP, in either direction."""
    targets = {nid: info["host"] for nid, info in hosts.items()}
    tasks = [
        _probe_from_node(
            nid, info["host"], info["user"], key_path, passphrase, ssh_port,
            targets, probe_port, timeout,
        )
        for nid, info in hosts.items()
    ]
    results = await asyncio.gather(*tasks)
    reachable_from = dict(results)

    edges: set[tuple[str, str]] = set()
    for source, targets_reached in reachable_from.items():
        for target in targets_reached:
            edges.add(tuple(sorted((source, target))))
    return sorted(edges)
