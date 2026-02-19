"""Generate the Ansible inventory YAML with embedded WireGuard config."""

from pathlib import Path
from typing import Optional

import networkx as nx
import yaml

from cli.wireguard import (
    DEFAULT_INTERFACE,
    DEFAULT_KEY_SALT,
    DEFAULT_LISTEN_PORT,
    DEFAULT_NETWORK_BASE,
    build_wireguard_config,
)


def _load_onprem(path: Optional[Path]) -> dict:
    """Return the on-prem host map from an existing inventory file, or {}."""
    if path and path.exists():
        with open(path) as f:
            doc = yaml.safe_load(f) or {}
        return (
            doc.get("all", {})
            .get("children", {})
            .get("onprem", {})
            .get("hosts", {})
        ) or {}
    return {}


def write_inventory(
    graph: nx.Graph,
    instance_map: dict[str, dict],
    ansible_user: str,
    ansible_key: Optional[str],
    output_path: Path,
    onprem_path: Optional[Path] = None,
    wg_interface: str = DEFAULT_INTERFACE,
    wg_listen_port: int = DEFAULT_LISTEN_PORT,
    wg_salt: str = DEFAULT_KEY_SALT,
    wg_network_base: str = DEFAULT_NETWORK_BASE,
) -> None:
    """Write the generated Ansible inventory to *output_path*.

    Cloud hosts are sourced from *instance_map* (keyed by node id, values
    contain ``public_ip`` / ``private_ip`` as returned by ``terraform output
    -json``).  On-prem hosts are copied verbatim from *onprem_path* so
    manually managed static inventory entries are preserved.

    WireGuard configuration is computed for every cloud host that has a
    reachable IP, using BFS-derived AllowedIPs so that multi-hop forwarding
    works correctly (see wireguard.py for details).
    """
    onprem_hosts = _load_onprem(onprem_path)

    # Build node_id → public IP for WireGuard config generation.
    host_ips: dict[str, str] = {}
    for node_id, meta in instance_map.items():
        ip = meta.get("public_ip") or meta.get("private_ip")
        if ip:
            host_ips[node_id] = ip

    wg_configs = build_wireguard_config(
        graph=graph,
        host_public_ips=host_ips,
        interface=wg_interface,
        listen_port=wg_listen_port,
        salt=wg_salt,
        network_base=wg_network_base,
    )

    cloud_hosts: dict = {}
    for node_id, meta in instance_map.items():
        ip = meta.get("public_ip") or meta.get("private_ip")
        if not ip:
            print(f"  [warn] {node_id} has no reachable IP, skipping inventory entry")
            continue

        entry: dict = {
            "ansible_host": ip,
            "ansible_user": ansible_user,
            "node_class": "cloud",
        }
        if ansible_key:
            entry["ansible_ssh_private_key_file"] = ansible_key
        if node_id in wg_configs:
            entry["wireguard"] = wg_configs[node_id]

        cloud_hosts[node_id] = entry

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
