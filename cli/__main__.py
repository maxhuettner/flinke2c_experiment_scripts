"""Network-sim experiment environment manager.

Usage (after `pip install -e .`):

    sim setup   -f config/topologies/1.json
    sim destroy -f config/topologies/1.json
    sim run     -f config/topologies/1.json -c "uname -a"
    sim run     -f config/topologies/1.json -c "ping -c3 10.10.10.3" -n a -n b

Environment variables (can be placed in .env):
    SSH_KEY_PATH            Private key for SSH/Ansible (default: ~/.ssh/id_ed25519)
    SSH_PUBLIC_KEY_PATH     Public key uploaded to AWS (derived from above if absent)
    SSH_USER                Username for the experiment 'run' command
    SSH_KEY_PASSPHRASE      Passphrase for the private key (optional)
    ANSIBLE_CLOUD_SSH_USER  Username Ansible uses on cloud nodes (default: ubuntu)
    ANSIBLE_SSH_KEY_PATH    Override SSH key path used by Ansible inventory
    AWS_REGION              Override the target AWS region
"""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

import click
import yaml
from dotenv import load_dotenv

from cli.inventory import write_inventory
from cli.terraform import build_tfvars, write_tfvars
from cli.topology import load_topology

# ---------------------------------------------------------------------------
# Fixed paths (relative to repo root — run the CLI from the repo root)
# ---------------------------------------------------------------------------
TERRAFORM_DIR = Path("resource_management/terraform")
TERRAFORM_TFVARS = TERRAFORM_DIR / "generated.auto.tfvars.json"
ANSIBLE_DIR = Path("exp_management/ansible")
ANSIBLE_INVENTORY = ANSIBLE_DIR / "inventory/generated_hosts.yml"
ANSIBLE_ONPREM = ANSIBLE_DIR / "inventory/onprem.yml"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _expand(path: str) -> str:
    return str(Path(path).expanduser())


def _read_if_exists(p: Path) -> Optional[str]:
    return p.read_text().strip() if p.exists() else None


def _pub_from_priv(priv: str) -> Path:
    p = Path(priv).expanduser()
    return p if p.suffix == ".pub" else Path(str(p) + ".pub")


def _load_ssh_public_key() -> Optional[str]:
    """Try several env-var conventions and fall back to ~/.ssh/id_ed25519.pub."""
    for var in ("SSH_PUBLIC_KEY_PATH", "ANSIBLE_SSH_PUBLIC_KEY_PATH"):
        if p_str := os.environ.get(var):
            if key := _read_if_exists(Path(p_str).expanduser()):
                return key

    for var in ("SSH_KEY_PATH", "ANSIBLE_SSH_KEY_PATH"):
        if priv := os.environ.get(var):
            if key := _read_if_exists(_pub_from_priv(priv)):
                return key

    return _read_if_exists(Path.home() / ".ssh" / "id_ed25519.pub")


def _load_ssh_key_path() -> Optional[str]:
    for var in ("ANSIBLE_SSH_KEY_PATH", "SSH_KEY_PATH"):
        p = os.environ.get(var)
        if p:
            return _expand(p)
    default = Path.home() / ".ssh" / "id_ed25519"
    return str(default) if default.exists() else None


def _run(cmd: list[str], cwd: Optional[Path] = None) -> None:
    result = subprocess.run(cmd, cwd=cwd)
    if result.returncode != 0:
        print(f"Command failed: {' '.join(str(c) for c in cmd)}", file=sys.stderr)
        sys.exit(result.returncode)


def _terraform(*args: str) -> None:
    _run(["terraform", *args])


def _fetch_terraform_outputs() -> dict:
    result = subprocess.run(
        ["terraform", f"-chdir={TERRAFORM_DIR}", "output", "-json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("terraform output -json failed", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def _prepare_tfvars(topology_file: str) -> None:
    """Write generated.auto.tfvars.json from the topology file."""
    graph = load_topology(topology_file)
    ssh_public_key = _load_ssh_public_key()
    aws_region = os.environ.get("AWS_REGION", "eu-central-1")
    tfvars = build_tfvars(graph, ssh_public_key=ssh_public_key, aws_region=aws_region)
    write_tfvars(TERRAFORM_TFVARS, tfvars)
    print(f"  wrote {TERRAFORM_TFVARS}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.group()
def main() -> None:
    """Network-sim experiment environment manager."""
    load_dotenv()


@main.command()
@click.option("-f", "--topology-file", required=True, help="Path to topology JSON")
def setup(topology_file: str) -> None:
    """Provision cloud infrastructure and configure all nodes."""
    graph = load_topology(topology_file)
    ssh_public_key = _load_ssh_public_key()
    aws_region = os.environ.get("AWS_REGION", "eu-central-1")

    # 1. Generate and write Terraform variables.
    tfvars = build_tfvars(graph, ssh_public_key=ssh_public_key, aws_region=aws_region)
    write_tfvars(TERRAFORM_TFVARS, tfvars)
    print(f"  wrote {TERRAFORM_TFVARS}")

    # 2. Provision EC2 instances.
    _terraform(f"-chdir={TERRAFORM_DIR}", "init")
    _terraform(
        f"-chdir={TERRAFORM_DIR}",
        "apply", "-auto-approve",
        f"-var-file={TERRAFORM_TFVARS.name}",
    )

    # 3. Fetch instance IPs from Terraform outputs.
    outputs = _fetch_terraform_outputs()
    instance_map: dict = outputs.get("instances", {}).get("value", {})

    # 4. Build Ansible inventory with WireGuard routing config.
    ansible_user = (
        os.environ.get("ANSIBLE_CLOUD_SSH_USER")
        or os.environ.get("CLOUD_SSH_USER")
        or "ubuntu"
    )
    ansible_key = _load_ssh_key_path()

    write_inventory(
        graph=graph,
        instance_map=instance_map,
        ansible_user=ansible_user,
        ansible_key=ansible_key,
        output_path=ANSIBLE_INVENTORY,
        onprem_path=ANSIBLE_ONPREM if ANSIBLE_ONPREM.exists() else None,
    )
    print(f"  wrote {ANSIBLE_INVENTORY}")

    # 5. Run Ansible to install packages, enable IP forwarding, and configure WireGuard.
    _run(["ansible-playbook", "playbooks/site.yml"], cwd=ANSIBLE_DIR)


@main.command()
@click.option("-f", "--topology-file", required=True, help="Path to topology JSON")
def destroy(topology_file: str) -> None:
    """Tear down all cloud infrastructure."""
    graph = load_topology(topology_file)
    ssh_public_key = _load_ssh_public_key()
    aws_region = os.environ.get("AWS_REGION", "eu-central-1")

    tfvars = build_tfvars(graph, ssh_public_key=ssh_public_key, aws_region=aws_region)
    write_tfvars(TERRAFORM_TFVARS, tfvars)
    print(f"  wrote {TERRAFORM_TFVARS}")

    _terraform(f"-chdir={TERRAFORM_DIR}", "init")
    _terraform(
        f"-chdir={TERRAFORM_DIR}",
        "destroy", "-auto-approve",
        f"-var-file={TERRAFORM_TFVARS.name}",
    )


@main.command("run")
@click.option("-f", "--topology-file", required=True, help="Path to topology JSON")
@click.option("-c", "--command", required=True, help="Shell command to run on nodes")
@click.option(
    "-n", "--node", "nodes",
    multiple=True,
    help="Limit to specific node ids (repeatable, default: all)",
)
def run_cmd(topology_file: str, command: str, nodes: tuple[str, ...]) -> None:
    """Run a shell command on all (or selected) nodes via SSH and stream output."""
    from cli.ssh import run_command

    if not ANSIBLE_INVENTORY.exists():
        print(
            f"Inventory not found at {ANSIBLE_INVENTORY}. Run 'setup' first.",
            file=sys.stderr,
        )
        sys.exit(1)

    with open(ANSIBLE_INVENTORY) as f:
        inv = yaml.safe_load(f) or {}

    all_hosts: dict = {}
    children = inv.get("all", {}).get("children", {})
    for group in children.values():
        all_hosts.update(group.get("hosts", {}))

    # SSH key and passphrase come from env; username is read per-host from the
    # inventory (ansible_user) so cloud nodes use "ubuntu" and on-prem nodes
    # can have their own user without touching .env.
    default_ssh_user = os.environ.get("SSH_USER", "ubuntu")
    ssh_key = _load_ssh_key_path()
    ssh_passphrase = os.environ.get("SSH_KEY_PASSPHRASE")

    if not ssh_key:
        print("No SSH key found. Set SSH_KEY_PATH in .env or environment.", file=sys.stderr)
        sys.exit(1)

    # Build per-node connection specs, optionally filtered by -n.
    targets: list[dict] = []
    for nid, vars_ in sorted(all_hosts.items()):
        if nodes and nid not in nodes:
            continue
        ip = vars_.get("ansible_host")
        if not ip:
            continue
        targets.append({
            "id": nid,
            "host": ip,
            "user": vars_.get("ansible_user", default_ssh_user),
        })

    if not targets:
        print("No matching hosts found.", file=sys.stderr)
        sys.exit(1)

    print(f"Running on: {', '.join(t['id'] for t in targets)}")

    # ANSI colours for per-node prefixes (imported lazily to keep startup fast)
    from cli.ssh import _colour, _RESET

    async def _run_all() -> dict[str, int]:
        tasks = [
            run_command(
                host=t["host"],
                command=command,
                username=t["user"],
                key_path=ssh_key,
                passphrase=ssh_passphrase,
                node_label=t["id"],
                colour=_colour(i),
            )
            for i, t in enumerate(targets)
        ]
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        return {
            t["id"]: (res if isinstance(res, int) else 1)
            for t, res in zip(targets, results_list)
        }

    results = asyncio.run(_run_all())

    failed = [n for n, rc in results.items() if rc != 0]
    if failed:
        print(f"\nFailed on: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
