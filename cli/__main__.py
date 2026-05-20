"""Network-sim experiment environment manager.

Usage (after `pip install -e .`):

    sim setup      -f config/topologies/1.json
    sim destroy    -f config/topologies/1.json
    sim run        -f config/topologies/1.json -c "uname -a"
    sim run        -f config/topologies/1.json -c "ping -c3 10.10.10.3" -n a -n b
    sim experiment -f config/topologies/edge-to-cloud.json \\
                   -e exp_management/experiments.yml \\
                   [-o results/] [--skip-data-upload]

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
from cli.terraform import build_tfvars_by_region, write_tfvars
from cli.topology import load_topology

# ---------------------------------------------------------------------------
# Fixed paths (relative to repo root — run the CLI from the repo root)
# ---------------------------------------------------------------------------
TERRAFORM_DIR = Path("resource_management/terraform")
TERRAFORM_REGION_ROOT = TERRAFORM_DIR / ".regions"
TERRAFORM_TFVARS_NAME = "generated.auto.tfvars.json"
TERRAFORM_MODULE_FILES = (
    "main.tf",
    "outputs.tf",
    "provider.tf",
    "variables.tf",
    "versions.tf",
)
ANSIBLE_DIR = Path("exp_management/ansible")
ANSIBLE_INVENTORY = ANSIBLE_DIR / "inventory/generated_hosts.yml"
ANSIBLE_ONPREM = ANSIBLE_DIR / "inventory/onprem.yml"
PROMETHEUS_TEMPLATE = Path("config/prometheus/prometheus.yml.j2")
PROMETHEUS_CONFIG = Path("config/prometheus/prometheus.yml")


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


def _terraform_workdir(region: str) -> Path:
    safe_region = region.replace("/", "_")
    return TERRAFORM_REGION_ROOT / safe_region


def _ensure_terraform_workdir(region: str) -> Path:
    workdir = _terraform_workdir(region)
    workdir.mkdir(parents=True, exist_ok=True)

    for filename in TERRAFORM_MODULE_FILES:
        source = TERRAFORM_DIR / filename
        target = workdir / filename
        contents = source.read_text()
        if not target.exists() or target.read_text() != contents:
            target.write_text(contents)

    return workdir


def _region_tfvars_path(region: str) -> Path:
    return _terraform_workdir(region) / TERRAFORM_TFVARS_NAME


def _has_terraform_state(workdir: Path) -> bool:
    return any(
        (workdir / filename).exists()
        for filename in ("terraform.tfstate", "terraform.tfstate.backup")
    )


def _existing_terraform_workdir(region: str, region_count: int) -> Path:
    regional = _terraform_workdir(region)
    if _has_terraform_state(regional):
        return regional
    if region_count == 1 and _has_terraform_state(TERRAFORM_DIR):
        return TERRAFORM_DIR
    return regional


def _terraform(workdir: Path, *args: str) -> None:
    _run(["terraform", f"-chdir={workdir}", *args])


def _fetch_terraform_outputs(workdir: Path) -> dict:
    result = subprocess.run(
        ["terraform", f"-chdir={workdir}", "output", "-json"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("terraform output -json failed", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def _prepare_tfvars_by_region(topology_file: str) -> dict[str, dict]:
    """Write per-region Terraform variable files from the topology file."""
    graph = load_topology(topology_file)
    ssh_public_key = _load_ssh_public_key()
    default_region = os.environ.get("AWS_REGION", "eu-central-1")
    try:
        tfvars_by_region = build_tfvars_by_region(
            graph,
            ssh_public_key=ssh_public_key,
            aws_region=default_region,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    for region, tfvars in tfvars_by_region.items():
        _ensure_terraform_workdir(region)
        path = _region_tfvars_path(region)
        write_tfvars(path, tfvars)
        print(f"  wrote {path}")

    return tfvars_by_region


def _has_cloud_instances(tfvars_by_region: dict[str, dict]) -> bool:
    """Return True when any region has EC2 instances to provision."""
    return any((tfvars or {}).get("ec2_instances") for tfvars in tfvars_by_region.values())


def _fetch_all_terraform_outputs(regions: list[str]) -> dict[str, dict]:
    instance_map: dict[str, dict] = {}
    region_count = len(regions)
    for region in regions:
        workdir = _existing_terraform_workdir(region, region_count)
        outputs = _fetch_terraform_outputs(workdir)
        for node_id, meta in outputs.get("instances", {}).get("value", {}).items():
            if node_id in instance_map:
                raise click.ClickException(
                    f"duplicate Terraform output for node {node_id!r} across regions"
                )
            instance_map[node_id] = meta
    return instance_map


def _write_prometheus_config(graph) -> None:
    """Render the local Prometheus config for capsys taskmanager scraping."""
    if not PROMETHEUS_TEMPLATE.exists():
        raise click.ClickException(
            f"Prometheus template not found at {PROMETHEUS_TEMPLATE}"
        )

    taskmanager_ips = [
        attrs["data"].address
        for _, attrs in graph.nodes(data=True)
        if attrs["data"].node_type.lower() == "compute"
    ]
    rendered_targets = "\n".join(
        f'          - "{ip}:9100"'
        for ip in taskmanager_ips
    ) or "          []"

    template = PROMETHEUS_TEMPLATE.read_text()
    placeholder = "{{ taskmanager_targets }}"
    if placeholder not in template:
        raise click.ClickException(
            f"Prometheus template at {PROMETHEUS_TEMPLATE} is missing {placeholder}"
        )

    PROMETHEUS_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    PROMETHEUS_CONFIG.write_text(template.replace(placeholder, rendered_targets))
    print(f"  wrote {PROMETHEUS_CONFIG}")


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
    default_region = os.environ.get("AWS_REGION", "eu-central-1")

    # 1. Generate and write Terraform variables.
    tfvars_by_region = _prepare_tfvars_by_region(topology_file)

    # 2. Provision EC2 instances when the topology includes cloud nodes.
    instance_map: dict = {}
    if _has_cloud_instances(tfvars_by_region):
        for region in sorted(tfvars_by_region):
            workdir = _ensure_terraform_workdir(region)
            _terraform(workdir, "init")
            _terraform(
                workdir,
                "apply",
                "-auto-approve",
                f"-var-file={TERRAFORM_TFVARS_NAME}",
            )

        # 3. Fetch instance IPs from Terraform outputs.
        instance_map = _fetch_all_terraform_outputs(sorted(tfvars_by_region))
    else:
        print("  no cloud nodes to provision; skipping Terraform apply")

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
        default_cloud_region=default_region,
    )
    print(f"  wrote {ANSIBLE_INVENTORY}")
    _write_prometheus_config(graph)

    # 5. Run Ansible to install packages, enable IP forwarding, and configure WireGuard.
    _run(["ansible-playbook", "playbooks/site.yml"], cwd=ANSIBLE_DIR)

    # 6. Reboot only AWS/cloud nodes after setup so cloud-side networking and
    # kernel settings come up cleanly without touching on-prem hosts.
    if instance_map:
        _run(["ansible-playbook", "playbooks/reboot-cloud.yml"], cwd=ANSIBLE_DIR)


@main.command("gen-inventory")
@click.option("-f", "--topology-file", required=True, help="Path to topology JSON")
def gen_inventory(topology_file: str) -> None:
    """Regenerate the Ansible inventory from the topology and current Terraform outputs.

    Use this after changing the topology file or onprem.yml without needing to
    re-provision cloud infrastructure.  Requires Terraform state to be present
    (i.e. 'setup' must have been run at least once).
    """
    graph = load_topology(topology_file)
    default_region = os.environ.get("AWS_REGION", "eu-central-1")
    try:
        tfvars_by_region = build_tfvars_by_region(
            graph,
            ssh_public_key=None,
            aws_region=default_region,
        )
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    instance_map: dict = {}
    if _has_cloud_instances(tfvars_by_region):
        instance_map = _fetch_all_terraform_outputs(sorted(tfvars_by_region))

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
        default_cloud_region=default_region,
    )
    print(f"  wrote {ANSIBLE_INVENTORY}")
    _write_prometheus_config(graph)


@main.command()
@click.option("-f", "--topology-file", required=True, help="Path to topology JSON")
def destroy(topology_file: str) -> None:
    """Remove on-prem WireGuard state and tear down cloud infrastructure."""
    graph = load_topology(topology_file)
    default_region = os.environ.get("AWS_REGION", "eu-central-1")
    tfvars_by_region = _prepare_tfvars_by_region(topology_file)

    instance_map: dict = {}
    if _has_cloud_instances(tfvars_by_region):
        try:
            instance_map = _fetch_all_terraform_outputs(sorted(tfvars_by_region))
        except SystemExit:
            print("  warning: could not fetch Terraform outputs; continuing with on-prem cleanup only")
            instance_map = {}

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
        default_cloud_region=default_region,
    )
    print(f"  wrote {ANSIBLE_INVENTORY}")

    _run(["ansible-playbook", "playbooks/wireguard-cleanup.yml"], cwd=ANSIBLE_DIR)

    if not _has_cloud_instances(tfvars_by_region):
        print("  no cloud nodes in topology; skipping Terraform destroy")
        return

    region_count = len(tfvars_by_region)
    for region in sorted(tfvars_by_region):
        workdir = _existing_terraform_workdir(region, region_count)
        if workdir == TERRAFORM_DIR:
            write_tfvars(TERRAFORM_DIR / TERRAFORM_TFVARS_NAME, tfvars_by_region[region])
            print(f"  wrote {TERRAFORM_DIR / TERRAFORM_TFVARS_NAME}")
        else:
            workdir = _ensure_terraform_workdir(region)
        _terraform(workdir, "init")
        _terraform(
            workdir,
            "destroy",
            "-auto-approve",
            f"-var-file={TERRAFORM_TFVARS_NAME}",
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
        out = {}
        for t, res in zip(targets, results_list):
            if isinstance(res, int):
                out[t["id"]] = res
            else:
                print(f"[{t['id']}] unhandled error: {res!r}", file=sys.stderr)
                out[t["id"]] = 1
        return out

    results = asyncio.run(_run_all())

    failed = [n for n, rc in results.items() if rc != 0]
    if failed:
        print(f"\nFailed on: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)


@main.command("experiment")
@click.option("-f", "--topology-file", required=True, help="Path to topology JSON")
@click.option(
    "-e", "--experiments-file", required=True,
    help="Path to experiments YAML (see exp_management/experiments.example.yml)",
)
@click.option(
    "-o", "--output-dir", default="results", show_default=True,
    help="Local directory where downloaded logs are saved",
)
@click.option(
    "--skip-data-upload", is_flag=True,
    help="Skip uploading source data files (already present on remote node)",
)
@click.option(
    "--start-with-rep",
    type=click.IntRange(min=1),
    default=None,
    help="Optional repetition number forwarded to source/sink containers",
)
@click.option(
    "--latency",
    is_flag=True,
    help="Forward --latency to source/sink containers",
)
def experiment_cmd(
    topology_file: str,
    experiments_file: str,
    output_dir: str,
    skip_data_upload: bool,
    start_with_rep: int | None,
    latency: bool,
) -> None:
    """Run a batch of streaming experiments on the provisioned nodes.

    Reads the experiment list from EXPERIMENTS_FILE and for each experiment:
    starts TCP sources + sink and a Flink cluster on the topology's source node,
    runs the SQL query for the requested number of repetitions, then downloads
    the logs to OUTPUT_DIR/<experiment-name>/.

    Source data files in exp_management/source_data/ are uploaded to the remote
    ~/data/ directory before the first experiment (pass --skip-data-upload if
    they are already present).
    """
    from cli.experiment import load_experiments, run_experiments

    ssh_key = _load_ssh_key_path()
    if not ssh_key:
        raise click.ClickException(
            "No SSH key found. Set SSH_KEY_PATH (or ANSIBLE_SSH_KEY_PATH) in .env."
        )
    passphrase = os.environ.get("SSH_KEY_PASSPHRASE")

    try:
        experiments = load_experiments(experiments_file)
    except (FileNotFoundError, KeyError, yaml.YAMLError) as exc:
        raise click.ClickException(f"Failed to load experiments file: {exc}") from exc

    if not experiments:
        raise click.ClickException("No experiments defined in the experiments file.")

    print(f"Loaded {len(experiments)} experiment(s) from {experiments_file}")

    try:
        asyncio.run(
            run_experiments(
                experiments=experiments,
                topology_file=topology_file,
                output_base=Path(output_dir),
                key_path=ssh_key,
                passphrase=passphrase,
                skip_data_upload=skip_data_upload,
                start_with_rep=start_with_rep,
                latency=latency,
            )
        )
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc


@main.command("profile")
@click.option("-f", "--topology-file", required=True, help="Path to topology JSON")
@click.option(
    "-e", "--experiments-file", required=True,
    help="Path to experiments YAML used to choose the Flink queries to profile",
)
@click.option(
    "--skip-data-upload", is_flag=True,
    help="Skip uploading source data files (already present on remote node)",
)
@click.option(
    "--latency",
    is_flag=True,
    help="Forward --latency to source/sink containers",
)
def profile_cmd(
    topology_file: str,
    experiments_file: str,
    skip_data_upload: bool,
    latency: bool,
) -> None:
    """Generate CAPSys schedulercfg files for the Flink queries in EXPERIMENTS_FILE."""
    from cli.experiment import load_experiments, run_profiles

    ssh_key = _load_ssh_key_path()
    if not ssh_key:
        raise click.ClickException(
            "No SSH key found. Set SSH_KEY_PATH (or ANSIBLE_SSH_KEY_PATH) in .env."
        )
    passphrase = os.environ.get("SSH_KEY_PASSPHRASE")

    graph = load_topology(topology_file)
    _write_prometheus_config(graph)

    try:
        experiments = load_experiments(experiments_file)
    except (FileNotFoundError, KeyError, yaml.YAMLError) as exc:
        raise click.ClickException(f"Failed to load experiments file: {exc}") from exc

    if not experiments:
        raise click.ClickException("No experiments defined in the experiments file.")

    print(f"Loaded {len(experiments)} experiment(s) from {experiments_file}")

    try:
        asyncio.run(
            run_profiles(
                experiments=experiments,
                topology_file=topology_file,
                key_path=ssh_key,
                passphrase=passphrase,
                skip_data_upload=skip_data_upload,
                latency=latency,
            )
        )
    except RuntimeError as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
