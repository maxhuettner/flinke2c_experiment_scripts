"""Flink experiment runner for the network-sim framework.

Experiment YAML format (e.g. exp_management/experiments.yml):

    repetitions: 3   # optional global default for all experiments
    experiments:
      - name: q1_local
        system: flink        # currently the only supported system
        query: q1            # resolves to exp_management/queries/flink/q1.sql
        # repetitions omitted -> uses global default (3)

      - name: q4_cluster
        system: flink
        query: q4
        repetitions: 2       # optional per-experiment override
        placement_method: cluster   # generates topology.graphml for Flink

Per-query extra args and task slot counts are read from
exp_management/configs/flink/query_config.yml and can be overridden with
`num_task_slots` in the experiment YAML.

`source_schema: nes` (system: flink only) tells the TCP source generator to
emit NES's trimmed field set instead of the full schema, and loads
setup_nes.sql instead of setup.sql. Pair it with a `_nes`-suffixed query file
(e.g. query: q1_nes) that only references fields NES also has, for a
byte-for-byte-comparable flink-vs-nes run.

Usage:
    sim experiment \\
        -f config/topologies/edge-to-cloud.json \\
        -e exp_management/experiments.yml \\
        [-o results/] \\
        [--skip-data-upload] \\
        [--start-with-rep N] \\
        [--no-latency]
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json as _json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import asyncssh
import networkx as nx
import yaml

from cli.ssh import _expand
from cli.topology import TopoNode, load_topology

# ── Fixed paths ───────────────────────────────────────────────────────────────

QUERIES_DIR       = Path("exp_management/queries/flink")
CONFIGS_DIR       = Path("exp_management/configs/flink")
QUERY_CONFIG_FILE = CONFIGS_DIR / "query_config.yml"
SOURCE_DATA_DIR   = Path("exp_management/source_data")
FLINK_LIB_DIR     = Path("lib")
ANSIBLE_INVENTORY = Path("exp_management/ansible/inventory/generated_hosts.yml")
SCRIPTS_DIR       = Path("scripts")
CAPSYS_DIR        = Path("capsys")
CSV_TO_PARQUET_SCRIPT = SCRIPTS_DIR / "convert_csv_to_parquet.py"
DOWNSAMPLE_LATENCY_SCRIPT = SCRIPTS_DIR / "downsample_latency_parquet.py"
REMOTE_LATENCY_PYTHON_PACKAGES = ("numpy", "polars")
CAPSYS_GENERATOR_SCRIPT = CAPSYS_DIR / "generate_local_sql_config.py"
CAPSYS_RUNDS_SCRIPT = CAPSYS_DIR / "runds2placement.py"
CAPSYS_CONFIG_TEMPLATE = CAPSYS_DIR / "examples/runds2placement-local.example.json"
CAPSYS_CONFIG_OUTPUT = CAPSYS_DIR / "expjson/local_sql.json"
PROMETHEUS_CONFIG_FILE = Path("config/prometheus/prometheus.yml")

FLINK_IMAGE = "maxhue/flinke2c:latest"
TCP_IMAGE   = "maxhue/tcp-streaming"
PROMETHEUS_IMAGE = "prom/prometheus:latest"

NES_QUERIES_DIR       = Path("exp_management/queries/nes")
NES_COORDINATOR_IMAGE = "maxhue/nes-coordinator"
NES_WORKER_IMAGE      = "maxhue/nes-worker"
NES_REST_PORT         = 8081
NES_CLUSTER_READY_TIMEOUT = 120  # seconds to wait for NES topology workers
NES_QUERY_PLACEMENT_TIMEOUT = 60  # seconds to wait for a submitted query to leave REGISTERED/OPTIMIZING

READY_SIGNAL = "Reading & binary encoding done"
DONE_SIGNAL  = "All connections closed, stopping logger"

POLL_INTERVAL  = 3     # seconds between docker-logs polls
READY_TIMEOUT  = 120   # seconds to wait for source ready
DONE_TIMEOUT   = 1800  # seconds to wait for experiment completion
FLINK_CLUSTER_READY_TIMEOUT = 180  # seconds to wait for Flink TMs in REST
FLINK_JOBMANAGER_START_DELAY = 5  # seconds to wait before starting taskmanagers
SSH_CONNECT_RETRIES = 3
SSH_CONNECT_RETRY_DELAY = 5
FLINK_EXPERIMENT_MAX_RETRIES = int(os.getenv("SIM_FLINK_EXPERIMENT_MAX_RETRIES", "2"))
TRACE_REMOTE_TIMINGS = (
    os.getenv("SIM_TRACE_REMOTE_TIMINGS", "").strip().lower() in ("1", "true", "yes")
)
CAPSYS_SOURCE_RATE = 5000


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ExperimentSpec:
    name: str
    system: str = "flink"
    query: str = ""
    repetitions: int = 1
    placement_method: str = ""      # "" (default) or e.g. "TOP_DOWN"; for system: flink
                                     # enables topology-aware placement (graphml), for
                                     # system: nes selects the NES placement strategy
                                     # sent as "placement" on /execute-query (mapped via
                                     # NES_PLACEMENT_STRATEGY_MAP; "" -> NES's own TopDown default)
    num_task_slots: Optional[int] = None
    bid_src_extra_arg: Optional[str] = None
    max_query_runtime: Optional[int] = None  # milliseconds; cancel Flink job via REST when exceeded
    graphml_file: str = ""          # explicit graphml filename in coordinator dir, e.g. "cloud.graphml"
    source_schema: str = ""         # "" (full schema) or "nes" (trimmed schema + tells the TCP source
                                     # generator to emit the NES-compatible field set, for fair flink-vs-nes runs)
    nes_num_slots: Optional[int] = None  # numberOfSlots for NES workers; None = unlimited
    nes_coordinator_num_slots: int = 1   # numberOfSlots for the NES coordinator's own worker role
    flink_rocksdb: Optional[bool] = None  # state.backend: rocksdb (system: flink only).
                                     # None = on (current default). Off relies purely on the
                                     # instance-type-sized heap (see _flink_process_memory_mb)
                                     # to avoid the GC-death-spiral/heartbeat-timeout failure
                                     # RocksDB was added to work around for q4's 24h window state.
    nes_restart_after_stop: Optional[bool] = None  # system: nes only. None/False = current
                                     # behavior (one coordinator+workers for all repetitions).
                                     # True: after each repetition (except the last), stop and
                                     # restart the NES coordinator and worker containers before
                                     # submitting the next one. Works around repetition 2+
                                     # consistently hanging in OPTIMIZING even after repetition
                                     # 1's query stops - the coordinator's topology resource
                                     # slots occupied by a stopped query don't appear to be
                                     # released, so a long-lived coordinator effectively loses
                                     # capacity every repetition. A fresh coordinator process has
                                     # a fresh in-memory topology with all slots free.


@dataclass
class NodeInfo:
    id: str
    host: str       # SSH-accessible IP
    user: str
    node_type: str  # "source" / "sink" / "compute"
    address: str    # overlay/topology address
    speed: Optional[int] = None  # CPU cap in % (e.g. 50 → --cpus 0.50)
    instance_type: str = "t3.micro"  # cloud instance type, from topology (for memory sizing)


# Total RAM (MiB) for the cloud instance types used in config/topologies/*.json.
# Source: AWS EC2 published instance specs. Extend as new types show up there.
AWS_INSTANCE_MEMORY_MB: dict[str, int] = {
    "t3.micro": 1024,
    "t4g.medium": 4096,
    "m6g.medium": 4096,
    "c6g.medium": 2048,
    "c6g.large": 4096,
}

# Used to derive NES's buffer-pool/hash-table sizing (_nes_memory_budget_mb)
# from the same per-node budget Flink's TaskManager uses, instead of NES's
# old fixed sizing that ignored node/instance_type entirely.
NES_PROCESS_OVERHEAD_MB = 256
NES_MIN_AVAILABLE_MB = 128
NES_BUFFER_SIZE_BYTES = 262144


def _flink_process_memory_mb(instance_type: str, fraction: float, fallback_mb: int) -> int:
    """Flink process memory (MiB): a fraction of total RAM, rest left for the
    OS/Docker. Falls back to fallback_mb for an unlisted instance type."""
    total = AWS_INSTANCE_MEMORY_MB.get(instance_type)
    if total is None:
        return fallback_mb
    return int(total * fraction)


def _worker_memory_mb(instance_type: str) -> int:
    """Memory budget (MiB) for a dedicated worker node running one
    TaskManager/NES-worker process. Single source of truth: used as Flink's
    process.size, as the docker --memory cap on both engines
    (_worker_memory_flag), and to derive NES's buffer-pool/hash-table sizing
    (_nes_memory_budget_mb), so both engines share the same per-node cap."""
    return _flink_process_memory_mb(instance_type, fraction=0.85, fallback_mb=2048)


def _nes_memory_budget_mb(instance_type: str) -> tuple[int, int]:
    """(global_buffer_pool_mb, max_hash_table_mb), sized off the same
    per-node budget as Flink's TaskManager (_worker_memory_mb), replacing
    NES's old fixed 1024m/2048m ceiling that ignored node size. Reserves
    NES_PROCESS_OVERHEAD_MB for the native process, splits the rest 1:2
    (same ratio as the old defaults)."""
    total = _worker_memory_mb(instance_type)
    available = max(total - NES_PROCESS_OVERHEAD_MB, NES_MIN_AVAILABLE_MB)
    buffer_pool_mb = max(available // 3, 1)
    max_hash_table_mb = available - buffer_pool_mb
    return buffer_pool_mb, max_hash_table_mb


class FlinkJobRetryableError(RuntimeError):
    """A Flink job entered a retry-worthy failure state."""


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_experiments(path: str) -> list[ExperimentSpec]:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    global_repetitions = int(raw.get("repetitions", 1))
    global_num_task_slots = raw.get("num_task_slots")
    global_bid_src_extra_arg = raw.get("bid_src_extra_arg")
    global_max_query_runtime = raw.get("max_query_runtime")
    global_nes_num_slots = raw.get("nes_num_slots")
    global_nes_coordinator_num_slots = raw.get("nes_coordinator_num_slots", 1)
    global_flink_rocksdb = raw.get("flink_rocksdb")
    global_nes_restart_after_stop = raw.get("nes_restart_after_stop")
    return [
        ExperimentSpec(
            name=e["name"],
            system=e.get("system", "flink"),
            query=e["query"],
            repetitions=int(e.get("repetitions", global_repetitions)),
            placement_method=e.get("placement_method", ""),
            num_task_slots=e.get("num_task_slots", global_num_task_slots),
            bid_src_extra_arg=e.get("bid_src_extra_arg", global_bid_src_extra_arg),
            max_query_runtime=e.get("max_query_runtime", global_max_query_runtime),
            graphml_file=e.get("graphml_file", ""),
            source_schema=e.get("source_schema", ""),
            nes_num_slots=e.get("nes_num_slots", global_nes_num_slots),
            nes_coordinator_num_slots=int(
                e.get("nes_coordinator_num_slots", global_nes_coordinator_num_slots)
            ),
            flink_rocksdb=e.get("flink_rocksdb", global_flink_rocksdb),
            nes_restart_after_stop=e.get("nes_restart_after_stop", global_nes_restart_after_stop),
        )
        for e in raw.get("experiments", [])
    ]


def load_query_config() -> dict:
    if QUERY_CONFIG_FILE.exists():
        with open(QUERY_CONFIG_FILE) as f:
            return yaml.safe_load(f) or {}
    return {}


def select_profile_experiments(experiments: list[ExperimentSpec]) -> list[ExperimentSpec]:
    """Return one Flink experiment per query for CAPSys profiling."""
    selected: dict[str, ExperimentSpec] = {}

    for exp in experiments:
        if exp.system.lower() != "flink":
            continue

        current = selected.get(exp.query)
        if current is None:
            selected[exp.query] = exp
            continue

        same_profile_shape = current.num_task_slots == exp.num_task_slots
        if not same_profile_shape:
            raise RuntimeError(
                "CAPSys profile input is ambiguous for query "
                f"{exp.query!r}: multiple Flink experiments define different "
                "task-slot settings for the same query."
            )

    return list(selected.values())


def load_nodes(topology_file: str) -> dict[str, NodeInfo]:
    """Merge topology node metadata with inventory SSH details."""
    graph = load_topology(topology_file)

    if not ANSIBLE_INVENTORY.exists():
        raise RuntimeError(
            f"Inventory not found at {ANSIBLE_INVENTORY}. Run 'sim setup' first."
        )

    with open(ANSIBLE_INVENTORY) as f:
        inv = yaml.safe_load(f) or {}

    all_hosts: dict = {}
    for group in inv.get("all", {}).get("children", {}).values():
        all_hosts.update(group.get("hosts", {}) or {})

    nodes: dict[str, NodeInfo] = {}
    for nid, attrs in graph.nodes(data=True):
        topo: TopoNode = attrs["data"]
        iv = all_hosts.get(nid, {})

        # On-prem topology ids (e.g. N3/src) map to inventory hostnames via
        # the topology's "on-prem-id" field (e.g. zs01/zs04).
        if not iv and topo.is_on_prem():
            onprem_id = (
                topo.extra.get("on-prem-id")
                or topo.extra.get("on_prem_id")
            )
            if onprem_id:
                iv = all_hosts.get(str(onprem_id), {})

        nodes[nid] = NodeInfo(
            id=nid,
            host=iv.get("ansible_host", topo.address),
            user=iv.get("ansible_user", "ubuntu"),
            node_type=topo.node_type.lower(),
            address=topo.address,
            speed=topo.speed,
            instance_type=topo.instance_type,
        )
    return nodes


# ── SSH / SFTP helpers ────────────────────────────────────────────────────────

def _conn_kwargs(node: NodeInfo, key_path: str, passphrase: Optional[str]) -> dict:
    kw: dict = {
        "host": node.host,
        "username": node.user,
        "client_keys": [_expand(key_path)],
        "known_hosts": None,
        "config": [],
        "keepalive_interval": 30,
        # Match fast manual SSH behavior: use only the configured private key
        # and skip slower auth fallbacks.
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
        kw["passphrase"] = passphrase
    return kw


async def _connect_node(
    node: NodeInfo,
    key_path: str,
    passphrase: Optional[str],
    *,
    retries: int = SSH_CONNECT_RETRIES,
    retry_delay: int = SSH_CONNECT_RETRY_DELAY,
) -> asyncssh.SSHClientConnection:
    """Connect to a node with retries and clear diagnostics."""
    last_exc: Optional[BaseException] = None

    for attempt in range(1, retries + 1):
        try:
            return await asyncssh.connect(**_conn_kwargs(node, key_path, passphrase))
        except (asyncssh.Error, OSError, TimeoutError) as exc:
            last_exc = exc
            if attempt == retries:
                break
            print(
                f"  SSH connect failed for {node.id} ({node.host}) "
                f"[attempt {attempt}/{retries}]: {exc}. Retrying in {retry_delay}s..."
            )
            await asyncio.sleep(retry_delay)

    raise RuntimeError(
        f"Failed to connect to node {node.id} ({node.host}) after {retries} attempts: "
        f"{last_exc}"
    )


async def _run(conn: asyncssh.SSHClientConnection, cmd: str, *, check: bool = True) -> str:
    started = time.monotonic()
    result = await conn.run(cmd, check=False)
    if TRACE_REMOTE_TIMINGS:
        elapsed = time.monotonic() - started
        peer = conn.get_extra_info("peername")
        host = peer[0] if isinstance(peer, tuple) and peer else "unknown"
        short_cmd = " ".join(cmd.split())
        if len(short_cmd) > 140:
            short_cmd = short_cmd[:137] + "..."
        stderr_snip = ""
        if result.stderr:
            err = " ".join(result.stderr.strip().split())
            if len(err) > 100:
                err = err[:97] + "..."
            stderr_snip = f" stderr='{err}'"
        print(
            f"  [timing host={host} status={result.exit_status} t={elapsed:.2f}s{stderr_snip}] "
            f"{short_cmd}"
        )
    if check and result.exit_status != 0:
        raise RuntimeError(
            f"Remote command failed (exit {result.exit_status}): {cmd!r}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}"
        )
    return (result.stdout or "").strip()


async def _get_home(conn: asyncssh.SSHClientConnection) -> str:
    return await _run(conn, "echo $HOME")


async def _upload_text(
    conn: asyncssh.SSHClientConnection, content: str, remote_path: str
) -> None:
    async with conn.start_sftp_client() as sftp:
        async with await sftp.open(remote_path, "w") as fh:
            await fh.write(content)


async def _upload_local_text_file(
    conn: asyncssh.SSHClientConnection, local_path: Path, remote_path: str
) -> None:
    await _upload_text(conn, local_path.read_text(), remote_path)


def _print_remote_output(prefix: str, output: str, *, stream: Any = sys.stdout) -> None:
    for line in output.splitlines():
        line = line.strip()
        if line:
            print(f"  [{prefix}] {line}", file=stream)


async def _ensure_remote_latency_python_environment(
    conn: asyncssh.SSHClientConnection,
    *,
    node_label: str,
    node_home: str,
) -> Optional[str]:
    """Ensure the remote node can run the latency preprocessing scripts."""
    remote_tools_dir = f"{node_home}/sim-tools"
    await _ensure_remote_dir(conn, remote_tools_dir)
    venv_dir = f"{remote_tools_dir}/latency-venv"
    venv_python = f"{venv_dir}/bin/python"
    module_list = ", ".join(REMOTE_LATENCY_PYTHON_PACKAGES)
    check_cmd = lambda python_exec: (
        shlex.quote(python_exec) + " -c " + shlex.quote(f"import {module_list}")
    )
    create_venv_cmd = f"rm -rf {shlex.quote(venv_dir)} && python3 -m venv {shlex.quote(venv_dir)}"

    check_result = await conn.run(check_cmd("python3"), check=False)
    if check_result.exit_status == 0:
        return "python3"

    venv_check_result = await conn.run(check_cmd(venv_python), check=False)
    if venv_check_result.exit_status == 0:
        return venv_python

    venv_support_check = await conn.run("python3 -m venv --help", check=False)
    if venv_support_check.exit_status != 0:
        print(f"  [{node_label}/latency-python] Installing Python venv packages...")
        venv_install = await conn.run(
            (
                "sudo -n apt-get update && "
                "sudo -n apt-get install -y python3-venv python3-full"
            ),
            check=False,
        )
        if venv_install.stdout:
            _print_remote_output(f"{node_label}/latency-python", venv_install.stdout)
        if venv_install.exit_status != 0:
            _print_remote_output(
                f"{node_label}/latency-python",
                venv_install.stderr or "python3-venv installation failed",
                stream=sys.stderr,
            )
            return None

    print(
        f"  [{node_label}/latency-python] Creating latency virtualenv and installing: "
        f"{', '.join(REMOTE_LATENCY_PYTHON_PACKAGES)}"
    )
    create_venv = await conn.run(create_venv_cmd, check=False)
    if create_venv.exit_status != 0:
        print(f"  [{node_label}/latency-python] Venv creation failed; installing additional Python venv packages...")
        repair_install = await conn.run(
            (
                "sudo -n apt-get update && "
                "sudo -n apt-get install -y python3-venv python3-full"
            ),
            check=False,
        )
        if repair_install.stdout:
            _print_remote_output(f"{node_label}/latency-python", repair_install.stdout)
        if repair_install.exit_status != 0:
            _print_remote_output(
                f"{node_label}/latency-python",
                repair_install.stderr or "python venv package installation failed",
                stream=sys.stderr,
            )
            _print_remote_output(
                f"{node_label}/latency-python",
                create_venv.stderr or create_venv.stdout or "virtualenv creation failed",
                stream=sys.stderr,
            )
            return None

        create_venv = await conn.run(create_venv_cmd, check=False)
        if create_venv.exit_status != 0:
            _print_remote_output(
                f"{node_label}/latency-python",
                create_venv.stderr or create_venv.stdout or "virtualenv creation failed",
                stream=sys.stderr,
            )
            return None

    install_cmd = " ".join([
        shlex.quote(venv_python),
        "-m pip install --disable-pip-version-check",
        *REMOTE_LATENCY_PYTHON_PACKAGES,
    ])
    install_result = await conn.run(install_cmd, check=False)
    if install_result.stdout:
        _print_remote_output(f"{node_label}/latency-python", install_result.stdout)
    if install_result.exit_status != 0:
        _print_remote_output(
            f"{node_label}/latency-python",
            install_result.stderr or "package installation failed",
                stream=sys.stderr,
            )
        return None

    recheck_result = await conn.run(check_cmd(venv_python), check=False)
    if recheck_result.exit_status != 0:
        _print_remote_output(
            f"{node_label}/latency-python",
            recheck_result.stderr or "package import check failed after install",
            stream=sys.stderr,
        )
        return None
    return venv_python


async def _ensure_remote_path_writable(
    conn: asyncssh.SSHClientConnection,
    *,
    node_label: str,
    remote_path: str,
    remote_user: str,
) -> bool:
    """Ensure the remote path is writable by the SSH user."""
    quoted_path = shlex.quote(remote_path)
    quoted_user = shlex.quote(remote_user)
    chown_cmd = f"sudo -n chown -R {quoted_user}:{quoted_user} {quoted_path}"
    chmod_cmd = f"sudo -n chmod -R u+rwX {quoted_path}"

    chown_result = await conn.run(chown_cmd, check=False)
    if chown_result.exit_status != 0:
        _print_remote_output(
            f"{node_label}/latency-perms",
            chown_result.stderr or "chown failed",
            stream=sys.stderr,
        )
        return False

    chmod_result = await conn.run(chmod_cmd, check=False)
    if chmod_result.exit_status != 0:
        _print_remote_output(
            f"{node_label}/latency-perms",
            chmod_result.stderr or "chmod failed",
            stream=sys.stderr,
        )
        return False

    return True


async def _run_remote_latency_preprocessing(
    conn: asyncssh.SSHClientConnection,
    *,
    node_label: str,
    node_home: str,
    remote_logs: str,
    remote_user: str,
) -> None:
    """Best-effort remote latency log shrinking before download."""
    if not CSV_TO_PARQUET_SCRIPT.exists() or not DOWNSAMPLE_LATENCY_SCRIPT.exists():
        print("  warning: latency preprocessing scripts are missing locally; skipping")
        return

    quoted_logs = shlex.quote(remote_logs)
    csv_probe = await conn.run(
        f"find {quoted_logs} -type f -name '*latency*.csv' -print -quit 2>/dev/null",
        check=False,
    )
    parquet_probe = await conn.run(
        f"find {quoted_logs} -type f -name '*latency*.parquet' -print -quit 2>/dev/null",
        check=False,
    )
    if not (csv_probe.stdout or "").strip() and not (parquet_probe.stdout or "").strip():
        print(f"  No remote latency files found on {node_label}; skipping preprocessing.")
        return

    python_exec = await _ensure_remote_latency_python_environment(
        conn,
        node_label=node_label,
        node_home=node_home,
    )
    if python_exec is None:
        print(
            f"  warning: remote latency preprocessing dependencies are unavailable on "
            f"{node_label}; skipping preprocessing."
        )
        return
    if not await _ensure_remote_path_writable(
        conn,
        node_label=node_label,
        remote_path=remote_logs,
        remote_user=remote_user,
    ):
        print(
            f"  warning: remote latency log path is not writable on {node_label}; "
            "skipping preprocessing."
        )
        return

    remote_tools_dir = f"{node_home}/sim-tools"
    await _ensure_remote_dir(conn, remote_tools_dir)
    remote_convert_script = f"{remote_tools_dir}/{CSV_TO_PARQUET_SCRIPT.name}"
    remote_downsample_script = f"{remote_tools_dir}/{DOWNSAMPLE_LATENCY_SCRIPT.name}"
    await asyncio.gather(
        _upload_local_text_file(conn, CSV_TO_PARQUET_SCRIPT, remote_convert_script),
        _upload_local_text_file(conn, DOWNSAMPLE_LATENCY_SCRIPT, remote_downsample_script),
    )

    if (csv_probe.stdout or "").strip():
        convert_cmd = " ".join([
            shlex.quote(python_exec),
            shlex.quote(remote_convert_script),
            quoted_logs,
            "--pattern",
            shlex.quote("*latency*.csv"),
            "--remove-csv",
        ])
        result = await conn.run(convert_cmd, check=False)
        if result.stdout:
            _print_remote_output(f"{node_label}/latency-convert", result.stdout)
        if result.exit_status != 0:
            _print_remote_output(
                f"{node_label}/latency-convert",
                result.stderr or "conversion failed",
                stream=sys.stderr,
            )
        parquet_probe = await conn.run(
            f"find {quoted_logs} -type f -name '*latency*.parquet' -print -quit 2>/dev/null",
            check=False,
        )

    if not (parquet_probe.stdout or "").strip():
        print(f"  [{node_label}/latency-convert] No latency parquet files produced; skipping downsampling.")
        return

    downsample_cmd = " ".join([
        shlex.quote(python_exec),
        shlex.quote(remote_downsample_script),
        quoted_logs,
        "--pattern",
        shlex.quote("*latency*.parquet"),
        "--max-p999-rel-error",
        "0.01",
        "--no-manifest",
    ])
    result = await conn.run(downsample_cmd, check=False)
    if result.stdout:
        _print_remote_output(f"{node_label}/latency-downsample", result.stdout)
    if result.exit_status != 0:
        _print_remote_output(
            f"{node_label}/latency-downsample",
            result.stderr or "downsampling failed",
            stream=sys.stderr,
        )




async def _download_logs(
    conn: asyncssh.SSHClientConnection, remote_logs: str, local_dir: Path
) -> None:
    """Download all files from remote_logs dir into local_dir, preserving structure."""
    result = await conn.run(f"find {remote_logs} -type f 2>/dev/null", check=False)
    files = [l.strip() for l in (result.stdout or "").splitlines() if l.strip()]
    if not files:
        print(f"  (no log files found in {remote_logs})")
        return
    local_dir.mkdir(parents=True, exist_ok=True)
    async with conn.start_sftp_client() as sftp:
        for remote_file in files:
            rel = remote_file[len(remote_logs):].lstrip("/")
            local_file = local_dir / rel
            local_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                await sftp.get(remote_file, str(local_file))
                print(f"  downloaded {rel}")
            except asyncssh.SFTPError as exc:
                print(f"  warning: could not download {rel}: {exc}", file=sys.stderr)


def _retry_attempt_output_dir(output_dir: Path, attempt: int) -> Path:
    return output_dir.parent / f"{output_dir.name}__retry_attempt_{attempt}"


# ── Container helpers ─────────────────────────────────────────────────────────

async def _assert_running(
    conn: asyncssh.SSHClientConnection, name: str, delay: float = 2.0
) -> None:
    """Wait *delay* seconds then raise if the container has already exited.

    Call this immediately after `docker run -d` to catch immediate startup
    crashes before they turn into a silent multi-minute wait.
    """
    await asyncio.sleep(delay)
    result = await conn.run(
        f"docker inspect --format={{{{.State.Status}}}} {name} 2>&1", check=False
    )
    status = (result.stdout or "").strip()
    if status != "running":
        logs = await conn.run(f"docker logs {name} 2>&1", check=False)
        raise RuntimeError(
            f"Container '{name}' exited immediately (status: {status!r}).\n"
            f"Logs:\n{(logs.stdout or logs.stderr or '(no output)').strip()}"
        )


async def _stop_containers(
    conn: asyncssh.SSHClientConnection, names: list[str]
) -> None:
    if not names:
        return
    joined = " ".join(names)
    await _run(conn, f"docker stop {joined} 2>/dev/null || true", check=False)
    await _run(conn, f"docker rm -f {joined} 2>/dev/null || true", check=False)


async def _graceful_stop_tcp(
    conn: asyncssh.SSHClientConnection,
    names: list[str],
    timeout: int = 5,
) -> None:
    """Send 'q' to each container's stdin in parallel, force-remove after *timeout*."""
    if not names:
        return
    cmds = " & ".join(
        f"(printf 'q\\n'; sleep {timeout}) | timeout {timeout + 1}"
        f" docker attach {n} 2>/dev/null"
        for n in names
    )
    await _run(conn, f"{cmds}; wait", check=False)
    joined = " ".join(names)
    await _run(conn, f"docker rm -f {joined} 2>/dev/null || true", check=False)


async def _poll_for_pattern(
    conn: asyncssh.SSHClientConnection,
    container_name: str,
    pattern: str,
    timeout: float,
    label: str = "",
    since: Optional[int] = None,
) -> None:
    """Poll 'docker logs <container>' until *pattern* appears, streaming new
    lines; raises TimeoutError after *timeout* seconds. *since* (Unix
    timestamp), if set, ignores earlier logs. Containers must not use --rm,
    or Docker drops the log buffer on exit before it can be read."""
    since_flag = f"--since {since} " if since is not None else ""
    deadline = time.monotonic() + timeout
    shown: set[str] = set()

    while time.monotonic() < deadline:
        result = await conn.run(
            f"docker logs {since_flag}{container_name} 2>&1", check=False
        )
        output = result.stdout or ""

        for line in output.splitlines():
            if line and line not in shown:
                shown.add(line)
                pfx = f"  [{label}] " if label else "  "
                print(f"{pfx}{line}")

        if pattern in output:
            return

        # Fail fast if the container exited before emitting the pattern
        status_result = await conn.run(
            f"docker inspect --format={{{{.State.Status}}}} {container_name} 2>&1",
            check=False,
        )
        status = (status_result.stdout or "").strip()
        if status == "exited":
            logs_result = await conn.run(f"docker logs {container_name} 2>&1", check=False)
            logs = (logs_result.stdout or logs_result.stderr or "(no output)").strip()
            raise RuntimeError(
                f"Container '{container_name}' exited before '{pattern}' was seen.\n"
                f"Logs:\n{logs}"
            )

        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Timed out after {timeout:.0f}s waiting for '{pattern}' "
        f"in container '{container_name}'"
    )


def _cpus_flag(node: NodeInfo) -> str:
    """Return a --cpus docker flag when the node has a speed cap, else empty string."""
    if node.speed is not None:
        return f"--cpus {node.speed / 100:.2f}"
    return ""


def _worker_memory_flag(node: NodeInfo) -> str:
    """Return a --memory docker flag from _worker_memory_mb(), so Flink and
    NES worker containers share the same per-node memory budget."""
    return f"--memory {_worker_memory_mb(node.instance_type)}m"


async def _sync_source_data(
    src: NodeInfo,
    src_home: str,
    key_path: str,
) -> None:
    """Rsync local source data to the remote ~/data/ directory (checksum-based)."""
    if not SOURCE_DATA_DIR.exists():
        return
    if not any(f for f in SOURCE_DATA_DIR.iterdir() if f.is_file()):
        return
    print(f"Syncing source data to {src.user}@{src.host}:{src_home}/data/ ...")
    proc = await asyncio.create_subprocess_exec(
        "rsync", "--checksum", "--archive", "--verbose", "--human-readable",
        "-e", f"ssh -i {_expand(key_path)} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o BatchMode=yes",
        str(SOURCE_DATA_DIR) + "/",
        f"{src.user}@{src.host}:{src_home}/data/",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    for line in stdout.decode().splitlines():
        line = line.strip()
        if line and not line.startswith(("sending", "sent ", "total size")):
            print(f"  {line}")
    if proc.returncode != 0:
        raise RuntimeError(f"rsync failed (exit {proc.returncode}):\n{stderr.decode()}")


async def _sync_flink_libs(
    node: NodeInfo,
    node_home: str,
    key_path: str,
) -> None:
    """Rsync local lib/*.jar to the remote ~/flinke2c-lib/ directory."""
    if not FLINK_LIB_DIR.exists():
        return
    jars = [f for f in FLINK_LIB_DIR.iterdir() if f.suffix == ".jar"]
    if not jars:
        return
    print(f"  Syncing {len(jars)} lib JAR(s) to {node.id}...")
    proc = await asyncio.create_subprocess_exec(
        "rsync", "--checksum", "--archive", "--verbose",
        "-e", f"ssh -i {_expand(key_path)} -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o BatchMode=yes",
        str(FLINK_LIB_DIR) + "/",
        f"{node.user}@{node.host}:{node_home}/flinke2c-lib/",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(f"rsync lib failed for {node.id} (exit {proc.returncode}):\n{stderr.decode()}")


async def _wait_sink_done(
    snk_conn: asyncssh.SSHClientConnection,
    sink_name: str,
    since: int,
    timeout: float = DONE_TIMEOUT,
) -> None:
    """Wait for the sink to signal end-of-repetition (the reliable signal,
    since source containers stay up across reps). *since* is a Unix
    timestamp filtering out signals from earlier repetitions."""
    await _poll_for_pattern(
        snk_conn,
        sink_name,
        DONE_SIGNAL,
        timeout,
        label=f"sink/{sink_name}",
        since=since,
    )


async def _clear_remote_path(conn: asyncssh.SSHClientConnection, path: str) -> None:
    """Clear a remote path, avoiding sudo unless strictly required."""
    # Fast path: clear as the SSH user.
    rm_result = await conn.run(f"rm -rf {path}", check=False)
    if rm_result.exit_status == 0:
        return

    # Fallback: clear as root for root-owned trees.
    sudo_result = await conn.run(f"sudo -n rm -rf {path}", check=False)
    if sudo_result.exit_status != 0:
        raise RuntimeError(
            f"Failed to clear remote path {path!r}.\n"
            f"rm stderr: {(rm_result.stderr or '').strip()}\n"
            f"sudo rm stderr: {(sudo_result.stderr or '').strip()}"
        )


async def _ensure_remote_dir(conn: asyncssh.SSHClientConnection, path: str) -> None:
    """Create a remote directory tree, falling back to sudo for root-owned parents."""
    mkdir_result = await conn.run(f"mkdir -p {path}", check=False)
    if mkdir_result.exit_status == 0:
        return

    sudo_result = await conn.run(f"sudo -n mkdir -p {path}", check=False)
    if sudo_result.exit_status != 0:
        raise RuntimeError(
            f"Failed to create remote directory {path!r}.\n"
            f"mkdir stderr: {(mkdir_result.stderr or '').strip()}\n"
            f"sudo mkdir stderr: {(sudo_result.stderr or '').strip()}"
        )


async def _assert_remote_dir_empty(conn: asyncssh.SSHClientConnection, path: str) -> None:
    """Fail if a remote directory still contains files after cleanup."""
    find_cmd = f"find {path} -type f -print -quit 2>/dev/null"
    result = await conn.run(find_cmd, check=False)
    if result.exit_status != 0:
        result = await conn.run(f"sudo -n {find_cmd}", check=False)

    leftover = (result.stdout or "").strip()
    if leftover:
        raise RuntimeError(
            f"Remote log directory {path!r} was not empty after cleanup.\n"
            f"Example leftover file: {leftover}"
        )


async def _wait_flink_taskmanagers_ready(
    snk_conn: asyncssh.SSHClientConnection,
    jm_name: str,
    expected_count: int,
    timeout: float = FLINK_CLUSTER_READY_TIMEOUT,
) -> None:
    """Wait until Flink REST reports all expected taskmanagers."""
    if expected_count <= 0:
        return

    deadline = time.monotonic() + timeout
    last_seen = -1
    while time.monotonic() < deadline:
        # Fail fast if the JM crashed before the cluster formed.
        jm_status = await snk_conn.run(
            f"docker inspect --format={{{{.State.Status}}}} {jm_name} 2>&1",
            check=False,
        )
        status = (jm_status.stdout or "").strip()
        if status and status != "running":
            jm_logs = await snk_conn.run(f"docker logs {jm_name} 2>&1", check=False)
            logs = (jm_logs.stdout or jm_logs.stderr or "(no output)").strip()
            raise RuntimeError(
                f"Flink jobmanager '{jm_name}' is not running (status: {status!r}).\n"
                f"Logs:\n{logs}"
            )

        # Query TM registrations from Flink REST.
        rest = await snk_conn.run(
            "curl -fsS http://127.0.0.1:8081/taskmanagers",
            check=False,
        )
        if rest.exit_status == 0 and rest.stdout:
            try:
                payload = _json.loads(rest.stdout)
            except _json.JSONDecodeError:
                payload = {}
            tms = payload.get("taskmanagers") if isinstance(payload, dict) else []
            seen = len(tms) if isinstance(tms, list) else 0
            if seen != last_seen:
                print(f"  Flink REST taskmanagers: {seen}/{expected_count}")
                last_seen = seen
            if seen >= expected_count:
                return

        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Timed out after {timeout:.0f}s waiting for Flink taskmanagers "
        f"({expected_count} expected, last seen {max(last_seen, 0)})."
    )


async def _flink_list_jobs(
    snk_conn: asyncssh.SSHClientConnection,
) -> list[dict]:
    """Return the Flink job list from the JobManager REST API."""
    result = await snk_conn.run(
        "curl -fsS http://127.0.0.1:8081/jobs",
        check=False,
    )
    if result.exit_status != 0:
        raise RuntimeError(
            "Failed to query Flink jobs via REST.\n"
            f"stderr: {(result.stderr or '').strip()}"
        )

    try:
        payload = _json.loads(result.stdout or "{}")
    except _json.JSONDecodeError as exc:
        raise RuntimeError(
            "Failed to decode Flink jobs REST response.\n"
            f"Body:\n{(result.stdout or '').strip()}"
        ) from exc

    jobs = payload.get("jobs")
    return jobs if isinstance(jobs, list) else []


async def _wait_flink_job_finished(
    snk_conn: asyncssh.SSHClientConnection,
    existing_job_ids: set[str],
    timeout: float = DONE_TIMEOUT,
) -> str:
    """Wait for the newly submitted Flink job to appear and reach FINISHED."""
    deadline = time.monotonic() + timeout
    job_id: Optional[str] = None
    last_status: Optional[str] = None
    saw_running = False

    while time.monotonic() < deadline:
        jobs = await _flink_list_jobs(snk_conn)

        if job_id is None:
            new_jobs = [
                j for j in jobs
                if isinstance(j, dict) and j.get("id") not in existing_job_ids
            ]
            if new_jobs:
                job_id = str(new_jobs[0].get("id"))
                print(f"  Flink job submitted: {job_id}")

        if job_id is not None:
            job = next(
                (
                    j for j in jobs
                    if isinstance(j, dict) and str(j.get("id")) == job_id
                ),
                None,
            )
            if job is not None:
                status = str(job.get("status", "UNKNOWN"))
                if status != last_status:
                    print(f"  Flink job {job_id}: {status}")
                    last_status = status

                if status == "RUNNING":
                    saw_running = True

                if status in {"RESTARTING", "FAILING", "FAILED", "CANCELLING", "CANCELED", "SUSPENDED"}:
                    raise FlinkJobRetryableError(
                        f"Flink job {job_id} entered terminal/error state {status!r}."
                    )

                if status == "FINISHED":
                    if not saw_running:
                        print(
                            "  Flink job reached FINISHED before RUNNING was observed "
                            f"(job_id={job_id})."
                        )
                    return job_id

        await asyncio.sleep(POLL_INTERVAL)

    if job_id is None:
        raise TimeoutError(
            f"Timed out after {timeout:.0f}s waiting for submitted Flink job to appear"
        )
    raise TimeoutError(
        f"Timed out after {timeout:.0f}s waiting for Flink job {job_id} to reach FINISHED"
    )


async def _wait_flink_job_terminal(
    snk_conn: asyncssh.SSHClientConnection,
    job_id: str,
    timeout: float = DONE_TIMEOUT,
) -> str:
    """Wait for a specific Flink job to reach FINISHED."""
    deadline = time.monotonic() + timeout
    last_status: Optional[str] = None
    saw_running = False

    while time.monotonic() < deadline:
        jobs = await _flink_list_jobs(snk_conn)
        job = next(
            (
                j for j in jobs
                if isinstance(j, dict) and str(j.get("id")) == job_id
            ),
            None,
        )
        if job is not None:
            status = str(job.get("status", "UNKNOWN"))
            if status != last_status:
                print(f"  Flink job {job_id}: {status}")
                last_status = status

            if status == "RUNNING":
                saw_running = True

            if status in {"RESTARTING", "FAILING", "FAILED", "CANCELLING", "CANCELED", "SUSPENDED"}:
                raise FlinkJobRetryableError(
                    f"Flink job {job_id} entered terminal/error state {status!r}."
                )

            if status == "FINISHED":
                if not saw_running:
                    print(
                        "  Flink job reached FINISHED before RUNNING was observed "
                        f"(job_id={job_id})."
                    )
                return job_id

        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Timed out after {timeout:.0f}s waiting for Flink job {job_id} to reach FINISHED"
    )


async def _wait_flink_job_running(
    snk_conn: asyncssh.SSHClientConnection,
    existing_job_ids: set[str],
    timeout: float = DONE_TIMEOUT,
) -> str:
    """Wait for the newly submitted Flink job to appear and reach RUNNING."""
    deadline = time.monotonic() + timeout
    job_id: Optional[str] = None
    last_status: Optional[str] = None

    while time.monotonic() < deadline:
        jobs = await _flink_list_jobs(snk_conn)

        if job_id is None:
            new_jobs = [
                j for j in jobs
                if isinstance(j, dict) and j.get("id") not in existing_job_ids
            ]
            if new_jobs:
                job_id = str(new_jobs[0].get("id"))
                print(f"  Flink job submitted: {job_id}")

        if job_id is not None:
            job = next(
                (
                    j for j in jobs
                    if isinstance(j, dict) and str(j.get("id")) == job_id
                ),
                None,
            )
            if job is not None:
                status = str(job.get("status", "UNKNOWN"))
                if status != last_status:
                    print(f"  Flink job {job_id}: {status}")
                    last_status = status

                if status == "RUNNING":
                    return job_id

                if status in {"RESTARTING", "FAILING", "FAILED", "CANCELLING", "CANCELED", "SUSPENDED"}:
                    raise FlinkJobRetryableError(
                        f"Flink job {job_id} entered terminal/error state {status!r}."
                    )

                if status == "FINISHED":
                    raise RuntimeError(
                        f"Flink job {job_id} finished before CAPSys profiling could attach."
                    )

        await asyncio.sleep(POLL_INTERVAL)

    if job_id is None:
        raise TimeoutError(
            f"Timed out after {timeout:.0f}s waiting for submitted Flink job to appear"
        )
    raise TimeoutError(
        f"Timed out after {timeout:.0f}s waiting for Flink job {job_id} to reach RUNNING"
    )


async def _cancel_flink_job(
    snk_conn: asyncssh.SSHClientConnection,
    job_id: str,
    timeout: float = 60,
) -> None:
    """Cancel a Flink job via REST and wait until it disappears or is canceled."""
    result = await snk_conn.run(
        f"curl -sS -o /dev/null -w '%{{http_code}}' -X PATCH http://127.0.0.1:8081/jobs/{job_id}",
        check=False,
    )
    status_code = (result.stdout or "").strip()
    if result.exit_status != 0:
        raise RuntimeError(
            f"Failed to request cancellation for Flink job {job_id}.\n"
            f"stderr: {(result.stderr or '').strip()}"
        )
    if status_code == "409":
        print(f"  Flink job {job_id}: already finished; ignoring cancel conflict")
        return
    if status_code and status_code not in {"200", "202"}:
        raise RuntimeError(
            f"Failed to request cancellation for Flink job {job_id}.\n"
            f"HTTP status: {status_code}\n"
            f"stderr: {(result.stderr or '').strip()}"
        )

    deadline = time.monotonic() + timeout
    last_status: Optional[str] = None
    while time.monotonic() < deadline:
        jobs = await _flink_list_jobs(snk_conn)
        job = next(
            (
                j for j in jobs
                if isinstance(j, dict) and str(j.get("id")) == job_id
            ),
            None,
        )
        if job is None:
            return

        status = str(job.get("status", "UNKNOWN"))
        if status != last_status:
            print(f"  Flink job {job_id}: {status}")
            last_status = status

        if status in {"CANCELED", "FAILED", "FINISHED", "SUSPENDED"}:
            return

        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Timed out after {timeout:.0f}s waiting for Flink job {job_id} to cancel"
    )


async def _wait_nes_topology_workers_ready(
    snk_conn: asyncssh.SSHClientConnection,
    nes_name: str,
    expected_count: int,
    timeout: float = NES_CLUSTER_READY_TIMEOUT,
) -> dict[str, Any]:
    """Wait until NES topology REST reports all worker nodes.

    expected_count includes the coordinator-local worker.
    """
    if expected_count <= 0:
        return {}

    deadline = time.monotonic() + timeout
    last_seen = -1
    topology_cmd = _nes_topology_fetch_cmd()

    while time.monotonic() < deadline:
        # Fail fast if the coordinator crashed before workers registered.
        coord_status = await snk_conn.run(
            f"docker inspect --format={{{{.State.Status}}}} {nes_name} 2>&1",
            check=False,
        )
        status = (coord_status.stdout or "").strip()
        if status and status != "running":
            coord_logs = await snk_conn.run(f"docker logs {nes_name} 2>&1", check=False)
            logs = (coord_logs.stdout or coord_logs.stderr or "(no output)").strip()
            raise RuntimeError(
                f"NES coordinator '{nes_name}' is not running (status: {status!r}).\n"
                f"Logs:\n{logs}"
            )

        rest = await snk_conn.run(topology_cmd, check=False)
        if rest.exit_status == 0 and rest.stdout:
            try:
                payload = _json.loads(rest.stdout)
            except _json.JSONDecodeError:
                payload = {}
            nodes = payload.get("nodes") if isinstance(payload, dict) else []
            seen = len(nodes) if isinstance(nodes, list) else 0
            if seen != last_seen:
                print(f"  NES topology nodes: {seen}/{expected_count}")
                last_seen = seen
            if seen >= expected_count:
                return payload if isinstance(payload, dict) else {}

        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Timed out after {timeout:.0f}s waiting for NES topology workers "
        f"({expected_count} expected, last seen {max(last_seen, 0)})."
    )


def _nes_topology_fetch_cmd() -> str:
    return (
        f"for p in /v1/nes/topology /nes/topology; do "
        f"curl -fsS http://127.0.0.1:{NES_REST_PORT}$p && exit 0; "
        "done; exit 1"
    )


def _rewrite_nes_query_sink_host(query: str, sink_host: str) -> str:
    """Replace the TcpSinkDescriptor host with the topology sink address."""
    pattern = r'TcpSinkDescriptor::create\("([^"]+)"\s*,'
    replacement = f'TcpSinkDescriptor::create("{sink_host}",'
    rewritten, count = re.subn(pattern, replacement, query, count=1)
    if count == 0:
        raise RuntimeError(
            "Failed to locate TcpSinkDescriptor in NES query for sink host rewrite."
        )
    return rewritten


def _extract_nes_topology_nodes(payload: dict[str, Any]) -> list[dict[str, Any]]:
    nodes = payload.get("nodes")
    if not isinstance(nodes, list):
        return []
    return [node for node in nodes if isinstance(node, dict)]


def _walk_keyed_values(value: Any, *, key: str = "") -> list[tuple[str, Any]]:
    found: list[tuple[str, Any]] = []
    if isinstance(value, dict):
        for child_key, child_value in value.items():
            found.extend(_walk_keyed_values(child_value, key=str(child_key)))
    elif isinstance(value, list):
        for item in value:
            found.extend(_walk_keyed_values(item, key=key))
    else:
        found.append((key, value))
    return found


def _parse_worker_id(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
    return None


def _extract_nes_worker_id(node: dict[str, Any]) -> Optional[int]:
    preferred_keys = ("workerid", "id")

    keyed_values = _walk_keyed_values(node)
    for key_name in preferred_keys:
        for key, value in keyed_values:
            if key.lower() != key_name:
                continue
            worker_id = _parse_worker_id(value)
            if worker_id is not None:
                return worker_id
    return None


def _normalize_host_candidates(raw: str) -> set[str]:
    candidates: set[str] = set()
    value = raw.strip()
    if not value:
        return candidates

    candidates.add(value)

    parsed = urlparse(value)
    if parsed.hostname:
        candidates.add(parsed.hostname)

    if "://" not in value and value.count(":") == 1:
        host_part = value.split(":", 1)[0].strip()
        if host_part:
            candidates.add(host_part)

    return candidates


def _extract_nes_worker_addresses(node: dict[str, Any]) -> set[str]:
    addresses: set[str] = set()
    for key, value in _walk_keyed_values(node):
        if not isinstance(value, str):
            continue
        key_name = key.lower()
        if not any(token in key_name for token in ("host", "addr", "ip")):
            continue
        addresses.update(_normalize_host_candidates(value))
    return addresses


def _map_topology_workers_to_nes_ids(
    payload: dict[str, Any],
    worker_nodes: list[NodeInfo],
) -> dict[str, int]:
    address_to_topology_id: dict[str, str] = {}
    for worker in worker_nodes:
        for candidate in (worker.address, worker.host):
            for normalized in _normalize_host_candidates(candidate):
                existing = address_to_topology_id.get(normalized)
                if existing and existing != worker.id:
                    raise RuntimeError(
                        "NES topology mapping is ambiguous: "
                        f"address {normalized!r} matches both {existing!r} and {worker.id!r}."
                    )
                address_to_topology_id[normalized] = worker.id

    mapping: dict[str, int] = {}
    unresolved = {worker.id for worker in worker_nodes}
    unmatched_nodes: list[dict[str, Any]] = []

    for node in _extract_nes_topology_nodes(payload):
        worker_id = _extract_nes_worker_id(node)
        if worker_id is None:
            unmatched_nodes.append(node)
            continue

        matched_topology_id: Optional[str] = None
        for address in _extract_nes_worker_addresses(node):
            topology_id = address_to_topology_id.get(address)
            if topology_id is None:
                continue
            matched_topology_id = topology_id
            break

        if matched_topology_id is None:
            unmatched_nodes.append(node)
            continue

        existing = mapping.get(matched_topology_id)
        if existing is not None and existing != worker_id:
            raise RuntimeError(
                "NES topology mapping is inconsistent: "
                f"topology node {matched_topology_id!r} matched worker IDs {existing} and {worker_id}."
            )

        mapping[matched_topology_id] = worker_id
        unresolved.discard(matched_topology_id)

    if unresolved:
        available = [
            {
                "workerId": _extract_nes_worker_id(node),
                "addresses": sorted(_extract_nes_worker_addresses(node)),
            }
            for node in _extract_nes_topology_nodes(payload)
        ]
        raise RuntimeError(
            "Failed to map all topology workers to NES worker IDs. "
            f"Unresolved topology nodes: {sorted(unresolved)}. "
            f"REST topology nodes: {available}"
        )

    return mapping


def _load_explicit_topology_edges(topology_file: str) -> list[tuple[str, str]]:
    with open(topology_file) as f:
        raw = _json.load(f)

    edges = raw.get("edges", [])
    if not isinstance(edges, list):
        return []

    explicit_edges: list[tuple[str, str]] = []
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        source = edge.get("source")
        target = edge.get("target")
        if isinstance(source, str) and isinstance(target, str):
            explicit_edges.append((source, target))
    return explicit_edges


def _desired_nes_topology_links(
    topology_file: str,
    worker_topology_ids: set[str],
    sink_topology_id: str,
) -> list[tuple[str, str]]:
    """Return NES parent->child links derived from source->sink topology edges.

    sink_topology_id is handled specially: it's not a placement worker_node,
    but it IS a real NES worker (the coordinator's local role, workerId 1),
    so an edge like "N1 -> snk" must still make N1 the coordinator's direct
    child, not get dropped like a true non-worker node such as "src".
    """
    desired_links: list[tuple[str, str]] = []
    seen_links: set[tuple[str, str]] = set()

    for source_id, target_id in _load_explicit_topology_edges(topology_file):
        if target_id == sink_topology_id:
            if source_id not in worker_topology_ids:
                continue
            link = (sink_topology_id, source_id)
        elif source_id not in worker_topology_ids or target_id not in worker_topology_ids:
            continue
        else:
            # Topology edges flow source->sink, while NES parent-child links
            # point upstream toward the sink: A->B becomes parent=B, child=A.
            link = (target_id, source_id)

        if link in seen_links:
            continue
        seen_links.add(link)
        desired_links.append(link)

    return desired_links


async def _reconcile_nes_worker_topology(
    *,
    snk_conn: asyncssh.SSHClientConnection,
    topology_file: str,
    snk: NodeInfo,
    worker_nodes: list[NodeInfo],
    rest_payload: dict[str, Any],
) -> None:
    desired_links = _desired_nes_topology_links(
        topology_file,
        {worker.id for worker in worker_nodes},
        sink_topology_id=snk.id,
    )
    if not desired_links:
        print("  NES topology has no explicit compute-to-compute links; keeping default parent assignments.")
        return

    # Include snk in the ID mapping too - it's a real NES worker (the
    # coordinator's own local worker role), needed to resolve any link
    # produced above with sink_topology_id as the parent.
    topology_to_worker_id = _map_topology_workers_to_nes_ids(rest_payload, worker_nodes + [snk])
    readable_mapping = ", ".join(
        f"{topology_id}={worker_id}"
        for topology_id, worker_id in sorted(topology_to_worker_id.items())
    )
    print(f"  NES worker ID mapping: {readable_mapping}")

    explicit_children = {
        topology_to_worker_id[child_topology_id]
        for _, child_topology_id in desired_links
    }
    for child_id in sorted(explicit_children):
        delete_payload = _json.dumps({"parentId": 1, "childId": child_id})
        delete_cmd = (
            f"curl -fsS -X DELETE http://127.0.0.1:{NES_REST_PORT}/v1/nes/topology/removeAsChild "
            f"-H 'Content-Type: application/json' "
            f"-d {shlex.quote(delete_payload)}"
        )
        delete_result = await snk_conn.run(delete_cmd, check=False)
        if delete_result.exit_status != 0:
            stderr = (delete_result.stderr or "").strip()
            if "404" not in stderr:
                raise RuntimeError(
                    "Failed to remove default NES parent-child link "
                    f"1->{child_id}: {stderr or delete_result.stdout or 'no output'}"
                )

    for parent_topology_id, child_topology_id in desired_links:
        parent_id = topology_to_worker_id[parent_topology_id]
        child_id = topology_to_worker_id[child_topology_id]
        if parent_id == child_id:
            raise RuntimeError(
                f"Refusing to create self-link for NES worker {parent_topology_id!r} (workerId={parent_id})."
            )

        add_payload = _json.dumps({"parentId": parent_id, "childId": child_id})
        add_cmd = (
            f"curl -fsS -X POST http://127.0.0.1:{NES_REST_PORT}/v1/nes/topology/addAsChild "
            f"-H 'Content-Type: application/json' "
            f"-d {shlex.quote(add_payload)}"
        )
        await _run(snk_conn, add_cmd)

    print(f"  Reconciled {len(desired_links)} NES parent-child link(s) from topology.")


# ── Config generators ─────────────────────────────────────────────────────────

def _coordinator_config(
    jm_address: str,
    placement_method: str = "",
    graphml_path: str = "",
    instance_type: str = "t3.micro",
    use_rocksdb: bool = True,
) -> str:
    # The JobManager shares its node with the tcp-sink container, so it only
    # gets half the instance's memory rather than the near-all share the
    # dedicated TaskManager nodes get in _worker_config. fallback_mb applies
    # to on-prem cluster nodes (no AWS instance_type/spec to size from) -
    # 1024m clears the JobManager's fixed off-heap/metaspace/overhead floor
    # with real heap left over, well under real cluster RAM.
    jm_memory_mb = _flink_process_memory_mb(instance_type, fraction=0.5, fallback_mb=1024)
    lines = [
        "parallelism:",
        "  default: 1",
        "",
        "io:",
        "  tmp:",
        "    dirs: /tmp",
        "",
    ]
    if placement_method:
        lines += [
            "cluster:",
            f"  placement-method: {placement_method}",
            "",
        ]
        if placement_method.strip().upper() == "CAPSYS":
            lines += [
                "cluster.capsys.scheduler-cfg.path: /conf/schedulercfg",
                "",
            ]
    lines += [
        "pipeline.operator-chaining.enabled: false",
        "",
        "jobmanager:",
        "  bind-host: 0.0.0.0",
        "  rpc:",
        f"    address: {jm_address}",
        "    port: 6123",
        "  memory:",
        "    process:",
        f"      size: {jm_memory_mb}m",
        "",
        "rest:",
        f"  address: {jm_address}",
        "  bind-address: 0.0.0.0",
        "  port: 8081",
        "",
    ]
    if graphml_path:
        lines += [
            "topology:",
            "  graphml:",
            f"    path: {graphml_path}",
            "",
        ]
    lines += [
        # GC heap-layout tuning is kept (it's a real JVM performance knob, not
        # overhead specific to Flink); verbose GC logging is dropped since it's
        # pure diagnostic I/O with no NES counterpart and no benefit here.
        "env:",
        "  java:",
        "    opts:",
        "      all: >-",
        "        -XX:NewRatio=3 -XX:ParallelGCThreads=4"
        " --add-opens=java.base/java.util=ALL-UNNAMED",
        "",
    ]
    # Checkpointing itself stays disabled either way (periodic snapshotting
    # has no NES equivalent). use_rocksdb toggles the state backend: q4's
    # join sits inside a 24h window that never naturally closes during a
    # normal run, so all matched state accumulates for the whole run. On
    # HashMapStateBackend (Flink's default, used when this is off) that state
    # lives entirely on-heap, which previously GC-death-spiraled into a
    # TaskManager heartbeat timeout on the old fixed heap size - observed in
    # practice, not just theoretical. RocksDB avoids that by keeping state
    # off-heap/on-disk (independent of whether checkpointing runs), at the
    # cost of serializing every state access - noticeably slower. Now that
    # taskmanager memory is sized from the real instance type instead of a
    # fixed 1728m (see _flink_process_memory_mb), it's worth testing whether
    # the extra heap alone is enough without RocksDB's overhead.
    if use_rocksdb:
        lines += [
            "state:",
            "  backend:",
            "    type: rocksdb",
            "",
            "state.backend.rocksdb.localdir: /tmp",
            "",
        ]
    lines += [
        "table:",
        "  exec:",
        "    mini-batch:",
        "      enabled: false",
        "  optimizer:",
        "    distinct-agg:",
        "      split:",
        "        enabled: true",
    ]
    return "\n".join(lines) + "\n"


def _worker_config(
    jm_address: str,
    tm_host: str,
    task_slots: int,
    instance_type: str = "t3.micro",
) -> str:
    tm_memory_mb = _worker_memory_mb(instance_type)
    lines = [
        "env:",
        "  java:",
        "    opts:",
        "      all: >-",
        "        --add-exports=java.base/sun.net.util=ALL-UNNAMED",
        "        --add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED",
        "        --add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
        "        --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
        "        --add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
        "        --add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED",
        "        --add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
        "        --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
        "        --add-opens=java.base/java.lang=ALL-UNNAMED",
        "        --add-opens=java.base/java.net=ALL-UNNAMED",
        "        --add-opens=java.base/java.io=ALL-UNNAMED",
        "        --add-opens=java.base/java.nio=ALL-UNNAMED",
        "        --add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "        --add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "        --add-opens=java.base/java.text=ALL-UNNAMED",
        "        --add-opens=java.base/java.time=ALL-UNNAMED",
        "        --add-opens=java.base/java.util=ALL-UNNAMED",
        "        --add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "        --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "        --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
        "",
        "jobmanager:",
        "  rpc:",
        f"    address: {jm_address}",
        "    port: 6123",
        "",
        "taskmanager:",
        "  bind-host: 0.0.0.0",
        f"  host: {tm_host}",
        f"  numberOfTaskSlots: {task_slots}",
        "  memory:",
        "    process:",
        f"      size: {tm_memory_mb}m",
        "",
        "parallelism:",
        "  default: 1",
        "",
        "rest:",
        "  address: 0.0.0.0",
        "  bind-address: 0.0.0.0",
        "  port: 8081",
        "",
        "web:",
        "  submit:",
        "    enable: false",
        "  cancel:",
        "    enable: false",
        "",
        "high-availability:",
        "  type: NONE",
    ]
    return "\n".join(lines) + "\n"


# ── NES config generators ─────────────────────────────────────────────────────

def _nes_coordinator_config(
    coordinator_host: str,
    latency: bool = True,
    num_slots: Optional[int] = 1,
    instance_type: str = "t3.micro",
) -> str:
    # The TCP source only puts a latency_ts value on the wire when started
    # with --latency, so the field must not be declared here otherwise
    # (see system_flag/latency_arg in _run_flink_experiment/_run_nes_experiment).
    lat = (
        "      - name: latency_ts\n"
        "        type: INT64\n"
        if latency
        else ""
    )
    # num_slots caps how many operators the placement algorithm may pin onto
    # this node; None leaves NES at its own (effectively unlimited) default.
    # A too-low value (e.g. 1) can make TopDownStrategy placement hang in
    # OPTIMIZING on star topologies where a pinned SOURCE/SINK's node has no
    # spare slot left for a colocated operator (e.g. plain q1: source+map+sink).
    slots = f"  numberOfSlots: {num_slots}\n" if num_slots is not None else ""
    buffer_pool_mb, max_hash_table_mb = _nes_memory_budget_mb(instance_type)
    global_buffers = (buffer_pool_mb * 1024 * 1024) // NES_BUFFER_SIZE_BYTES
    source_local_buffers = max(global_buffers // 4, 1)
    max_hash_table_bytes = max_hash_table_mb * 1024 * 1024
    return f"""\
logLevel: LOG_ERROR

optimizer:
   distributedJoinOptimizationMode: MATRIX

restIp: 127.0.0.1
coordinatorHost: {coordinator_host}
restPort: {NES_REST_PORT}

worker:
  localWorkerHost: {coordinator_host}
  coordinatorHost: {coordinator_host}
  numberOfBuffersInGlobalBufferManager: {global_buffers}
  numberOfBuffersInSourceLocalBufferPool: {source_local_buffers}
  bufferSizeInBytes: {NES_BUFFER_SIZE_BYTES}
{slots}
  queryCompiler:
    maxHashTableSize: {max_hash_table_bytes}
    joinStrategy: HASH_JOIN_LOCAL
    numberOfPartitions: 512
    preAllocPageCnt: 4
    pageSize: 65536

  workerId: 1

logicalSources:
  - logicalSourceName: bids
    fields:
      - name: auction
        type: INT64
      - name: bidder
        type: INT64
      - name: price
        type: FLOAT64
      - name: dateTime
        type: INT64
{lat}
  - logicalSourceName: persons
    fields:
      - name: id
        type: INT64
      - name: dateTime
        type: INT64
{lat}
  - logicalSourceName: auctions
    fields:
      - name: id
        type: INT64
      - name: initialBid
        type: INT64
      - name: reserve
        type: INT64
      - name: dateTime
        type: INT64
      - name: expires
        type: INT64
      - name: seller
        type: INT64
      - name: category
        type: INT64
{lat}"""


def _nes_worker_config(
    worker_host: str,
    coordinator_host: str,
    source_host: Optional[str] = None,
    num_slots: Optional[int] = None,
    instance_type: str = "t3.micro",
) -> str:
    slots = f"numberOfSlots: {num_slots}\n" if num_slots is not None else ""
    buffer_pool_mb, max_hash_table_mb = _nes_memory_budget_mb(instance_type)
    global_buffers = (buffer_pool_mb * 1024 * 1024) // NES_BUFFER_SIZE_BYTES
    source_local_buffers = max(global_buffers // 4, 1)
    max_hash_table_bytes = max_hash_table_mb * 1024 * 1024
    cfg = f"""\
logLevel: LOG_ERROR
localWorkerHost: {worker_host}
coordinatorHost: {coordinator_host}
numberOfBuffersInGlobalBufferManager: {global_buffers}
numberOfBuffersInSourceLocalBufferPool: {source_local_buffers}
bufferSizeInBytes: {NES_BUFFER_SIZE_BYTES}
{slots}

queryCompiler:
  maxHashTableSize: {max_hash_table_bytes}
  joinStrategy: HASH_JOIN_LOCAL
  numberOfPartitions: 512
  preAllocPageCnt: 4
  pageSize: 65536

"""
    if source_host is not None:
        cfg += f"""\
physicalSources:
    - logicalSourceName: bids
      physicalSourceName: bids_1
      type: TCP_SOURCE
      configuration:
        socketHost: {source_host}
        socketPort: 10000
        decideMessageSize: BUFFER_SIZE_FROM_SOCKET
        bytesUsedForSocketBufferSizeTransfer: 4
        inputFormat: FE2C_BINARY

    - logicalSourceName: auctions
      physicalSourceName: auctions_1
      type: TCP_SOURCE
      configuration:
        socketHost: {source_host}
        socketPort: 10001
        decideMessageSize: BUFFER_SIZE_FROM_SOCKET
        bytesUsedForSocketBufferSizeTransfer: 4
        inputFormat: FE2C_BINARY

    - logicalSourceName: persons
      physicalSourceName: persons_1
      type: TCP_SOURCE
      configuration:
        socketHost: {source_host}
        socketPort: 10002
        decideMessageSize: BUFFER_SIZE_FROM_SOCKET
        bytesUsedForSocketBufferSizeTransfer: 4
        inputFormat: FE2C_BINARY

"""
    cfg += f"""\
parentId: 1
"""
    return cfg


# ── GraphML generator ─────────────────────────────────────────────────────────

def generate_graphml(
    graph: nx.Graph,
    src_id: str,
    sink_id: Optional[str],
    task_slots: int,
) -> str:
    """Build a topology.graphml from the network graph for Flink cluster placement."""
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<graphml xmlns="http://graphml.graphdrawing.org/xmlns"',
        '         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"',
        '         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns'
        ' http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">',
        '  <key id="type"    for="node" attr.name="type"              attr.type="string"/>',
        '  <key id="id"      for="node" attr.name="id"                attr.type="string"/>',
        '  <key id="compute" for="node" attr.name="computeCapability" attr.type="double"/>',
        '  <key id="memory"  for="node" attr.name="memoryCapability"  attr.type="double"/>',
        '  <key id="slots"   for="node" attr.name="slots"             attr.type="int"/>',
        '',
        '  <graph id="processing-topology" edgedefault="directed">',
    ]

    for nid, attrs in graph.nodes(data=True):
        topo: TopoNode = attrs["data"]
        if nid == src_id:
            lines += [
                f'    <node id="{nid}">',
                '      <data key="type">source</data>',
                '      <data key="id">Source1</data>',
                '    </node>',
                '',
            ]
        elif nid == sink_id:
            lines += [
                f'    <node id="{nid}">',
                '      <data key="type">sink</data>',
                '      <data key="id">Sink1</data>',
                '    </node>',
                '',
            ]
        else:
            lines += [
                f'    <node id="{nid}">',
                '      <data key="type">compute</data>',
                f'      <data key="id">{topo.address}</data>',
                '      <data key="compute">1.0</data>',
                '      <data key="memory">1.0</data>',
                f'      <data key="slots">{task_slots}</data>',
                '    </node>',
                '',
            ]

    # Edges are already directed correctly in the topology JSON — emit as-is.
    # For the no-edges case (flat cloud topology), generate a full mesh.
    if graph.number_of_edges() > 0:
        for u, v in graph.edges():
            lines.append(f'    <edge id="{u}_{v}" source="{u}" target="{v}"/>')
    else:
        # No edges: fully connected mesh (non-sink → non-source)
        non_sink = [n for n, d in graph.nodes(data=True)
                    if d["data"].node_type.lower() != "sink"]
        non_src  = [n for n, d in graph.nodes(data=True)
                    if d["data"].node_type.lower() != "source"]
        for u in non_sink:
            for v in non_src:
                if u != v:
                    lines.append(f'    <edge id="{u}_{v}" source="{u}" target="{v}"/>')

    lines += ['  </graph>', '</graphml>']
    return '\n'.join(lines) + '\n'


def _rewrite_graphml_slots(graphml_content: str, task_slots: int) -> tuple[str, int]:
    """Rewrite all GraphML <data key="slots"> values to task_slots."""
    pattern = re.compile(r'(<data\s+key="slots"\s*>\s*)\d+(\s*</data>)')
    rewritten, count = pattern.subn(rf"\g<1>{task_slots}\g<2>", graphml_content)
    return rewritten, count


def _rewrite_sql_tcp_hosts(sql_content: str, source_host: str, sink_host: str) -> tuple[str, int, int]:
    """Rewrite tcp-source/tcp-sink host values in SQL WITH connector blocks."""
    source_pattern = re.compile(
        r"('connector'\s*=\s*'tcp-source'\s*,\s*'host'\s*=\s*')[^']+(')",
        flags=re.IGNORECASE,
    )
    sink_pattern = re.compile(
        r"('connector'\s*=\s*'tcp-sink'\s*,\s*'host'\s*=\s*')[^']+(')",
        flags=re.IGNORECASE,
    )
    rewritten, src_count = source_pattern.subn(rf"\g<1>{source_host}\2", sql_content)
    rewritten, snk_count = sink_pattern.subn(rf"\g<1>{sink_host}\2", rewritten)
    return rewritten, src_count, snk_count


def _resolve_capsys_python() -> str:
    """Return a Python interpreter that can run the CAPSys scripts locally."""
    candidates = [
        sys.executable,
        str(Path("venv/bin/python")),
        str(Path(".venv/bin/python")),
        "python3",
    ]
    probe = "import networkx, numpy, pandas, requests"
    seen: set[str] = set()

    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)

        candidate_path = Path(candidate)
        if candidate not in ("python3",) and not candidate_path.exists():
            continue

        result = subprocess.run(
            [candidate, "-c", probe],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return candidate

    raise RuntimeError(
        "No local Python interpreter with CAPSys dependencies found. "
        "Install the CLI extras/dependencies and ensure numpy, pandas, requests, "
        "and networkx are available."
    )


async def _run_local_command(cmd: list[str], *, cwd: Optional[Path] = None) -> None:
    """Run a local command and stream its output to the terminal."""
    print(f"  Running local command: {shlex.join(cmd)}")
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(cwd) if cwd is not None else None,
    )
    rc = await proc.wait()
    if rc != 0:
        raise RuntimeError(f"Local command failed (exit {rc}): {shlex.join(cmd)}")


def _capsys_local_run_dirs(config_path: Path) -> list[Path]:
    config_key = config_path.name.replace(".", "")
    return [
        Path(f"{config_key}_profile_iter0"),
        Path(f"{config_key}_custom_iter1"),
    ]


def _clear_local_capsys_artifacts(config_path: Path, output_name: str) -> None:
    for run_dir in _capsys_local_run_dirs(config_path):
        if run_dir.exists():
            shutil.rmtree(run_dir)

    output_file = CAPSYS_DIR / f"schedulercfg_{output_name}"
    if output_file.exists():
        output_file.unlink()


def _patch_capsys_config(
    config_path: Path,
    *,
    worker_ips: list[str],
    workers_slot: int,
    jm_host: str,
    jm_port: int,
    prometheus_port: int,
) -> None:
    config = _json.loads(config_path.read_text())
    config["iplist"] = worker_ips
    config["workers_slot"] = workers_slot
    config["jmip"] = jm_host
    config["jmpt"] = jm_port
    config["prometheus_port"] = prometheus_port
    config_path.write_text(_json.dumps(config, indent=2) + "\n")


def _capsys_schedulercfg_path(query: str, topology_name: str, rep_index: int) -> Path:
    return CAPSYS_DIR / f"schedulercfg_{query}_{topology_name}_{rep_index}"


async def _stage_capsys_schedulercfg(
    *,
    snk_conn: asyncssh.SSHClientConnection,
    snk_home: str,
    query: str,
    topology_name: str,
    rep_index: int,
) -> None:
    local_cfg = _capsys_schedulercfg_path(query, topology_name, rep_index)
    if not local_cfg.exists():
        raise FileNotFoundError(
            "CAPSYS schedulercfg not found for experiment repetition: "
            f"{local_cfg}. Run 'sim profile' first for this query/topology/repetition."
        )

    remote_cfg = f"{snk_home}/flinke2c-conf/schedulercfg"
    await _upload_local_text_file(snk_conn, local_cfg, remote_cfg)
    print(f"  Staged CAPSYS placement: {local_cfg.name} -> {remote_cfg}")


# "DETERMINISTIC" is a sim-CLI-level placement_method, not a Flink one: on the
# wire it's sent to Flink as "CAPSYS" (see _flink_wire_placement_method) so
# the custom flinke2c scheduler reads cluster.capsys.scheduler-cfg.path same
# as real CAPSYS does. The only difference is which local schedulercfg file
# gets staged - a single reproducible one from capsys/deterministic/ (see
# capsys/generate_deterministic_placement.py) instead of a DFS-searched,
# per-repetition one from capsys/.
DETERMINISTIC_SCHEDULERCFG_DIR = CAPSYS_DIR / "deterministic"


def _flink_wire_placement_method(placement_method: str) -> str:
    return "CAPSYS" if placement_method.strip().upper() == "DETERMINISTIC" else placement_method


def _deterministic_schedulercfg_path(query: str, topology_name: str) -> Path:
    return DETERMINISTIC_SCHEDULERCFG_DIR / f"schedulercfg_{query}_{topology_name}"


async def _stage_deterministic_schedulercfg(
    *,
    snk_conn: asyncssh.SSHClientConnection,
    snk_home: str,
    query: str,
    topology_name: str,
) -> None:
    local_cfg = _deterministic_schedulercfg_path(query, topology_name)
    if not local_cfg.exists():
        raise FileNotFoundError(
            f"Deterministic schedulercfg not found: {local_cfg}. Generate it with "
            f"'python3 capsys/generate_deterministic_placement.py --topology {topology_name} "
            f"--queries {query}' first."
        )

    remote_cfg = f"{snk_home}/flinke2c-conf/schedulercfg"
    await _upload_local_text_file(snk_conn, local_cfg, remote_cfg)
    print(f"  Staged deterministic placement: {local_cfg.name} -> {remote_cfg}")


@asynccontextmanager
async def _forward_local_port(
    conn: asyncssh.SSHClientConnection,
    *,
    remote_host: str,
    remote_port: int,
):
    """Forward a remote TCP port to an ephemeral local port."""
    listener = await conn.forward_local_port("127.0.0.1", 0, remote_host, remote_port)
    try:
        yield listener.get_port()
    finally:
        listener.close()
        await listener.wait_closed()


# ── Per-experiment logic ───────────────────────────────────────────────────────

async def _run_flink_repetition(
    *,
    rep: int,
    total_reps: int,
    exp: ExperimentSpec,
    src_conn: asyncssh.SSHClientConnection,
    snk_conn: asyncssh.SSHClientConnection,
    snk_home: str,
    bid_name: str,
    sink_name: str,
    combined_sql: str,
    lib_jars: list[Path],
    topology_name: str,
) -> None:
    """Submit the SQL query and wait for sink completion and Flink job finish.
    Other containers are already running for the whole experiment; only the
    sql-client is started here."""
    rep_start = int(time.time())
    print(f"\n--- Repetition {rep}/{total_reps} ---")
    sql_name = f"flink-sql-{exp.name}-r{rep}-{rep_start}"
    existing_job_ids = {
        str(j.get("id"))
        for j in await _flink_list_jobs(snk_conn)
        if isinstance(j, dict) and j.get("id")
    }
    max_runtime_s = (
        exp.max_query_runtime / 1000.0
        if exp.max_query_runtime is not None
        else None
    )

    sql_lib_mounts = " ".join(
        f"-v {snk_home}/flinke2c-lib/{j.name}:/opt/flink/lib/{j.name}:ro"
        for j in lib_jars
    )

    placement = exp.placement_method.strip().upper()
    if placement == "CAPSYS":
        await _stage_capsys_schedulercfg(
            snk_conn=snk_conn,
            snk_home=snk_home,
            query=exp.query,
            topology_name=topology_name,
            rep_index=rep - 1,
        )
    elif placement == "DETERMINISTIC":
        await _stage_deterministic_schedulercfg(
            snk_conn=snk_conn,
            snk_home=snk_home,
            query=exp.query,
            topology_name=topology_name,
        )

    # ── Submit SQL query ───────────────────────────────────────────────────
    print(f"  Submitting query '{exp.query}'...")
    await _upload_text(snk_conn, combined_sql, f"{snk_home}/flink_query.sql")
    await _run(snk_conn, " ".join(filter(None, [
        "docker run --privileged -d --rm --network=host",
        f"--name {sql_name}",
        f"-v {snk_home}/flinke2c-conf:/conf/",
        f"-v {snk_home}/flink_query.sql:/tmp/flink_query.sql:ro",
        sql_lib_mounts,
        FLINK_IMAGE,
        "sql-client embedded -f /tmp/flink_query.sql",
    ])))

    # ── Wait for completion ────────────────────────────────────────────────
    job_id = await _wait_flink_job_running(snk_conn, existing_job_ids)
    print(f"  Waiting for repetition to finish ('{DONE_SIGNAL}')...")
    sink_task = asyncio.create_task(_wait_sink_done(snk_conn, sink_name, since=rep_start))
    job_task = asyncio.create_task(_wait_flink_job_terminal(snk_conn, job_id))

    try:
        if max_runtime_s is None:
            await asyncio.gather(sink_task, job_task)
            print(f"  Repetition {rep} complete.")
            return

        done, pending = await asyncio.wait(
            {sink_task, job_task},
            timeout=max_runtime_s,
            return_when=asyncio.ALL_COMPLETED,
        )
        if not pending:
            print(f"  Repetition {rep} complete.")
            return

        print(
            f"  Max query runtime reached after {exp.max_query_runtime} ms; "
            f"cancelling Flink job {job_id} via REST..."
        )
        await _cancel_flink_job(snk_conn, job_id)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        try:
            await _wait_sink_done(snk_conn, sink_name, since=rep_start, timeout=30)
        except TimeoutError:
            print(
                f"  Sink did not emit '{DONE_SIGNAL}' within 30s after cancellation; "
                "continuing with experiment teardown."
            )
        print(f"  Repetition {rep} reached max runtime and was canceled.")
    finally:
        if not sink_task.done():
            sink_task.cancel()
        if not job_task.done():
            job_task.cancel()
        await asyncio.gather(sink_task, job_task, return_exceptions=True)


async def _run_flink_experiment(
    exp: ExperimentSpec,
    topology_file: str,
    graph: nx.Graph,
    src: NodeInfo,
    src_conn: asyncssh.SSHClientConnection,
    src_home: str,
    snk_conn: asyncssh.SSHClientConnection,
    snk_home: str,
    snk: NodeInfo,
    worker_nodes: list[NodeInfo],
    worker_conns: dict[str, asyncssh.SSHClientConnection],
    worker_homes: dict[str, str],
    lib_jars: list[Path],
    qcfg: dict,
    output_dir: Path,
    attempt: int = 1,
    start_with_rep: Optional[int] = None,
    latency: bool = True,
    skip_log_download_on_retryable_failure: bool = False,
) -> None:
    per_query  = qcfg.get(exp.query, {})
    task_slots = (
        exp.num_task_slots
        if exp.num_task_slots is not None
        else per_query.get("num_task_slots", 1)
    )
    bid_extra = (
        exp.bid_src_extra_arg
        if exp.bid_src_extra_arg is not None
        else per_query.get("bid_src_extra_arg", "")
    )
    topology_name = Path(topology_file).stem

    # Load SQL
    query_sql_path = QUERIES_DIR / f"{exp.query}.sql"
    setup_sql_path = QUERIES_DIR / (
        "setup_nes.sql" if exp.source_schema == "nes" else "setup.sql"
    )
    if not query_sql_path.exists():
        raise FileNotFoundError(f"Query file not found: {query_sql_path}")
    combined_sql = ""
    if setup_sql_path.exists():
        combined_sql += setup_sql_path.read_text() + "\n"
    combined_sql += query_sql_path.read_text()
    combined_sql, src_rewrites, snk_rewrites = _rewrite_sql_tcp_hosts(
        combined_sql, source_host=src.address, sink_host=snk.address
    )
    if src_rewrites or snk_rewrites:
        print(
            "  Rewrote SQL TCP connector hosts "
            f"(sources={src_rewrites} -> {src.address}, sinks={snk_rewrites} -> {snk.address})"
        )

    # Resolve graphml: explicit graphml_file > topology-stem static file > dynamic
    graphml_content:  Optional[str] = None
    graphml_filename: str = ""
    if exp.placement_method:
        if exp.graphml_file:
            static_path = CONFIGS_DIR / "coordinator" / exp.graphml_file
        else:
            topo_stem   = Path(topology_file).stem
            static_path = CONFIGS_DIR / "coordinator" / f"{topo_stem}.graphml"
        if static_path.exists():
            graphml_content  = static_path.read_text()
            graphml_content, slot_updates = _rewrite_graphml_slots(graphml_content, task_slots)
            graphml_filename = static_path.name
            print(
                f"  Using static graphml: {static_path} "
                f"(set {slot_updates} slot entry/entries to {task_slots})"
            )
        else:
            graphml_content  = generate_graphml(graph, src.id, snk.id, task_slots)
            graphml_filename = "topology.graphml"
            print(f"  Generating graphml dynamically (no static file found: {static_path})")

    graphml_path    = f"/conf/{graphml_filename}" if graphml_filename else ""
    use_rocksdb     = exp.flink_rocksdb if exp.flink_rocksdb is not None else True
    coordinator_cfg = _coordinator_config(
        snk.address, _flink_wire_placement_method(exp.placement_method),
        graphml_path, snk.instance_type, use_rocksdb
    )
    worker_cfgs     = {
        wn.id: _worker_config(snk.address, wn.address, task_slots, wn.instance_type)
        for wn in worker_nodes
    }

    # Experiment-scoped container names (shared across all repetitions)
    exp_id       = f"{exp.name}-{int(time.time())}"
    bid_name     = f"tcp-bid-{exp_id}"
    auction_name = f"tcp-auction-{exp_id}"
    person_name  = f"tcp-person-{exp_id}"
    sink_name    = f"tcp-sink-{exp_id}"
    jm_name      = f"flink-jm-{exp_id}"
    tm_names     = {wn.id: f"flink-tm-{wn.id}-{exp_id}" for wn in worker_nodes}

    # exp.system is always "flink" here; source_schema separately controls what
    # field set the TCP source generator emits, so a "flink" run can be told to
    # emit the NES-compatible (trimmed) schema for a fair flink-vs-nes comparison.
    system_flag = f"--system {exp.source_schema}" if exp.source_schema else ""
    start_with_rep_arg = (
        f"--start-with-rep {start_with_rep}" if start_with_rep is not None else ""
    )
    latency_arg = "--latency" if latency else ""

    # Kill all containers on each node (dedicated experiment nodes).
    _kill_all = "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true"
    await _run(src_conn, _kill_all, check=False)
    await _run(snk_conn, _kill_all, check=False)
    for wn in worker_nodes:
        await _run(worker_conns[wn.id], _kill_all, check=False)

    # Clear log contents before starting (sudo needed: container writes as root)
    print("  Clearing remote logs...")
    clear_jobs = [
        _clear_remote_path(src_conn, f"{src_home}/logs/"),
        _clear_remote_path(snk_conn, f"{snk_home}/logs/"),
    ]
    clear_jobs.extend(
        _clear_remote_path(worker_conns[wn.id], f"{worker_homes[wn.id]}/logs/")
        for wn in worker_nodes
    )
    await asyncio.gather(*clear_jobs)
    verify_jobs = [
        _assert_remote_dir_empty(src_conn, f"{src_home}/logs/"),
        _assert_remote_dir_empty(snk_conn, f"{snk_home}/logs/"),
    ]
    verify_jobs.extend(
        _assert_remote_dir_empty(worker_conns[wn.id], f"{worker_homes[wn.id]}/logs/")
        for wn in worker_nodes
    )
    await asyncio.gather(*verify_jobs)

    worker_containers = {wn.id: [tm_names[wn.id]] for wn in worker_nodes}

    download_logs = True
    retryable_failure = False
    try:
        # Upload Flink configs once
        print("  Uploading Flink configs...")
        await _upload_text(snk_conn, coordinator_cfg, f"{snk_home}/flinke2c-conf/config.yaml")
        for wn in worker_nodes:
            await _upload_text(
                worker_conns[wn.id], worker_cfgs[wn.id],
                f"{worker_homes[wn.id]}/flinke2c-conf/config.yaml",
            )

        if exp.placement_method and graphml_content is not None:
            print(f"  Uploading graphml ({graphml_filename})...")
            await _upload_text(
                snk_conn, graphml_content,
                f"{snk_home}/flinke2c-conf/{graphml_filename}",
            )

        # Start all containers — they stay up for every repetition
        print("  Starting bid source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {bid_name}",
            f"-v {src_home}/logs/bids:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/bid_events.parquet",
            f"--address 0.0.0.0:10000 {system_flag} --schema bid --exp-name {exp.name}",
            start_with_rep_arg,
            latency_arg,
            bid_extra,
        ])).strip())
        await _assert_running(src_conn, bid_name)

        print("  Starting auction source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {auction_name}",
            f"-v {src_home}/logs/auctions:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/auction_events.parquet",
            f"--address 0.0.0.0:10001 {system_flag} --schema auction --exp-name {exp.name}",
            start_with_rep_arg,
            latency_arg,
        ])))
        await _assert_running(src_conn, auction_name)

        print("  Starting person source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {person_name}",
            f"-v {src_home}/logs/persons:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/person_events.parquet",
            f"--address 0.0.0.0:10002 {system_flag} --schema person --exp-name {exp.name}",
            start_with_rep_arg,
            latency_arg,
        ])))
        await _assert_running(src_conn, person_name)

        print("  Starting sink...")
        await _run(snk_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {sink_name}",
            f"-v {snk_home}/logs:/opt/tcp/logs",
            TCP_IMAGE,
            f"sink --exp-name {exp.name}",
            start_with_rep_arg,
            latency_arg,
        ])))
        await _assert_running(snk_conn, sink_name)

        jm_lib_mounts = " ".join(
            f"-v {snk_home}/flinke2c-lib/{j.name}:/opt/flink/lib/{j.name}:ro"
            for j in lib_jars
        )
        print("  Starting Flink jobmanager...")
        await _run(snk_conn, " ".join(filter(None, [
            "docker run --privileged -d --network=host",
            f"--name {jm_name}",
            f"-v {snk_home}/flinke2c-conf:/conf/",
            jm_lib_mounts,
            FLINK_IMAGE,
            "jobmanager",
        ])))
        await _assert_running(snk_conn, jm_name)
        print(f"  Waiting {FLINK_JOBMANAGER_START_DELAY}s before starting taskmanagers...")
        await asyncio.sleep(FLINK_JOBMANAGER_START_DELAY)

        print(f"  Starting {len(worker_nodes)} taskmanager(s)...")
        async def _start_taskmanager(wn: NodeInfo) -> None:
            wh = worker_homes[wn.id]
            tm_lib_mounts = " ".join(
                f"-v {wh}/flinke2c-lib/{j.name}:/opt/flink/lib/{j.name}:ro"
                for j in lib_jars
            )
            await _run(worker_conns[wn.id], " ".join(filter(None, [
                "docker run --privileged -d --network=host",
                f"--name {tm_names[wn.id]}",
                _cpus_flag(wn),
                _worker_memory_flag(wn),
                f"-v {wh}/flinke2c-conf:/conf/",
                tm_lib_mounts,
                FLINK_IMAGE,
                "taskmanager",
            ])))
            await _assert_running(worker_conns[wn.id], tm_names[wn.id])

        await asyncio.gather(*(_start_taskmanager(wn) for wn in worker_nodes))

        # Wait for sources to finish loading and for the cluster to form
        print(f"  Waiting for TCP sources to report ready ('{READY_SIGNAL}')...")
        await asyncio.gather(
            _poll_for_pattern(src_conn, bid_name, READY_SIGNAL,
                              timeout=READY_TIMEOUT, label=bid_name),
            _poll_for_pattern(src_conn, auction_name, READY_SIGNAL,
                              timeout=READY_TIMEOUT, label=auction_name),
            _poll_for_pattern(src_conn, person_name, READY_SIGNAL,
                              timeout=READY_TIMEOUT, label=person_name),
        )
        print("  Sources are ready.")
        print(f"  Waiting for Flink REST to report {len(worker_nodes)} taskmanager(s)...")
        await _wait_flink_taskmanagers_ready(
            snk_conn=snk_conn,
            jm_name=jm_name,
            expected_count=len(worker_nodes),
        )
        print("  Flink cluster is ready.")

        for rep in range(1, exp.repetitions + 1):
            await _run_flink_repetition(
                rep=rep,
                total_reps=exp.repetitions,
                exp=exp,
                src_conn=src_conn,
                snk_conn=snk_conn,
                snk_home=snk_home,
                bid_name=bid_name,
                sink_name=sink_name,
                combined_sql=combined_sql,
                lib_jars=lib_jars,
                topology_name=topology_name,
            )
    except FlinkJobRetryableError:
        retryable_failure = True
        raise
    finally:
        print("  Stopping containers...")
        # TCP streaming containers: send 'q', wait 5 s, then force-remove
        await _graceful_stop_tcp(src_conn, [bid_name, auction_name, person_name])
        await _graceful_stop_tcp(snk_conn, [sink_name])
        # Flink containers: regular stop
        await _stop_containers(snk_conn, [jm_name])
        for wn in worker_nodes:
            await _stop_containers(worker_conns[wn.id], worker_containers[wn.id])

        download_target = output_dir
        if retryable_failure and skip_log_download_on_retryable_failure:
            download_target = _retry_attempt_output_dir(output_dir, attempt)
            print(
                "  Retryable failure detected; downloading preliminary results to "
                f"{download_target} before retry."
            )

        if not download_logs:
            print("  Skipping log download.")
        else:
            # Always download logs — even if a repetition failed.
            print("\n  Preprocessing remote latency logs before download...")
            await asyncio.gather(
                _run_remote_latency_preprocessing(
                    src_conn,
                    node_label=src.id,
                    node_home=src_home,
                    remote_logs=f"{src_home}/logs",
                    remote_user=src.user,
                ),
                _run_remote_latency_preprocessing(
                    snk_conn,
                    node_label=snk.id,
                    node_home=snk_home,
                    remote_logs=f"{snk_home}/logs",
                    remote_user=snk.user,
                ),
            )

            print(f"\n  Downloading logs to {download_target}/...")
            if download_target.exists():
                shutil.rmtree(download_target)
            download_target.mkdir(parents=True, exist_ok=True)
            await _download_logs(src_conn, f"{src_home}/logs", download_target / src.id)
            await _download_logs(snk_conn, f"{snk_home}/logs", download_target / snk.id)
            print(f"  Logs saved to {download_target}")


async def _run_flink_profile_query(
    *,
    exp: ExperimentSpec,
    topology_file: str,
    graph: nx.Graph,
    src: NodeInfo,
    src_conn: asyncssh.SSHClientConnection,
    src_home: str,
    snk: NodeInfo,
    snk_conn: asyncssh.SSHClientConnection,
    snk_home: str,
    worker_nodes: list[NodeInfo],
    worker_conns: dict[str, asyncssh.SSHClientConnection],
    worker_homes: dict[str, str],
    lib_jars: list[Path],
    qcfg: dict,
    capsys_python: str,
    latency: bool = True,
) -> None:
    per_query = qcfg.get(exp.query, {})
    task_slots = (
        exp.num_task_slots
        if exp.num_task_slots is not None
        else per_query.get("num_task_slots", 1)
    )
    bid_extra = (
        exp.bid_src_extra_arg
        if exp.bid_src_extra_arg is not None
        else per_query.get("bid_src_extra_arg", "")
    )
    topology_name = Path(topology_file).stem
    base_output_name = f"{exp.query}_{topology_name}"
    profile_placement_method = ""

    query_sql_path = QUERIES_DIR / f"{exp.query}.sql"
    setup_sql_path = QUERIES_DIR / "setup.sql"
    if not query_sql_path.exists():
        raise FileNotFoundError(f"Query file not found: {query_sql_path}")

    combined_sql = ""
    if setup_sql_path.exists():
        combined_sql += setup_sql_path.read_text() + "\n"
    combined_sql += query_sql_path.read_text()
    combined_sql, src_rewrites, snk_rewrites = _rewrite_sql_tcp_hosts(
        combined_sql, source_host=src.address, sink_host=snk.address
    )
    if src_rewrites or snk_rewrites:
        print(
            "  Rewrote SQL TCP connector hosts "
            f"(sources={src_rewrites} -> {src.address}, sinks={snk_rewrites} -> {snk.address})"
        )

    if exp.placement_method:
        print(
            f"  Ignoring placement_method={exp.placement_method!r} during profiling; "
            "CAPSys profiling runs without Flink placement enabled."
        )

    graphml_content: Optional[str] = None
    graphml_filename: str = ""

    graphml_path = f"/conf/{graphml_filename}" if graphml_filename else ""
    use_rocksdb = exp.flink_rocksdb if exp.flink_rocksdb is not None else True
    coordinator_cfg = _coordinator_config(
        snk.address, profile_placement_method, graphml_path, snk.instance_type, use_rocksdb
    )
    worker_cfgs = {
        wn.id: _worker_config(snk.address, wn.address, task_slots, wn.instance_type)
        for wn in worker_nodes
    }

    prom_remote_dir = f"{snk_home}/prometheus"
    prom_remote_cfg = f"{prom_remote_dir}/prometheus.yml"
    latency_arg = "--latency" if latency else ""
    _kill_all = "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true"
    exp_id = f"profile-{exp.query}-{int(time.time())}"
    bid_name = f"tcp-bid-{exp_id}"
    auction_name = f"tcp-auction-{exp_id}"
    person_name = f"tcp-person-{exp_id}"
    sink_name = f"tcp-sink-{exp_id}"
    jm_name = f"flink-jm-{exp_id}"
    prom_name = f"prometheus-{exp_id}"
    tm_names = {wn.id: f"flink-tm-{wn.id}-{exp_id}" for wn in worker_nodes}
    worker_containers = {wn.id: [tm_names[wn.id]] for wn in worker_nodes}

    await _run(src_conn, _kill_all, check=False)
    await _run(snk_conn, _kill_all, check=False)
    for wn in worker_nodes:
        await _run(worker_conns[wn.id], _kill_all, check=False)

    print("  Clearing remote logs...")
    clear_jobs = [
        _clear_remote_path(src_conn, f"{src_home}/logs/"),
        _clear_remote_path(snk_conn, f"{snk_home}/logs/"),
    ]
    clear_jobs.extend(
        _clear_remote_path(worker_conns[wn.id], f"{worker_homes[wn.id]}/logs/")
        for wn in worker_nodes
    )
    await asyncio.gather(*clear_jobs)
    verify_jobs = [
        _assert_remote_dir_empty(src_conn, f"{src_home}/logs/"),
        _assert_remote_dir_empty(snk_conn, f"{snk_home}/logs/"),
    ]
    verify_jobs.extend(
        _assert_remote_dir_empty(worker_conns[wn.id], f"{worker_homes[wn.id]}/logs/")
        for wn in worker_nodes
    )
    await asyncio.gather(*verify_jobs)

    try:
        print("  Uploading Flink and Prometheus configs...")
        await _upload_text(snk_conn, coordinator_cfg, f"{snk_home}/flinke2c-conf/config.yaml")
        await _ensure_remote_dir(snk_conn, prom_remote_dir)
        await _upload_local_text_file(snk_conn, PROMETHEUS_CONFIG_FILE, prom_remote_cfg)
        for wn in worker_nodes:
            await _upload_text(
                worker_conns[wn.id], worker_cfgs[wn.id],
                f"{worker_homes[wn.id]}/flinke2c-conf/config.yaml",
            )

        if profile_placement_method and graphml_content is not None:
            print(f"  Uploading graphml ({graphml_filename})...")
            await _upload_text(
                snk_conn, graphml_content,
                f"{snk_home}/flinke2c-conf/{graphml_filename}",
            )

        print("  Starting bid source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {bid_name}",
            f"-v {src_home}/logs/bids:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/bid_events.parquet",
            f"--address 0.0.0.0:10000 --schema bid --exp-name {exp.name}",
            f"--rate {CAPSYS_SOURCE_RATE}",
            latency_arg,
            bid_extra,
        ])).strip())
        await _assert_running(src_conn, bid_name)

        print("  Starting auction source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {auction_name}",
            f"-v {src_home}/logs/auctions:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/auction_events.parquet",
            f"--address 0.0.0.0:10001 --schema auction --exp-name {exp.name}",
            f"--rate {CAPSYS_SOURCE_RATE}",
            latency_arg,
        ])))
        await _assert_running(src_conn, auction_name)

        print("  Starting person source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {person_name}",
            f"-v {src_home}/logs/persons:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/person_events.parquet",
            f"--address 0.0.0.0:10002 --schema person --exp-name {exp.name}",
            f"--rate {CAPSYS_SOURCE_RATE}",
            latency_arg,
        ])))
        await _assert_running(src_conn, person_name)

        print("  Starting sink...")
        await _run(snk_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {sink_name}",
            f"-v {snk_home}/logs:/opt/tcp/logs",
            TCP_IMAGE,
            f"sink --exp-name {exp.name}",
            latency_arg,
        ])))
        await _assert_running(snk_conn, sink_name)

        jm_lib_mounts = " ".join(
            f"-v {snk_home}/flinke2c-lib/{j.name}:/opt/flink/lib/{j.name}:ro"
            for j in lib_jars
        )
        print("  Starting Flink jobmanager...")
        await _run(snk_conn, " ".join(filter(None, [
            "docker run --privileged -d --network=host",
            f"--name {jm_name}",
            f"-v {snk_home}/flinke2c-conf:/conf/",
            jm_lib_mounts,
            FLINK_IMAGE,
            "jobmanager",
        ])))
        await _assert_running(snk_conn, jm_name)
        print(f"  Waiting {FLINK_JOBMANAGER_START_DELAY}s before starting taskmanagers...")
        await asyncio.sleep(FLINK_JOBMANAGER_START_DELAY)

        print(f"  Starting {len(worker_nodes)} taskmanager(s)...")

        async def _start_taskmanager(wn: NodeInfo) -> None:
            wh = worker_homes[wn.id]
            tm_lib_mounts = " ".join(
                f"-v {wh}/flinke2c-lib/{j.name}:/opt/flink/lib/{j.name}:ro"
                for j in lib_jars
            )
            await _run(worker_conns[wn.id], " ".join(filter(None, [
                "docker run --privileged -d --network=host",
                f"--name {tm_names[wn.id]}",
                _cpus_flag(wn),
                _worker_memory_flag(wn),
                f"-v {wh}/flinke2c-conf:/conf/",
                tm_lib_mounts,
                FLINK_IMAGE,
                "taskmanager",
            ])))
            await _assert_running(worker_conns[wn.id], tm_names[wn.id])

        await asyncio.gather(*(_start_taskmanager(wn) for wn in worker_nodes))

        print("  Starting Prometheus...")
        await _run(snk_conn, " ".join([
            "docker run -d --network=host",
            f"--name {prom_name}",
            f"-v {prom_remote_cfg}:/etc/prometheus/prometheus.yml:ro",
            PROMETHEUS_IMAGE,
            "--config.file=/etc/prometheus/prometheus.yml",
            "--web.listen-address=0.0.0.0:9090",
        ]))
        await _assert_running(snk_conn, prom_name)

        print(f"  Waiting for TCP sources to report ready ('{READY_SIGNAL}')...")
        await asyncio.gather(
            _poll_for_pattern(src_conn, bid_name, READY_SIGNAL, timeout=READY_TIMEOUT, label=bid_name),
            _poll_for_pattern(src_conn, auction_name, READY_SIGNAL, timeout=READY_TIMEOUT, label=auction_name),
            _poll_for_pattern(src_conn, person_name, READY_SIGNAL, timeout=READY_TIMEOUT, label=person_name),
        )
        print("  Sources are ready.")
        print(f"  Waiting for Flink REST to report {len(worker_nodes)} taskmanager(s)...")
        await _wait_flink_taskmanagers_ready(
            snk_conn=snk_conn,
            jm_name=jm_name,
            expected_count=len(worker_nodes),
        )
        print("  Flink cluster is ready.")

        for rep in range(exp.repetitions):
            output_name = f"{base_output_name}_{rep}"
            _clear_local_capsys_artifacts(CAPSYS_CONFIG_OUTPUT, output_name)
            print(f"\n  --- Repetition {rep + 1}/{exp.repetitions} ---")

            existing_job_ids = {
                str(j.get("id"))
                for j in await _flink_list_jobs(snk_conn)
                if isinstance(j, dict) and j.get("id")
            }
            sql_name = f"flink-sql-profile-{exp.query}-rep{rep}-{int(time.time())}"
            sql_lib_mounts = " ".join(
                f"-v {snk_home}/flinke2c-lib/{j.name}:/opt/flink/lib/{j.name}:ro"
                for j in lib_jars
            )
            print(f"  Submitting query '{exp.query}' for CAPSys profiling...")
            await _upload_text(snk_conn, combined_sql, f"{snk_home}/flink_query.sql")
            await _run(snk_conn, " ".join(filter(None, [
                "docker run --privileged -d --rm --network=host",
                f"--name {sql_name}",
                f"-v {snk_home}/flinke2c-conf:/conf/",
                f"-v {snk_home}/flink_query.sql:/tmp/flink_query.sql:ro",
                sql_lib_mounts,
                FLINK_IMAGE,
                "sql-client embedded -f /tmp/flink_query.sql",
            ])))

            job_id = await _wait_flink_job_running(snk_conn, existing_job_ids)
            print(f"  Profiling Flink job {job_id}...")

            async with _forward_local_port(
                snk_conn, remote_host="127.0.0.1", remote_port=8081
            ) as local_rest_port, _forward_local_port(
                snk_conn, remote_host="127.0.0.1", remote_port=9090
            ) as local_prom_port:
                await _run_local_command([
                    capsys_python,
                    str(CAPSYS_GENERATOR_SCRIPT),
                    "--template",
                    str(CAPSYS_CONFIG_TEMPLATE),
                    "--output",
                    str(CAPSYS_CONFIG_OUTPUT),
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(local_rest_port),
                    "--job-id",
                    job_id,
                    "--source-rate",
                    str(CAPSYS_SOURCE_RATE),
                ])
                _patch_capsys_config(
                    CAPSYS_CONFIG_OUTPUT,
                    worker_ips=[wn.address for wn in worker_nodes],
                    workers_slot=task_slots,
                    jm_host="127.0.0.1",
                    jm_port=local_rest_port,
                    prometheus_port=local_prom_port,
                )
                await _run_local_command([
                    capsys_python,
                    str(CAPSYS_RUNDS_SCRIPT),
                    str(CAPSYS_CONFIG_OUTPUT),
                    "attach",
                    "profile",
                    "0",
                    "custom",
                ])

            print(f"  Cancelling Flink job {job_id}...")
            await _cancel_flink_job(snk_conn, job_id)
            await _run_local_command([
                capsys_python,
                str(CAPSYS_RUNDS_SCRIPT),
                str(CAPSYS_CONFIG_OUTPUT),
                "plan",
                "custom",
                "1",
                "custom",
                output_name,
            ])
            print(f"  Generated {CAPSYS_DIR / ('schedulercfg_' + output_name)}")
    finally:
        print("  Stopping containers...")
        await _graceful_stop_tcp(src_conn, [bid_name, auction_name, person_name])
        await _graceful_stop_tcp(snk_conn, [sink_name])
        await _stop_containers(snk_conn, [jm_name, prom_name])
        for wn in worker_nodes:
            await _stop_containers(worker_conns[wn.id], worker_containers[wn.id])


# ── NES experiment logic ──────────────────────────────────────────────────────

# Maps placement_method (shared with the Flink "TOP_DOWN"-style convention) to
# the exact NES::Optimizer::PlacementStrategy enum spelling expected by the
# /execute-query REST endpoint's "placement" field. "" (unset) defaults to
# TopDown, NES's own default strategy.
NES_PLACEMENT_STRATEGY_MAP = {
    "": "TopDown",
    "TOP_DOWN": "TopDown",
    "BOTTOM_UP": "BottomUp",
    "IFCOP": "IFCOP",
    "ILP": "ILP",
    "ML_HEURISTIC": "MlHeuristic",
    "ELEGANT_PERFORMANCE": "ELEGANT_PERFORMANCE",
    "ELEGANT_ENERGY": "ELEGANT_ENERGY",
    "ELEGANT_BALANCED": "ELEGANT_BALANCED",
}

# QueryState values (nebulastream-private nes-common/include/Util/QueryState.hpp)
# a query can still be in before it's actually running, and terminal states
# that mean it never will be.
_NES_PRE_RUNNING_STATES = {"REGISTERED", "OPTIMIZING"}
_NES_TERMINAL_FAILURE_STATES = {"STOPPED", "MARKED_FOR_FAILURE", "FAILED"}


async def _wait_nes_query_running(
    snk_conn: asyncssh.SSHClientConnection,
    query_id: int,
    timeout: float = NES_QUERY_PLACEMENT_TIMEOUT,
) -> None:
    """Poll NES's /query-status until the submitted query leaves REGISTERED/OPTIMIZING.

    Needed because AddQueryRequest.cpp never awaits its placement amendment's
    future (unlike Stop/FailQueryRequest): a genuine placement failure (e.g.
    nes_num_slots too tight) resolves that future to false internally, but
    nothing reads it, so status just stays OPTIMIZING forever with no error.
    Without this poll, a query that never started would silently run out the
    full sink-done timeout instead.
    """
    deadline = time.monotonic() + timeout
    status = "UNKNOWN"
    status_cmd = (
        f"curl -sS http://127.0.0.1:{NES_REST_PORT}"
        f"/v1/nes/query/query-status?queryId={query_id}"
    )
    while time.monotonic() < deadline:
        rest = await snk_conn.run(status_cmd, check=False)
        if rest.exit_status == 0 and rest.stdout:
            try:
                payload = _json.loads(rest.stdout)
            except _json.JSONDecodeError:
                payload = {}
            if isinstance(payload, dict) and "status" in payload:
                status = payload["status"]
            if status in _NES_TERMINAL_FAILURE_STATES:
                raise RuntimeError(
                    f"NES query {query_id} entered {status} before running. "
                    "This usually means placement failed (nes_num_slots / "
                    "nes_coordinator_num_slots too tight for the query's "
                    "operator count)."
                )
            if status not in _NES_PRE_RUNNING_STATES:
                return
        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"NES query {query_id} still {status!r} after {timeout:.0f}s. "
        "A placement failure never changes query status (AddQueryRequest "
        "doesn't await its placement amendment's result), so this almost "
        "always means nes_num_slots/nes_coordinator_num_slots are too "
        "tight for the query's operator count - raise them and retry."
    )


async def _stop_nes_query(snk_conn: asyncssh.SSHClientConnection, query_id: int) -> bool:
    """Explicitly stop a finished NES query via DELETE /stop-query.

    A query that finishes on its own only marks itself STOPPED for REST
    reporting; it never releases its topology slots (TopologyNode::
    releaseSlots), which only the explicit DELETE /stop-query path
    (StopQueryRequest.cpp) triggers. That path synchronously awaits the
    placement-removal amendment, so this call returning is confirmation
    enough -- no separate poll needed. Skipping this leaks slots permanently
    on a long-lived coordinator (nes_restart_after_stop works around the
    same leak differently, by restarting the whole cluster).
    """
    stop_cmd = (
        f"curl -sS -X DELETE "
        f"'http://127.0.0.1:{NES_REST_PORT}/v1/nes/query/stop-query?queryId={query_id}'"
    )
    rest = await snk_conn.run(stop_cmd, check=False)
    if rest.exit_status != 0:
        print(f"  Warning: failed to stop NES query {query_id}: {rest.stderr or rest.stdout}")
        return False
    print(f"  NES stop-query response for {query_id}: {rest.stdout}")
    return True


async def _run_nes_repetition(
    *,
    rep: int,
    total_reps: int,
    exp: ExperimentSpec,
    snk_conn: asyncssh.SSHClientConnection,
    src_conn: asyncssh.SSHClientConnection,
    bid_name: str,
    sink_name: str,
    query_str: str,
) -> None:
    rep_start = int(time.time())
    print(f"\n--- Repetition {rep}/{total_reps} ---")

    nes_placement = NES_PLACEMENT_STRATEGY_MAP.get(exp.placement_method.strip().upper(), "TopDown")
    payload = _json.dumps({"userQuery": query_str, "placement": nes_placement})
    curl_cmd = (
        f"curl -sS -X POST http://127.0.0.1:{NES_REST_PORT}/v1/nes/query/execute-query"
        f" -H 'Content-Type: application/json'"
        f" -d '{payload}'"
    )
    print(f"  Submitting NES query '{exp.query}'...")
    result = await _run(snk_conn, curl_cmd)
    print(f"  NES response: {result}")
    try:
        query_id = _json.loads(result).get("queryId")
    except _json.JSONDecodeError:
        query_id = None
    if query_id is not None:
        print(f"  Waiting for query {query_id} to leave placement (REGISTERED/OPTIMIZING)...")
        await _wait_nes_query_running(snk_conn, query_id)
    else:
        print("  Could not parse queryId from NES response; skipping placement check.")

    print(f"  Waiting for repetition to finish ('{DONE_SIGNAL}')...")
    await _wait_sink_done(snk_conn, sink_name, since=rep_start)

    if query_id is not None:
        print(f"  Stopping NES query {query_id} to release its topology slots...")
        await _stop_nes_query(snk_conn, query_id)

    print(f"  Repetition {rep} complete.")


async def _run_nes_experiment(
    exp: ExperimentSpec,
    topology_file: str,
    graph: nx.Graph,
    src: NodeInfo,
    src_conn: asyncssh.SSHClientConnection,
    src_home: str,
    snk: NodeInfo,
    snk_conn: asyncssh.SSHClientConnection,
    snk_home: str,
    worker_nodes: list[NodeInfo],
    worker_conns: dict[str, asyncssh.SSHClientConnection],
    worker_homes: dict[str, str],
    output_dir: Path,
    qcfg: dict,
    start_with_rep: Optional[int] = None,
    latency: bool = True,
) -> None:
    # Queries that aggregate/forward latency_ts (currently q4, q5) need a
    # "_nolatency" variant for --no-latency runs, since the TCP source only
    # puts latency_ts on the wire when started with --latency; q1 needs no
    # variant because its plain .map() never references the field by name.
    query_path = NES_QUERIES_DIR / (
        f"{exp.query}_nolatency.txt" if not latency else f"{exp.query}.txt"
    )
    if not query_path.exists():
        query_path = NES_QUERIES_DIR / f"{exp.query}.txt"
    if not query_path.exists():
        raise FileNotFoundError(f"NES query file not found: {query_path}")
    per_query = qcfg.get(exp.query, {})
    bid_extra = (
        exp.bid_src_extra_arg
        if exp.bid_src_extra_arg is not None
        else per_query.get("bid_src_extra_arg", "")
    )
    # experiments.*.yml (per-experiment, then its own global default) takes
    # priority; query_config.yml's per-query entry is the fallback base
    # default, same precedence as bid_src_extra_arg above.
    restart_after_stop = (
        exp.nes_restart_after_stop
        if exp.nes_restart_after_stop is not None
        else bool(per_query.get("nes_restart_after_stop", False))
    )
    query_str = _rewrite_nes_query_sink_host(
        query_path.read_text().strip(),
        snk.address,
    )

    # Build configs. nes_coordinator_num_slots tunes the coordinator's own
    # worker role (defaults to 1); nes_num_slots tunes the workers separately
    # (defaults to unlimited/None when unset).
    coordinator_cfg = _nes_coordinator_config(
        snk.address, latency, num_slots=exp.nes_coordinator_num_slots,
        instance_type=snk.instance_type,
    )
    # Workers: the worker actually adjacent to src in the topology graph gets
    # physicalSources pointing to src - not just "the first worker_nodes
    # entry", which is topology-JSON declaration order and can put the
    # source on a node the graph's own edges say it isn't connected to
    # (e.g. a cloud node that's several on-prem hops away from src on
    # paper). Wiring the source there means NES's ingestion point can skip
    # right past the topology's weaker on-prem legs entirely, so TopDown
    # placement never has a reason to route any real work through them.
    source_neighbor_ids = set(graph.neighbors(src.id)) if src.id in graph else set()
    source_worker = next(
        (wn for wn in worker_nodes if wn.id in source_neighbor_ids), None
    )
    if source_worker is None:
        # No worker_nodes entry is a direct graph-neighbor of src (e.g. src
        # connects only to a non-worker node, or an unusual topology) - fall
        # back to the prior behavior instead of silently dropping the
        # source connection.
        source_worker = worker_nodes[0] if worker_nodes else None

    worker_cfgs: dict[str, str] = {}
    for wn in worker_nodes:
        source_host = src.address if wn is source_worker else None
        worker_cfgs[wn.id] = _nes_worker_config(
            worker_host=wn.address,
            coordinator_host=snk.address,
            source_host=source_host,
            num_slots=exp.nes_num_slots,
            instance_type=wn.instance_type,
        )

    exp_id    = f"{exp.name}-{int(time.time())}"
    bid_name    = f"tcp-bid-{exp_id}"
    auc_name    = f"tcp-auction-{exp_id}"
    person_name = f"tcp-person-{exp_id}"
    nes_name    = f"nes-coord-{exp_id}"
    sink_name   = f"tcp-sink-{exp_id}"
    wn_names    = {wn.id: f"nes-worker-{wn.id}-{exp_id}" for wn in worker_nodes}
    start_with_rep_arg = (
        f"--start-with-rep {start_with_rep}" if start_with_rep is not None else ""
    )
    latency_arg = "--latency" if latency else ""

    # Kill all containers on all nodes
    _kill_all = "docker ps -aq | xargs -r docker rm -f 2>/dev/null || true"
    await _run(src_conn, _kill_all, check=False)
    await _run(snk_conn, _kill_all, check=False)
    for wn in worker_nodes:
        await _run(worker_conns[wn.id], _kill_all, check=False)

    # Clear logs (sudo: containers write as root)
    print("  Clearing remote logs...")
    clear_jobs = [
        _clear_remote_path(src_conn, f"{src_home}/logs/"),
        _clear_remote_path(snk_conn, f"{snk_home}/logs/"),
    ]
    clear_jobs.extend(
        _clear_remote_path(worker_conns[wn.id], f"{worker_homes[wn.id]}/logs/")
        for wn in worker_nodes
    )
    await asyncio.gather(*clear_jobs)
    verify_jobs = [
        _assert_remote_dir_empty(src_conn, f"{src_home}/logs/"),
        _assert_remote_dir_empty(snk_conn, f"{snk_home}/logs/"),
    ]
    verify_jobs.extend(
        _assert_remote_dir_empty(worker_conns[wn.id], f"{worker_homes[wn.id]}/logs/")
        for wn in worker_nodes
    )
    await asyncio.gather(*verify_jobs)

    try:
        # Upload NES configs
        print("  Uploading NES configs...")
        await _run(snk_conn, f"mkdir -p {snk_home}/nes-conf")
        await _upload_text(snk_conn, coordinator_cfg, f"{snk_home}/nes-conf/coordinator.yaml")
        for wn in worker_nodes:
            wh = worker_homes[wn.id]
            await _run(worker_conns[wn.id], f"mkdir -p {wh}/nes-conf", check=False)
            await _upload_text(worker_conns[wn.id], worker_cfgs[wn.id], f"{wh}/nes-conf/worker.yaml")

        print("  Starting bid source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {bid_name}",
            f"-v {src_home}/logs/bids:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/bid_events.parquet",
            f"--address 0.0.0.0:10000 --system nes --schema bid --exp-name {exp.name}",
            start_with_rep_arg,
            latency_arg,
            bid_extra,
        ])))
        await _assert_running(src_conn, bid_name)

        print("  Starting auction source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {auc_name}",
            f"-v {src_home}/logs/auctions:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/auction_events.parquet",
            f"--address 0.0.0.0:10001 --system nes --schema auction --exp-name {exp.name}",
            start_with_rep_arg,
            latency_arg,
        ])))
        await _assert_running(src_conn, auc_name)

        print("  Starting person source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {person_name}",
            f"-v {src_home}/logs/persons:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/person_events.parquet",
            f"--address 0.0.0.0:10002 --system nes --schema person --exp-name {exp.name}",
            start_with_rep_arg,
            latency_arg,
        ])))
        await _assert_running(src_conn, person_name)

        # Start TCP sink on snk
        print("  Starting sink...")
        await _run(snk_conn, " ".join(filter(None, [
            "docker run --privileged -d -i --init --network=host",
            f"--name {sink_name}",
            f"-v {snk_home}/logs:/opt/tcp/logs",
            TCP_IMAGE,
            f"sink --exp-name {exp.name}",
            start_with_rep_arg,
            latency_arg,
        ])))
        await _assert_running(snk_conn, sink_name)

        # Start NES coordinator on snk
        async def _start_nes_coordinator() -> None:
            print("  Starting NES coordinator...")
            await _run(snk_conn, " ".join([
                "docker run --privileged -d --init --network=host",
                f"--name {nes_name}",
                f"-v {snk_home}/nes-conf/coordinator.yaml:/config.yaml",
                NES_COORDINATOR_IMAGE,
            ]))
            await _assert_running(snk_conn, nes_name)

        # Start NES workers, then wait for them to register and reconcile the
        # topology. Split from _start_nes_coordinator (rather than one
        # combined "start cluster" step) because the initial startup path
        # needs to wait for the TCP sources to be ready in between the two -
        # a mid-experiment restart (nes_restart_after_stop) doesn't, since
        # the sources keep running across repetitions.
        expected_nes_nodes = len(worker_nodes) + 1  # +1 for coordinator-local worker

        async def _start_nes_worker(wn: NodeInfo) -> None:
            wh = worker_homes[wn.id]
            await _run(worker_conns[wn.id], " ".join(filter(None, [
                "docker run --privileged -d --init --network=host",
                f"--name {wn_names[wn.id]}",
                _cpus_flag(wn),
                _worker_memory_flag(wn),
                f"-v {wh}/nes-conf/worker.yaml:/config.yaml",
                NES_WORKER_IMAGE,
            ])))
            await _assert_running(worker_conns[wn.id], wn_names[wn.id])

        async def _start_nes_workers_and_wait_ready() -> None:
            print(f"  Starting {len(worker_nodes)} NES worker(s)...")
            await asyncio.gather(*(_start_nes_worker(wn) for wn in worker_nodes))

            print(
                "  Waiting for NES topology to report "
                f"{expected_nes_nodes} worker node(s) (includes coordinator worker)..."
            )
            rest_payload = await _wait_nes_topology_workers_ready(
                snk_conn=snk_conn,
                nes_name=nes_name,
                expected_count=expected_nes_nodes,
            )
            print("  NES cluster is ready.")
            print("  Reconciling NES worker topology...")
            await _reconcile_nes_worker_topology(
                snk_conn=snk_conn,
                topology_file=topology_file,
                snk=snk,
                worker_nodes=worker_nodes,
                rest_payload=rest_payload,
            )

        async def _stop_nes_cluster() -> None:
            await _stop_containers(snk_conn, [nes_name])
            for wn in worker_nodes:
                await _stop_containers(worker_conns[wn.id], [wn_names[wn.id]])

        await _start_nes_coordinator()

        # Wait for sources to finish loading data
        print(f"  Waiting for TCP sources to report ready ('{READY_SIGNAL}')...")
        await asyncio.gather(
            _poll_for_pattern(src_conn, bid_name, READY_SIGNAL,
                              timeout=READY_TIMEOUT, label=bid_name),
            _poll_for_pattern(src_conn, auc_name, READY_SIGNAL,
                              timeout=READY_TIMEOUT, label=auc_name),
            _poll_for_pattern(src_conn, person_name, READY_SIGNAL,
                              timeout=READY_TIMEOUT, label=person_name),
        )
        print("  Sources are ready.")

        await _start_nes_workers_and_wait_ready()

        for rep in range(1, exp.repetitions + 1):
            await _run_nes_repetition(
                rep=rep,
                total_reps=exp.repetitions,
                exp=exp,
                snk_conn=snk_conn,
                src_conn=src_conn,
                bid_name=bid_name,
                sink_name=sink_name,
                query_str=query_str,
            )
            if restart_after_stop and rep < exp.repetitions:
                print(
                    "  nes_restart_after_stop is set - restarting NES coordinator "
                    f"and workers before repetition {rep + 1}..."
                )
                await _stop_nes_cluster()
                await _start_nes_coordinator()
                await _start_nes_workers_and_wait_ready()
    finally:
        print("  Stopping containers...")
        await _graceful_stop_tcp(src_conn, [bid_name, auc_name, person_name])
        await _graceful_stop_tcp(snk_conn, [sink_name])
        await _stop_containers(snk_conn, [nes_name])
        for wn in worker_nodes:
            await _stop_containers(worker_conns[wn.id], [wn_names[wn.id]])

        print("\n  Preprocessing remote latency logs before download...")
        await asyncio.gather(
            _run_remote_latency_preprocessing(
                src_conn,
                node_label=src.id,
                node_home=src_home,
                remote_logs=f"{src_home}/logs",
                remote_user=src.user,
            ),
            _run_remote_latency_preprocessing(
                snk_conn,
                node_label=snk.id,
                node_home=snk_home,
                remote_logs=f"{snk_home}/logs",
                remote_user=snk.user,
            ),
        )

        print(f"\n  Downloading logs to {output_dir}/...")
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        await _download_logs(src_conn, f"{src_home}/logs", output_dir / src.id)
        await _download_logs(snk_conn, f"{snk_home}/logs", output_dir / snk.id)
        print(f"  Logs saved to {output_dir}")


# ── Top-level entry point ─────────────────────────────────────────────────────

async def run_experiments(
    experiments: list[ExperimentSpec],
    topology_file: str,
    output_base: Path,
    key_path: str,
    passphrase: Optional[str] = None,
    skip_data_upload: bool = False,
    start_with_rep: Optional[int] = None,
    latency: bool = True,
) -> None:
    graph = load_topology(topology_file)
    nodes = load_nodes(topology_file)
    qcfg  = load_query_config()

    src_list = [n for n in nodes.values() if n.node_type == "source"]
    if not src_list:
        raise RuntimeError(
            "No node with node_type='source' found in topology. "
            "The source node runs the TCP sources, sink container, and Flink jobmanager."
        )
    src = src_list[0]

    sink_list = [n for n in nodes.values() if n.node_type == "sink"]
    if not sink_list:
        raise RuntimeError(
            "No node with node_type='sink' found in topology. "
            "A dedicated sink node is required to run the TCP sink container."
        )
    snk = sink_list[0]

    worker_nodes = [n for n in nodes.values()
                    if n.node_type not in ("source", "sink")]

    print(f"Source / coordinator : {src.id}  ({src.host})")
    print(f"Sink node            : {snk.id}  ({snk.host})")
    print(f"Worker nodes         : {[n.id for n in worker_nodes]}")

    # Open SSH connections
    print("\nConnecting to nodes...")
    src_conn = await _connect_node(src, key_path, passphrase)
    snk_conn = await _connect_node(snk, key_path, passphrase)
    worker_conns: dict[str, asyncssh.SSHClientConnection] = {}

    try:
        for wn in worker_nodes:
            worker_conns[wn.id] = await _connect_node(wn, key_path, passphrase)

        # Resolve home directories for SFTP (~ is not expanded by SFTP protocol)
        src_home = await _get_home(src_conn)
        snk_home = await _get_home(snk_conn)
        worker_homes = {
            wn.id: await _get_home(worker_conns[wn.id]) for wn in worker_nodes
        }

        # One-time setup: directories and source data
        print("\nPreparing remote directories...")
        await _ensure_remote_dir(src_conn, f"{src_home}/data")
        await _ensure_remote_dir(src_conn, f"{src_home}/logs/bids")
        await _ensure_remote_dir(src_conn, f"{src_home}/logs/auctions")
        await _ensure_remote_dir(src_conn, f"{src_home}/logs/persons")
        await _ensure_remote_dir(src_conn, f"{src_home}/flinke2c-conf")
        await _ensure_remote_dir(snk_conn, f"{snk_home}/logs")
        await _ensure_remote_dir(snk_conn, f"{snk_home}/flinke2c-conf")
        await _ensure_remote_dir(snk_conn, f"{snk_home}/flinke2c-lib")
        for wn in worker_nodes:
            wh = worker_homes[wn.id]
            await _ensure_remote_dir(worker_conns[wn.id], f"{wh}/logs")
            await _ensure_remote_dir(worker_conns[wn.id], f"{wh}/flinke2c-conf")
            await _ensure_remote_dir(worker_conns[wn.id], f"{wh}/flinke2c-lib")

        if not skip_data_upload:
            await _sync_source_data(src, src_home, key_path)

        # Sync Flink lib JARs to snk and all workers
        lib_jars = sorted(FLINK_LIB_DIR.glob("*.jar")) if FLINK_LIB_DIR.exists() else []
        if lib_jars:
            print("\nSyncing Flink lib JARs...")
            await _sync_flink_libs(snk, snk_home, key_path)
            for wn in worker_nodes:
                await _sync_flink_libs(wn, worker_homes[wn.id], key_path)

        # Run each experiment in sequence
        for exp in experiments:
            print(f"\n{'='*60}")
            print(f"Experiment : {exp.name}  [{exp.system}]")
            print(f"Query      : {exp.query}   Reps: {exp.repetitions}"
                  f"   Placement: {exp.placement_method or 'default'}")
            print(f"{'='*60}")

            if exp.system == "nes":
                await _run_nes_experiment(
                    exp=exp,
                    topology_file=topology_file,
                    graph=graph,
                    src=src,
                    src_conn=src_conn,
                    src_home=src_home,
                    snk=snk,
                    snk_conn=snk_conn,
                    snk_home=snk_home,
                    worker_nodes=worker_nodes,
                    worker_conns=worker_conns,
                    worker_homes=worker_homes,
                    output_dir=output_base / exp.name,
                    qcfg=qcfg,
                    start_with_rep=start_with_rep,
                    latency=latency,
                )
            elif exp.system == "flink":
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        if attempt > 1:
                            print(
                                f"  Retrying Flink experiment from scratch "
                                f"(attempt {attempt}/{FLINK_EXPERIMENT_MAX_RETRIES + 1})..."
                            )
                        await _run_flink_experiment(
                            exp=exp,
                            topology_file=topology_file,
                            graph=graph,
                            src=src,
                            src_conn=src_conn,
                            src_home=src_home,
                            snk_conn=snk_conn,
                            snk_home=snk_home,
                            snk=snk,
                            worker_nodes=worker_nodes,
                            worker_conns=worker_conns,
                            worker_homes=worker_homes,
                            lib_jars=lib_jars,
                            qcfg=qcfg,
                            output_dir=output_base / exp.name,
                            attempt=attempt,
                            start_with_rep=start_with_rep,
                            latency=latency,
                            skip_log_download_on_retryable_failure=(
                                attempt <= FLINK_EXPERIMENT_MAX_RETRIES
                            ),
                        )
                        break
                    except FlinkJobRetryableError as exc:
                        if attempt > FLINK_EXPERIMENT_MAX_RETRIES:
                            raise RuntimeError(
                                f"Flink experiment {exp.name!r} failed after "
                                f"{attempt} attempt(s): {exc}"
                            ) from exc
                        print(
                            f"  Retryable Flink failure detected: {exc}\n"
                            f"  Re-running experiment {exp.name!r}."
                        )
            else:
                raise ValueError(f"Unknown system: {exp.system!r}. Supported: 'flink', 'nes'")

        print("\nAll experiments complete.")

    finally:
        for conn in worker_conns.values():
            conn.close()
        snk_conn.close()
        src_conn.close()


async def run_profiles(
    experiments: list[ExperimentSpec],
    topology_file: str,
    key_path: str,
    passphrase: Optional[str] = None,
    skip_data_upload: bool = False,
    latency: bool = True,
) -> None:
    graph = load_topology(topology_file)
    nodes = load_nodes(topology_file)
    qcfg = load_query_config()
    profile_experiments = select_profile_experiments(experiments)

    if not profile_experiments:
        raise RuntimeError("No Flink experiments found to profile.")

    if not PROMETHEUS_CONFIG_FILE.exists():
        raise RuntimeError(
            f"Prometheus config not found at {PROMETHEUS_CONFIG_FILE}. "
            "Run 'sim setup' first or regenerate the Prometheus config."
        )

    capsys_python = _resolve_capsys_python()

    src_list = [n for n in nodes.values() if n.node_type == "source"]
    if not src_list:
        raise RuntimeError(
            "No node with node_type='source' found in topology. "
            "The source node runs the TCP sources."
        )
    src = src_list[0]

    sink_list = [n for n in nodes.values() if n.node_type == "sink"]
    if not sink_list:
        raise RuntimeError(
            "No node with node_type='sink' found in topology. "
            "A dedicated sink/coordinator node is required."
        )
    snk = sink_list[0]

    worker_nodes = [n for n in nodes.values() if n.node_type not in ("source", "sink")]

    print(f"Source / coordinator : {src.id}  ({src.host})")
    print(f"Sink node            : {snk.id}  ({snk.host})")
    print(f"Worker nodes         : {[n.id for n in worker_nodes]}")
    print(f"CAPSys queries       : {[exp.query for exp in profile_experiments]}")

    print("\nConnecting to nodes...")
    src_conn = await asyncssh.connect(**_conn_kwargs(src, key_path, passphrase))
    snk_conn = await asyncssh.connect(**_conn_kwargs(snk, key_path, passphrase))
    worker_conns: dict[str, asyncssh.SSHClientConnection] = {}

    try:
        for wn in worker_nodes:
            worker_conns[wn.id] = await asyncssh.connect(
                **_conn_kwargs(wn, key_path, passphrase)
            )

        src_home = await _get_home(src_conn)
        snk_home = await _get_home(snk_conn)
        worker_homes = {
            wn.id: await _get_home(worker_conns[wn.id]) for wn in worker_nodes
        }

        print("\nPreparing remote directories...")
        await _ensure_remote_dir(src_conn, f"{src_home}/data")
        await _ensure_remote_dir(src_conn, f"{src_home}/logs/bids")
        await _ensure_remote_dir(src_conn, f"{src_home}/logs/auctions")
        await _ensure_remote_dir(src_conn, f"{src_home}/logs/persons")
        await _ensure_remote_dir(src_conn, f"{src_home}/flinke2c-conf")
        await _ensure_remote_dir(snk_conn, f"{snk_home}/logs")
        await _ensure_remote_dir(snk_conn, f"{snk_home}/flinke2c-conf")
        await _ensure_remote_dir(snk_conn, f"{snk_home}/flinke2c-lib")
        for wn in worker_nodes:
            wh = worker_homes[wn.id]
            await _ensure_remote_dir(worker_conns[wn.id], f"{wh}/logs")
            await _ensure_remote_dir(worker_conns[wn.id], f"{wh}/flinke2c-conf")
            await _ensure_remote_dir(worker_conns[wn.id], f"{wh}/flinke2c-lib")

        if not skip_data_upload:
            await _sync_source_data(src, src_home, key_path)

        lib_jars = sorted(FLINK_LIB_DIR.glob("*.jar")) if FLINK_LIB_DIR.exists() else []
        if lib_jars:
            print("\nSyncing Flink lib JARs...")
            await _sync_flink_libs(snk, snk_home, key_path)
            for wn in worker_nodes:
                await _sync_flink_libs(wn, worker_homes[wn.id], key_path)

        for exp in profile_experiments:
            print(f"\n{'='*60}")
            print(
                f"CAPSys profile : {exp.query}  [{Path(topology_file).stem}]"
                f"   Reps: {exp.repetitions}"
            )
            print(f"{'='*60}")
            await _run_flink_profile_query(
                exp=exp,
                topology_file=topology_file,
                graph=graph,
                src=src,
                src_conn=src_conn,
                src_home=src_home,
                snk=snk,
                snk_conn=snk_conn,
                snk_home=snk_home,
                worker_nodes=worker_nodes,
                worker_conns=worker_conns,
                worker_homes=worker_homes,
                lib_jars=lib_jars,
                qcfg=qcfg,
                capsys_python=capsys_python,
                latency=latency,
            )

        print("\nCAPSys profiling complete.")

    finally:
        for conn in worker_conns.values():
            conn.close()
        snk_conn.close()
        src_conn.close()
