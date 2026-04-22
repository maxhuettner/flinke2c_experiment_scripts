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

Usage:
    sim experiment \\
        -f config/topologies/edge-to-cloud.json \\
        -e exp_management/experiments.yml \\
        [-o results/] \\
        [--skip-data-upload] \\
        [--start-with-rep N]
"""

from __future__ import annotations

import asyncio
import json as _json
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

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

FLINK_IMAGE = "maxhue/flinke2c:latest"
TCP_IMAGE   = "maxhue/tcp-streaming"

NES_QUERIES_DIR       = Path("exp_management/queries/nes")
NES_COORDINATOR_IMAGE = "maxhue/nes-coordinator"
NES_WORKER_IMAGE      = "maxhue/nes-worker"
NES_REST_PORT         = 8081
NES_CLUSTER_READY_TIMEOUT = 120  # seconds to wait for NES topology workers

READY_SIGNAL = "Reading & binary encoding done"
DONE_SIGNAL  = "All connections closed, stopping logger"

POLL_INTERVAL  = 3     # seconds between docker-logs polls
READY_TIMEOUT  = 120   # seconds to wait for source ready
DONE_TIMEOUT   = 1800  # seconds to wait for experiment completion
FLINK_CLUSTER_READY_TIMEOUT = 180  # seconds to wait for Flink TMs in REST
TRACE_REMOTE_TIMINGS = (
    os.getenv("SIM_TRACE_REMOTE_TIMINGS", "").strip().lower() in ("1", "true", "yes")
)


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ExperimentSpec:
    name: str
    system: str = "flink"
    query: str = ""
    repetitions: int = 1
    placement_method: str = ""      # "" or e.g. "TOP_DOWN"
    num_task_slots: Optional[int] = None
    graphml_file: str = ""          # explicit graphml filename in coordinator dir, e.g. "cloud.graphml"


@dataclass
class NodeInfo:
    id: str
    host: str       # SSH-accessible IP
    user: str
    node_type: str  # "source" / "sink" / "compute"
    address: str    # overlay/topology address
    speed: Optional[int] = None  # CPU cap in % (e.g. 50 → --cpus 0.50)


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_experiments(path: str) -> list[ExperimentSpec]:
    with open(path) as f:
        raw = yaml.safe_load(f) or {}
    global_repetitions = int(raw.get("repetitions", 1))
    return [
        ExperimentSpec(
            name=e["name"],
            system=e.get("system", "flink"),
            query=e["query"],
            repetitions=int(e.get("repetitions", global_repetitions)),
            placement_method=e.get("placement_method", ""),
            num_task_slots=e.get("num_task_slots"),
            graphml_file=e.get("graphml_file", ""),
        )
        for e in raw.get("experiments", [])
    ]


def load_query_config() -> dict:
    if QUERY_CONFIG_FILE.exists():
        with open(QUERY_CONFIG_FILE) as f:
            return yaml.safe_load(f) or {}
    return {}


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
    """Send 'q' to each TCP streaming container's stdin, then force-remove.

    All containers receive the quit signal in parallel.  After *timeout*
    seconds any that have not exited on their own are force-removed.
    """
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
    """Poll 'docker logs <container>' until *pattern* appears.

    Prints new lines as they appear. Raises TimeoutError if the pattern
    is not seen within *timeout* seconds.

    *since* is an optional Unix timestamp; when set only logs produced
    after that time are considered (useful for persistent containers).

    Note: monitored containers must NOT be started with --rm, otherwise
    Docker removes their log buffer on exit before we can read it.
    """
    since_flag = f"--since {since} " if since is not None else ""
    deadline = time.monotonic() + timeout
    shown: set[str] = set()

    while time.monotonic() < deadline:
        result = await conn.run(
            f"docker logs {since_flag}{container_name} 2>&1", check=False
        )
        output = result.stdout or ""

        # Stream new lines to stdout
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
        "-e", f"ssh -i {_expand(key_path)} -o StrictHostKeyChecking=no -o BatchMode=yes",
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
        "-e", f"ssh -i {_expand(key_path)} -o StrictHostKeyChecking=no -o BatchMode=yes",
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
    """Wait for the sink to signal end-of-repetition.

    The TCP source containers stay up across repetitions and typically log
    "Waiting for q..." after sending their current batch, so the sink log is
    the reliable end-of-repetition signal here.

    *since* is a Unix timestamp; only log lines produced after that time
    are checked, so signals from earlier repetitions are ignored.
    """
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


async def _wait_nes_topology_workers_ready(
    snk_conn: asyncssh.SSHClientConnection,
    nes_name: str,
    expected_count: int,
    timeout: float = NES_CLUSTER_READY_TIMEOUT,
) -> None:
    """Wait until NES topology REST reports all worker nodes.

    expected_count includes the coordinator-local worker.
    """
    if expected_count <= 0:
        return

    deadline = time.monotonic() + timeout
    last_seen = -1
    topology_cmd = (
        f"for p in /v1/nes/topology /nes/topology; do "
        f"curl -fsS http://127.0.0.1:{NES_REST_PORT}$p && exit 0; "
        "done; exit 1"
    )

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
                return

        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Timed out after {timeout:.0f}s waiting for NES topology workers "
        f"({expected_count} expected, last seen {max(last_seen, 0)})."
    )


# ── Config generators ─────────────────────────────────────────────────────────

def _coordinator_config(
    jm_address: str,
    placement_method: str = "",
    graphml_path: str = "",
) -> str:
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
        "      size: 600m",
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
        "env:",
        "  java:",
        "    opts:",
        "      all: >-",
        "        -verbose:gc -XX:NewRatio=3 -XX:+PrintGCDetails -XX:+PrintGCDateStamps"
        " -XX:ParallelGCThreads=4 --add-opens=java.base/java.util=ALL-UNNAMED",
        "      jobmanager: >-",
        "        -Xms256m -Xmx768m",
        "        -Xloggc:$FLINK_LOG_DIR/jobmanager-gc.log",
        "        -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=512M",
        "      taskmanager: >-",
        "        -Xloggc:$FLINK_LOG_DIR/taskmanager-gc.log",
        "        -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=512M",
        "",
        "state:",
        "  backend:",
        "    type: rocksdb",
        "    incremental: true",
        "    local-recovery: true",
        "  checkpoints:",
        "    dir: file:///tmp/checkpoint",
        "",
        "state.backend.rocksdb.localdir: /tmp",
        "",
        "execution:",
        "  checkpointing:",
        "    interval: 180000",
        "    mode: EXACTLY_ONCE",
        "    checkpoints-after-tasks-finish:",
        "      enabled: false",
        "",
        "table:",
        "  exec:",
        "    mini-batch:",
        "      enabled: true",
        "      allow-latency: 2s",
        "      size: 50000",
        "  optimizer:",
        "    distinct-agg:",
        "      split:",
        "        enabled: true",
    ]
    return "\n".join(lines) + "\n"


def _worker_config(jm_address: str, tm_host: str, task_slots: int) -> str:
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
        "      size: 1728m",
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

def _nes_coordinator_config(coordinator_host: str) -> str:
    return f"""\
logLevel: LOG_ERROR

restIp: 127.0.0.1
coordinatorHost: {coordinator_host}
restPort: {NES_REST_PORT}

worker:
  localWorkerHost: {coordinator_host}
  coordinatorHost: {coordinator_host}
  numberOfBuffersInGlobalBufferManager: 4096
  numberOfBuffersInSourceLocalBufferPool: 1024
  bufferSizeInBytes: 262144

  queryCompiler:
    maxHashTableSize: 2147483648
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

  - logicalSourceName: persons
    fields:
      - name: id
        type: INT64
      - name: dateTime
        type: INT64

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
"""


def _nes_worker_config(
    worker_host: str,
    coordinator_host: str,
    worker_id: int,
    source_host: Optional[str] = None,
) -> str:
    cfg = f"""\
logLevel: LOG_ERROR
localWorkerHost: {worker_host}
coordinatorHost: {coordinator_host}
numberOfBuffersInGlobalBufferManager: 4096
numberOfBuffersInSourceLocalBufferPool: 1024
bufferSizeInBytes: 262144

queryCompiler:
  maxHashTableSize: 2147483648
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
workerId: {worker_id}

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
) -> None:
    """Submit the SQL query and wait for both sources and sink to signal done.

    All containers (sources, sink, Flink cluster) are already running and
    stay up for the entire experiment; only the sql-client is started here.
    """
    rep_start = int(time.time())
    print(f"\n--- Repetition {rep}/{total_reps} ---")
    sql_name = f"flink-sql-{exp.name}-r{rep}-{rep_start}"

    sql_lib_mounts = " ".join(
        f"-v {snk_home}/flinke2c-lib/{j.name}:/opt/flink/lib/{j.name}:ro"
        for j in lib_jars
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
    print(f"  Waiting for repetition to finish ('{DONE_SIGNAL}')...")
    await _wait_sink_done(snk_conn, sink_name, since=rep_start)
    print(f"  Repetition {rep} complete.")


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
    start_with_rep: Optional[int] = None,
) -> None:
    per_query  = qcfg.get(exp.query, {})
    task_slots = exp.num_task_slots or per_query.get("num_task_slots", 1)
    bid_extra  = per_query.get("bid_src_extra_arg", "")

    # Load SQL
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
    coordinator_cfg = _coordinator_config(snk.address, exp.placement_method, graphml_path)
    worker_cfgs     = {
        wn.id: _worker_config(snk.address, wn.address, task_slots)
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

    system_flag = "--system nes" if exp.system == "nes" else ""
    start_with_rep_arg = (
        f"--start-with-rep {start_with_rep}" if start_with_rep is not None else ""
    )

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
        f"-v {src_home}/logs:/opt/tcp/logs",
        f"-v {src_home}/data:/data:ro",
        TCP_IMAGE,
        "source /data/bid_events.parquet",
        f"--address 0.0.0.0:10000 {system_flag} --schema bid --exp-name {exp.name}",
        start_with_rep_arg,
        bid_extra,
    ])).strip())
    await _assert_running(src_conn, bid_name)

    print("  Starting auction source...")
    await _run(src_conn, " ".join(filter(None, [
        "docker run --privileged -d -i --init --network=host",
        f"--name {auction_name}",
        f"-v {src_home}/logs:/opt/tcp/logs",
        f"-v {src_home}/data:/data:ro",
        TCP_IMAGE,
        "source /data/auction_events.parquet",
        f"--address 0.0.0.0:10001 {system_flag} --schema auction --exp-name {exp.name}",
        start_with_rep_arg,
    ])))
    await _assert_running(src_conn, auction_name)

    print("  Starting person source...")
    await _run(src_conn, " ".join(filter(None, [
        "docker run --privileged -d -i --init --network=host",
        f"--name {person_name}",
        f"-v {src_home}/logs:/opt/tcp/logs",
        f"-v {src_home}/data:/data:ro",
        TCP_IMAGE,
        "source /data/person_events.parquet",
        f"--address 0.0.0.0:10002 {system_flag} --schema person --exp-name {exp.name}",
        start_with_rep_arg,
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

    worker_containers = {wn.id: [tm_names[wn.id]] for wn in worker_nodes}

    try:
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
            )
    finally:
        print("  Stopping containers...")
        # TCP streaming containers: send 'q', wait 5 s, then force-remove
        await _graceful_stop_tcp(src_conn, [bid_name, auction_name, person_name])
        await _graceful_stop_tcp(snk_conn, [sink_name])
        # Flink containers: regular stop
        await _stop_containers(snk_conn, [jm_name])
        for wn in worker_nodes:
            await _stop_containers(worker_conns[wn.id], worker_containers[wn.id])

        # Always download logs — even if a repetition failed
        print(f"\n  Downloading logs to {output_dir}/...")
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        await _download_logs(src_conn, f"{src_home}/logs", output_dir / src.id)
        await _download_logs(snk_conn, f"{snk_home}/logs", output_dir / snk.id)
        print(f"  Logs saved to {output_dir}")


# ── NES experiment logic ──────────────────────────────────────────────────────

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

    payload = _json.dumps({"userQuery": query_str, "placement": "TopDown"})
    curl_cmd = (
        f"curl -s -X POST http://127.0.0.1:{NES_REST_PORT}/v1/nes/query/execute-query"
        f" -H 'Content-Type: application/json'"
        f" -d '{payload}'"
    )
    print(f"  Submitting NES query '{exp.query}'...")
    result = await _run(snk_conn, curl_cmd)
    print(f"  NES response: {result}")

    print(f"  Waiting for repetition to finish ('{DONE_SIGNAL}')...")
    await _wait_sink_done(snk_conn, sink_name, since=rep_start)
    print(f"  Repetition {rep} complete.")


async def _run_nes_experiment(
    exp: ExperimentSpec,
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
    start_with_rep: Optional[int] = None,
) -> None:
    query_path = NES_QUERIES_DIR / f"{exp.query}.txt"
    if not query_path.exists():
        raise FileNotFoundError(f"NES query file not found: {query_path}")
    query_str = query_path.read_text().strip()

    # Build configs
    coordinator_cfg = _nes_coordinator_config(snk.address)
    # Workers: first compute node gets physicalSources pointing to src
    worker_cfgs: dict[str, str] = {}
    for idx, wn in enumerate(worker_nodes):
        worker_id   = idx + 2   # coordinator=1; workers start at 2
        source_host = src.address if idx == 0 else None
        worker_cfgs[wn.id] = _nes_worker_config(
            worker_host=wn.address,
            coordinator_host=snk.address,
            worker_id=worker_id,
            source_host=source_host,
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
        f"-v {src_home}/logs:/opt/tcp/logs",
        f"-v {src_home}/data:/data:ro",
        TCP_IMAGE,
        "source /data/bid_events.parquet",
        f"--address 0.0.0.0:10000 --system nes --schema bid --exp-name {exp.name}",
        start_with_rep_arg,
    ])))
    await _assert_running(src_conn, bid_name)

    print("  Starting auction source...")
    await _run(src_conn, " ".join(filter(None, [
        "docker run --privileged -d -i --init --network=host",
        f"--name {auc_name}",
        f"-v {src_home}/logs:/opt/tcp/logs",
        f"-v {src_home}/data:/data:ro",
        TCP_IMAGE,
        "source /data/auction_events.parquet",
        f"--address 0.0.0.0:10001 --system nes --schema auction --exp-name {exp.name}",
        start_with_rep_arg,
    ])))
    await _assert_running(src_conn, auc_name)

    print("  Starting person source...")
    await _run(src_conn, " ".join(filter(None, [
        "docker run --privileged -d -i --init --network=host",
        f"--name {person_name}",
        f"-v {src_home}/logs:/opt/tcp/logs",
        f"-v {src_home}/data:/data:ro",
        TCP_IMAGE,
        "source /data/person_events.parquet",
        f"--address 0.0.0.0:10002 --system nes --schema person --exp-name {exp.name}",
        start_with_rep_arg,
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
    ])))
    await _assert_running(snk_conn, sink_name)

    # Start NES coordinator on snk
    print("  Starting NES coordinator...")
    await _run(snk_conn, " ".join([
        "docker run --privileged -d --init --network=host",
        f"--name {nes_name}",
        f"-v {snk_home}/nes-conf/coordinator.yaml:/config.yaml",
        NES_COORDINATOR_IMAGE,
    ]))
    await _assert_running(snk_conn, nes_name)

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

    # Start NES workers on compute nodes
    print(f"  Starting {len(worker_nodes)} NES worker(s)...")
    async def _start_nes_worker(wn: NodeInfo) -> None:
        wh = worker_homes[wn.id]
        await _run(worker_conns[wn.id], " ".join(filter(None, [
            "docker run --privileged -d --init --network=host",
            f"--name {wn_names[wn.id]}",
            _cpus_flag(wn),
            f"-v {wh}/nes-conf/worker.yaml:/config.yaml",
            NES_WORKER_IMAGE,
        ])))
        await _assert_running(worker_conns[wn.id], wn_names[wn.id])
    await asyncio.gather(*(_start_nes_worker(wn) for wn in worker_nodes))

    expected_nes_nodes = len(worker_nodes) + 1  # +1 for coordinator-local worker
    print(
        "  Waiting for NES topology to report "
        f"{expected_nes_nodes} worker node(s) (includes coordinator worker)..."
    )
    await _wait_nes_topology_workers_ready(
        snk_conn=snk_conn,
        nes_name=nes_name,
        expected_count=expected_nes_nodes,
    )
    print("  NES cluster is ready.")

    try:
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
    finally:
        print("  Stopping containers...")
        await _graceful_stop_tcp(src_conn, [bid_name, auc_name, person_name])
        await _graceful_stop_tcp(snk_conn, [sink_name])
        await _stop_containers(snk_conn, [nes_name])
        for wn in worker_nodes:
            await _stop_containers(worker_conns[wn.id], [wn_names[wn.id]])

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
    src_conn = await asyncssh.connect(**_conn_kwargs(src, key_path, passphrase))
    snk_conn = await asyncssh.connect(**_conn_kwargs(snk, key_path, passphrase))
    worker_conns: dict[str, asyncssh.SSHClientConnection] = {}

    try:
        for wn in worker_nodes:
            worker_conns[wn.id] = await asyncssh.connect(
                **_conn_kwargs(wn, key_path, passphrase)
            )

        # Resolve home directories for SFTP (~ is not expanded by SFTP protocol)
        src_home = await _get_home(src_conn)
        snk_home = await _get_home(snk_conn)
        worker_homes = {
            wn.id: await _get_home(worker_conns[wn.id]) for wn in worker_nodes
        }

        # One-time setup: directories and source data
        print("\nPreparing remote directories...")
        await _run(src_conn, f"mkdir -p {src_home}/data {src_home}/logs {src_home}/flinke2c-conf")
        await _run(snk_conn, f"mkdir -p {snk_home}/logs {snk_home}/flinke2c-conf {snk_home}/flinke2c-lib")
        for wn in worker_nodes:
            wh = worker_homes[wn.id]
            await _run(worker_conns[wn.id], f"mkdir -p {wh}/logs {wh}/flinke2c-conf {wh}/flinke2c-lib")

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
                    start_with_rep=start_with_rep,
                )
            elif exp.system == "flink":
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
                    start_with_rep=start_with_rep,
                )
            else:
                raise ValueError(f"Unknown system: {exp.system!r}. Supported: 'flink', 'nes'")

        print("\nAll experiments complete.")

    finally:
        for conn in worker_conns.values():
            conn.close()
        snk_conn.close()
        src_conn.close()
