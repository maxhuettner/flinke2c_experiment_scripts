"""Flink experiment runner for the network-sim framework.

Experiment YAML format (e.g. exp_management/experiments.yml):

    experiments:
      - name: q1_local
        system: flink        # currently the only supported system
        query: q1            # resolves to exp_management/queries/flink/q1.sql
        repetitions: 3

      - name: q4_cluster
        system: flink
        query: q4
        repetitions: 2
        placement_method: cluster   # generates topology.graphml for Flink

Per-query extra args and task slot counts are read from
exp_management/configs/flink/query_config.yml and can be overridden with
`num_task_slots` in the experiment YAML.

Usage:
    sim experiment \\
        -f config/topologies/edge-to-cloud.json \\
        -e exp_management/experiments.yml \\
        [-o results/] \\
        [--skip-data-upload]
"""

from __future__ import annotations

import asyncio
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
ANSIBLE_INVENTORY = Path("exp_management/ansible/inventory/generated_hosts.yml")

FLINK_IMAGE = "maxhue/flinke2c:latest"
TCP_IMAGE   = "maxhue/tcp-streaming"

READY_SIGNAL = "Reading & binary encoding done"
DONE_SIGNAL  = "All connections closed, stopping logger"

POLL_INTERVAL  = 3     # seconds between docker-logs polls
READY_TIMEOUT  = 120   # seconds to wait for source ready
DONE_TIMEOUT   = 1800  # seconds to wait for experiment completion
CLUSTER_WARMUP = 8     # seconds for Flink cluster to form after starting


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


# ── Loaders ───────────────────────────────────────────────────────────────────

def load_experiments(path: str) -> list[ExperimentSpec]:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return [
        ExperimentSpec(
            name=e["name"],
            system=e.get("system", "flink"),
            query=e["query"],
            repetitions=int(e.get("repetitions", 1)),
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
        nodes[nid] = NodeInfo(
            id=nid,
            host=iv.get("ansible_host", topo.address),
            user=iv.get("ansible_user", "ubuntu"),
            node_type=topo.node_type.lower(),
            address=topo.address,
        )
    return nodes


# ── SSH / SFTP helpers ────────────────────────────────────────────────────────

def _conn_kwargs(node: NodeInfo, key_path: str, passphrase: Optional[str]) -> dict:
    kw: dict = {
        "host": node.host,
        "username": node.user,
        "client_keys": [_expand(key_path)],
        "known_hosts": None,
        "keepalive_interval": 30,
    }
    if passphrase:
        kw["passphrase"] = passphrase
    return kw


async def _run(conn: asyncssh.SSHClientConnection, cmd: str, *, check: bool = True) -> str:
    result = await conn.run(cmd, check=False)
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


async def _sync_file(
    conn: asyncssh.SSHClientConnection, local: Path, remote_path: str
) -> bool:
    """Upload *local* to *remote_path* only if missing or a different size.

    Returns True if the file was uploaded, False if already up to date.
    """
    local_size = local.stat().st_size
    async with conn.start_sftp_client() as sftp:
        try:
            st = await sftp.stat(remote_path)
            if st.size == local_size:
                return False
        except asyncssh.SFTPError:
            pass  # file doesn't exist — upload it
        await sftp.put(str(local), remote_path)
    return True


async def _download_logs(
    conn: asyncssh.SSHClientConnection, remote_logs: str, local_dir: Path
) -> None:
    """Download all files from remote_logs dir into local_dir."""
    local_dir.mkdir(parents=True, exist_ok=True)
    result = await _run(conn, f"ls {remote_logs} 2>/dev/null || true", check=False)
    if not result:
        print(f"  (no log files found in {remote_logs})")
        return
    async with conn.start_sftp_client() as sftp:
        for fname in result.splitlines():
            fname = fname.strip()
            if not fname:
                continue
            remote_file = f"{remote_logs}/{fname}"
            local_file = local_dir / fname
            try:
                await sftp.get(remote_file, str(local_file))
                print(f"  downloaded {fname}")
            except asyncssh.SFTPError as exc:
                print(f"  warning: could not download {fname}: {exc}", file=sys.stderr)


# ── Container helpers ─────────────────────────────────────────────────────────

async def _stop_containers(
    conn: asyncssh.SSHClientConnection, names: list[str]
) -> None:
    if not names:
        return
    joined = " ".join(names)
    # stop first (graceful), then force-remove
    await _run(conn, f"docker stop {joined} 2>/dev/null || true", check=False)
    await _run(conn, f"docker rm -f {joined} 2>/dev/null || true", check=False)


async def _poll_for_pattern(
    conn: asyncssh.SSHClientConnection,
    container_name: str,
    pattern: str,
    timeout: float,
    label: str = "",
) -> None:
    """Poll 'docker logs <container>' until *pattern* appears.

    Prints new lines as they appear. Raises TimeoutError if the pattern
    is not seen within *timeout* seconds.

    Note: monitored containers must NOT be started with --rm, otherwise
    Docker removes their log buffer on exit before we can read it.
    """
    deadline = time.monotonic() + timeout
    shown: set[str] = set()

    while time.monotonic() < deadline:
        result = await conn.run(
            f"docker logs {container_name} 2>&1", check=False
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

        await asyncio.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Timed out after {timeout:.0f}s waiting for '{pattern}' "
        f"in container '{container_name}'"
    )


async def _sync_source_data(
    conn: asyncssh.SSHClientConnection,
    src_home: str,
) -> None:
    """Sync local source data files to the remote ~/data/ directory, skipping unchanged files."""
    if not SOURCE_DATA_DIR.exists():
        return
    data_files = [f for f in SOURCE_DATA_DIR.iterdir() if f.is_file()]
    if not data_files:
        return
    print(f"Syncing {len(data_files)} source data file(s)...")
    for df in data_files:
        print(f"  {df.name}: ", end="", flush=True)
        uploaded = await _sync_file(conn, df, f"{src_home}/data/{df.name}")
        print("uploaded" if uploaded else "already present, skipped")


async def _wait_both_done(
    src_conn: asyncssh.SSHClientConnection,
    bid_name: str,
    sink_name: str,
    timeout: float = DONE_TIMEOUT,
) -> None:
    """Wait concurrently for bid source and sink to signal completion."""
    await asyncio.gather(
        _poll_for_pattern(src_conn, bid_name, DONE_SIGNAL, timeout, label=f"src/{bid_name}"),
        _poll_for_pattern(src_conn, sink_name, DONE_SIGNAL, timeout, label=f"sink/{sink_name}"),
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
        "      size: 8G",
        "",
        "rest:",
        "  address: 0.0.0.0",
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


# ── Per-experiment logic ───────────────────────────────────────────────────────

async def _run_flink_repetition(
    *,
    rep: int,
    total_reps: int,
    exp: ExperimentSpec,
    src_conn: asyncssh.SSHClientConnection,
    src_home: str,
    worker_nodes: list[NodeInfo],
    worker_conns: dict[str, asyncssh.SSHClientConnection],
    worker_homes: dict[str, str],
    bid_extra: str,
    combined_sql: str,
    coordinator_cfg: str,
    worker_cfgs: dict[str, str],
    # graphml_content is None when placement_method is empty
    graphml_content: Optional[str],
    graphml_filename: str,
) -> None:
    print(f"\n--- Repetition {rep}/{total_reps} ---")
    run_id = f"{exp.name}-r{rep}-{int(time.time())}"

    # Container names — no --rm on bid/sink so docker logs survives container exit
    bid_name     = f"tcp-bid-{run_id}"
    auction_name = f"tcp-auction-{run_id}"
    sink_name    = f"tcp-sink-{run_id}"
    jm_name      = f"flink-jm-{run_id}"
    sql_name     = f"flink-sql-{run_id}"
    tm_names     = {wn.id: f"flink-tm-{wn.id}-{run_id}" for wn in worker_nodes}

    src_containers    = [bid_name, auction_name, sink_name, jm_name, sql_name]
    worker_containers = {wn.id: [tm_names[wn.id]] for wn in worker_nodes}

    try:
        # ── Clear previous logs ────────────────────────────────────────────
        await _run(src_conn, f"rm -f {src_home}/logs/* 2>/dev/null || true", check=False)
        for wn in worker_nodes:
            await _run(
                worker_conns[wn.id],
                f"rm -f {worker_homes[wn.id]}/logs/* 2>/dev/null || true",
                check=False,
            )

        # ── Upload Flink configs ───────────────────────────────────────────
        print("  Uploading Flink configs...")
        await _upload_text(
            src_conn, coordinator_cfg, f"{src_home}/flinke2c-conf/config.yaml"
        )
        for wn in worker_nodes:
            await _upload_text(
                worker_conns[wn.id],
                worker_cfgs[wn.id],
                f"{worker_homes[wn.id]}/flinke2c-conf/config.yaml",
            )

        # ── Upload graphml for cluster placement ───────────────────────────
        if exp.placement_method and graphml_content is not None:
            print(f"  Uploading graphml ({graphml_filename})...")
            await _upload_text(
                src_conn, graphml_content,
                f"{src_home}/flinke2c-conf/{graphml_filename}",
            )

        # ── Start TCP source containers ────────────────────────────────────
        # --system flag: only NES needs it; Flink uses its own TCP connector
        system_flag = "--system nes" if exp.system == "nes" else ""

        print("  Starting bid source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run -d --init --network=host",
            f"--name {bid_name}",
            f"-v {src_home}/logs:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/bid_events.parquet",
            f"--address 0.0.0.0:10000 {system_flag} --schema bid --exp-name bid",
            bid_extra,
        ])).strip())

        print("  Starting auction source...")
        await _run(src_conn, " ".join(filter(None, [
            "docker run -d --rm --init --network=host",
            f"--name {auction_name}",
            f"-v {src_home}/logs:/opt/tcp/logs",
            f"-v {src_home}/data:/data:ro",
            TCP_IMAGE,
            "source /data/auction_events.parquet",
            f"--address 0.0.0.0:10001 {system_flag} --schema auction --exp-name auction",
        ])))

        print("  Starting sink...")
        await _run(src_conn, " ".join([
            "docker run -d --init --network=host",
            f"--name {sink_name}",
            f"-v {src_home}/logs:/opt/tcp/logs",
            TCP_IMAGE,
            "sink --exp-name test",
        ]))

        # ── Wait for bid source ready ──────────────────────────────────────
        print(f"  Waiting for bid source ready ('{READY_SIGNAL}')...")
        await _poll_for_pattern(
            src_conn, bid_name, READY_SIGNAL,
            timeout=READY_TIMEOUT, label=bid_name,
        )
        print("  Source is ready.")

        # ── Start Flink cluster ────────────────────────────────────────────
        print("  Starting Flink jobmanager...")
        await _run(src_conn, " ".join([
            "docker run -d --network=host",
            f"--name {jm_name}",
            f"-v {src_home}/flinke2c-conf:/conf/",
            FLINK_IMAGE,
            "jobmanager",
        ]))

        print(f"  Starting {len(worker_nodes)} taskmanager(s)...")
        for wn in worker_nodes:
            wh = worker_homes[wn.id]
            await _run(worker_conns[wn.id], " ".join([
                "docker run -d --network=host",
                f"--name {tm_names[wn.id]}",
                f"-v {wh}/flinke2c-conf:/conf/",
                FLINK_IMAGE,
                "taskmanager",
            ]))

        print(f"  Waiting {CLUSTER_WARMUP}s for cluster to form...")
        await asyncio.sleep(CLUSTER_WARMUP)

        # ── Submit SQL query ───────────────────────────────────────────────
        print(f"  Submitting query '{exp.query}'...")
        await _upload_text(src_conn, combined_sql, f"{src_home}/flink_query.sql")
        await _run(src_conn, " ".join([
            "docker run -d --rm --network=host",
            f"--name {sql_name}",
            f"-v {src_home}/flinke2c-conf:/conf/",
            f"-v {src_home}/flink_query.sql:/tmp/flink_query.sql:ro",
            FLINK_IMAGE,
            "sql-client embedded -f /tmp/flink_query.sql",
        ]))

        # ── Wait for completion ────────────────────────────────────────────
        print(f"  Waiting for experiment to finish ('{DONE_SIGNAL}')...")
        await _wait_both_done(src_conn, bid_name, sink_name)
        print(f"  Repetition {rep} complete.")

    finally:
        print("  Stopping containers...")
        await _stop_containers(src_conn, src_containers)
        for wn in worker_nodes:
            await _stop_containers(worker_conns[wn.id], worker_containers[wn.id])


async def _run_flink_experiment(
    exp: ExperimentSpec,
    topology_file: str,
    graph: nx.Graph,
    src: NodeInfo,
    src_conn: asyncssh.SSHClientConnection,
    src_home: str,
    worker_nodes: list[NodeInfo],
    worker_conns: dict[str, asyncssh.SSHClientConnection],
    worker_homes: dict[str, str],
    sink_id: Optional[str],
    qcfg: dict,
    output_dir: Path,
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
            graphml_filename = static_path.name
            print(f"  Using static graphml: {static_path}")
        else:
            graphml_content  = generate_graphml(graph, src.id, sink_id, task_slots)
            graphml_filename = "topology.graphml"
            print(f"  Generating graphml dynamically (no static file found: {static_path})")

    graphml_path    = f"/conf/{graphml_filename}" if graphml_filename else ""
    coordinator_cfg = _coordinator_config(src.address, exp.placement_method, graphml_path)
    worker_cfgs     = {
        wn.id: _worker_config(src.address, wn.address, task_slots)
        for wn in worker_nodes
    }

    for rep in range(1, exp.repetitions + 1):
        await _run_flink_repetition(
            rep=rep,
            total_reps=exp.repetitions,
            exp=exp,
            src_conn=src_conn,
            src_home=src_home,
            worker_nodes=worker_nodes,
            worker_conns=worker_conns,
            worker_homes=worker_homes,
            bid_extra=bid_extra,
            combined_sql=combined_sql,
            coordinator_cfg=coordinator_cfg,
            worker_cfgs=worker_cfgs,
            graphml_content=graphml_content,
            graphml_filename=graphml_filename,
        )

    # Download logs after all repetitions of this experiment
    print(f"\nDownloading logs to {output_dir}/...")
    output_dir.mkdir(parents=True, exist_ok=True)
    await _download_logs(src_conn, f"{src_home}/logs", output_dir / src.id)
    print(f"  Logs saved to {output_dir}")


# ── Top-level entry point ─────────────────────────────────────────────────────

async def run_experiments(
    experiments: list[ExperimentSpec],
    topology_file: str,
    output_base: Path,
    key_path: str,
    passphrase: Optional[str] = None,
    skip_data_upload: bool = False,
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
    sink_id   = sink_list[0].id if sink_list else None

    worker_nodes = [n for n in nodes.values()
                    if n.node_type not in ("source", "sink")]

    print(f"Source / coordinator : {src.id}  ({src.host})")
    print(f"Worker nodes         : {[n.id for n in worker_nodes]}")
    if sink_id:
        print(f"Sink node            : {sink_id}")

    # Open SSH connections
    print("\nConnecting to nodes...")
    src_conn = await asyncssh.connect(**_conn_kwargs(src, key_path, passphrase))
    worker_conns: dict[str, asyncssh.SSHClientConnection] = {}

    try:
        for wn in worker_nodes:
            worker_conns[wn.id] = await asyncssh.connect(
                **_conn_kwargs(wn, key_path, passphrase)
            )

        # Resolve home directories for SFTP (~ is not expanded by SFTP protocol)
        src_home = await _get_home(src_conn)
        worker_homes = {
            wn.id: await _get_home(worker_conns[wn.id]) for wn in worker_nodes
        }

        # One-time setup: directories and source data
        print("\nPreparing remote directories...")
        await _run(src_conn, f"mkdir -p {src_home}/data {src_home}/logs {src_home}/flinke2c-conf")
        for wn in worker_nodes:
            wh = worker_homes[wn.id]
            await _run(worker_conns[wn.id], f"mkdir -p {wh}/logs {wh}/flinke2c-conf")

        if not skip_data_upload:
            await _sync_source_data(src_conn, src_home)

        # Run each experiment in sequence
        for exp in experiments:
            print(f"\n{'='*60}")
            print(f"Experiment : {exp.name}")
            print(f"Query      : {exp.query}   Reps: {exp.repetitions}"
                  f"   Placement: {exp.placement_method or 'default'}")
            print(f"{'='*60}")

            await _run_flink_experiment(
                exp=exp,
                topology_file=topology_file,
                graph=graph,
                src=src,
                src_conn=src_conn,
                src_home=src_home,
                worker_nodes=worker_nodes,
                worker_conns=worker_conns,
                worker_homes=worker_homes,
                sink_id=sink_id,
                qcfg=qcfg,
                output_dir=output_base / exp.name,
            )

        print("\nAll experiments complete.")

    finally:
        for conn in worker_conns.values():
            conn.close()
        src_conn.close()
