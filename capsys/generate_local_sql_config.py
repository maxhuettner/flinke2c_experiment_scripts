#!/usr/bin/env python3
"""Generate a local CAPSys profiling config from a running Flink job."""

import argparse
import json
import re
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen


def sanitize_operator_name(description: str) -> str:
    return (
        description.replace(" ", "_")
        .replace("+", "_")
        .replace("-", "_")
        .replace(",", "_")
        .replace(":", "_")
        .replace(";", "_")
        .replace("<br/>", "")
        .replace("(", "_")
        .replace(")", "_")
        .replace("/", "_")
    )


def collapse_underscores(value: str) -> str:
    return re.sub(r"_+", "_", value).strip("_")


def simple_label(description: str) -> str:
    source_match = re.search(r"Source:\s*([^\[]+)", description)
    if source_match:
        return collapse_underscores(f"Source_{source_match.group(1)}")

    sink_match = re.search(r"Sink:\s*([^\[]+)", description)
    if sink_match:
        return collapse_underscores(f"Sink_{sink_match.group(1)}")

    for token in [
        "Window",
        "Join",
        "Calc",
        "Filter",
        "Aggregate",
        "Map",
        "WatermarkAssigner",
        "Sink",
        "Source",
    ]:
        if token in description:
            return token

    cleaned = collapse_underscores(sanitize_operator_name(description))
    return cleaned or "Operator"


def uniquify(label: str, used: set[str]) -> str:
    if label not in used:
        used.add(label)
        return label

    index = 2
    while f"{label}_{index}" in used:
        index += 1
    unique = f"{label}_{index}"
    used.add(unique)
    return unique


def is_source_description(description: str) -> bool:
    return "Source:" in description or "TableSourceScan" in description


def get_json(url: str) -> dict:
    with urlopen(url) as response:
        return json.load(response)


def choose_job(base_url: str, job_id: str) -> tuple[str, str]:
    if job_id:
        job = get_json(f"{base_url}/jobs/{job_id}")
        return job_id, job.get("name", job_id)

    overview = get_json(f"{base_url}/jobs/overview")
    running_jobs = [job for job in overview.get("jobs", []) if job.get("state") == "RUNNING"]
    if not running_jobs:
        raise RuntimeError("No RUNNING Flink job found")

    running_jobs.sort(key=lambda job: job.get("start-time", 0), reverse=True)
    selected = running_jobs[0]
    return selected["jid"], selected.get("name", selected["jid"])


def build_mapping(
    plan_nodes: list[dict], vertex_names: dict[str, str]
) -> tuple[dict[str, str], list[dict[str, int]], list[list[str]]]:
    used_labels: set[str] = set()
    mapping: dict[str, str] = {}
    id_to_label: dict[str, str] = {}
    srcratelist: list[dict[str, int]] = []

    for node in plan_nodes:
        description = node["description"]
        sanitized = sanitize_operator_name(description)
        label = uniquify(simple_label(description), used_labels)
        display = vertex_names.get(node["id"], label)
        mapping[sanitized] = display
        id_to_label[node["id"]] = display

        if is_source_description(description):
            srcratelist.append({label: 1000})

    # [parent, child] pairs (data flows parent -> child), by display label -
    # lets a placement algorithm traverse the real operator DAG (e.g. BFS
    # from the sink) instead of just Flink's plan-enumeration order.
    edges: list[list[str]] = []
    for node in plan_nodes:
        child = id_to_label[node["id"]]
        for inp in node.get("inputs", []):
            parent = id_to_label.get(inp["id"])
            if parent:
                edges.append([parent, child])

    return mapping, srcratelist, edges


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate capsys/expjson/local_sql.json from a running Flink job."
    )
    parser.add_argument(
        "--template",
        default="capsys/examples/runds2placement-local.example.json",
        help="Template JSON to use as the base config.",
    )
    parser.add_argument(
        "--output",
        default="capsys/expjson/local_sql.json",
        help="Where to write the generated config.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Flink JobManager REST host.")
    parser.add_argument("--port", type=int, default=8081, help="Flink JobManager REST port.")
    parser.add_argument(
        "--job-id",
        default="",
        help="Optional Flink job id. If omitted, the newest RUNNING job is used.",
    )
    parser.add_argument(
        "--source-rate",
        type=int,
        default=1000,
        help="Default source rate for each detected source operator.",
    )
    args = parser.parse_args()

    template_path = Path(args.template)
    output_path = Path(args.output)

    if not template_path.exists():
        print(f"Template not found: {template_path}", file=sys.stderr)
        return 1

    try:
        config = json.loads(template_path.read_text())
    except json.JSONDecodeError as exc:
        print(f"Failed to parse template JSON: {exc}", file=sys.stderr)
        return 1

    base_url = f"http://{args.host}:{args.port}"

    try:
        job_id, job_name = choose_job(base_url, args.job_id)
        job = get_json(f"{base_url}/jobs/{job_id}")
        plan = get_json(f"{base_url}/jobs/{job_id}/plan")
    except (RuntimeError, HTTPError, URLError) as exc:
        print(f"Failed to query Flink REST API: {exc}", file=sys.stderr)
        return 1

    plan_nodes = plan.get("plan", {}).get("nodes", [])
    if not plan_nodes:
        print(f"No plan nodes found for job {job_id}", file=sys.stderr)
        return 1

    vertex_names = {vertex["id"]: vertex["name"] for vertex in job.get("vertices", [])}
    mapping, srcratelist, edges = build_mapping(plan_nodes, vertex_names)
    if srcratelist:
        srcratelist = [{label: args.source_rate} for item in srcratelist for label in item]

    config["expname"] = collapse_underscores(job_name) or config.get("expname", "local_sql")
    config["jmip"] = args.host
    config["jmpt"] = args.port
    config["jobid"] = job_id
    config["mapping"] = mapping
    config["edges"] = edges
    config["srcratelist"] = srcratelist or config.get("srcratelist", [])
    config["schedulercfg1st"] = []
    config.setdefault("use_single_process_plan", True)
    config.setdefault("warmup_sec", 30)
    config.setdefault("warmup_first_sec", 30)
    config.setdefault("run_period_sec", 60)
    config.setdefault("run_freq_sec", 5)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(config, indent=2) + "\n")

    print(f"Wrote {output_path}")
    print(f"Job: {job_name} ({job_id})")
    print("Detected operators:")
    for key, value in mapping.items():
        print(f"  {key} -> {value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
