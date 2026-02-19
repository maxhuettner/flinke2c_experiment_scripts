"""Async SSH helpers for running commands on experiment nodes.

Each node gets a colour-coded log prefix so concurrent output from multiple
nodes stays readable.
"""

import asyncio
from pathlib import Path
from typing import Optional

import asyncssh

# ANSI colour codes (works on any modern terminal / iTerm2 / VSCode)
_COLOURS = [
    "\033[36m",   # cyan
    "\033[32m",   # green
    "\033[33m",   # yellow
    "\033[35m",   # magenta
    "\033[34m",   # blue
    "\033[91m",   # bright red
    "\033[96m",   # bright cyan
    "\033[92m",   # bright green
]
_RESET = "\033[0m"


def _colour(index: int) -> str:
    return _COLOURS[index % len(_COLOURS)]


def _expand(path: str) -> str:
    return str(Path(path).expanduser())


async def run_command(
    host: str,
    command: str,
    username: str,
    key_path: str,
    passphrase: Optional[str] = None,
    node_label: str = "",
    colour: str = "",
    port: int = 22,
) -> int:
    """Connect to *host*, run *command*, stream prefixed output, return exit status."""
    prefix = f"{colour}[{node_label}]{_RESET} " if node_label else ""

    conn_kwargs: dict = {
        "host": host,
        "port": port,
        "username": username,
        "client_keys": [_expand(key_path)],
        "known_hosts": None,   # skip host-key verification for lab environments
    }
    if passphrase:
        conn_kwargs["passphrase"] = passphrase

    try:
        async with asyncssh.connect(**conn_kwargs) as conn:
            result = await conn.run(command, check=False)
            if result.stdout:
                for line in result.stdout.splitlines():
                    print(f"{prefix}{line}")
            if result.stderr:
                for line in result.stderr.splitlines():
                    print(f"{prefix}[stderr] {line}")
            return result.exit_status or 0
    except (asyncssh.Error, OSError) as exc:
        print(f"{prefix}[connection error] {exc}")
        return 1


async def run_on_all(
    hosts: dict[str, str],   # node_id → IP
    command: str,
    username: str,
    key_path: str,
    passphrase: Optional[str] = None,
    port: int = 22,
) -> dict[str, int]:
    """Run *command* concurrently on every host; return {node_id: exit_status}."""
    node_ids = sorted(hosts)
    tasks = [
        run_command(
            host=hosts[nid],
            command=command,
            username=username,
            key_path=key_path,
            passphrase=passphrase,
            node_label=nid,
            colour=_colour(i),
            port=port,
        )
        for i, nid in enumerate(node_ids)
    ]
    results_list = await asyncio.gather(*tasks, return_exceptions=True)
    return {
        nid: (res if isinstance(res, int) else 1)
        for nid, res in zip(node_ids, results_list)
    }
