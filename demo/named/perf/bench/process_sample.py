# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""What a soak samples of the server (REQ-1911): its process tree's resident memory, threads, file
descriptors and sockets from ``/proc``, the bytes under each spill directory, and Redis's memory
and keys from ``INFO``. A figure that cannot be read is reported as ``{"error": reason}``, never
guessed (a developer's Mac has no ``/proc``)."""

# Requirements: REQ-1911

from __future__ import annotations

import os
import socket
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from paging import PROC, pid_tree


def _status(proc: Path, pid: int) -> dict[str, str]:
    fields = {}
    for line in (proc / str(pid) / "status").read_text().splitlines():
        key, _, value = line.partition(":")
        fields[key] = value.strip()
    return fields


def _dir_bytes(path: Path) -> int:
    return sum(
        (Path(root) / name).lstat().st_size for root, _, files in os.walk(path) for name in files
    )


def tree(root: int, spill_dirs: Iterable[Path], proc: Path = PROC) -> dict[str, Any]:
    """``root`` and its live descendants summed: processes, ``rss_kb``, threads, fds, the fds
    that are sockets; and the bytes under each spill directory, by its path."""
    try:
        totals = {"processes": 0, "rss_kb": 0, "threads": 0, "fds": 0, "sockets": 0}
        if not (proc / str(root)).exists():
            raise FileNotFoundError(f"no {proc / str(root)}")
        for pid in pid_tree(proc, root):
            try:
                status = _status(proc, pid)
                fds = [os.readlink(fd) for fd in (proc / str(pid) / "fd").iterdir()]
            except FileNotFoundError:
                continue  # exited while walked
            totals["processes"] += 1
            totals["rss_kb"] += int(status["VmRSS"].split()[0])
            totals["threads"] += int(status["Threads"])
            totals["fds"] += len(fds)
            totals["sockets"] += sum(target.startswith("socket:") for target in fds)
        return {**totals, "spill_bytes": {str(d): _dir_bytes(d) for d in spill_dirs}}
    except OSError as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def parse_redis_info(text: str) -> dict[str, int]:
    """``used_memory``, ``used_memory_rss`` and the keys of every database, from ``INFO``."""
    fields = dict(
        line.split(":", 1) for line in text.splitlines() if ":" in line and not line.startswith("#")
    )
    keys = sum(
        int(value.split(",")[0].removeprefix("keys="))
        for name, value in fields.items()
        if name.startswith("db")
    )
    return {
        "used_memory": int(fields["used_memory"]),
        "used_memory_rss": int(fields["used_memory_rss"]),
        "keys": keys,
    }


def redis_info(host: str, port: int) -> dict[str, Any]:
    """Redis's ``INFO``, read over one connection (an inline command; the reply is one bulk
    string, ``$<length>\\r\\n<text>\\r\\n``)."""
    try:
        with socket.create_connection((host, port), timeout=5) as conn:
            conn.sendall(b"INFO\r\n")
            reader = conn.makefile("rb")
            header = reader.readline()
            if not header.startswith(b"$"):
                raise ValueError(f"Redis answered {header!r}")
            text = reader.read(int(header[1:]))
        return parse_redis_info(text.decode())
    except (OSError, ValueError) as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}
