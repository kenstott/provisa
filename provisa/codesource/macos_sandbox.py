# Copyright (c) 2026 Kenneth Stott
# Canary: aa1a0c48-7afa-4d39-8f13-9d32574d2400
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The macOS sandbox a code source's worker runs in (REQ-1984, REQ-1993).

The worker is started under ``sandbox-exec`` with a profile that denies everything, then allows:
reading the directories named (the code, the runtime, the host program) and the system's own
libraries; writing nothing but ``/dev/null``; running only its runtime; and connecting only to its
worker socket, to the egress proxy on loopback, and to the minted-token listener (REQ-2226). A host without ``sandbox-exec`` refuses the
worker by name (:class:`SandboxUnavailable`)."""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path

from provisa.codesource.linux_sandbox import SandboxUnavailable

SANDBOX_EXEC = Path("/usr/bin/sandbox-exec")

#: What every process on macOS reads to start: the loader's cache, the system libraries, locale
#: and time zone data.
_SYSTEM_READ = (
    "/usr/lib",
    "/usr/share/icu",
    "/usr/share/zoneinfo",
    "/System/Library",
    "/System/Volumes/Preboot/Cryptexes",
    "/private/var/db/dyld",
    "/private/var/db/timezone",
    "/Library/Apple/System/Library",
    "/private/etc/ssl",
)


def _quote(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def profile(
    *,
    executable: Path,
    read: Sequence[Path],
    socket_path: Path | None,
    proxy_port: int,
    connect_ports: Sequence[int] = (),
) -> str:
    """The sandbox profile for one worker (or, with no ``socket_path``, one run of a reader)."""
    readable = [os.path.realpath(p) for p in (*read, executable.parent)]
    lines = [
        "(version 1)",
        "(deny default)",
        f"(allow process-exec (literal {_quote(os.path.realpath(executable))}))",
        "(allow signal (target self))",
        "(allow sysctl-read)",
        "(allow file-read-metadata)",
        "(allow file-read*",
        *(f"  (subpath {_quote(p)})" for p in (*_SYSTEM_READ, *readable)),
        # The root directory itself (not what is under it): Node lists it as it starts.
        '  (literal "/") (literal "/dev/null") (literal "/dev/urandom") (literal "/dev/random")',
        '  (literal "/dev/dtracehelper"))',
        '(allow file-write-data (literal "/dev/null") (literal "/dev/dtracehelper"))',
        '(allow file-ioctl (literal "/dev/dtracehelper"))',
        "(allow mach-lookup",
        '  (global-name "com.apple.system.notification_center")',
        '  (global-name "com.apple.system.logger"))',
        "(allow ipc-posix-shm-read-data ipc-posix-shm-read-metadata)",
        "(allow system-socket)",
        "(allow network-outbound",
        *(
            [f"  (remote unix-socket (path-literal {_quote(os.path.realpath(socket_path))}))"]
            if socket_path is not None
            else []
        ),
        *(f'  (remote ip "localhost:{port}")' for port in connect_ports),
        f'  (remote ip "localhost:{proxy_port}"))',
    ]
    return "\n".join(lines) + "\n"


def confined_argv(
    argv: Sequence[str],
    *,
    executable: Path,
    read: Sequence[Path],
    socket_path: Path | None,
    proxy_port: int,
    connect_ports: Sequence[int] = (),
) -> list[str]:
    """``argv`` run under the sandbox."""
    if not SANDBOX_EXEC.is_file():
        raise SandboxUnavailable(f"{SANDBOX_EXEC} is not on this host")
    text = profile(
        executable=executable,
        read=read,
        socket_path=socket_path,
        proxy_port=proxy_port,
        connect_ports=connect_ports,
    )
    return [str(SANDBOX_EXEC), "-p", text, *argv]
