# Copyright (c) 2026 Kenneth Stott
# Canary: 65a54b42-16e0-4d65-9cd7-6b8aab148bfc
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A worker's host, started inside the operating system's sandbox (REQ-1984, REQ-1993).

Every runtime but Deno (which keeps to its own permissions) is started through here: on macOS
under ``sandbox-exec`` (:mod:`provisa.codesource.macos_sandbox`), on Linux through the Landlock
and seccomp launcher (:mod:`provisa.codesource.linux_sandbox`), which needs the worker's socket
handed down already connected. Either way it reads only the directories named, writes nothing,
and reaches the network only through the egress proxy. Elsewhere (Windows) a code source is
refused by name: a worker never runs unsandboxed."""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import platform
import sys
import threading
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

from provisa.codesource import macos_sandbox
from provisa.codesource.egress import EgressProxy
from provisa.codesource.linux_sandbox import SandboxUnavailable, launcher_argv
from provisa.codesource.worker import HostCommand

_proxies: dict[frozenset[str], EgressProxy] = {}


def _minted_ports() -> tuple[int, ...]:
    """REQ-2226: the minted-token listener, the one port of Provisa's a worker may reach. A worker
    only: a reader imports the code to read its types and has no call to read a table for."""
    from provisa.core import settings_registry

    port = settings_registry.value("server.minted_pgwire_port")
    return (port,) if port else ()


_proxies_lock = threading.Lock()


def egress_proxy(allow: Iterable[str]) -> EgressProxy:
    """The process's egress proxy for this allow-list, started on first use."""
    key = frozenset(a.strip().lower() for a in allow if a.strip())
    with _proxies_lock:
        proxy = _proxies.get(key)
        if proxy is None:
            proxy = _proxies[key] = EgressProxy(key)
        return proxy


def sandboxed(
    argv: Callable[[str], Sequence[str]],
    *,
    executable: Path,
    read: Sequence[Path],
    env: dict[str, str],
    cwd: Path,
    egress: Iterable[str],
) -> HostCommand:
    """The host command ``argv`` (given the socket's address), confined to reading ``read`` and
    the runtime ``executable``'s own directory, with the network only through the egress proxy
    and, for a call given a table (REQ-2226), to the minted-token listener's one port."""
    proxy = egress_proxy(egress)
    env = {**env, **proxy.environment()}
    minted = _minted_ports()
    system = platform.system()
    if system == "Darwin":

        def _macos(address: str) -> list[str]:
            socket_path = Path(address)
            return macos_sandbox.confined_argv(
                argv(address),
                executable=executable,
                read=[*read, socket_path.parent],
                socket_path=socket_path,
                proxy_port=proxy.port,
                connect_ports=minted,
            )

        return HostCommand(argv=_macos, env=env, cwd=cwd)
    if system == "Linux":
        readable = [str(p) for p in (*read, executable.parent)]

        def _linux(address: str) -> list[str]:
            return launcher_argv(
                sys.executable,
                read=readable,
                proxy_port=proxy.port,
                argv=argv(address),
                connect_ports=minted,
            )

        return HostCommand(argv=_linux, env=env, cwd=cwd, inherit_socket=True)
    raise SandboxUnavailable(f"a code source cannot run on {system}: it has no code-source sandbox")


def sandboxed_once(
    argv: Sequence[str],
    *,
    executable: Path,
    read: Sequence[Path],
    env: dict[str, str],
    egress: Iterable[str],
) -> tuple[list[str], dict[str, str]]:
    """``argv`` and its environment for one confined run with no worker socket (a reader, which
    imports the code to read its types, and importing code is running it)."""
    proxy = egress_proxy(egress)
    env = {**env, **proxy.environment()}
    system = platform.system()
    if system == "Darwin":
        return macos_sandbox.confined_argv(
            argv, executable=executable, read=read, socket_path=None, proxy_port=proxy.port
        ), env
    if system == "Linux":
        readable = [str(p) for p in (*read, executable.parent)]
        return launcher_argv(sys.executable, read=readable, proxy_port=proxy.port, argv=argv), env
    raise SandboxUnavailable(f"a code source cannot run on {system}: it has no code-source sandbox")
