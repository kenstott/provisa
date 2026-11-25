# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2e47c1-5a8b-4f06-b3e9-71c0f4a8d25b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dev-mode code source's pgwire sidecar, launched and managed by Provisa (REQ-2267 part 3).

A built source's host serves its tables over PostgreSQL itself (provisa/codehost/pgfront.py). A
dev-mode source's code runs in this process's worker pool, so its PostgreSQL face is a process of
its own, provisa/codehost/sidecar.py, launched like the other pgwire replicas
(provisa/federation/pgwire_replica.py): a stable port per source from a :class:`PortAllocator`,
a health check, and a stop that returns once the port is free.

THE BRIDGE. The sidecar's worker is this process. It is started with :data:`CONNECTIONS`
connections this process keeps the other ends of, and on each this process speaks the host's side
of the code-source protocol (provisa/codesource/worker.py): it says hello, and each ``call`` the
sidecar makes is run on the source's worker from the pool (``Prepared.worker``), its rows sent back
as the worker gives them, no more than the call's ``window`` ahead of the sidecar's credits. A
``cancel`` closes the call, which cancels it in the worker. So a read through the sidecar is the
same call, on the same worker, as every other read of the source.

LIFECYCLE. :func:`ensure_sidecar` starts a source's sidecar once (again after it has ended, or when
the source's code has changed) and returns where it listens; :func:`stop_sidecar` stops it, which
``source.forget`` does when the source is edited, switched to built, or deleted;
:func:`stop_all_sidecars` stops every one at shutdown. A built source has no sidecar: its host is
its PostgreSQL face.
"""

# Requirements: REQ-2267
from __future__ import annotations

import collections
import json
import socket
import subprocess  # nosec B404 - argv-list invocation of Provisa's own sidecar program
import sys
import threading
import time
import traceback
from collections.abc import Callable, Generator
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from provisa.codehost import front
from provisa.codehost.sidecar import LISTENING
from provisa.codesource.names import name_tables
from provisa.codesource.worker import ABSENT, PROTOCOL, Worker, decode
from provisa.federation.pgwire_replica import (
    SERVER_READY_SECONDS,
    SERVER_STOP_SECONDS,
    PortAllocator,
    PortPair,
    ServerExited,
    ServerLifecycleError,
    _port_is_free,
    _tcp_health,
    engine_visible_host,
)

if TYPE_CHECKING:
    from provisa.codesource.source import Prepared

#: The sidecar's program.
PROGRAM = Path(front.__file__).with_name("sidecar.py")
#: How many connections a sidecar has to its worker, each serving one call at a time.
CONNECTIONS = front.CONNECTIONS
#: How many of the sidecar's last stderr lines a failure names.
_STDERR_LINES = 40
#: How often a start looks whether its sidecar is listening.
_READY_POLL_S = 0.05


@dataclass(frozen=True)
class Endpoint:
    """Where a sidecar listens: ``host`` as this process reaches it, ``engine_host`` as an engine
    that is not on this host's loopback does (``pgwire_replica.engine_visible_host``)."""

    host: str
    port: int
    engine_host: str


class _Call:
    """One call the sidecar made: its credits and whether it was cancelled."""

    def __init__(self, window: int) -> None:
        self.credits = threading.Semaphore(window)
        self.cancelled = False


class _Bridge:
    """This process's side of one of the sidecar's connections: the host's side of the protocol,
    each call run on the source's worker."""

    def __init__(
        self, conn: socket.socket, worker: Callable[[], Worker], hello: dict, name: str
    ) -> None:
        self._conn = conn
        self._worker = worker
        self._send_lock = threading.Lock()
        self._calls: dict[int, _Call] = {}
        self._lock = threading.Lock()
        self._send(front.frame(hello))
        threading.Thread(target=self._read, name=name, daemon=True).start()

    def _send(self, data: bytes) -> None:
        with self._send_lock:
            self._conn.sendall(data)

    def _read(self) -> None:
        while True:
            try:
                body = front.read_body(self._conn)
            except OSError:
                body = None
            if body is None:
                self._cancel_all()
                return
            message = json.loads(body[1:])
            op = message["op"]
            if op == "call":
                call = _Call(int(message["window"]))
                with self._lock:
                    self._calls[message["id"]] = call
                threading.Thread(
                    target=self._serve,
                    args=(message, call),
                    name="pgwire-sidecar-call",
                    daemon=True,
                ).start()
            elif op == "credit":
                with self._lock:
                    call = self._calls.get(message["id"])
                if call is not None:
                    for _ in range(int(message["n"])):
                        call.credits.release()
            elif op == "cancel":
                self._cancel(message["id"])
            elif op == "ping":
                self._send(front.frame({"op": "pong", "id": message["id"]}))
            else:
                raise ValueError(f"the pgwire sidecar sent an unknown message {op!r}")

    def _cancel(self, call_id: int) -> None:
        with self._lock:
            call = self._calls.get(call_id)
        if call is not None:
            call.cancelled = True
            call.credits.release()  # a call waiting for a credit wakes to find itself cancelled

    def _cancel_all(self) -> None:
        with self._lock:
            ids = list(self._calls)
        for call_id in ids:
            self._cancel(call_id)

    def _serve(self, message: dict, call: _Call) -> None:
        call_id = message["id"]
        batches: Generator[bytes, None, None] | None = None
        try:
            batches = self._worker().batches(
                message["export"],
                message["kind"],
                [_argument(a) for a in message["args"]],
                message["names"],
                schema=message["schema"],
                takes=message.get("takes"),
                context=decode(message["context"]) if "context" in message else None,
            )
            for ipc in batches:
                call.credits.acquire()
                if call.cancelled:
                    return
                self._send(front.rows_frame(call_id, ipc))
            if not call.cancelled:
                self._send(front.frame({"op": "end", "id": call_id}))
        except Exception as exc:  # noqa: BLE001 -- the call's failure, sent to the sidecar's client
            if not call.cancelled:
                self._failed(call_id, exc)
        finally:
            if batches is not None:
                batches.close()  # a call left before its end is cancelled in the worker
            with self._lock:
                self._calls.pop(call_id, None)

    def _failed(self, call_id: int, exc: Exception) -> None:
        try:
            self._send(
                front.frame(
                    {
                        "op": "error",
                        "id": call_id,
                        "message": str(exc),
                        "stack": "".join(traceback.format_exception(exc)),
                    }
                )
            )
        except OSError:
            return  # the sidecar has gone: there is no one left to tell

    def close(self) -> None:
        # On Linux, close() while another thread is blocked on recv() does not
        # deliver EOF to the peer: the kernel keeps the socket alive for the
        # blocked syscall.  shutdown(SHUT_RDWR) tears down the connection
        # immediately regardless of blocking threads, so the sidecar sees EOF
        # and calls os._exit(3) as designed.
        try:
            self._conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # already closed or not connected
        self._conn.close()


def _argument(value: Any) -> Any:
    """A wire argument as the pool's worker takes it: an absent one as :data:`ABSENT`."""
    if value == {"$absent": True}:
        return ABSENT
    return decode(value)


class Sidecar:
    """One dev-mode source's sidecar: started, health-checked and stopped. ``worker`` gives the
    source's worker from the pool, at each call; ``names`` are its name tables."""

    def __init__(
        self,
        *,
        source_id: str,
        content_hash: str,
        names: dict,
        worker: Callable[[], Worker],
        ports: PortPair,
        bounds: dict[str, int],
        spill_dir: Path,
    ) -> None:
        self.source_id = source_id
        self.content_hash = content_hash
        self._names = names
        self._worker = worker
        self._ports = ports
        self._bounds = bounds
        self._spill_dir = spill_dir
        self._process: subprocess.Popen | None = None
        self._bridges: list[_Bridge] = []
        self._stderr: collections.deque[str] = collections.deque(maxlen=_STDERR_LINES)
        self._stderr_thread: threading.Thread | None = None
        self._listening = threading.Event()

    @property
    def ports(self) -> PortPair:
        return self._ports

    @property
    def endpoint(self) -> Endpoint:
        return Endpoint(
            self._ports.pgwire_host, self._ports.pgwire_port, engine_visible_host(self._ports)
        )

    def command(self, connections: str) -> list[str]:
        return [
            sys.executable,
            str(PROGRAM),
            str(self._ports.pgwire_port),
            self._ports.pgwire_host,
            str(self._spill_dir),
            json.dumps(self._bounds),
            connections,
        ]

    def start(self, timeout: float = SERVER_READY_SECONDS) -> None:
        """Launch the sidecar and wait until it listens; :class:`ServerExited` naming what it
        said when it ends first, :class:`ServerLifecycleError` when it does not listen in time."""
        import socket

        if self._process is not None:
            raise ServerLifecycleError(
                f"the pgwire sidecar of {self.source_id!r} on port {self._ports.pgwire_port} "
                "is already running"
            )
        self._listening.clear()
        pairs = [socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM) for _ in range(CONNECTIONS)]
        theirs = [t.fileno() for _, t in pairs]
        try:
            self._process = subprocess.Popen(  # noqa: S603  # nosec B603 - Provisa's own program
                self.command(f"fd:{','.join(map(str, theirs))}"),
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                errors="replace",
                pass_fds=tuple(theirs),
            )
        finally:
            for _, t in pairs:
                t.close()
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr, name=f"pgwire-sidecar-stderr-{self.source_id}", daemon=True
        )
        self._stderr_thread.start()
        assert self._process.stdin is not None
        self._process.stdin.write(json.dumps(self._names))
        self._process.stdin.close()
        hello = {
            "op": "hello",
            "protocol": PROTOCOL,
            "code_hash": self.content_hash,
            "exports": sorted(self._names),
        }
        self._bridges = [
            _Bridge(mine, self._worker, hello, f"pgwire-sidecar-{self.source_id}-{i}")
            for i, (mine, _) in enumerate(pairs)
        ]
        until = time.monotonic() + timeout
        # Listening is its own word that it bound its port: a connection to the port alone could
        # be answered by whatever else holds it.
        while not (self._listening.is_set() and self.health()):
            if self._process.poll() is not None:
                code = self._process.returncode
                said = self._said()
                self.stop()
                raise ServerExited(self.source_id, code, said)
            if time.monotonic() >= until:
                self.stop()
                raise ServerLifecycleError(
                    f"the pgwire sidecar of {self.source_id!r} did not listen on port "
                    f"{self._ports.pgwire_port} within {timeout:g}s"
                )
            time.sleep(_READY_POLL_S)

    def alive(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def health(self) -> bool:
        """Whether the started sidecar is running and accepting connections."""
        if self._process is None:
            raise ServerLifecycleError("the pgwire sidecar was health-checked before its start")
        return self.alive() and _tcp_health(self._ports.pgwire_host, self._ports.pgwire_port)

    def stop(self) -> None:
        """End the sidecar and wait until its port is free (a no-op when it is not running)."""
        process = self._process
        if process is None:
            return
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=SERVER_STOP_SECONDS)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=SERVER_STOP_SECONDS)
        for bridge in self._bridges:
            bridge.close()
        self._bridges = []
        self._process = None
        if not self._listening.is_set():
            return  # it never bound its port: whatever holds the port is not the sidecar
        deadline = time.monotonic() + SERVER_STOP_SECONDS
        while not _port_is_free(self._ports.pgwire_port):
            if time.monotonic() >= deadline:
                raise ServerLifecycleError(
                    f"the pgwire sidecar's port {self._ports.pgwire_port} is still bound "
                    f"{SERVER_STOP_SECONDS}s after it ended"
                )
            time.sleep(0.1)

    def _drain_stderr(self) -> None:
        process = self._process
        assert process is not None and process.stderr is not None
        for line in process.stderr:
            if line.startswith(LISTENING):
                self._listening.set()
            self._stderr.append(line.rstrip("\n"))

    def _said(self) -> str:
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=2)  # what it wrote before it exited
        return "\n".join(self._stderr)


# -- the process's sidecars -------------------------------------------------------------------


#: Each source's port, kept for the life of the process: a sidecar started again listens where an
#: engine's catalog already dials it.
_PORTS = PortAllocator()
_sidecars: dict[tuple[str, str], Sidecar] = {}  # org, source
_lock = threading.Lock()
_atexit_registered = False


def _bounds() -> dict[str, int]:
    from provisa.codesource.worker import duckdb_bounds

    return duckdb_bounds()


def _spill_dir(org_id: str, source_id: str) -> Path:
    from provisa.core.org_files import node_path

    return node_path("code-sidecar", source_id, org_id=org_id)


def ensure_sidecar(prepared: Prepared) -> Endpoint:
    """The dev-mode source's sidecar, started once -- again when it has ended or the source's code
    has changed -- and listening; where it listens. A built source is refused: its host is its
    PostgreSQL face."""
    if prepared.remote is not None:
        raise ValueError(
            f"source {prepared.source_id!r} is built: its host serves PostgreSQL itself"
        )
    key = (prepared.org_id, prepared.source_id)
    with _lock:
        held = _sidecars.get(key)
        if held is not None and held.alive() and held.content_hash == prepared.content_hash:
            return held.endpoint
        if held is not None:
            held.stop()
            del _sidecars[key]
        sidecar = Sidecar(
            source_id=prepared.source_id,
            content_hash=prepared.content_hash,
            names=name_tables(prepared.manifest),
            worker=prepared.worker,
            ports=_PORTS.allocate(key),
            bounds=_bounds(),
            spill_dir=_spill_dir(prepared.org_id, prepared.source_id),
        )
        sidecar.start()
        _sidecars[key] = sidecar
        _reap_on_exit()
        return sidecar.endpoint


def stop_sidecar(source_id: str) -> None:
    """Stop the sidecar of ``source_id``, in every org: its source was edited or deleted."""
    with _lock:
        ending = [_sidecars.pop(k) for k in [k for k in _sidecars if k[1] == source_id]]
    for sidecar in ending:
        sidecar.stop()


def stop_all_sidecars() -> None:
    """Stop every sidecar this process started (shutdown)."""
    with _lock:
        ending = list(_sidecars.values())
        _sidecars.clear()
    for sidecar in ending:
        sidecar.stop()


def _reap_on_exit() -> None:
    """This interpreter's exit stops its sidecars, however it exits short of a kill; a killed one's
    sidecars end when their connections close (sidecar.py). Under :data:`_lock`."""
    global _atexit_registered
    if _atexit_registered:
        return
    import atexit

    atexit.register(stop_all_sidecars)
    _atexit_registered = True


__all__ = [
    "Endpoint",
    "Sidecar",
    "ensure_sidecar",
    "stop_all_sidecars",
    "stop_sidecar",
]
