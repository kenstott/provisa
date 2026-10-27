# Copyright (c) 2026 Kenneth Stott
# Canary: 928a2077-86da-44df-8dd2-9a4f7ddaeb94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The worker a code source's calls run in, whatever its language (REQ-1984, REQ-1993).

One long-running process per code source per Provisa process, keyed by the source and the content
hash of its installed code (:class:`WorkerPool`). The process runs the language's host program
(Node's ``provisa/node/host.mjs``, Python's ``host.py``), which imports the code and serves calls
over a Unix domain socket in a private directory: Provisa listens, the host connects
:data:`CONNECTIONS` times. A socket, not the process's output, so nothing the code prints reaches
the protocol; what it prints to stderr is kept (the last lines) to name a crash.

A CONNECTION PER CONCURRENT CALL. Each call leases one of the worker's connections for its whole
run, and a call that finds every one leased waits for one (REQ-2100).

CREDIT. A call's host sends at most :data:`_QUEUED` rows frames (the call's ``window``) ahead of
the call's thread: each frame the thread takes gives one back (``credit``). So the function is
pulled at most a window of frames, and the one it is building, ahead of its reader, whatever the
sockets between them buffer, and a slow reader holds back its own function and no other call.

A frame is a 4-byte big-endian length, then that many bytes: a kind byte and its body. ``J`` is
a control message, UTF-8 JSON. ``A`` is rows: the call's id (8 bytes, big-endian) and an Arrow IPC
stream of them, built against the schema the call gave (:mod:`provisa.codesource.arrow_rows`).
Arrow is the only form rows take (REQ-2098). In a JSON message a timestamp travels as
``{"$date": iso}``, an integer outside JSON's exact range as ``{"$bigint": digits}``, a decimal as
``{"$decimal": digits}``, a calendar date with no time as ``{"$day": iso}``. An argument the read
does not give is ``{"$absent": true}`` (:data:`ABSENT`), and ``names`` are the parameters' names in
the order ``args`` gives them, for a host whose language passes arguments by name.

    Provisa -> host  {op: "call", id, export, kind: "table"|"command"|"stream"|"columns", args, names,
                      schema,  -- the rows' Arrow schema, base64 IPC; null for a single value
                      traceparent,  -- the request's trace (W3C), or null (REQ-2106)
                      window,  -- how many rows frames the host may send before a credit
                      takes, context}  -- a @table or @stream function's: what it takes, its
                                          inputs (made of args) and its read context, in order
                     {op: "credit", id, n}  -- the call's thread took n of its rows frames
                     {op: "call", id, export, kind: "table", calls: [{args, names}, ...], schema,
                      traceparent}  -- REQ-2099: a table called once per argument set, in
                                       order, in the host, its rows sent under the one call id
                     {op: "cancel", id}      {op: "ping", id}
    host -> Provisa  {op: "hello", protocol, code_hash, exports}
                     rows (an Arrow frame)   {op: "value", id, value}
                     {op: "end", id, cpu_ms}  {op: "error", id, message, stack}  {op: "pong", id}
    Provisa -> host  {op: "refused", message}, in place of any call, before the socket closes

REQ-2106: every call is a stage of its request (otel_compat.stage, REQ-1910) -- its function,
kind, argument count (never a value), rows, batches, duration, the worker's CPU (the host's
``cpu_ms``), local or remote, and the code's hash -- and the request's traceparent goes with it,
so the code's own tracing joins the request's trace. While a call runs, Provisa's services
(provisa/codesource/services.py) answer the worker's requests naming it (``X-Provisa-Call``) in
the request's scope: their stages report to the same request.

The hello names the protocol the host speaks (:data:`PROTOCOL`) and the content hash of the code
it runs (``PROVISA_CODE_HASH`` in its environment: the worker gives it, an image built for a
remote host carries it). A host that speaks another protocol, or runs other code than the source
registered, is refused by name (REQ-2098).

A call blocks its request's thread, as every driver call does (REQ-1882), and is cancelled through
the request's deadline (:func:`provisa.core.request_deadline.cancel_on_deadline`): the host is
told to stop the call, and the waiting thread is released at once. A crash fails every call in
flight, naming the source and the export; the next call starts a new process.
"""

# Requirements: REQ-1984, REQ-1993, REQ-1882, REQ-2104
from __future__ import annotations

import collections
import itertools
import json
import os
import queue
import shutil
import socket
import struct
import subprocess  # nosec B404 - argv-list invocation only, never shell=True
import tempfile
import threading
import time
from collections.abc import Callable, Generator, Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

from provisa.core import request_deadline
from provisa.core.request_context import current_acting_role
from provisa.otel_compat import CarriedScope, carried_scope, current_traceparent, get_tracer, stage

if TYPE_CHECKING:
    from provisa.codesource.services import ServiceGrant

#: The protocol the hosts speak; a host built for another one is refused by name (REQ-2098).
PROTOCOL = 4

#: REQ-2100: how many connections a worker has, each serving one call at a time.
CONNECTIONS = 8
#: The variable a host reads how many times to connect to its socket path from.
CONNECTIONS_ENV = "PROVISA_HOST_CONNECTIONS"
#: How many of a call's frames its connection holds for the call's thread before it stops reading.
_QUEUED = 4
#: How often a reader waiting on a full queue looks whether its lane is being shut.
_DELIVER_POLL_S = 0.1


def duckdb_bounds() -> dict[str, int]:
    """REQ-2100: the operator's bounds on a code source's DuckDB -- its memory, threads and spill
    cap -- as a code host is given them, with pairing and with every call."""
    from provisa.core import settings_registry

    return {
        "memory_mb": int(settings_registry.value("codesource.duckdb_memory_mb")),
        "threads": int(settings_registry.value("codesource.duckdb_threads")),
        "spill_cap_mb": int(settings_registry.value("codesource.duckdb_spill_cap_mb")),
    }


#: The variable a host reads the content hash of its code from, which its hello names.
CODE_HASH_ENV = "PROVISA_CODE_HASH"

#: How long a started host has to connect and say hello.
START_TIMEOUT_S = 30.0

#: How often a start checks that its host is still coming while it waits for it to connect.
_ACCEPT_POLL_S = 0.05

#: REQ-2098: a remote host that failed is dialled again after this, doubling to the most.
_REMOTE_BACKOFF_S = 1.0
_REMOTE_BACKOFF_MAX_S = 60.0

#: How many of the host's last stderr lines a crash names.
STDERR_LINES = 40

_tracer = get_tracer(__name__)

#: REQ-2106: the call a worker's services request names, by its worker's services token and the
#: call's id: the request's tracing scope, which the services' stages report into.
_scopes: dict[tuple[str, int], CarriedScope] = {}
#: REQ-2133: the role the request a call serves acts as, beside its scope: the dq service answers
#: what the meta domain shows that role.
_roles: dict[tuple[str, int], str | None] = {}
_scopes_lock = threading.Lock()


def call_scope(token: str, call_id: int) -> CarriedScope | None:
    """The tracing scope of the call ``call_id`` of the worker holding ``token``, while it runs."""
    with _scopes_lock:
        return _scopes.get((token, call_id))


def call_role(token: str, call_id: int) -> str | None:
    """The role the request served by call ``call_id`` of the worker holding ``token`` acts as,
    while the call runs; None when it names none or is not running."""
    with _scopes_lock:
        return _roles.get((token, call_id))


_HEAD = struct.Struct(">I")
_ID = struct.Struct(">Q")
JSON_FRAME = b"J"
ARROW_FRAME = b"A"
_EXACT = 2**53 - 1


@dataclass(frozen=True)
class HostCommand:
    """How a language starts its host for one source. ``argv`` is given the socket's address,
    which the host reads as its first argument: a path (inside a private directory the worker
    makes for it, which a sandbox names), or with ``inherit_socket`` ``fd:N``, a socket already
    connected and inherited as descriptor N (the Linux sandbox refuses the host any new socket but
    TCP, so it cannot connect one). The process gets exactly ``env``, in ``cwd``, and with
    ``services`` the address of Provisa's services to code, under a token issued as the process
    starts and revoked when it stops (REQ-2104)."""

    argv: Callable[[str], Sequence[str]]
    env: dict[str, str]
    cwd: Path
    inherit_socket: bool = False
    services: "ServiceGrant | None" = None

    def argv_for(self, address: str) -> list[str]:
        return list(self.argv(address))


class WorkerFailed(Exception):
    """The worker could not serve the call: it did not start, or it ended while serving it."""

    def __init__(self, source_id: str, message: str, *, export: str | None = None) -> None:
        where = f"source {source_id!r}" + (f", export {export!r}" if export else "")
        super().__init__(f"{where}: {message}")
        self.source_id = source_id
        self.export = export


class CodeFailed(Exception):
    """The source's own code raised: its message and stack, named by the source and export."""

    def __init__(self, source_id: str, export: str, message: str, stack: str) -> None:
        super().__init__(f"source {source_id!r}, export {export!r}: {message}")
        self.source_id = source_id
        self.export = export
        self.stack = stack


# -- values on the wire -----------------------------------------------------------------------------


class _Absent:
    """An argument the read does not give: the function's own default applies."""

    def __repr__(self) -> str:
        return "ABSENT"


#: An optional parameter the read gives no value for: sent as absent, so the function's own
#: default applies (JavaScript's ``undefined``; Python's keyword left out), never as null.
ABSENT = _Absent()


def encode(value: Any) -> Any:
    """``value`` as the wire carries it."""
    if value is ABSENT:
        return {"$absent": True}
    if isinstance(value, bool) or value is None or isinstance(value, (str, float)):
        return value
    if isinstance(value, int):
        return value if -_EXACT <= value <= _EXACT else {"$bigint": str(value)}
    if isinstance(value, Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, datetime):
        return {"$date": value.isoformat()}
    if isinstance(value, date):
        return {"$day": value.isoformat()}
    if isinstance(value, dict):
        return {str(k): encode(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [encode(v) for v in value]
    raise TypeError(f"{type(value).__name__} has no form on the code-source wire")


def decode(value: Any) -> Any:
    """A wire value as Python's: a timestamp as an aware ``datetime``, a big integer as ``int``, a
    decimal as ``Decimal``."""
    if isinstance(value, list):
        return [decode(v) for v in value]
    if isinstance(value, dict):
        if len(value) == 1:
            if "$date" in value:
                return datetime.fromisoformat(value["$date"].replace("Z", "+00:00"))
            if "$day" in value:
                return date.fromisoformat(value["$day"])
            if "$bigint" in value:
                return int(value["$bigint"])
            if "$decimal" in value:
                return Decimal(value["$decimal"])
        return {k: decode(v) for k, v in value.items()}
    return value


def _frame(message: dict) -> bytes:
    body = JSON_FRAME + json.dumps(message, separators=(",", ":")).encode()
    return _HEAD.pack(len(body)) + body


def _read_exact(conn: socket.socket, size: int) -> bytes | None:
    chunks, left = [], size
    while left:
        chunk = conn.recv(min(left, 1 << 20))
        if not chunk:
            return None
        chunks.append(chunk)
        left -= len(chunk)
    return b"".join(chunks)


def _read_frame(conn: socket.socket) -> dict | None:
    """The next message: a control message as sent, rows as ``{op: "rows", id, arrow}``."""
    head = _read_exact(conn, _HEAD.size)
    if head is None:
        return None
    body = _read_exact(conn, _HEAD.unpack(head)[0])
    if body is None:
        return None
    kind, rest = body[:1], body[1:]
    if kind == ARROW_FRAME:
        return {"op": "rows", "id": _ID.unpack(rest[: _ID.size])[0], "arrow": rest[_ID.size :]}
    if kind != JSON_FRAME:
        raise ValueError(f"a frame of unknown kind {kind!r}")
    return json.loads(rest)


def refusal(hello: dict, content_hash: str) -> str | None:
    """Why a host's hello is refused, or None: another protocol, or other code than the source
    registered (REQ-2098)."""
    spoken = hello.get("protocol")
    if spoken != PROTOCOL:
        return (
            f"its host speaks protocol {spoken}, and this Provisa speaks {PROTOCOL}: "
            "run a host built for this Provisa version"
        )
    held = hello.get("code_hash")
    if held != content_hash:
        return f"its host runs code {held}, not the {content_hash} the source registered"
    return None


# -- one worker -------------------------------------------------------------------------------------


class _Cancelled:
    """Put on a call's queue when its deadline cancels it: the waiting thread is released."""

    def __init__(self, call_id: int) -> None:
        self.call_id = call_id


class _Ended:
    """Put on every call's queue when the worker ends."""

    def __init__(self, message: str) -> None:
        self.message = message


class _Lane:
    """One of a worker's connections, leased by one call at a time. What the host sends under the
    call's id waits in ``waiting`` for the call's thread, at most :data:`_QUEUED` frames; anything
    else (a call that has closed, still sending) is dropped."""

    def __init__(self, conn: socket.socket) -> None:
        self.conn = conn
        self.waiting: queue.Queue = queue.Queue(maxsize=_QUEUED)
        self._send_lock = threading.Lock()
        self._lock = threading.Lock()
        self._call_id: int | None = None
        #: Set once the lane's reader has left its socket (Worker._read): only then is it closed.
        self.read_done = threading.Event()
        #: Set once the lane is being shut: its reader takes no more of what the host sends.
        self._shutting = threading.Event()

    def send(self, message: dict) -> None:
        with self._send_lock:
            self.conn.sendall(_frame(message))

    def deliver(self, message: dict) -> None:
        """A frame the host sent: to the leasing call's thread, waiting while its queue is full."""
        with self._lock:
            current = self._call_id
        if current is None or message.get("id") != current:
            return
        while not self._shutting.is_set():
            try:
                self.waiting.put(message, timeout=_DELIVER_POLL_S)
                return
            except queue.Full:
                continue  # the call's thread has not taken its rows yet

    def force(self, item: object) -> None:
        """``item`` put on the queue whatever it holds: what a cancel or an end puts, which the
        call's thread takes next; the rows it displaces are not wanted any more."""
        while True:
            try:
                self.waiting.put_nowait(item)
                return
            except queue.Full:
                try:
                    self.waiting.get_nowait()
                except queue.Empty:
                    pass  # the call's thread took one meanwhile: there is room now

    def lease(self, call_id: int | None) -> None:
        """The lane's call from now (None: no call), what an earlier one left in its queue gone."""
        with self._lock:
            self._call_id = call_id
        while True:
            try:
                self.waiting.get_nowait()
            except queue.Empty:
                return

    def shut(self) -> None:
        """Shut the socket, which wakes the reader, and close it once the reader has left it. A
        closed socket's number is the next one opened: a reader still in its recv would read that
        connection's bytes (a TLS handshake fails with a bad record MAC). A reader waiting on a
        full queue gives up its frame, the lane taking no more."""
        self._shutting.set()
        try:
            self.conn.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass  # already closed by the host's exit: nothing left to shut
        self.read_done.wait()
        self.conn.close()


class Worker:
    """One running host process and its socket."""

    def __init__(self, source_id: str, content_hash: str, command: HostCommand) -> None:
        self.source_id = source_id
        self.content_hash = content_hash
        self._command = command
        self._ids = itertools.count(1)
        #: Its connections, and those no call holds.
        self._lanes: list[_Lane] = []
        self._idle: list[_Lane] = []
        self._leases = threading.Condition()
        self._stderr: collections.deque[str] = collections.deque(maxlen=STDERR_LINES)
        self._ended: str | None = None
        self._dir: Path | None = None
        self._process: subprocess.Popen | None = None
        self._stderr_thread: threading.Thread | None = None
        self._services_token: str | None = None
        self.exports: list[str] = []

    # -- lifecycle ------------------------------------------------------------------------------

    def start(self, timeout: float = START_TIMEOUT_S) -> None:
        """Start the host and wait for its hello on each of its connections;
        :class:`WorkerFailed` when one does not come."""
        if self._command.inherit_socket:
            pairs = [
                socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM) for _ in range(CONNECTIONS)
            ]
            children = [child.fileno() for _, child in pairs]
            try:
                self._spawn(f"fd:{','.join(map(str, children))}", tuple(children))
            finally:
                for _, child in pairs:
                    child.close()
            conns = [mine for mine, _ in pairs]
        else:
            self._dir = Path(tempfile.mkdtemp(prefix="provisa-code-"))
            self._dir.chmod(0o700)
            path = self._dir / "worker.sock"
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                listener.bind(str(path))
                listener.listen(CONNECTIONS)
                listener.settimeout(_ACCEPT_POLL_S)
                self._spawn(str(path), ())
                conns = [self._accept(listener, timeout) for _ in range(CONNECTIONS)]
            finally:
                listener.close()
        hello: dict = {}
        for conn in conns:
            hello = self._hello(conn, timeout, conns)
        self._attach(conns, hello, f"code-worker-{self.source_id}")

    def _hello(self, conn: socket.socket, timeout: float, conns: list[socket.socket]) -> dict:
        """The host's hello on ``conn``, checked; the worker stopped, and ``conns`` closed, when it
        does not come or is refused."""

        def close() -> None:
            for each in conns:
                each.close()

        conn.settimeout(timeout)
        try:
            hello = _read_frame(conn)
        except TimeoutError:
            close()
            self.stop()
            raise WorkerFailed(
                self.source_id,
                f"its host did not say it was ready within {timeout:g}s{self._said()}",
            ) from None
        if hello is None or hello.get("op") != "hello":
            close()
            self.stop()
            raise WorkerFailed(self.source_id, f"its host ended before it was ready{self._said()}")
        refused = refusal(hello, self.content_hash)
        if refused is not None:
            try:
                conn.sendall(_frame({"op": "refused", "message": refused}))
            except OSError:
                pass  # the host has gone already: the refusal is still raised by name below
            close()
            self.stop()
            raise WorkerFailed(self.source_id, refused)
        conn.settimeout(None)
        return hello

    def _attach(self, conns: list[socket.socket], hello: dict, name: str) -> None:
        """The worker's connections, each read on a thread of its own, every one idle."""
        self._lanes = [_Lane(conn) for conn in conns]
        with self._leases:
            self._idle = list(self._lanes)
        self.exports = list(hello.get("exports") or [])
        for i, lane in enumerate(self._lanes):
            threading.Thread(
                target=self._read, args=(lane,), name=f"{name}-{i}", daemon=True
            ).start()

    def _spawn(self, address: str, pass_fds: tuple[int, ...]) -> None:
        env = {
            **self._command.env,
            CODE_HASH_ENV: self.content_hash,
            CONNECTIONS_ENV: str(CONNECTIONS),
        }
        if self._command.services is not None:
            from provisa.codesource import services

            self._services_token = services.issue(self._command.services)
            env[services.SERVICES_ENV] = services.address(self._services_token)
        self._process = subprocess.Popen(  # nosec B603 - the pinned runtime and Provisa's host
            self._command.argv_for(address),
            cwd=self._command.cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            errors="replace",
            pass_fds=pass_fds,
        )
        self._stderr_thread = threading.Thread(
            target=self._drain_stderr, name=f"code-worker-stderr-{self.source_id}", daemon=True
        )
        self._stderr_thread.start()

    def _accept(self, listener: socket.socket, timeout: float) -> socket.socket:
        """The host's connection; a host that exits first, or does not connect in time, is named
        with what it said."""
        process = self._process
        assert process is not None
        until = time.monotonic() + timeout
        while True:
            try:
                conn, _ = listener.accept()
                return conn
            except TimeoutError:
                pass  # not yet: whether the host is still coming is checked below
            if process.poll() is not None:
                self.stop()
                raise WorkerFailed(
                    self.source_id, f"its host ended before it connected{self._said()}"
                )
            if time.monotonic() >= until:
                self.stop()
                raise WorkerFailed(
                    self.source_id, f"its host did not connect within {timeout:g}s{self._said()}"
                )

    def alive(self) -> bool:
        return self._ended is None and self._process is not None and self._process.poll() is None

    def stop(self) -> None:
        """End the host: its sockets are closed (the host exits when one closes), then the
        process."""
        self._end("the worker was stopped")
        for lane in self._lanes:
            lane.shut()
        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
        if self._dir is not None:
            shutil.rmtree(self._dir, ignore_errors=True)

    def _drain_stderr(self) -> None:
        process = self._process
        assert process is not None and process.stderr is not None
        for line in process.stderr:
            self._stderr.append(line.rstrip("\n"))

    def _said(self) -> str:
        if self._process is not None and self._process.poll() is not None:
            if self._stderr_thread is not None:
                self._stderr_thread.join(timeout=2)  # what it wrote before it exited
            tail = "\n".join(self._stderr)
            return f" (exit {self._process.returncode})" + (f":\n{tail}" if tail else "")
        return ""

    def _end(self, message: str) -> None:
        if self._services_token is not None:
            from provisa.codesource import services

            services.revoke(self._services_token)  # an ended worker's token reads nothing
        with self._leases:
            if self._ended is None:
                self._ended = message
            ended = self._ended
            self._leases.notify_all()  # a call waiting for a connection fails, naming the end
        for lane in self._lanes:
            lane.force(_Ended(ended))

    def _read(self, lane: _Lane) -> None:
        try:
            while (message := _read_frame(lane.conn)) is not None:
                lane.deliver(message)
        except OSError:
            pass  # the socket closed under the read: the worker has ended, said below
        finally:
            lane.read_done.set()
        if self._process is not None:
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass  # still running with its socket closed: stop() ends it
        self._end(f"its host ended{self._said()}")

    # -- calls ----------------------------------------------------------------------------------

    def _send(self, lane: _Lane, message: dict) -> None:
        if self._ended is not None:
            raise WorkerFailed(self.source_id, self._ended)
        lane.send(message)

    def _lease(self, timeout: float | None = None) -> _Lane | None:
        """A connection no call holds, waited for while every one is leased: until the request's
        deadline, or ``timeout`` (None when it passes)."""
        woken = threading.Event()

        def wake() -> None:
            woken.set()
            with self._leases:
                self._leases.notify_all()

        with request_deadline.cancel_on_deadline(wake):
            with self._leases:
                ready = self._leases.wait_for(
                    lambda: self._idle or self._ended is not None or woken.is_set(), timeout
                )
                if self._ended is not None:
                    raise WorkerFailed(self.source_id, self._ended)
                if not ready:
                    return None
                lane = None if woken.is_set() else self._idle.pop()
        if lane is None:
            # The deadline's own error is raised on the way out of cancel_on_deadline; this is
            # reached only when it did not fire (a cancel with no budget passed).
            raise WorkerFailed(self.source_id, "the call was cancelled")
        return lane

    def _open(self) -> tuple[_Lane, int]:
        if not self._lanes:
            raise WorkerFailed(self.source_id, self._ended or "the worker is not started")
        lane = self._lease()
        assert lane is not None  # no timeout: a connection, or a refusal raised
        call_id = next(self._ids)
        lane.lease(call_id)
        return lane, call_id

    def _close(self, lane: _Lane, call_id: int) -> None:
        lane.lease(None)
        with self._leases:
            self._idle.append(lane)
            self._leases.notify()
        if self._services_token is not None:
            with _scopes_lock:
                _scopes.pop((self._services_token, call_id), None)
                _roles.pop((self._services_token, call_id), None)

    def _cancel(self, lane: _Lane, call_id: int) -> None:
        """Tell the host to stop the call, and release the thread waiting for it. Called from the
        deadline's watchdog thread."""
        lane.force(_Cancelled(call_id))
        try:
            self._send(lane, {"op": "cancel", "id": call_id})
        except (OSError, WorkerFailed):
            pass  # the worker has ended: there is no call left to stop

    def _next(self, lane: _Lane, call_id: int, export: str) -> dict:
        with request_deadline.cancel_on_deadline(lambda: self._cancel(lane, call_id)):
            while True:
                message = lane.waiting.get()
                # What an earlier call on the connection left behind is not this call's.
                if isinstance(message, _Cancelled) and message.call_id != call_id:
                    continue
                if isinstance(message, dict) and message.get("id") != call_id:
                    continue
                break
        if isinstance(message, _Cancelled):
            # The deadline's own error is raised on the way out of cancel_on_deadline; this is
            # reached only when it did not fire (a cancel with no budget passed).
            raise WorkerFailed(self.source_id, "the call was cancelled", export=export)
        if isinstance(message, _Ended):
            raise WorkerFailed(self.source_id, message.message, export=export)
        if message["op"] == "error":
            raise CodeFailed(self.source_id, export, message["message"], message.get("stack", ""))
        if message["op"] == "rows":
            # Taken: the host may send one more of the call's frames.
            try:
                self._send(lane, {"op": "credit", "id": call_id, "n": 1})
            except (OSError, WorkerFailed):
                pass  # the worker has ended: the call fails on its next message, naming it
        return message

    def rows(
        self,
        export: str,
        kind: str,
        args: Sequence[Any],
        names: Sequence[str] = (),
        *,
        schema: str,
        takes: Sequence[str] | None = None,
        context: dict | None = None,
    ) -> Iterator[list[dict]]:
        """A table's or a stream's rows, batch by batch, as the host sends them against
        ``schema`` (arrow_rows.schema_text). Closing the iterator before its end cancels the
        call in the host."""
        from contextlib import closing

        from provisa.codesource.arrow_rows import rows_of

        # Closed here, not by the collector: closing rows is what cancels the call in the host.
        with closing(
            self.batches(export, kind, args, names, schema=schema, takes=takes, context=context)
        ) as batches:
            for ipc in batches:
                yield rows_of(ipc)

    def batches(
        self,
        export: str,
        kind: str,
        args: Sequence[Any],
        names: Sequence[str] = (),
        *,
        schema: str,
        residual: dict | None = None,
        takes: Sequence[str] | None = None,
        context: dict | None = None,
    ) -> Generator[bytes, None, None]:
        """REQ-2100: :meth:`rows`, each batch as the Arrow IPC stream the host sent. With a
        ``residual`` (a remote host's front end, provisa/codehost/front.py), the batches are the
        statement's answer over the function's rows."""
        import pyarrow as pa

        if kind not in ("table", "stream"):
            raise ValueError(f"rows() serves a table or a stream, not a {kind}")
        with stage(_tracer, "codesource.call") as span:
            self._describe(span, export, kind, args)
            lane, call_id = self._open()
            finished = False
            rows = batches = 0
            try:
                frame = self._call_frame(
                    call_id, export, kind, args, names, schema, takes=takes, context=context
                )
                if residual is not None:
                    frame["residual"] = residual
                self._send(lane, frame)
                while True:
                    message = self._next(lane, call_id, export)
                    if message["op"] == "end":
                        finished = True
                        span.set_attribute("codesource.worker_cpu_ms", message.get("cpu_ms") or 0)
                        return
                    if message["op"] == "rows":
                        ipc = message["arrow"]
                        rows += pa.ipc.open_stream(pa.py_buffer(ipc)).read_all().num_rows
                        batches += 1
                        yield ipc
            finally:
                span.set_attribute("codesource.rows", rows)
                span.set_attribute("codesource.batches", batches)
                if not finished:
                    try:
                        self._send(lane, {"op": "cancel", "id": call_id})
                    except (OSError, WorkerFailed):
                        pass  # the worker has ended: there is no call left to stop
                self._close(lane, call_id)

    def rows_each(
        self,
        export: str,
        calls: Sequence[tuple[Sequence[Any], Sequence[str]]],
        *,
        schema: str,
        keyed: bool = False,
        takes: Sequence[str] | None = None,
        context: dict | None = None,
        contexts: Sequence[dict] | None = None,
    ) -> Iterator[list[dict]]:
        """A table's rows for each of ``calls`` (its arguments and their names), in order, from
        one call to the host (REQ-2099): the host calls the function once per argument set
        in-process, so a join that feeds a table many argument sets pays one round trip, not one
        per set. Closing the iterator before its end cancels the call in the host."""
        from provisa.codesource.arrow_rows import rows_of

        with stage(_tracer, "codesource.call") as span:
            self._describe(span, export, "table", calls[0][0] if calls else ())
            span.set_attribute("codesource.argument_sets", len(calls))
            lane, call_id = self._open()
            finished = False
            rows = batches = 0
            try:
                frame = self._call_frame(
                    call_id, export, "table", (), (), schema, takes=takes, context=context
                )
                del frame["args"], frame["names"]
                frame["calls"] = [
                    {"args": encode(list(args)), "names": list(names)} for args, names in calls
                ]
                if contexts is not None:
                    # REQ-2100: each call its own read context (a loader's batch of keys).
                    for call, given in zip(frame["calls"], contexts, strict=True):
                        call["context"] = encode(given)
                if keyed:
                    # REQ-2100: a loader call, answered with each key's rows, each with its $key.
                    frame["keyed"] = True
                self._send(lane, frame)
                while True:
                    message = self._next(lane, call_id, export)
                    if message["op"] == "end":
                        finished = True
                        span.set_attribute("codesource.worker_cpu_ms", message.get("cpu_ms") or 0)
                        return
                    if message["op"] == "rows":
                        batch = rows_of(message["arrow"])
                        rows, batches = rows + len(batch), batches + 1
                        yield batch
            finally:
                span.set_attribute("codesource.rows", rows)
                span.set_attribute("codesource.batches", batches)
                if not finished:
                    try:
                        self._send(lane, {"op": "cancel", "id": call_id})
                    except (OSError, WorkerFailed):
                        pass  # the worker has ended: there is no call left to stop
                self._close(lane, call_id)

    def command(
        self,
        export: str,
        args: Sequence[Any],
        names: Sequence[str] = (),
        *,
        schema: str | None = None,
    ) -> Any:
        """A command's result: its single value, or -- given its rows' ``schema`` -- the rows it
        returned, as a list."""
        from provisa.codesource.arrow_rows import rows_of

        with stage(_tracer, "codesource.call") as span:
            self._describe(span, export, "command", args)
            lane, call_id = self._open()
            batches = 0
            try:
                self._send(lane, self._call_frame(call_id, export, "command", args, names, schema))
                collected: list[dict] = []
                value: Any = None
                while (message := self._next(lane, call_id, export))["op"] != "end":
                    if message["op"] == "value":
                        value = decode(message["value"])
                    elif message["op"] == "rows":
                        collected.extend(rows_of(message["arrow"]))
                        batches += 1
                        value = collected
                span.set_attribute("codesource.worker_cpu_ms", message.get("cpu_ms") or 0)
                span.set_attribute("codesource.rows", len(collected))
                span.set_attribute("codesource.batches", batches)
                return value
            finally:
                self._close(lane, call_id)

    def columns(
        self, export: str, columns: Sequence[list], names: Sequence[str], *, schema: str
    ) -> list[dict]:
        """REQ-2131: a ``@columns`` function's results for one batch: ``columns`` are its input
        columns, each a list of the batch's values in row order, and the results come back as
        rows of its output ``schema``, in order."""
        from provisa.codesource.arrow_rows import rows_of

        with stage(_tracer, "codesource.call") as span:
            self._describe(span, export, "columns", columns)
            lane, call_id = self._open()
            try:
                self._send(
                    lane,
                    self._call_frame(
                        call_id, export, "columns", [list(c) for c in columns], names, schema
                    ),
                )
                results: list[dict] = []
                while (message := self._next(lane, call_id, export))["op"] != "end":
                    if message["op"] == "rows":
                        results.extend(rows_of(message["arrow"]))
                span.set_attribute("codesource.worker_cpu_ms", message.get("cpu_ms") or 0)
                span.set_attribute("codesource.rows", len(results))
                return results
            finally:
                self._close(lane, call_id)

    def _describe(self, span: Any, export: str, kind: str, args: Sequence[Any]) -> None:
        """What a call is, on its stage: never an argument's value."""
        span.set_attribute("codesource.source", self.source_id)
        span.set_attribute("codesource.function", export)
        span.set_attribute("codesource.kind", kind)
        span.set_attribute("codesource.arg_count", sum(1 for a in args if a is not ABSENT))
        span.set_attribute("codesource.host", self.where_kind)
        span.set_attribute("codesource.code_hash", self.content_hash)

    #: Where the worker runs: a local process, or a remote host (provisa/codesource/remote.py).
    where_kind = "local"

    def _call_frame(
        self,
        call_id: int,
        export: str,
        kind: str,
        args: Sequence[Any],
        names: Sequence[str],
        schema: str | None,
        *,
        takes: Sequence[str] | None = None,
        context: dict | None = None,
    ) -> dict:
        """The call's frame, with the request's trace; the call's scope held for the services
        requests that name it, until the call closes. ``takes`` (REQ-2100): what a @table or
        @stream function takes, its ``inputs`` (made of ``args``) and its read ``context``."""
        if self._services_token is not None:
            with _scopes_lock:
                _scopes[(self._services_token, call_id)] = carried_scope()
                _roles[(self._services_token, call_id)] = current_acting_role.get()
        extra: dict = {}
        if takes is not None:
            extra = {"takes": list(takes), "context": encode(context)}
        return {
            "op": "call",
            "id": call_id,
            "export": export,
            "kind": kind,
            "args": encode(list(args)),
            "names": list(names),
            "schema": schema,
            "traceparent": current_traceparent(),
            "window": _QUEUED,
            **extra,
        }

    def ping(self, timeout: float = 5.0) -> bool:
        """Whether the host answers on a connection, within ``timeout``."""
        until = time.monotonic() + timeout
        try:
            lane = self._lease(timeout)
        except WorkerFailed:
            return False
        if lane is None:
            return False  # every connection busy the whole time: no answer in time
        call_id = next(self._ids)
        lane.lease(call_id)
        try:
            self._send(lane, {"op": "ping", "id": call_id})
            while (left := until - time.monotonic()) > 0:
                try:
                    message = lane.waiting.get(timeout=left)
                except queue.Empty:
                    return False
                if isinstance(message, _Ended):
                    return False
                if isinstance(message, dict) and message.get("id") == call_id:
                    return message.get("op") == "pong"
            return False
        except (OSError, WorkerFailed):
            return False
        finally:
            self._close(lane, call_id)


# -- pulled on one thread -----------------------------------------------------------------------------


_PULLED_DONE = object()


def pulled(items: Iterator[Any], depth: int = _QUEUED) -> Iterator[Any]:
    """``items`` -- a call's batches -- pulled on one thread of their own, in the caller's context,
    and handed over through a queue of ``depth``: for a reader that takes them on threads of its own
    (DuckDB's scan of a RecordBatchReader), so the call's stage is entered and left on one thread,
    in the request it belongs to. Closing this ends the call (its generator is closed)."""
    import contextvars

    handed: queue.Queue = queue.Queue(maxsize=depth)
    stop = threading.Event()

    def pull() -> None:
        try:
            for item in items:
                while not stop.is_set():
                    try:
                        handed.put(item, timeout=0.1)
                        break
                    except queue.Full:
                        continue
                if stop.is_set():
                    break
        except BaseException as exc:  # noqa: BLE001 -- the call's failure, raised to its reader
            handed.put(exc)
            return
        finally:
            close = getattr(items, "close", None)
            if close is not None:
                close()
        handed.put(_PULLED_DONE)

    context = contextvars.copy_context()
    threading.Thread(target=context.run, args=(pull,), name="code-call-pull", daemon=True).start()
    try:
        while (item := handed.get()) is not _PULLED_DONE:
            if isinstance(item, BaseException):
                raise item
            yield item
    finally:
        stop.set()


# -- the pool ---------------------------------------------------------------------------------------


class WorkerPool:
    """The process's workers, as many per ``(source, content hash)`` as the source runs (REQ-2131:
    each its ``slot``), each started on first use. A worker whose process has ended is dropped and
    the next call starts another."""

    def __init__(self) -> None:
        self._workers: dict[tuple[str | None, str, str, int], Worker] = {}
        #: REQ-2098: a remote host's failed dials: (count, next try, why).
        self._failures: dict[tuple[str | None, str, str, int], tuple[int, float, str]] = {}
        self._lock = threading.Lock()

    def get(
        self,
        source_id: str,
        content_hash: str,
        command: Callable[[], HostCommand],
        *,
        org: str | None = None,
        slot: int = 0,
    ) -> Worker:
        """``org``: the org the worker serves; one org's worker never serves another's calls,
        since it holds that org's services grant (REQ-2104). ``slot``: which of the source's
        workers (REQ-2131)."""
        key = (org, source_id, content_hash, slot)
        with self._lock:
            worker = self._workers.get(key)
            if worker is not None and worker.alive():
                return worker
            if worker is not None:
                worker.stop()
            worker = Worker(source_id, content_hash, command())
            worker.start()
            self._workers[key] = worker
            return worker

    def remote(
        self,
        source_id: str,
        content_hash: str,
        org: str,
        make: Callable[[], "Worker"],
        *,
        slot: int = 0,
    ) -> "Worker":
        """REQ-2098: a built source's worker, its remote host dialled on first use and again
        after it stops answering -- with backoff: a host that failed is not dialled again until
        its wait (1 s, doubling to a minute) has passed, and a call meanwhile fails naming it.
        ``slot``: which of the source's workers, each its own connection (REQ-2131)."""
        key = (org, source_id, content_hash, slot)
        with self._lock:
            worker = self._workers.get(key)
            if worker is not None and worker.alive():
                return worker
            if worker is not None:
                worker.stop()
                self._workers.pop(key, None)
            failed = self._failures.get(key)
            if failed is not None and time.monotonic() < failed[1]:
                raise WorkerFailed(
                    source_id,
                    f"{failed[2]} (next try in {failed[1] - time.monotonic():.0f}s)",
                )
            worker = make()
            try:
                worker.start()
            except WorkerFailed as exc:
                count = 1 if failed is None else failed[0] + 1
                wait = min(_REMOTE_BACKOFF_MAX_S, _REMOTE_BACKOFF_S * 2 ** (count - 1))
                self._failures[key] = (count, time.monotonic() + wait, str(exc))
                raise
            self._failures.pop(key, None)
            self._workers[key] = worker
            return worker

    def remote_status(self, org: str, source_id: str) -> dict:
        """What this process's pool holds for a built source: a live connection, or the last
        failure and its next try."""
        with self._lock:
            live = [
                k
                for k, w in self._workers.items()
                if k[0] == org and k[1] == source_id and w.alive()
            ]
            failures = [
                (k, f) for k, f in self._failures.items() if k[0] == org and k[1] == source_id
            ]
        if live:
            return {"connected": True}
        if failures:
            (_key, (count, at, message)) = failures[-1]
            return {
                "connected": False,
                "error": message,
                "failures": count,
                "next_try_s": max(0.0, at - time.monotonic()),
            }
        return {"connected": False}

    def stop_source(self, source_id: str) -> None:
        """Stop every worker of ``source_id``: its source was edited or deleted."""
        with self._lock:
            ending = [k for k in self._workers if k[1] == source_id]
            for key in [k for k in self._failures if k[1] == source_id]:
                del self._failures[key]
            workers = [self._workers.pop(k) for k in ending]
        for worker in workers:
            worker.stop()

    def stop_all(self) -> None:
        with self._lock:
            workers = list(self._workers.values())
            self._workers.clear()
        for worker in workers:
            worker.stop()


#: The process's one pool.
pool = WorkerPool()


def private_environment(**extra: str) -> dict[str, str]:
    """The environment a host gets: PATH only, plus what the language adds. No credential of the
    Provisa process reaches the code."""
    return {"PATH": os.environ.get("PATH", ""), **extra}
