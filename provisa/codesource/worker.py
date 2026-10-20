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
over a Unix domain socket in a private directory: Provisa listens, the host connects. A socket,
not the process's output, so nothing the code prints reaches the protocol; what it prints to
stderr is kept (the last lines) to name a crash.

A frame is a 4-byte big-endian length, then that many bytes: a kind byte and its body. ``J`` is
a control message, UTF-8 JSON. ``A`` is rows: the call's id (8 bytes, big-endian) and an Arrow IPC
stream of them, built against the schema the call gave (:mod:`provisa.codesource.arrow_rows`).
Arrow is the only form rows take (REQ-2098). In a JSON message a timestamp travels as
``{"$date": iso}``, an integer outside JSON's exact range as ``{"$bigint": digits}``, a decimal as
``{"$decimal": digits}``, a calendar date with no time as ``{"$day": iso}``. An argument the read
does not give is ``{"$absent": true}`` (:data:`ABSENT`), and ``names`` are the parameters' names in
the order ``args`` gives them, for a host whose language passes arguments by name.

    Provisa -> host  {op: "call", id, export, kind: "table"|"command"|"stream", args, names,
                      schema}  -- the rows' Arrow schema, base64 IPC; null for a single value
                     {op: "cancel", id}      {op: "ping", id}
    host -> Provisa  {op: "hello", protocol, code_hash, exports}
                     rows (an Arrow frame)   {op: "value", id, value}
                     {op: "end", id}         {op: "error", id, message, stack}  {op: "pong", id}
    Provisa -> host  {op: "refused", message}, in place of any call, before the socket closes

The hello names the protocol the host speaks (:data:`PROTOCOL`) and the content hash of the code
it runs (``PROVISA_CODE_HASH`` in its environment: the worker gives it, an image built for a
remote host carries it). A host that speaks another protocol, or runs other code than the source
registered, is refused by name (REQ-2098).

A call blocks its request's thread, as every driver call does (REQ-1882), and is cancelled through
the request's deadline (:func:`provisa.core.request_deadline.cancel_on_deadline`): the host is
told to stop the call, and the waiting thread is released at once. A crash fails every call in
flight, naming the source and the export; the next call starts a new process.
"""

# Requirements: REQ-1984, REQ-1993, REQ-1882
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
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from provisa.core import request_deadline

#: The protocol the hosts speak; a host built for another one is refused by name (REQ-2098).
PROTOCOL = 2

#: The variable a host reads the content hash of its code from, which its hello names.
CODE_HASH_ENV = "PROVISA_CODE_HASH"

#: How long a started host has to connect and say hello.
START_TIMEOUT_S = 30.0

#: How often a start checks that its host is still coming while it waits for it to connect.
_ACCEPT_POLL_S = 0.05

#: How many of the host's last stderr lines a crash names.
STDERR_LINES = 40

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
    TCP, so it cannot connect one). The process gets exactly ``env``, in ``cwd``."""

    argv: Callable[[str], Sequence[str]]
    env: dict[str, str]
    cwd: Path
    inherit_socket: bool = False

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


class _Ended:
    """Put on every call's queue when the worker ends."""

    def __init__(self, message: str) -> None:
        self.message = message


class Worker:
    """One running host process and its socket."""

    def __init__(self, source_id: str, content_hash: str, command: HostCommand) -> None:
        self.source_id = source_id
        self.content_hash = content_hash
        self._command = command
        self._ids = itertools.count(1)
        self._calls: dict[int, queue.SimpleQueue] = {}
        self._calls_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._stderr: collections.deque[str] = collections.deque(maxlen=STDERR_LINES)
        self._ended: str | None = None
        self._dir: Path | None = None
        self._conn: socket.socket | None = None
        self._process: subprocess.Popen | None = None
        self._stderr_thread: threading.Thread | None = None
        self.exports: list[str] = []

    # -- lifecycle ------------------------------------------------------------------------------

    def start(self, timeout: float = START_TIMEOUT_S) -> None:
        """Start the host and wait for its hello; :class:`WorkerFailed` when it does not come."""
        if self._command.inherit_socket:
            conn, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                self._spawn(f"fd:{child.fileno()}", (child.fileno(),))
            finally:
                child.close()
        else:
            self._dir = Path(tempfile.mkdtemp(prefix="provisa-code-"))
            self._dir.chmod(0o700)
            path = self._dir / "worker.sock"
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                listener.bind(str(path))
                listener.listen(1)
                listener.settimeout(_ACCEPT_POLL_S)
                self._spawn(str(path), ())
                conn = self._accept(listener, timeout)
            finally:
                listener.close()
        conn.settimeout(timeout)
        try:
            hello = _read_frame(conn)
        except TimeoutError:
            conn.close()
            self.stop()
            raise WorkerFailed(
                self.source_id,
                f"its host did not say it was ready within {timeout:g}s{self._said()}",
            ) from None
        if hello is None or hello.get("op") != "hello":
            conn.close()
            self.stop()
            raise WorkerFailed(self.source_id, f"its host ended before it was ready{self._said()}")
        refused = refusal(hello, self.content_hash)
        if refused is not None:
            try:
                conn.sendall(_frame({"op": "refused", "message": refused}))
            except OSError:
                pass  # the host has gone already: the refusal is still raised by name below
            conn.close()
            self.stop()
            raise WorkerFailed(self.source_id, refused)
        conn.settimeout(None)
        self._conn = conn
        self.exports = list(hello.get("exports") or [])
        threading.Thread(
            target=self._read, name=f"code-worker-{self.source_id}", daemon=True
        ).start()

    def _spawn(self, address: str, pass_fds: tuple[int, ...]) -> None:
        self._process = subprocess.Popen(  # nosec B603 - the pinned runtime and Provisa's host
            self._command.argv_for(address),
            cwd=self._command.cwd,
            env={**self._command.env, CODE_HASH_ENV: self.content_hash},
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
        """End the host: its socket is closed (the host exits when it closes), then the process."""
        self._end("the worker was stopped")
        if self._conn is not None:
            try:
                self._conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass  # already closed by the host's exit: nothing left to shut
            self._conn.close()
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
        with self._calls_lock:
            if self._ended is None:
                self._ended = message
            calls = list(self._calls.values())
        for waiting in calls:
            waiting.put(_Ended(self._ended))

    def _read(self) -> None:
        conn = self._conn
        assert conn is not None
        try:
            while (message := _read_frame(conn)) is not None:
                with self._calls_lock:
                    waiting = self._calls.get(message["id"])
                if waiting is not None:
                    waiting.put(message)
        except OSError:
            pass  # the socket closed under the read: the worker has ended, said below
        if self._process is not None:
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass  # still running with its socket closed: stop() ends it
        self._end(f"its host ended{self._said()}")

    # -- calls ----------------------------------------------------------------------------------

    def _send(self, message: dict) -> None:
        conn = self._conn
        if conn is None or self._ended is not None:
            raise WorkerFailed(self.source_id, self._ended or "the worker is not started")
        with self._send_lock:
            conn.sendall(_frame(message))

    def _open(self) -> tuple[int, queue.SimpleQueue]:
        call_id = next(self._ids)
        waiting: queue.SimpleQueue = queue.SimpleQueue()
        with self._calls_lock:
            if self._ended is not None:
                raise WorkerFailed(self.source_id, self._ended)
            self._calls[call_id] = waiting
        return call_id, waiting

    def _close(self, call_id: int) -> None:
        with self._calls_lock:
            self._calls.pop(call_id, None)

    def _cancel(self, call_id: int, waiting: queue.SimpleQueue) -> None:
        """Tell the host to stop the call, and release the thread waiting for it. Called from the
        deadline's watchdog thread."""
        waiting.put(_Cancelled())
        try:
            self._send({"op": "cancel", "id": call_id})
        except (OSError, WorkerFailed):
            pass  # the worker has ended: there is no call left to stop

    def _next(self, call_id: int, waiting: queue.SimpleQueue, export: str) -> dict:
        with request_deadline.cancel_on_deadline(lambda: self._cancel(call_id, waiting)):
            message = waiting.get()
        if isinstance(message, _Cancelled):
            # The deadline's own error is raised on the way out of cancel_on_deadline; this is
            # reached only when it did not fire (a cancel with no budget passed).
            raise WorkerFailed(self.source_id, "the call was cancelled", export=export)
        if isinstance(message, _Ended):
            raise WorkerFailed(self.source_id, message.message, export=export)
        if message["op"] == "error":
            raise CodeFailed(self.source_id, export, message["message"], message.get("stack", ""))
        return message

    def rows(
        self,
        export: str,
        kind: str,
        args: Sequence[Any],
        names: Sequence[str] = (),
        *,
        schema: str,
    ) -> Iterator[list[dict]]:
        """A table's or a stream's rows, batch by batch, as the host sends them against
        ``schema`` (arrow_rows.schema_text). Closing the iterator before its end cancels the
        call in the host."""
        from provisa.codesource.arrow_rows import rows_of

        if kind not in ("table", "stream"):
            raise ValueError(f"rows() serves a table or a stream, not a {kind}")
        call_id, waiting = self._open()
        finished = False
        try:
            self._send(
                {
                    "op": "call",
                    "id": call_id,
                    "export": export,
                    "kind": kind,
                    "args": encode(list(args)),
                    "names": list(names),
                    "schema": schema,
                }
            )
            while True:
                message = self._next(call_id, waiting, export)
                if message["op"] == "end":
                    finished = True
                    return
                if message["op"] == "rows":
                    yield rows_of(message["arrow"])
        finally:
            if not finished:
                try:
                    self._send({"op": "cancel", "id": call_id})
                except (OSError, WorkerFailed):
                    pass  # the worker has ended: there is no call left to stop
            self._close(call_id)

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

        call_id, waiting = self._open()
        try:
            self._send(
                {
                    "op": "call",
                    "id": call_id,
                    "export": export,
                    "kind": "command",
                    "args": encode(list(args)),
                    "names": list(names),
                    "schema": schema,
                }
            )
            collected: list[dict] = []
            value: Any = None
            while (message := self._next(call_id, waiting, export))["op"] != "end":
                if message["op"] == "value":
                    value = decode(message["value"])
                elif message["op"] == "rows":
                    collected.extend(rows_of(message["arrow"]))
                    value = collected
            return value
        finally:
            self._close(call_id)

    def ping(self, timeout: float = 5.0) -> bool:
        """Whether the host answers."""
        call_id, waiting = self._open()
        try:
            self._send({"op": "ping", "id": call_id})
            try:
                message = waiting.get(timeout=timeout)
            except queue.Empty:
                return False
            return isinstance(message, dict) and message.get("op") == "pong"
        except (OSError, WorkerFailed):
            return False
        finally:
            self._close(call_id)


# -- the pool ---------------------------------------------------------------------------------------


class WorkerPool:
    """The process's workers, one per ``(source, content hash)``, started on first use. A worker
    whose process has ended is dropped and the next call starts another."""

    def __init__(self) -> None:
        self._workers: dict[tuple[str, str], Worker] = {}
        self._lock = threading.Lock()

    def get(self, source_id: str, content_hash: str, command: Callable[[], HostCommand]) -> Worker:
        key = (source_id, content_hash)
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

    def stop_source(self, source_id: str) -> None:
        """Stop every worker of ``source_id``: its source was edited or deleted."""
        with self._lock:
            ending = [k for k in self._workers if k[0] == source_id]
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
