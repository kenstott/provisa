# Copyright (c) 2026 Kenneth Stott
# Canary: 043d758a-1407-4ab5-bce9-2767d3f0b39a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The host a Python source's worker runs (REQ-1993): the protocol of every code source
(provisa/codesource/worker.py), in Python.

    <python> host.py <socket path | fd:N> <code directory> <entry module>

It imports the entry module (which imports nothing of Provisa's), connects to the socket (or
takes the one it inherited, already connected), says which functions it has, and serves calls
until the socket closes, when it exits. A coroutine function is awaited; a generator or an async
generator is iterated; any other function runs on a thread, so a slow one holds no other call.
A table's rows go in batches of 1000, a stream's each as it is yielded. A row is a dict, a
dataclass, a NamedTuple or an object with annotated fields.

Rows go as Arrow IPC batches against the schema the call gives (REQ-2098,
provisa/codesource/arrow_rows.py): a field Arrow has no exact form for (a ``numeric``, a ``json``
value) is text, named so in its metadata. Its one import beside the standard library is pyarrow:
the pinned copy Provisa gives the local worker (``PROVISA_HOST_LIB``, put after the code's own
paths, so code that installs its own pyarrow keeps it), or the one in a remote host's image.
"""

# Requirements: REQ-1993, REQ-2098
from __future__ import annotations

import asyncio
import dataclasses
import datetime
import decimal
import enum
import importlib
import inspect
import json
import os
import socket
import struct
import sys
import traceback
import typing

BATCH = 1000
#: The protocol this host speaks (provisa/codesource/worker.py PROTOCOL).
PROTOCOL = 2
_EXACT = 2**53 - 1
_HEAD = struct.Struct(">I")
_ID = struct.Struct(">Q")
_IR_KEY = b"provisa.ir"
_WHOLE_ROW = "$row"
_DONE = object()
#: An argument the read does not give: left out of the call, so the function's default applies.
_ABSENT = object()


def to_wire(value: typing.Any) -> typing.Any:  # a value the code returned: any shape
    """``value`` as the wire carries it."""
    if value is None or isinstance(value, (bool, str, float)):
        return value
    if isinstance(value, int):
        return value if -_EXACT <= value <= _EXACT else {"$bigint": str(value)}
    if isinstance(value, decimal.Decimal):
        return {"$decimal": str(value)}
    if isinstance(value, datetime.datetime):
        return {"$date": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"$day": value.isoformat()}
    if isinstance(value, enum.Enum):
        return to_wire(value.value)
    if isinstance(value, dict):
        return {str(k): to_wire(v) for k, v in value.items()}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: to_wire(getattr(value, f.name)) for f in dataclasses.fields(value)}
    if isinstance(value, tuple) and hasattr(value, "_asdict"):
        return to_wire(typing.cast(typing.Any, value)._asdict())
    if isinstance(value, (list, tuple, set, frozenset)):
        return [to_wire(v) for v in value]
    annotations = getattr(type(value), "__annotations__", None)
    if annotations:
        return {name: to_wire(getattr(value, name, None)) for name in annotations}
    raise TypeError(f"a {type(value).__name__} has no form a row or a value can take")


def from_wire(value: typing.Any) -> typing.Any:  # a decoded JSON value: any shape
    if isinstance(value, list):
        return [from_wire(v) for v in value]
    if isinstance(value, dict):
        if len(value) == 1:
            if "$date" in value:
                return datetime.datetime.fromisoformat(value["$date"].replace("Z", "+00:00"))
            if "$day" in value:
                return datetime.date.fromisoformat(value["$day"])
            if "$bigint" in value:
                return int(value["$bigint"])
            if "$decimal" in value:
                return decimal.Decimal(value["$decimal"])
            if value.get("$absent") is True:
                return _ABSENT
        return {k: from_wire(v) for k, v in value.items()}
    return value


def exported(module: typing.Any) -> list[str]:  # a module object
    """The module's public functions that are the code's own (the reader's rule)."""
    package = module.__name__.split(".")[0]
    names = getattr(module, "__all__", None) or [n for n in vars(module) if not n.startswith("_")]
    return [
        n
        for n in names
        if inspect.isfunction(getattr(module, n, None))
        and (getattr(module, n).__module__ or "").split(".")[0] == package
    ]


def _call_arguments(args: list, names: list[str]) -> tuple[list, dict]:
    """The call's positional arguments up to the first absent one, and every given one after it
    by its name: an absent one is left out, so its default applies."""
    if _ABSENT not in args:
        return args, {}
    first = args.index(_ABSENT)
    keywords = {
        name: value
        for name, value in zip(names[first:], args[first:], strict=True)
        if value is not _ABSENT
    }
    return args[:first], keywords


def _arrow() -> typing.Any:  # the pyarrow module
    lib = os.environ.get("PROVISA_HOST_LIB")
    # Set for the local worker only (python/source.py): a remote host's image installs pyarrow
    # into its interpreter, where the import finds it without a path of its own.
    if lib and lib not in sys.path:
        sys.path.append(lib)
    return importlib.import_module("pyarrow")


def _plain(value: typing.Any) -> typing.Any:  # a row or a field of one, as the code made it
    """A record (a dataclass, a NamedTuple, an annotated object) as a dict; an enum as its value."""
    if isinstance(value, enum.Enum):
        return value.value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {f.name: getattr(value, f.name) for f in dataclasses.fields(value)}
    if isinstance(value, tuple) and hasattr(value, "_asdict"):
        return typing.cast(typing.Any, value)._asdict()
    if isinstance(value, (dict, list, tuple, set, frozenset, str, bytes, int, float, bool)):
        return value
    if value is None or isinstance(value, (datetime.date, decimal.Decimal)):
        return value
    annotations = getattr(type(value), "__annotations__", None)
    if annotations:
        return {name: getattr(value, name, None) for name in annotations}
    return value


def _for_field(value: typing.Any, field: typing.Any) -> typing.Any:  # a value; a pyarrow Field
    """``value`` in the form ``field`` takes it."""
    pa = _arrow()
    if value is None:
        return None
    ir = (field.metadata or {}).get(_IR_KEY)
    if ir == b"json":
        return json.dumps(to_wire(value), separators=(",", ":"))
    if ir == b"numeric":
        return str(value)
    value = _plain(value)
    kind = field.type
    if pa.types.is_struct(kind):
        return {
            kind.field(i).name: _for_field(value.get(kind.field(i).name), kind.field(i))
            for i in range(kind.num_fields)
        }
    if pa.types.is_list(kind):
        return [_for_field(v, kind.value_field) for v in value]
    return value


class Host:
    def __init__(self, module: typing.Any, writer: asyncio.StreamWriter) -> None:
        self.module = module
        self.writer = writer
        self.running: dict[int, asyncio.Task] = {}

    async def send(self, message: dict) -> None:
        body = b"J" + json.dumps(message, separators=(",", ":")).encode()
        self.writer.write(_HEAD.pack(len(body)) + body)
        await self.writer.drain()

    async def send_rows(self, call_id: int, rows: list, schema: typing.Any) -> None:
        """``rows`` as one Arrow IPC frame, against ``schema`` (a pyarrow Schema)."""
        pa = _arrow()
        if schema.names == [_WHOLE_ROW]:
            field = schema.field(_WHOLE_ROW)
            values = [{_WHOLE_ROW: _for_field(row, field)} for row in rows]
        else:
            fields = [schema.field(i) for i in range(len(schema))]
            values = []
            for row in rows:
                plain = _plain(row)
                if not isinstance(plain, dict):
                    raise TypeError(f"a {type(row).__name__} is not a row")
                values.append({f.name: _for_field(plain.get(f.name), f) for f in fields})
        batch = pa.RecordBatch.from_pylist(values, schema=schema)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as stream:
            stream.write_batch(batch)
        body = b"A" + _ID.pack(call_id) + sink.getvalue().to_pybytes()
        self.writer.write(_HEAD.pack(len(body)) + body)
        await self.writer.drain()

    async def serve(
        self,
        call_id: int,
        name: str,
        kind: str,
        args: list,
        names: list[str],
        schema: str | None,
    ) -> None:
        try:
            fn = getattr(self.module, name, None)
            if not inspect.isfunction(fn) or name not in exported(self.module):
                raise LookupError(f"the code has no function {name}")
            positional, keywords = _call_arguments(from_wire(args), names)
            rows_schema = None
            if schema is not None:
                import base64

                pa = _arrow()
                rows_schema = pa.ipc.open_stream(pa.py_buffer(base64.b64decode(schema))).schema
            await self._run(call_id, fn, kind, positional, keywords, rows_schema)
            await self.send({"op": "end", "id": call_id})
        except asyncio.CancelledError:
            pass  # Provisa stopped the call and is no longer waiting for it
        except BaseException as exc:  # noqa: BLE001 -- the code's own failure, sent back by name
            await self.send(
                {
                    "op": "error",
                    "id": call_id,
                    "message": f"{type(exc).__name__}: {exc}",
                    "stack": "".join(traceback.format_exception(exc)),
                }
            )
        finally:
            self.running.pop(call_id, None)

    async def _run(
        self,
        call_id: int,
        fn: typing.Callable,
        kind: str,
        args: list,
        keywords: dict,
        schema: typing.Any,  # the rows' pyarrow Schema; None for a single value
    ) -> None:
        if inspect.isasyncgenfunction(fn) or inspect.isgeneratorfunction(fn):
            result = fn(*args, **keywords)
        elif inspect.iscoroutinefunction(fn):
            result = await fn(*args, **keywords)
        else:
            result = await asyncio.to_thread(fn, *args, **keywords)
        if schema is None:
            # A single value, or a command whose result is not rows: its JSON value message.
            await self.send({"op": "value", "id": call_id, "value": to_wire(result)})
        elif hasattr(result, "__aiter__"):
            await self._rows(call_id, kind, self._async_rows(result), schema)
        elif isinstance(result, (list, tuple)) and not hasattr(result, "_asdict"):
            for at in range(0, len(result), BATCH):
                await self.send_rows(call_id, list(result[at : at + BATCH]), schema)
        elif inspect.isgenerator(result) or (
            hasattr(result, "__iter__") and not isinstance(result, dict)
        ):
            await self._rows(call_id, kind, self._sync_rows(iter(result)), schema)
        else:
            raise TypeError(f"a {type(result).__name__} is not a collection of rows")

    @staticmethod
    async def _async_rows(rows: typing.AsyncIterator) -> typing.AsyncIterator:
        try:
            async for row in rows:
                yield row
        finally:
            close = getattr(rows, "aclose", None)
            if close is not None:
                await close()

    @staticmethod
    async def _sync_rows(rows: typing.Iterator) -> typing.AsyncIterator:
        # Each step on a thread: a generator that blocks between rows holds no other call.
        try:
            while (row := await asyncio.to_thread(next, rows, _DONE)) is not _DONE:
                yield row
        finally:
            close = getattr(rows, "close", None)
            if close is not None:
                close()

    async def _rows(
        self, call_id: int, kind: str, rows: typing.AsyncIterator, schema: typing.Any
    ) -> None:
        batch: list = []
        async for row in rows:
            if kind == "stream":
                await self.send_rows(call_id, [row], schema)  # a stream's row goes as it comes
                continue
            batch.append(row)
            if len(batch) >= BATCH:
                await self.send_rows(call_id, batch, schema)
                batch = []
        if batch:
            await self.send_rows(call_id, batch, schema)


async def run(address: str, code: str, entry: str) -> None:
    module = load(code, entry)
    if address.startswith("fd:"):
        # The Linux sandbox refuses the host any new socket but TCP: Provisa hands it this one.
        sock = socket.socket(fileno=int(address[3:]))
    else:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(address)
    reader, writer = await asyncio.open_connection(sock=sock)
    await serve(module, reader, writer)


def load(code: str, entry: str) -> typing.Any:  # the code's entry module
    sys.path.insert(0, code)
    return importlib.import_module(entry)


async def serve(
    module: typing.Any, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    """Say hello on one connection to Provisa and serve its calls until it closes. A remote host
    serves one connection per Provisa process (remote_host.py), each with its own calls."""
    host = Host(module, writer)
    await host.send(
        {
            "op": "hello",
            "protocol": PROTOCOL,
            "code_hash": os.environ["PROVISA_CODE_HASH"],
            "exports": exported(module),
        }
    )
    while True:
        try:
            head = await reader.readexactly(_HEAD.size)
            message = json.loads((await reader.readexactly(_HEAD.unpack(head)[0]))[1:])
        except (asyncio.IncompleteReadError, ConnectionError):
            for task in list(host.running.values()):
                task.cancel()
            return  # the socket closed: Provisa stopped the worker
        op = message["op"]
        if op == "refused":
            raise Refused(message["message"])
        if op == "ping":
            await host.send({"op": "pong", "id": message["id"]})
        elif op == "cancel":
            task = host.running.get(message["id"])
            if task is not None:
                task.cancel()
        elif op == "call":
            host.running[message["id"]] = asyncio.create_task(
                host.serve(
                    message["id"],
                    message["export"],
                    message["kind"],
                    message["args"],
                    message["names"],
                    message["schema"],
                )
            )


class Refused(Exception):
    """Provisa refused this host: its protocol or its code is not the source's."""


def main(argv: list[str]) -> int:
    address, code, entry = argv
    # What the code prints goes to stderr, which Provisa keeps to name a crash.
    os.dup2(2, 1)
    try:
        asyncio.run(run(address, code, entry))
    except Refused as refused:
        sys.stderr.write(f"provisa refused this host: {refused}\n")
        return 2
    os._exit(0)  # threads still running a cancelled call do not hold the process open


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
