# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e1a94-5b2f-4d06-8e19-a4f6c0d27b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One run of an organization's Python preflight, in a process of its own (REQ-2133, REQ-1165).

The server starts this file under the code source's sandbox (:mod:`provisa.mv.preflight_confined`),
one process per run. It is run by its path, isolated (``-I``): it imports the standard library and
pyarrow, and loads its two siblings (the purity check and the verdict vocabulary) by path, so no
other part of Provisa is in this process.

The exchange is over the process's own pipes, a frame at a time (one byte of kind, four of length,
the payload), and this side always speaks first:

- the server's first frame is the run: the check's source, the fields of its ``ctx``, the names of
  its inputs;
- ``N`` asks for the next batch of one input, answered by ``B`` (one Arrow IPC stream) or ``E``
  (that input has ended). An input is read only as the check iterates it, in the order it chooses;
- ``R`` is the verdict and ``F`` a check that raised, each with the warnings ``ctx.warn`` took.

``ctx`` here is a copy: what the check does to it reaches the server only as that data.
"""

# Requirements: REQ-2133, REQ-1165
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import os
import pickle  # nosec B403 - reads only what the server wrote to this process's stdin
import struct
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import IO, Any

HERE = Path(__file__).resolve().parent
SERVICES_CLIENT = HERE.parent / "python" / "services" / "provisa_services" / "__init__.py"

#: This process, named in the verdict so the server's log says where the check ran.
PID = os.getpid()

_HEAD = struct.Struct(">cI")

RUN, NEXT, BATCH, END, RESULT, FAILED = b"I", b"N", b"B", b"E", b"R", b"F"


def write_frame(out: IO[bytes], kind: bytes, payload: bytes = b"") -> None:
    out.write(_HEAD.pack(kind, len(payload)) + payload)
    out.flush()


def read_frame(inp: IO[bytes]) -> tuple[bytes, bytes]:
    head = inp.read(_HEAD.size)
    if len(head) != _HEAD.size:
        raise EOFError("the other side of the preflight's pipe closed")
    kind, length = _HEAD.unpack(head)
    payload = inp.read(length)
    if len(payload) != length:
        raise EOFError("the other side of the preflight's pipe closed inside a frame")
    return kind, payload


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # registered before it runs: its dataclasses resolve in it
    spec.loader.exec_module(module)
    return module


def _context(verdicts: ModuleType, fields: dict[str, Any]) -> Any:
    """The check's ``ctx``: the server's fields, and the same ``warn`` and verdict constructors
    as :class:`provisa.events.processor.NodeContext`."""

    class Context:
        def __init__(self) -> None:
            self.__dict__.update(fields)
            self.warns = list(fields["warns"])

        def warn(self, reasons: Any) -> None:
            if isinstance(reasons, str):
                self.warns.append(reasons)
            else:
                self.warns.extend(reasons)

        def ok(self) -> Any:
            return verdicts.CONTINUE

        def abort(self, reason: str | None = None) -> Any:
            return verdicts.Verdict(verdicts.Decision.ABORT, reason)

        def quarantine(self, reason: str | None = None) -> Any:
            return verdicts.Verdict(verdicts.Decision.QUARANTINE, reason)

    return Context()


def _rows(inp: IO[bytes], out: IO[bytes], node: str) -> Iterator[dict]:
    import pyarrow as pa

    while True:
        write_frame(out, NEXT, node.encode())
        kind, payload = read_frame(inp)
        if kind == END:
            return
        for batch in pa.ipc.open_stream(payload):
            yield from batch.to_pylist()


async def _awaited(value: Any) -> Any:
    return await value


def run(inp: IO[bytes], out: IO[bytes]) -> None:
    kind, payload = read_frame(inp)
    assert kind == RUN, kind
    # S301/B301: the frame is the server's, read from this process's own stdin.
    asked = pickle.loads(payload)  # noqa: S301  # nosec B301
    checks = _load("provisa_preflight_check", HERE / "preprocess.py")
    verdicts = _load("provisa_preflight_verdict", HERE / "preflight.py")
    ctx = _context(verdicts, asked["ctx"])
    before = len(ctx.warns)

    def answer(kind: bytes, body: dict[str, Any]) -> None:
        body["warns"] = [str(w) for w in ctx.warns[before:]]
        body["pid"] = PID
        write_frame(out, kind, json.dumps(body, default=str).encode())

    try:
        fn = checks.compile_preprocess(
            asked["source"],
            services=lambda: _load(checks.SERVICES_MODULE, SERVICES_CLIENT),
        )
        streams: dict[str, Any] = {}
        for node in asked["nodes"]:
            rows = _rows(inp, out, node)
            # An input the server holds whole is a list here too, as it is in the server.
            streams[node] = list(rows) if node in asked["whole"] else rows
        result = fn(streams, ctx)
        if inspect.isawaitable(result):
            result = asyncio.run(_awaited(result))
        verdict = verdicts.to_verdict(result)
    except Exception as exc:  # noqa: BLE001 - a check that raises is an outcome the server reports
        answer(
            FAILED,
            {
                "type": type(exc).__name__,
                "message": str(exc),
                "contract": isinstance(exc, verdicts.PreflightContractError),
            },
        )
        return
    answer(RESULT, {"decision": verdict.decision.value, "reason": verdict.reason})


def main() -> None:
    inp = os.fdopen(0, "rb")
    # The frames go out on the pipe the process was started with; whatever else writes to
    # standard output from here on goes nowhere, so nothing can break a frame.
    out = os.fdopen(os.dup(1), "wb")
    os.dup2(os.open(os.devnull, os.O_WRONLY), 1)
    run(inp, out)


if __name__ == "__main__":
    main()
