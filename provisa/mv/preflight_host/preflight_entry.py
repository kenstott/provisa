# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e1a94-5b2f-4d06-8e19-a4f6c0d27b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The entry module of a held organization's preflight worker (REQ-2133, REQ-1165).

Python's host (``provisa/python/host.py``) imports this as the worker's code and hands each
preflight call to :func:`_provisa_preflight`. It is not part of the server: it imports the standard
library, and loads the purity check and the verdict vocabulary (its two neighbours in
``provisa/mv``) by path, so nothing else of Provisa's is in the worker.

The check's ``ctx`` is a copy of the server's fields: what the check does to it reaches the server
only as the answer's data. It has no public function: the worker serves preflights and nothing
else."""

# Requirements: REQ-2133, REQ-1165
from __future__ import annotations

import asyncio
import importlib.util
import inspect
import os
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import ModuleType
from typing import Any

_MV = Path(__file__).resolve().parent.parent

#: This process, named in each answer so the server's log says where the check ran.
PID = os.getpid()


def _load(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # registered before it runs: its dataclasses resolve in it
    spec.loader.exec_module(module)
    return module


_checks = _load("provisa_preflight_check", _MV / "preprocess.py")
_verdicts = _load("provisa_preflight_verdict", _MV / "preflight.py")


class _Context:
    """The check's ``ctx``: the server's fields, and the same ``warn`` and verdict constructors
    as :class:`provisa.events.processor.NodeContext`."""

    def __init__(self, fields: dict[str, Any]) -> None:
        self.__dict__.update(fields)
        # The wire has no tuple: these are tuples in the server.
        if fields.get("window") is not None:
            self.window = tuple(fields["window"])
        if fields.get("columns") is not None:
            self.columns = [tuple(c) for c in fields["columns"]]
        self.nearest_inputs = tuple(fields.get("nearest_inputs") or ())
        self.warns = list(fields["warns"])

    def warn(self, reasons: Any) -> None:
        if isinstance(reasons, str):
            self.warns.append(reasons)
        else:
            self.warns.extend(reasons)

    def ok(self) -> Any:
        return _verdicts.CONTINUE

    def abort(self, reason: str | None = None) -> Any:
        return _verdicts.Verdict(_verdicts.Decision.ABORT, reason)

    def quarantine(self, reason: str | None = None) -> Any:
        return _verdicts.Verdict(_verdicts.Decision.QUARANTINE, reason)


def _rows(pull: Callable[[str], bytes | None], name: str, pa: Any) -> Iterator[dict]:
    while (ipc := pull(name)) is not None:
        for batch in pa.ipc.open_stream(ipc):
            yield from batch.to_pylist()


async def _awaited(value: Any) -> Any:
    return await value


def _provisa_preflight(
    source: str,
    fields: dict[str, Any],
    inputs: list[str],
    whole: list[str],
    pull: Callable[[str], bytes | None],
    pa: Any,
) -> dict[str, Any]:
    """Run the check ``source`` and answer with its verdict, or what it raised, and the warnings it
    took. ``pull`` gives the next batch of a named input; an input in ``whole`` is a list here, as
    it is in the server. Called on a thread of the host's."""
    ctx = _Context(fields)
    before = len(ctx.warns)

    def answer(body: dict[str, Any]) -> dict[str, Any]:
        return {**body, "warns": [str(w) for w in ctx.warns[before:]], "pid": PID}

    try:
        fn = _checks.compile_preprocess(
            source, services=lambda: sys.modules[_checks.SERVICES_MODULE]
        )
        streams: dict[str, Any] = {}
        for name in inputs:
            rows = _rows(pull, name, pa)
            streams[name] = list(rows) if name in whole else rows
        result = fn(streams, ctx)
        if inspect.isawaitable(result):
            result = asyncio.run(_awaited(result))
        verdict = _verdicts.to_verdict(result)
    except Exception as exc:  # noqa: BLE001 - a check that raises is an outcome the server reports
        return answer(
            {
                "raised": type(exc).__name__,
                "message": str(exc),
                "contract": isinstance(exc, _verdicts.PreflightContractError),
            }
        )
    return answer({"decision": verdict.decision.value, "reason": verdict.reason})
