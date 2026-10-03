# Copyright (c) 2026 Kenneth Stott
# Canary: 908cc420-43c2-4239-9fbb-72015b38a879
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What a statement's answer must say about itself beside its rows (REQ-1350).

A warning is not an error: the statement is answered, and the answer carries the warning — a
stable code, its params, and the English text — on every surface, in that surface's own warning
channel. The pipeline opens one collector per statement (:func:`collecting`); whatever deep in
the call finds something to say (:func:`warn`) adds to it; the pipeline puts what was collected
on the plan and the result. No surface detects a warning on its own.

A warning raised with no collector open is not dropped: it raises :class:`NoWarningChannel`, so
a surface that cannot tell its caller fails instead of answering as if nothing were wrong.
"""

# Requirements: REQ-1350

from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class ServerWarning:
    """One warning: ``code`` names it (``serverErrors.<code>`` in the UI catalog), ``params``
    fill its text, ``message`` is the English text for a client with no catalog."""

    code: str
    message: str
    params: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "params": dict(self.params), "message": self.message}


class NoWarningChannel(RuntimeError):
    """A warning was raised where no collector is open: the surface answering this statement
    has no way to tell its caller, so the statement fails with the warning's own text."""

    def __init__(self, warning: ServerWarning) -> None:
        self.warning = warning
        super().__init__(warning.message)


_collector: ContextVar[list[ServerWarning] | None] = ContextVar(
    "provisa_statement_warnings", default=None
)


@contextmanager
def collecting() -> Generator[list[ServerWarning]]:
    """Collect the warnings raised inside the block, in the order raised. A block nested in an
    open one adds to the same list: one statement, one list."""
    current = _collector.get()
    if current is not None:
        yield current
        return
    found: list[ServerWarning] = []
    token = _collector.set(found)
    try:
        yield found
    finally:
        _collector.reset(token)


def warn(warning: ServerWarning) -> None:
    """Add ``warning`` to the statement's collector; raise :class:`NoWarningChannel` when none
    is open. The same warning twice is kept once."""
    found = _collector.get()
    if found is None:
        raise NoWarningChannel(warning)
    if warning not in found:
        found.append(warning)


def raised() -> list[ServerWarning]:
    """What the open collector holds so far (empty when none is open): for a step that must
    act on a warning raised earlier in the same statement."""
    return list(_collector.get() or [])


def header_value(warnings: list[ServerWarning]) -> str:
    """The warnings as one HTTP-safe header value: JSON with every non-ASCII character escaped
    (``ensure_ascii``), so a header channel that admits only latin-1 or ASCII carries it."""
    return json.dumps([w.as_dict() for w in warnings], ensure_ascii=True, separators=(",", ":"))
