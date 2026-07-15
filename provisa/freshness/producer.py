# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On-stale producer command for file-based sources (REQ-861).

A file source (csv/parquet/sqlite/files) may carry an optional ``producer_command`` — an argv
list that refreshes the file IN PLACE. It is the ACT-on-stale response paired with the REQ-860
source freshness gate: the gate DETECTS staleness (and only then does the read path invoke the
loader), so running the producer here, before the file is read, freshens the file without
changing its ``path`` or defining an MV.

The command is executed with ``shell=False`` over the argv list (injection-safe — never a shell
string). A non-zero exit FAILS LOUD (:class:`ProducerCommandError`); a stale file is never read
silently after a failed producer.
"""

from __future__ import annotations

import asyncio
import subprocess
from typing import Any

# File-based source types (REQ-861). A producer command only applies to these — a non-file source
# has no file to refresh in place.
_FILE_SOURCE_TYPES: frozenset[str] = frozenset({"sqlite", "csv", "parquet", "files"})


class ProducerCommandError(RuntimeError):
    """A source's producer command exited non-zero; the (still-stale) file must not be read."""


def _source_type(source: Any) -> str:
    """The source's type as a plain string (accepts an enum member or a bare string)."""
    stype = source.type
    return stype.value if hasattr(stype, "value") else str(stype)


def has_producer(source: Any) -> bool:
    """Whether ``source`` is a file source carrying a producer command (REQ-861).

    True only for a file-based type with a non-empty ``producer_command``; the argv is what the
    read path runs on-stale before reading. A producer declared on a non-file source is ignored
    here — the field is meaningful only where there is a file to refresh in place.
    """
    return _source_type(source) in _FILE_SOURCE_TYPES and bool(
        getattr(source, "producer_command", None)
    )


async def run_producer(source: Any) -> None:
    """Run ``source``'s producer command, failing loud on a non-zero exit (REQ-861).

    Preconditions: :func:`has_producer` is True. Runs the argv list with ``shell=False`` off the
    event loop (subprocess is blocking). A non-zero return code raises :class:`ProducerCommandError`
    with the captured stderr — the caller must NOT proceed to read the still-stale file.
    """
    cmd = source.producer_command
    if not cmd:
        raise ValueError(
            f"source {source.id!r}: run_producer called with no producer_command (REQ-861)"
        )
    await asyncio.to_thread(_run, list(cmd), source.id)


def _run(cmd: list[str], source_id: str) -> None:
    result = subprocess.run(cmd, shell=False, check=False, capture_output=True, text=True)  # noqa: S603
    if result.returncode:
        raise ProducerCommandError(
            f"source {source_id!r}: producer command {cmd!r} exited {result.returncode}: "
            f"{result.stderr.strip()}"
        )
