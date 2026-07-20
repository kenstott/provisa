# Copyright (c) 2026 Kenneth Stott
# Canary: 184f2a0d-5e7b-4c69-8d30-9f5b4c2e70d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Shell transport adapter (REQ-940).

Invokes an external processor as a subprocess: NDJSON rows in on stdin, NDJSON rows out on stdout.
Runs the command as an argv LIST (never ``shell=True`` — no shell injection surface), with a timeout,
and fails loud on a non-zero exit. Schema is validated at both ends by the shared contract.
"""

from __future__ import annotations

import subprocess  # nosec B404 - argv-list invocation only, never shell=True
from collections.abc import Iterable, Iterator

from provisa.processors.contract import Schema, TransportAdapter, validate_rows
from provisa.processors.framing import ndjson_decode, ndjson_encode


class ProcessorError(RuntimeError):  # REQ-940
    """An external processor invocation failed (non-zero exit, timeout, or transport error)."""


class ShellAdapter(TransportAdapter):  # REQ-940
    """Run a processor as ``argv`` with NDJSON on stdin/stdout.

    ``argv`` is a command list (e.g. ``["./transform.py"]``); it is executed directly with no shell.
    A non-zero exit or a timeout raises :class:`ProcessorError` — the transform is treated as failed,
    never as "returned no rows"."""

    def __init__(self, argv: list[str], *, timeout: float = 30.0) -> None:
        if not argv:
            raise ValueError("ShellAdapter requires a non-empty argv")
        self._argv = argv
        self._timeout = timeout

    def process(
        self, rows: Iterable[dict], *, schema_in: Schema, schema_out: Schema
    ) -> Iterator[dict]:
        payload = b"".join(
            ndjson_encode(validate_rows(rows, schema_in, where="shell input"))
        )
        try:
            completed = subprocess.run(  # nosec B603 - argv list, no shell, trusted operator command
                self._argv,
                input=payload,
                capture_output=True,
                timeout=self._timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise ProcessorError(f"shell processor {self._argv[0]!r} failed: {exc}") from exc
        if completed.returncode != 0:
            raise ProcessorError(
                f"shell processor {self._argv[0]!r} exited {completed.returncode}: "
                f"{completed.stderr.decode('utf-8', 'replace')[:500]}"
            )
        out_rows = ndjson_decode(completed.stdout.splitlines())
        return validate_rows(out_rows, schema_out, where="shell output")
