# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Per-stage pipeline observability (REQ-914).

Emit the canonical SQL at each pipeline boundary (compile → govern → transpile) as an
OpenTelemetry span event so the transform sequence is inspectable per request. The pre/post
governance pair doubles as the compliance receipt: the diff is exactly what policy added.

PII SAFETY: this is a governance product; SQL text carries literal values (RLS predicate
values, inlined params) and the pre-governance form is by definition ungoverned. Capture is
therefore OFF by default and redacts literals when on. Controlled by ``PROVISA_TRACE_SQL``:

    off       (default) — emit only the stage name; no SQL text leaves the process.
    redacted  — parse and blank every literal to ``?`` before attaching.
    full      — attach raw SQL. DEV ONLY. Leaks literal values into traces.

``PROVISA_TRACE_AST=1`` additionally attaches the SQLGlot AST repr. The AST is redacted the
same way — every literal is blanked — so it is safe to enable independently of the SQL mode.
"""

from __future__ import annotations

import os

import sqlglot
import sqlglot.errors
import sqlglot.expressions as exp


def _mode() -> str:
    # Read per-call so it can be toggled in a dev session without a restart.
    return os.environ.get("PROVISA_TRACE_SQL", "off").lower()


def _redacted_tree(sql: str) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
    """Parse ``sql`` and return the tree with every literal blanked to a placeholder.

    Raises sqlglot.errors.ParseError on unparseable input — callers in the trace path catch it
    and surface the failure on the span rather than failing the request (see trace_stage).
    """

    def _blank(node: exp.Expression) -> exp.Expression:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(node, exp.Literal):
            return exp.Placeholder()
        return node

    return sqlglot.parse_one(sql, read="postgres").transform(_blank)


def redact_sql(sql: str) -> str:
    """Return ``sql`` with every literal replaced by a placeholder, structure intact."""
    return _redacted_tree(sql).sql(dialect="postgres")


def redact_ast(sql: str) -> str:
    """Return the SQLGlot AST repr with every literal blanked — no literal values survive."""
    return repr(_redacted_tree(sql))


def _current_span():
    try:
        from opentelemetry import trace as _trace
    except ImportError:
        return None
    return _trace.get_current_span()


def trace_stage(stage: str, sql: str) -> None:
    """Record a ``pipeline.stage`` span event for ``stage`` with mode-gated SQL detail.

    Never raises: a telemetry failure must not break the request path. In ``redacted`` mode a
    parse failure is surfaced as a ``sql.redact_error`` attribute (loud, not silent) instead of
    dropping the event or propagating.
    """
    span = _current_span()
    if span is None:
        return

    attrs: dict[str, str] = {"pipeline.stage": stage}
    mode = _mode()
    if mode == "full":
        attrs["sql"] = sql
    elif mode == "redacted":
        try:
            attrs["sql.redacted"] = redact_sql(sql)
        except sqlglot.errors.SqlglotError as e:  # surface on span; telemetry must not fail request
            attrs["sql.redact_error"] = f"{type(e).__name__}: {e}"

    if os.environ.get("PROVISA_TRACE_AST") == "1":
        # AST is redacted too — literals are blanked so no PII value survives the repr.
        try:
            attrs["sql.ast"] = redact_ast(sql)
        except sqlglot.errors.SqlglotError as e:
            attrs["sql.ast_error"] = f"{type(e).__name__}: {e}"

    span.add_event(f"pipeline.stage:{stage}", attributes=attrs)
