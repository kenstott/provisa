# Copyright (c) 2026 Kenneth Stott
# Canary: e75682d2-10d0-4f60-89dc-25206b84ac4b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Trace spans landed for a native engine (REQ-862, REQ-1910).

Trino reads the ops signals from its ``otel`` Iceberg catalog. A native engine has none, so this
process lands its own trace records as Parquet under ``<telemetry_parquet_dir()>/traces``: one
closed file per export batch in the day's folder (``_date=YYYY-MM-DD``), readable the moment it
is written, and a scheduled compaction that merges each day's small files into files of up to
``COMPACT_TARGET_BYTES``. The engine reads the folder as ``otel.signals.traces``, the name Trino
gives, so ``"ops"."traces"`` is the same governed table on both.

The rows are those Trino's compaction keeps: a request span or the root span of background work
(``ops_schema.is_trace_record``), never a catalog-driver statement span. Rows are kept
indefinitely, as Trino keeps them; compaction removes only the batch files it merged.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Any, Callable, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

from provisa.observability.ops_schema import (
    CATALOG_DRIVER_SCOPES,
    OPS_SIGNAL_VIEWS,
    OPS_TABLES,
    is_trace_record,
    telemetry_parquet_dir,
    trace_row,
)

log = logging.getLogger(__name__)

#: The size a compacted file is filled to.
COMPACT_TARGET_BYTES = 64 * 1024 * 1024

#: A zero-row file of the traces schema at the folder's root, never removed: the engine's view
#: over the folder binds its columns from it alone (traces_scan), and the folder's glob always
#: matches it (DuckDB refuses a glob matching no file).
SCHEMA_FILE = "_schema.parquet"

#: A traces row's identity. Deduplicating on it rather than every column costs a fifth to a half as
#: much: on a million rows a count read 0.04 s against 0.20 s, a route's aggregate 0.021 s against
#: 0.044 s (0.003 s and 0.006 s with no deduplication).
SPAN_KEY = ("trace_id", "span_id", "tenant_id")

#: Each day's batches compaction has merged, by name, with when: kept in place until no read
#: can still hold a listing of them (``compact``). Not Parquet, so the view's glob skips it.
MERGED_FILE = "_merged.json"

_ARROW = {
    "text": pa.string(),
    "integer": pa.int32(),
    "bigint": pa.int64(),
    "timestamp": pa.timestamp("us"),
    "date": pa.date32(),
}

TRACES_SCHEMA = pa.schema([(col, _ARROW[typ]) for col, typ, _key in OPS_TABLES["traces"]])


def traces_dir() -> str:
    return os.path.join(telemetry_parquet_dir(), "traces")


def traces_scan(root: str) -> str:
    """The DuckDB scan the engine's ``otel.signals.traces`` view reads: every Parquet file under
    ``root``, the schema file included. A file being written is a dot-named ``.tmp`` until it is
    renamed into place, so the scan never reads one partly written.

    The view binds its columns from the schema file alone, the first file named, so defining it
    opens no batch: compaction removes the batches it merged, and a batch listed by a bind that
    read every file (``union_by_name``) could be gone by the time it was opened. Every file is
    written with ``TRACES_SCHEMA`` (``write_batch``, ``compact``), so none needs its columns
    matched by name.

    A query of the view lists the folder and then opens each file, so a batch removed between the
    two would fail it ("Cannot open file"). ``compact`` keeps each batch it merged in place, beside
    its merged copy, until no read can still hold a listing of it; the view reads each span once
    (DISTINCT ON its key, ``SPAN_KEY``: a batch's rows and their merged copy are the same spans,
    and a record landed for another org is another span of that org's)."""
    schema = os.path.join(root, SCHEMA_FILE).replace("'", "''")
    every = os.path.join(root, "**", "*.parquet").replace("'", "''")
    key = ", ".join(SPAN_KEY)
    return (
        f"SELECT DISTINCT ON ({key}) * "
        f"FROM read_parquet(['{schema}', '{every}'], hive_partitioning = false)"
    )


def signal_views(root: str, transpile: Callable[[str], str]) -> dict[str, str]:
    """The views a native engine defines in ``otel.signals``, by name: ``traces`` over the Parquet
    under ``root``, then each ops view over it (``OPS_SIGNAL_VIEWS``), its PostgreSQL SELECT
    transpiled by ``transpile`` to the engine's SQL."""
    ensure_schema_file(root)
    views = {"traces": traces_scan(root)}
    for name, _cols, select in OPS_SIGNAL_VIEWS:
        views[name] = transpile(select)
    return views


def ensure_schema_file(root: str) -> None:
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, SCHEMA_FILE)
    if not os.path.exists(path):
        _write_atomic(TRACES_SCHEMA.empty_table(), path)


def _write_atomic(table: pa.Table, path: str) -> None:
    tmp = os.path.join(os.path.dirname(path), f".{uuid.uuid4().hex}.tmp")
    pq.write_table(table, tmp)
    os.replace(tmp, path)


def _day_dir(root: str, day: Any) -> str:
    return os.path.join(root, f"_date={day.isoformat()}")


def span_row(span: Any) -> dict:
    """A finished SDK span (``ReadableSpan``) as a ``traces`` row."""
    ctx = span.context
    parent = span.parent
    scope = span.instrumentation_scope
    return trace_row(
        trace_id=format(ctx.trace_id, "032x"),
        span_id=format(ctx.span_id, "016x"),
        parent_span_id=format(parent.span_id, "016x") if parent is not None else None,
        name=span.name,
        # The wire's numbering, which the other lanes store: OTLP's SpanKind counts from
        # SPAN_KIND_UNSPECIFIED = 0, so each SDK kind is its value plus one (INTERNAL 0 -> 1).
        kind=span.kind.value + 1,
        attributes=dict(span.attributes or {}),
        resource_attributes=dict(span.resource.attributes),
        scope_name=scope.name if scope is not None else None,
        start_ns=span.start_time or 0,
        end_ns=span.end_time or 0,
        status_code=span.status.status_code.value,
        status_message=span.status.description,
    )


def write_batch(root: str, rows: Sequence[dict]) -> list[str]:
    """Land ``rows`` as one closed file per day they fall on; the paths written."""
    by_day: dict[Any, list[dict]] = {}
    for row in rows:
        by_day.setdefault(row["_date"], []).append(row)
    written = []
    for day, day_rows in by_day.items():
        folder = _day_dir(root, day)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, f"batch-{uuid.uuid4().hex}.parquet")
        _write_atomic(pa.Table.from_pylist(day_rows, schema=TRACES_SCHEMA), path)
        written.append(path)
    return written


def landed_rows(spans: Sequence[Any]) -> list[dict]:
    """The rows of ``spans`` the traces table keeps (see the module docstring)."""
    rows = []
    for span in spans:
        scope = span.instrumentation_scope
        if scope is not None and scope.name in CATALOG_DRIVER_SCOPES:
            continue
        row = span_row(span)
        if is_trace_record(row):
            rows.append(row)
    return rows


def compact(
    root: str,
    target_bytes: int = COMPACT_TARGET_BYTES,
    *,
    grace_s: float,
    clock: Callable[[], float] = time.time,
) -> int:
    """Merge each day's files smaller than ``target_bytes`` into files of up to that size; the
    number of merged files removed.

    A merged file is kept in place, beside its merged copy, for ``grace_s`` seconds -- longer
    than any read of the folder can hold a listing of it (``limits.longest_request_timeout``) --
    and is removed by the first compaction after that; it is never merged again. A read listing
    the folder meanwhile opens every file it listed and reads each row once (``traces_scan``).
    The merged files are renamed into place before any input is recorded, so no row is ever
    absent from the folder."""
    if not os.path.isdir(root):
        return 0
    removed = 0
    for entry in sorted(os.listdir(root)):
        folder = os.path.join(root, entry)
        if not (entry.startswith("_date=") and os.path.isdir(folder)):
            continue
        merged = _merged(folder)
        small = [
            os.path.join(folder, name)
            for name in sorted(os.listdir(folder))
            if name.endswith(".parquet")
            and name not in merged
            and os.path.getsize(os.path.join(folder, name)) < target_bytes
        ]
        if len(small) >= 2:
            # Each file read as itself: a dataset read of a path under ``_date=...`` would take
            # the folder name for a partition column and clash with the file's own ``_date``.
            table = pa.concat_tables([pq.ParquetFile(p).read().cast(TRACES_SCHEMA) for p in small])
            in_bytes = sum(os.path.getsize(p) for p in small)
            # Rows per merged file, from the inputs' own bytes per row: files of about target_bytes.
            per_file = max(1, table.num_rows * target_bytes // max(in_bytes, 1))
            for start in range(0, table.num_rows, per_file):
                _write_atomic(
                    table.slice(start, per_file),
                    os.path.join(folder, f"rolled-{uuid.uuid4().hex}.parquet"),
                )
            at = clock()
            merged.update({os.path.basename(p): at for p in small})
        now = clock()
        for name, at in list(merged.items()):
            if now - at >= grace_s:
                os.remove(os.path.join(folder, name))
                del merged[name]
                removed += 1
        _write_merged(folder, merged)
    return removed


def _merged(folder: str) -> dict[str, float]:
    path = os.path.join(folder, MERGED_FILE)
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def _write_merged(folder: str, merged: dict[str, float]) -> None:
    path = os.path.join(folder, MERGED_FILE)
    if not merged:
        if os.path.exists(path):
            os.remove(path)
        return
    tmp = os.path.join(folder, f".{uuid.uuid4().hex}.tmp")
    with open(tmp, "w") as f:
        json.dump(merged, f)
    os.replace(tmp, path)


def _landed(root: str) -> pa.Table:
    """Every trace row landed under ``root``, each file read as itself (see ``compact``) and a
    merged batch kept beside its merged copy not read again."""
    paths = [
        os.path.join(folder, name)
        for folder, _dirs, names in os.walk(root)
        for name in sorted(names)
        if name.endswith(".parquet")
        and name != SCHEMA_FILE
        and not name.startswith(".")
        and name not in _merged(folder)
    ]
    tables = [pq.ParquetFile(p).read().cast(TRACES_SCHEMA) for p in sorted(paths)]
    return pa.concat_tables(tables) if tables else TRACES_SCHEMA.empty_table()


def refresh_records(root: str, mv_ids: Sequence[str]) -> list[dict]:
    """REQ-862: the refresh records landed under ``root`` of the materialized views ``mv_ids``,
    oldest first: what each refresh was built from, as its span recorded it."""
    import json

    from provisa.observability.ops_schema import MV_REFRESH_LINEAGE_SPAN

    wanted = set(mv_ids)
    rows = [
        r
        for r in _landed(root).to_pylist()
        if r["span_name"] == MV_REFRESH_LINEAGE_SPAN
        and json.loads(r["span_attributes"] or "{}").get("mv.id") in wanted
    ]
    return sorted(rows, key=lambda r: r["timestamp"])


def land_records(root: str, rows: Sequence[dict], org_id: str) -> list[str]:
    """REQ-862: land ``rows`` -- refresh records carried with the history they built -- as
    ``org_id``'s, its organisation the one that now holds that history. Each record keeps its
    times and its trace: a copy of what was recorded, not a new recording."""
    return write_batch(root, [{**r, "tenant_id": org_id} for r in rows])


class ParquetSpanExporter:
    """The span exporter of the native lane: each batch it is handed lands as closed files."""

    def __init__(self, root: str) -> None:
        self._root = root
        ensure_schema_file(root)

    def export(self, spans: Sequence[Any]) -> Any:
        from opentelemetry.sdk.trace.export import SpanExportResult

        rows = landed_rows(spans)
        if rows:
            write_batch(self._root, rows)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass

    def force_flush(self, timeout_millis: int = 30000) -> bool:  # noqa: ARG002
        return True
