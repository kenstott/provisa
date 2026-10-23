# Copyright (c) 2026 Kenneth Stott
# Canary: 3c3c1d18-62ef-4033-a756-a528fede5fca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A native engine's traces (REQ-862): the process lands its trace records as Parquet, the engine
reads them at otel.signals.traces -- the name Trino reads -- with the refresh lineage view over
them, each org reads only its own, and each day's small files are merged into files of up to
64 MB, no row lost. Run on DuckDB."""

# Requirements: REQ-862, REQ-1349, REQ-1910

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from typing import Any

import duckdb
import pyarrow.parquet as pq
import pytest
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from provisa.api.otel_setup import OrgSpanProcessor
from provisa.compiler.rls import ORG_SCOPED_SIGNALS_RULE, RLSContext, org_scoped_signals
from provisa.core.request_context import current_org, session_vars_for
from provisa.mv import refresh
from provisa.mv.models import MVDefinition
from provisa.observability import native_traces as nt
from provisa.observability import otlp2sql
from provisa.observability.ops_schema import (
    MV_REFRESH_LINEAGE_SQL,
    MV_REFRESH_LINEAGE_VIEW,
    OPS_TABLES,
    REQUEST_SPAN_ATTR,
)
from provisa.pgwire._pipeline import _resolve_session_settings
from provisa.transpiler.transpile import transpile_to_duckdb, transpile_to_trino

pytestmark = pytest.mark.unit


@contextmanager
def _serving(org: str | None):
    token = current_org.set(org)
    try:
        yield
    finally:
        current_org.reset(token)


def _provider(exporter) -> TracerProvider:
    provider = TracerProvider(resource=Resource.create({"service.name": "provisa"}))
    provider.add_span_processor(OrgSpanProcessor())
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider


def _engine(root: str) -> duckdb.DuckDBPyConnection:
    """The views a DuckDB engine defines over the landed traces, as its attach walk does."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS otel")
    con.execute("CREATE SCHEMA otel.signals")
    for name, select in nt.signal_views(root, transpile_to_duckdb).items():
        con.execute(f"CREATE VIEW otel.signals.{name} AS {select}")
    return con


def _refresh_lineage(provider: TracerProvider, mv_id: str) -> None:
    """One MV refresh's lineage record, emitted by the refresh's own code."""
    mv = MVDefinition(
        id=mv_id,
        source_tables=["orders"],
        target_catalog="mat",
        target_schema="mv",
        target_table=f"{mv_id}_t",
    )
    tracer = refresh._tracer
    refresh._tracer = provider.get_tracer("provisa.mv.refresh")
    try:
        refresh._emit_column_lineage_span(
            mv, "SELECT o.id, o.amount * 2 AS doubled FROM orders o", "2026-10-17T00:00:00"
        )
    finally:
        refresh._tracer = tracer


def test_a_span_is_the_same_row_from_the_sdk_as_from_the_wire():
    exporter = InMemorySpanExporter()
    provider = _provider(exporter)
    with _serving("acme"), provider.get_tracer("t").start_as_current_span("work") as span:
        span.set_attribute("provisa.table", "orders")
    [finished] = exporter.get_finished_spans()
    assert nt.span_row(finished) == otlp2sql._trace_rows(encode_spans([finished]))[0]


def test_the_lane_keeps_records_and_drops_detail_and_driver_statements():
    exporter = InMemorySpanExporter()
    provider = _provider(exporter)
    tracer = provider.get_tracer("t")
    with tracer.start_as_current_span("background-root"):
        with tracer.start_as_current_span("detail"):
            pass
        with tracer.start_as_current_span("request") as req:
            req.set_attribute(REQUEST_SPAN_ATTR, "http")
    driver = provider.get_tracer("opentelemetry.instrumentation.psycopg")
    with driver.start_as_current_span("SELECT 1"):
        pass
    kept = {row["span_name"] for row in nt.landed_rows(exporter.get_finished_spans())}
    assert kept == {"background-root", "request"}


def test_with_nothing_landed_the_engine_reads_an_empty_table_of_every_column(tmp_path):
    con = _engine(str(tmp_path / "traces"))
    assert con.execute("SELECT count(*) FROM otel.signals.traces").fetchone() == (0,)
    cols = [r[0] for r in con.execute("DESCRIBE otel.signals.traces").fetchall()]
    assert cols == [c for c, _t, _k in OPS_TABLES["traces"]]


def test_a_refresh_is_read_back_through_the_lineage_view(tmp_path):
    root = str(tmp_path / "traces")
    con = _engine(root)
    exporter = nt.ParquetSpanExporter(root)
    provider = _provider(exporter)
    with _serving("acme"):
        _refresh_lineage(provider, "view-view_exposure")
    [(mv_id, target, definition, kind, trace_id, derivations, tenant)] = con.execute(
        "SELECT mv_id, target_table, definition_version, input_version_kind, trace_id, "
        f"column_derivations, tenant_id FROM otel.signals.{MV_REFRESH_LINEAGE_VIEW}"
    ).fetchall()
    assert (mv_id, target, tenant) == ("view-view_exposure", "view-view_exposure_t", "acme")
    assert definition and kind and len(trace_id) == 32
    assert {d["output"] for d in json.loads(derivations)} == {"id", "doubled"}


def test_a_refresh_inside_a_request_is_still_a_record_of_its_own(tmp_path):
    exporter = InMemorySpanExporter()
    provider = _provider(exporter)
    with provider.get_tracer("t").start_as_current_span("POST /admin/mv/refresh") as req:
        req.set_attribute(REQUEST_SPAN_ATTR, "http")
        _refresh_lineage(provider, "view-view_exposure")
    rows = nt.landed_rows(exporter.get_finished_spans())
    assert {r["span_name"] for r in rows} == {"POST /admin/mv/refresh", "mv.refresh.column_lineage"}
    lineage = next(r for r in rows if r["span_name"] == "mv.refresh.column_lineage")
    assert lineage["parent_span_id"] is None


def test_each_org_reads_only_its_own_refreshes(tmp_path):
    root = str(tmp_path / "traces")
    con = _engine(root)
    provider = _provider(nt.ParquetSpanExporter(root))
    with _serving("acme"):
        _refresh_lineage(provider, "view-acme_report")
    with _serving("globex"):
        _refresh_lineage(provider, "view-globex_report")
    _refresh_lineage(provider, "view-no_org")  # recorded for no org

    tables = [
        {"id": 7, "source_id": "provisa-otel", "table_name": "traces"},
        {"id": 10, "source_id": "provisa-otel", "table_name": MV_REFRESH_LINEAGE_VIEW},
    ]
    assert org_scoped_signals(RLSContext.empty(), tables).rules[10] == ORG_SCOPED_SIGNALS_RULE

    def read(org: str | None, table: str) -> set[str]:
        with _serving(org):
            rule = _resolve_session_settings(
                ORG_SCOPED_SIGNALS_RULE, session_vars_for(None), "duckdb"
            )
        column = "mv_id" if table == MV_REFRESH_LINEAGE_VIEW else "span_attributes ->> 'mv.id'"
        sql = f"SELECT {column} FROM otel.signals.{table} WHERE {rule}"
        return {r[0] for r in con.execute(transpile_to_duckdb(sql)).fetchall()}

    for table in (MV_REFRESH_LINEAGE_VIEW, "traces"):
        assert read("acme", table) == {"view-acme_report"}
        assert read("globex", table) == {"view-globex_report"}
        assert read(None, table) == set()


@pytest.mark.parametrize(
    ("transpile", "reads"),
    [
        (transpile_to_duckdb, """span_attributes ->> '$."lineage.definition_version"'"""),
        (
            transpile_to_trino,
            """JSON_EXTRACT_SCALAR(span_attributes, '$["lineage.definition_version"]')""",
        ),
    ],
    ids=["duckdb", "trino"],
)
def test_the_lineage_view_reads_a_dotted_attribute_on_each_engine(transpile, reads):
    sql = transpile(MV_REFRESH_LINEAGE_SQL)
    assert f"{reads} AS definition_version" in sql
    # Never a cast to JSON first: Trino's CAST(varchar AS JSON) is a JSON string, not a parse.
    assert "AS JSON" not in sql.upper()
    assert "otel.signals.traces" in sql


def _files(root) -> dict[str, list[str]]:
    return {
        day: sorted(os.listdir(os.path.join(root, day)))
        for day in sorted(os.listdir(root))
        if day.startswith("_date=")
    }


def _land(root: str, days: dict[str, int]) -> None:
    import datetime as dt

    for day, batches in days.items():
        for n in range(batches):
            ts = dt.datetime.fromisoformat(f"{day}T12:00:00")
            row: dict[str, Any] = {c: None for c, _t, _k in OPS_TABLES["traces"]}
            row.update(
                trace_id=f"{day}-{n}",
                span_id=f"{n:016x}",
                span_name="work",
                timestamp=ts,
                span_attributes="{}",
                _date=ts.date(),
            )
            nt.write_batch(root, [row])


def test_each_batch_is_a_closed_file_in_its_days_folder(tmp_path):
    root = str(tmp_path / "traces")
    _land(root, {"2026-10-16": 2, "2026-10-17": 1})
    files = _files(root)
    assert {day: len(names) for day, names in files.items()} == {
        "_date=2026-10-16": 2,
        "_date=2026-10-17": 1,
    }
    assert all(
        n.startswith("batch-") and n.endswith(".parquet") for ns in files.values() for n in ns
    )


def test_compaction_merges_each_days_small_files_and_keeps_every_row(tmp_path):
    root = str(tmp_path / "traces")
    con = _engine(root)
    _land(root, {"2026-10-16": 5, "2026-10-17": 3})
    before = sorted(con.execute("SELECT trace_id FROM otel.signals.traces").fetchall())
    assert nt.compact(root, grace_s=0) == 8
    after = sorted(con.execute("SELECT trace_id FROM otel.signals.traces").fetchall())
    assert after == before
    files = _files(root)
    assert {day: len(names) for day, names in files.items()} == {
        "_date=2026-10-16": 1,
        "_date=2026-10-17": 1,
    }
    assert all(n.startswith("rolled-") for ns in files.values() for n in ns)


def test_compaction_rolls_a_day_into_files_of_the_target_size(tmp_path):
    root = str(tmp_path / "traces")
    _land(root, {"2026-10-17": 6})
    one = os.path.getsize(
        os.path.join(root, "_date=2026-10-17", _files(root)["_date=2026-10-17"][0])
    )
    # A target of about two batches' bytes: the six rows roll into three files.
    nt.compact(root, target_bytes=2 * one + 1, grace_s=0)
    names = _files(root)["_date=2026-10-17"]
    assert len(names) == 3
    rows = [
        pq.ParquetFile(os.path.join(root, "_date=2026-10-17", n)).metadata.num_rows for n in names
    ]
    assert sorted(rows) == [2, 2, 2]


def test_compaction_leaves_full_files_and_a_lone_file_as_they_are(tmp_path):
    root = str(tmp_path / "traces")
    _land(root, {"2026-10-17": 1})
    assert nt.compact(root, grace_s=0) == 0
    assert len(_files(root)["_date=2026-10-17"]) == 1
    _land(root, {"2026-10-17": 1})
    # Both files are at least the target: neither is small, nothing is merged.
    assert nt.compact(root, target_bytes=1, grace_s=0) == 0
    assert len(_files(root)["_date=2026-10-17"]) == 2


def test_with_no_folder_compaction_has_nothing_to_do(tmp_path):
    assert nt.compact(str(tmp_path / "absent"), grace_s=0) == 0


def test_the_process_lands_traces_only_while_the_engine_reads_them(monkeypatch, tmp_path):
    from provisa.api import otel_setup

    monkeypatch.setenv("PROVISA_TELEMETRY_DIR", str(tmp_path))
    exports = otel_setup._SpanExports()
    monkeypatch.setattr(otel_setup, "_span_exports", exports)
    otel_setup.land_traces_natively(True)
    [processor] = exports.current()
    assert os.path.isfile(os.path.join(nt.traces_dir(), nt.SCHEMA_FILE))
    otel_setup.land_traces_natively(False)
    assert exports.current() == []
    processor.shutdown()


def _vanishing(gone: set[str]) -> duckdb.DuckDBPyConnection:
    """A connection whose ``vanish://`` filesystem lists the files named in ``gone`` but has
    them no longer there when they are opened: compaction removed them between glob and read."""
    from fsspec.implementations.memory import MemoryFileSystem

    class Vanishing(MemoryFileSystem):
        protocol = ("vanish",)
        store: dict = {}
        pseudo_dirs = [""]

        def _open(self, path, mode="rb", *args, **kwargs):
            if mode.startswith("r") and any(path.endswith(f"/{name}") for name in gone):
                raise FileNotFoundError(path)
            return super()._open(path, mode, *args, **kwargs)

    con = duckdb.connect()
    con.register_filesystem(Vanishing())
    return con


def test_defining_the_view_opens_no_batch_compaction_may_have_removed(tmp_path):
    """The warm-up's CREATE VIEW once read every listed file to bind the columns, and a batch
    compaction removed after the listing failed it. It binds from the schema file alone."""
    local = str(tmp_path / "traces")
    nt.ensure_schema_file(local)
    _land(local, {"2026-10-23": 1})
    [(day, [batch])] = _files(local).items()
    con = _vanishing({batch})
    for name in (nt.SCHEMA_FILE, f"{day}/{batch}"):
        con.execute(f"COPY (SELECT * FROM read_parquet('{local}/{name}')) TO 'vanish://t/{name}'")
    listed = sorted(
        r[0] for r in con.execute("SELECT * FROM glob('vanish://t/**/*.parquet')").fetchall()
    )
    assert listed == [f"vanish://t/{day}/{batch}", f"vanish://t/{nt.SCHEMA_FILE}"]
    every_file = (
        "SELECT * FROM read_parquet('vanish://t/**/*.parquet', "
        "union_by_name = true, hive_partitioning = false)"
    )
    with pytest.raises(duckdb.Error, match="FileNotFoundError"):
        con.execute(f"CREATE VIEW bound_from_every_file AS {every_file}")
    con.execute(f"CREATE VIEW traces AS {nt.traces_scan('vanish://t')}")
    cols = [r[0] for r in con.execute("DESCRIBE traces").fetchall()]
    assert cols == [c for c, _t, _k in OPS_TABLES["traces"]]


def test_a_read_after_compaction_lists_the_folder_again(tmp_path):
    """The view names a glob, not the files listed when it was defined: a read after compaction
    finds the merged file and no removed batch."""
    root = str(tmp_path / "traces")
    con = _engine(root)
    _land(root, {"2026-10-23": 3})
    before = sorted(con.execute("SELECT trace_id FROM otel.signals.traces").fetchall())
    assert nt.compact(root, grace_s=0) == 3
    assert sorted(con.execute("SELECT trace_id FROM otel.signals.traces").fetchall()) == before


def test_a_merged_batch_is_kept_while_a_read_could_hold_its_listing(tmp_path):
    """A read lists the folder and then opens each file: compaction keeps each batch it merged in
    place for the grace, so a listing taken before it still opens every file, and the view reads
    each row once meanwhile. The first compaction after the grace removes them."""
    root = str(tmp_path / "traces")
    con = _engine(root)
    _land(root, {"2026-10-23": 3})
    before = sorted(con.execute("SELECT trace_id FROM otel.signals.traces").fetchall())
    day = os.path.join(root, "_date=2026-10-23")
    listed = sorted(os.path.join(day, n) for n in os.listdir(day) if n.endswith(".parquet"))

    clock = [1000.0]
    assert nt.compact(root, grace_s=60, clock=lambda: clock[0]) == 0
    # The read that listed the batches before compaction opens every one of them.
    paths = ", ".join(f"'{p}'" for p in listed)
    assert con.execute(f"SELECT count(*) FROM read_parquet([{paths}])").fetchone() == (3,)
    # Each row once, though each is in its batch and in the merged file.
    assert sorted(con.execute("SELECT trace_id FROM otel.signals.traces").fetchall()) == before
    names = [n for n in _files(root)["_date=2026-10-23"] if n.endswith(".parquet")]
    assert len([n for n in names if n.startswith("rolled-")]) == 1 and len(names) == 4

    # Within the grace a second compaction neither removes the batches nor merges them again.
    clock[0] = 1059.0
    assert nt.compact(root, grace_s=60, clock=lambda: clock[0]) == 0
    assert len([n for n in _files(root)["_date=2026-10-23"] if n.endswith(".parquet")]) == 4
    clock[0] = 1061.0
    assert nt.compact(root, grace_s=60, clock=lambda: clock[0]) == 3
    assert [n[:7] for n in _files(root)["_date=2026-10-23"]] == ["rolled-"]
    assert sorted(con.execute("SELECT trace_id FROM otel.signals.traces").fetchall()) == before
    assert not os.path.exists(os.path.join(day, nt.MERGED_FILE))


def test_refresh_records_are_read_once_while_their_batches_are_kept(tmp_path):
    root = str(tmp_path / "traces")
    _land(root, {"2026-10-23": 3})
    rows = len(nt._landed(root))
    nt.compact(root, grace_s=60)
    assert len(nt._landed(root)) == rows
