# Copyright (c) 2026 Kenneth Stott
# Canary: c78ec5c8-fc3f-4b51-a3ac-d1ddc3ad0b0e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-884: internal operational logs exposed as first-class ``ops``-domain tables.

query_audit_log (REQ-074) is registered as ``ops.query_audit_log`` in the federated
catalog and routed through the same role + domain access control as business tables.

REQ-1386: management report views (usage_ranking, deprecated_usage, pii_access,
policy_denials, surface_mix, query_health, stale_metadata, join_hotspots) created
and registered on the same seed path — always seeded on install, not demo data —
with org_admin stewarding the ops domain.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import sqlglot
from sqlglot.errors import ParseError

from provisa.api._meta_views import (
    _META_TABLE_VIEWS,
    _OPS_LOG_TABLE_ALIAS,
    _OPS_LOG_TABLE_VIEWS,
    _OPS_REPORT_VIEWS,
    _ops_table_usage_ddl,
)


# --------------------------------------------------------------------------- #
# Registry / seed structure                                                    #
# --------------------------------------------------------------------------- #


class TestOpsRegistry:
    def test_query_audit_log_registered_as_ops_log(self):
        # REQ-884: the audit log is in the ops-log registry, exposed via a view.
        assert "query_audit_log" in _OPS_LOG_TABLE_ALIAS
        assert _OPS_LOG_TABLE_ALIAS["query_audit_log"] == "query_audit_log_ops"
        assert "query_audit_log" in _OPS_LOG_TABLE_VIEWS

    def test_view_exposes_governed_columns(self):
        # REQ-884: safe columns exposed; REQ-689: encrypted text NOT exposed.
        ddl = _OPS_LOG_TABLE_VIEWS["query_audit_log"]
        for col in (
            "user_id",
            "role_id",
            "query_hash",
            "table_ids",
            "source",
            "status_code",
            "duration_ms",
            "logged_at",
        ):
            assert col in ddl, f"expected column {col!r} in ops view"
        assert "query_text_enc" not in ddl

    def test_ops_domain_seeded_in_schema(self):
        # REQ-884: the built-in ops domain row exists (FK target for registrations).
        schema_sql = (
            Path(__file__).resolve().parents[2] / "provisa" / "core" / "schema.sql"
        ).read_text()
        # REQ-1386: seeded with org_admin as steward.
        assert (
            "INSERT INTO domains (id, description, steward, origin) "
            "VALUES ('ops', 'Operational telemetry', 'org_admin', 'seed')" in schema_sql
        )

    def test_seed_registers_under_ops_domain(self):
        # REQ-884: _seed_ops_domain registers query_audit_log under source
        # provisa-admin / domain ops with the curated view's columns.
        from provisa.api.startup_seed import _seed_ops_domain

        view_cols = [
            {"column_name": "id", "data_type": "bigint", "is_primary_key": True},
            {"column_name": "user_id", "data_type": "text", "is_primary_key": False},
            {"column_name": "status_code", "data_type": "integer", "is_primary_key": False},
        ]
        conn = AsyncMock()
        conn.upsert_returning = AsyncMock(return_value=42)
        conn.reflect_columns = AsyncMock(return_value=view_cols)

        asyncio.run(_seed_ops_domain(conn, org_id="default"))

        reg_call = conn.upsert_returning.await_args_list[0]
        payload = reg_call.args[1]
        assert payload["domain_id"] == "ops"
        assert payload["source_id"] == "provisa-admin"
        assert payload["table_name"] == "query_audit_log"

        registered_col_names = {call.args[1]["column_name"] for call in conn.upsert.await_args_list}
        assert {"id", "user_id", "status_code"} <= registered_col_names
        assert "query_text_enc" not in registered_col_names

    def test_seed_registers_report_views_and_steward(self):
        # REQ-1386: every report view is registered under ops with pk `id`, and
        # org_admin is set as the ops-domain steward.
        from provisa.api.startup_seed import _seed_ops_domain

        view_cols = [
            {"column_name": "id", "data_type": "bigint", "is_primary_key": True},
            {"column_name": "user_id", "data_type": "text", "is_primary_key": False},
        ]
        conn = AsyncMock()
        conn.upsert_returning = AsyncMock(return_value=42)
        conn.reflect_columns = AsyncMock(return_value=view_cols)

        asyncio.run(_seed_ops_domain(conn, org_id="default"))

        registered = {
            call.args[1]["table_name"]: call.args[1]
            for call in conn.upsert_returning.await_args_list
        }
        for view_name in _OPS_REPORT_VIEWS:
            assert view_name in registered, f"report view {view_name!r} not registered"
            assert registered[view_name]["domain_id"] == "ops"
            assert registered[view_name]["source_id"] == "provisa-admin"

        steward_update = conn.execute_core.await_args_list[-1].args[0]
        compiled = str(steward_update)
        assert "UPDATE domains" in compiled
        assert steward_update.compile().params["steward"] == "org_admin"


# --------------------------------------------------------------------------- #
# Catalog surfacing (pgwire)                                                    #
# --------------------------------------------------------------------------- #


class _Col:
    def __init__(self, name: str, dtype: str, nullable: bool = True):
        self.column_name = name
        self.data_type = dtype
        self.is_nullable = nullable


def _ops_ctx():
    from provisa.compiler.sql_gen import CompilationContext, TableMeta

    ctx = CompilationContext()
    ctx.tables = {
        "query_audit_log": TableMeta(
            table_id=1,
            field_name="query_audit_log",
            type_name="QueryAuditLog",
            source_id="provisa-admin",
            catalog_name="provisa-admin",
            schema_name="org_default",
            table_name="query_audit_log",
            domain_id="ops",
        )
    }
    return ctx


class TestOpsCatalog:
    def test_ops_table_surfaces_in_catalog_index(self):
        # REQ-884: ops.query_audit_log appears in the pgwire catalog under schema 'ops'.
        from provisa.pgwire.catalog_populate import _build_catalog_index

        col_types = {
            1: [
                _Col("id", "bigint", nullable=False),
                _Col("user_id", "text"),
                _Col("status_code", "integer"),
            ]
        }
        idx = _build_catalog_index(_ops_ctx(), col_types)

        schemas = {row[1] for row in idx.tables}
        names = {(row[1], row[2]) for row in idx.tables}
        assert "ops" in schemas
        assert ("ops", "query_audit_log") in names

        toid = idx.table_id_to_oid[1]
        col_names = {c[1] for c in idx.all_cols if c[0] == toid}
        assert {"id", "user_id", "status_code"} <= col_names


# --------------------------------------------------------------------------- #
# Governance: role + domain access enforcement (V001)                          #
# --------------------------------------------------------------------------- #


def _gov_ctx():
    from provisa.compiler.stage2 import GovernanceContext

    gov = GovernanceContext()
    gov.table_map = {"ops.query_audit_log": 1, "query_audit_log": 1}
    return gov


def _table_id_to_meta():
    return {m.table_id: m for m in _ops_ctx().tables.values()}


class TestOpsGovernance:
    SQL = "SELECT user_id, status_code FROM ops.query_audit_log WHERE status_code = 200"

    def test_role_with_ops_access_allowed(self):
        # REQ-884: a role holding ops-domain access may query ops.query_audit_log.
        from provisa.compiler.sql_validator import _check_domain_access

        tree = sqlglot.parse_one(self.SQL, read="postgres")
        violations = _check_domain_access(
            tree, _gov_ctx(), _table_id_to_meta(), domain_access=["ops"]
        )
        assert violations == []

    def test_role_without_ops_access_denied(self):
        # REQ-884: a role lacking ops-domain access is denied (V001).
        from provisa.compiler.sql_validator import _check_domain_access

        tree = sqlglot.parse_one(self.SQL, read="postgres")
        violations = _check_domain_access(
            tree, _gov_ctx(), _table_id_to_meta(), domain_access=["shelter"]
        )
        assert any(v.code == "V001" for v in violations)
        assert any("ops" in v.message for v in violations)


# --------------------------------------------------------------------------- #
# REQ-1386: management report views                                            #
# --------------------------------------------------------------------------- #

_REPORT_VIEW_NAMES = {
    "usage_ranking",
    "deprecated_usage",
    "pii_access",
    "policy_denials",
    "surface_mix",
    "query_health",
    "stale_metadata",
    "join_hotspots",
    "tag_usage",
    "queries",
}


class _OtelEngine:
    """Stands in for the EngineRuntime the seed reads its otel-catalog capability from."""

    def __init__(self, has_otel_catalog: bool) -> None:
        self.has_otel_catalog = has_otel_catalog


class TestOpsStewardGrant:
    """REQ-1386: ops is a lockdown domain — a column with no grant is in no role's schema."""

    def test_telemetry_columns_seeded_visible_to_steward(self, monkeypatch):
        from provisa.api import startup_seed
        from provisa.api.startup_seed import _seed_ops_pg

        # The rows name the `otel` Iceberg catalog, which only an engine carrying it can reach
        # (EngineBackend.has_otel_catalog), so the seed runs behind that capability.
        monkeypatch.setattr(startup_seed.state, "federation_engine", _OtelEngine(True))
        conn = AsyncMock()
        conn.upsert_returning = AsyncMock(return_value=7)

        asyncio.run(_seed_ops_pg(conn))

        assert conn.upsert.await_args_list, "no ops telemetry columns seeded"
        for call in conn.upsert.await_args_list:
            payload = call.args[1]
            assert payload["visible_to"] == ["org_admin"], payload["column_name"]

    def test_engine_without_otel_catalog_drops_the_rows(self, monkeypatch):
        # An engine with no `otel` catalog would advertise unreachable tables: an unlabeled
        # Cypher MATCH (n) unions every label and compiled `FROM "otel"."signals"."traces"`.
        # The seed owns those rows, so it removes any a previous engine pinning left behind.
        from provisa.api import startup_seed
        from provisa.api.startup_seed import _seed_ops_pg

        monkeypatch.setattr(startup_seed.state, "federation_engine", _OtelEngine(False))
        conn = AsyncMock()
        conn.upsert_returning = AsyncMock(return_value=7)

        asyncio.run(_seed_ops_pg(conn))

        assert not conn.upsert.await_args_list, "ops rows seeded on an engine without otel"
        assert conn.execute_core.await_args_list, "stale provisa-otel rows not deleted"

    def test_existing_columns_converge_on_steward_grant(self):
        # A column seeded before the grant existed gains org_admin and sandbox (REQ-1608); grants
        # made to other roles through the UI (REQ-1133) are kept, and an already-granted column
        # is untouched.
        from provisa.api.startup_seed import _ensure_ops_steward_grant

        rows = [(1, []), (2, ["analyst"]), (3, ["org_admin", "sandbox"])]

        class _Result:
            def all(self):
                return rows

        calls: list = []

        async def _execute_core(stmt):
            calls.append(stmt)
            return _Result()

        conn = AsyncMock()
        conn.execute_core = _execute_core

        asyncio.run(_ensure_ops_steward_grant(conn))

        updates = [s for s in calls[1:]]
        assert len(updates) == 2, "expected exactly the two ungranted columns to be updated"
        assert [u.compile().params["visible_to"] for u in updates] == [
            ["org_admin", "sandbox"],
            ["analyst", "org_admin", "sandbox"],
        ]


class TestReportViewRegistry:
    def test_all_report_views_present(self):
        assert set(_OPS_REPORT_VIEWS) == _REPORT_VIEW_NAMES

    def test_every_view_exposes_id_and_no_encrypted_text(self):
        # Each view carries an `id` pk column; REQ-689: encrypted text never exposed.
        for name, ddl in _OPS_REPORT_VIEWS.items():
            assert "id" in ddl.lower(), f"{name}: no id column"
            assert "query_text_enc" not in ddl, f"{name}: exposes encrypted text"

    def test_report_view_ddl_parses_as_postgres(self):
        for name, ddl in _OPS_REPORT_VIEWS.items():
            try:
                sqlglot.parse_one(ddl, read="postgres")
            except ParseError as exc:  # pragma: no cover
                raise AssertionError(f"{name}: DDL does not parse: {exc}") from exc

    def test_unnest_base_view_per_dialect(self):
        assert "json_each" in _ops_table_usage_ddl("sqlite")
        assert "JSON_TABLE" in _ops_table_usage_ddl("mysql")
        assert "from_json" in _ops_table_usage_ddl("duckdb")
        assert "jsonb_array_elements_text" in _ops_table_usage_ddl("postgresql")


@pytest.mark.parametrize("uri", ["sqlite+pysqlite:///:memory:", "duckdb:///:memory:"])
async def test_report_views_functional(uri):
    """REQ-1386: create the report views on a real embedded backend, feed audit
    rows + tags, and query every view — validates the SQL on each dialect."""
    from sqlalchemy import insert, text

    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import _init_schema_portable
    from provisa.core.schema_org import (
        query_audit_log,
        registered_tables,
        sources,
        tag_assignments,
        user_directory,
    )
    from provisa.api.startup_seed import _adapt_view_ddl

    db = Database(create_engine_from_url(uri), name="ops-report-test")
    await _init_schema_portable(db)
    try:
        async with db.acquire() as conn:
            await conn.execute_core(
                insert(sources).values(id="s1", type="postgres", origin="admin")
            )
            await conn.execute_core(
                insert(registered_tables).values(
                    id=1,
                    source_id="s1",
                    domain_id="shelter",
                    schema_name="public",
                    table_name="orders",
                    description="orders",
                    origin="admin",
                )
            )
            await conn.execute_core(
                insert(registered_tables).values(
                    id=2,
                    source_id="s1",
                    domain_id="shelter",
                    schema_name="public",
                    table_name="customers",  # no description
                    origin="admin",
                )
            )
            await conn.execute_core(
                insert(tag_assignments).values(
                    tag_id="deprecated",
                    base_tag_id="deprecated",
                    object_type="table",
                    table_id=1,
                    object_key="table:1",
                    reason="legacy",
                    origin="admin",
                )
            )
            await conn.execute_core(
                insert(tag_assignments).values(
                    tag_id="pii",
                    base_tag_id="pii",
                    object_type="column",
                    table_id=2,
                    column_name="email",
                    object_key="column:2:email",
                    origin="admin",
                )
            )
            # REQ-1439: the tenant-local directory the report views resolve names through. bob is
            # deliberately absent, so the LEFT JOIN's null branch is exercised too.
            await conn.execute_core(
                insert(user_directory).values(
                    user_id="alice", display_name="Alice Adams", email="alice@example.com"
                )
            )
            for uid, tids, status, dur in (
                ("alice", [1, 2], 200, 40),
                ("alice", [1], 200, 10),
                ("bob", [2], 403, 5),
            ):
                await conn.execute_core(
                    insert(query_audit_log).values(
                        user_id=uid,
                        role_id="analyst",
                        query_hash="h",
                        table_ids=tids,
                        source="graphql",
                        status_code=status,
                        duration_ms=dur,
                    )
                )
            dialect = conn.capabilities.dialect
            await conn.execute(_adapt_view_ddl(_ops_table_usage_ddl(dialect), dialect))
            # tag_usage reads tags_meta, which _seed_meta_domain creates first at startup
            # (startup_seed.py: _seed_meta_domain then _seed_ops_domain). DuckDB resolves a
            # view's references when it is created, so the order matters here too.
            for ddl in _META_TABLE_VIEWS.values():
                await conn.execute(_adapt_view_ddl(ddl, dialect))
            for ddl in _OPS_REPORT_VIEWS.values():
                await conn.execute(_adapt_view_ddl(ddl, dialect))

            async def rows(sql):
                return (await conn.execute_core(text(sql))).fetchall()

            usage = {r[0]: r[1] for r in await rows("SELECT id, query_count FROM usage_ranking")}
            assert usage[1] == 2  # orders queried twice
            assert usage[2] == 2  # customers queried twice (incl. the denial)

            dep = await rows("SELECT table_id, user_id, user_name FROM deprecated_usage")
            assert {(r[0], r[1], r[2]) for r in dep} == {(1, "alice", "Alice Adams")}

            pii = await rows("SELECT table_id, pii_column, user_id, user_name FROM pii_access")
            # bob has no directory row, so the report still shows the access with a null name.
            assert (2, "email", "bob", None) in {(r[0], r[1], r[2], r[3]) for r in pii}

            denials = await rows("SELECT user_id, user_name, status_code FROM policy_denials")
            assert denials == [("bob", None, 403)]

            # REQ-1910: one row per statement; a statement over several tables names its first
            # registered table and counts them; a statement's own outcome and duration.
            queries = await rows(
                "SELECT user_id, role_id, source, status_code, duration_ms, table_name, "
                "domain_id, table_count, route, row_count FROM queries ORDER BY duration_ms DESC"
            )
            assert [tuple(r) for r in queries] == [
                ("alice", "analyst", "graphql", 200, 40, "orders", "shelter", 2, None, None),
                ("alice", "analyst", "graphql", 200, 10, "orders", "shelter", 1, None, None),
                ("bob", "analyst", "graphql", 403, 5, "customers", "shelter", 1, None, None),
            ]

            mix = await rows("SELECT source, query_count FROM surface_mix")
            assert mix == [("graphql", 3)]

            health = await rows(
                "SELECT query_count, error_count, max_duration_ms FROM query_health"
            )
            assert [(r[0], r[1], r[2]) for r in health] == [(3, 1, 40)]

            stale = {
                (r[0], r[1]) for r in await rows("SELECT object_type, issue FROM stale_metadata")
            }
            assert ("table", "missing_description") in stale  # customers
            assert ("domain", "missing_steward") in stale  # shelter et al.

            hot = await rows(
                "SELECT table_id_a, table_id_b, co_occurrence_count FROM join_hotspots"
            )
            assert [(r[0], r[1], r[2]) for r in hot] == [(1, 2, 1)]

            tags = {
                r[0]: r[1:]
                for r in await rows(
                    "SELECT id, assignment_count, tables_tagged, columns_tagged, "
                    "query_count, distinct_users FROM tag_usage"
                )
            }
            # deprecated marks table 1, read by two statements from one user.
            assert tags["deprecated"] == (1, 1, 0, 2, 1)
            # pii marks a column of table 2 — the tagged *table* is what carries the traffic,
            # so its three readers (alice's join, bob's denial) resolve to two statements.
            assert tags["pii"] == (1, 1, 1, 2, 2)
            # A system tag nobody applied is still a row, with zeroes rather than absent.
            assert tags["technical"] == (0, 0, 0, 0, 0)
    finally:
        await db.close()


class TestViewDdlIsReseedableAfterNarrowing:
    """A release that REMOVES a column from an ops view (the reports dropping tenant_id) must
    still seed against the previous release's view. PostgreSQL's CREATE OR REPLACE VIEW accepts
    only added trailing columns — a narrowed one raises "cannot drop columns from view" and the
    app never finishes startup."""

    def test_every_dialect_drops_the_view_before_recreating_it(self):
        from provisa.api.startup_seed import _adapt_view_ddl

        ddl = "CREATE OR REPLACE VIEW policy_denials AS SELECT id FROM query_audit_log"
        for dialect in ("postgresql", "sqlite", "mysql", "duckdb"):
            adapted = _adapt_view_ddl(ddl, dialect)
            assert adapted.startswith("DROP VIEW IF EXISTS policy_denials"), dialect
            assert "CREATE VIEW policy_denials AS" in adapted, dialect
            assert "OR REPLACE" not in adapted, dialect

    def test_postgres_cascades_so_a_dependent_report_view_does_not_block_the_drop(self):
        from provisa.api.startup_seed import _adapt_view_ddl

        ddl = "CREATE OR REPLACE VIEW query_audit_log_ops AS SELECT id FROM query_audit_log"
        assert "DROP VIEW IF EXISTS query_audit_log_ops CASCADE;" in _adapt_view_ddl(
            ddl, "postgresql"
        )
        # SQLite/MySQL have no CASCADE keyword for DROP VIEW.
        assert "CASCADE" not in _adapt_view_ddl(ddl, "sqlite")
        assert "CASCADE" not in _adapt_view_ddl(ddl, "mysql")


async def test_the_queries_report_from_the_audit_log_matches_the_report_from_query_spans(tmp_path):
    """REQ-1910: the `queries` report moved from the trace table's ``provisa.query.*`` spans to
    ``query_audit_log``. On a fixed mix — three requests, one of them two statements — the old
    report (every span stored, the span-name predicate) and the audit-backed report list the same
    statements: same table, domain, role and outcome, one row per statement."""
    import sqlalchemy as sa
    from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
    from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
    from opentelemetry.proto.trace.v1.trace_pb2 import ResourceSpans, ScopeSpans, Span, Status
    from sqlalchemy import insert, text

    from provisa.api.startup_seed import _adapt_view_ddl
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.db import _init_schema_portable
    from provisa.core.schema_org import query_audit_log, registered_tables, sources
    from provisa.observability import otlp2sql
    from provisa.observability.ops_schema import ensure_tables

    # (trace, table id, table, role, failed) — the second request is two statements.
    mix = [
        ("a1", 1, "orders", "analyst", False),
        ("a2", 2, "customers", "org_admin", False),
        ("a2", 1, "orders", "org_admin", True),
        ("a3", 2, "customers", "analyst", False),
    ]

    # Old report: every span a traces row; the view selected the provisa.query.* spans.
    def _span(trace, span_id, name, parent="", attrs=None, failed=False):
        return Span(
            trace_id=bytes.fromhex(trace * 16),
            span_id=bytes.fromhex(f"{span_id:016x}"),
            parent_span_id=bytes.fromhex(parent) if parent else b"",
            name=name,
            start_time_unix_nano=1_720_000_000_000_000_000,
            end_time_unix_nano=1_720_000_000_050_000_000,
            status=Status(code=Status.STATUS_CODE_ERROR if failed else Status.STATUS_CODE_OK),
            attributes=[
                KeyValue(key=k, value=AnyValue(string_value=v)) for k, v in (attrs or {}).items()
            ],
        )

    spans = []
    for n, (trace, _tid, table, role, failed) in enumerate(mix):
        root = f"{n + 1:016x}"
        spans += [
            _span(trace, n + 1, "POST /data/graphql"),
            _span(trace, n + 101, "GET GET", parent=root),
            _span(
                trace,
                n + 201,
                "provisa.query.duckdb",
                parent=root,
                attrs={"provisa.table": table, "provisa.domain": "shelter", "provisa.role": role},
                failed=failed,
            ),
        ]
    otel = sa.create_engine(f"sqlite:///{tmp_path / 'otel.sqlite'}")
    traces = ensure_tables(otel)["traces"]
    req = ExportTraceServiceRequest(
        resource_spans=[ResourceSpans(scope_spans=[ScopeSpans(spans=spans)])]
    )
    with otel.begin() as cx:
        cx.execute(sa.insert(traces), otlp2sql._trace_rows(req))
        old = sorted(
            (r[0], r[1], r[2], r[3], r[4] == Status.STATUS_CODE_ERROR)
            for r in cx.execute(
                sa.text(
                    "SELECT trace_id, table_name, domain_id, role_id, status_code FROM traces "
                    "WHERE span_name LIKE 'provisa.query%'"
                )
            )
        )

    # New report: the audit rows the same statements wrote, read through the seeded view.
    db = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.sqlite'}"), name="queries"
    )
    await _init_schema_portable(db)
    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="s1", type="postgres", origin="admin"))
        for tid, name in ((1, "orders"), (2, "customers")):
            await conn.execute_core(
                insert(registered_tables).values(
                    id=tid,
                    source_id="s1",
                    domain_id="shelter",
                    schema_name="public",
                    table_name=name,
                    origin="admin",
                )
            )
        for trace, tid, _table, role, failed in mix:
            await conn.execute_core(
                insert(query_audit_log).values(
                    user_id="u",
                    role_id=role,
                    query_hash="h",
                    table_ids=[tid],
                    source="graphql",
                    status_code=500 if failed else 200,
                    duration_ms=50,
                    trace_id=trace * 16,
                )
            )
        dialect = conn.capabilities.dialect
        await conn.execute(_adapt_view_ddl(_ops_table_usage_ddl(dialect), dialect))
        await conn.execute(_adapt_view_ddl(_OPS_REPORT_VIEWS["queries"], dialect))
        new = sorted(
            (r[0], r[1], r[2], r[3], r[4] >= 400)
            for r in (
                await conn.execute_core(
                    text(
                        "SELECT trace_id, table_name, domain_id, role_id, status_code FROM queries"
                    )
                )
            ).fetchall()
        )

    assert len(new) == len(mix)  # one row per statement, the two-statement request included
    assert new == old
