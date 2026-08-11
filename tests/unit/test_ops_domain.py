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
            "INSERT INTO domains (id, description, steward) "
            "VALUES ('ops', 'Operational telemetry', 'org_admin')" in schema_sql
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
}


class TestOpsStewardGrant:
    """REQ-1386: ops is a lockdown domain — a column with no grant is in no role's schema."""

    def test_telemetry_columns_seeded_visible_to_steward(self):
        from provisa.api.startup_seed import _seed_ops_pg

        conn = AsyncMock()
        conn.upsert_returning = AsyncMock(return_value=7)

        asyncio.run(_seed_ops_pg(conn))

        assert conn.upsert.await_args_list, "no ops telemetry columns seeded"
        for call in conn.upsert.await_args_list:
            payload = call.args[1]
            assert payload["visible_to"] == ["org_admin"], payload["column_name"]

    def test_existing_columns_converge_on_steward_grant(self):
        # A column seeded before the grant existed gains org_admin; grants made to other
        # roles through the UI (REQ-1133) are kept, and an already-granted column is untouched.
        from provisa.api.startup_seed import _ensure_ops_steward_grant

        rows = [(1, []), (2, ["analyst"]), (3, ["org_admin"])]

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
            ["org_admin"],
            ["analyst", "org_admin"],
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


@pytest.mark.parametrize("uri", ["sqlite+aiosqlite:///:memory:", "duckdb:///:memory:"])
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
    )
    from provisa.api.startup_seed import _adapt_view_ddl

    db = Database(create_engine_from_url(uri), name="ops-report-test")
    await _init_schema_portable(db)
    try:
        async with db.acquire() as conn:
            await conn.execute_core(insert(sources).values(id="s1", type="postgres"))
            await conn.execute_core(
                insert(registered_tables).values(
                    id=1, source_id="s1", domain_id="shelter",
                    schema_name="public", table_name="orders", description="orders",
                )
            )
            await conn.execute_core(
                insert(registered_tables).values(
                    id=2, source_id="s1", domain_id="shelter",
                    schema_name="public", table_name="customers",  # no description
                )
            )
            await conn.execute_core(
                insert(tag_assignments).values(
                    tag_id="deprecated", object_type="table", table_id=1,
                    object_key="table:1", reason="legacy",
                )
            )
            await conn.execute_core(
                insert(tag_assignments).values(
                    tag_id="pii", object_type="column", table_id=2,
                    column_name="email", object_key="column:2:email",
                )
            )
            for uid, tids, status, dur in (
                ("alice", [1, 2], 200, 40),
                ("alice", [1], 200, 10),
                ("bob", [2], 403, 5),
            ):
                await conn.execute_core(
                    insert(query_audit_log).values(
                        user_id=uid, role_id="analyst", query_hash="h",
                        table_ids=tids, source="graphql",
                        status_code=status, duration_ms=dur,
                    )
                )
            dialect = conn.capabilities.dialect
            await conn.execute(_adapt_view_ddl(_ops_table_usage_ddl(dialect), dialect))
            for ddl in _OPS_REPORT_VIEWS.values():
                await conn.execute(_adapt_view_ddl(ddl, dialect))

            async def rows(sql):
                return (await conn.execute_core(text(sql))).fetchall()

            usage = {
                r[0]: r[1]
                for r in await rows(
                    "SELECT id, query_count FROM usage_ranking"
                )
            }
            assert usage[1] == 2  # orders queried twice
            assert usage[2] == 2  # customers queried twice (incl. the denial)

            dep = await rows("SELECT table_id, user_id FROM deprecated_usage")
            assert {(r[0], r[1]) for r in dep} == {(1, "alice")}

            pii = await rows("SELECT table_id, pii_column, user_id FROM pii_access")
            assert (2, "email", "bob") in {(r[0], r[1], r[2]) for r in pii}

            denials = await rows("SELECT user_id, status_code FROM policy_denials")
            assert denials == [("bob", 403)]

            mix = await rows("SELECT source, query_count FROM surface_mix")
            assert mix == [("graphql", 3)]

            health = await rows(
                "SELECT query_count, error_count, max_duration_ms FROM query_health"
            )
            assert [(r[0], r[1], r[2]) for r in health] == [(3, 1, 40)]

            stale = {(r[0], r[1]) for r in await rows(
                "SELECT object_type, issue FROM stale_metadata"
            )}
            assert ("table", "missing_description") in stale  # customers
            assert ("domain", "missing_steward") in stale  # shelter et al.

            hot = await rows(
                "SELECT table_id_a, table_id_b, co_occurrence_count FROM join_hotspots"
            )
            assert [(r[0], r[1], r[2]) for r in hot] == [(1, 2, 1)]
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
