# Copyright (c) 2026 Kenneth Stott
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
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import sqlglot

from provisa.api._meta_views import _OPS_LOG_TABLE_ALIAS, _OPS_LOG_TABLE_VIEWS


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
        assert "INSERT INTO domains (id, description) VALUES ('ops'" in schema_sql

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

        reg_call = conn.upsert_returning.await_args
        payload = reg_call.args[1]
        assert payload["domain_id"] == "ops"
        assert payload["source_id"] == "provisa-admin"
        assert payload["table_name"] == "query_audit_log"

        registered_col_names = {call.args[1]["column_name"] for call in conn.upsert.await_args_list}
        assert {"id", "user_id", "status_code"} <= registered_col_names
        assert "query_text_enc" not in registered_col_names


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
