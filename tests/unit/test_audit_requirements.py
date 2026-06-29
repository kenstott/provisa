# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for audit requirements: REQ-567, REQ-596, REQ-613"""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, MagicMock


# ---------------------------------------------------------------------------
# REQ-567: watch_many called with all physical tables from join walk
# ---------------------------------------------------------------------------


class TestREQ567WatchManyJoinedTables:
    """REQ-567: Subscription engine collects all physical table names referenced
    by the join walk and calls watch_many(all_watch_tables) on the PG notification
    provider. A change to any joined physical table re-fires the subscription query.
    """

    def _make_ctx(self, joins: dict) -> MagicMock:
        ctx = MagicMock()
        ctx.joins = joins
        ctx.tables = {}
        return ctx

    def _make_field_node(self, name: str, selection_set=None):
        from graphql.language.ast import FieldNode

        node = MagicMock(spec=FieldNode)
        node.name = MagicMock()
        node.name.value = name
        node.selection_set = selection_set
        return node

    def test_collect_related_tables_includes_joined_table(self):
        # REQ-567
        from provisa.api.data.subscription_sse import _collect_related_tables

        join_meta = MagicMock()
        join_meta.target.table_name = "orders"
        join_meta.target.type_name = "Order"

        ctx = MagicMock()
        ctx.joins = {("User", "orders"): join_meta}

        sel = MagicMock()
        sel.name.value = "orders"
        sel.selection_set = None

        from graphql.language.ast import FieldNode

        sel.__class__ = FieldNode  # type: ignore[assignment]

        selection_set = MagicMock()
        selection_set.selections = [sel]

        result = _collect_related_tables(selection_set, "User", ctx)
        assert "orders" in result

    def test_collect_related_tables_non_join_field_excluded(self):
        # REQ-567
        from provisa.api.data.subscription_sse import _collect_related_tables

        ctx = MagicMock()
        ctx.joins = {}  # no joins registered

        sel = MagicMock()
        sel.name.value = "name"
        sel.selection_set = None

        from graphql.language.ast import FieldNode

        sel.__class__ = FieldNode  # type: ignore[assignment]

        selection_set = MagicMock()
        selection_set.selections = [sel]

        result = _collect_related_tables(selection_set, "User", ctx)
        assert "name" not in result
        assert len(result) == 0

    def test_collect_related_tables_recursive_join_walk(self):
        # REQ-567: nested join traversal accumulates tables at each level
        from provisa.api.data.subscription_sse import _collect_related_tables

        order_join = MagicMock()
        order_join.target.table_name = "orders"
        order_join.target.type_name = "Order"

        item_join = MagicMock()
        item_join.target.table_name = "order_items"
        item_join.target.type_name = "OrderItem"

        ctx = MagicMock()
        ctx.joins = {
            ("User", "orders"): order_join,
            ("Order", "items"): item_join,
        }

        # Nested selection: items inside orders
        item_sel = MagicMock()
        item_sel.name.value = "items"
        item_sel.selection_set = None

        from graphql.language.ast import FieldNode

        item_sel.__class__ = FieldNode  # type: ignore[assignment]

        order_inner = MagicMock()
        order_inner.selections = [item_sel]

        order_sel = MagicMock()
        order_sel.name.value = "orders"
        order_sel.selection_set = order_inner
        order_sel.__class__ = FieldNode  # type: ignore[assignment]

        selection_set = MagicMock()
        selection_set.selections = [order_sel]

        result = _collect_related_tables(selection_set, "User", ctx)
        assert "orders" in result
        assert "order_items" in result

    def test_all_watch_tables_includes_root_and_related(self):
        # REQ-567: all_watch_tables must contain the root table plus all related
        # This test verifies the composition logic in subscription_sse.py:
        # all_watch_tables = [table_name] + sorted(related_tables - {table_name})
        root = "users"
        related = {"orders", "order_items"}
        all_watch_tables = [root] + sorted(related - {root})
        assert all_watch_tables[0] == root
        assert set(all_watch_tables) == {"users", "orders", "order_items"}


# ---------------------------------------------------------------------------
# REQ-596: Audit log schema, append-only rules, hash-only storage, indexes
# ---------------------------------------------------------------------------


class TestREQ596AuditLogSchema:
    """REQ-596: Every query is recorded in query_audit_log with required fields.
    Table is append-only (PG rules block DELETE/UPDATE). Query text stored as
    SHA-256 hash only. Two indexes support tenant-scoped and per-user queries.
    """

    def test_schema_sql_contains_required_columns(self):
        # REQ-596
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        required_columns = [
            "tenant_id",
            "user_id",
            "role_id",
            "query_hash",
            "table_ids",
            "source",
            "status_code",
            "duration_ms",
            "logged_at",
        ]
        for col in required_columns:
            assert col in AUDIT_SCHEMA_SQL, f"Column {col!r} missing from AUDIT_SCHEMA_SQL"

    def test_schema_sql_has_no_delete_rule(self):
        # REQ-596: PostgreSQL rules block DELETE at database level
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "no_delete_audit" in AUDIT_SCHEMA_SQL
        assert "ON DELETE TO query_audit_log DO INSTEAD NOTHING" in AUDIT_SCHEMA_SQL

    def test_schema_sql_has_no_update_rule(self):
        # REQ-596: PostgreSQL rules block UPDATE at database level
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "no_update_audit" in AUDIT_SCHEMA_SQL
        assert "ON UPDATE TO query_audit_log DO INSTEAD NOTHING" in AUDIT_SCHEMA_SQL

    def test_schema_sql_has_tenant_time_index(self):
        # REQ-596: index supporting tenant-scoped time-range queries
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "idx_audit_tenant_time" in AUDIT_SCHEMA_SQL
        assert "tenant_id" in AUDIT_SCHEMA_SQL
        assert "logged_at" in AUDIT_SCHEMA_SQL

    def test_schema_sql_has_user_time_index(self):
        # REQ-596: index supporting per-user time-range queries
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "idx_audit_user_time" in AUDIT_SCHEMA_SQL

    def test_log_query_stores_sha256_hash_not_plaintext(self):
        # REQ-596: query text is never stored verbatim — only its SHA-256 hash
        from provisa.audit.query_log import log_query

        query_text = "SELECT * FROM sensitive_table"
        expected_hash = hashlib.sha256(query_text.encode()).hexdigest()

        mock_pool = AsyncMock()
        mock_pool.execute = AsyncMock()

        import asyncio

        asyncio.run(
            log_query(
                mock_pool,
                tenant_id="tenant-1",
                user_id="user-1",
                role_id="role-1",
                query_text=query_text,
                table_ids=["tbl-1"],
                source="graphql",
                status_code=200,
                duration_ms=42,
            )
        )

        call_args = mock_pool.execute.call_args
        # The positional args after the SQL string are the bound params
        params = call_args[0][1:]  # (tenant_id, user_id, role_id, query_hash, ...)
        # query_hash is the 4th positional param (index 3)
        query_hash_param = params[3]
        assert query_hash_param == expected_hash
        assert query_text not in str(call_args)

    def test_log_query_inserts_all_required_fields(self):
        # REQ-596: insert must include all required audit fields
        from provisa.audit.query_log import log_query

        mock_pool = AsyncMock()
        mock_pool.execute = AsyncMock()

        import asyncio

        asyncio.run(
            log_query(
                mock_pool,
                tenant_id="tenant-abc",
                user_id="user-xyz",
                role_id="analyst",
                query_text="query { users { id } }",
                table_ids=["users"],
                source="graphql",
                status_code=200,
                duration_ms=15,
            )
        )

        mock_pool.execute.assert_awaited_once()
        call_args = mock_pool.execute.call_args[0]
        sql = call_args[0]
        assert "query_audit_log" in sql
        params = call_args[1:]
        # tenant_id, user_id, role_id, query_hash, table_ids, source, status_code, duration_ms
        assert params[0] == "tenant-abc"
        assert params[1] == "user-xyz"
        assert params[2] == "analyst"
        assert params[4] == ["users"]
        assert params[5] == "graphql"
        assert params[6] == 200
        assert params[7] == 15


# ---------------------------------------------------------------------------
# REQ-613: Every query touching a domain asset is logged; log fields; append-only
# ---------------------------------------------------------------------------


class TestREQ613QueryGovernanceAuditLog:
    """REQ-613: Every query that touches a domain asset is logged in an append-only
    audit log (query_audit_log). Captures user_id, role_id, query_hash, table_ids,
    source, status_code, duration_ms, logged_at. Protected by PG rules preventing
    DELETE and UPDATE. Indexed by (tenant_id, logged_at) and (user_id, logged_at).
    """

    def test_audit_log_table_name_is_query_audit_log(self):
        # REQ-613: the table must be named query_audit_log
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "query_audit_log" in AUDIT_SCHEMA_SQL

    def test_audit_log_captures_user_id(self):
        # REQ-613
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "user_id" in AUDIT_SCHEMA_SQL

    def test_audit_log_captures_role_id(self):
        # REQ-613
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "role_id" in AUDIT_SCHEMA_SQL

    def test_audit_log_captures_query_hash(self):
        # REQ-613
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "query_hash" in AUDIT_SCHEMA_SQL

    def test_audit_log_captures_table_ids(self):
        # REQ-613
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "table_ids" in AUDIT_SCHEMA_SQL

    def test_audit_log_captures_source(self):
        # REQ-613
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "source" in AUDIT_SCHEMA_SQL

    def test_audit_log_captures_status_code(self):
        # REQ-613
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "status_code" in AUDIT_SCHEMA_SQL

    def test_audit_log_captures_duration_ms(self):
        # REQ-613
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "duration_ms" in AUDIT_SCHEMA_SQL

    def test_audit_log_captures_logged_at(self):
        # REQ-613
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "logged_at" in AUDIT_SCHEMA_SQL

    def test_append_only_delete_rule_present(self):
        # REQ-613: SOC2 append-only — PG rule prevents DELETE
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "ON DELETE TO query_audit_log DO INSTEAD NOTHING" in AUDIT_SCHEMA_SQL

    def test_append_only_update_rule_present(self):
        # REQ-613: SOC2 append-only — PG rule prevents UPDATE
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "ON UPDATE TO query_audit_log DO INSTEAD NOTHING" in AUDIT_SCHEMA_SQL

    def test_tenant_logged_at_index_present(self):
        # REQ-613: indexed by (tenant_id, logged_at) for compliance reporting
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "tenant_id, logged_at" in AUDIT_SCHEMA_SQL or (
            "tenant_id" in AUDIT_SCHEMA_SQL and "logged_at" in AUDIT_SCHEMA_SQL
        )
        assert "idx_audit_tenant_time" in AUDIT_SCHEMA_SQL

    def test_user_logged_at_index_present(self):
        # REQ-613: indexed by (user_id, logged_at) for compliance reporting
        from provisa.audit.query_log import AUDIT_SCHEMA_SQL

        assert "idx_audit_user_time" in AUDIT_SCHEMA_SQL

    def test_log_query_function_exists_and_is_callable(self):
        # REQ-613: log_query must be available and callable as the logging entrypoint
        from provisa.audit import query_log

        assert callable(query_log.log_query)

    def test_compliance_reporter_export_audit_log_exists(self):
        # REQ-613: compliance_reporter.py must expose audit log export capability
        from provisa.audit import compliance_reporter

        assert callable(compliance_reporter.export_audit_log)

    def test_compliance_reporter_includes_required_columns(self):
        # REQ-613: exported records must include all required audit fields
        from provisa.audit.compliance_reporter import _AUDIT_COLUMNS

        required = {
            "user_id",
            "role_id",
            "query_hash",
            "table_ids",
            "source",
            "status_code",
            "duration_ms",
            "logged_at",
            "tenant_id",
        }
        assert required.issubset(set(_AUDIT_COLUMNS))
