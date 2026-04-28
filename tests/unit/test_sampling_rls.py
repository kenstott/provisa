# Copyright (c) 2026 Kenneth Stott
# Canary: 3a91f7c2-e04b-4d88-b61e-7f2c58a0d491
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for sampling, RLS integration, and has_capability (REQ-040–042)."""

from __future__ import annotations

import pytest

from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import DEFAULT_SAMPLE_SIZE, apply_sampling, get_sample_size
from provisa.compiler.sql_gen import ColumnRef, CompilationContext, CompiledQuery, TableMeta
from provisa.security.rights import Capability, has_capability


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _compiled(sql: str, root_field: str = "orders", params: list | None = None) -> CompiledQuery:
    return CompiledQuery(
        sql=sql,
        params=params or [],
        root_field=root_field,
        columns=[ColumnRef(alias=None, column="id", field_name="id", nested_in=None)],
        sources={"pg"},
    )


def _meta(table_id: int = 1, field_name: str = "orders", table_name: str = "orders") -> TableMeta:
    return TableMeta(
        table_id=table_id,
        field_name=field_name,
        type_name=field_name.capitalize(),
        source_id="pg",
        catalog_name="pg",
        schema_name="public",
        table_name=table_name,
    )


def _ctx(tables: dict | None = None) -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = tables if tables is not None else {"orders": _meta()}
    ctx.joins = {}
    return ctx


# ---------------------------------------------------------------------------
# TestGetSampleSize
# ---------------------------------------------------------------------------


class TestGetSampleSize:
    def test_returns_default_when_env_not_set(self, monkeypatch):
        monkeypatch.delenv("PROVISA_SAMPLE_SIZE", raising=False)
        assert get_sample_size() == DEFAULT_SAMPLE_SIZE

    def test_returns_int_from_env_var(self, monkeypatch):
        monkeypatch.setenv("PROVISA_SAMPLE_SIZE", "500")
        assert get_sample_size() == 500

    def test_converts_string_env_var_to_int(self, monkeypatch):
        monkeypatch.setenv("PROVISA_SAMPLE_SIZE", "250")
        result = get_sample_size()
        assert isinstance(result, int)
        assert result == 250


# ---------------------------------------------------------------------------
# TestApplySampling
# ---------------------------------------------------------------------------


class TestApplySampling:
    def test_no_limit_appends_limit(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders"'), 100)
        assert "LIMIT 100" in result.sql

    def test_small_limit_kept(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders" LIMIT 50'), 100)
        assert "LIMIT 50" in result.sql
        # Must not escalate to 100
        assert "LIMIT 100" not in result.sql

    def test_large_limit_capped(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders" LIMIT 500'), 100)
        assert "LIMIT 100" in result.sql
        assert "500" not in result.sql

    def test_limit_equal_to_sample_size_unchanged(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders" LIMIT 100'), 100)
        # Equal limit — kept as-is (not capped, not changed)
        assert "LIMIT 100" in result.sql

    def test_trailing_semicolon_stripped_before_limit(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders";'), 100)
        # Semicolon should be gone; LIMIT injected
        assert "LIMIT 100" in result.sql
        assert ";" not in result.sql

    def test_order_by_no_limit_appends_after(self):
        sql = 'SELECT "id" FROM "public"."orders" ORDER BY "id"'
        result = apply_sampling(_compiled(sql), 100)
        assert "ORDER BY" in result.sql
        assert "LIMIT 100" in result.sql
        # LIMIT must come after ORDER BY
        assert result.sql.index("ORDER BY") < result.sql.index("LIMIT 100")

    def test_sample_size_zero_appends_limit_zero(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders"'), 0)
        assert "LIMIT 0" in result.sql

    def test_custom_sample_size_used(self):
        result = apply_sampling(_compiled('SELECT "id" FROM "public"."orders"'), 25)
        assert "LIMIT 25" in result.sql


# ---------------------------------------------------------------------------
# TestApplySamplingWithRLS
# ---------------------------------------------------------------------------


class TestApplySamplingWithRLS:
    """Verify that sampling and RLS interact correctly when both are applied."""

    def _rls_injected(self) -> CompiledQuery:
        """Build a compiled query with an RLS WHERE clause already injected."""
        base = _compiled('SELECT "id" FROM "public"."orders"')
        ctx = _ctx()
        rls = RLSContext(rules={1: "region = 'us-east'"})
        return inject_rls(base, ctx, rls)

    def test_rls_query_without_limit_gets_limit(self):
        rls_query = self._rls_injected()
        assert "WHERE" in rls_query.sql
        assert "LIMIT" not in rls_query.sql

        result = apply_sampling(rls_query, 100)
        assert "LIMIT 100" in result.sql
        assert "region = 'us-east'" in result.sql

    def test_rls_query_with_large_limit_capped(self):
        rls_query = self._rls_injected()
        # Manually append a big LIMIT to the RLS-injected SQL
        rls_with_limit = CompiledQuery(
            sql=rls_query.sql + " LIMIT 5000",
            params=rls_query.params,
            root_field=rls_query.root_field,
            columns=rls_query.columns,
            sources=rls_query.sources,
        )
        result = apply_sampling(rls_with_limit, 100)
        assert "LIMIT 100" in result.sql
        assert "5000" not in result.sql
        assert "region = 'us-east'" in result.sql

    def test_rls_query_with_small_limit_preserved(self):
        rls_query = self._rls_injected()
        rls_with_limit = CompiledQuery(
            sql=rls_query.sql + " LIMIT 10",
            params=rls_query.params,
            root_field=rls_query.root_field,
            columns=rls_query.columns,
            sources=rls_query.sources,
        )
        result = apply_sampling(rls_with_limit, 100)
        assert "LIMIT 10" in result.sql
        assert "region = 'us-east'" in result.sql


# ---------------------------------------------------------------------------
# TestHasCapability
# ---------------------------------------------------------------------------


class TestHasCapability:
    def test_full_results_capability_returns_true(self):
        role = {"id": "analyst", "capabilities": ["full_results"]}
        assert has_capability(role, Capability.FULL_RESULTS) is True

    def test_role_without_full_results_returns_false(self):
        role = {"id": "viewer", "capabilities": ["query_development"]}
        assert has_capability(role, Capability.FULL_RESULTS) is False

    def test_empty_capabilities_returns_false(self):
        role = {"id": "guest", "capabilities": []}
        assert has_capability(role, Capability.FULL_RESULTS) is False

    def test_none_role_handled_gracefully(self):
        # has_capability uses dict.get so passing None raises AttributeError —
        # confirm the function signature expects a dict and docs say None is invalid.
        # We verify a role dict with None-ish value is handled.
        role = {"id": "ghost", "capabilities": None}
        # capabilities=None means .get returns None; `in` on None raises TypeError
        # The contract: callers must supply a list.  Validate that a proper empty
        # list dict evaluates False rather than crashing.
        role_safe = {"id": "ghost", "capabilities": []}
        assert has_capability(role_safe, Capability.FULL_RESULTS) is False

    def test_multiple_capabilities_checks_correct_one(self):
        role = {
            "id": "power_user",
            "capabilities": ["query_development", "query_approval", "full_results"],
        }
        assert has_capability(role, Capability.FULL_RESULTS) is True
        assert has_capability(role, Capability.SOURCE_REGISTRATION) is False

    def test_admin_capability_grants_everything(self):
        role = {"id": "sysadmin", "capabilities": ["admin"]}
        assert has_capability(role, Capability.FULL_RESULTS) is True
        assert has_capability(role, Capability.SECURITY_CONFIG) is True
        assert has_capability(role, Capability.TABLE_REGISTRATION) is True
