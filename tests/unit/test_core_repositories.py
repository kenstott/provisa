# Copyright (c) 2026 Kenneth Stott
# Canary: f4a5b6c7-d8e9-0123-def0-234567890123
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for core repositories: source, domain, role, table, relationship, rls, function."""

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, call

from provisa.core.models import (
    Cardinality,
    Column,
    Domain,
    Function,
    FunctionArgument,
    GovernanceLevel,
    InlineType,
    RLSRule,
    Relationship,
    Role,
    Source,
    SourceType,
    Table,
    Webhook,
)
from provisa.core.repositories import (
    domain as domain_repo,
    role as role_repo,
    rls as rls_repo,
    source as source_repo,
    table as table_repo,
    relationship as rel_repo,
    function as func_repo,
)


# ---------------------------------------------------------------------------
# Shared factories
# ---------------------------------------------------------------------------


def _source(id="pg1", type="postgresql", **kwargs):
    return Source(
        id=id,
        type=type,
        host=kwargs.get("host", "localhost"),
        port=kwargs.get("port", 5432),
        database=kwargs.get("database", "provisa"),
        username=kwargs.get("username", "user"),
        password=kwargs.get("password", "pw"),
    )


def _domain(id="sales", description="Sales data"):
    return Domain(id=id, description=description)


def _role(id="analyst", capabilities=None, domain_access=None):
    return Role(
        id=id,
        capabilities=capabilities or ["query_development"],
        domain_access=domain_access or ["sales"],
    )


def _table(source_id="pg1", schema="public", table="orders", domain_id="sales",
           governance="pre-approved", columns=None):
    return Table(
        source_id=source_id,
        domain_id=domain_id,
        **{"schema": schema, "table": table},
        governance=governance,
        columns=columns or [
            Column(name="id", visible_to=["admin", "analyst"]),
            Column(name="amount", visible_to=["admin"]),
        ],
    )


def _relationship(
    id="orders-customers",
    source_table_id="orders",
    target_table_id="customers",
    source_column="customer_id",
    target_column="id",
    cardinality="many-to-one",
):
    return Relationship(
        id=id,
        source_table_id=source_table_id,
        target_table_id=target_table_id,
        source_column=source_column,
        target_column=target_column,
        cardinality=cardinality,
    )


def _rls_rule(table_id="orders", role_id="analyst", filter="region = 'us'"):
    return RLSRule(table_id=table_id, role_id=role_id, filter=filter)


def _function(**kwargs):
    defaults = dict(
        name="get_order",
        source_id="pg1",
        **{"schema": "public"},
        function_name="get_order_fn",
        returns="pg1.public.orders",
        arguments=[FunctionArgument(name="order_id", type="Int")],
        visible_to=["admin"],
        writable_by=["admin"],
        domain_id="sales",
        description="Fetch order by id",
    )
    defaults.update(kwargs)
    return Function(**defaults)


def _webhook(**kwargs):
    defaults = dict(
        name="notify",
        url="https://hook.example.com/notify",
        method="POST",
        timeout_ms=5000,
        returns=None,
        inline_return_type=[InlineType(name="ticket_id", type="String")],
        arguments=[FunctionArgument(name="message", type="String")],
        visible_to=["admin"],
        domain_id="ops",
        description="Send notification",
    )
    defaults.update(kwargs)
    return Webhook(**defaults)


def _make_conn():
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value="OK")
    conn.fetchval = AsyncMock(return_value=1)
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetch = AsyncMock(return_value=[])
    return conn


def _make_row(**kwargs):
    """Return a MagicMock that acts as an asyncpg Row (supports dict conversion)."""
    row = MagicMock()
    row.__iter__ = MagicMock(return_value=iter(kwargs.items()))
    row.keys = MagicMock(return_value=list(kwargs.keys()))
    # Support dict(row) via mapping protocol
    row.__class__ = type("FakeRow", (), {"keys": lambda s: list(kwargs.keys())})
    # Simplest approach: make it return a real dict from dict()
    # asyncpg rows support dict() via __iter__ of (key, value) pairs
    # We'll use a simpler approach: monkey-patch
    real_dict = dict(**kwargs)
    row._real = real_dict

    class _Row(dict):
        pass

    return _Row(kwargs)


# ---------------------------------------------------------------------------
# source_repo
# ---------------------------------------------------------------------------


class TestSourceRepo:
    @pytest.mark.asyncio
    async def test_upsert_calls_execute(self):
        conn = _make_conn()
        src = _source()
        await source_repo.upsert(conn, src)
        conn.execute.assert_awaited_once()
        sql, *args = conn.execute.call_args[0]
        assert "INSERT INTO sources" in sql
        assert "ON CONFLICT" in sql

    @pytest.mark.asyncio
    async def test_upsert_passes_correct_values(self):
        conn = _make_conn()
        src = _source(id="pg1", type="postgresql")
        await source_repo.upsert(conn, src)
        _, *args = conn.execute.call_args[0]
        assert "pg1" in args
        assert "postgresql" in args

    @pytest.mark.asyncio
    async def test_upsert_dialect_for_postgresql(self):
        conn = _make_conn()
        src = _source(type="postgresql")
        await source_repo.upsert(conn, src)
        _, *args = conn.execute.call_args[0]
        assert "postgres" in args  # dialect value

    @pytest.mark.asyncio
    async def test_upsert_empty_dialect_for_mongodb(self):
        conn = _make_conn()
        src = _source(id="mg1", type="mongodb", port=27017)
        await source_repo.upsert(conn, src)
        _, *args = conn.execute.call_args[0]
        assert "" in args  # empty dialect

    @pytest.mark.asyncio
    async def test_get_returns_none_when_not_found(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        result = await source_repo.get(conn, "nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_returns_dict_when_found(self):
        conn = _make_conn()
        row = _make_row(id="pg1", type="postgresql", host="localhost", port=5432,
                        database="d", username="u", dialect="postgres")
        conn.fetchrow = AsyncMock(return_value=row)
        result = await source_repo.get(conn, "pg1")
        assert result is not None
        assert result["id"] == "pg1"

    @pytest.mark.asyncio
    async def test_list_all_returns_empty_list(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        result = await source_repo.list_all(conn)
        assert result == []

    @pytest.mark.asyncio
    async def test_list_all_returns_dicts(self):
        conn = _make_conn()
        rows = [
            _make_row(id="pg1", type="postgresql", host="h", port=5432, database="d",
                      username="u", dialect="postgres"),
            _make_row(id="my1", type="mysql", host="h", port=3306, database="d",
                      username="u", dialect="mysql"),
        ]
        conn.fetch = AsyncMock(return_value=rows)
        result = await source_repo.list_all(conn)
        assert len(result) == 2
        assert result[0]["id"] == "pg1"

    @pytest.mark.asyncio
    async def test_delete_returns_true_on_match(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 1")
        result = await source_repo.delete(conn, "pg1")
        assert result is True

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_not_found(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 0")
        result = await source_repo.delete(conn, "missing")
        assert result is False

    @pytest.mark.asyncio
    async def test_get_queries_correct_table(self):
        conn = _make_conn()
        await source_repo.get(conn, "pg1")
        sql = conn.fetchrow.call_args[0][0]
        assert "FROM sources" in sql

    @pytest.mark.asyncio
    async def test_delete_queries_correct_table(self):
        conn = _make_conn()
        await source_repo.delete(conn, "pg1")
        sql = conn.execute.call_args[0][0]
        assert "DELETE FROM sources" in sql


# ---------------------------------------------------------------------------
# domain_repo
# ---------------------------------------------------------------------------


class TestDomainRepo:
    @pytest.mark.asyncio
    async def test_upsert_calls_execute(self):
        conn = _make_conn()
        dom = _domain()
        await domain_repo.upsert(conn, dom)
        conn.execute.assert_awaited_once()
        sql = conn.execute.call_args[0][0]
        assert "INSERT INTO domains" in sql

    @pytest.mark.asyncio
    async def test_upsert_passes_id_and_description(self):
        conn = _make_conn()
        dom = _domain(id="finance", description="Finance domain")
        await domain_repo.upsert(conn, dom)
        _, *args = conn.execute.call_args[0]
        assert "finance" in args
        assert "Finance domain" in args

    @pytest.mark.asyncio
    async def test_get_returns_none_when_missing(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        assert await domain_repo.get(conn, "nonexistent") is None

    @pytest.mark.asyncio
    async def test_get_returns_dict(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=_make_row(id="sales", description="Sales"))
        result = await domain_repo.get(conn, "sales")
        assert result["id"] == "sales"

    @pytest.mark.asyncio
    async def test_list_all_empty(self):
        conn = _make_conn()
        result = await domain_repo.list_all(conn)
        assert result == []

    @pytest.mark.asyncio
    async def test_delete_true_on_match(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 1")
        assert await domain_repo.delete(conn, "sales") is True

    @pytest.mark.asyncio
    async def test_delete_false_when_missing(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 0")
        assert await domain_repo.delete(conn, "missing") is False


# ---------------------------------------------------------------------------
# role_repo
# ---------------------------------------------------------------------------


class TestRoleRepo:
    @pytest.mark.asyncio
    async def test_upsert_calls_execute(self):
        conn = _make_conn()
        role = _role()
        await role_repo.upsert(conn, role)
        conn.execute.assert_awaited_once()
        sql = conn.execute.call_args[0][0]
        assert "INSERT INTO roles" in sql

    @pytest.mark.asyncio
    async def test_upsert_passes_capabilities(self):
        conn = _make_conn()
        role = _role(id="admin", capabilities=["admin", "query_development"], domain_access=["*"])
        await role_repo.upsert(conn, role)
        _, *args = conn.execute.call_args[0]
        assert "admin" in args
        caps = next(a for a in args if isinstance(a, list) and "admin" in a)
        assert "admin" in caps

    @pytest.mark.asyncio
    async def test_get_returns_none_when_missing(self):
        conn = _make_conn()
        assert await role_repo.get(conn, "missing") is None

    @pytest.mark.asyncio
    async def test_get_returns_dict(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(
            return_value=_make_row(id="analyst", capabilities=["query_development"],
                                   domain_access=["sales"])
        )
        result = await role_repo.get(conn, "analyst")
        assert result["id"] == "analyst"

    @pytest.mark.asyncio
    async def test_list_all_empty(self):
        conn = _make_conn()
        assert await role_repo.list_all(conn) == []

    @pytest.mark.asyncio
    async def test_delete_true_on_match(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 1")
        assert await role_repo.delete(conn, "analyst") is True

    @pytest.mark.asyncio
    async def test_delete_false_when_missing(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 0")
        assert await role_repo.delete(conn, "missing") is False


# ---------------------------------------------------------------------------
# table_repo
# ---------------------------------------------------------------------------


class TestTableRepo:
    @pytest.mark.asyncio
    async def test_upsert_returns_table_id(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=42)
        conn.execute = AsyncMock(return_value="OK")
        tbl = _table()
        table_id = await table_repo.upsert(conn, tbl)
        assert table_id == 42

    @pytest.mark.asyncio
    async def test_upsert_inserts_table_row(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=1)
        conn.execute = AsyncMock(return_value="OK")
        tbl = _table()
        await table_repo.upsert(conn, tbl)
        sql = conn.fetchval.call_args[0][0]
        assert "INSERT INTO registered_tables" in sql

    @pytest.mark.asyncio
    async def test_upsert_deletes_old_columns(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=7)
        conn.execute = AsyncMock(return_value="OK")
        tbl = _table()
        await table_repo.upsert(conn, tbl)
        execute_sqls = [c[0][0] for c in conn.execute.call_args_list]
        assert any("DELETE FROM table_columns" in s for s in execute_sqls)

    @pytest.mark.asyncio
    async def test_upsert_inserts_columns(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=1)
        conn.execute = AsyncMock(return_value="OK")
        tbl = _table(columns=[
            Column(name="id", visible_to=["admin"]),
            Column(name="name", visible_to=["admin", "analyst"]),
        ])
        await table_repo.upsert(conn, tbl)
        execute_sqls = [c[0][0] for c in conn.execute.call_args_list]
        column_inserts = [s for s in execute_sqls if "INSERT INTO table_columns" in s]
        assert len(column_inserts) == 2

    @pytest.mark.asyncio
    async def test_get_returns_none_when_missing(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        result = await table_repo.get(conn, 99)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_returns_dict_with_columns(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(
            return_value=_make_row(
                id=1, source_id="pg1", domain_id="sales",
                schema_name="public", table_name="orders",
                governance="pre-approved", alias=None, description=None,
            )
        )
        conn.fetch = AsyncMock(return_value=[
            _make_row(
                column_name="id", visible_to=["admin"], writable_by=[],
                unmasked_to=[], mask_type=None, mask_pattern=None,
                mask_replace=None, mask_value=None, mask_precision=None,
            )
        ])
        result = await table_repo.get(conn, 1)
        assert result is not None
        assert result["table_name"] == "orders"
        assert len(result["columns"]) == 1

    @pytest.mark.asyncio
    async def test_get_by_name_returns_none_when_missing(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        result = await table_repo.get_by_name(conn, "pg1", "public", "missing_table")
        assert result is None

    @pytest.mark.asyncio
    async def test_find_by_table_name_returns_none_when_missing(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        result = await table_repo.find_by_table_name(conn, "missing_table")
        assert result is None

    @pytest.mark.asyncio
    async def test_find_by_table_name_raises_on_ambiguity(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[
            _make_row(id=1, source_id="pg1", schema_name="public", table_name="orders",
                      domain_id="sales", governance="pre-approved", alias=None, description=None),
            _make_row(id=2, source_id="pg2", schema_name="public", table_name="orders",
                      domain_id="sales", governance="pre-approved", alias=None, description=None),
        ])
        with pytest.raises(ValueError, match="Ambiguous table name"):
            await table_repo.find_by_table_name(conn, "orders")

    @pytest.mark.asyncio
    async def test_find_by_table_name_returns_single_match(self):
        conn = _make_conn()
        row = _make_row(id=1, source_id="pg1", schema_name="public", table_name="orders",
                        domain_id="sales", governance="pre-approved", alias=None, description=None)
        conn.fetch = AsyncMock(return_value=[row])
        result = await table_repo.find_by_table_name(conn, "orders")
        assert result is not None
        assert result["table_name"] == "orders"

    @pytest.mark.asyncio
    async def test_list_all_empty(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        result = await table_repo.list_all(conn)
        assert result == []

    @pytest.mark.asyncio
    async def test_delete_returns_true(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 1")
        assert await table_repo.delete(conn, 1) is True

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_missing(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 0")
        assert await table_repo.delete(conn, 99) is False

    @pytest.mark.asyncio
    async def test_upsert_passes_governance_value(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=1)
        conn.execute = AsyncMock(return_value="OK")
        tbl = _table(governance="registry-required")
        await table_repo.upsert(conn, tbl)
        sql, *args = conn.fetchval.call_args[0]
        assert "registry-required" in args


# ---------------------------------------------------------------------------
# relationship_repo
# ---------------------------------------------------------------------------


class TestRelationshipRepo:
    @pytest.mark.asyncio
    async def test_upsert_resolves_table_names(self):
        conn = _make_conn()

        source_row = _make_row(id=1, source_id="pg1", schema_name="public",
                               table_name="orders", domain_id="sales",
                               governance="pre-approved", alias=None, description=None)
        target_row = _make_row(id=2, source_id="pg1", schema_name="public",
                               table_name="customers", domain_id="sales",
                               governance="pre-approved", alias=None, description=None)

        fetch_results = [[source_row], [target_row]]
        call_count = 0

        async def _fetch(sql, *args, **kwargs):
            nonlocal call_count
            result = fetch_results[call_count] if call_count < len(fetch_results) else []
            call_count += 1
            return result

        conn.fetch = _fetch
        rel = _relationship()
        await rel_repo.upsert(conn, rel)
        sql = conn.execute.call_args[0][0]
        assert "INSERT INTO relationships" in sql

    @pytest.mark.asyncio
    async def test_upsert_raises_when_source_table_missing(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])  # no tables found
        rel = _relationship()
        with pytest.raises(ValueError, match="Source table not registered"):
            await rel_repo.upsert(conn, rel)

    @pytest.mark.asyncio
    async def test_upsert_raises_when_target_table_missing(self):
        conn = _make_conn()
        source_row = _make_row(id=1, source_id="pg1", schema_name="public",
                               table_name="orders", domain_id="sales",
                               governance="pre-approved", alias=None, description=None)

        call_count = 0

        async def _fetch(sql, *args, **kwargs):
            nonlocal call_count
            result = [source_row] if call_count == 0 else []
            call_count += 1
            return result

        conn.fetch = _fetch
        rel = _relationship()
        with pytest.raises(ValueError, match="Target table not registered"):
            await rel_repo.upsert(conn, rel)

    @pytest.mark.asyncio
    async def test_get_returns_none_when_missing(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        assert await rel_repo.get(conn, "nonexistent") is None

    @pytest.mark.asyncio
    async def test_get_returns_dict(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=_make_row(
            id="orders-customers", source_table_id=1, target_table_id=2,
            source_column="customer_id", target_column="id",
            cardinality="many-to-one", materialize=False, refresh_interval=300,
        ))
        result = await rel_repo.get(conn, "orders-customers")
        assert result["id"] == "orders-customers"

    @pytest.mark.asyncio
    async def test_list_all_empty(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        assert await rel_repo.list_all(conn) == []

    @pytest.mark.asyncio
    async def test_delete_true_on_match(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 1")
        assert await rel_repo.delete(conn, "orders-customers") is True

    @pytest.mark.asyncio
    async def test_delete_false_when_missing(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 0")
        assert await rel_repo.delete(conn, "missing") is False

    @pytest.mark.asyncio
    async def test_upsert_stores_cardinality_value(self):
        conn = _make_conn()
        source_row = _make_row(id=1, source_id="pg1", schema_name="public",
                               table_name="orders", domain_id="sales",
                               governance="pre-approved", alias=None, description=None)
        target_row = _make_row(id=2, source_id="pg1", schema_name="public",
                               table_name="customers", domain_id="sales",
                               governance="pre-approved", alias=None, description=None)
        call_count = 0

        async def _fetch(sql, *args, **kwargs):
            nonlocal call_count
            result = [source_row] if call_count == 0 else [target_row]
            call_count += 1
            return result

        conn.fetch = _fetch
        rel = _relationship(cardinality="one-to-many")
        await rel_repo.upsert(conn, rel)
        _, *args = conn.execute.call_args[0]
        assert "one-to-many" in args


# ---------------------------------------------------------------------------
# rls_repo
# ---------------------------------------------------------------------------


class TestRLSRepo:
    def _conn_with_table(self, table_id=1):
        """Return a conn where find_by_table_name resolves to the given table_id."""
        conn = _make_conn()
        row = _make_row(
            id=table_id, source_id="pg1", schema_name="public",
            table_name="orders", domain_id="sales",
            governance="pre-approved", alias=None, description=None,
        )
        conn.fetch = AsyncMock(return_value=[row])
        return conn

    @pytest.mark.asyncio
    async def test_upsert_calls_execute(self):
        conn = self._conn_with_table(table_id=1)
        rule = _rls_rule()
        await rls_repo.upsert(conn, rule)
        conn.execute.assert_awaited_once()
        sql = conn.execute.call_args[0][0]
        assert "INSERT INTO rls_rules" in sql

    @pytest.mark.asyncio
    async def test_upsert_raises_when_table_not_found(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])  # no table found
        rule = _rls_rule()
        with pytest.raises(ValueError, match="Table not registered"):
            await rls_repo.upsert(conn, rule)

    @pytest.mark.asyncio
    async def test_upsert_passes_filter_expr(self):
        conn = self._conn_with_table(table_id=5)
        rule = _rls_rule(filter="tenant_id = 'acme'")
        await rls_repo.upsert(conn, rule)
        _, *args = conn.execute.call_args[0]
        assert "tenant_id = 'acme'" in args

    @pytest.mark.asyncio
    async def test_upsert_passes_table_id_and_role(self):
        conn = self._conn_with_table(table_id=5)
        rule = _rls_rule(role_id="viewer")
        await rls_repo.upsert(conn, rule)
        _, *args = conn.execute.call_args[0]
        assert 5 in args
        assert "viewer" in args

    @pytest.mark.asyncio
    async def test_get_for_table_role_returns_none(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        result = await rls_repo.get_for_table_role(conn, 1, "analyst")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_for_table_role_returns_dict(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=_make_row(
            id=1, table_id=1, role_id="analyst", filter_expr="region = 'us'"
        ))
        result = await rls_repo.get_for_table_role(conn, 1, "analyst")
        assert result["filter_expr"] == "region = 'us'"

    @pytest.mark.asyncio
    async def test_list_for_role_empty(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        result = await rls_repo.list_for_role(conn, "analyst")
        assert result == []

    @pytest.mark.asyncio
    async def test_list_for_role_returns_matching_rules(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[
            _make_row(id=1, table_id=1, role_id="analyst", filter_expr="region = 'us'"),
            _make_row(id=2, table_id=2, role_id="analyst", filter_expr="active = true"),
        ])
        result = await rls_repo.list_for_role(conn, "analyst")
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_list_all_returns_all_rules(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[
            _make_row(id=1, table_id=1, role_id="analyst", filter_expr="x = 1"),
        ])
        result = await rls_repo.list_all(conn)
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_delete_returns_true(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 1")
        assert await rls_repo.delete(conn, 1, "analyst") is True

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_missing(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 0")
        assert await rls_repo.delete(conn, 99, "analyst") is False

    @pytest.mark.asyncio
    async def test_delete_sql_correct(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 1")
        await rls_repo.delete(conn, 3, "admin")
        sql, *args = conn.execute.call_args[0]
        assert "DELETE FROM rls_rules" in sql
        assert 3 in args
        assert "admin" in args


# ---------------------------------------------------------------------------
# function_repo (upsert_function, get_function, list_functions, delete_function)
# ---------------------------------------------------------------------------


class TestFunctionRepo:
    @pytest.mark.asyncio
    async def test_upsert_function_returns_id(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=10)
        fn = _function()
        result = await func_repo.upsert_function(conn, fn)
        assert result == 10

    @pytest.mark.asyncio
    async def test_upsert_function_calls_fetchval(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=1)
        fn = _function()
        await func_repo.upsert_function(conn, fn)
        conn.fetchval.assert_awaited_once()
        sql = conn.fetchval.call_args[0][0]
        assert "INSERT INTO tracked_functions" in sql

    @pytest.mark.asyncio
    async def test_upsert_function_serializes_arguments(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=1)
        fn = _function()
        await func_repo.upsert_function(conn, fn)
        _, *args = conn.fetchval.call_args[0]
        args_json = next(a for a in args if isinstance(a, str) and "order_id" in a)
        parsed = json.loads(args_json)
        assert parsed[0]["name"] == "order_id"

    @pytest.mark.asyncio
    async def test_get_function_returns_none_when_missing(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        result = await func_repo.get_function(conn, "missing_fn")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_function_returns_dict_with_parsed_args(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=_make_row(
            id=1,
            name="get_order",
            source_id="pg1",
            schema_name="public",
            function_name="get_order_fn",
            returns="pg1.public.orders",
            arguments=json.dumps([{"name": "order_id", "type": "Int"}]),
            visible_to=["admin"],
            writable_by=["admin"],
            domain_id="sales",
            description="Fetch order",
        ))
        result = await func_repo.get_function(conn, "get_order")
        assert result is not None
        assert result["name"] == "get_order"
        assert isinstance(result["arguments"], list)
        assert result["arguments"][0]["name"] == "order_id"

    @pytest.mark.asyncio
    async def test_list_functions_empty(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        result = await func_repo.list_functions(conn)
        assert result == []

    @pytest.mark.asyncio
    async def test_delete_function_returns_true(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 1")
        assert await func_repo.delete_function(conn, "get_order") is True

    @pytest.mark.asyncio
    async def test_delete_function_returns_false_when_missing(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 0")
        assert await func_repo.delete_function(conn, "missing_fn") is False

    @pytest.mark.asyncio
    async def test_delete_function_sql_correct(self):
        conn = _make_conn()
        await func_repo.delete_function(conn, "my_fn")
        sql = conn.execute.call_args[0][0]
        assert "DELETE FROM tracked_functions" in sql


# ---------------------------------------------------------------------------
# function_repo — webhooks
# ---------------------------------------------------------------------------


class TestWebhookRepo:
    @pytest.mark.asyncio
    async def test_upsert_webhook_returns_id(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=5)
        wh = _webhook()
        result = await func_repo.upsert_webhook(conn, wh)
        assert result == 5

    @pytest.mark.asyncio
    async def test_upsert_webhook_calls_fetchval(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=1)
        wh = _webhook()
        await func_repo.upsert_webhook(conn, wh)
        sql = conn.fetchval.call_args[0][0]
        assert "INSERT INTO tracked_webhooks" in sql

    @pytest.mark.asyncio
    async def test_upsert_webhook_serializes_inline_return_type(self):
        conn = _make_conn()
        conn.fetchval = AsyncMock(return_value=1)
        wh = _webhook()
        await func_repo.upsert_webhook(conn, wh)
        _, *args = conn.fetchval.call_args[0]
        irt_json = next(
            (a for a in args if isinstance(a, str) and "ticket_id" in a), None
        )
        assert irt_json is not None
        parsed = json.loads(irt_json)
        assert parsed[0]["name"] == "ticket_id"

    @pytest.mark.asyncio
    async def test_get_webhook_returns_none_when_missing(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=None)
        result = await func_repo.get_webhook(conn, "missing")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_webhook_parses_arguments(self):
        conn = _make_conn()
        conn.fetchrow = AsyncMock(return_value=_make_row(
            id=1,
            name="notify",
            url="https://hook.example.com",
            method="POST",
            timeout_ms=5000,
            returns=None,
            inline_return_type=json.dumps([{"name": "ticket_id", "type": "String"}]),
            arguments=json.dumps([{"name": "message", "type": "String"}]),
            visible_to=["admin"],
            domain_id="ops",
            description="Send notification",
        ))
        result = await func_repo.get_webhook(conn, "notify")
        assert result is not None
        assert isinstance(result["arguments"], list)
        assert result["arguments"][0]["name"] == "message"
        assert isinstance(result["inline_return_type"], list)
        assert result["inline_return_type"][0]["name"] == "ticket_id"

    @pytest.mark.asyncio
    async def test_list_webhooks_empty(self):
        conn = _make_conn()
        conn.fetch = AsyncMock(return_value=[])
        result = await func_repo.list_webhooks(conn)
        assert result == []

    @pytest.mark.asyncio
    async def test_delete_webhook_true(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 1")
        assert await func_repo.delete_webhook(conn, "notify") is True

    @pytest.mark.asyncio
    async def test_delete_webhook_false(self):
        conn = _make_conn()
        conn.execute = AsyncMock(return_value="DELETE 0")
        assert await func_repo.delete_webhook(conn, "missing") is False


# ---------------------------------------------------------------------------
# function_repo — round-trip helpers (function_from_dict, webhook_from_dict)
# ---------------------------------------------------------------------------


class TestFunctionFromDict:
    def test_reconstructs_function(self):
        d = {
            "name": "get_order",
            "source_id": "pg1",
            "schema_name": "public",
            "function_name": "get_order_fn",
            "returns": "pg1.public.orders",
            "arguments": [{"name": "order_id", "type": "Int"}],
            "visible_to": ["admin"],
            "writable_by": ["admin"],
            "domain_id": "sales",
            "description": "Fetch order by id",
        }
        fn = func_repo.function_from_dict(d)
        assert isinstance(fn, Function)
        assert fn.name == "get_order"
        assert len(fn.arguments) == 1
        assert fn.arguments[0].name == "order_id"

    def test_defaults_for_optional_fields(self):
        d = {
            "name": "simple_fn",
            "source_id": "pg1",
            "schema_name": "public",
            "function_name": "simple_fn",
            "returns": "pg1.public.t",
        }
        fn = func_repo.function_from_dict(d)
        assert fn.arguments == []
        assert fn.visible_to == []
        assert fn.writable_by == []


class TestWebhookFromDict:
    def test_reconstructs_webhook(self):
        d = {
            "name": "notify",
            "url": "https://hook.example.com/notify",
            "method": "POST",
            "timeout_ms": 3000,
            "returns": None,
            "inline_return_type": [{"name": "ticket_id", "type": "String"}],
            "arguments": [{"name": "msg", "type": "String"}],
            "visible_to": ["admin"],
            "domain_id": "ops",
            "description": "Send notification",
        }
        wh = func_repo.webhook_from_dict(d)
        assert isinstance(wh, Webhook)
        assert wh.name == "notify"
        assert wh.timeout_ms == 3000
        assert wh.inline_return_type[0].name == "ticket_id"

    def test_defaults_for_optional_fields(self):
        d = {"name": "simple_hook", "url": "https://hook.example.com"}
        wh = func_repo.webhook_from_dict(d)
        assert wh.method == "POST"
        assert wh.timeout_ms == 5000
        assert wh.arguments == []
