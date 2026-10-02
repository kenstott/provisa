# Copyright (c) 2026 Kenneth Stott
# Canary: 9669c1e4-dcc3-45ff-8bd2-4506fb9969bf
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Engine reads outside the query pipeline address a replica-served table at its replica
(REQ-1912).

A table served from its replica has nothing at its registered name on the engine: a floored
source has no live attach at all. The query pipeline renames such a table at the transpile seam;
these are the engine reads that build their own statement — the discovery sample and column
listing, the materialized-view preflight inputs, the statistics refresh — and each must reach the
table through the same published routes, by the rewrite (``address_replicas``) or, for a statement
that is not a query, by the one-table lookup (``read_address``).
"""

# Requirements: REQ-1912, REQ-826

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pytest

from provisa.executor.result import QueryResult
from provisa.federation.engine import build_engine
from provisa.federation.replica_address import (
    AmbiguousReplica,
    ReplicaRoute,
    ReplicaRoutes,
    read_address,
)
from provisa.federation.replica_guard import ReplicaUnavailable
from provisa.federation.runtime import EngineRuntime

pytestmark = pytest.mark.unit

_LIVE = ("src", "public", "customers")
_REGISTERED = ("src", "public", "orders")
_REPLICA = ("provisa_admin", "org_acme_replicas", "src__public__orders")


def _routes(engine_name: str, **extra: Any) -> ReplicaRoutes:
    return ReplicaRoutes(
        engine_name=engine_name,
        routes={_REGISTERED: ReplicaRoute("src", "orders", _REPLICA)},
        **extra,
    )


class _Runtime(EngineRuntime):
    """The real runtime over the published routes; the engine terminals record what they are
    given instead of dialing an engine."""

    def __init__(self, routes_extra: dict | None = None) -> None:
        engine = build_engine("trino")
        state = SimpleNamespace(replica_routes=_routes(engine.name, **(routes_extra or {})))
        super().__init__(engine, state)
        self.statements: list[str] = []
        self.analyzed: list[tuple[str | None, str, str]] = []
        self.rows: list[tuple] = []

    async def execute_engine(self, sql: str, *args: Any, **kwargs: Any) -> QueryResult:
        del args, kwargs
        self.statements.append(sql)
        return QueryResult(rows=list(self.rows), column_names=[])

    def execute_engine_stream(self, sql: str, *args: Any, **kwargs: Any) -> tuple[Any, Any]:
        del args, kwargs
        self.statements.append(sql)
        return object(), iter(pa.Table.from_pylist([{"id": 1}]).to_batches())

    async def analyze_landed_table(self, *, catalog: str | None, schema: str, table: str) -> None:
        self.analyzed.append((catalog, schema, table))


# -- the one-table lookup ---------------------------------------------------------------------------


def test_read_address_is_the_replica_for_a_served_table_and_the_name_itself_for_a_live_one():
    routes = _routes("trino")
    assert read_address(_REGISTERED, routes) == _REPLICA
    assert read_address(_LIVE, routes) == _LIVE


def test_read_address_refuses_what_the_rewrite_refuses():
    cause = RuntimeError("store unreachable")
    unreconciled = _routes("trino", unreconciled={("src", "orders"): cause})
    with pytest.raises(ReplicaUnavailable):
        read_address(_REGISTERED, unreconciled)
    shared = ReplicaRoutes(engine_name="trino", ambiguous={_REGISTERED: ("a", "b")})
    with pytest.raises(AmbiguousReplica):
        read_address(_REGISTERED, shared)


def test_the_runtime_answers_the_lookup_from_the_published_routes():
    rt = _Runtime()
    assert rt.read_address(*_REGISTERED) == _REPLICA
    assert rt.read_address(*_LIVE) == _LIVE
    # A runtime with no registry published has no table served from a replica.
    bare = EngineRuntime(build_engine("trino"), SimpleNamespace())
    assert bare.read_address(*_REGISTERED) == _REGISTERED


# -- discovery: sample and column listing -----------------------------------------------------------


async def test_discovery_lists_the_columns_of_a_replica_served_table_at_its_replica():
    from provisa.discovery.collector import _fetch_column_types

    rt = _Runtime()
    rt.rows = [("id", "BIGINT")]
    assert await _fetch_column_types(rt, *_REGISTERED) == [{"name": "id", "type": "bigint"}]
    (sql,) = rt.statements
    assert "FROM provisa_admin.information_schema.columns" in sql
    assert "table_schema = 'org_acme_replicas' AND table_name = 'src__public__orders'" in sql
    assert "FROM src." not in sql  # the source has no live attach to list


async def test_discovery_lists_the_columns_of_a_live_table_through_its_own_catalog():
    from provisa.discovery.collector import _fetch_column_types

    rt = _Runtime()
    await _fetch_column_types(rt, *_LIVE)
    (sql,) = rt.statements
    assert "FROM src.information_schema.columns" in sql
    assert "table_schema = 'public' AND table_name = 'customers'" in sql


async def test_discovery_samples_a_replica_served_table_at_its_replica():
    from provisa.discovery.collector import _fetch_samples

    rt = _Runtime()
    rt.rows = [(7,)]
    samples = await _fetch_samples(rt, *_REGISTERED, [{"name": "id", "type": "bigint"}], 5)
    assert samples == [{"id": "7"}]
    (sql,) = rt.statements
    assert '"provisa_admin"."org_acme_replicas"."src__public__orders"' in sql
    assert "src.public.orders" not in sql.replace('"', "")


# -- materialized-view preflight inputs -------------------------------------------------------------


async def test_a_preflight_stream_reads_the_replica_and_keeps_the_inputs_own_name():
    from provisa.mv.preflight_eval import _open_input_streams

    rt = _Runtime()
    streams = await _open_input_streams(rt, ["src.public.orders", "src.public.customers"])
    # The check names its inputs as the view's definition does.
    assert sorted(streams) == ["src.public.customers", "src.public.orders"]
    served, live = rt.statements
    assert '"provisa_admin"."org_acme_replicas"."src__public__orders"' in served
    assert live == 'SELECT * FROM "src"."public"."customers"'


async def test_a_preflight_count_probe_reads_the_replica():
    from provisa.mv.preflight_eval import evaluate_streams

    rt = _Runtime()
    rt.rows = [(0,)]
    check = (
        "def preflight(streams, ctx):\n"
        "    if any(r['id'] < 0 for r in streams['src.public.orders']):\n"
        "        return ctx.abort('negative id')\n"
        "    return ctx.ok()"
    )
    verdict = await evaluate_streams(rt, check, ["src.public.orders"], SimpleNamespace())
    assert verdict is not None and verdict.is_continue
    (sql,) = rt.statements
    assert "count(*)" in sql.lower() and "_preflight" in sql  # pushed down, not streamed
    assert '"provisa_admin"."org_acme_replicas"."src__public__orders"' in sql
    assert '"src"."public"."orders"' not in sql


async def test_a_refresh_names_its_preflight_inputs_as_the_view_does():
    """The refresh hands the preflight the view's SELECT with its served inputs already renamed
    to their replicas; the inputs the check is keyed by come from the view's own definition."""
    from provisa.mv.models import MVDefinition
    from provisa.mv.refresh import _build_refresh_sql, _evaluate_preflight

    rt = _Runtime()
    check = (  # not expressible as one count probe: it is evaluated over the input's stream
        "def preflight(streams, ctx):\n"
        "    if len(list(streams['src.public.orders'])) > 2:\n"
        "        return ctx.quarantine('too many')\n"
        "    return ctx.ok()"
    )
    mv = MVDefinition(
        id="mv-orders",
        source_tables=[],
        target_catalog="provisa_admin",
        target_schema="org_acme_mv_cache",
        sql='SELECT "o"."id" FROM "src"."public"."orders" AS "o"',
        refresh_interval=300,
        preprocess=check,
    )
    select_sql = await _build_refresh_sql(mv, rt)
    assert "org_acme_replicas" in select_sql

    verdict = await _evaluate_preflight(rt, mv, select_sql)

    assert verdict is not None and verdict.is_continue  # the check found its input by name
    (scan,) = rt.statements
    assert '"provisa_admin"."org_acme_replicas"."src__public__orders"' in scan


# -- statistics refresh -----------------------------------------------------------------------------


class _Rows:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def fetchall(self) -> list:
        return self._rows


class _Conn:
    async def execute_core(self, stmt: Any) -> _Rows:
        del stmt
        return _Rows(
            [
                SimpleNamespace(schema_name="public", table_name="orders"),
                SimpleNamespace(schema_name="public", table_name="customers"),
            ]
        )


class _Acquire:
    async def __aenter__(self) -> _Conn:
        return _Conn()

    async def __aexit__(self, *exc: Any) -> bool:
        del exc
        return False


async def test_refreshing_statistics_analyzes_a_replica_served_table_at_its_replica(monkeypatch):
    from provisa.api.admin import schema_mutation
    from provisa.api.admin.schema_mutation import Mutation
    from tests.unit.gate_identity import grant

    rt = _Runtime()
    state = SimpleNamespace(federation_engine=rt, catalog_for=lambda sid: sid)
    info, _ = grant(monkeypatch, "source_registration", state=state)
    monkeypatch.setattr("provisa.api.app.state", state)

    async def _pool() -> Any:
        return SimpleNamespace(acquire=lambda: _Acquire())

    monkeypatch.setattr(schema_mutation, "_get_pool", _pool)

    result = await Mutation().refresh_source_statistics(info, source_id="src")

    assert result.success, result.message
    # The served table is analyzed where it lives, in the store; no statement names its source.
    assert rt.analyzed == [_REPLICA]
    assert rt.statements == ["ANALYZE src.public.customers"]


# -- a registered name resolved to its engine name --------------------------------------------------


def _registry(monkeypatch, *tables: tuple[str, str, str]) -> SimpleNamespace:
    from provisa.federation import registry_view

    async def _registered(_state):
        return [
            SimpleNamespace(source_id=source_id, schema_name=schema, table_name=name)
            for source_id, schema, name in tables
        ]

    monkeypatch.setattr(registry_view, "registered_tables", _registered)
    return SimpleNamespace(catalog_for=lambda source_id: source_id.replace("-", "_"))


async def test_a_registered_name_resolves_to_its_catalog_physical_name(monkeypatch):
    from provisa.federation.replica_routing import registered_table_key

    state = _registry(monkeypatch, ("sales-pg", "public", "orders"), ("crm", "main", "customers"))
    assert await registered_table_key(build_engine("trino"), state, "orders") == (
        "sales_pg",
        "public",
        "orders",
    )
    # An engine whose SQL has no catalog names the table with the catalog folded into the schema.
    assert await registered_table_key(build_engine("pg"), state, "orders") == (
        None,
        "sales_pg_public",
        "orders",
    )


async def test_a_name_no_table_registers_or_two_sources_register_is_refused(monkeypatch):
    from provisa.federation.replica_routing import (
        AmbiguousRegisteredTable,
        UnknownRegisteredTable,
        registered_table_key,
    )

    state = _registry(monkeypatch, ("sales-pg", "public", "orders"), ("erp", "dbo", "orders"))
    engine = build_engine("trino")
    with pytest.raises(UnknownRegisteredTable):
        await registered_table_key(engine, state, "customers")
    with pytest.raises(AmbiguousRegisteredTable) as refused:
        await registered_table_key(engine, state, "orders")
    assert "erp" in str(refused.value) and "sales-pg" in str(refused.value)


async def test_a_join_pattern_reads_a_replica_served_table_at_its_replica(monkeypatch):
    """The reason the names are resolved: once a join-pattern table has an engine name, the
    address seam can serve it from its replica."""
    from provisa.mv.models import JoinPattern, MVDefinition
    from provisa.mv.refresh import _build_refresh_sql

    rt = _Runtime()
    state = _registry(monkeypatch, ("src", "public", "orders"), ("src", "public", "customers"))
    rt._state.catalog_for = state.catalog_for
    mv = MVDefinition(
        id="mv-orders-customers",
        source_tables=["orders", "customers"],
        target_catalog="provisa_admin",
        target_schema="org_acme_mv_cache",
        join_pattern=JoinPattern(
            left_table="orders",
            left_column="customer_id",
            right_table="customers",
            right_column="id",
        ),
    )

    sql = await _build_refresh_sql(mv, rt)

    assert 'FROM "provisa_admin"."org_acme_replicas"."src__public__orders" AS "orders" ' in sql
    assert 'JOIN "src"."public"."customers" AS "customers" ON ' in sql
