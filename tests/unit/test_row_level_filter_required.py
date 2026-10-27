# Copyright (c) 2026 Kenneth Stott
# Canary: 3d6f8a14-5c2e-4b97-a1d3-8e7b0c4f6a25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A read of a row-level replica table must filter it (REQ-1915, amended 2026-10-01).

A table replicated ROW BY ROW (``row_materialize``, on an engine that cannot attach its source) is
replicated that way because it is large, so it is never read whole. A statement that read it with
no filter used to behave three ways: the chokepoint surfaces copied the whole source table into
the worker (a 20M-document collection never finished; Neo4j ran out of memory building the node
set), pgwire and Flight answered from whatever rows happened to be replicated, and GraphQL read an
API cache table that was never created. A read the engine federates is refused at planning, on
every surface, naming the table; a single-source read through the source's own driver (DIRECT)
reads the source live and is not refused (maintainer ruling 2026-10-05). FILTERED means one of:

- a KEY filter: the statement's predicates resolve concrete values for the table's primary key —
  an equality, an IN list, or an OR of key equalities, with literals or bound parameters;
- a key the engine supplies: the table is the target of a JOIN whose ON condition is a single
  column-to-column equality between one of its columns and a column of another relation;
- any other filter: a predicate on the table's own columns, with literals or bound parameters
  (= <> < <= > >= IN BETWEEN LIKE, IS [NOT] NULL, AND/OR/NOT of those), whose rows are fetched
  from the source (``FilterBound``).

The interim rule (a read had to bind the KEY, ``data.row_level_key_required``) is replaced: the
cases it refused for a non-key predicate are now filtered reads, kept below as such."""

# Requirements: REQ-1915, REQ-1865

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from provisa.api.errors import ApiError, RowLevelFilterRequired
from provisa.compiler.pk_bounds import FilterBound
from provisa.audit.context import audit_identity_scope
from provisa.cache.store import NoopCacheStore
from provisa.compiler.directives import NO_CACHE_HINT, cache_hint_for
from provisa.pgwire import _pipeline, governed_plan
from tests.unit.test_buffered_auto_threshold import _decision
from tests.unit.test_governed_sql_engine_internals import _state as gov_state

pytestmark = pytest.mark.asyncio


def _table(name: str, key: str = "id", table_id: int = 1, alias=None) -> dict:
    return {
        "id": table_id,
        "source_id": "neo",
        "schema_name": "sales",
        "table_name": name,
        "alias": alias,
        "domain_id": "sales",
        "row_materialize": True,
        "columns": [
            {
                "column_name": key,
                "data_type": "integer",
                "is_primary_key": True,
                "native_filter_type": None,
                "embedding": False,
                "visible_to": ["analyst"],
            },
            {
                "column_name": "customer_id",
                "data_type": "integer",
                "is_primary_key": False,
                "native_filter_type": None,
                "embedding": False,
                "visible_to": ["analyst"],
            },
        ],
    }


@pytest.fixture
def planning(monkeypatch):
    """The pipeline's planning stages over a state whose ``sales.orders`` is replicated row by
    row from a source the bound engine has no connector for."""
    import provisa.api.app as app_mod

    monkeypatch.setattr(governed_plan, "_rebuild_in_progress", lambda: False)
    monkeypatch.setattr("provisa.audit.pipeline.write_audit", AsyncMock(return_value=None))
    state = gov_state(["*"])
    state.tables = [_table("orders")]
    state.source_types = {"pg": "postgresql", "neo": "neo4j"}
    state.source_dialects = {"pg": "postgres"}
    state.source_catalogs = {"pg": "pg", "neo": "neo"}
    state.source_pools = SimpleNamespace(source_ids=["pg"], has=lambda sid: sid == "pg")
    state.response_cache_store = NoopCacheStore()
    state.settings_overrides = {}
    state.federation_engine = SimpleNamespace(
        dialect="duckdb",
        connectors={},  # no connector for neo4j: the engine cannot attach it
        engine=SimpleNamespace(name="duckdb", catalog_qualified=True, connectors={}),
        transpile_physical=lambda sql: sql,
    )
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    with (
        patch.object(
            _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_decision("ENGINE"))
        ),
        audit_identity_scope("u-1", "test"),
    ):
        yield state


def _assert_names_table_and_key(error: RowLevelFilterRequired) -> None:
    assert isinstance(error, ApiError) and error.status_code == 400
    assert error.code == "data.row_level_filter_required"
    assert error.params == {"table": "orders"}
    assert "orders" in str(error) and "because it is large" in str(error)


# -- the two stages, as the surfaces reach them ---------------------------------------------------


async def test_pgwire_refuses_an_unfiltered_read_of_a_row_level_table(planning):
    """D3 symptom 2: pgwire answered `42P01 relation … does not exist` (or a partial replica)."""
    with pytest.raises(RowLevelFilterRequired) as refused:
        await _pipeline.govern_pgwire_plan(
            "SELECT o.id FROM sales.orders o LIMIT 1", "analyst", deliver=None
        )
    _assert_names_table_and_key(refused.value)


async def test_bolt_refuses_an_unfiltered_read_instead_of_replicating_the_node_set(planning):
    """D3 symptom 3: Bolt's engine path (the compiled stage) copied every node of the label."""
    with pytest.raises(RowLevelFilterRequired) as refused:
        await _pipeline._govern_and_route_compiled(
            "SELECT o.id FROM sales.orders o LIMIT 1",
            "analyst",
            state=planning,
            buffered=True,
            cache_hint=NO_CACHE_HINT,
            sdl_joins=False,
        )
    _assert_names_table_and_key(refused.value)


async def test_a_cached_answer_does_not_stand_in_for_the_refusal(planning, monkeypatch):
    """An unbound statement skips the cache read on both stages: an answer some earlier build
    cached for it is never served, and its federated read is refused."""
    read = AsyncMock(return_value=(object(), ()))
    monkeypatch.setattr(_pipeline, "_cached_before_routing", read)
    sql = "-- @provisa cache=true\nSELECT o.id FROM sales.orders o LIMIT 1"
    with pytest.raises(RowLevelFilterRequired):
        await _pipeline._govern_and_route(sql, "analyst", serve_cached=True)
    with pytest.raises(RowLevelFilterRequired):
        await _pipeline._govern_and_route_compiled(
            "SELECT o.id FROM sales.orders o LIMIT 1",
            "analyst",
            state=planning,
            cache_hint=cache_hint_for("sql", sql),
            serve_cached=True,
            sdl_joins=True,
        )
    read.assert_not_awaited()


@pytest.mark.parametrize(
    ("sql", "params"),
    [
        ("SELECT o.id FROM sales.orders o WHERE o.id = 7", None),
        ("SELECT o.id FROM sales.orders o WHERE o.id = $1", [7]),
        ("SELECT o.id FROM sales.orders o WHERE o.id IN (1, 2, 3)", None),
        ("SELECT o.id FROM sales.orders o WHERE o.id = 1 OR o.id = 2", None),
    ],
    ids=["literal", "bound-parameter", "in-list", "or-of-key-equalities"],
)
async def test_a_read_that_binds_the_key_is_planned_on_both_stages(planning, sql, params):
    raw = await _pipeline._govern_and_route(sql, "analyst", params=params)
    compiled = await _pipeline._govern_and_route_compiled(
        sql,
        "analyst",
        exec_params=params,
        state=planning,
        cache_hint=NO_CACHE_HINT,
        sdl_joins=True,
    )
    assert raw.pk_bounds and compiled.pk_bounds


async def test_a_graphql_field_is_refused_before_its_cache_and_its_route(planning, monkeypatch):
    """/data/graphql reads each field through the compiled pipeline: the same decision point. The
    cache is never read for it; the route decides the refusal (a federated read is refused)."""
    from provisa.api.data import endpoint
    from provisa.compiler.directives import CacheHint
    from provisa.compiler.sql_types import ColumnRef, CompiledQuery

    planning.mv_registry = SimpleNamespace(get_fresh=lambda: [])
    cache_read = AsyncMock(return_value=(None, ()))
    monkeypatch.setattr(_pipeline, "_cached_before_routing", cache_read)
    routed = _pipeline._optimize_and_route  # the fixture's stand-in, restored by the fixture
    compiled = CompiledQuery(
        sql='SELECT "t0"."id" FROM "sales"."orders" AS "t0" LIMIT 100',
        params=[],
        root_field="orders",
        columns=[ColumnRef("t0", "id", "id", None)],
        sources={"neo"},
    )
    with pytest.raises(RowLevelFilterRequired) as refused:
        await endpoint._execute_one_field(
            compiled,
            planning.contexts["analyst"],
            planning,
            "analyst",
            "json",
            delivery=None,
            steward_hint=None,
            query_session_props=None,
            cache_hint=CacheHint(opt_in=True, ttl=None, debug_trace=False),
        )
    _assert_names_table_and_key(refused.value)
    cache_read.assert_not_awaited()
    routed.assert_awaited()  # the refusal is the ENGINE route's (maintainer ruling 2026-10-05)


async def test_a_direct_unfiltered_read_is_planned_live_with_no_bound(planning, monkeypatch):
    """Maintainer ruling 2026-10-05: a single-source read through the source's own driver reads
    the source live -- no bound, so nothing is fetched into the copy, and it is not refused. Nor is
    it answered from the cache: the statement binds the row-level table neither way."""
    cache_read = AsyncMock(return_value=(object(), ()))
    monkeypatch.setattr(_pipeline, "_cached_before_routing", cache_read)
    sql = "-- @provisa cache=true\nSELECT o.id FROM sales.orders o LIMIT 1"
    with patch.object(
        _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_decision("DIRECT"))
    ):
        raw = await _pipeline._govern_and_route(sql, "analyst", serve_cached=True)
        compiled = await _pipeline._govern_and_route_compiled(
            "SELECT o.id FROM sales.orders o LIMIT 1",
            "analyst",
            state=planning,
            cache_hint=cache_hint_for("sql", sql),
            serve_cached=True,
            sdl_joins=True,
        )
    assert raw.route.name == "DIRECT" and raw.pk_bounds == ()
    assert compiled.route.name == "DIRECT" and compiled.pk_bounds == ()
    cache_read.assert_not_awaited()


async def test_a_direct_filtered_read_fetches_nothing_into_the_copy(planning, monkeypatch):
    with patch.object(
        _pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_decision("DIRECT"))
    ):
        plan = await _pipeline._govern_and_route(
            "SELECT o.id FROM sales.orders o WHERE o.customer_id = 5", "analyst"
        )
    assert plan.pk_bounds == ()


async def test_the_flag_is_ignored_when_the_engine_attaches_the_source(planning):
    """Settled: row_materialize is the reach only for an engine that cannot attach the source."""
    reads_in_place = SimpleNamespace(reads_in_place=True)
    planning.federation_engine.connectors = {"neo4j": reads_in_place}
    planning.federation_engine.engine.connectors = {"neo4j": reads_in_place}
    plan = await _pipeline._govern_and_route("SELECT o.id FROM sales.orders o LIMIT 1", "analyst")
    assert plan.pk_bounds == ()


# -- what counts as bound, at the decision point --------------------------------------------------


def _decide(monkeypatch, sql: str, params=None, tables=None):
    by_name = {}
    for t in tables or [_table("orders")]:
        ns = SimpleNamespace(
            id=t["id"],
            source_id=t["source_id"],
            schema_name=t["schema_name"],
            table_name=t["table_name"],
            alias=t["alias"],
            row_materialize=True,
            columns=[
                SimpleNamespace(
                    name=c["column_name"],
                    data_type=c["data_type"],
                    is_primary_key=c["is_primary_key"],
                    native_filter_type=None,
                )
                for c in t["columns"]
            ],
        )
        by_name[t["table_name"]] = ns
    monkeypatch.setattr(_pipeline, "_row_materialize_tables_in_memory", lambda state: by_name)
    state = SimpleNamespace(tables=[], federation_engine=object())
    return _pipeline._pk_bounds(_pipeline._pk_bounds_inputs(sql, state), params)


@pytest.mark.parametrize(
    "sql",
    [
        # the row-level table is the JOIN target, on one column equal to a column of the other side
        "SELECT c.name FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id = 5",
        "SELECT c.name FROM customers c LEFT JOIN orders o ON c.id = o.customer_id",
    ],
    ids=["join-on-a-column", "left-join-reversed"],
)
def test_a_join_that_pushes_the_key_down_is_bound(monkeypatch, sql):
    assert _decide(monkeypatch, sql) == ()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT o.id FROM orders o",
        "SELECT count(*) FROM orders",
        # the row-level table is in FROM and only the OTHER relation is joined to it
        "SELECT o.id FROM orders o JOIN customers c ON c.id = o.customer_id",
        "SELECT c.name FROM customers c JOIN orders o ON o.customer_id = c.id AND o.id > c.id",
        "SELECT * FROM (SELECT o.id FROM orders o) s",
    ],
    ids=["no-filter", "count", "in-from-position", "composite-on", "inside-a-subquery"],
)
def test_an_unfiltered_read_is_refused(monkeypatch, sql):
    with pytest.raises(RowLevelFilterRequired) as refused:
        _decide(monkeypatch, sql)
    assert refused.value.params == {"table": "orders"}


@pytest.mark.parametrize(
    ("sql", "predicate"),
    [
        ("SELECT o.id FROM orders o WHERE o.customer_id = 5", '"customer_id" = 5'),
        (
            "SELECT o.id FROM orders o WHERE o.id = 1 OR o.customer_id = 5",
            '"id" = 1 OR "customer_id" = 5',
        ),
        ("SELECT o.id FROM orders o WHERE o.id > 10", '"id" > 10'),
        ("SELECT c.name FROM customers c JOIN orders o ON o.customer_id = 5", '"customer_id" = 5'),
    ],
    ids=["filter-on-another-column", "or-with-a-non-key", "key-range", "on-a-literal"],
)
def test_a_non_key_filter_is_a_filtered_read(monkeypatch, sql, predicate):
    """Refused under the interim key rule; REQ-1915 (amended 2026-10-01) reads them through the
    filter, fetched from the source."""
    (bound,) = _decide(monkeypatch, sql)
    assert isinstance(bound, FilterBound)
    assert (bound.table_name, bound.predicate, bound.pk_columns) == ("orders", predicate, ("id",))


def test_a_filter_s_bound_parameters_are_inlined_in_its_canonical_form(monkeypatch):
    (bound,) = _decide(
        monkeypatch, "SELECT o.id FROM orders o WHERE o.customer_id BETWEEN $1 AND $2", [3, 9]
    )
    assert bound.predicate == '"customer_id" BETWEEN 3 AND 9'


def test_a_key_filter_stays_a_key_read(monkeypatch):
    """The cheapest case keeps its per-row path even with another predicate beside it."""
    (bound,) = _decide(
        monkeypatch, "SELECT o.id FROM orders o WHERE o.id = 4 AND o.customer_id > 1"
    )
    assert not isinstance(bound, FilterBound) and bound.values == ((4,),)


def test_every_reference_must_be_bound_in_its_own_select(monkeypatch):
    """A key bound on one reference does not cover another reference to the same table."""
    one_branch = "SELECT o.id FROM orders o WHERE o.id = 1 UNION ALL SELECT o2.id FROM orders o2"
    with pytest.raises(RowLevelFilterRequired):
        _decide(monkeypatch, one_branch)
    both = _decide(monkeypatch, one_branch + " WHERE o2.id IN (1, 2)")
    assert [b.values for b in both] == [((1,), (2,))]
    # a key predicate in an enclosing query does not bind the read inside it
    with pytest.raises(RowLevelFilterRequired):
        _decide(monkeypatch, "SELECT s.id FROM (SELECT o.id FROM orders o) s WHERE s.id = 1")
    # ... the read's own SELECT does, wherever that SELECT sits
    inner = _decide(
        monkeypatch,
        "WITH w AS (SELECT o.* FROM orders o WHERE o.id = $1) SELECT w.id FROM w WHERE FALSE",
        [9],
    )
    assert [b.values for b in inner] == [((9,),)]


def test_two_joins_to_one_table_are_not_both_pushed_down(monkeypatch):
    """Key pushdown probes one JOIN per table: with two, each reference binds its key itself."""
    with pytest.raises(RowLevelFilterRequired):
        _decide(
            monkeypatch,
            "SELECT c.name FROM customers c JOIN orders a ON a.customer_id = c.id "
            "JOIN orders b ON b.customer_id = c.id",
        )


def test_a_union_has_no_key_pushdown(monkeypatch):
    with pytest.raises(RowLevelFilterRequired):
        _decide(
            monkeypatch,
            "SELECT c.name FROM customers c JOIN orders o ON o.customer_id = c.id "
            "UNION ALL SELECT c.name FROM customers c",
        )


def test_a_key_on_the_join_condition_binds_the_reference(monkeypatch):
    found = _decide(monkeypatch, "SELECT c.name FROM customers c LEFT JOIN orders o ON o.id = 5")
    assert [b.values for b in found] == [((5,),)]


def test_a_write_to_a_row_level_table_is_not_a_read_of_it(monkeypatch):
    """The written table is changed at the source, never read from the replica."""
    assert _decide(monkeypatch, "INSERT INTO orders (id, customer_id) VALUES (1, 2)") == ()
    assert _decide(monkeypatch, "UPDATE orders SET customer_id = 3 WHERE customer_id = 2") == ()
    assert _decide(monkeypatch, "DELETE FROM orders WHERE customer_id = 2") == ()
    with pytest.raises(RowLevelFilterRequired):  # ... but what an INSERT selects from is read
        _decide(monkeypatch, "INSERT INTO archive (id) SELECT o.id FROM orders o")


def test_every_row_level_table_of_the_statement_must_be_bound(monkeypatch):
    tables = [_table("orders"), _table("customers", table_id=2)]
    # customers bound by key, orders bound by the join: planned
    assert _decide(
        monkeypatch,
        "SELECT o.id FROM customers c JOIN orders o ON o.customer_id = c.id WHERE c.id = $1",
        [5],
        tables,
    )
    # orders joined, customers unbound: refused for customers
    with pytest.raises(RowLevelFilterRequired) as refused:
        _decide(
            monkeypatch,
            "SELECT o.id FROM customers c JOIN orders o ON o.customer_id = c.id",
            None,
            tables,
        )
    assert refused.value.params["table"] == "customers"
