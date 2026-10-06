# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7d3e95-4a1c-4f28-8e6d-2c9f5a7b1e43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How a profile run samples a table too large to read whole (REQ-1934).

The method comes from the reach of the table -- the route the governed pipeline chose and the
declared traits of what executes there -- and the sampled statement is checked after transpile."""

# Requirements: REQ-1934

from __future__ import annotations

import random
from types import SimpleNamespace

import duckdb
import pytest
import sqlglot

from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext, TableMeta
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.federation.engine import (
    build_duckdb_engine,
    build_pg_engine,
    build_trino_engine,
)
from provisa.federation.replica_address import ReplicaRoutes
from provisa.profiler.sampling import (
    SampleClauseLost,
    choose_method,
    require_sample_clause,
    sample_reach,
)
from provisa.profiler.schema import field_names
from provisa.profiler.statement import (
    ColumnSpec,
    Sample,
    is_integer_key,
    key_bounds_sql,
    key_ranges,
    parse_profile_result,
    profile_sql,
)
from provisa.transpiler.router import Route
from provisa.transpiler.transpile import transpile
from tests.helpers import unscoped_role

_META = TableMeta(
    table_id=7,
    field_name="orders",
    type_name="Orders",
    source_id="src",
    catalog_name="src",
    schema_name="public",
    table_name="orders",
    domain_id="sales",
)
_COLUMNS = [
    ColumnSpec("id", "bigint", "numeric", "id"),
    ColumnSpec("region", "varchar", "text", "region"),
]


def _state(stype: str, engine=None, *, floored: dict | None = None, serving=frozenset()):
    return SimpleNamespace(
        source_types={"src": stype},
        federation_engine=SimpleNamespace(engine=engine),
        replica_routes=ReplicaRoutes(
            engine_name=getattr(engine, "name", ""), floored=floored or {}, serving=serving
        ),
    )


# -- method selection per reach ------------------------------------------------------------------


@pytest.mark.parametrize(
    "stype,block,key_range",
    [
        ("postgresql", True, True),
        ("duckdb", True, False),
        ("mysql", False, True),  # sqlglot drops TABLESAMPLE for MySQL: never block
        ("cockroachdb", False, True),  # shares the postgresql driver, not its TABLESAMPLE
        ("snowflake", False, False),  # unmeasured
    ],
)
def test_a_direct_route_samples_by_the_source_types_own_sql(stype, block, key_range):
    reach = sample_reach(_state(stype), Route.DIRECT, _META)
    assert (reach.block, reach.key_range) == (block, key_range)
    assert reach.where == f"direct {stype}"


@pytest.mark.parametrize(
    "build,stype,block,key_range",
    [
        # DuckDB exposes an attached table through a view: the sample runs after a full scan; and
        # its scanner repeats a key range once per 1000-page ctid task.
        (build_duckdb_engine, "postgresql", False, False),
        # Trino reads a JDBC table as one split: SYSTEM keeps all of it or none.
        (build_trino_engine, "postgresql", False, True),
        # postgres_fdw refuses TABLESAMPLE on a foreign table.
        (build_pg_engine, "postgresql", False, True),
        (build_duckdb_engine, "parquet", False, False),
        # Trino drops whole iceberg splits before reading them.
        (build_trino_engine, "iceberg", True, False),
        # Same split sampling in Trino, but unmeasured on hive/delta: the row filter.
        (build_trino_engine, "delta_lake", False, False),
        (build_trino_engine, "hive", False, False),
    ],
)
def test_an_engine_route_samples_by_its_connector_for_the_source(build, stype, block, key_range):
    engine = build()
    reach = sample_reach(_state(stype, engine), Route.ENGINE, _META)
    assert (reach.block, reach.key_range) == (block, key_range)


def test_a_table_read_from_its_replica_is_sampled_by_the_row_filter():
    engine = build_trino_engine()
    floored = sample_reach(
        _state("postgresql", engine, floored={7: ("src", "replicate")}), Route.ENGINE, _META
    )
    serving = sample_reach(
        _state("postgresql", engine, serving=frozenset({("src", "public", "orders")})),
        Route.ENGINE,
        _META,
    )
    for reach in (floored, serving):
        assert (reach.block, reach.key_range) == (False, False)
        assert reach.where == "replica on trino"
        assert choose_method(reach, has_key=True) == "random"


def test_block_comes_first_then_an_indexed_key_then_the_row_filter():
    pg = sample_reach(_state("postgresql"), Route.DIRECT, _META)
    mysql = sample_reach(_state("mysql"), Route.DIRECT, _META)
    assert choose_method(pg, has_key=True) == "block"
    assert choose_method(mysql, has_key=True) == "key_range"
    assert choose_method(mysql, has_key=False) == "random"


@pytest.mark.parametrize(
    "data_type,keyed",
    [("bigint", True), ("integer", True), ("int4", True), ("varchar", False), ("uuid", False)],
)
def test_only_an_integer_key_carries_key_ranges(data_type, keyed):
    assert is_integer_key(ColumnSpec("k", data_type, "numeric", "k")) is keyed


# -- key ranges ----------------------------------------------------------------------------------


def test_key_ranges_cover_the_fraction_spread_over_the_key_space():
    ranges = key_ranges(1, 1_000_000, 0.01, random.Random(3))
    assert len(ranges) == 16
    widths = [b - a + 1 for a, b in ranges]
    assert sum(widths) == pytest.approx(10_000, rel=0.01)
    # One range per stratum: ordered, disjoint, inside the bounds, one in each sixteenth.
    for i, (a, b) in enumerate(ranges):
        assert 1 + i * 62_500 <= a <= b <= (i + 1) * 62_500
    assert all(ranges[i][1] < ranges[i + 1][0] for i in range(len(ranges) - 1))


def test_a_small_key_space_gets_fewer_ranges_never_an_empty_one():
    ranges = key_ranges(10, 19, 0.3, random.Random(1))
    assert len(ranges) == 3
    assert all(10 <= a <= b <= 19 for a, b in ranges)


def test_key_ranges_refuse_empty_bounds():
    with pytest.raises(ValueError, match="empty"):
        key_ranges(5, 4, 0.1, random.Random(0))


def test_the_key_range_statement_reads_only_the_rows_in_its_ranges():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    con.execute(
        "CREATE TABLE d.orders AS SELECT range + 1 AS id, "
        "CASE WHEN range % 2 = 0 THEN 'east' ELSE 'west' END AS region FROM range(100000)"
    )
    bounds = con.execute(
        sqlglot.transpile(key_bounds_sql("d.orders", "id"), read="postgres", write="duckdb")[0]
    ).fetchone()
    assert bounds == (1, 100000)
    ranges = key_ranges(bounds[0], bounds[1], 0.05, random.Random(7))
    sample = Sample("key_range", 0.05, "id", ranges)
    sql = profile_sql("d.orders", _COLUMNS, [], sample, 100)
    for a, b in ranges:
        assert f'WHERE t."id" BETWEEN {a} AND {b}' in sql
    assert sql.count(" UNION ALL ") == len(ranges) - 1
    res = con.execute(sqlglot.transpile(sql, read="postgres", write="duckdb")[0])
    agg = parse_profile_result([d[0] for d in res.description], res.fetchall(), _COLUMNS, [])
    assert agg.profiled_rows == sum(b - a + 1 for a, b in ranges)
    assert agg.profiled_rows == pytest.approx(5000, rel=0.01)


# -- block sample SQL after transpile ------------------------------------------------------------


def _block_sql() -> str:
    return profile_sql("d.orders", _COLUMNS, [], Sample("block", 0.025), 100)


def test_the_block_sample_is_a_percentage_capped_at_the_whole_table():
    assert Sample("block", 0.025).percent == 2.5
    assert Sample("block", 3.0).percent == 100.0
    assert '"d"."orders" t TABLESAMPLE SYSTEM (2.5)' in _block_sql()


@pytest.mark.parametrize(
    "dialect,clause",
    [
        ("postgres", "TABLESAMPLE SYSTEM (2.5)"),
        ("duckdb", "TABLESAMPLE SYSTEM (2.5 PERCENT)"),
        ("trino", "TABLESAMPLE SYSTEM (2.5)"),
        ("snowflake", "TABLESAMPLE SYSTEM (2.5)"),
        ("bigquery", "TABLESAMPLE SYSTEM (2.5 PERCENT)"),
        ("tsql", "TABLESAMPLE SYSTEM (2.5 PERCENT)"),
    ],
)
def test_the_block_sample_survives_transpile(dialect, clause):
    physical = transpile(_block_sql(), dialect)
    assert clause in physical
    require_sample_clause("block", physical, dialect, f"direct {dialect}", 0)


def test_a_transpile_that_drops_the_block_sample_fails_by_name():
    physical = transpile(_block_sql(), "mysql")
    assert "TABLESAMPLE" not in physical.upper()
    with pytest.raises(SampleClauseLost, match="block sample lost in the 'mysql' statement"):
        require_sample_clause("block", physical, "mysql", "direct mysql", 0)


def test_the_block_sample_runs_on_duckdb_after_transpile():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA d")
    con.execute(
        "CREATE TABLE d.orders AS SELECT range AS id, 'r' || (range % 5)::VARCHAR AS region "
        "FROM range(2000000)"
    )
    res = con.execute(transpile(_block_sql(), "duckdb"))
    agg = parse_profile_result([d[0] for d in res.description], res.fetchall(), _COLUMNS, [])
    # DuckDB samples whole vectors of 2048 rows: the realised share is near, not at, 2.5%.
    assert 0.0125 * 2_000_000 < agg.profiled_rows < 0.05 * 2_000_000


def test_a_key_range_statement_that_lost_ranges_fails_by_name():
    sql = "SELECT * FROM t WHERE t.id BETWEEN 1 AND 5"
    require_sample_clause("key_range", sql, "postgres", "direct postgresql", 1)
    with pytest.raises(SampleClauseLost, match="1 of 2 key ranges"):
        require_sample_clause("key_range", sql, "postgres", "direct postgresql", 2)


def test_governance_keeps_the_block_sample_and_adds_the_row_rule():
    ctx = CompilationContext()
    ctx.tables = {"orders": _META}
    table = {
        "id": 7,
        "source_id": "src",
        "schema_name": "public",
        "table_name": "orders",
        "domain_id": "sales",
        "columns": [
            {"column_name": "id", "data_type": "bigint", "visible_to": ["org_admin"]},
            {"column_name": "region", "data_type": "varchar", "visible_to": ["org_admin"]},
        ],
    }
    gov = build_governance_context(
        "org_admin",
        RLSContext(rules={7: "region <> 'north'"}),
        {},
        ctx,
        tables=[table],
        role=unscoped_role("org_admin"),
    )
    governed = apply_governance(
        'SELECT t."id" FROM orders t TABLESAMPLE SYSTEM (2.5) WHERE t."id" > 0', gov
    )
    assert "TABLESAMPLE SYSTEM (2.5)" in governed
    assert "north" in governed


def test_lowering_the_semantic_ref_keeps_the_block_sample():
    """normalize_table_refs rebuilt each table ref without its TABLESAMPLE, so a block-sampled
    statement routed DIRECT read the whole table (found by the run's sample-clause guard)."""
    from provisa.compiler.sql_rewrite import (
        normalize_table_refs,
        rewrite_semantic_to_catalog_physical,
        rewrite_semantic_to_physical,
    )

    ctx = CompilationContext()
    ctx.tables = {"orders": _META}
    sql = 'SELECT t."id" FROM "sales"."orders" t TABLESAMPLE SYSTEM (2.5)'
    assert "TABLESAMPLE SYSTEM (2.5)" in normalize_table_refs(sql, ctx)
    direct = rewrite_semantic_to_physical(sql, ctx)
    assert '"public"."orders" AS "t" TABLESAMPLE SYSTEM (2.5)' in direct
    engine = rewrite_semantic_to_catalog_physical(normalize_table_refs(sql, ctx), ctx)
    assert '"src"."public"."orders" AS "t" TABLESAMPLE SYSTEM (2.5)' in engine


# -- the runs record -----------------------------------------------------------------------------


def test_the_runs_record_names_the_method_the_target_and_every_attempt():
    names = field_names("runs")
    i = names.index("sampled")
    assert names[i : i + 6] == (
        "sampled",
        "sample_method",
        "target_fraction",
        "sample_fraction",
        "sample_attempts",
        "profiled_rows",
    )


def test_a_sample_names_a_known_method_and_only_a_key_range_names_ranges():
    with pytest.raises(ValueError, match="unknown sample method"):
        Sample("tablesample", 0.1)
    with pytest.raises(ValueError, match="fraction"):
        Sample("whole", 0.5)
    with pytest.raises(ValueError, match="key-range"):
        Sample("key_range", 0.1)
    with pytest.raises(ValueError, match="key-range"):
        Sample("block", 0.1, "id", ((1, 2),))


# -- the run's read: escalation, the route check, the empty sample --------------------------------


class _FakePipeline:
    """Stands in for the governed pipeline: routes every statement to ``route`` and answers a
    profile statement with the row count ``rows_for`` gives its SQL."""

    def __init__(self, monkeypatch, route, rows_for, *, block_route=None):
        import provisa.profiler.run as run_mod

        self.statements: list[str] = []
        self.route = route
        self.block_route = block_route or route

        async def _route(sql):
            self.statements.append(sql)
            r = self.block_route if "TABLESAMPLE" in sql else self.route
            return SimpleNamespace(route=r, sql=transpile(sql, "postgres"), dialect="postgres")

        async def _execute(plan):
            if plan.sql.startswith("SELECT MIN("):
                return ["lo", "hi"], [(1, 100_000)]
            return ["sql"], [(plan.sql,)]

        def _parse(names, rows, columns, fanouts):
            return SimpleNamespace(profiled_rows=rows_for(rows[0][0]))

        monkeypatch.setattr(run_mod, "_route", _route)
        monkeypatch.setattr(run_mod, "_execute", _execute)
        monkeypatch.setattr(run_mod, "parse_profile_result", _parse)


def _target(key="id"):
    from provisa.profiler.run import Target

    return Target(7, "orders", "sales.orders", _COLUMNS, [], {}, _META, key)


def _percent(sql: str) -> float:
    import re

    found = re.search(r"TABLESAMPLE SYSTEM \(([\d.]+)\)", sql)
    assert found is not None, sql
    return float(found.group(1))


async def _read(state, target, row_count=100_000, fraction=0.01):
    from provisa.profiler.run import read_profile

    return await read_profile(state, target, row_count, fraction, 100, random.Random(5))


async def test_a_block_sample_under_half_its_target_is_read_again_at_four_times(monkeypatch):
    # The source's blocks are coarse: under 4% it returns nothing, then 1000 rows per percent.
    _FakePipeline(
        monkeypatch,
        Route.DIRECT,
        lambda sql: 0 if _percent(sql) < 4 else int(_percent(sql) * 1000),
    )
    read = await _read(_state("postgresql"), _target())
    assert read.method == "block"
    assert read.attempts == [{"percent": 1.0, "rows": 0}, {"percent": 4.0, "rows": 4000}]


async def test_escalation_stops_at_the_whole_table(monkeypatch):
    _FakePipeline(monkeypatch, Route.DIRECT, lambda sql: 10 if _percent(sql) < 100 else 300)
    read = await _read(_state("postgresql"), _target(), fraction=0.2)
    assert [a["percent"] for a in read.attempts] == [20.0, 80.0, 100.0]


async def test_an_empty_block_sample_of_a_counted_table_fails_by_name(monkeypatch):
    from provisa.profiler.run import ProfileError

    _FakePipeline(monkeypatch, Route.DIRECT, lambda sql: 0)
    with pytest.raises(
        ProfileError,
        match=r"block sample of 'orders' \(direct postgresql\) came back empty at 100.0%",
    ):
        await _read(_state("postgresql"), _target())


async def test_an_empty_table_sampled_by_block_is_not_a_failure(monkeypatch):
    _FakePipeline(monkeypatch, Route.DIRECT, lambda sql: 0)
    read = await _read(_state("postgresql"), _target(), row_count=0)
    assert read.attempts == [{"percent": 1.0, "rows": 0}]


async def test_a_sampled_statement_that_routes_elsewhere_fails(monkeypatch):
    from provisa.profiler.run import ProfileError

    _FakePipeline(monkeypatch, Route.DIRECT, lambda sql: 1000, block_route=Route.ENGINE)
    with pytest.raises(ProfileError, match="method was chosen for"):
        await _read(_state("postgresql"), _target())


async def test_a_keyed_table_without_block_reach_reads_key_ranges(monkeypatch):
    pipe = _FakePipeline(monkeypatch, Route.DIRECT, lambda sql: 990)
    read = await _read(_state("mysql"), _target())
    assert read.method == "key_range"
    assert read.attempts == [{"percent": 1.0, "rows": 990}]
    assert pipe.statements[1].startswith('SELECT MIN(t."id")')
    assert pipe.statements[2].count("BETWEEN") == 16


async def test_a_table_with_no_key_and_no_block_reach_keeps_the_row_filter(monkeypatch):
    pipe = _FakePipeline(monkeypatch, Route.DIRECT, lambda sql: 1000)
    read = await _read(_state("mysql"), _target(key=None))
    assert read.method == "random"
    # The routed row-filter statement is the one executed: no second statement.
    assert len(pipe.statements) == 1 and "RANDOM() < 0.01" in pipe.statements[0]
