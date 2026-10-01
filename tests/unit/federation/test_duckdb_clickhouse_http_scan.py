# Copyright (c) 2026 Kenneth Stott
# Canary: 8e1c4b27-3d9a-4f60-b5e2-7a0d9c6f1e38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-899 (amended): DuckDB reads a live ClickHouse table over ClickHouse's HTTP interface.

The query-time rewrite (clickhouse_http_scan.rewrite) replaces each registered ClickHouse table
with read_parquet(<url>) whose ClickHouse query projects only the used columns and carries the
pushable literal predicates, bound $N params inlined. Pure unit tests: the ClickHouse query is
decoded from the URL; nothing contacts a server except the failure test, which targets a closed
port on purpose.
"""

from __future__ import annotations

import re
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

from provisa.federation.clickhouse_http_scan import (
    ClickHouseRelation,
    ClickHouseScanError,
    rewrite,
    secret_ddl,
)

_KEY = ("bench_clickhouse", "default", "order_events")
_REL = ClickHouseRelation(
    base_url="http://ch.example:8123",
    database="default",
    table="order_events",
    columns=(
        ("order_id", "UInt64"),
        ("customer_id", "Nullable(UInt32)"),
        ("event_type", "LowCardinality(String)"),
        ("amount", "Decimal(18, 2)"),
        ("event_ts", "DateTime64(3)"),
        ("status", "Enum8('new' = 1, 'done' = 2)"),
        ("event_uuid", "UUID"),
        ("region", "String"),
        ("payload", "String"),
    ),
)
_RELS = {_KEY: _REL}
_PHYS = '"bench_clickhouse"."default"."order_events"'


def _reads(sql: str) -> list[dict[str, str]]:
    """Every read_parquet URL in ``sql``, decoded to its query-string parameters."""
    urls = re.findall(r"read_parquet\('([^']+)'\)", sql, flags=re.I)
    return [{k: v[0] for k, v in parse_qs(urlparse(u).query).items()} for u in urls]


def _ch_sql(sql: str) -> str:
    reads = _reads(sql)
    assert len(reads) == 1, sql
    return reads[0]["query"]


def test_projection_carries_only_the_used_columns():
    out = rewrite(
        f"SELECT e.order_id, e.event_type FROM {_PHYS} AS e WHERE e.amount > 10",
        None,
        _RELS,
        deadline_s=None,
    )
    assert out is not None
    ch = _ch_sql(out)
    assert ch.startswith('SELECT "order_id", "event_type", "amount" FROM "default"."order_events"')
    assert '"payload"' not in ch and '"region"' not in ch
    assert ch.endswith("FORMAT Parquet")


def test_star_projects_every_column():
    ch = _ch_sql(rewrite(f"SELECT * FROM {_PHYS} AS e", None, _RELS, deadline_s=None) or "")
    for name, _ in _REL.columns:
        assert f'"{name}"' in ch


def test_predicates_and_bound_params_reach_clickhouse():
    sql = (
        f'SELECT o.order_id, e.event_type FROM "pg"."public"."orders" AS o '
        f"JOIN {_PHYS} AS e ON o.order_id = e.order_id "
        "WHERE o.order_id = $1 AND e.order_id = $1 AND e.customer_id IN ($2, 7) "
        "AND e.amount BETWEEN 1.5 AND 20 AND e.region = 'EU' AND e.status = 'done'"
    )
    ch = _ch_sql(rewrite(sql, [42, 9], _RELS, deadline_s=None) or "")
    where = ch.split(" WHERE ", 1)[1]
    assert '"order_id" = 42' in where
    assert '"customer_id" IN (9, 7)' in where
    assert "\"amount\" BETWEEN toDecimal128('1.5', 1) AND 20" in where
    assert "\"region\" = 'EU'" in where
    assert "toString(\"status\") = 'done'" in where  # compared on the exported (string) value


def test_string_param_is_escaped_for_clickhouse():
    ch = _ch_sql(
        rewrite(
            f"SELECT e.region FROM {_PHYS} AS e WHERE e.region = $1",
            ["a'b\\c"],
            _RELS,
            deadline_s=None,
        )
        or ""
    )
    assert "\"region\" = 'a\\'b\\\\c'" in ch


def test_not_pushed_or_datetime_or_outer_preserved_side():
    sql = (
        f'SELECT e.order_id FROM {_PHYS} AS e LEFT JOIN "pg"."public"."orders" AS o '
        "ON o.order_id = e.order_id AND e.region = 'EU' "
        "WHERE (e.order_id = 1 OR e.order_id = 2) AND e.event_ts > '2020-01-01'"
    )
    ch = _ch_sql(rewrite(sql, None, _RELS, deadline_s=None) or "")
    assert " WHERE " not in ch  # OR, DateTime (time-zone semantics), preserved-side ON: none pushed


def test_own_left_join_on_is_pushed():
    sql = (
        f'SELECT o.order_id, e.region FROM "pg"."public"."orders" AS o LEFT JOIN {_PHYS} AS e '
        "ON o.order_id = e.order_id AND e.region = 'EU'"
    )
    ch = _ch_sql(rewrite(sql, None, _RELS, deadline_s=None) or "")
    assert ch.split(" WHERE ", 1)[1].startswith("\"region\" = 'EU'")


def test_no_widening_under_rls():
    """The governed statement's RLS predicate is pushed as a COPY and stays in the outer SQL, and
    nothing the governed statement doesn't already filter on is added."""
    governed = (
        f"SELECT t.order_id, t.amount FROM (SELECT * FROM {_PHYS} AS g WHERE g.region = 'EU') AS t "
        "WHERE t.amount > 5"
    )
    out = rewrite(governed, None, _RELS, deadline_s=None) or ""
    ch = _ch_sql(out)
    assert ch.split(" WHERE ", 1)[1] == "\"region\" = 'EU' FORMAT Parquet"
    outer = re.sub(r"read_parquet\('[^']+'\)", "<scan>", out, flags=re.I)
    assert "g.region = 'EU'" in outer and "t.amount > 5" in outer


def test_secret_never_in_sql_or_url():
    ddl = secret_ddl("bench-clickhouse", "http://ch.example:8123", "reader", "s3cr3t")
    assert "'X-ClickHouse-User': 'reader'" in ddl and "'X-ClickHouse-Key': 's3cr3t'" in ddl
    assert "SCOPE 'http://ch.example:8123/'" in ddl
    out = rewrite(f"SELECT e.order_id FROM {_PHYS} AS e", None, _RELS, deadline_s=None) or ""
    assert "s3cr3t" not in out and "reader" not in out


def test_deadline_bounds_clickhouse_execution_and_urls_are_unique():
    sql = f"SELECT e.order_id FROM {_PHYS} AS e"
    a = _reads(rewrite(sql, None, _RELS, deadline_s=12.2) or "")[0]
    b = _reads(rewrite(sql, None, _RELS, deadline_s=None) or "")[0]
    assert a["max_execution_time"] == "14"  # ceil(12.2) + 1: Provisa's deadline fires first
    assert "max_execution_time" not in b  # outside a request: no request budget
    assert a["query_id"] != b["query_id"]


def test_type_conversions_and_casts():
    rel = ClickHouseRelation(
        "http://h:8123",
        "db",
        "t",
        (
            ("u", "UUID"),
            ("us", "Array(Nullable(UUID))"),
            ("big", "Decimal(76, 2)"),
            ("h", "Int128"),
            ("d", "Date"),
            ("ts", "DateTime('UTC')"),
            ("ip", "IPv4"),
        ),
    )
    out = rewrite('SELECT * FROM "c"."db"."t" AS x', None, {("c", "db", "t"): rel}, deadline_s=None)
    assert out is not None
    ch = _ch_sql(out)
    assert 'toString("u") AS "u"' in ch
    assert 'arrayMap(_v0 -> toString(_v0), "us") AS "us"' in ch
    assert 'toString("big") AS "big"' in ch
    assert 'toDate32("d") AS "d"' in ch
    assert 'toDateTime64("ts", 0) AS "ts"' in ch
    assert 'toString("ip") AS "ip"' in ch
    assert 'CAST("u" AS UUID)' in out and 'CAST("us" AS UUID[])' in out
    assert 'CAST("h" AS INT128)' in out  # DuckDB's HUGEINT


def test_unmapped_type_raises():
    rel = ClickHouseRelation("http://h:8123", "db", "t", (("m", "Map(String, UUID)"),))
    with pytest.raises(ClickHouseScanError, match="Map/Tuple"):
        rewrite('SELECT m FROM "c"."db"."t"', None, {("c", "db", "t"): rel}, deadline_s=None)


def test_write_target_raises():
    with pytest.raises(ClickHouseScanError, match="does not write"):
        rewrite(f"INSERT INTO {_PHYS} SELECT 1", None, _RELS, deadline_s=None)


def test_unrelated_statement_untouched():
    assert rewrite('SELECT 1 FROM "pg"."public"."orders"', None, _RELS, deadline_s=None) is None


def test_describe_reads_no_rows():
    ch = _ch_sql(
        rewrite(f"SELECT * FROM {_PHYS}", None, _RELS, deadline_s=None, describe=True) or ""
    )
    assert ch.endswith(" LIMIT 0 FORMAT Parquet")


# -- runtime + residency -------------------------------------------------------------------------


def test_duckdb_declares_clickhouse_live_so_row_materialize_is_ignored():
    from provisa.federation.engine import build_duckdb_engine
    from provisa.federation.strategy import engine_attaches

    engine = build_duckdb_engine()
    assert engine_attaches(engine, "clickhouse")
    assert engine.connector_for("clickhouse").extension == "httpfs"


def _row_materialized_clickhouse(monkeypatch):
    table = SimpleNamespace(
        source_id="bench-clickhouse", table_name="order_events", row_materialize=True
    )
    source = SimpleNamespace(id="bench-clickhouse", type=SimpleNamespace(value="clickhouse"))

    async def _tables(state):
        return [table]

    async def _sources(state):
        return [source]

    monkeypatch.setattr("provisa.federation.registry_view.registered_tables", _tables)
    monkeypatch.setattr("provisa.federation.registry_view.registered_sources", _sources)


@pytest.mark.asyncio
async def test_active_row_materialize_excludes_clickhouse_on_duckdb(monkeypatch):
    from provisa.federation.engine import build_duckdb_engine
    from provisa.federation.query_residency import active_row_materialize_tables

    _row_materialized_clickhouse(monkeypatch)
    state = SimpleNamespace(federation_engine=build_duckdb_engine())
    assert await active_row_materialize_tables(state) == []


@pytest.mark.asyncio
async def test_key_pushdown_never_touches_a_row_materialize_clickhouse_table(monkeypatch):
    """The row_materialize key pushdown selects nothing for a declared-live ClickHouse table: its
    engine and backend are never reached (they raise if they are)."""
    from provisa.federation.engine import build_duckdb_engine
    from provisa.federation.query_residency import pushdown_row_materialize

    _row_materialized_clickhouse(monkeypatch)

    class _Untouchable:
        def __getattr__(self, name):
            raise AssertionError(f"row_materialize reached the engine ({name})")

    duck = build_duckdb_engine()
    state = SimpleNamespace(federation_engine=SimpleNamespace(engine=duck))
    monkeypatch.setattr(type(duck), "backend", property(lambda self: _Untouchable()))
    sql = (
        f'SELECT o.order_id FROM "pg"."public"."orders" AS o JOIN {_PHYS} AS order_events '
        "ON o.order_id = order_events.order_id"
    )
    assert await pushdown_row_materialize(state, sql, "duckdb") == set()


def _runtime_with_relation(base_url: str):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime()
    rt._con.execute("INSTALL httpfs")
    rt._con.execute("LOAD httpfs")
    rt._httpfs_loaded = True
    rt._ch_relations[_KEY] = ClickHouseRelation(base_url, "default", "order_events", _REL.columns)
    return rt


@pytest.mark.asyncio
async def test_failed_live_read_raises():
    """A declared-live ClickHouse read that fails raises from the engine; there is no detour."""
    rt = _runtime_with_relation("http://127.0.0.1:1")  # closed port
    try:
        with pytest.raises(duckdb.Error):
            await rt.run(f"SELECT e.order_id FROM {_PHYS} AS e WHERE e.order_id = $1", [1])
        with pytest.raises(duckdb.Error):
            rt.run_arrow(f"SELECT e.order_id FROM {_PHYS} AS e")
    finally:
        rt.close()


def test_unrewritten_reference_fails_loudly():
    """No view stands at a ClickHouse physical name: SQL that bypasses the rewrite cannot read
    anything (a Catalog Error), never a stale or empty stand-in."""
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime()
    source = SimpleNamespace(
        id="bench-clickhouse", schema_name="default", table_name="order_events"
    )
    try:
        assert rt._phys_name(source) == _PHYS  # the catalog + schema exist; no relation in them
        with pytest.raises(duckdb.CatalogException):
            rt._con.execute(f"SELECT * FROM {_PHYS}")
    finally:
        rt.close()


def test_force_download_is_scoped_to_the_statement_cursor():
    rt = _runtime_with_relation("http://127.0.0.1:1")
    try:
        live = rt._open_cursor(live_http=True)
        sibling = rt._open_cursor(live_http=False)
        setting = "SELECT current_setting('force_download')"
        assert live.execute(setting).fetchone() == (True,)
        assert sibling.execute(setting).fetchone() == (False,)
        assert rt._con.execute(setting).fetchone() == (False,)
        retries = "SELECT current_setting('http_retries')"
        assert live.execute(retries).fetchone() == (0,)
        assert sibling.execute(retries).fetchone() == (3,)
        live.close()
        sibling.close()
    finally:
        rt.close()
