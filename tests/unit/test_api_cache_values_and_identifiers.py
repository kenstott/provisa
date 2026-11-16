# Copyright (c) 2026 Kenneth Stott
# Canary: 6fd959a3-5f9b-4d2e-b8f2-9f2cc5c5b948
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-318: what an API returns is data in the engine cache, never statement text.

A response value reaches the cache table as a bound parameter where the connection's driver
binds values, and as a literal the engine's own dialect escapes where the backend declares no
bind marker. A column, table, schema or catalog name is an identifier quoted by that dialect.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import duckdb
import pytest

from provisa.api_source import engine_cache
from provisa.api_source.engine_cache import CacheLocation, create_and_insert, ensure_cache_schema
from provisa.executor.session import EngineSession, StoreBrokerSession
from provisa.federation.materialize_broker import get_broker

ODD_VALUE = "it's a\\b; DROP TABLE keep_me; --"
ODD_COLUMN = 'na"me'


def _col(name: str, type_name: str):
    return SimpleNamespace(name=name, type=type_name)


COLUMNS = [_col("id", "integer"), _col(ODD_COLUMN, "string"), _col("doc", "jsonb")]
ROWS = [
    {"id": 1, ODD_COLUMN: ODD_VALUE, "doc": {"k": "v'"}},
    {"id": 2, ODD_COLUMN: None, "doc": None},
]


@pytest.fixture(autouse=True)
def _clear_caches():
    engine_cache._SCHEMA_EXISTS_CACHE.clear()
    engine_cache._TABLE_EXISTS_CACHE.clear()
    yield
    engine_cache._SCHEMA_EXISTS_CACHE.clear()
    engine_cache._TABLE_EXISTS_CACHE.clear()


def _duckdb_engine_session(tmp_path):
    del tmp_path
    return EngineSession(duckdb.connect(), dialect="duckdb", placeholder="?"), "memory"


def _duckdb_broker_session(tmp_path):
    return StoreBrokerSession(get_broker(str(tmp_path / "materialize.duckdb"))), "mat_store"


@pytest.mark.parametrize("make_session", [_duckdb_engine_session, _duckdb_broker_session])
def test_values_and_column_names_round_trip_as_data_on_duckdb(tmp_path, make_session):
    session, catalog = make_session(tmp_path)
    loc = CacheLocation(catalog=catalog, schema="api_cache", backend="relational")
    ensure_cache_schema(session, loc)
    session.execute(f"CREATE TABLE {catalog}.api_cache.keep_me (n INTEGER)").fetchall()

    create_and_insert(session, loc, "r_0123", ROWS, COLUMNS)

    got = session.execute(
        f'SELECT id, "na""me", doc FROM {catalog}.api_cache."r_0123" ORDER BY id'
    ).fetchall()
    assert got == [(1, ODD_VALUE, '{"k": "v\'"}'), (2, None, None)]
    # The value was stored, not run: the other table is still there.
    assert session.execute(f"SELECT count(*) FROM {catalog}.api_cache.keep_me").fetchall() == [(0,)]


class _RecordingSession:
    """A session whose backend declares no bind marker: every value must arrive as a literal."""

    placeholder = None

    def __init__(self, dialect: str) -> None:
        self.dialect = dialect
        self.executed: list[tuple[str, list | None]] = []

    def execute(self, sql: str, params: list | None = None) -> "_RecordingSession":
        self.executed.append((sql, params))
        return self

    def fetchall(self) -> list:
        return []


@pytest.mark.parametrize(
    ("dialect", "column", "column_sql", "value_sql"),
    [
        # Standard strings: a quote is doubled and a backslash is an ordinary character.
        ("trino", ODD_COLUMN, '"na""me"', "'it''s a\\b; DROP TABLE keep_me; --'"),
        ("postgres", ODD_COLUMN, '"na""me"', "'it''s a\\b; DROP TABLE keep_me; --'"),
        # A dialect whose strings take backslash escapes, and whose identifier quote is a backtick.
        ("mysql", "na`me", "`na``me`", "'it''s a\\\\b; DROP TABLE keep_me; --'"),
    ],
)
def test_statement_text_is_rendered_by_the_dialect(dialect, column, column_sql, value_sql):
    session = _RecordingSession(dialect)
    loc = CacheLocation(catalog="provisa_admin", schema="odd schema", backend="relational")
    columns = [_col("id", "integer"), _col(column, "string"), _col("doc", "jsonb")]
    rows = [{"id": 1, column: ODD_VALUE, "doc": None}]

    create_and_insert(session, loc, "r_0123", rows, columns)

    q = column_sql[0]
    ref = f"provisa_admin.{q}odd schema{q}.{q}r_0123{q}"
    assert session.executed == [
        (
            f"CREATE TABLE IF NOT EXISTS {ref} "
            f"({q}id{q} BIGINT, {column_sql} VARCHAR, {q}doc{q} VARCHAR)",
            None,
        ),
        (
            f"INSERT INTO {ref} ({q}id{q}, {column_sql}, {q}doc{q}) VALUES (1, {value_sql}, NULL)",
            None,
        ),
    ]


async def test_promotion_target_is_a_quoted_schema_qualified_name(monkeypatch):
    import provisa.api.app as app
    from provisa.api_source import promotions, router_integration

    targets: list[str] = []

    async def _apply(_conn, table_name, _promotions, cast_source=False):
        del cast_source
        targets.append(table_name)
        return 0

    @asynccontextmanager
    async def _acquire():
        yield object()

    monkeypatch.setattr(promotions, "apply_promotions", _apply)
    monkeypatch.setattr(app.state, "tenant_db", SimpleNamespace(acquire=_acquire), raising=False)
    endpoint = SimpleNamespace(promotions=[object()])

    plain = CacheLocation(catalog="provisa_admin", schema="org_a_api_cache", backend="relational")
    await router_integration._apply_cache_promotions(plain, "r_0123", endpoint)
    odd = CacheLocation(catalog="provisa_admin", schema='s"; x', backend="relational")
    await router_integration._apply_cache_promotions(odd, 't"; y', endpoint)

    assert targets == ['org_a_api_cache."r_0123"', '"s""; x"."t""; y"']


async def test_a_joins_key_read_goes_through_the_pipeline_with_its_names_quoted(monkeypatch):
    """REQ-1986: the keys a join gives a parameter are read through the one pipeline as the role
    (the parent lookup hydration made on the tenant plane is gone): the binder's read, each
    governed relation in it named again as its registered table, every name an identifier."""
    from provisa.api.data import materialization
    from provisa.compiler.nf_extractor import JoinKey
    from provisa.pgwire import _pipeline

    governed: list[tuple[str, str]] = []

    async def _govern(sql, role_id, **_kwargs):
        governed.append((sql, role_id))
        return "plan"

    async def _execute(_plan):
        return SimpleNamespace(column_names=["key"], rows=[(1,), (2,)])

    monkeypatch.setattr(_pipeline, "_govern_and_route", _govern)
    monkeypatch.setattr(_pipeline, "_execute_plan", _execute)
    monkeypatch.setattr(
        "provisa.compiler.sql_rewrite.semantic_ref", lambda meta: '"s;x"."pa""rent"'
    )
    parent = SimpleNamespace(table_name='pa"rent', schema_name="s;x")
    ctx = SimpleNamespace(tables={"parent": parent})
    query = (
        'SELECT DISTINCT "p"."jo""in" AS "key" FROM (SELECT "p"."jo""in" AS "jo""in" '
        'FROM "pg"."s;x"."pa""rent" AS "p") AS p WHERE p."jo""in" > 3'
    )

    keys = await materialization._join_keys(JoinKey(query, "parent.join"), ctx, "analyst")

    assert keys == [1, 2]
    assert governed == [
        (
            'SELECT DISTINCT "p"."jo""in" AS "key" FROM "s;x"."pa""rent" AS "p" WHERE p."jo""in" > 3',
            "analyst",
        )
    ]


@pytest.mark.parametrize("make_session", [_duckdb_engine_session, _duckdb_broker_session])
def test_rows_go_under_their_own_columns_into_a_table_that_holds_more(tmp_path, make_session):
    """A cache table that already holds a column these rows do not carry -- a parameter column,
    a generated column a promotion added -- takes the rows under their names. Positional values
    failed there with a count that did not match the table's."""
    session, catalog = make_session(tmp_path)
    loc = CacheLocation(catalog=catalog, schema="api_cache", backend="relational")
    ensure_cache_schema(session, loc)
    session.execute(
        f'CREATE TABLE {catalog}.api_cache."r_0456" ("limit" BIGINT, id BIGINT, name VARCHAR)'
    ).fetchall()

    columns = [_col("id", "integer"), _col("name", "string")]
    create_and_insert(session, loc, "r_0456", [{"id": 1, "name": "a"}], columns)

    got = session.execute(f'SELECT "limit", id, name FROM {catalog}.api_cache."r_0456"').fetchall()
    assert got == [(None, 1, "a")]
