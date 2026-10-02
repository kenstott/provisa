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
        (f"INSERT INTO {ref} VALUES (1, {value_sql}, NULL)", None),
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


async def test_hydration_parent_lookup_quotes_its_names():
    from provisa.api.data.hydration import _hydrate_dataloader

    fetched: list[str] = []

    class _Conn:
        capabilities = SimpleNamespace(schemas=True)

        async def fetch(self, sql: str):
            fetched.append(sql)
            return []

    @asynccontextmanager
    async def _acquire():
        yield _Conn()

    state = SimpleNamespace(tenant_db=SimpleNamespace(acquire=_acquire), api_endpoints={})
    parent = SimpleNamespace(table_name='pa"rent', schema_name="s;x")

    await _hydrate_dataloader(
        src=None,
        endpoint=SimpleNamespace(table_name="child"),
        ttl=300,
        source_id="src",
        dataloader_col=None,
        dataloader_parent_join_col='jo"in',
        dataloader_parent_table_meta=parent,
        state=state,
        hydration_rows={},
    )

    assert fetched == ['SELECT DISTINCT "jo""in" FROM "s;x"."pa""rent" WHERE "jo""in" IS NOT NULL']
