# Copyright (c) 2026 Kenneth Stott
# Canary: a80590b8-8935-40c1-b7f4-f3d7d6433660
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A replica's vector index is the fallback for a vector search (REQ-424, REQ-425).

Where the engine reads a replica through its store's own SQL, the replica holds an embedding
column as a vector with an HNSW index: the DuckDB engine indexes its copy of a replica in its
embedded store, the PostgreSQL engine holds a ``REAL[]`` indexed through its pgvector cast.
A top-k search over a replica reads the index and says its answer is approximate; every other
search is exact. On Trino the replica holds the embedding as its JSON text, and nothing is
indexed or said.
"""

# Requirements: REQ-424, REQ-425

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pytest
import sqlglot

from provisa.core.ir_types import to_physical
from provisa.core.statement_warnings import collecting
from provisa.federation.replica_address import ReplicaRoute, ReplicaRoutes
from provisa.federation.replica_target import PostgresStoreTarget, _pg_swap, pg_statement_copy
from provisa.federation.residency import resolve_landing_args
from provisa.vector.similarity import index_top_k, lower

_REPLICAS = "org_acme_replicas"
_REPLICA = (None, _REPLICAS, "src__public__docs")
_SEARCH = (
    'SELECT "d"."id", cosine_similarity("d"."vec", $1, \'mini\', 3) AS "_similarity" '
    'FROM "org_acme_replicas"."src__public__docs" AS "d" '
    'ORDER BY cosine_similarity("d"."vec", $1, \'mini\', 3) DESC LIMIT 3'
)


# -- what lands --------------------------------------------------------------------------------


def _table():
    columns = [
        SimpleNamespace(
            name="id", data_type="integer", is_primary_key=True, embedding=False, epoch_unit=None
        ),
        SimpleNamespace(
            name="vec",
            data_type="array(double)",
            is_primary_key=False,
            embedding=True,
            epoch_unit=None,
        ),
    ]
    return SimpleNamespace(
        columns=columns,
        schema_name="public",
        table_name="docs",
        change_signal=None,
        live=None,
        watermark_column=None,
        probe_type=None,
        landing_filter=None,
    )


@pytest.mark.parametrize("platform", ["duckdb", "postgres"])
def test_an_embedding_column_lands_as_a_vector_where_the_engine_indexes_it(platform):
    source = SimpleNamespace(change_signal=None, type="postgresql")
    args = resolve_landing_args(source, _table(), platform=platform)  # type: ignore[arg-type]
    assert args.columns == [("id", "integer"), ("vec", "vector")]


def test_on_trino_an_embedding_column_lands_as_any_other_column():
    source = SimpleNamespace(change_signal=None, type="postgresql")
    args = resolve_landing_args(source, _table(), platform="trino")  # type: ignore[arg-type]
    assert args.columns == [("id", "integer"), ("vec", "text")]


def test_a_vector_is_pgvector_in_postgresql_a_float_list_in_duckdb_and_text_elsewhere():
    import duckdb_engine

    from provisa.core.ir_types import Embedding

    assert to_physical("vector", "postgresql") == "REAL[]"
    assert Embedding().compile(dialect=duckdb_engine.Dialect()) == "FLOAT[]"
    assert to_physical("vector", "sqlite") == "TEXT"


# -- the PostgreSQL store ----------------------------------------------------------------------


class _Cursor:
    def __init__(self, dims: int | None = 3) -> None:
        self.statements: list[str] = []
        self._dims = dims
        self._last = ""

    def execute(self, sql: str, *_args) -> None:
        self.statements.append(sql)
        self._last = sql

    def fetchone(self):
        if self._last.startswith("SELECT array_length"):
            return None if self._dims is None else (self._dims,)
        if self._last.startswith("SELECT count(*)"):
            return (2, 1, 1)
        raise AssertionError(f"unexpected fetch after {self._last!r}")

    @property
    def rowcount(self) -> int:
        return 2


def _target(native: bool) -> PostgresStoreTarget:
    return PostgresStoreTarget(
        "postgresql://h/db",
        schema=_REPLICAS,
        table="src__public__docs",
        columns=[("id", "integer"), ("vec", "vector")],
        native_vectors=native,
    )


def test_a_postgresql_store_the_engine_reads_holds_a_vector_and_one_it_does_not_holds_text():
    assert '"vec" REAL[]' in _target(True)._create_ddl()
    assert '"vec" TEXT' in _target(False)._create_ddl()


async def test_an_indexed_embedding_is_written_as_its_floats_and_one_held_as_text_as_json():
    for native, written_vec in ((True, [1.0, 0.5, 0.0]), (False, "[1.0, 0.5, 0.0]")):
        target = _target(native)
        target._create_ddl()
        written: list[list] = []
        target._copy = SimpleNamespace(write_row=written.append)
        await target.write(
            pa.record_batch([]),
            [{"id": 1, "vec": [1, 0.5, 0]}, {"id": 2, "vec": None}],
        )
        assert written == [[1, written_vec], [2, None]]
    target = _target(True)
    target._create_ddl()
    written = []
    target._copy = SimpleNamespace(write_row=written.append)
    await target.write(pa.record_batch([]), [{"id": 3, "vec": "[0, 1, 2]"}])
    assert written == [[3, [0.0, 1.0, 2.0]]]


def test_the_swap_indexes_each_vector_through_its_cast_at_its_length_for_cosine_distance():
    cur = _Cursor(dims=3)
    _pg_swap(cur, _REPLICAS, "src__public__docs", "build__x", ["vec"])
    replica = f'"{_REPLICAS}"."src__public__docs"'
    (index,) = [s for s in cur.statements if s.startswith("CREATE INDEX")]
    assert index.endswith(f'ON {replica} USING hnsw ((CAST("vec" AS vector(3))) vector_cosine_ops)')


def test_a_replica_with_no_vector_has_nothing_to_index():
    cur = _Cursor(dims=None)
    _pg_swap(cur, _REPLICAS, "src__public__docs", "build__x", ["vec"])
    assert not [s for s in cur.statements if "CREATE INDEX" in s]


def test_the_engine_statement_copy_brings_up_pgvector_and_indexes_the_replica(monkeypatch):
    from provisa.federation import replica_guard

    monkeypatch.setattr(replica_guard, "require_pg_replica_table", lambda *a, **k: True)
    cur = _Cursor(dims=3)
    pg_statement_copy(
        cur,
        source_relation='"fdw"."docs"',
        schema=_REPLICAS,
        table="src__public__docs",
        columns=[("id", "integer"), ("vec", "vector")],
        prior_hash=None,
    )
    assert "CREATE EXTENSION IF NOT EXISTS vector" in cur.statements
    assert any("USING hnsw" in s for s in cur.statements)


# -- the DuckDB engine's copy of a replica -----------------------------------------------------


class _Broker:
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def canary(self) -> int:
        return 1

    def fetch_arrow(self, schema: str, table: str):
        del schema, table
        return self._table, 1


def _duckdb_runtime(rows: list[list[float] | None]):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    runtime = object.__new__(DuckDBFederationRuntime)
    runtime._con = duckdb.connect()
    runtime._ext_loaded = set()
    runtime._store_copy_canary = {}
    runtime._store_relation_lock = __import__("threading").Lock()
    # the embedded store file, read through a per-statement copy (REQ-1999: MotherDuck's is not)
    runtime._attached_store_dsn = "duckdb:///store.duckdb"
    runtime._store_broker = _Broker(
        pa.table(
            {
                "id": pa.array(range(len(rows)), pa.int32()),
                "vec": pa.array(rows, pa.list_(pa.float32())),
            }
        )
    )
    return runtime


def _rows(n: int) -> list[list[float] | None]:
    return [[float(i % 7), float(i % 5) + 1.0, float(i % 3)] for i in range(n)]


def _duckdb_search(runtime, sql: str):
    duck = lower(sqlglot.transpile(sql, read="postgres", write="duckdb")[0], "duckdb")
    duck = runtime._refresh_store_relations(duck.replace("$1", "?"))
    return duck, ["[1, 2, 0]"] * duck.count("?")


def test_the_engine_indexes_its_copy_of_a_replica_and_a_top_k_search_reads_the_index():
    runtime = _duckdb_runtime(_rows(200))
    searched, used = index_top_k(_SEARCH, frozenset({_REPLICA}))
    assert used
    duck, params = _duckdb_search(
        runtime, searched.replace('"org_acme_replicas"', '"mat_store"."org_acme_replicas"')
    )
    plan = runtime._con.execute("EXPLAIN " + duck, params).fetchall()[0][1]
    assert "HNSW" in plan
    rows = runtime._con.execute(duck, params).fetchall()
    assert len(rows) == 3 and rows[0][1] >= rows[1][1] >= rows[2][1]


def test_a_copy_outside_the_replicas_schema_is_not_indexed():
    runtime = _duckdb_runtime(_rows(20))
    runtime._refresh_store_relations('SELECT * FROM "mat_store"."org_acme_mv"."docs"')
    indexes = runtime._con.execute("SELECT count(*) FROM duckdb_indexes()").fetchone()
    assert indexes == (0,)


def test_an_empty_replica_is_copied_with_nothing_to_index():
    runtime = _duckdb_runtime([])
    runtime._refresh_store_relations(f'SELECT * FROM "mat_store"."{_REPLICAS}"."src__public__docs"')
    assert runtime._con.execute("SELECT count(*) FROM duckdb_indexes()").fetchone() == (0,)


def test_vectors_of_two_lengths_in_one_column_fail_the_read():
    runtime = _duckdb_runtime([[1.0, 0.0, 0.0], [1.0, 0.0]])
    with pytest.raises(duckdb.Error):
        runtime._refresh_store_relations(
            f'SELECT * FROM "mat_store"."{_REPLICAS}"."src__public__docs"'
        )


# -- which searches read the index -------------------------------------------------------------


def test_a_top_k_search_over_a_replica_is_written_nearest_first_by_distance():
    searched, used = index_top_k(_SEARCH, frozenset({_REPLICA}))
    assert used
    assert 'ORDER BY PROVISA_COSINE_DISTANCE("d"."vec", $1, \'mini\', 3) ASC LIMIT 3' in searched
    assert 'COSINE_SIMILARITY("d"."vec", $1, \'mini\', 3) AS "_similarity"' in searched


@pytest.mark.parametrize(
    "sql",
    [
        _SEARCH.replace(" LIMIT 3", ""),  # not a top-k: every row, exactly ordered
        _SEARCH.replace(") DESC", ") ASC"),  # least similar first
        _SEARCH.replace('"src__public__docs"', '"other"'),  # a table read live
    ],
)
def test_every_other_search_is_left_exact(sql):
    assert index_top_k(sql, frozenset({_REPLICA})) == (sql, False)


def test_a_search_in_a_join_reads_the_index_of_the_replica_its_column_belongs_to():
    sql = (
        'SELECT "d"."id" FROM "org_acme_replicas"."src__public__docs" AS "d" '
        'JOIN "crm"."public"."tags" AS "t" ON "t"."id" = "d"."id" '
        'ORDER BY cosine_similarity("d"."vec", $1, \'mini\', 3) DESC LIMIT 5'
    )
    assert index_top_k(sql, frozenset({_REPLICA}))[1]
    other = sql.replace('cosine_similarity("d"', 'cosine_similarity("t"')
    assert index_top_k(other, frozenset({_REPLICA})) == (other, False)


def test_the_distance_is_written_in_each_engine_s_indexed_form():
    sql = "SELECT 1 FROM t ORDER BY provisa_cosine_distance(\"vec\", $1, 'mini', 3) LIMIT 3"
    assert 'ARRAY_COSINE_DISTANCE(CAST("vec" AS REAL[3]), CAST($1 AS REAL[3]))' in lower(
        sqlglot.transpile(sql, read="postgres", write="duckdb")[0], "duckdb"
    )
    # The cast a replica's index is built on.
    assert 'CAST("vec" AS VECTOR(3)) OPERATOR(<=>) CAST($1 AS VECTOR(3))' in lower(
        sql, "postgres", pgvector=True
    )
    trino = lower(sqlglot.transpile(sql, read="postgres", write="trino")[0], "trino")
    assert "(1 - COSINE_SIMILARITY(" in trino


# -- the engine seam ---------------------------------------------------------------------------


class _Backend:
    computes_fakes = True

    def __init__(self, indexes: bool) -> None:
        self._indexes = indexes

    def indexes_replica_vectors(self, state) -> bool:
        del state
        return self._indexes

    def transpile_physical(self, pg_sql: str) -> str:
        return pg_sql

    def exact_reads(self, state, physical_sql: str) -> str:
        del state
        return physical_sql  # an engine whose reads keep a source's values as they are


def _runtime(indexes: bool):
    from provisa.federation.runtime import EngineRuntime

    route = ReplicaRoute("src", "docs", _REPLICA)
    routes = ReplicaRoutes(engine_name="duckdb", routes={(None, "src", "docs"): route})
    engine = SimpleNamespace(name="duckdb", backend=_Backend(indexes), catalog_qualified=False)
    return EngineRuntime(engine, SimpleNamespace(replica_routes=routes))  # type: ignore[arg-type]


_LIVE_SEARCH = _SEARCH.replace('"org_acme_replicas"."src__public__docs"', '"src"."docs"')


def test_an_engine_reading_its_own_store_searches_the_index_and_says_it_is_approximate():
    with collecting() as found:
        physical = _runtime(True).transpile_physical(_LIVE_SEARCH)
    assert "PROVISA_COSINE_DISTANCE" in physical
    assert [w.code for w in found] == ["statement.approximate_index"]


def test_an_engine_reading_its_store_through_a_connector_computes_the_search_exactly():
    with collecting() as found:
        physical = _runtime(False).transpile_physical(_LIVE_SEARCH)
    assert "PROVISA_COSINE_DISTANCE" not in physical
    assert '"org_acme_replicas"."src__public__docs"' in physical
    assert found == []


# -- an embedding is never REQ-1965's JSON ----------------------------------------------------


def test_an_embedding_never_lands_as_json_on_any_platform():
    # REQ-1965 lands a native array as JSON; an embedding is exempt end to end (REQ-424)
    source = SimpleNamespace(change_signal=None, type="postgresql")
    for platform in ("duckdb", "postgres", "trino", "sqlite"):
        args = resolve_landing_args(source, _table(), platform=platform)  # type: ignore[arg-type]
        assert dict(args.columns)["vec"] != "json", platform


def test_an_embedding_on_a_duckdb_replica_serves_cosine_similarity():
    """What lands for an embedding on DuckDB is a float list, and the lowered similarity search
    reads it: nearest first."""
    import duckdb

    from provisa.core.ir_types import Embedding

    source = SimpleNamespace(change_signal=None, type="postgresql")
    args = resolve_landing_args(source, _table(), platform="duckdb")  # type: ignore[arg-type]
    assert dict(args.columns)["vec"] == "vector"
    import duckdb_engine

    float_list = Embedding().compile(dialect=duckdb_engine.Dialect())
    con = duckdb.connect()
    con.execute(f"CREATE TABLE docs (id INTEGER, vec {float_list})")
    con.execute("INSERT INTO docs VALUES (1, [1.0, 0.0, 0.0]), (2, [0.0, 1.0, 0.0])")
    search = (
        'SELECT "d"."id" FROM "docs" AS "d" '
        "ORDER BY cosine_similarity(\"d\".\"vec\", '[0.1, 0.9, 0.0]', 'mini', 3) DESC LIMIT 2"
    )
    lowered = lower(search, "duckdb", pgvector=False)
    assert [r[0] for r in con.execute(lowered).fetchall()] == [2, 1]
