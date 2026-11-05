# Copyright (c) 2026 Kenneth Stott
# Canary: 83ef9331-52e1-4db1-ae20-ead15ef2fdd6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Foreign keys are read by ONE PostgreSQL query, for a PostgreSQL source over its pooled driver
and for a source read through its bundled Calcite pgwire server over that server's endpoint.

The query reads a foreign key's referenced columns from the key it references
(``referential_constraints`` -> ``key_column_usage`` by ``position_in_unique_constraint``), so a
composite key keeps its column pairing. It is run here against real keys in an in-memory DuckDB,
whose information_schema follows the standard as PostgreSQL's does."""

# Requirements: REQ-018, REQ-413

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest

from provisa.discovery import fk_introspect as fk


class _DuckDriver:
    """The driver seam ``_pg_fks`` reads through, over a DuckDB connection."""

    def __init__(self, con) -> None:
        self._con = con
        self.closed = False

    async def execute(self, sql, params=None):
        return SimpleNamespace(rows=self._con.execute(sql, params or []).fetchall())

    async def close(self) -> None:
        self.closed = True


@pytest.fixture
def keyed():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA s")
    con.execute("CREATE TABLE s.parent (a INT, b INT, name VARCHAR, PRIMARY KEY (a, b))")
    con.execute("CREATE TABLE s.ref (id INT PRIMARY KEY)")
    con.execute(
        "CREATE TABLE s.child (id INT PRIMARY KEY, pb INT, pa INT, "
        "r INT REFERENCES s.ref (id), FOREIGN KEY (pa, pb) REFERENCES s.parent (a, b))"
    )
    # A second parent in the schema: its key must never be paired with child's foreign keys.
    con.execute("CREATE TABLE s.other (a INT, b INT, PRIMARY KEY (a, b))")
    return _DuckDriver(con)


@pytest.mark.asyncio
async def test_a_single_column_key_is_paired_with_the_column_it_references(keyed):
    rows = (await keyed.execute(fk._PG_OUTBOUND, ["s", "child"])).rows
    assert ("r", "ref", "id") in rows


@pytest.mark.asyncio
async def test_a_composite_key_keeps_its_pairing_column_by_column(keyed):
    """Two pairs, not the four that listing the referenced columns without pairing gave."""
    rows = (await keyed.execute(fk._PG_OUTBOUND, ["s", "child"])).rows
    assert [r for r in rows if r[1] == "parent"] == [("pa", "parent", "a"), ("pb", "parent", "b")]
    assert not [r for r in rows if r[1] == "other"]


@pytest.mark.asyncio
async def test_keys_pointing_at_a_table_are_read_from_its_side(keyed):
    assert (await keyed.execute(fk._PG_INBOUND, ["s", "parent"])).rows == [
        ("child", "pa", "a"),
        ("child", "pb", "b"),
    ]
    assert (await keyed.execute(fk._PG_INBOUND, ["s", "ref"])).rows == [("child", "r", "id")]
    assert (await keyed.execute(fk._PG_INBOUND, ["s", "other"])).rows == []


@pytest.mark.asyncio
async def test_registering_either_table_finds_the_same_keys(keyed):
    from_child = await fk._pg_fks(keyed, "s", "child")
    from_parent = await fk._pg_fks(keyed, "s", "parent")
    pair = {"fk_table": "child", "fk_column": "pa", "ref_table": "parent", "ref_column": "a"}
    assert pair in from_child and pair in from_parent


def test_the_query_asks_only_for_views_every_postgres_wire_source_serves():
    for sql in (fk._PG_OUTBOUND, fk._PG_INBOUND):
        assert "constraint_column_usage" not in sql
        assert "referential_constraints" in sql and "position_in_unique_constraint" in sql


# -- which driver a source's keys are read through ----------------------------------------------


def _pools(**drivers):
    return SimpleNamespace(has=lambda sid: sid in drivers, get=lambda sid: drivers[sid])


@pytest.mark.asyncio
async def test_a_postgres_source_is_read_through_its_pooled_driver(keyed):
    driver, close = await fk._constraint_driver(_pools(pg=keyed), "postgresql", "pg")
    assert driver is keyed and close is None  # the pool's own: never closed here


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stype", ["files", "sharepoint", "splunk", "salesforce", "cloudops", "govdata"]
)
async def test_a_pgwire_adapter_source_is_read_through_its_servers_endpoint(
    monkeypatch, keyed, stype
):
    from provisa.federation import pgwire_replica as pr

    dialled: list[tuple] = []

    class _Driver:
        async def connect(self, host, port, database, user, password):
            dialled.append((host, port, database, user, password))

        execute = keyed.execute
        close = keyed.close

    async def _source(source_id):
        return SimpleNamespace(id=source_id, type=stype, username="aa-key")

    monkeypatch.setattr("provisa.api.admin.schema_query._source_for_introspection", _source)
    monkeypatch.setattr(
        pr, "ensure_endpoint_for_discovery", lambda source: pr.PortPair(5440, "127.0.0.1", 5540)
    )
    monkeypatch.setattr("provisa.executor.drivers.pgwire_server.PgwireServerDriver", _Driver)

    found = await fk.introspect_fk_constraints(_pools(), stype, "src", "s", "child")

    # The govdata server signs the connection in with the source's key; the others check none.
    password = "aa-key" if stype == "govdata" else ""
    assert dialled == [("127.0.0.1", 5440, "provisa", "provisa", password)]
    assert {"column_name": "r", "referenced_table": "ref", "referenced_column": "id"} in found
    assert keyed.closed  # a connection of its own, closed when the keys are read


@pytest.mark.asyncio
@pytest.mark.parametrize("stype", ["mysql", "mariadb", "openapi", "mongodb"])
async def test_a_source_that_is_not_postgres_wire_is_not_read_by_the_postgres_query(stype, keyed):
    assert await fk._constraint_driver(_pools(src=keyed), stype, "src") == (None, None)
    assert await fk.introspect_fk_constraints(_pools(src=keyed), stype, "src", "s", "child") == []


@pytest.mark.asyncio
async def test_a_starting_adapter_yields_no_candidates_and_is_not_an_error(monkeypatch):
    from provisa.federation import pgwire_replica as pr

    async def _source(source_id):
        return SimpleNamespace(id=source_id)

    def _starting(source):
        raise pr.SourceStillStartingError(source.id)

    monkeypatch.setattr("provisa.api.admin.schema_query._source_for_introspection", _source)
    monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", _starting)
    assert await fk.introspect_fk_constraints(_pools(), "govdata", "src", "sec", "t") == []


@pytest.mark.asyncio
async def test_tracking_a_starting_adapters_keys_tracks_none_and_any_other_failure_raises(
    monkeypatch,
):
    """Only a bundled server not yet listening is expected; a catalog that fails to answer is
    never taken for a table with no foreign keys."""
    from provisa.federation import pgwire_replica as pr

    async def _source(source_id):
        return SimpleNamespace(id=source_id)

    def _starting(source):
        raise pr.SourceStillStartingError(source.id)

    monkeypatch.setattr("provisa.api.admin.schema_query._source_for_introspection", _source)
    monkeypatch.setattr(pr, "ensure_endpoint_for_discovery", _starting)
    assert await fk.auto_register_fk_relationships(_pools(), "govdata", "src", "s", "t", None) == 0

    class _Broken:
        async def execute(self, sql, params=None):
            raise RuntimeError("catalog unreachable")

    with pytest.raises(RuntimeError, match="catalog unreachable"):
        await fk.auto_register_fk_relationships(
            _pools(pg=_Broken()), "postgresql", "pg", "s", "t", None
        )
