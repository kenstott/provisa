# Copyright (c) 2026 Kenneth Stott
# Canary: e63baf87-13f1-4f39-8d34-be1ccf9df3e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-318: column names in the API replica cache are data, so every one is a quoted identifier.

A column is named after an OpenAPI property or a response key. Whatever characters that name
holds, it stays one identifier in the statement: the table gets a column of exactly that name
and nothing else in the statement changes meaning.
"""

import pytest

from provisa.core.database import Database, _is_multi_statement, create_engine_from_url
from provisa.openapi.pg_cache import _ident, _insert_rows, _relation, cache_openapi_table

ODD_NAME = 'a"; DROP TABLE keep_me; --'


@pytest.fixture
async def sqlite_conn(tmp_path):
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}")
    db = Database(engine, "test")
    async with db.acquire() as conn:
        yield conn
    engine.dispose()


async def test_column_name_with_quote_and_semicolon_stays_one_identifier(sqlite_conn):
    # The embedded quote is doubled, so the identifier ends only at its own closing quote.
    assert _ident(ODD_NAME) == '"a""; DROP TABLE keep_me; --"'

    # A ``;`` inside a quoted identifier does not make the statement a script to be split.
    assert _is_multi_statement(f"CREATE TABLE t ({_ident(ODD_NAME)} TEXT)") is False
    assert _is_multi_statement('CREATE TABLE "a" (n INT); CREATE TABLE "b" (n INT)') is True

    await sqlite_conn.execute('CREATE TABLE "keep_me" ("n" INTEGER)')
    await sqlite_conn.execute('INSERT INTO "keep_me" ("n") VALUES (1)')

    # DDL: the property name becomes a column of exactly that name.
    schema = {"type": "object", "properties": {"id": {"type": "integer"}, ODD_NAME: {}}}
    await cache_openapi_table(
        "http://unused.invalid", "/pet/{petId}", {}, sqlite_conn, "default", "pets", schema
    )
    # DML: the same name as a response key, its value a bound parameter.
    inserted = await _insert_rows(
        sqlite_conn, "default", "pets", ["id", ODD_NAME], [{"id": 7, ODD_NAME: "v"}], "hash"
    )
    assert inserted == 1

    rel = _relation(sqlite_conn, "default", "pets")
    rows = await sqlite_conn.fetch(f"SELECT * FROM {rel}")
    assert [dict(r) for r in rows][0][ODD_NAME] == "v"
    assert set(dict(rows[0])) == {"id", ODD_NAME, "_params_hash", "_cached_at"}
    # The other table is untouched: the name never ended the identifier.
    assert await sqlite_conn.fetchval('SELECT COUNT(*) FROM "keep_me"') == 1
