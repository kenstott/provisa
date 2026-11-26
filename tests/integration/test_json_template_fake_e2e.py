# Copyright (c) 2026 Kenneth Stott
# Canary: 0e7c4b92-5a1f-4d38-9b6e-3f2a8c1d7e45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2279 scenario: a JSON column contact with the template fake
{"email": email(), "phone": phone_number()}, read in the fake environment on DuckDB and on Trino,
gives each row an object with an email-shaped email and a phone-shaped phone, and the same row
the same object on both engines. The faked read is the governed statement's own projection
(provisa.fakes.projection), transpiled for each engine as the server sends it."""

from __future__ import annotations

import json
import os
import re
import secrets
from pathlib import Path

import duckdb
import pytest
import sqlglot.expressions as exp

from provisa.fakes import digest as digest_mod
from provisa.fakes.duckdb_functions import register
from provisa.fakes.kinds import parse
from provisa.fakes.portable import CURRENT_VERSION
from provisa.fakes.projection import faked_projection
from provisa.fakes.read_sql import Column
from provisa.transpiler.transpile import transpile_to_duckdb, transpile_to_trino

pytestmark = [pytest.mark.requires_trino, pytest.mark.integration]

CONTACT = '{"email": email(), "phone": phone_number()}'

#: Each row's real contact, as JSON text both engines write alike (keys sorted, no spaces), so
#: each digests the same text.
_REAL = {
    1: '{"email":"ann@example.com","phone":"555-0101"}',
    2: '{"email":"bob@example.com","phone":"555-0102"}',
    3: '{"email":"ann@example.com","phone":"555-0101"}',
    4: None,
}


@pytest.fixture
def key():
    """One platform key, held by the embedded engine and written to the Trino engine's key
    directory, as a Provisa sharing the engine writes its own."""
    value = secrets.token_bytes(32)
    path = Path(os.environ["PROVISA_FAKE_KEY_DIR"]) / f"{digest_mod.fingerprint(value)}.key"
    path.write_text(value.hex())
    previous = digest_mod._key
    digest_mod.set_key(value)
    yield value
    digest_mod._key = previous
    path.unlink()


def _governed(key: bytes) -> str:
    col = Column("contact", "json", "other", parse(CONTACT), True, None, CURRENT_VERSION)
    projection = faked_projection(
        exp.table_("people"),
        "t",
        [("id", "integer", "integer"), ("contact", "json", "other")],
        {"contact": col},
        digest_mod.fingerprint(key),
        None,
    )
    return exp.select("*").from_(projection.subquery("p")).sql(dialect="postgres")


def _people(parse_json: str, real: dict[int, str | None] = _REAL) -> str:
    rows = ", ".join(
        f"({i}, {'CAST(NULL AS JSON)' if v is None else parse_json.format(v.replace(chr(39), 2 * chr(39)))})"
        for i, v in real.items()
    )
    return f"WITH people(id, contact) AS (VALUES {rows}) "


def _objects(rows) -> dict[int, object]:
    return {int(i): None if v is None else json.loads(v) for i, v in rows}


def test_the_contact_template_gives_the_same_object_per_row_on_duckdb_and_trino(trino_conn, key):
    governed = _governed(key)

    con = duckdb.connect()
    register(con)
    duck_sql = _people("CAST('{}' AS JSON)") + transpile_to_duckdb(governed, generated=True)
    on_duckdb = _objects(con.execute(duck_sql).fetchall())
    con.close()

    cur = trino_conn.cursor()
    cur.execute(_people("JSON_PARSE('{}')") + transpile_to_trino(governed, generated=True))
    on_trino = _objects(cur.fetchall())

    assert on_duckdb == on_trino
    assert on_duckdb[4] is None
    for i in (1, 2, 3):
        obj = on_duckdb[i]
        assert isinstance(obj, dict) and list(obj) == ["email", "phone"]
        assert re.fullmatch(r"[^@\s]+@[^@\s]+\.[a-z]+", obj["email"]), obj
        assert re.search(r"\d{3}", obj["phone"]), obj
        assert obj["email"] != json.loads(str(_REAL[i]))["email"]
    assert on_duckdb[1] == on_duckdb[3] and on_duckdb[1] != on_duckdb[2]


#: One contact stored three ways, and another: spaced, and keys reordered, the stored text differs
#: by engine and by writer, and each engine digests its canonical text (REQ-2279).
_WRITTEN = {
    1: '{"email":"ann@example.com","phone":"555-0101"}',
    2: '{ "email" : "ann@example.com",\n  "phone" : "555-0101" }',
    3: '{"phone": "555-0101", "email": "ann@example.com"}',
    4: '{"phone":"555-0102","email":"bob@example.com"}',
}


@pytest.mark.parametrize("stable", [True, False], ids=["stable", "unstable"])
def test_spaced_or_reordered_json_gives_one_object_on_duckdb_and_trino(trino_conn, key, stable):
    col = Column("contact", "json", "other", parse(CONTACT), stable, None, CURRENT_VERSION)
    projection = faked_projection(
        exp.table_("people"),
        "t",
        [("id", "integer", "integer"), ("contact", "json", "other")],
        {"contact": col},
        digest_mod.fingerprint(key),
        None,
    )
    governed = exp.select("*").from_(projection.subquery("p")).sql(dialect="postgres")

    con = duckdb.connect()
    register(con)
    duck_sql = _people("CAST('{}' AS JSON)", _WRITTEN) + transpile_to_duckdb(
        governed, generated=True
    )
    on_duckdb = _objects(con.execute(duck_sql).fetchall())
    con.close()

    cur = trino_conn.cursor()
    cur.execute(
        _people("JSON_PARSE('{}')", _WRITTEN) + transpile_to_trino(governed, generated=True)
    )
    on_trino = _objects(cur.fetchall())

    assert on_duckdb[1] == on_duckdb[2] == on_duckdb[3] != on_duckdb[4]
    if stable:
        # The stable fake is the portable definition: the same object on both engines.
        assert on_duckdb == on_trino
    assert on_trino[1] == on_trino[2] == on_trino[3] != on_trino[4]
