# Copyright (c) 2026 Kenneth Stott
# Canary: f1a7cc48-c3cc-4300-abe9-1b594614dfd4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The join canvas's statement for the maintainer's canvas runs on DuckDB.

provisa-ui/src/components/canvas/__fixtures__/pets-canvas.sql is what the canvas builds for
pet_companions, pets, vets and playgroups with playgroups.id→pets.id, pets.id→vets.id and
pet_companions' two joins into pets (joinGraph.test.ts pins it). Every table it names is joined
before it is referenced, and pets is read twice, once per companion."""

from __future__ import annotations

from pathlib import Path

import duckdb

_SQL = (
    Path(__file__).resolve().parents[2]
    / "provisa-ui/src/components/canvas/__fixtures__/pets-canvas.sql"
)


def test_the_canvas_statement_parses_and_runs_on_duckdb():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA petstore")
    con.execute("CREATE TABLE petstore.pets (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO petstore.pets VALUES (1, 'Rex'), (2, 'Tom')")
    con.execute("CREATE TABLE petstore.vets (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO petstore.vets VALUES (1, 'Dr A'), (2, 'Dr B')")
    con.execute("CREATE TABLE petstore.playgroups (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO petstore.playgroups VALUES (1, 'Park'), (2, 'Yard')")
    con.execute(
        "CREATE TABLE petstore.pet_companions "
        "(pet_id INTEGER, companion_pet_id INTEGER, relation_type VARCHAR)"
    )
    con.execute("INSERT INTO petstore.pet_companions VALUES (1, 2, 'littermate')")
    rows = con.execute(_SQL.read_text()).fetchall()
    columns = [d[0] for d in con.description]
    assert len(rows) == 1
    row = dict(zip(columns, rows[0], strict=False))
    # The pet and its companion, each its own read of pets.
    assert (row["pet_id"], row["companion_pet_id"]) == (1, 2)
    assert {"Rex", "Tom"} <= set(rows[0])


def test_pets_and_their_companions_alone_read_pets_twice_on_duckdb():
    """pets and pet_companions alone: the two approved joins of the junction are a join of
    pet_companions and a second read of pets, for the companion (pets-companions.sql)."""
    con = duckdb.connect()
    con.execute("CREATE SCHEMA petstore")
    con.execute("CREATE TABLE petstore.pets (id INTEGER, name VARCHAR)")
    con.execute("INSERT INTO petstore.pets VALUES (1, 'Rex'), (2, 'Tom')")
    con.execute(
        "CREATE TABLE petstore.pet_companions "
        "(pet_id INTEGER, companion_pet_id INTEGER, relation_type VARCHAR)"
    )
    con.execute("INSERT INTO petstore.pet_companions VALUES (1, 2, 'littermate')")
    sql = (_SQL.parent / "pets-companions.sql").read_text()
    rows = con.execute(sql).fetchall()
    assert len(rows) == 1
    assert {"Rex", "Tom"} <= set(rows[0])
