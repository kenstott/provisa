# Copyright (c) 2026 Kenneth Stott
# Canary: 2e7b4d91-5a3c-4f06-9c8d-7b1e6a20f4c5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1651: a SQLite file source's PRIMARY KEY comes from the file's own PRAGMA, in key order."""

import sqlite3

import pytest

from provisa.federation.connector_sqlite import primary_key_columns

pytestmark = pytest.mark.unit


def test_primary_key_columns_in_key_order(tmp_path):
    path = str(tmp_path / "pets.sqlite")
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE pets (id INTEGER PRIMARY KEY, name TEXT)")
    con.execute(
        "CREATE TABLE visits (vet_id INTEGER, pet_id INTEGER, note TEXT, PRIMARY KEY (pet_id, vet_id))"
    )
    con.execute("CREATE TABLE notes (body TEXT)")
    con.commit()
    con.close()
    assert primary_key_columns(path, "pets") == ["id"]
    assert primary_key_columns(path, "visits") == ["pet_id", "vet_id"]
    assert primary_key_columns(path, "notes") == []
