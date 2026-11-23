# Copyright (c) 2026 Kenneth Stott
# Canary: 8b3e5f1a-2c74-4d09-9e6b-1f0a7c3d5e92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1961 (amended 2026-10-28, PGWIRE SENDS NUMERIC(P,S); VARCHAR AND CHAR KEEP THEIR TYPE):
a text column whose source type was varchar, varchar(n), char(n) or bpchar is described on pgwire
with that native type's OID and length typmod (1043 or 1042), and the catalog's atttypmod says
the same; a registered numeric(p,s) is described with its typmod, and the catalog agrees.
Registration keeping the native type is tests/unit/test_character_native_type.py."""

# Requirements: REQ-1961

from __future__ import annotations

import duckdb
import pytest
from buenavista.core import BVType

from provisa.compiler.result_shape import registered_type
from provisa.pgwire.cast_types import registered_spelling
from provisa.pgwire.catalog_populate import CatalogIndex, _populate_pg_attribute
from provisa.pgwire.server import _describe_registered

# (registered type, native type, OID pgwire describes, typmod pgwire describes)
_CASES = [
    ("text", "varchar(20)", 1043, 24),
    ("text", "character varying(20)", 1043, 24),
    ("text", "varchar", 1043, -1),
    ("text", "character(3)", 1042, 7),
    ("text", "char(3)", 1042, 7),
    ("text", "bpchar", 1042, -1),
    ("numeric(18,2)", None, 1700, (18 << 16 | 2) + 4),
]


def _described(registered: str, native: str | None) -> tuple[int | None, int]:
    """The (OID, typmod) pgwire describes a result column projecting the registered column as,
    given the wire type its value arrives in."""
    shape: list[tuple[str, str]] = [("c", registered_type(registered, native))]
    types = [BVType.DECIMAL if registered.startswith("numeric") else BVType.TEXT]
    oids: list[int | None] = [None]
    typmods = [-1]
    _describe_registered(shape, types, oids, typmods, [])
    return oids[0], typmods[0]


@pytest.mark.parametrize(("registered", "native", "oid", "typmod"), _CASES)
def test_pgwire_describes_a_registered_column_by_its_native_type_and_length(
    registered, native, oid, typmod
):
    assert _described(registered, native) == (oid, typmod)


def test_a_text_column_with_no_native_character_type_is_described_as_text():
    assert _described("text", None) == (None, -1)


def _catalog_attribute(listed_type: str) -> tuple[int, int]:
    db = duckdb.connect()
    idx = CatalogIndex("provisa")
    idx.all_cols.append((90001, "c", listed_type, True, 1))
    _populate_pg_attribute(db, idx)
    ((atttypid, atttypmod),) = db.execute(
        "SELECT atttypid, atttypmod FROM _pg_attribute WHERE attrelid = 90001"
    ).fetchall()
    return atttypid, atttypmod


@pytest.mark.parametrize(("registered", "native", "oid", "typmod"), _CASES)
def test_the_catalogs_atttypid_and_atttypmod_agree_with_the_wire(registered, native, oid, typmod):
    assert _catalog_attribute(registered_spelling(registered, native)) == (oid, typmod)
