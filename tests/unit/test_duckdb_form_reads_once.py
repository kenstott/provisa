# Copyright (c) 2026 Kenneth Stott
# Canary: 7b1f7d0e-6f3a-4c55-9a2e-2f4f0a91c3d7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement's DuckDB form costs what the statement costs (REQ-1989): the JSON forms make the
text several times longer, so a step that reads the written text again pays for all of them, and
a form read once per member pays again for each member. A Cypher relationship read has one member
per property of both ends: 224 of them held a backend's core for the whole of a five-minute test."""

# Requirements: REQ-1989

from __future__ import annotations

from collections.abc import Iterator

import pytest
import sqlglot.dialects.dialect as sqlglot_dialect

from provisa.transpiler.transpile import _duckdb_form

pytestmark = pytest.mark.unit


def _relationship(members: int) -> str:
    """The statement a Cypher ``RETURN r`` compiles to, ``members`` properties in its object."""
    properties = ", ".join(f"'c{i}', a.\"c{i}\"" for i in range(members))
    return (
        f"SELECT JSON_OBJECT('type', 'X', 'properties', JSON_OBJECT({properties})) AS r "
        'FROM "pg"."public"."a" AS a'
    )


@pytest.fixture
def read_texts(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    texts: list[str] = []
    read = sqlglot_dialect.Dialect.parse

    def counted(self, sql, **options):
        texts.append(sql)
        return read(self, sql, **options)

    monkeypatch.setattr(sqlglot_dialect.Dialect, "parse", counted)
    yield texts


def test_the_written_form_is_never_read_again(read_texts: list[str]) -> None:
    written = _duckdb_form.__wrapped__(_relationship(40))
    assert max(len(text) for text in read_texts) < len(written) / 2


def test_a_member_adds_no_reading(read_texts: list[str]) -> None:
    _duckdb_form.__wrapped__(_relationship(1))  # the form each member takes is read here
    read_texts.clear()
    _duckdb_form.__wrapped__(_relationship(10))
    ten = len(read_texts)
    read_texts.clear()
    _duckdb_form.__wrapped__(_relationship(40))
    assert len(read_texts) == ten


def test_the_form_keeps_each_members_own_value() -> None:
    written = _duckdb_form.__wrapped__(_relationship(3))
    for column in ("c0", "c1", "c2"):
        assert f'TO_JSON(a."{column}")' in written, written


def test_a_length_the_list_step_writes_is_told_apart_like_any_other() -> None:
    # cardinality is written as len, by name; the step after it tells a list from a text and a
    # blob only on the typed node DuckDB's parser reads that name as.
    assert "TYPEOF" in _duckdb_form.__wrapped__("SELECT cardinality(x) AS n FROM t")
