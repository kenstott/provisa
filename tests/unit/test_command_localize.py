# Copyright (c) 2026 Kenneth Stott
# Canary: 4d1f8a63-2c07-4e59-b3a8-6f0c9d2e5b71
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1159: inline command localization — detect-all, execute-once, typed substitution."""

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from provisa.executor.command_localize import localize_commands

_CMDS = {
    "enrich": {
        "name": "enrich",
        "output_columns": [
            {"name": "id", "type": "integer"},
            {"name": "embedding", "type": "text"},
            {"name": "geo", "type": "text"},
        ],
    },
    "labels": {
        "name": "labels",
        "output_columns": [{"name": "id", "type": "integer"}, {"name": "label", "type": "text"}],
    },
}


def _runner(results):
    calls = []

    async def run(name, args):
        calls.append((name, args))
        return results[name]

    return run, calls


def _run(sql, results, **kw):
    run, calls = _runner(results)
    tree = sqlglot.parse_one(sql, read="postgres")
    import asyncio

    hit = asyncio.run(localize_commands(tree, _CMDS, run, dialect="duckdb", **kw))
    return hit, tree.sql(dialect="duckdb"), calls


def test_no_command_no_localization():
    hit, out, calls = _run("SELECT id FROM orders", {})
    assert hit is False and calls == []


def test_join_command_localized_and_executable():
    rows = {"enrich": [{"id": 1, "embedding": "[0.1]", "geo": "x"}, {"id": 2, "embedding": "[0.2]", "geo": ""}]}
    hit, out, calls = _run(
        "SELECT o.n, e.geo FROM orders o JOIN enrich('main.public.orders') e ON o.id = e.id", rows
    )
    assert hit is True
    assert calls == [("enrich", {"a0": "main.public.orders"})]
    assert "VALUES" in out and "AS e(id, embedding, geo)" in out
    # the enrich half executes standalone in duckdb (proves the substituted relation is valid SQL)
    sub = sqlglot.parse_one("SELECT e.id, e.geo FROM enrich('x') e", read="postgres")
    import asyncio

    asyncio.run(localize_commands(sub, _CMDS, _runner(rows)[0], dialect="duckdb"))
    assert duckdb.sql(sub.sql(dialect="duckdb")).fetchall() == [(1, "x"), (2, "")]


def test_detect_all_multiple_commands():
    rows = {"enrich": [{"id": 1, "embedding": "[0]", "geo": "x"}], "labels": [{"id": 1, "label": "vip"}]}
    hit, out, calls = _run(
        "SELECT * FROM enrich('a') e JOIN labels('b') l ON e.id = l.id", rows
    )
    assert hit is True
    assert {c[0] for c in calls} == {"enrich", "labels"}
    assert out.count("VALUES") == 2


def test_repeated_command_executes_once():
    rows = {"enrich": [{"id": 1, "embedding": "[0]", "geo": "x"}]}
    hit, out, calls = _run(
        "SELECT * FROM enrich('a') e1 JOIN enrich('a') e2 ON e1.id = e2.id", rows
    )
    assert hit is True
    assert calls == [("enrich", {"a0": "a"})]  # executed once despite two call sites
    assert out.count("VALUES") == 2  # but substituted at both sites


def test_empty_result_yields_empty_typed_relation():
    hit, out, calls = _run("SELECT e.id FROM enrich('a') e", {"enrich": []})
    assert hit is True
    assert "WHERE FALSE" in out
    assert duckdb.sql(out).fetchall() == []


def test_unaliased_command_fails_loud():
    with pytest.raises(ValueError, match="must carry an alias"):
        _run("SELECT * FROM enrich('a')", {"enrich": [{"id": 1, "embedding": "x", "geo": "y"}]})


def test_types_pinned_from_contract():
    rows = {"enrich": [{"id": 1, "embedding": "[0.1]", "geo": "x"}]}
    hit, out, calls = _run("SELECT e.id FROM enrich('a') e", rows)
    # first row casts to the declared IR types' physical form (duckdb: INT / TEXT)
    assert "CAST(1 AS INT)" in out and "CAST('[0.1]' AS TEXT)" in out


def test_large_result_uses_registrar_when_over_threshold():
    rows = {"enrich": [{"id": i, "embedding": "e", "geo": "g"} for i in range(5)]}
    registered = {}

    async def registrar(name, rws, cols):
        registered["call"] = (name, len(rws), cols)
        return "tmp_enrich_rel"

    run, calls = _runner(rows)
    tree = sqlglot.parse_one("SELECT e.id FROM enrich('a') e", read="postgres")
    import asyncio

    hit = asyncio.run(
        localize_commands(
            tree, _CMDS, run, dialect="duckdb", values_max_rows=2, register_relation=registrar
        )
    )
    assert hit is True
    assert registered["call"] == ("enrich", 5, ["id", "embedding", "geo"])
    assert "tmp_enrich_rel" in tree.sql(dialect="duckdb")
    assert "VALUES" not in tree.sql(dialect="duckdb")
