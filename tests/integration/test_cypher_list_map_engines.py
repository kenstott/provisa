# Copyright (c) 2026 Kenneth Stott
# Canary: 9a41d6e3-5c2f-4b87-a0e9-1f7b3c8d2e56
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a Cypher read's lists and maps answer alike on the pg engine, Trino and DuckDB
(REQ-345, REQ-347). On the pg engine a filter of a null list was the empty list; toInteger and
size() of an item of a list of several types, and a quantifier over [], were refused; a computed
step of 0 was refused as an integer's invalid input; a map was refused ("function map(text[],
json[]) does not exist"). On DuckDB size() of such an item counted its JSON text."""

# Requirements: REQ-345, REQ-347

from __future__ import annotations

import json

import httpx
import pytest
import pytest_asyncio

from provisa.transpiler.transpile import RANGE_STEP_ZERO
from tests.integration.test_cypher_range_engines import (
    _ROLE,
    _cypher,
    _pg_server,
    _rows,
    _trino_server,
)

pytestmark = [pytest.mark.integration]


def _duckdb_server(org: str):
    """The DuckDB engine on a SQLite control plane, its store a DuckDB file of its own."""
    import tempfile
    from pathlib import Path

    from tests.integration.isolated_server import IsolatedServer

    store_dir = tempfile.TemporaryDirectory()
    srv = IsolatedServer(
        org,
        engine="duckdb",
        config="tests/fixtures/duckdb_sqlite_config.yaml",
        control_plane="own",
        materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
    )
    return srv, [store_dir.cleanup]


@pytest_asyncio.fixture(
    scope="module",
    params=[
        pytest.param("pg", id="pg", marks=pytest.mark.local_platform),
        pytest.param("trino", id="trino", marks=pytest.mark.requires_trino),
        pytest.param("duckdb", id="duckdb"),
    ],
)
async def server(request, tmp_path_factory):
    from tests.integration.isolated_server import drop_org_schema

    org = f"cypher_lists_maps_{request.param}"
    if request.param == "pg":
        srv, owned = _pg_server(org, tmp_path_factory)
        cleanups = [pg.stop for pg in owned]
    elif request.param == "trino":
        srv, _ = _trino_server(org)
        cleanups = []
    else:
        srv, cleanups = _duckdb_server(org)
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()
        if request.param == "trino":
            await drop_org_schema(org)
        for cleanup in cleanups:
            cleanup()


def _value(v):
    """A JSON value as its value, whether the engine answers it as JSON text or not."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return v
    if isinstance(v, dict):
        return {k: _value(x) for k, x in v.items()}
    return v


def test_a_filter_of_a_null_list_is_null(server):
    answer = _cypher(
        server,
        "WITH [1, 2] AS l WITH CASE WHEN false THEN l END AS n "
        "RETURN [x IN n WHERE x > 1] AS a, [x IN n WHERE x > 1 | x * 10] AS b",
    )
    assert _rows(answer) == [(None, None)]


def test_a_conversion_of_an_item_of_several_types_reads_its_value(server):
    answer = _cypher(
        server,
        "WITH [1, 'a', 2.5, '3'] AS l RETURN toInteger(l[0]) AS a, toFloat(l[2]) AS b, "
        "toInteger(l[3]) AS c, toFloat(l[0]) AS d, toInteger(l[1]) AS e",
    )
    assert _rows(answer) == [(1, 2.5, 3, 1.0, None)]


def test_the_size_and_comparison_of_an_item_of_several_types(server):
    sized = _cypher(
        server, "WITH [1, 'ab', [1, 2, 3]] AS l RETURN size(l[1]) AS a, size(l[2]) AS b"
    )
    assert _rows(sized) == [(2, 3)]
    compared = _cypher(server, "WITH [1, 'ab', 2] AS l RETURN l[0] < l[2] AS c, l[1] = 'ab' AS d")
    assert _rows(compared) == [(True, True)]


def test_a_quantifier_over_an_empty_list(server):
    answer = _cypher(
        server,
        "RETURN all(x IN [] WHERE x > 0) AS a, any(x IN [] WHERE x > 0) AS b, "
        "none(x IN [] WHERE x > 0) AS c, single(x IN [] WHERE x > 0) AS d",
    )
    assert _rows(answer) == [(True, False, True, False)]


def test_a_computed_step_of_zero_is_refused_by_its_own_message(server):
    resp = httpx.post(
        f"{server.base_url}/data/cypher",
        json={"query": "WITH 0 AS s UNWIND range(1, 3, s) AS i RETURN i"},
        headers={"x-provisa-role": _ROLE},
        timeout=server.request_timeout + 30,
    )
    # Each engine's refusal as the router answers it (Trino's through cypher.execution_failed).
    assert resp.status_code != 200, resp.text
    error = resp.json()["params"]["error"]
    assert RANGE_STEP_ZERO in error, error
    # Its own error, never a cast's (invalid input syntax for type integer: "range() ...").
    assert "invalid input syntax" not in error, error


def test_a_map_is_read_by_its_keys(server):
    answer = _cypher(
        server, "WITH {a: 1, b: 'x', c: [1, 2]} AS m RETURN m['a'] AS a, m.b AS b, m AS m"
    )
    ((a, b, m),) = _rows(answer)
    assert (a, b) == (1, "x")
    assert _value(m) == {"a": 1, "b": "x", "c": [1, 2]}
    keyed = _cypher(server, "UNWIND [{a: 1}, {a: 2}] AS m RETURN m['a'] AS a ORDER BY a")
    assert _rows(keyed) == [(1,), (2,)]
    keys = _cypher(server, "WITH {a: 1, b: 'x'} AS m RETURN keys(m) AS ks, size(keys(m)) AS n")
    ((ks, n),) = _rows(keys)
    assert (sorted(ks), n) == (["a", "b"], 2)
