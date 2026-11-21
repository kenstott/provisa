# Copyright (c) 2026 Kenneth Stott
# Canary: c1050f2f-7fd1-451c-a165-1b86d1bbf350
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a Cypher read's list functions on the pg engine (REQ-345, REQ-347). The Cypher read
names them as Trino does (element_at, slice, transform, any_match / all_match / none_match,
reduce, range_step, type_refused, json_kind, json_size, json_order_key, json_list_compare);
PostgreSQL has none of them, nor lambdas, and refused each ("function element_at(integer[],
integer) does not exist"). On the pg engine each answers as openCypher does: an index 0-based and
negative from the end, a slice's end excluded, a quantifier three-valued, a computed step of 0
and a conversion of a kind it does not take refused."""

# Requirements: REQ-345, REQ-347, REQ-2176

from __future__ import annotations

import json

import httpx
import pytest
import pytest_asyncio

from tests.integration.test_cypher_range_engines import _ROLE, _cypher, _pg_server, _rows

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]


@pytest_asyncio.fixture(scope="module")
async def server(tmp_path_factory):
    srv, owned = _pg_server("cypher_lists_pg", tmp_path_factory)
    try:
        srv.start()
        yield srv
    finally:
        srv.stop_process()
        for pg in owned:
            pg.stop()


def _refusal(srv, statement: str) -> str:
    """The text a statement the engine refuses is answered with."""
    resp = httpx.post(
        f"{srv.base_url}/data/cypher",
        json={"query": statement},
        headers={"x-provisa-role": _ROLE},
        timeout=srv.request_timeout + 30,
    )
    assert resp.status_code != 200, resp.text
    return resp.text


def _value(v):
    """An item of a list of several types as its value, whether answered as JSON text or not."""
    if isinstance(v, str):
        try:
            return json.loads(v)
        except ValueError:
            return v
    return v


def test_an_index_reads_from_either_end(server):
    answer = _cypher(
        server,
        "WITH [1, 2, 3] AS l, 1 AS k RETURN l[0] AS a, l[-1] AS b, l[k] AS c, l[-k] AS d, "
        "head(l) AS h, last(l) AS z, l[5] AS o",
    )
    assert _rows(answer) == [(1, 3, 2, 3, 1, 3, None)]


def test_a_slice_excludes_its_end(server):
    answer = _cypher(
        server,
        "WITH [1, 2, 3, 4] AS l RETURN l[1..3] AS a, l[..2] AS b, l[2..] AS c, "
        "l[-3..-1] AS d, tail(l) AS t",
    )
    assert _rows(answer) == [([2, 3], [1, 2], [3, 4], [2, 3], [2, 3, 4])]


def test_a_comprehension_filters_and_projects_in_order(server):
    answer = _cypher(
        server,
        "WITH [1, 2, 3] AS l RETURN [x IN l WHERE x > 1 | x * 10] AS a, [x IN l | x + 1] AS b, "
        "reverse(l) AS r",
    )
    assert _rows(answer) == [([20, 30], [2, 3, 4], [3, 2, 1])]


def test_quantifiers_are_three_valued(server):
    answer = _cypher(
        server,
        "WITH [1, 2, 3] AS l, [1, null] AS m "
        "RETURN any(x IN l WHERE x > 2) AS a, all(x IN l WHERE x > 0) AS b, "
        "none(x IN l WHERE x > 5) AS c, single(x IN l WHERE x = 2) AS d, "
        "any(x IN m WHERE x > 1) AS e, all(x IN m WHERE x > 0) AS f, "
        "none(x IN m WHERE x > 0) AS g",
    )
    assert _rows(answer) == [(True, True, True, True, None, None, False)]


def test_reduce_folds_the_list(server):
    answer = _cypher(
        server,
        "RETURN reduce(s = 0, x IN [1, 2, 3] | s + x) AS s, "
        "reduce(p = 1, x IN [2, 3, 4] | p * x) AS p",
    )
    assert _rows(answer) == [(6, 24)]


def test_a_computed_negative_step_counts_down_inclusive(server):
    answer = _cypher(server, "WITH -3 AS s UNWIND range(10, 1, s) AS i RETURN i ORDER BY i DESC")
    assert [r[0] for r in _rows(answer)] == [10, 7, 4, 1]


def test_a_computed_step_of_zero_is_refused(server):
    assert "range() takes a step other than 0" in _refusal(
        server, "WITH 0 AS s UNWIND range(1, 3, s) AS i RETURN i"
    )


def test_a_conversion_of_a_kind_it_does_not_take_is_a_type_error(server):
    assert "TypeError" in _refusal(server, "RETURN [x IN [1, 'a', [2]] | toString(x)] AS l")
    answer = _cypher(server, "RETURN [x IN [1, 'a', true] | toString(x)] AS l")
    assert _rows(answer) == [(["1", "a", "true"],)]


def test_lists_of_several_types_compare_element_by_element(server):
    answer = _cypher(
        server,
        "WITH [[1, 'a'], [1, 'b'], 'x'] AS types WITH types[0] AS a, types[1] AS b "
        "RETURN a < b AS r, a > b AS s",
    )
    assert _rows(answer) == [(True, False)]
    incomparable = _cypher(
        server, "WITH [['1'], [1], 'x'] AS types WITH types[0] AS a, types[1] AS b RETURN a < b"
    )
    assert _rows(incomparable) == [(None,)]


def test_values_of_several_types_order_lists_first_and_a_prefix_before_it(server):
    answer = _cypher(
        server, "WITH [[1, 0], [1], [0, 9], 'x'] AS types UNWIND types AS t RETURN t ORDER BY t"
    )
    assert [_value(r[0]) for r in _rows(answer)] == [[0, 9], [1], [1, 0], "x"]


def test_the_size_of_an_item_of_several_types(server):
    answer = _cypher(server, "RETURN [x IN [1, 'ab', [1, 2, 3]] | size(x)] AS l")
    assert _rows(answer) == [([None, 2, 3],)]
