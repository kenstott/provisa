# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7c9e4a-6d1f-4c8b-a3e5-0f9d8c7b6a51
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt result's summary says what its RUN wrote (REQ-670, REQ-2176): the last PULL's or
DISCARD's SUCCESS carries ``stats``, only its counters that are not zero and
``contains-updates``, and none when nothing changed. A count that could not be made is no
stats, and a notification with the RUN's SUCCESS."""

# Requirements: REQ-670, REQ-2176

from __future__ import annotations

from unittest.mock import patch

import pytest
from neo4j._work.summary import SummaryCounters

import provisa.bolt.messages as msg
from provisa.cypher import counters
from tests.unit.test_bolt_tx_results_by_qid import _said, _session

pytestmark = pytest.mark.asyncio

#: What each statement does when it runs: its rows, and what it counts.
_DOES = {
    "write": ([], {"nodes_created": 2, "labels_added": 2, "properties_set": 3}),
    "read": ([[1]], {}),
    "other write": ([], {"relationships_deleted": 1}),
    "write returning": ([[1], [2]], {"nodes_created": 2}),
}


async def _run(session, cypher: str) -> None:
    async def execute(statement, *_args, **_kwargs):
        if statement == "uncounted":
            counters.unknown("the source reported no row count for INSERT")
            return [], [], None
        rows, counted = _DOES[statement]
        counters.count(**counted)
        return ["n"] if rows else [], rows, None

    with (
        patch.object(session, "_resolve_db", return_value=("analyst", False)),
        patch("provisa.bolt.session._execute_cypher", execute),
    ):
        await session.handle_run([cypher, {}, {}])


def _summaries(said) -> list[dict]:
    return [f[0] for tag, f in said if tag == msg.SUCCESS and "has_more" in f[0]]


async def test_a_writes_last_pull_says_what_it_wrote_as_the_driver_reads_it():
    session, writer = _session()
    await _run(session, "write")
    session.handle_pull([{"n": -1}])
    [summary] = _summaries(await _said(writer))
    assert summary["stats"] == {
        "nodes-created": 2,
        "labels-added": 2,
        "properties-set": 3,
        "contains-updates": True,
    }
    read = SummaryCounters(summary["stats"])
    assert (read.nodes_created, read.labels_added, read.properties_set) == (2, 2, 3)
    assert read.contains_updates is True
    assert read.relationships_created == 0


async def test_a_read_has_no_stats():
    session, writer = _session()
    await _run(session, "read")
    session.handle_pull([{"n": -1}])
    [summary] = _summaries(await _said(writer))
    assert "stats" not in summary


async def test_only_the_last_pull_of_a_result_carries_its_stats():
    session, writer = _session()
    await _run(session, "write returning")
    session.handle_pull([{"n": 1}])
    session.handle_pull([{"n": -1}])
    first, last = _summaries(await _said(writer))
    assert "stats" not in first and last["stats"]["nodes-created"] == 2


async def test_a_discarded_write_says_what_it_wrote():
    session, writer = _session()
    await _run(session, "write")
    session.handle_discard([{"n": -1}])
    [summary] = _summaries(await _said(writer))
    assert summary["stats"]["properties-set"] == 3


async def test_each_result_of_a_transaction_keeps_its_own_stats():
    session, writer = _session()
    session.handle_begin([{}])
    await _run(session, "write")
    await _run(session, "other write")  # the first is set aside, unpulled
    session.handle_pull([{"n": -1, "qid": 0}])
    session.handle_pull([{"n": -1, "qid": 1}])
    first, second = _summaries(await _said(writer, start=1))
    assert first["stats"]["nodes-created"] == 2
    assert second["stats"] == {"relationships-deleted": 1, "contains-updates": True}


async def test_a_count_that_could_not_be_made_is_no_stats_and_a_notification():
    session, writer = _session()
    await _run(session, "uncounted")
    session.handle_pull([{"n": -1}])
    said = await _said(writer)
    [run] = [f[0] for tag, f in said if tag == msg.SUCCESS and "fields" in f[0]]
    [note] = run["notifications"]
    assert note["code"] == "Provisa.cypher.count_unknown"
    assert "the source reported no row count for INSERT" in note["description"]
    [summary] = _summaries(said)
    assert "stats" not in summary


async def test_bolts_governed_write_counts_the_rows_the_source_reports():
    from provisa.bolt.session import _execute_write_cypher
    from provisa.executor.result import QueryResult
    from tests.unit.test_cypher_write_return_and_junction import _map

    sent: list[str] = []

    async def govern(sql, *_args, **_kwargs):
        sent.append(sql)
        return object()

    async def execute(_plan):
        return QueryResult(rows=[], column_names=[], rowcount=2)

    with (
        patch("provisa.bolt.session._bolt_label_map", return_value=_map()),
        patch("provisa.pgwire._pipeline._govern_and_route_compiled", govern),
        patch("provisa.pgwire._pipeline._execute_plan", execute),
        counters.counting() as counted,
    ):
        await _execute_write_cypher(
            "MATCH (n:Person) WHERE n.name = 'Ann' SET n.name = 'Ann', n.id = 9",
            {},
            "analyst",
            None,
            False,
            None,
        )
    assert sent and sent[0].startswith("UPDATE")
    assert counted.stats() == {"properties-set": 4, "contains-updates": True}
