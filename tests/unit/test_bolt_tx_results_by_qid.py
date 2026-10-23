# Copyright (c) 2026 Kenneth Stott
# Canary: 306e4c44-d9f1-4df4-8d89-639545df66a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An explicit Bolt transaction's results are each pulled by the qid its RUN gave (Bolt 4+).

A driver holds a transaction's results at once: it RUNs the next before the last's records are
all pulled, then PULLs each by its qid. Each pull gets its own result's records, never the latest
RUN's; a result left unpulled at COMMIT, ROLLBACK or RESET ends its request (REQ-1905)."""

# Requirements: REQ-802, REQ-1905

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

import provisa.bolt.messages as msg
from provisa.core import request_deadline

pytestmark = pytest.mark.asyncio

_RESULTS = {"first": [[1], [2], [3]], "second": [[10], [20]]}


def _session():
    from tests.unit.test_bolt_show_databases import _make_session

    return _make_session(["analyst"])


async def _run(session, cypher: str) -> None:
    async def execute(statement, *_args, **_kwargs):
        return ["n"], [list(r) for r in _RESULTS[statement]], None

    with (
        patch.object(session, "_resolve_db", return_value=("analyst", False)),
        patch("provisa.bolt.session._execute_cypher", execute),
    ):
        await session.handle_run([cypher, {}, {}])


async def _said(writer, start: int = 0) -> list[tuple[int, list]]:
    from provisa.bolt.framing import read_message
    from provisa.bolt.packstream import unpack_fields

    reader = asyncio.StreamReader()
    reader.feed_data(writer.getvalue())
    reader.feed_eof()
    out = []
    while not reader.at_eof():
        data = await read_message(reader)
        out.append((data[1], unpack_fields(data[2:])))
    return out[start:]


async def test_each_pull_by_qid_gets_its_own_results_records():
    session, writer = _session()
    session.handle_begin([{}])
    await _run(session, "first")
    session.handle_pull([{"n": 1}])
    await _run(session, "second")  # the first is still open: two records unpulled
    session.handle_pull([{"n": -1, "qid": 0}])
    session.handle_pull([{"n": -1}])  # -1: the last RUN's
    session.handle_commit()

    said = await _said(writer, start=1)  # after BEGIN's SUCCESS
    runs = [f[0] for tag, f in said if tag == msg.SUCCESS and "fields" in f[0]]
    assert [r["qid"] for r in runs] == [0, 1]
    records = [f[0] for tag, f in said if tag == msg.RECORD]
    assert records == [[1], [2], [3], [10], [20]]
    summaries = [f[0]["has_more"] for tag, f in said if tag == msg.SUCCESS and "has_more" in f[0]]
    assert summaries == [True, False, False]
    assert said[-1] == (msg.SUCCESS, [{}])  # COMMIT
    assert request_deadline.expire_all("the test is over") == 0


async def test_a_result_left_open_ends_with_its_transaction():
    for end in ("handle_commit", "handle_rollback", "handle_reset"):
        session, _writer = _session()
        session.handle_begin([{}])
        await _run(session, "first")
        session.handle_pull([{"n": 1}])
        await _run(session, "second")
        getattr(session, end)()
        assert request_deadline.expire_all("the test is over") == 0, end


async def test_a_qid_no_open_result_has_is_refused():
    session, writer = _session()
    session.handle_begin([{}])
    await _run(session, "first")
    session.handle_pull([{"n": -1, "qid": 7}])
    tag, fields = (await _said(writer))[-1]
    assert tag == msg.FAILURE
    assert fields[0]["code"] == "Neo.ClientError.Request.Invalid"
    assert "qid 7" in fields[0]["message"]


async def test_an_auto_commit_run_carries_no_qid():
    session, writer = _session()
    await _run(session, "first")
    session.handle_pull([{"n": -1}])
    run = (await _said(writer))[0][1][0]
    assert "qid" not in run
    assert request_deadline.expire_all("the test is over") == 0
