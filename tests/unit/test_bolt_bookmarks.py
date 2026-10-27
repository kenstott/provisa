# Copyright (c) 2026 Kenneth Stott
# Canary: c91b7489-ccb5-46d3-ba29-c40d16f60170
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Bolt unit of work ends with a bookmark (the Neo4j drivers' causal chaining): a COMMIT's
SUCCESS, and an auto-commit result's last PULL. A write is applied as it runs and a transaction
is a no-op (REQ-883), so every bookmark a client passes back is already met: one it sends with
BEGIN or RUN is accepted and asks for nothing more."""

# Requirements: REQ-802, REQ-883

from __future__ import annotations

import pytest

import provisa.bolt.messages as msg
from tests.unit.test_bolt_tx_results_by_qid import _run, _said, _session

pytestmark = pytest.mark.asyncio


def _bookmarks(said) -> list[str]:
    return [f[0]["bookmark"] for tag, f in said if tag == msg.SUCCESS and "bookmark" in f[0]]


async def test_a_commit_ends_with_a_bookmark():
    session, writer = _session()
    session.handle_begin([{"bookmarks": ["provisa:earlier"]}])
    await _run(session, "first")
    session.handle_pull([{"n": -1}])
    session.handle_commit()
    [bookmark] = _bookmarks(await _said(writer))
    assert bookmark.startswith("provisa:")


async def test_an_auto_commit_result_ends_with_a_bookmark_and_each_is_new():
    session, writer = _session()
    await _run(session, "first")
    session.handle_pull([{"n": 1}])  # more to come: no bookmark yet
    session.handle_pull([{"n": -1}])
    await _run(session, "second")
    session.handle_pull([{"n": -1}])
    first, second = _bookmarks(await _said(writer))
    assert first != second
