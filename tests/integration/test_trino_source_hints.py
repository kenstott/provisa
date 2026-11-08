# Copyright (c) 2026 Kenneth Stott
# Canary: 7e1c2f94-3a6d-4b08-a5c1-d29e60f7b318
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-281: on a real Trino, only the @provisa vocabulary reaches the session.

The hints dict also carries each connector's settings. Bench P2: bench-mongodb's
``direct_connection`` went out as SET SESSION and Trino answered INVALID_SESSION_PROPERTY, failing
every statement over the source. A raw session property stays on the source too; only a real plan
shows the vocabulary changing what Trino does.
"""

# Requirements: REQ-281

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
import trino

from provisa.executor.trino import execute_trino
from provisa.pgwire._pipeline import _engine_session_hints

pytestmark = [pytest.mark.requires_trino, pytest.mark.integration]

_JOIN = (
    "EXPLAIN SELECT a.node_id FROM system.runtime.nodes a "
    "JOIN system.runtime.nodes b ON a.node_id = b.node_id"
)


@pytest.fixture
def state():
    kwargs = dict(
        host="localhost",
        port=int(os.environ["TRINO_PORT"]),
        user="itest",
        catalog="system",
        schema="runtime",
        http_scheme="http",
    )
    return SimpleNamespace(
        engine_conn=trino.dbapi.connect(**kwargs),
        engine_conn_kwargs=kwargs,
        engine_session_hints={},
        source_types={"mongo": "mongodb"},
    )


def _run(state, hints: dict[str, str], sql: str = "SELECT 1"):
    state.source_federation_hints = {"mongo": hints}
    plan_hints = _engine_session_hints(state, ("mongo",), None, sql, writes=False)
    result = execute_trino(state.engine_conn, sql, session_hints=plan_hints)
    return plan_hints or {}, result


def test_a_statement_over_a_source_with_a_connector_setting_answers(state):
    hints, result = _run(state, {"direct_connection": "true"})
    assert result.rows == [[1]]
    assert hints == {}


@pytest.mark.parametrize(
    "join, distribution", [("broadcast", "REPLICATED"), ("partitioned", "PARTITIONED")]
)
def test_a_vocabulary_hint_takes_effect_and_a_raw_key_is_not_sent(state, join, distribution):
    hints, result = _run(
        state,
        {"join": join, "query_max_run_time": "3h", "direct_connection": "true"},
        _JOIN,
    )
    assert hints == {"join_distribution_type": join.upper()}
    assert f"distribution = {distribution}" in result.rows[0][0]
    cur = state.engine_conn.cursor()
    cur.execute("SHOW SESSION LIKE 'query_max_run_time'")
    _, value, default, *_ = cur.fetchall()[0]
    assert value == default != "3h"
