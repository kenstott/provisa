# Copyright (c) 2026 Kenneth Stott
# Canary: 7e1c2f94-3a6d-4b08-a5c1-d29e60f7b318
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-281: on a real Trino, a source's federation hints reach the session only as session
properties.

The hints dict also carries each connector's settings. Bench P2: bench-mongodb's
``direct_connection`` went out as SET SESSION and Trino answered INVALID_SESSION_PROPERTY, failing
every statement over the source. ``SHOW SESSION`` is the list the plan reads, and its row shape is
what no fake can prove.
"""

# Requirements: REQ-281

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import trino

from provisa.executor.trino import execute_trino
from provisa.federation.backend import TrinoBackend
from provisa.federation.runtime import EngineRuntime
from provisa.pgwire._pipeline import _defined_session_props, _engine_session_hints

pytestmark = [pytest.mark.requires_trino, pytest.mark.integration]


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
    engine_state = SimpleNamespace(
        engine_conn=trino.dbapi.connect(**kwargs),
        engine_conn_kwargs=kwargs,
        engine_session_hints={},
        source_types={"mongo": "mongodb"},
    )
    backend = TrinoBackend(MagicMock())
    engine_state.federation_engine = EngineRuntime(MagicMock(backend=backend), engine_state)
    return engine_state


async def _run(state, hints: dict[str, str]):
    state.source_federation_hints = {"mongo": hints}
    plan_hints = _engine_session_hints(
        state,
        ("mongo",),
        None,
        "SELECT 1",
        writes=False,
        defined=await _defined_session_props(state, ("mongo",)),
    )
    result = execute_trino(state.engine_conn, "SELECT 1", session_hints=plan_hints)
    return plan_hints or {}, result


async def test_a_statement_over_a_source_with_a_connector_setting_answers(state):
    hints, result = await _run(state, {"direct_connection": "true"})
    assert result.rows == [[1]]
    assert "direct_connection" not in hints


async def test_a_raw_session_property_is_sent_and_takes_effect(state):
    hints, _ = await _run(state, {"query_max_memory": "3GB", "direct_connection": "true"})
    assert hints == {"query_max_memory": "3GB"}
    cur = state.engine_conn.cursor()
    cur.execute("SHOW SESSION LIKE 'query_max_memory'")
    assert cur.fetchall()[0][1] == "3GB"
