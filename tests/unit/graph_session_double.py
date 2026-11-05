# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8a2b61-9d37-4e0c-b6a5-1c3e7d92f058
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An empty graph session for a test of a Cypher surface that is not about it (REQ-2176): every
statement runs in its principal's graph session, which a stand-in state holds no control plane
for. Each statement is given a new, empty one, bound as the real one is."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any


def empty_graph_sessions(monkeypatch: Any) -> list[str]:
    """Give each statement an empty graph session; returns the principals they were given to."""
    from provisa.api import graph_session
    from provisa.compiler import temp_tables
    from provisa.compiler.temp_tables import TempSession

    given: list[str] = []

    @asynccontextmanager
    async def session_for(_state, user_id, _claims, *, writes):
        outer = graph_session._bound.get()
        if outer is not None:
            yield outer
            return
        given.append(user_id)
        held = graph_session.GraphSession(
            user_id, TempSession(id=graph_session.session_id(str(user_id)), kept=True), writes
        )
        token = graph_session._bound.set(held)
        temp = temp_tables.bind(held.session)
        try:
            yield held
        finally:
            temp_tables.unbind(temp)
            graph_session._bound.reset(token)

    monkeypatch.setattr(graph_session, "session_for", session_for)
    return given
