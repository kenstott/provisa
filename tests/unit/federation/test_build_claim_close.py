# Copyright (c) 2026 Kenneth Stott
# Canary: f945d1dc-5498-4288-80bf-13988872cd74
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A build claim's locks are free once its close returns.

A claim's locks are session advisory locks. Ending the session frees them, but the server frees
them only when the backend finishes exiting, which the client does not wait for: a runner's next
pass, started at once, could find a replica still locked by a claim already closed, and leave it
out without a word (its hold was said once). Close releases them on the session itself first.
The test stands in for a session that has not ended yet by keeping dispose from ending it.
"""

from __future__ import annotations

from typing import Any

from provisa.federation.replica_locks import BuildClaim

_KEY = ("src", "public", "t3")


def test_a_closed_claims_replica_lock_is_free_at_once(control_plane, monkeypatch):
    held = BuildClaim(control_plane.url)
    assert held.try_replica("org", _KEY)
    assert held.try_engine_slot("engine", 1)
    # The session outlives the close for a moment, as on a server still ending the backend:
    # neither closing the connection nor disposing the engine ends it here.
    session: Any = held._conn
    monkeypatch.setattr(session, "close", lambda: None)
    monkeypatch.setattr(held._engine, "dispose", lambda *a, **k: None)
    held.close()
    other = BuildClaim(control_plane.url)
    try:
        assert other.try_replica("org", _KEY), "a closed claim still holds the replica's lock"
        assert other.try_engine_slot("engine", 1), "a closed claim still holds the engine slot"
    finally:
        other.close()
        monkeypatch.undo()
        session.close()
