# Copyright (c) 2026 Kenneth Stott
# Canary: 9be8110e-7f60-44e5-8a54-33886e82528d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The sandboxes of one demo version share one connector server (REQ-2006, REQ-1992): a files
demo sandbox started a JVM of its own, ~570 MB each, over the same read-only files every other
sandbox of that version reads."""

# Requirements: REQ-2006, REQ-1992

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.core import demo_bundles
from provisa.core.request_context import current_org
from provisa.federation import pgwire_replica as pr

V1 = Path("/bundles/files/v1")
V2 = Path("/bundles/files/v2")


@pytest.fixture
def demo_orgs(monkeypatch):
    """sbxa00000000001 and sbxb00000000002 opened from files v1, sbxc00000000003 from v2."""
    opened = {"sbxa00000000001": V1, "sbxb00000000002": V1, "sbxc00000000003": V2}
    monkeypatch.setattr(demo_bundles, "bundled_demos", lambda: ("files",))
    monkeypatch.setattr(demo_bundles, "own_demo_directory", lambda org: opened.get(org))
    monkeypatch.setattr(pr, "_ENDPOINTS", {})
    monkeypatch.setattr(pr, "_ENDPOINT_USERS", {})
    closed: list[tuple[str, str]] = []
    monkeypatch.setattr(pr.ConnectorReplica, "close", lambda self: closed.append(self._key))
    return closed


def _drop(directory: Path, globs=("complaints_*.csv",)):
    return SimpleNamespace(
        id="drop",
        type=SimpleNamespace(value="files"),
        path=str(directory / "drop"),
        file_glob_tables=[{"table": "complaints", "file_glob": g} for g in globs],
    )


def _as(org_id, fn, *args):
    token = current_org.set(org_id)
    try:
        return fn(*args)
    finally:
        current_org.reset(token)


def test_sandboxes_of_one_version_share_a_server_and_others_do_not(demo_orgs):
    a = _as("sbxa00000000001", pr.server_key, _drop(V1))
    assert a == _as("sbxb00000000002", pr.server_key, _drop(V1))
    assert a[0] == "sbxtplfiles"
    assert a == _as("sbxtplfiles", pr.server_key, _drop(V1)), "the template reads the same files"
    assert a != _as("sbxc00000000003", pr.server_key, _drop(V2)), "another version"
    changed = _as("sbxb00000000002", pr.server_key, _drop(V1, globs=("other_*.csv",)))
    assert changed != a, "a sandbox whose tables changed reads differently"
    assert _as("acme", pr.server_key, _drop(V1)) == ("acme", "drop"), "an ordinary org's own"


def test_a_shared_server_stops_when_the_last_sandbox_reading_it_goes(demo_orgs):
    closed = demo_orgs
    first = _as("sbxa00000000001", pr._endpoint_replica, _drop(V1))
    assert _as("sbxb00000000002", pr._endpoint_replica, _drop(V1)) is first
    assert len(pr._ENDPOINTS) == 1
    pr.stop_org_servers("sbxa00000000001")
    assert closed == [], "another sandbox still reads it"
    pr.stop_org_servers("sbxb00000000002")
    assert closed == [first._key] and pr._ENDPOINTS == {} and pr._ENDPOINT_USERS == {}
