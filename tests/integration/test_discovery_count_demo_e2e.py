# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1b9f3a-52c7-4e08-b6a4-0f8e3c7d2a51
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: the discovery dialog's requests before Run, on the default demo (REQ-413).

The ``test`` instance: the smoke check's isolated instance of the default demo (native DuckDB
engine, its own embedded PostgreSQL control plane, its own home and leased ports). The dialog asks
for the request count as soon as it opens; for the demo's pet-store and shelter domains (API
tables that need an argument, views, data-quality tables, SQLite tables) the count is answered
with a count, never an unhandled server error.
"""

# Requirements: REQ-413

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from tests.smoke import test_main_is_startable as smoke
from tests.smoke.test_main_is_startable import Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.local_platform]


@pytest.fixture(scope="module")
def instance() -> Iterator[Instance]:
    """The smoke check's instance of the default demo, its server holding a key for the model's
    vendor: the count asks no model, but Semantics needs its vendor to have a key (any key). The
    key is in this module's instance only."""
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    mp.setenv("ANTHROPIC_API_KEY", "discovery-count-test-key")
    try:
        yield from smoke.instance.__wrapped__()
    finally:
        mp.undo()


_HEADERS = {"X-Provisa-Role": "org_admin"}
_MODEL = {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"}


def _count(inst: Instance, scope: dict) -> httpx.Response:
    return httpx.post(
        f"{inst.url}/admin/discover/runs/count", json=scope, headers=_HEADERS, timeout=300
    )


def _log(inst: Instance) -> str:
    return (inst.log_dir / "backend.log").read_text()[-6000:]


@pytest.mark.parametrize(
    "scope",
    [
        pytest.param(
            {
                "kinds": ["foreign_keys", "semantics"],
                "source_domains": ["pet-store", "shelter"],
                **_MODEL,
            },
            id="the-maintainers-scope",
        ),
        pytest.param(
            {"kinds": ["semantics"], "source_domains": ["shelter"], **_MODEL},
            id="api-tables-that-need-an-argument",
        ),
        pytest.param(
            {"kinds": ["semantics"], "source_domains": ["pet-store"], **_MODEL},
            id="views-quality-tables-and-files",
        ),
        pytest.param(
            {"kinds": ["foreign_keys"], "source_domains": ["pet-store", "shelter"]},
            id="foreign-keys-alone",
        ),
    ],
)
def test_the_count_before_run_is_answered_for_the_demos_domains(instance: Instance, scope):
    resp = _count(instance, scope)
    assert resp.status_code == 200, f"{resp.status_code} {resp.text}\n{_log(instance)}"
    counted = resp.json()
    assert counted["source_tables"] > 0
    assert counted["target_tables"] == counted["source_tables"]
    if "semantics" in scope["kinds"]:
        assert counted["requests"] >= 1
        assert sum(counted["by_group"].values()) == counted["requests"]
