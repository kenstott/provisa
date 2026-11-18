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
tables that require a parameter, views, data-quality tables, SQLite tables) the count is answered
with a count, never an unhandled server error. A table that requires a parameter (REQ-1986) is
counted and never read.

The count reads nothing: it is an upper bound from the model store, so across it no source of the
demo is called and the server runs no statement on a table; its time is printed.
"""

# Requirements: REQ-413, REQ-1986

from __future__ import annotations

import time
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


#: The settings the dialog always sends, at their defaults (REQ-413, amended 2026-11-18).
_SETTINGS = {"threshold": 0.7, "samples_per_column": 5}


def _count(inst: Instance, scope: dict) -> httpx.Response:
    return httpx.post(
        f"{inst.url}/admin/discover/runs/count",
        json={**_SETTINGS, **scope},
        headers=_HEADERS,
        timeout=300,
    )


def _log(inst: Instance) -> str:
    return (inst.log_dir / "backend.log").read_text()[-6000:]


#: The demo's own sources, each a server of this instance with its access log: a call to a
#: source is a line in one of these.
_SOURCE_LOGS = ("petstore.log", "graphql-demo.log", "grpc-demo.log")
#: The server's line for every statement that lands or reads an API or hot table's rows.
_STATEMENT = "[MAT]"


def _activity(inst: Instance) -> dict[str, int]:
    """How much the instance has read so far: the lines of each source's access log, and the
    statement lines of the server's log."""
    seen = {name: len((inst.log_dir / name).read_text().splitlines()) for name in _SOURCE_LOGS}
    seen["statements"] = (inst.log_dir / "backend.log").read_text().count(_STATEMENT)
    return seen


def _settled(inst: Instance) -> dict[str, int]:
    """The instance's activity once what it started at boot has ended (its landings): unchanged
    over three seconds."""
    deadline = time.monotonic() + 120
    before = _activity(inst)
    while True:
        time.sleep(3)
        now = _activity(inst)
        if now == before:
            return now
        assert time.monotonic() < deadline, f"the instance is still reading: {before} -> {now}"
        before = now


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
    """Answered from the model store alone: across the count, and a second one as the dialog
    recounts, no source of the demo is called and the server runs no statement on a table."""
    before = _settled(instance)
    seconds = []
    for _ in range(2):
        started = time.monotonic()
        resp = _count(instance, scope)
        seconds.append(round(time.monotonic() - started, 3))
        assert resp.status_code == 200, f"{resp.status_code} {resp.text}\n{_log(instance)}"
    time.sleep(1)  # anything the count had started would have reached a source by now
    assert _activity(instance) == before, f"the count read: {before} -> {_activity(instance)}"
    counted = resp.json()
    print(f"count {scope['source_domains']} {scope['kinds']}: {seconds} s: {counted}")
    assert counted["source_tables"] > 0
    # Both domains hold API tables that require a parameter (get_order_by_id, animal_breed).
    assert 0 < counted["tables_not_read"] < counted["source_tables"]
    assert "ParameterRequired" not in _log(instance)
    assert counted["target_tables"] == counted["source_tables"]
    assert counted["requests_are"] == "upper_bound"
    if "semantics" in scope["kinds"]:
        assert counted["requests"] >= 1
        assert sum(counted["by_group"].values()) == counted["requests"]
        # The demo's string columns are registered varchar: they are text, and are compared.
        assert counted["by_group"].get("text", 0) >= 1, counted["by_group"]
    else:
        assert counted["requests"] == 0 and counted["by_group"] == {}


def test_the_demos_string_columns_are_registered_in_their_sources_spelling(instance: Instance):
    """Why the count had no text group: the string columns of the demo's domains are registered
    as their sources spell them (varchar), not as the IR's ``text``."""
    from collections import Counter

    columns = smoke._sql_rows(instance, "SELECT domain_id, data_type FROM meta.table_columns")
    spelled = Counter(
        (c["domain_id"], c["data_type"])
        for c in columns
        if c["domain_id"] in ("pet-store", "shelter")
    )
    print("registered types of pet-store and shelter:", sorted(spelled.items()))
    strings = {
        t for (_, t), _ in spelled.items() if t.split(" ", 1)[0].split("(", 1)[0] == "varchar"
    }
    assert strings, spelled
