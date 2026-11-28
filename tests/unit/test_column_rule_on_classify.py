# Copyright (c) 2026 Kenneth Stott
# Canary: 3c1f0a52-7a0e-4e1b-9d6c-6a5f1c2b8e47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2278: classifying a column brings it under a column rule keyed on its tag or classification
at once, and un-classifying releases it at once: each of the four classification endpoints (table
column decide and forget, command column decide and forget) asks whether a rule is keyed on the
column's tag or kind and, when one is, rebuilds the runtime."""

# Requirements: REQ-2278

from __future__ import annotations

import types
from contextlib import asynccontextmanager
from typing import Any, cast

import pytest

from provisa.api.admin import profiler_classifications_router as router_mod
from provisa.profiler import classifications

KIND = "us_ssn"


class _Conn:
    @asynccontextmanager
    async def transaction(self):
        yield


class _Db:
    @asynccontextmanager
    async def acquire(self):
        yield _Conn()


class _Probe:
    """Records the keyed_by questions and the runtime rebuilds."""

    def __init__(self, ruled: bool) -> None:
        self.ruled = ruled
        self.asked: list[tuple[tuple[str, ...], tuple[str, ...]]] = []
        self.rebuilds = 0


def _request() -> Any:
    return types.SimpleNamespace(state=types.SimpleNamespace())


@pytest.fixture
def wired(monkeypatch):
    async def _noop(*args, **kwargs):
        return None

    async def _member(conn, table_id, request):
        return {"served": None}

    async def _command(request, conn, name):
        return types.SimpleNamespace(name=name, domain_id="sales")

    async def _true(*args, **kwargs):
        return True

    monkeypatch.setattr(router_mod, "_db", lambda: _Db())
    monkeypatch.setattr(router_mod, "_member", _member)
    monkeypatch.setattr(router_mod, "_steward_of", _noop)
    monkeypatch.setattr(router_mod, "_command_steward", _command)
    monkeypatch.setattr(router_mod, "_physical", lambda table_id, column, served: column)
    monkeypatch.setattr(classifications, "accept_effects", _noop)
    monkeypatch.setattr(classifications, "record", _noop)
    monkeypatch.setattr(classifications, "forget", _true)
    monkeypatch.setattr(classifications, "accept_command_column_effects", _noop)
    monkeypatch.setattr(classifications, "record_command_column", _noop)
    monkeypatch.setattr(classifications, "forget_command_column", _true)
    import provisa.api.admin.command_columns as command_columns

    monkeypatch.setattr(command_columns, "require_output_column", lambda command, column: None)

    def arm(ruled: bool) -> _Probe:
        probe = _Probe(ruled)

        async def keyed_by(conn, tags=(), kinds=()):
            probe.asked.append((tuple(tags), tuple(kinds)))
            return probe.ruled

        async def rebuild():
            probe.rebuilds += 1

        monkeypatch.setattr(router_mod, "keyed_by", keyed_by)
        import provisa.api.app as app

        monkeypatch.setattr(app, "_rebuild_schemas", rebuild)
        return probe

    return arm


def _table_decision(status: str = "accepted") -> Any:
    return router_mod.ClassificationDecision(
        column="tax_id", kind=KIND, status=status, evidence="e", runId="run-1"
    )


def _command_decision(status: str = "accepted") -> Any:
    return router_mod.CommandClassification(column="tax_id", kind=KIND, status=status, evidence="e")


def _forget() -> Any:
    return router_mod.ForgetClassification(physicalColumn="tax_id", kind=KIND)


@pytest.mark.asyncio
async def test_classifying_a_table_column_applies_a_keyed_column_rule_at_once(wired):
    probe = wired(True)
    await router_mod.decide_classification(cast(Any, _request()), 1, _table_decision())
    assert probe.asked == [(("pii",), (KIND,))]
    assert probe.rebuilds == 1


@pytest.mark.asyncio
async def test_classifying_a_table_column_no_rule_keyed_rebuilds_nothing(wired):
    probe = wired(False)
    await router_mod.decide_classification(cast(Any, _request()), 1, _table_decision())
    assert probe.asked == [(("pii",), (KIND,))]
    assert probe.rebuilds == 0


@pytest.mark.asyncio
async def test_unclassifying_a_table_column_releases_a_keyed_column_rule_at_once(wired):
    probe = wired(True)
    await router_mod.forget_classification(cast(Any, _request()), 1, _forget())
    assert probe.asked == [((), (KIND,))]
    assert probe.rebuilds == 1


@pytest.mark.asyncio
async def test_unclassifying_a_table_column_no_rule_keyed_rebuilds_nothing(wired):
    probe = wired(False)
    await router_mod.forget_classification(cast(Any, _request()), 1, _forget())
    assert probe.rebuilds == 0


@pytest.mark.asyncio
async def test_classifying_a_command_column_applies_a_keyed_column_rule_at_once(wired):
    probe = wired(True)
    await router_mod.decide_command_classification(
        cast(Any, _request()), "cmd", _command_decision()
    )
    assert probe.asked == [(("pii",), (KIND,))]
    assert probe.rebuilds == 1


@pytest.mark.asyncio
async def test_classifying_a_command_column_no_rule_keyed_rebuilds_nothing(wired):
    probe = wired(False)
    await router_mod.decide_command_classification(
        cast(Any, _request()), "cmd", _command_decision()
    )
    assert probe.rebuilds == 0


@pytest.mark.asyncio
async def test_unclassifying_a_command_column_releases_a_keyed_column_rule_at_once(wired):
    probe = wired(True)
    await router_mod.forget_command_classification(cast(Any, _request()), "cmd", _forget())
    assert probe.asked == [((), (KIND,))]
    assert probe.rebuilds == 1


@pytest.mark.asyncio
async def test_unclassifying_a_command_column_no_rule_keyed_rebuilds_nothing(wired):
    probe = wired(False)
    await router_mod.forget_command_classification(cast(Any, _request()), "cmd", _forget())
    assert probe.rebuilds == 0
