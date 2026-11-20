# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The MV registry is read by every request (the rewriter's ``get_fresh``) while a schema build on
another thread registers and unregisters views in it. A read walks a snapshot of the registry, so
a build that changes its size mid-read never fails the read ("dictionary changed size during
iteration", answered to a SELECT of a stored view)."""

# Requirements: REQ-199, REQ-543

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

import pytest

from provisa.mv.models import MVDefinition, MVStatus
from provisa.mv.registry import MVRegistry


@dataclass
class _BuildMidRead(MVDefinition):
    """A view whose ``enabled`` read is where a concurrent build changes the registry: the
    interleaving a build on another thread gives a request's walk of it."""

    during: Callable[[], None] = field(default=lambda: None)

    def __getattribute__(self, name: str):
        if name == "enabled":
            object.__getattribute__(self, "during")()
        return object.__getattribute__(self, name)


def _mv(mv_id: str) -> MVDefinition:
    return MVDefinition(id=mv_id, source_tables=["t"], target_catalog="c", target_schema="s")


def _registry_changed_mid_read(change: Callable[[MVRegistry], None]) -> MVRegistry:
    reg = MVRegistry()
    reading = _BuildMidRead(
        id="view-theme_beauty", source_tables=["t"], target_catalog="c", target_schema="s"
    )
    reading.during = lambda: change(reg)
    reg.register(reading)
    reg.register(_mv("view-theme_time"))
    return reg


@pytest.mark.parametrize(
    "change",
    [
        pytest.param(lambda reg: reg.register(_mv("view-new")), id="register"),
        pytest.param(lambda reg: reg.unregister("view-theme_time"), id="unregister"),
    ],
)
def test_a_build_changing_the_registry_mid_read_does_not_fail_the_read(change):
    reg = _registry_changed_mid_read(change)

    assert reg.get_fresh() == []
    assert {mv.id for mv in reg.get_enabled()} >= {"view-theme_beauty"}
    assert {mv.id for mv in reg.get_due_for_refresh()} >= {"view-theme_beauty"}


def test_mark_stale_walks_a_snapshot():
    reg = MVRegistry()
    first = _mv("a")
    first.status = MVStatus.FRESH
    reg.register(first)
    second = _mv("b")
    second.status = MVStatus.FRESH
    reg.register(second)

    class _Grows(list):
        def __contains__(self, item):
            reg.register(_mv(f"added-{len(reg.all())}"))
            return super().__contains__(item)

    first.source_tables = _Grows(["t"])

    assert sorted(reg.mark_stale("t")) == ["a", "b"]


@pytest.mark.parametrize(
    "fault",
    [
        RuntimeError("dictionary changed size during iteration"),
        KeyError("view-theme_beauty"),
        AttributeError("'NoneType' object has no attribute 'sql'"),
    ],
    ids=["runtime", "key", "attribute"],
)
def test_a_fault_of_the_servers_code_is_a_named_500_not_a_400(fault, caplog):
    from provisa.api.data.endpoint_dev import _sql_error
    from provisa.api.errors import ApiError

    with caplog.at_level("ERROR", logger="provisa.api.data.endpoint_dev"):
        answered = _sql_error(fault)

    assert isinstance(answered, ApiError)
    assert (answered.status_code, answered.code) == (500, "data.statement_failed")
    assert answered.params == {"kind": type(fault).__name__, "error": str(fault)}
    [record] = caplog.records
    assert record.exc_info is not None and record.exc_info[1] is fault


def test_an_engines_error_subclassing_a_builtin_stays_the_statements_400():
    from fastapi import HTTPException

    from provisa.api.data.endpoint_dev import _sql_error
    from provisa.api.errors import ApiError

    class EngineNotImplemented(NotImplementedError):  # as pyarrow's ArrowNotImplementedError
        pass

    for rejected in (EngineNotImplemented("no such function"), ValueError("syntax error at 1")):
        answered = _sql_error(rejected)
        assert isinstance(answered, HTTPException) and not isinstance(answered, ApiError)
        assert (answered.status_code, answered.detail) == (400, str(rejected))
