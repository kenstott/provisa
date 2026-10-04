# Copyright (c) 2026 Kenneth Stott
# Canary: 7ed7869c-dc42-45ba-88dd-ccbeb08dcb73

"""A delta reload falls back to a whole rebuild only by a declared rule, never silently (REQ-874)."""

from __future__ import annotations

import pytest

from provisa.federation import delta
from provisa.federation import replica_state as rs

pytestmark = pytest.mark.unit


def _reason(**kw):
    base = dict(
        has_delta=True, has_cursor=True, reason=rs.REASON_REFRESH,
        definition_changed=False, store_applies_delta=True, rebuild_due=False,
    )  # fmt: skip
    base.update(kw)
    return delta.delta_build_reason(**base)


def test_a_well_formed_refresh_applies_the_delta():
    assert _reason() is None


def test_no_delta_declared_is_a_whole_rebuild():
    assert _reason(has_delta=False) == delta.SKIP_NO_DELTA


def test_first_build_without_a_cursor_rebuilds_whole():
    assert _reason(has_cursor=False) == delta.SKIP_FIRST_BUILD


@pytest.mark.parametrize("reason", [rs.REASON_MODEL, rs.REASON_DEFINITION])
def test_a_model_or_definition_request_rebuilds_whole(reason):
    assert _reason(reason=reason) == delta.SKIP_DEFINITION


def test_a_definition_change_rebuilds_whole():
    assert _reason(definition_changed=True) == delta.SKIP_DEFINITION


def test_an_operator_request_rebuilds_whole():
    assert _reason(reason=rs.REASON_OPERATOR) == delta.SKIP_OPERATOR


def test_the_rebuild_interval_rebuilds_whole():
    assert _reason(rebuild_due=True) == delta.SKIP_REBUILD_EVERY


def test_a_store_that_cannot_apply_a_delta_rebuilds_whole():
    assert _reason(store_applies_delta=False) == delta.SKIP_STORE


def test_definition_wins_over_the_later_cases():
    # A definition change AND an operator request: the first rule decides.
    assert _reason(definition_changed=True, reason=rs.REASON_OPERATOR) == delta.SKIP_DEFINITION
