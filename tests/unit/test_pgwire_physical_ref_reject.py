# Copyright (c) 2026 Kenneth Stott
# Canary: 15eee378-bd39-4a76-8499-3b3f03752d24
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""One-model enforcement: the raw-SQL path accepts ONLY the semantic domain.table the
catalog advertises. A physical source-catalog reference (e.g.
``inquiries_sqlite.default.inquiries``) is an internal lowering artifact exposed to no
client; accepting it would run ungoverned against the raw source (RLS/masking bind to the
semantic table). It must be rejected as an invalid reference before any governance runs.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.asyncio


def _fake_state():
    """Minimal AppState reaching the physical-ref guard: one role context + one source catalog."""
    return SimpleNamespace(
        contexts={"analyst": SimpleNamespace()},
        rls_contexts={},
        roles={},
        source_catalogs={"pet-store-sqlite": "inquiries_sqlite"},
    )


async def _reject_check(monkeypatch, sql: str):
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline

    monkeypatch.setattr(app_mod, "state", _fake_state(), raising=False)
    return await _pipeline._govern_and_route(sql, "analyst")


async def test_physical_source_catalog_ref_rejected(monkeypatch):
    with pytest.raises(PermissionError, match="physical source names are internal"):
        await _reject_check(monkeypatch, 'SELECT * FROM "inquiries_sqlite"."default"."inquiries"')


async def test_physical_source_catalog_ref_rejected_unquoted(monkeypatch):
    with pytest.raises(PermissionError, match="physical source names are internal"):
        await _reject_check(monkeypatch, "SELECT * FROM inquiries_sqlite.default.inquiries")


async def test_internal_result_catalog_ref_rejected(monkeypatch):
    # The reserved internal catalogs (iceberg/otel/results) are equally off-limits as refs.
    with pytest.raises(PermissionError, match="physical source names are internal"):
        await _reject_check(monkeypatch, "SELECT * FROM results.public.cache")


async def test_semantic_ref_passes_the_guard(monkeypatch):
    # The semantic domain.table the catalog advertises must clear the physical-ref guard
    # (it fails later in governance under this minimal fake state — that's fine; what matters
    # is it is NOT rejected as a physical reference).
    with pytest.raises(Exception) as ei:  # noqa: PT011 - asserting it is NOT the physical-ref error
        await _reject_check(monkeypatch, "SELECT * FROM pet_store.inquiries")
    assert "physical source names are internal" not in str(ei.value)


async def test_unknown_two_part_catalog_not_treated_as_physical(monkeypatch):
    # A 3-part ref whose leading part is NOT a known source catalog (e.g. a virtual database
    # name a client fully-qualifies with) must not trip the physical-source guard.
    with pytest.raises(Exception) as ei:
        await _reject_check(monkeypatch, "SELECT * FROM provisa.pet_store.inquiries")
    assert "physical source names are internal" not in str(ei.value)
