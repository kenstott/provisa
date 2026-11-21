# Copyright (c) 2026 Kenneth Stott
# Canary: 8b2e4f71-0c93-4d6a-a5e8-1f7d3c9b2e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A view read through the one pipeline (semantic_statement): the view mark never leaves the
server, a CTE named like a view is the CTE, and a clause a view cannot carry is refused, never
dropped (REQ-039, REQ-135)."""

# Requirements: REQ-039, REQ-135

from __future__ import annotations

import traceback
from unittest.mock import AsyncMock, patch

import pytest

from provisa.audit import pipeline as audit_pipeline
from provisa.audit.context import audit_identity_scope
from provisa.compiler import sql_validator
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.view_expand import VIEW_MARK, ViewClauseNotSupported
from tests.unit.test_view_inputs_in_service import _OVER_A_SERVED_TABLE, _build, _view

pytestmark = pytest.mark.unit


def _state(monkeypatch):
    state = _build(monkeypatch, view=_view(_OVER_A_SERVED_TABLE), withdrawn=[])
    # Each test validates afresh: no outcome kept by another test's identical statement.
    monkeypatch.setattr(
        state._active_runtime(), "compiled_query_cache", CompiledQueryCache(), raising=False
    )
    return state


async def test_a_view_read_is_governed_over_its_body_without_the_mark(monkeypatch):
    from provisa.pgwire import _pipeline

    _state(monkeypatch)
    with audit_identity_scope("ada", "http"):
        governed = await _pipeline.govern_statement(
            'SELECT id FROM "sales"."refund_notes" LIMIT 5', "r"
        )
    assert '"orders"' in governed.governed_semantic, governed.governed_semantic
    assert VIEW_MARK not in governed.governed_semantic
    assert VIEW_MARK not in governed.sql


async def test_the_domain_access_refusal_names_no_mark(monkeypatch):
    from provisa.pgwire import _pipeline

    _state(monkeypatch)

    def _unreadable(tree, _commands):
        if "_validate_sql" in {f.name for f in traceback.extract_stack()}:
            return []  # validate_sql's own reading (the test below)
        raise ValueError(f"cannot read {tree.sql(dialect='postgres')}")

    with (
        patch.object(sql_validator, "view_command_inputs", side_effect=_unreadable),
        patch.object(audit_pipeline, "write_denial", new=AsyncMock()),  # no record database here
    ):
        with audit_identity_scope("ada", "http"):
            with pytest.raises(PermissionError) as refused:
                await _pipeline.govern_statement(
                    'SELECT id FROM "sales"."refund_notes" LIMIT 5', "r"
                )
    assert "Domain-access check could not be evaluated" in str(refused.value)
    assert "cannot read" in str(refused.value)
    assert VIEW_MARK not in str(refused.value)


async def test_an_error_validating_a_marked_statement_names_no_mark(monkeypatch):
    from provisa.pgwire import _pipeline

    _state(monkeypatch)

    def _unreadable(tree, _commands):
        raise ValueError(f"cannot read {tree.sql(dialect='postgres')}")

    with patch.object(sql_validator, "view_command_inputs", side_effect=_unreadable):
        with audit_identity_scope("ada", "http"):
            with pytest.raises(ValueError) as refused:
                await _pipeline.govern_statement(
                    'SELECT id FROM "sales"."refund_notes" LIMIT 5', "r"
                )
    assert "cannot read" in str(refused.value)
    assert VIEW_MARK not in str(refused.value)


async def test_tablesample_on_a_view_is_refused_naming_it(monkeypatch):
    from provisa.pgwire import _pipeline

    _state(monkeypatch)
    with audit_identity_scope("ada", "http"):
        with pytest.raises(ViewClauseNotSupported) as refused:
            await _pipeline.govern_statement(
                'SELECT id FROM "sales"."refund_notes" TABLESAMPLE SYSTEM (10)', "r"
            )
    assert "TABLESAMPLE" in str(refused.value)
    assert VIEW_MARK not in str(refused.value)


async def test_a_cte_named_like_the_view_is_the_cte(monkeypatch):
    from provisa.pgwire import _pipeline

    _state(monkeypatch)
    with audit_identity_scope("ada", "http"):
        governed = await _pipeline.govern_statement(
            "WITH refund_notes AS (SELECT 7 AS id) SELECT id FROM refund_notes", "r"
        )
    assert '"orders"' not in governed.governed_semantic, governed.governed_semantic
    assert "7" in governed.governed_semantic
