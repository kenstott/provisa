# Copyright (c) 2026 Kenneth Stott
# Canary: 22130ce1-aac3-4afb-aedb-e51181fd61ce
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

# Requirements: REQ-016, REQ-308, REQ-602

"""Regression tests for GH#115 — graphql_remote plural table loses columns after updateTable.

Root cause (two co-dependent bugs):
  1. update_table (schema_mutation.py) does `columns, _ = await _build_columns_for_input(...)`
     — the error MutationResult is silently discarded with `_` instead of being returned to
     the caller. When column-type resolution fails (see bug 2), the empty column list is passed
     to table_repo.upsert, overwriting the auto-registered rows.
  2. resolve_available_columns_metadata (schema_query.py) has no graphql_remote branch — it
     falls through to the engine information_schema path, which returns nothing for a source
     type with no physical SQL catalog, causing _ensure_source_column_types to produce
     unresolved columns and return a failure MutationResult.

These tests capture both failure modes so a regression can't re-enter silently.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Bug 1: update_table silently discards _build_columns_for_input error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_table_propagates_column_resolution_error() -> None:
    """update_table must return the MutationResult error from _build_columns_for_input,
    not silently discard it and proceed with an empty column list."""
    from provisa.api.admin.types import ColumnInput, MutationResult, TableInput

    _error = MutationResult(
        success=False,
        message="Cannot register: no data type could be resolved from the source",
        code="schema.column_types_unresolved",
        params={"columns": "name, species"},
    )

    _input = TableInput(
        source_id="gql_ns_src",
        table_name="gql_ns__animal_breeds",
        schema_name="graphql",
        domain_id="",
        columns=[
            ColumnInput(name="name", visible_to=["org_admin"]),
            ColumnInput(name="species", visible_to=["org_admin"]),
        ],
    )

    _info = MagicMock()
    _info.context = {}

    _mock_pool = MagicMock()
    # track whether acquire() is called — after the fix it must NOT be called because
    # update_table returns the error immediately, before any DB work.
    _acquire_call_count: list[int] = [0]

    def _counting_acquire():
        _acquire_call_count[0] += 1
        raise AssertionError("pool.acquire must not be called when _build_columns_for_input fails")

    _mock_pool.acquire = MagicMock(side_effect=_counting_acquire)

    # Patch _build_columns_for_input to simulate failed type resolution (no graphql_remote
    # branch in resolve_available_columns_metadata → engine returns nothing → error).
    # require_capability is imported locally inside update_table; patch at its source module.
    # _get_pool is imported at module level into schema_mutation from schema_helpers.
    with (
        patch(
            "provisa.api.admin.schema_mutation._build_columns_for_input",
            new=AsyncMock(return_value=([], _error)),
        ),
        patch(
            "provisa.api.admin.capabilities.require_capability",
            return_value=None,
        ),
        patch(
            "provisa.api.admin.schema_mutation._get_pool",
            new=AsyncMock(return_value=_mock_pool),
        ),
    ):
        from provisa.api.admin.schema_mutation import Mutation

        mutation = Mutation()
        result = await mutation.update_table(_info, _input)

    assert result.success is False, (
        "update_table must propagate the column-resolution error from _build_columns_for_input; "
        "got success=True, meaning the error was silently discarded and an empty column list was used"
    )
    assert "data type" in result.message.lower() or result.code == "schema.column_types_unresolved"
    assert _acquire_call_count[0] == 0, (
        "pool.acquire must not be called when _build_columns_for_input returns an error; "
        "the error must be returned immediately without touching the DB"
    )


# ---------------------------------------------------------------------------
# Bug 2: resolve_available_columns_metadata has no graphql_remote branch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_resolve_available_columns_metadata_handles_graphql_remote() -> None:
    """resolve_available_columns_metadata must return stored column types for graphql_remote
    sources (read from table_columns) rather than falling through to engine introspection
    and returning an empty list."""

    # Import app module first so the patch target exists.
    import provisa.api.app  # noqa: F401
    from provisa.api.admin.schema_query import resolve_available_columns_metadata

    _source_id = "gql_ns_src"
    _schema_name = "graphql"
    _table_name = "gql_ns__animal_breeds"

    # Fake stored rows as if auto-registration already wrote them into table_columns.
    _row_name = MagicMock()
    _row_name.column_name = "name"
    _row_name.data_type = "varchar"
    _row_species = MagicMock()
    _row_species.column_name = "species"
    _row_species.data_type = "varchar"
    _row_id = MagicMock()
    _row_id.column_name = "id"
    _row_id.data_type = "integer"
    _stored_rows = [_row_name, _row_species, _row_id]

    _mock_result = MagicMock()
    _mock_result.fetchall.return_value = _stored_rows

    _mock_conn = AsyncMock()
    _mock_conn.execute_core = AsyncMock(return_value=_mock_result)

    _mock_pool = MagicMock()
    _mock_pool.acquire = MagicMock(
        return_value=MagicMock(
            __aenter__=AsyncMock(return_value=_mock_conn),
            __aexit__=AsyncMock(return_value=False),
        )
    )

    _mock_state = MagicMock()
    _mock_state.source_types = {_source_id: "graphql_remote"}
    # Disable the native_store engine path so the function doesn't try to run SQL.
    _mock_state.federation_engine.engine.native_store = None
    # Disable the catalog-based fallback path too.
    _empty_result = MagicMock()
    _empty_result.rows = []
    _mock_state.federation_engine.execute_engine = AsyncMock(return_value=_empty_result)
    _mock_state.catalog_for = MagicMock(return_value="memory")

    with (
        patch("provisa.api.app.state", _mock_state),
        patch(
            "provisa.api.admin.schema_query._get_pool",
            AsyncMock(return_value=_mock_pool),
        ),
    ):
        result = await resolve_available_columns_metadata(_source_id, _schema_name, _table_name)

    assert result, (
        "resolve_available_columns_metadata must return stored column types for a graphql_remote "
        "source; got empty list, meaning it fell through to engine introspection and found nothing"
    )
    names = {c.name for c in result}
    assert "name" in names and "species" in names, (
        f"Expected 'name' and 'species' in returned columns; got {names}"
    )
    types = {c.name: c.data_type for c in result}
    assert types["name"] == "varchar"
    assert types["species"] == "varchar"
