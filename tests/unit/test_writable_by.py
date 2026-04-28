# Copyright (c) 2026 Kenneth Stott
# Canary: 1bee1a98-ba98-49db-abfc-a8b616530067
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for writable_by column permission enforcement."""

import pytest
from types import SimpleNamespace

from provisa.core.models import Column


def test_column_model_writable_by_default():
    """writable_by defaults to empty list."""
    col = Column(name="id", visible_to=["admin"])
    assert col.writable_by == []


def test_column_model_writable_by_set():
    """writable_by can be set explicitly."""
    col = Column(name="total", visible_to=["admin", "analyst"], writable_by=["admin"])
    assert col.writable_by == ["admin"]


def test_check_writable_by_blocks_when_empty():
    """Empty writable_by means no role can write."""
    from provisa.api.data.endpoint import _check_writable_by
    from fastapi import HTTPException

    table_meta = SimpleNamespace(
        columns=[{"column_name": "total", "writable_by": []}]
    )
    with pytest.raises(HTTPException) as exc_info:
        _check_writable_by(table_meta, ["total"], "analyst")
    assert exc_info.value.status_code == 403


def test_check_writable_by_allows_permitted_role():
    """Role in writable_by is allowed."""
    from provisa.api.data.endpoint import _check_writable_by

    table_meta = SimpleNamespace(
        columns=[{"column_name": "total", "writable_by": ["admin"]}]
    )
    _check_writable_by(table_meta, ["total"], "admin")


def test_check_writable_by_blocks_unpermitted_role():
    """Role not in writable_by gets 403."""
    from provisa.api.data.endpoint import _check_writable_by
    from fastapi import HTTPException

    table_meta = SimpleNamespace(
        columns=[{"column_name": "total", "writable_by": ["admin"]}]
    )
    with pytest.raises(HTTPException) as exc_info:
        _check_writable_by(table_meta, ["total"], "analyst")
    assert exc_info.value.status_code == 403
    assert "total" in exc_info.value.detail


def test_check_writable_by_ignores_unknown_columns():
    """Columns not in table metadata are not checked."""
    from provisa.api.data.endpoint import _check_writable_by

    table_meta = SimpleNamespace(
        columns=[{"column_name": "total", "writable_by": ["admin"]}]
    )
    # "unknown_col" not in metadata — should pass
    _check_writable_by(table_meta, ["unknown_col"], "analyst")


def test_check_writable_by_multiple_columns_all_permitted():
    """All columns allow the role — no error."""
    from provisa.api.data.endpoint import _check_writable_by

    table_meta = SimpleNamespace(
        columns=[
            {"column_name": "name", "writable_by": ["analyst", "admin"]},
            {"column_name": "email", "writable_by": ["analyst"]},
        ]
    )
    _check_writable_by(table_meta, ["name", "email"], "analyst")


def test_check_writable_by_multiple_columns_one_blocked():
    """First forbidden column raises 403."""
    from provisa.api.data.endpoint import _check_writable_by
    from fastapi import HTTPException

    table_meta = SimpleNamespace(
        columns=[
            {"column_name": "name", "writable_by": ["analyst", "admin"]},
            {"column_name": "salary", "writable_by": ["admin"]},
        ]
    )
    with pytest.raises(HTTPException) as exc_info:
        _check_writable_by(table_meta, ["name", "salary"], "analyst")
    assert "salary" in exc_info.value.detail
