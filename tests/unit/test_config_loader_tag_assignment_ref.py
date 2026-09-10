# Copyright (c) 2026 Kenneth Stott
# Canary: cf59ecc9-f721-4fe0-bda3-6ae653fd23fd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for tag-assignment table resolution in the config loader (REQ-1377, REQ-1266).

Source coverage:
  - provisa/core/config_loader.py — _resolve_tag_assignment_table

The demo config object is shared by every demo org. After the default org boots, its runtime
rewrites ``state.config.tag_assignments`` from its own rows, which carry the default org's
``registered_tables`` serials alongside ``table_ref``. A second org loading that config must
resolve ``table_ref`` against ITS OWN registry: the serial is org-local, the ref is not.
Carrying the serial through produced a foreign-key violation on every non-default org build.
"""

from typing import Any

import pytest

from provisa.core import config_loader
from provisa.core.models import TagAssignment


_CONN: Any = object()  # resolve_table_id is patched; the connection is never touched


@pytest.fixture
def resolver(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fake ``tag_repo.resolve_table_id`` recording each lookup; maps refs to this org's ids."""
    calls: list[tuple[str, str, str]] = []
    ids = {("inquiries-sqlite", "default", "users"): 58}

    async def _resolve(_conn: Any, source: str, schema: str, table: str) -> int | None:
        calls.append((source, schema, table))
        return ids.get((source, schema, table))

    monkeypatch.setattr(config_loader.tag_repo, "resolve_table_id", _resolve)
    return {"calls": calls}


async def test_table_ref_wins_over_stale_serial_from_another_org(resolver: dict[str, Any]) -> None:
    ta = TagAssignment(
        tag_id="pii",
        object_type="column",
        table_id=50,  # the default org's serial for the same table
        table_ref="inquiries-sqlite.default.users",
        column_name="email",
    )
    resolved = await config_loader._resolve_tag_assignment_table(_CONN, ta)
    assert resolved.table_id == 58
    assert resolver["calls"] == [("inquiries-sqlite", "default", "users")]


async def test_ref_only_assignment_resolves(resolver: dict[str, Any]) -> None:
    ta = TagAssignment(
        tag_id="pii",
        object_type="column",
        table_ref="inquiries-sqlite.default.users",
        column_name="email",
    )
    resolved = await config_loader._resolve_tag_assignment_table(_CONN, ta)
    assert resolved.table_id == 58


async def test_serial_only_assignment_is_untouched(resolver: dict[str, Any]) -> None:
    ta = TagAssignment(tag_id="pii", object_type="column", table_id=7, column_name="email")
    resolved = await config_loader._resolve_tag_assignment_table(_CONN, ta)
    assert resolved is ta
    assert resolver["calls"] == []


async def test_unregistered_ref_is_an_error(resolver: dict[str, Any]) -> None:
    ta = TagAssignment(
        tag_id="pii", object_type="column", table_ref="nope.default.users", column_name="email"
    )
    with pytest.raises(ValueError, match="not registered"):
        await config_loader._resolve_tag_assignment_table(_CONN, ta)


async def test_malformed_ref_is_an_error(resolver: dict[str, Any]) -> None:
    ta = TagAssignment(tag_id="pii", object_type="column", table_ref="users", column_name="email")
    with pytest.raises(ValueError, match="source.schema.table"):
        await config_loader._resolve_tag_assignment_table(_CONN, ta)
