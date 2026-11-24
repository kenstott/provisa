# Copyright (c) 2026 Kenneth Stott
# Canary: 5dc78cb3-8334-40bc-a26e-706b63c75f11
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1058: a Hasura import whose public role carries a write is refused by name, not trimmed."""

from __future__ import annotations

import pytest

from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.models import (
    HasuraMetadata,
    HasuraPermission,
    HasuraSource,
    HasuraTable,
)


def _metadata(table: HasuraTable) -> HasuraMetadata:
    return HasuraMetadata(
        sources=[
            HasuraSource(
                name="default",
                connection_info={"database_url": "postgres://u:p@db:5432/app"},
                tables=[table],
            )
        ]
    )


def _select() -> HasuraPermission:
    return HasuraPermission(role="public", columns=["id", "title"])


@pytest.mark.parametrize(
    ("operation", "field"),
    [
        ("insert", "insert_permissions"),
        ("update", "update_permissions"),
        ("delete", "delete_permissions"),
    ],
)
def test_a_public_write_is_refused_naming_the_table_and_permission(operation, field):
    write = HasuraPermission(role="public", columns=["title"] if operation != "delete" else [])
    table = HasuraTable(name="articles", select_permissions=[_select()])
    setattr(table, field, [write])

    with pytest.raises(ValueError) as refused:
        convert_metadata(_metadata(table))

    message = str(refused.value)
    assert "'public' is read-only" in message
    assert f"{operation} permission on public.articles" in message


def test_every_public_write_is_named_in_one_refusal():
    table = HasuraTable(
        name="articles",
        insert_permissions=[HasuraPermission(role="public", columns=["title"])],
        update_permissions=[HasuraPermission(role="public", columns=["title"])],
        delete_permissions=[HasuraPermission(role="public")],
    )

    with pytest.raises(ValueError) as refused:
        convert_metadata(_metadata(table))

    message = str(refused.value)
    for operation in ("insert", "update", "delete"):
        assert f"{operation} permission on public.articles" in message


def test_a_public_select_is_still_imported():
    table = HasuraTable(name="articles", select_permissions=[_select()])

    config = convert_metadata(_metadata(table))

    imported = next(t for t in config.tables if t.table_name == "articles")
    visible = {c.name: c.visible_to for c in imported.columns}
    assert "public" in visible["id"]
    assert "public" in visible["title"]
    assert not any(g.role == "public" for g in imported.write_grants)
