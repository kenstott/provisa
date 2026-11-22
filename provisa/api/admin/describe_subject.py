# Copyright (c) 2026 Kenneth Stott
# Canary: 9b31e7c4-5d2a-4f86-b0e1-73c8a4d6f215
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The table an AI description is written for: a registered table by its id, or a table the
Register table form is about to register, by its source, schema, name and columns.

The description prompt reads only those four things, so a table need not be registered to be
described. Either form is gated the same way: the caller holds ``table_registration`` (the right
the table edit path requires) and may read the source -- a source open to named domains is read
by a caller reaching one of them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DescribeSubject:
    source_id: str
    schema_name: str
    table_name: str
    columns: list[str]


def unregistered_subject(
    table_id: str | None,
    source_id: str | None,
    schema_name: str | None,
    table_name: str | None,
    columns: list[str] | None,
) -> DescribeSubject | str:
    """The table id for the registered form (``table_id`` alone); the subject for the pre-registration
    form (``source_id``, ``schema_name``, ``table_name`` and ``columns``, all of them). Any other
    combination is refused, naming the arguments."""
    unregistered = {
        "sourceId": source_id,
        "schemaName": schema_name,
        "tableName": table_name,
        "columns": columns,
    }
    given = sorted(k for k, v in unregistered.items() if v is not None)
    if table_id is not None:
        if given:
            raise ValueError(
                "give either tableId or sourceId, schemaName, tableName and columns, not both "
                f"(tableId given with {', '.join(given)})"
            )
        return table_id
    if source_id is None or schema_name is None or table_name is None or columns is None:
        missing = sorted(k for k, v in unregistered.items() if v is None)
        raise ValueError(
            "give either tableId or sourceId, schemaName, tableName and columns "
            f"(missing {', '.join(missing)})"
        )
    return DescribeSubject(source_id, schema_name, table_name, list(columns))


def require_source_readable(info: Any, source_id: str) -> None:
    """Refuse a source the app does not hold, or one open only to domains the caller reaches
    none of. An empty ``allowed_domains`` opens a source to every domain."""
    from provisa.api.admin.capabilities import require_domain
    from provisa.api.app import state

    if source_id not in state.source_types:
        raise ValueError(f"unknown source {source_id!r}")
    allowed = state.source_allowed_domains.get(source_id)
    if not allowed:
        return
    for domain_id in allowed:
        try:
            require_domain(info, domain_id)
        except PermissionError:
            continue
        return
    raise PermissionError(
        f"No access to source {source_id!r}: it is open to domains {sorted(allowed)}, "
        "none of which the caller reaches"
    )
