# Copyright (c) 2026 Kenneth Stott
# Canary: 8274f85c-0cda-4597-b78b-d2fe6d7de7a6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every governance judgment in an org's model, with its why (REQ-2094): each tag assignment,
each column's fake and synthetic rule, each mask entry, each row rule and each write grant.

A judgment's identity in the review queue is its (object_type, object_key, judgment):

- a tag assignment: ``tag_assignment``, ``<base tag>@<the assignment's object key>``;
- a fake: ``column``, ``<table id>:<column>``;
- a mask entry: ``mask_entry``, ``<table id>:<column>#<its position in the list>``;
- a row rule: ``row_rule``, its id; a write grant: ``write_grant``, its id.
"""

# Requirements: REQ-2094

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select

from provisa.judgments.findings import FAKE, MASK, ROW_RULE, TAG, WRITE_GRANT


@dataclass(frozen=True)
class Judgment:
    judgment: str
    object_type: str
    object_key: str
    why: dict[str, Any]
    table_id: int | None = None
    column: str | None = None  # the decided column, for a column-level judgment
    expires_on: str | None = None  # a tag assignment's planned end
    sensitive: bool = False  # on a sensitive column
    extra: dict[str, Any] = field(default_factory=dict)


def tag_key(base_tag_id: str, object_key: str) -> str:
    return f"{base_tag_id}@{object_key}"


def _iso(value: Any) -> Any:
    return value.isoformat() if hasattr(value, "isoformat") else value


async def judgments(conn: Any) -> list[Judgment]:
    """Every judgment of the org whose model ``conn`` is bound to."""
    from provisa.core.schema_org import (
        registered_tables,
        rls_rules,
        table_columns,
        table_write_grants,
        tag_assignments,
        tags,
    )
    from provisa.security.sensitive import sensitive_assignments

    sens = {
        (r[0], r[2])
        for r in (
            await conn.execute_core(sensitive_assignments(tag_assignments, registered_tables, tags))
        ).fetchall()
    }
    out: list[Judgment] = []
    ta = tag_assignments
    for r in (await conn.execute_core(select(ta))).fetchall():
        a = dict(r._mapping)
        why = {
            "reason": a["reason"],
            "purpose": a["purpose"],
            "bases": a["bases"] or [],
            "review_on": a["review_on"],
            "decided_by": a["decided_by"],
            "decided_at": _iso(a["decided_at"]),
            "evidence": a["evidence"] or {},
        }
        out.append(
            Judgment(
                TAG,
                "tag_assignment",
                tag_key(a["base_tag_id"], a["object_key"]),
                why,
                table_id=a["table_id"],
                column=a["column_name"] if a["object_type"] == "column" else None,
                expires_on=a["expires_on"],
                sensitive=(a["table_id"], a["column_name"]) in sens,
                extra={"tag_id": a["tag_id"], "object_kind": a["object_type"]},
            )
        )
    tc = table_columns
    for r in (
        await conn.execute_core(
            select(
                tc.c.table_id,
                tc.c.column_name,
                tc.c.fake,
                tc.c.synthetic_rule,
                tc.c.fake_why,
                tc.c.masks,
            )
        )
    ).fetchall():
        tid, name, fake, rule, fake_why, masks = r
        sensitive = (tid, name) in sens
        if fake or rule:
            out.append(
                Judgment(
                    FAKE,
                    "column",
                    f"{tid}:{name}",
                    dict(fake_why or {}),
                    table_id=tid,
                    column=name,
                    sensitive=sensitive,
                    extra={"fake": fake, "synthetic_rule": rule},
                )
            )
        for i, entry in enumerate(masks or []):
            out.append(
                Judgment(
                    MASK,
                    "mask_entry",
                    f"{tid}:{name}#{i}",
                    dict(entry.get("why") or {}),
                    table_id=tid,
                    column=name,
                    sensitive=sensitive,
                    extra={"to": entry.get("to"), "position": i},
                )
            )
    for r in (await conn.execute_core(select(rls_rules))).fetchall():
        rule = dict(r._mapping)
        out.append(
            Judgment(
                ROW_RULE,
                "row_rule",
                str(rule["id"]),
                {**dict(rule["why"] or {}), "purpose": rule["description"] or None},
                table_id=rule["table_id"],
                extra={"role_id": rule["role_id"]},
            )
        )
    wg = table_write_grants
    for r in (
        await conn.execute_core(
            select(wg.c.id, wg.c.table_id, wg.c.role_id, wg.c.operation, wg.c.columns, wg.c.why)
        )
    ).fetchall():
        out.append(
            Judgment(
                WRITE_GRANT,
                "write_grant",
                str(r[0]),
                dict(r[5] or {}),
                table_id=r[1],
                extra={"role_id": r[2], "operation": r[3], "columns": list(r[4] or [])},
            )
        )
    return out
