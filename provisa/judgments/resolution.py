# Copyright (c) 2026 Kenneth Stott
# Canary: eaedf2b9-12df-4d7e-9e08-89e2cfe253af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Working the queue (REQ-2094): *Reaffirm* re-records a judgment's why with today's decider,
time and evidence and resolves its open findings; *Dismiss* resolves a posture finding with a
note. A judgment changed in its editor is restamped by its own save, and the next review clears
what no longer holds.

What resolving a finding needs is the right that edits the decision: ``sensitive_data`` on a
sensitive column, else the decision's own editor's right; a posture finding needs
``access_config``. Seeing one needs ``sensitive_data`` on a sensitive column, else
``view_governance``.
"""

# Requirements: REQ-2094

from __future__ import annotations

import datetime as dt
from typing import Any

from sqlalchemy import and_, select, update

from provisa.judgments import findings as fq

REAFFIRMED = "reaffirmed"
DISMISSED = "dismissed"


class ResolveRefused(ValueError):
    """A finding that cannot be resolved this way, said by why."""


def read_rights(sensitive: bool) -> tuple[str, ...]:
    from provisa.security.sensitive import SENSITIVE_DATA

    return (SENSITIVE_DATA,) if sensitive else ("view_governance",)


def resolve_rights(judgment: str, sensitive: bool) -> tuple[str, ...]:
    """The rights (any one of) resolving a finding on ``judgment`` needs."""
    from provisa.api.admin._hiding_guard import GOVERNANCE_RIGHTS, MASK_RIGHTS
    from provisa.security.sensitive import SENSITIVE_DATA

    if judgment == fq.POSTURE:
        return ("access_config",)
    if sensitive:
        return (SENSITIVE_DATA,)
    if judgment == fq.TAG:
        return ("table_registration",)
    if judgment == fq.FAKE:
        return ("table_registration", *GOVERNANCE_RIGHTS)
    if judgment == fq.MASK:
        return MASK_RIGHTS
    if judgment in (fq.ROW_RULE, fq.WRITE_GRANT):
        return ("masking_config",)
    raise ValueError(f"judgment {judgment!r} is not one of {fq.JUDGMENTS}")


def _restamped(why: dict | None, evidence: dict, by: str | None, at: dt.datetime) -> dict:
    from provisa.judgments.why import given_part

    return {**given_part(why), "decided_by": by, "decided_at": at.isoformat(), "evidence": evidence}


async def _reg(conn: Any, table_id: int) -> dict:
    from provisa.core.schema_org import registered_tables as rt

    row = (await conn.execute_core(select(rt).where(rt.c.id == table_id))).fetchone()
    if row is None:
        raise ResolveRefused(f"table {table_id} is no longer registered")
    return dict(row._mapping)


async def reaffirm(conn: Any, finding: dict, *, by: str | None, at: dt.datetime) -> int:
    """Re-record the finding's judgment with today's decider, time and evidence, and resolve every
    open finding on it. Returns how many findings were resolved."""
    from provisa.core.schema_org import (
        rls_rules,
        table_columns,
        table_write_grants,
        tag_assignments,
    )
    from provisa.judgments.evidence import evidence

    from provisa.core import model_change

    kind, key = finding["object_type"], finding["object_key"]
    if finding["judgment"] == fq.POSTURE:
        raise ResolveRefused("a posture finding is dismissed with a note, not reaffirmed")
    model_change.name("reaffirm", finding["judgment"], key)
    if finding["category"] == fq.UNDECIDED:
        raise ResolveRefused("an undecided column is decided in its editor, not reaffirmed")
    if kind == "tag_assignment":
        base, object_key = key.split("@", 1)
        ta = tag_assignments
        where = and_(ta.c.base_tag_id == base, ta.c.object_key == object_key)
        row = (await conn.execute_core(select(ta).where(where))).fetchone()
        if row is None:
            raise ResolveRefused(f"tag {base} is no longer on {object_key}")
        a = row._mapping
        ev = (
            {}
            if a["table_id"] is None
            else await evidence(
                conn,
                None,
                await _reg(conn, a["table_id"]),
                a["column_name"] if a["object_type"] == "column" else None,
            )
        )
        await conn.execute_core(
            update(ta).where(where).values(decided_by=by, decided_at=at, evidence=ev)
        )
    elif kind in ("column", "mask_entry"):
        column_key, _, position = key.partition("#")
        tid_text, column = column_key.split(":", 1)
        tid = int(tid_text)
        tc = table_columns
        where = and_(tc.c.table_id == tid, tc.c.column_name == column)
        row = (await conn.execute_core(select(tc.c.fake_why, tc.c.masks).where(where))).fetchone()
        if row is None:
            raise ResolveRefused(f"column {column} of table {tid} is no longer registered")
        ev = await evidence(conn, None, await _reg(conn, tid), column)
        if kind == "column":
            values: dict[str, Any] = {"fake_why": _restamped(row[0], ev, by, at)}
        else:
            masks = [dict(e) for e in row[1] or []]
            i = int(position)
            if i >= len(masks):
                raise ResolveRefused(f"column {column} has no mask entry {i + 1} any more")
            masks[i]["why"] = _restamped(masks[i].get("why"), ev, by, at)
            values = {"masks": masks}
        await conn.execute_core(update(tc).where(where).values(values))
    elif kind in ("row_rule", "write_grant"):
        table = rls_rules if kind == "row_rule" else table_write_grants
        row = (
            await conn.execute_core(
                select(table.c.table_id, table.c.why).where(table.c.id == int(key))
            )
        ).fetchone()
        if row is None:
            raise ResolveRefused(f"{kind.replace('_', ' ')} {key} no longer exists")
        ev = {} if row[0] is None else await evidence(conn, None, await _reg(conn, row[0]))
        await conn.execute_core(
            update(table).where(table.c.id == int(key)).values(why=_restamped(row[1], ev, by, at))
        )
    else:
        raise ResolveRefused(f"a {kind} finding cannot be reaffirmed")
    jf = fq.jf
    result = await conn.execute_core(
        update(jf)
        .where(
            jf.c.object_type == kind,
            jf.c.object_key == key,
            jf.c.judgment == finding["judgment"],
            jf.c.resolved_at.is_(None),
        )
        .values(resolved_at=at, resolved_by=by, resolution=REAFFIRMED)
    )
    return result.rowcount or 0


async def dismiss(conn: Any, finding: dict, *, note: str, by: str | None, at: dt.datetime) -> None:
    """Dismiss a posture finding, with what the operator says about it."""
    if finding["judgment"] != fq.POSTURE:
        raise ResolveRefused("only a posture finding is dismissed; reaffirm or edit the judgment")
    if not note.strip():
        raise ResolveRefused("dismissing a posture finding needs a note")
    await fq.resolve(conn, finding["id"], by=by, resolution=f"{DISMISSED}: {note}", at=at)


def finding_column(finding: dict) -> tuple[int, str] | None:
    """The (table id, physical column) a finding is on -- a column, a mask entry, or a tag
    assignment on a column -- or None for a finding on anything else."""
    kind, key = finding["object_type"], finding["object_key"]
    if kind == "tag_assignment":
        object_key = key.split("@", 1)[1]
        if not object_key.startswith("column:"):
            return None
        _, tid, column = object_key.split(":", 2)
    elif kind in ("column", "mask_entry"):
        tid, column = key.partition("#")[0].split(":", 1)
    else:
        return None
    return int(tid), column


async def is_sensitive(conn: Any, finding: dict) -> bool:
    """Whether the finding is on a sensitive column (or a tag assignment on one)."""
    from provisa.core.schema_org import (
        glossary_term_refs,
        glossary_terms,
        registered_tables,
        tag_assignments,
        tags,
    )
    from provisa.security.sensitive import sensitive_assignments

    on = finding_column(finding)
    if on is None:
        return False
    table_id, column = on
    sens = sensitive_assignments(
        tag_assignments, registered_tables, tags, glossary_term_refs, glossary_terms
    ).subquery()
    row = (
        await conn.execute_core(
            select(sens.c.table_id).where(sens.c.table_id == table_id, sens.c.column_name == column)
        )
    ).fetchone()
    return row is not None
