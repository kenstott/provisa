# Copyright (c) 2026 Kenneth Stott
# Canary: 167bd755-fcdb-478f-8ff9-c11e0cbb9669
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""RLS rule repository — CRUD for row-level security rules, via SQLAlchemy Core (dialect-portable)."""

# Requirements: REQ-041, REQ-402, REQ-403

from provisa.core import model_change
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete as _delete, insert, select, update

from provisa.core.models import RLSRule
from provisa.core.repositories import table as table_repo
from provisa.core.schema_org import rls_rules
from provisa.encryption import encryption_service

if TYPE_CHECKING:
    from provisa.core.database import Connection


def _encrypt_filter(filter_expr: str) -> bytes:  # REQ-686
    """Encrypt an RLS filter for storage. The predicate is injected as SQL at every
    governance read, so it is sensitive metadata — kept ciphertext at rest (BYTEA)."""
    return encryption_service().encrypt(filter_expr.encode("utf-8"))


def _decrypt_row(row) -> dict:  # REQ-686
    """Return the row as a dict with ``filter_expr`` decrypted back to SQL text."""
    d = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
    raw = d.get("filter_expr")
    if raw is not None:
        d["filter_expr"] = encryption_service().decrypt(bytes(raw)).decode("utf-8")
    return d


#: Saves of rules are one at a time: "this right is in no other rule" holds only until the write.
_RULE_SAVE_LOCK = 7261748191


class RuleRightTaken(ValueError):
    """A save putting a right in a second rule of the role on the target (REQ-2188)."""

    def __init__(self, role_id: str, target: Any, right: str, rule_id: int) -> None:
        super().__init__(
            f"Right {right!r} of role {role_id!r} on {target!r} is already in row rule {rule_id}: "
            "a right is in at most one rule. Take it out of that rule, or edit that rule"
        )
        self.role_id = role_id
        self.right = right
        self.rule_id = rule_id


def _rights(rule: RLSRule) -> str:
    return "".join(rule.rights)


def _replaced(rule: RLSRule, held: list[dict]) -> dict | None:
    """The stored rule this save replaces: the one it names, else the one holding exactly these
    rights, which is how a save with no id (a config load, a re-apply) finds its own rule."""
    if rule.id is None:
        return next((h for h in held if h["rights"] == _rights(rule)), None)
    named = next((h for h in held if h["id"] == rule.id), None)
    if named is None:
        raise ValueError(f"Row rule {rule.id} is not {rule.role_id!r}'s rule on this target")
    return named


async def upsert(  # REQ-041, REQ-402, REQ-686, REQ-1919, REQ-2188
    conn: "Connection", rule: RLSRule
) -> None:
    """Save one rule: THE save, for every surface. Resolves table_id from table name for
    table-level rules. Refused by name (:class:`RuleRightTaken`) when a right it holds is in
    another rule of the role on the target. Two rules may share a predicate."""
    target = rule.table_id or rule.domain_id or rule.action_name or rule.tag_id
    model_change.name(
        "upsert", "row filter", f"{rule.role_id} {_rights(rule)} on {target}"
    )  # REQ-1524
    filter_enc = _encrypt_filter(rule.filter)
    if rule.action_name:  # REQ-1679
        scope = {"action_name": rule.action_name}
    elif rule.tag_id:  # REQ-1384
        scope = {"tag_id": rule.tag_id}
    elif rule.domain_id:
        scope = {"domain_id": rule.domain_id}
    else:
        if not rule.table_id:
            raise ValueError("One of table_id, domain_id, action_name or tag_id must be provided")
        tbl = await table_repo.find_by_table_name(conn, rule.table_id)
        if tbl is None:
            raise ValueError(f"Table not registered: {rule.table_id}")
        scope = {"table_id": tbl["id"]}
    ((scope_column, scope_value),) = scope.items()
    async with conn.transaction():
        await conn.advisory_xact_lock(_RULE_SAVE_LOCK)
        held = [
            _decrypt_row(r)
            for r in (
                await conn.execute_core(
                    select(rls_rules)
                    .where(
                        rls_rules.c[scope_column] == scope_value,
                        rls_rules.c.role_id == rule.role_id,
                    )
                    .order_by(rls_rules.c.id)
                )
            ).fetchall()
        ]
        previous = _replaced(rule, held)
        for other in held:
            if other is previous:
                continue
            taken = next((r for r in rule.rights if r in other["rights"]), None)
            if taken is not None:
                raise RuleRightTaken(rule.role_id, target, taken, other["id"])
        why = await _stamped_why(conn, rule, previous, scope_column, scope_value)
        values = {
            "filter_expr": filter_enc,
            "description": rule.description,  # REQ-2081
            "why": why,  # REQ-2094
            "rights": _rights(rule),  # REQ-2188
        }
        if previous is None:
            await conn.execute_core(
                insert(rls_rules).values(**scope, role_id=rule.role_id, **values)
            )
        else:
            await conn.execute_core(
                update(rls_rules).where(rls_rules.c.id == previous["id"]).values(**values)
            )


async def _stamped_why(
    conn: "Connection", rule: RLSRule, previous: dict | None, scope_column: str, scope_value: Any
) -> dict:
    """The rule's why to store (REQ-2094): its reason, bases and review-by date -- the stored ones
    when the save does not restate them; its description is its purpose -- with the decider, time
    and evidence moved when the filter or its why changes."""
    from provisa.judgments.evidence import evidence
    from provisa.judgments.why import stamp_for

    given = None if rule.why is None else {k: v for k, v in rule.why.items() if k != "purpose"}

    async def _evidence() -> dict:
        if scope_column != "table_id":
            return {}
        reg = await table_repo.get(conn, int(scope_value))
        return {} if reg is None else await evidence(conn, None, reg)

    return await stamp_for(
        conn,
        None if previous is None else previous.get("why"),
        given,
        changed=previous is None or previous["filter_expr"] != rule.filter,
        where=f"row rule for {rule.role_id}",
        evidence=_evidence,
    )


async def get_for_table_role(  # REQ-041, REQ-403, REQ-2188
    conn: "Connection", table_id: int, role_id: str, right: str = "R"
) -> dict | None:
    result = await conn.execute_core(
        select(rls_rules).where(
            rls_rules.c.table_id == table_id,
            rls_rules.c.role_id == role_id,
        )
    )
    return next((_decrypt_row(r) for r in result.fetchall() if right in r.rights), None)


async def list_for_role(
    conn: "Connection", role_id: str
) -> list[dict]:  # REQ-041, REQ-402, REQ-403
    result = await conn.execute_core(
        select(rls_rules).where(rls_rules.c.role_id == role_id).order_by(rls_rules.c.id)
    )
    return [_decrypt_row(r) for r in result.fetchall()]


async def tags_with_filters(conn: "Connection") -> set[str]:  # REQ-1384
    """The ids of the tags that carry a row filter. No predicate is decrypted to answer."""
    result = await conn.execute_core(
        select(rls_rules.c.tag_id).where(rls_rules.c.tag_id.is_not(None))
    )
    return {r[0] for r in result.fetchall()}


async def list_for_tag(conn: "Connection", tag_id: str) -> list[dict]:  # REQ-1384
    result = await conn.execute_core(
        select(rls_rules).where(rls_rules.c.tag_id == tag_id).order_by(rls_rules.c.id)
    )
    return [_decrypt_row(r) for r in result.fetchall()]


async def list_all(conn: "Connection") -> list[dict]:  # REQ-041, REQ-402
    result = await conn.execute_core(select(rls_rules).order_by(rls_rules.c.id))
    return [_decrypt_row(r) for r in result.fetchall()]


async def delete(  # REQ-041, REQ-402, REQ-2188
    conn: "Connection",
    role_id: str,
    table_id: int | None = None,
    domain_id: str | None = None,
    action_name: str | None = None,
    tag_id: str | None = None,
    rule_id: int | None = None,
) -> bool:
    """Delete the role's rule ``rule_id`` on the target, or with none every rule it has there."""
    model_change.name(  # REQ-1524
        "delete", "row filter", f"{role_id} on {table_id or domain_id or action_name or tag_id}"
    )
    if tag_id:  # REQ-1384
        target = rls_rules.c.tag_id == tag_id
    elif action_name:  # REQ-1679
        target = rls_rules.c.action_name == action_name
    elif domain_id:
        target = rls_rules.c.domain_id == domain_id
    else:
        target = rls_rules.c.table_id == table_id
    stmt = _delete(rls_rules).where(target, rls_rules.c.role_id == role_id)
    if rule_id is not None:
        stmt = stmt.where(rls_rules.c.id == rule_id)
    result = await conn.execute_core(stmt)
    return (result.rowcount or 0) > 0
