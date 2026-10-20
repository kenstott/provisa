# Copyright (c) 2026 Kenneth Stott
# Canary: 4ff6e483-58a2-42af-8911-d1f25c28973d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's settings that have mutations of their own -- its naming convention, role TTLs,
replicate, write grants and draft -- checked and set the one way, whether a mutation of their own
saves one of them or registerTable / updateTable carries them with the table (REQ-2109, settled:
one save applies everything in one control-plane transaction, and a refusal of any one leaves
nothing saved)."""

# Requirements: REQ-2109, REQ-1907, REQ-826, REQ-2074, REQ-1676, REQ-1921

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import strawberry
from sqlalchemy import select, update

from provisa.api.admin.types import MutationResult
from provisa.core.schema_org import registered_tables, roles

if TYPE_CHECKING:
    from provisa.api.admin.types import RoleTtlInput, TableInput, WriteGrantInput
    from provisa.core.database import Connection
    from provisa.core.models import Table, WriteGrant


class TableSaveRefused(Exception):
    """A check inside a table save's transaction refused it: the transaction rolls back, and the
    save answers with ``result``."""

    def __init__(self, result: MutationResult) -> None:
        super().__init__(result.message)
        self.result = result


def carried(value: Any) -> bool:
    """Whether a TableInput setting is carried: left out (UNSET), it is not."""
    return value is not strawberry.UNSET


def invalid_replicate(replicate: int | None) -> MutationResult | None:  # REQ-826
    """The refusal for a ``replicate`` value that is not one the setting has, else None."""
    from provisa.core.replicate import check_replicate

    try:
        check_replicate(replicate)
    except ValueError as exc:
        return MutationResult(
            success=False,
            message=str(exc),
            code="schema.replicate_invalid",
            params={"value": str(replicate)},
        )
    return None


async def role_ttl_refusal(
    conn: "Connection", role_ttl: "list[RoleTtlInput]"
) -> MutationResult | None:  # REQ-1907
    """Why a role -> TTL list cannot be saved: a role twice, a negative TTL, a role that does not
    exist. None when it can; any violation refuses the whole list."""
    seen: set[str] = set()
    for entry in role_ttl:
        if entry.role in seen:
            return MutationResult(
                success=False,
                message=f"Role {entry.role!r} appears more than once",
                code="schema.role_ttl_duplicate_role",
                params={"role": entry.role},
            )
        seen.add(entry.role)
        if entry.ttl < 0:
            return MutationResult(
                success=False,
                message=f"TTL for role {entry.role!r} must be >= 0, got {entry.ttl}",
                code="schema.role_ttl_negative",
                params={"role": entry.role, "ttl": entry.ttl},
            )
    known = {
        r[0]
        for r in (
            await conn.execute_core(select(roles.c.id).where(roles.c.id.in_(seen)))
        ).fetchall()
    }
    for entry in role_ttl:
        if entry.role not in known:
            return MutationResult(
                success=False,
                message=f"Role {entry.role!r} does not exist",
                code="schema.role_ttl_unknown_role",
                params={"role": entry.role},
            )
    return None


def write_grant_invalid(reason: str) -> MutationResult:  # REQ-2074
    return MutationResult(
        success=False,
        message=reason,
        code="schema.write_grant_invalid",
        params={"reason": reason},
    )


async def write_grant_roles_refusal(
    conn: "Connection", grants: "list[WriteGrantInput]"
) -> MutationResult | None:  # REQ-2074
    """The refusal naming a grant's role that does not exist, else None."""
    from provisa.core.repositories import role as role_repo

    for role_id in sorted({g.role for g in grants}):
        if await role_repo.get(conn, role_id) is None:
            return MutationResult(
                success=False,
                message=f"Role {role_id!r} not found",
                code="schema.role_not_found",
                params={"role": role_id},
            )
    return None


def write_grant_models(grants: "list[WriteGrantInput]") -> "list[WriteGrant]":  # REQ-2074
    """The grants as the model holds them; raises pydantic's ValidationError for one that does
    not fit a grant."""
    from provisa.core.models import WriteGrant

    return [
        WriteGrant.model_validate(
            {
                "role": g.role,
                "operation": g.operation,
                "columns": list(g.columns),
                "rows": g.rows,
                "check": g.check,
                "why": g.why,  # REQ-2094
                "presets": [{k: v for k, v in vars(p).items() if v is not None} for p in g.presets],
            }
        )
        for g in grants
    ]


def write_grant_predicates_refusal(
    table_id: int, grants: "list[WriteGrant]", registry: list[dict]
) -> MutationResult | None:  # REQ-1676
    """Why a grant's rows or check does not resolve against the table as a row filter must, else
    None. ``registry`` is every registered table, the table itself among them."""
    from provisa.compiler.rls_validate import validate_rls_predicate

    target = [t for t in registry if t["id"] == table_id]
    for grant in grants:
        for stated in (grant.rows, grant.check):
            if stated is None:
                continue
            problem = validate_rls_predicate(stated, target, registry)
            if problem is not None:
                return write_grant_invalid(f"{grant.operation} grant for {grant.role!r}: {problem}")
    return None


async def save_naming(conn: "Connection", table_id: int, convention: str | None) -> int:
    """Set a table's GraphQL naming convention (None: inherit). Returns the rows it changed."""
    result = await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == table_id)
        .values(gql_naming_convention=convention)
    )
    return result.rowcount or 0


def naming_refusal(convention: str | None) -> MutationResult | None:
    """The refusal for a naming convention that is not one of the presets (None inherits)."""
    from provisa.compiler.naming import VALID_CONVENTIONS, normalize_convention

    if convention is None or normalize_convention(convention) in VALID_CONVENTIONS:
        return None
    return MutationResult(
        success=False,
        message=(
            f"unknown naming convention {convention!r}; expected one of {sorted(VALID_CONVENTIONS)}"
        ),
        code="schema.naming_convention_invalid",
        params={"convention": convention},
    )


def _null_refused(name: str) -> MutationResult:
    return MutationResult(
        success=False,
        message=f"{name} cannot be null: leave it out to keep what the table has",
        code="schema.table_setting_null",
        params={"setting": name},
    )


async def with_carried_settings(
    info: Any, conn: "Connection", inp: "TableInput", model: "Table"
) -> "Table":
    """``model`` with the settings ``inp`` carries set on it, each checked as its own mutation
    checks it; raises TableSaveRefused for one that cannot be saved. Write grants are checked
    here against the table's columns; their predicates are judged once the table is stored
    (write_grant_predicates_refusal), since a predicate may read the table itself."""
    from pydantic import ValidationError

    from provisa.api.admin.capabilities import require_capability, require_right_in_domains
    from provisa.core.models import Table

    updates: dict[str, Any] = {}
    if carried(inp.role_ttl):
        if inp.role_ttl is None:
            raise TableSaveRefused(_null_refused("roleTtl"))
        refusal = await role_ttl_refusal(conn, inp.role_ttl)
        if refusal is not None:
            raise TableSaveRefused(refusal)
        updates["role_ttl"] = {e.role: e.ttl for e in inp.role_ttl}
    if carried(inp.replicate):
        refusal = invalid_replicate(inp.replicate)
        if refusal is not None:
            raise TableSaveRefused(refusal)
        updates["replicate"] = inp.replicate
    if carried(inp.draft):
        if inp.draft is None:
            raise TableSaveRefused(_null_refused("draft"))
        updates["draft"] = inp.draft
    if carried(inp.gql_naming_convention):
        refusal = naming_refusal(inp.gql_naming_convention)
        if refusal is not None:
            raise TableSaveRefused(refusal)
        updates["gql_naming_convention"] = inp.gql_naming_convention
    if carried(inp.write_grants):
        if inp.write_grants is None:
            raise TableSaveRefused(_null_refused("writeGrants"))
        # REQ-2074: who may write which rows is governance, gated as a row filter is.
        require_capability(info, "masking_config")
        require_right_in_domains(info, "masking_config", {model.domain_id})
        refusal = await write_grant_roles_refusal(conn, inp.write_grants)
        if refusal is not None:
            raise TableSaveRefused(refusal)
        try:
            updates["write_grants"] = write_grant_models(inp.write_grants)
        except ValidationError as refused:
            raise TableSaveRefused(
                write_grant_invalid("; ".join(e["msg"] for e in refused.errors()))
            ) from refused
    staged = model.model_copy(update=updates)
    if "write_grants" in updates:
        try:
            # The table's own rules for its grants: one per role and operation, its columns.
            Table.model_validate(staged.model_dump())
        except ValidationError as refused:
            raise TableSaveRefused(
                write_grant_invalid("; ".join(e["msg"] for e in refused.errors()))
            ) from refused
    return staged


async def save_carried_after_store(
    conn: "Connection", inp: "TableInput", model: "Table", table_id: int
) -> None:
    """What of the carried settings is written beside the stored row, in the same transaction:
    the naming convention (the row's upsert leaves it as stored), and the check of the write
    grants' predicates against the table as stored. Raises TableSaveRefused."""
    from provisa.core.repositories import table as table_repo

    if carried(inp.gql_naming_convention):
        await save_naming(conn, table_id, inp.gql_naming_convention)
    if carried(inp.write_grants) and any(
        g.rows is not None or g.check is not None for g in model.write_grants
    ):
        refusal = write_grant_predicates_refusal(
            table_id, list(model.write_grants), await table_repo.list_all(conn)
        )
        if refusal is not None:
            raise TableSaveRefused(refusal)
