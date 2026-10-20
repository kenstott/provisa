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
replicate, write grants, draft, Cache TTL, paging, region, load protection and row rules --
checked and set the one way, whether a mutation of their own
saves one of them or registerTable / updateTable carries them with the table (REQ-2109, settled:
one save applies everything in one control-plane transaction, and a refusal of any one leaves
nothing saved)."""

# Requirements: REQ-2109, REQ-1907, REQ-826, REQ-2074, REQ-1676, REQ-1921, REQ-318, REQ-1141
# Requirements: REQ-1934, REQ-2081, REQ-2031, REQ-1443

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import strawberry
from sqlalchemy import select, update

from provisa.api.admin.types import MutationResult
from provisa.core.schema_org import registered_tables, roles, sources

if TYPE_CHECKING:
    from provisa.api.admin.types import RLSRuleInput, RoleTtlInput, TableInput, WriteGrantInput
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
    from provisa.core.models import Column, Table

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
    if carried(inp.cache_ttl):
        updates["cache_ttl"] = cache_ttl_checked(model, inp.cache_ttl)
    staged = model.model_copy(update=updates)
    if "write_grants" in updates:
        try:
            # The table's own rules for its grants: one per role and operation, its columns --
            # judged on the grants and columns alone, as setTableWriteGrants judges them.
            Table(
                source_id=staged.source_id,
                domain_id=staged.domain_id,
                schema_name=staged.schema_name,
                table_name=staged.table_name,
                columns=[Column(name=c.name, visible_to=[]) for c in staged.columns],
                write_grants=list(staged.write_grants),
            )
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


def cache_ttl_checked(model: "Table", cache_ttl: int | None) -> int | None:
    """``cache_ttl`` once it is one a table may have (REQ-2031), and, on a checker's table, the
    interval it scans on (REQ-1443). Raises TableSaveRefused."""
    from types import SimpleNamespace

    from provisa.api.app import state
    from provisa.core.cache_ttl import cache_ttl_refusal
    from provisa.dq.registration import is_checker_source_type, require_scan_interval

    problem = cache_ttl_refusal(cache_ttl)
    if problem is not None:
        raise TableSaveRefused(
            MutationResult(
                success=False,
                message=problem,
                code="schema.invalid_cache_ttl",
                params={"table": model.table_name},
            )
        )
    if is_checker_source_type((getattr(state, "source_types", None) or {}).get(model.source_id)):
        try:
            require_scan_interval(SimpleNamespace(table_name=model.table_name, cache_ttl=cache_ttl))
        except ValueError as exc:
            raise TableSaveRefused(
                MutationResult(
                    success=False,
                    message=str(exc),
                    code="dq.scan_interval_required",
                    params={"table": model.table_name},
                )
            ) from exc
    return cache_ttl


async def load_protection_refusal(
    conn: "Connection", model: "Table"
) -> MutationResult | None:  # REQ-1141
    """Why a table declaring load protection cannot be saved: protected, as declared or as its
    source has it, with no refresh gate armed (off-peak window, Cache TTL, probing signal) on the
    table or its source. None when it can, or when the table declares none of it."""
    from provisa.api.admin.schema_mutation import _validate_load_protection

    if model.load_protected is None and not model.off_peak_window:
        return None
    src = (
        await conn.execute_core(
            select(
                sources.c.load_protected,
                sources.c.cache_ttl,
                sources.c.change_signal,
                sources.c.off_peak_window,
            ).where(sources.c.id == model.source_id)
        )
    ).fetchone()
    effective = (
        (src.load_protected if src else None)
        if model.load_protected is None
        else (model.load_protected)
    )
    return _validate_load_protection(
        effective,
        model.off_peak_window or (src.off_peak_window if src else None),
        model.cache_ttl if model.cache_ttl is not None else (src.cache_ttl if src else None),
        model.change_signal or (src.change_signal if src else None),
        f"table {model.table_name}",
    )


async def save_carried_region(
    info: Any,
    conn: "Connection",
    inp: "TableInput",
    table_id: int,
    *,
    stored_region: str | None,
    stored_draft: bool,
    draft: bool,
) -> None:
    """Move a stored table to the region an update carries, judged as setTableRegion judges it:
    the caller's residency grant (a draft table, as stored or as saved now, is claimed by its
    destination alone) and the platform's regions. Raises TableSaveRefused."""
    from provisa.api.admin.schema_mutation import _residency_refusal
    from provisa.core.repositories import region as region_repo

    if not carried(inp.region) or inp.region == stored_region:
        return
    refusal = _residency_refusal(
        info, f"table {inp.table_name}", stored_region, inp.region, draft=stored_draft or draft
    )
    if refusal is not None:
        raise TableSaveRefused(refusal)
    try:
        await region_repo.set_table_region(conn, table_id, inp.region)
    except ValueError as refused:
        raise TableSaveRefused(
            MutationResult(
                success=False,
                message=str(refused),
                code="schema.region_refused",
                params={"table": table_id, "region": inp.region, "reason": str(refused)},
            )
        ) from refused


async def row_rule_refusal(
    conn: "Connection", role_id: str, description: str
) -> MutationResult | None:  # REQ-041, REQ-2081
    """Why a row rule cannot bind: it names no role, gives no description, or its role does not
    exist. None when it can."""
    from provisa.core.repositories import role as role_repo

    if not role_id:
        return MutationResult(
            success=False, message="RLS rule names no role", code="schema.rls_rule_no_role"
        )
    if not description.strip():
        return MutationResult(
            success=False,
            message="A row rule needs a description of what it is for",
            code="schema.rls_rule_description_missing",
        )
    if await role_repo.get(conn, role_id) is None:
        return MutationResult(
            success=False,
            message=f"Role {role_id!r} not found",
            code="schema.role_not_found",
            params={"role": role_id},
        )
    return None


async def save_row_rules(info: Any, conn: "Connection", inp: "TableInput", model: "Table") -> None:
    """The row rules a save carries, upserted on the stored table as upsertRlsRule saves one:
    gated by masking_config in its domain, each predicate resolved against the table. Raises
    TableSaveRefused."""
    from provisa.api.admin.capabilities import require_capability, require_right_in_domains
    from provisa.compiler.rls_validate import validate_rls_predicate
    from provisa.core.models import RLSRule
    from provisa.core.repositories import rls as rls_repo
    from provisa.core.repositories import table as table_repo

    if not carried(inp.row_rules):
        return
    if inp.row_rules is None:
        raise TableSaveRefused(_null_refused("rowRules"))
    if not inp.row_rules:
        return
    require_capability(info, "masking_config")
    require_right_in_domains(info, "masking_config", {model.domain_id})
    name = model.alias or model.table_name
    registry = await table_repo.list_all(conn)
    targets = [t for t in registry if (t.get("alias") or t["table_name"]) == name]
    rule: RLSRuleInput
    for rule in inp.row_rules:
        refusal = await row_rule_refusal(conn, rule.role_id, rule.description)
        if refusal is not None:
            raise TableSaveRefused(refusal)
        problem = validate_rls_predicate(rule.filter_expr, targets, registry)
        if problem is not None:
            raise TableSaveRefused(
                MutationResult(
                    success=False,
                    message=problem,
                    code="schema.rls_rule_invalid",
                    params={"reason": problem},
                )
            )
        try:
            await rls_repo.upsert(
                conn,
                RLSRule(
                    table_id=name,
                    role_id=rule.role_id,
                    filter=rule.filter_expr,
                    description=rule.description,
                    why=rule.why,  # REQ-2094
                ),
            )
        except ValueError as refused:
            raise TableSaveRefused(MutationResult(success=False, message=str(refused))) from refused


async def save_carried_paging(conn: "Connection", inp: "TableInput", table_id: int) -> None:
    """The paging an update carries, saved as updateTablePaging saves it (null clears it). Run
    last in the save: the endpoint this process serves the table from takes it too. Raises
    TableSaveRefused."""
    from provisa.api.admin._table_paging import save_table_paging
    from provisa.api.app import state

    if not carried(inp.pagination):
        return
    result = await save_table_paging(state, conn, table_id, inp.pagination)
    if not result.success:
        raise TableSaveRefused(result)
