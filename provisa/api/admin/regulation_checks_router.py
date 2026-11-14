# Copyright (c) 2026 Kenneth Stott
# Canary: b18f5d3e-7a2c-4e96-8d04-c3e9a1f6b257
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Keeping a regulation's check on a column's values as a data quality rule (REQ-2245).

``POST /admin/tables/{table_id}/regulation-checks`` -- with ``dq_rw`` and nothing else -- adds a
Soda ``failed_rows`` check (``expression`` the condition, a stable qualifier its identity) to the
checker table that scans the table, and cites the library section the condition came from, in one
transaction (:mod:`provisa.dq.regulation`). It is refused, storing nothing:

- by name, when no checker scans the table, or only Great Expectations ones (a Soda one is needed);
- when the condition cannot be parsed, or reads anything but the named column -- another column,
  another table, a subquery -- since the stored failed-rows count depends on all it reads;
- unless the column is read in plain by the caller, by the checker's own pgwire user, and by every
  role served the checker table's results: each is read from that role's governance context, the
  one the query guard checks a statement against (:func:`provisa.api.mcp.column_state.column_state`);
- when the cited document or section is not there.
"""

# Requirements: REQ-2245

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import select

from provisa.api.admin.capabilities import require_capability_request
from provisa.api.app import state
from provisa.api.errors import ApiError

router = APIRouter(prefix="/admin", tags=["admin", "data-quality"])

_REFUSED = "dq.regulation_check_refused"


class RegulationCheckInput(BaseModel):
    # The column the condition is on: its published or registered name.
    column: str
    # A SQL condition true of the rows that may not comply.
    condition: str
    # The cited library section: a document and its heading, or its page when it has none.
    documentId: int
    sectionHeading: str | None = None
    sectionPage: int | None = None
    # The checker table to add to; may be left out where exactly one Soda checker scans the table.
    checkerTableId: int | None = None


def _db() -> Any:
    if state.model_db is None:
        raise ApiError(503, "dq.database_unavailable", "Database unavailable")
    return state.model_db


def _governance(role: str) -> Any:
    """``role``'s governance context: the one the query pipeline checks its statements against."""
    from provisa.api.role_surface import built_surface
    from provisa.pgwire.governed_plan import role_governance

    return role_governance(state, built_surface(state.surfaces[role]), role)


def _state_of(role: str, table_id: int, column: str) -> str:
    from provisa.api.mcp.column_state import column_state

    return column_state(_governance(role), table_id, column)


def _refused(message: str) -> ApiError:
    return ApiError(409, _REFUSED, message)


async def _checker_role(conn: Any, source_id: str) -> tuple[str, str]:
    """``(user, acting role)`` of the pgwire sign-in a checker source's scans make: its stored
    user, mapped to a role as pgwire maps a sign-in (provisa.pgwire.server)."""
    import jwt

    from provisa.auth.models import validator_for_scheme
    from provisa.auth.role_mapping import resolve_role
    from provisa.auth.wiring import build_auth_provider
    from provisa.core.schema_org import sources
    from provisa.core.secrets_store import bound_to_request_org
    from provisa.dq.scan import checker_connection
    from provisa.pgwire.server import credential_presentation, trust_mode, trusted_roles
    from provisa.security.meta_role import ensure_meta_role

    mapping = (
        await conn.execute_core(select(sources.c.mapping).where(sources.c.id == source_id))
    ).scalar_one()
    async with bound_to_request_org():
        connection = checker_connection(mapping)
    user = connection["user"]
    cannot = f"the checker's user {user!r} cannot sign in"
    if trust_mode(state):
        try:
            roles, qualifiers = trusted_roles(state, user)[1:]
        except PermissionError as exc:
            raise _refused(f"{cannot}: {exc}") from None
    else:
        auth_config = state.auth_config
        assert auth_config is not None  # trust_mode is False only with one
        default_role = auth_config.get("default_role")
        if not default_role:
            raise _refused(f"{cannot}: pgwire sign-in requires auth.default_role")
        provider = build_auth_provider(auth_config, admin_pool=state.admin_db)
        scheme, token = credential_presentation(
            auth_config["provider"], user, connection["password"]
        )
        validator = validator_for_scheme(provider, scheme)
        if validator is None:
            raise _refused(f"{cannot}: the provider takes no {scheme} credential")
        try:
            identity = await validator(token)
        except (ValueError, jwt.PyJWTError):
            raise _refused(f"{cannot} with its stored credential") from None
        roles = (resolve_role(identity, auth_config.get("role_mapping", []), default_role),)
        qualifiers = ()
    try:
        return user, ensure_meta_role(state, list(roles), qualifiers)
    except PermissionError as exc:
        raise _refused(f"{cannot}: {exc}") from None


async def _result_readers(conn: Any, checker_table_id: int) -> list[str]:
    """The roles served the checker table's results: its stored grant, expanded and narrowed as
    the schema build does (security.rights.served_roles)."""
    from provisa.core.schema_org import registered_tables as rt
    from provisa.core.schema_org import table_columns as tc
    from provisa.security.rights import served_roles

    domain_id = (
        await conn.execute_core(select(rt.c.domain_id).where(rt.c.id == checker_table_id))
    ).scalar_one()
    columns = (
        await conn.execute_core(
            select(tc.c.column_name, tc.c.visible_to, tc.c.scope).where(
                tc.c.table_id == checker_table_id
            )
        )
    ).fetchall()
    readers: set[str] = set()
    for c in columns:
        readers |= served_roles(state.roles, state.role_chains, domain_id, dict(c._mapping))
    return sorted(readers)


def _citation_doc(c: Any) -> dict:
    return {
        "documentId": c.document_id,
        "documentTitle": c.document_title,
        "documentSource": c.document_source,
        "sectionHeading": c.section_heading,
        "sectionPage": c.section_page,
        "textSha256": c.text_sha256,
        "citedBy": c.cited_by,
        "citedAt": c.cited_at.isoformat(),
        "state": c.state,
    }


@router.post("/tables/{table_id}/regulation-checks")
async def keep_regulation_check(
    request: Request, table_id: int, body: RegulationCheckInput
) -> dict:
    require_capability_request(request, "dq_rw")
    from provisa.api.admin.profiler_checks_router import _checker_doc, _readable_checkers
    from provisa.api.admin.read_rights import served_columns_request
    from provisa.compiler.stage2 import registered_name
    from provisa.core.schema_org import registered_tables as rt
    from provisa.dq import regulation
    from provisa.dq.contract import model_contexts
    from provisa.profiler import export
    from provisa.reference_library import citations, library

    if not body.condition.strip():
        raise ApiError(422, "dq.regulation_check_invalid", "the condition is empty")
    if (body.sectionHeading is None) == (body.sectionPage is None):
        raise ApiError(
            422,
            "dq.regulation_check_invalid",
            "name the cited section by its heading or, when it has none, its page",
        )
    role = getattr(request.state, "role", None)
    if role is None:
        raise _refused("the request acts as no role, so no column is read in plain by it")
    place = citations.Place(body.documentId, body.sectionHeading, body.sectionPage)
    identity = getattr(request.state, "identity", None)
    async with _db().acquire() as conn:
        name = (
            await conn.execute_core(select(rt.c.table_name).where(rt.c.id == table_id))
        ).scalar_one_or_none()
        # REQ-2152: a table the caller is served no column of is answered as one that is not there.
        if name is None or await served_columns_request(conn, request, table_id) == set():
            raise ApiError(404, "dq.table_not_found", f"No table {table_id}")
        what = f"table {name!r}"
        gov = _governance(role)
        column = registered_name(gov, table_id, body.column)

        def _is_the_column(ref: Any) -> bool:
            # Named as the plain-read test names it (registered_name), qualified by nothing or by
            # a name the caller's statements give this table.
            qualifier = ".".join(p.name for p in ref.parts[:-1])
            return registered_name(gov, table_id, ref.name) == column and (
                not qualifier or qualifier == name or gov.table_map.get(qualifier) == table_id
            )

        try:
            reads_more = regulation.condition_refusal(body.condition, body.column, _is_the_column)
        except regulation.ConditionUnreadable as exc:
            raise ApiError(422, "dq.regulation_check_invalid", str(exc)) from None
        if reads_more is not None:
            raise _refused(reads_more)
        found = await _readable_checkers(
            conn, request, await export.checkers_scanning(conn, model_contexts(state), {table_id})
        )
        soda = [c for c in found if c.checker == regulation.SODA]
        named_gx = body.checkerTableId is not None and any(
            c.id == body.checkerTableId and c.checker != regulation.SODA for c in found
        )
        if found and (not soda or named_gx):
            others = ", ".join(
                f"{c.table_name} (id {c.id})" for c in found if c.checker != regulation.SODA
            )
            raise _refused(
                f"a Soda checker is needed to keep a condition on {what}; it is scanned by "
                f"Great Expectations checker tables only ({others})"
                if not soda
                else f"checker table {body.checkerTableId} is a Great Expectations checker; a "
                f"Soda checker is needed to keep a condition on {what}"
            )
        try:
            checker = export.pick(soda, body.checkerTableId, what)
        except export.ExportRefused as exc:
            raise _refused(str(exc)) from exc
        checker_user, checker_role = await _checker_role(conn, checker.source_id)
        readers = [("you", _state_of(role, table_id, column))]
        readers.append(
            (f"the checker's user {checker_user!r}", _state_of(checker_role, table_id, column))
        )
        readers += [
            (f"role {r!r}", _state_of(r, table_id, column))
            for r in await _result_readers(conn, checker.id)
        ]
        not_plain = [(who, s) for who, s in readers if s != "plain"]
        if not_plain:
            raise _refused(regulation.not_plain_text(body.column, not_plain))
        qualifier = regulation.regulation_qualifier(table_id, column, body.condition, place.key())
        key = regulation.check_object_key(
            regulation.CheckKey(checker.id, qualifier, table_id, column)
        )
        try:
            async with conn.transaction():
                if not await export.add_check(
                    conn, checker, regulation.regulation_check(qualifier, body.condition)
                ):
                    raise _refused(
                        f"checker table {checker.table_name!r} already keeps this condition on "
                        f"column {body.column!r} citing that section"
                    )
                cited = await citations.cite(
                    conn,
                    citations.DQ_CHECK,
                    None,
                    key,
                    place,
                    getattr(identity, "user_id", None),
                )
        except library.LibraryRefused as exc:  # no such document, or no such section of it
            raise ApiError(exc.status, exc.code, str(exc)) from exc
    from provisa.api.app import _rebuild_schemas

    await _rebuild_schemas()
    return {
        "checkerTable": _checker_doc(checker),
        "qualifier": qualifier,
        "objectKey": key,
        "column": column,
        "condition": body.condition,
        "citation": _citation_doc(cited),
    }
