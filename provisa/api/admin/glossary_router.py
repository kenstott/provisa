# Copyright (c) 2026 Kenneth Stott
# Canary: 5e9f2a41-7c8d-4b3e-a6f0-2d1c8b4e9a55
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Business-glossary admin surface (REQ-1387): term curation over the derived vocabulary.

Terms themselves are lifecycle-managed by the semantic layer (the table repository
creates/links and removes/deprecates them); this router carries the human curation —
rename, definition, ref moves, experts, abstract terms and their typed edges — and
queues a metadata publish after every mutation because the term graph exports.
"""

# Requirements: REQ-1387

from typing import TYPE_CHECKING, cast

from fastapi import APIRouter, Request

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import require_org_settings
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo

if TYPE_CHECKING:
    from provisa.core.database import Connection, Database

router = APIRouter(prefix="/admin/glossary", tags=["admin", "glossary"])


async def _pool() -> "Database":
    from provisa.api.app import state

    assert state.tenant_db is not None
    return state.tenant_db


async def _notify(org_id: str, reason: str) -> None:
    from provisa.api.metadata_export.publishing import notify_model_changed

    await notify_model_changed(org_id, reason=reason)


@router.get("/terms")
async def list_terms(
    request: Request, q: str | None = None, include_deprecated: bool = True
) -> list[dict]:
    require_org_settings(request)
    require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        return await glossary_repo.list_terms(
            cast("Connection", conn), q=q, include_deprecated=include_deprecated
        )


@router.get("/terms/{term_id}")
async def get_term(request: Request, term_id: int) -> dict:
    require_org_settings(request)
    require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        term = await glossary_repo.get_term(cast("Connection", conn), term_id)
    if term is None:
        raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
    return term


@router.post("/terms")
async def create_abstract_term(request: Request) -> dict:
    """Create an abstract term — a user concept with no physical refs."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        try:
            term_id = await glossary_repo.create_abstract_term(
                cast("Connection", conn),
                str(body.get("name", "")),
                definition=body.get("definition"),
            )
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    await _notify(org_id, "glossary term created")
    return {"id": term_id}


@router.patch("/terms/{term_id}")
async def update_term(request: Request, term_id: int) -> dict:
    """Rename and/or set the definition."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        found = False
        try:
            if "name" in body:
                found = await glossary_repo.rename_term(_conn, term_id, str(body["name"]))
            if "definition" in body:
                found = await glossary_repo.set_definition(_conn, term_id, body["definition"])
            if "export_excluded" in body:
                found = await glossary_repo.set_export_excluded(
                    _conn, term_id, bool(body["export_excluded"])
                )
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    if not found:
        raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
    await _notify(org_id, "glossary term updated")
    return {"ok": True}


@router.delete("/terms/{term_id}")
async def delete_term(request: Request, term_id: int) -> dict:
    require_org_settings(request)
    org_id = require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        try:
            deleted = await glossary_repo.delete_term(cast("Connection", conn), term_id)
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    if not deleted:
        raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
    await _notify(org_id, "glossary term deleted")
    return {"ok": True}


def _require_table_registration(request: Request) -> None:
    from provisa.api.admin._platform_guard import _require_right
    from provisa.security.rights import Capability

    _require_right(request, Capability.TABLE_REGISTRATION.value)


@router.get("/ref")
async def term_for_ref(request: Request, table_id: int, column_name: str) -> dict:
    """The glossary summary for one physical column — the hover popup's lookup.

    Gated on table_registration rather than org_settings: it serves the Tables surface
    read-only, and a term summary reveals nothing the column list doesn't already.
    """
    _require_table_registration(request)
    require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        term = await glossary_repo.get_term_by_ref(
            cast("Connection", conn), table_id, column_name
        )
    if term is None:
        raise ApiError(404, "glossary.ref_not_found", "no term for that column")
    return term


@router.post("/terms/{term_id}/definition/generate")
async def generate_definition(request: Request, term_id: int) -> dict:
    """Draft a definition for the term with the org's AI model.

    Generation only — nothing persists until the user saves the draft through the
    PATCH endpoint, so a bad draft costs nothing.
    """
    require_org_settings(request)
    require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        term = await glossary_repo.get_term(cast("Connection", conn), term_id)
    if term is None:
        raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
    refs = ", ".join(
        f"{r['alias'] or r['table_name']}.{r['column_name']}" for r in term["refs"]
    )
    related = ", ".join(
        f"{e['rel_type']} {e['name']}" for e in term["edges_out"] + term["edges_in"]
    )
    kind = "an abstract business concept" if term["is_abstract"] else "a business term"
    prompt = (
        f"You are a data catalog assistant. Write a concise one-to-two sentence business "
        f"definition for {kind} named '{term['name']}' in an enterprise data glossary. "
        + (f"It is bound to these physical columns: {refs}. " if refs else "")
        + (f"Related terms: {related}. " if related else "")
        + "Respond with only the definition text, no preamble."
    )
    from provisa.api.admin.schema_helpers import _call_llm

    definition = await _call_llm(prompt, "glossary_definition", max_tokens=256)
    return {"definition": definition}


@router.post("/definitions/generate")
async def generate_all_definitions(request: Request) -> dict:
    """Draft-and-save definitions for every term that has none.

    Bulk generation persists directly — unlike the per-term draft there is no editor
    holding the result — but only ever fills EMPTY definitions; human text is never
    overwritten. One publish notification covers the batch.
    """
    require_org_settings(request)
    org_id = require_active_org_id(request)
    from provisa.api.admin.schema_helpers import _call_llm

    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        terms = await glossary_repo.list_terms(_conn)
        generated = 0
        for summary in terms:
            if summary["definition"]:
                continue
            term = await glossary_repo.get_term(_conn, summary["id"])
            if term is None:
                continue
            refs = ", ".join(
                f"{r['alias'] or r['table_name']}.{r['column_name']}" for r in term["refs"]
            )
            related = ", ".join(
                f"{e['rel_type']} {e['name']}" for e in term["edges_out"] + term["edges_in"]
            )
            kind = "an abstract business concept" if term["is_abstract"] else "a business term"
            prompt = (
                f"You are a data catalog assistant. Write a concise one-to-two sentence "
                f"business definition for {kind} named '{term['name']}' in an enterprise "
                f"data glossary. "
                + (f"It is bound to these physical columns: {refs}. " if refs else "")
                + (f"Related terms: {related}. " if related else "")
                + "Respond with only the definition text, no preamble."
            )
            definition = await _call_llm(prompt, "glossary_definition", max_tokens=256)
            if definition.strip():
                await glossary_repo.set_definition(_conn, term["id"], definition.strip())
                generated += 1
    if generated:
        await _notify(org_id, "glossary definitions generated")
    return {"generated": generated}


@router.post("/relationships/generate")
async def generate_relationships(request: Request) -> dict:
    """Suggest-and-save typed edges (KIND_OF/PART_OF/...) across the whole glossary.

    The org's AI model reads the full term list and proposes edges within the closed
    rel-type set; anything malformed — unknown term, self-edge, free-form type — is
    dropped, existing edges upsert idempotently, and one notification covers the batch.
    """
    require_org_settings(request)
    org_id = require_active_org_id(request)
    import json

    from provisa.api.admin.schema_helpers import _call_llm
    from provisa.core.glossary import TERM_EDGE_TYPES

    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        terms = await glossary_repo.list_terms(_conn, include_deprecated=False)
        if len(terms) < 2:
            return {"added": 0}
        by_name = {t["name"]: t["id"] for t in terms}
        term_lines = "\n".join(
            f"- {t['name']}" + (f": {t['definition']}" if t["definition"] else "")
            for t in terms
        )
        prompt = (
            "You are a data catalog assistant. Given this business-glossary term list, "
            "propose semantic relationships between terms. Allowed relationship types: "
            "KIND_OF (from is a kind of to), PART_OF (from is a part of to), "
            "SYNONYM_OF, RELATED_TO. Only propose relationships you are confident in; "
            "fewer, correct edges beat many speculative ones.\n\n"
            f"Terms:\n{term_lines}\n\n"
            'Respond with only a JSON array like [{"from": "term name", "to": "term name", '
            '"rel_type": "KIND_OF"}] — no prose, no code fences.'
        )
        raw = await _call_llm(prompt, "glossary_relationships", max_tokens=2048)
        raw = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```")
        try:
            proposals = json.loads(raw)
        except ValueError as exc:
            raise ApiError(
                502, "glossary.generation_unparseable", f"Model response was not JSON: {exc}"
            ) from exc
        if not isinstance(proposals, list):
            raise ApiError(502, "glossary.generation_unparseable", "Model response was not a list")
        added = 0
        for p in proposals:
            if not isinstance(p, dict):
                continue
            from_id = by_name.get(p.get("from"))
            to_id = by_name.get(p.get("to"))
            rel_type = p.get("rel_type")
            if from_id is None or to_id is None or from_id == to_id:
                continue
            if rel_type not in TERM_EDGE_TYPES:
                continue
            await glossary_repo.add_edge(_conn, from_id, to_id, rel_type)
            added += 1
    if added:
        await _notify(org_id, "glossary relationships generated")
    return {"added": added}


@router.post("/refs/move")
async def move_ref(request: Request) -> dict:
    """Move one physical ref to another term (consolidation)."""
    require_org_settings(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        try:
            moved = await glossary_repo.move_ref(
                cast("Connection", conn),
                int(body["table_id"]),
                str(body["column_name"]),
                int(body["to_term_id"]),
            )
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    if not moved:
        raise ApiError(404, "glossary.ref_not_found", "physical ref not found")
    await _notify(org_id, "glossary ref moved")
    return {"ok": True}


@router.post("/terms/{term_id}/edges")
async def add_edge(request: Request, term_id: int) -> dict:
    require_org_settings(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        try:
            await glossary_repo.add_edge(
                cast("Connection", conn),
                term_id,
                int(body["to_term_id"]),
                str(body["rel_type"]),
            )
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    await _notify(org_id, "glossary edge added")
    return {"ok": True}


@router.delete("/terms/{term_id}/edges")
async def remove_edge(request: Request, term_id: int, to_term_id: int, rel_type: str) -> dict:
    require_org_settings(request)
    org_id = require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        removed = await glossary_repo.remove_edge(
            cast("Connection", conn), term_id, to_term_id, rel_type
        )
    if not removed:
        raise ApiError(404, "glossary.edge_not_found", "edge not found")
    await _notify(org_id, "glossary edge removed")
    return {"ok": True}


@router.post("/terms/{term_id}/experts")
async def add_expert(request: Request, term_id: int) -> dict:
    require_org_settings(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        try:
            await glossary_repo.add_expert(
                cast("Connection", conn),
                term_id,
                str(body["user_id"]),
                kind=str(body.get("kind", "expert")),
            )
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    await _notify(org_id, "glossary expert added")
    return {"ok": True}


@router.delete("/terms/{term_id}/experts/{user_id}")
async def remove_expert(request: Request, term_id: int, user_id: str) -> dict:
    require_org_settings(request)
    org_id = require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        removed = await glossary_repo.remove_expert(cast("Connection", conn), term_id, user_id)
    if not removed:
        raise ApiError(404, "glossary.expert_not_found", "expert not found")
    await _notify(org_id, "glossary expert removed")
    return {"ok": True}
