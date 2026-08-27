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

# Requirements: REQ-1387, REQ-1590, REQ-1591

from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter, Query, Request

from provisa.api.admin._guards import require_active_org_id
from provisa.api.admin._platform_guard import _require_right
from provisa.api.errors import ApiError
from provisa.core.repositories import glossary as glossary_repo
from provisa.security.rights import Capability

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


def _require_glossary_read(request: Request) -> None:
    """REQ-1590: may this caller SEE the glossary?

    Its own right, not ``org_settings``: looking a term up to understand a column is not
    administering the org, and gating the surface on org settings shut every non-admin out of
    the vocabulary the model is described in.
    """
    _require_right(request, Capability.GLOSSARY_READ.value)


def _require_glossary_rw(request: Request) -> None:
    """REQ-1590: may this caller CURATE the glossary?

    Rename, definitions, ref moves, edges, experts, and the generation endpoints that persist.
    Granted alongside ``glossary_read`` — the page a curator works in is gated on read.
    """
    _require_right(request, Capability.GLOSSARY_RW.value)


def _authority(request: Request) -> frozenset[str] | None:
    """The domains this caller may reach, ``None`` for unlimited (REQ-1591)."""
    from provisa.api.admin.capabilities import allowed_domains_request

    return allowed_domains_request(request)


def _curate_authority(request: Request) -> frozenset[str] | None:
    """The domains this caller may CURATE in — where a role of theirs carries ``glossary_rw``.

    REQ-1592: narrower than :func:`_authority`, which unions domain_access over every role the
    caller holds regardless of which right came from where. Curation is the act being authorized,
    so it is curation's own scope that bounds it; otherwise a read-only role scoped to finance
    lends its domain to a ``glossary_rw`` role scoped to sales.
    """
    from provisa.api.admin.capabilities import allowed_domains_for_capability_request

    return allowed_domains_for_capability_request(request, Capability.GLOSSARY_RW.value)


def _is_org_curator(request: Request) -> bool:
    """Whether this caller holds ``org_glossary_rw`` — the override (REQ-1592).

    It is asked rather than required, because it does not admit anyone to the surface: it is what
    lets a curator past the two rules that would otherwise strand a term — the enterprise ``*``
    scope, which belongs to no domain, and a term whose authors have all departed.
    """
    from provisa.api.admin.capabilities import has_capability_request

    return has_capability_request(request, Capability.ORG_GLOSSARY_RW.value)


def _caller_id(request: Request) -> str | None:
    """The acting user id, or ``None`` when no authenticated identity resolved (dev / no-auth)."""
    identity = getattr(request.state, "identity", None)
    user_id = getattr(identity, "user_id", None)
    return None if user_id is None or user_id == "anonymous" else str(user_id)


def _view_scope(request: Request, selected: "list[str] | None") -> frozenset[str] | None:
    """The caller's authority INTERSECTED with the domains the navbar filter has selected.

    REQ-1591: the filter is a view preference and never widens authority, so a selection is
    intersected in, and an unlimited role narrowed by a selection sees only the selection. An
    absent parameter leaves authority alone — the UI sends none when every domain is checked.

    Repeated ``domains=`` parameters rather than one comma-joined string, because the no-domain
    domain's id IS the empty string (``schema.sql`` seeds it): joined, a selection of it and an
    empty selection are the same text, and the filter would silently mean the opposite of what
    was clicked.
    """
    allowed = _authority(request)
    if selected is None:
        return allowed
    chosen = frozenset(selected)
    return chosen if allowed is None else chosen & allowed


def _require_domain(request: Request, domain_id: str) -> None:
    """Refuse a TABLE's domain the caller may not act in (REQ-1591).

    Routed through :func:`_authority` rather than calling ``require_domain_request`` directly so
    this router asks its domain question in exactly one place — the table gate and the term gate
    then cannot disagree about who the caller is.
    """
    allowed = _authority(request)
    if allowed is not None and domain_id not in allowed:
        raise ApiError(403, "auth.domain_denied", f"No access to domain {domain_id!r}")


async def _require_term_readable(conn: "Connection", term_id: int, request: Request) -> None:
    """Refuse a term the caller's domains do not reach — ANY of its domains suffices (REQ-1591).

    A term is prose about a concept, so a term spanning two domains is readable by either domain's
    people; and an enterprise-wide term (``*``) is the org's shared vocabulary and is readable by
    every holder of ``glossary_read`` (REQ-1592). Curating is a stricter question, asked separately
    by :func:`_require_term_curatable`.
    """
    from provisa.core.glossary import readable_term

    allowed = _authority(request)
    if allowed is None:
        return
    scope = (await glossary_repo.term_domains(conn)).get(term_id, set())
    if not readable_term(allowed, scope):
        raise ApiError(403, "auth.domain_denied", f"term {term_id} is outside your domains")


async def _require_term_curatable(conn: "Connection", term_id: int, request: Request) -> None:
    """May this caller CHANGE this term? (REQ-1592)

    Three ways in, in this order:

    1. ``org_glossary_rw`` — the org's glossary owner, past every rule below.
    2. The caller is one of the term's AUTHORS. Claiming authorship is claiming the definition,
       so an author keeps their term wherever it is scoped.
    3. The term is UNCLAIMED (no authors), is not enterprise-wide, and one of the caller's
       ``glossary_rw`` domains is among its domains.

    Rule 3 is the old REQ-1591 gate, now bounded twice over. It stops at ``*`` because an
    enterprise-wide term belongs to no domain in particular, and the ANY rule would otherwise hand
    the org's shared vocabulary to whoever holds the narrowest domain it touches — declaring a term
    broadly would make it LEAST protected, which inverts the intent. And it stops at authorship
    because once someone has put their name to a definition, editing it over their head is an act
    for the glossary's owner, not for a passer-by who happens to share a domain.

    A term whose authors have all left the org is frozen to everyone but ``org_glossary_rw``. That
    is the intended cost of rule 2 and the reason the override exists.
    """
    from provisa.core.glossary import ENTERPRISE_DOMAIN, within_domains

    if _is_org_curator(request):
        return
    authors = await glossary_repo.term_authors(conn, term_id)
    if authors:
        caller = _caller_id(request)
        if caller is not None and caller in authors:
            return
        raise ApiError(
            403,
            "glossary.term_claimed",
            f"term {term_id} is authored by {', '.join(sorted(authors))}",
        )
    scope = (await glossary_repo.term_domains(conn)).get(term_id, set())
    if ENTERPRISE_DOMAIN in scope:
        raise ApiError(
            403,
            "auth.domain_denied",
            f"term {term_id} is enterprise-wide; org_glossary_rw is required to change it",
        )
    allowed = _curate_authority(request)
    if not within_domains(allowed, scope):
        raise ApiError(403, "auth.domain_denied", f"term {term_id} is outside your domains")


def _declared_domains(request: Request, raw: "Any", *, current: "set[str]") -> set[str]:
    """Validate the domains an abstract term is being declared in (REQ-1591, REQ-1592).

    An abstract term holds no refs, so nothing derives its scope and the declaration is the whole
    answer. In multi-domain mode at least one is required — an unscoped term is reachable by every
    glossary-rights holder, which would otherwise be a way to mint a term outside the gate. In
    single-domain mode a domain gates nothing and the empty declaration is the correct one.

    REQ-1592 adds the enterprise scope and tightens who may name a domain:

    * ``*`` is EXCLUSIVE. ``["*", "sales"]`` is a contradiction — enterprise-wide and also sales —
      and is refused rather than silently reduced to one of the two readings.
    * Declaring ``*``, or taking it away, takes ``org_glossary_rw``. Both directions, because the
      protection is worth nothing if any curator can lift it by rescoping the term into their own
      domain first and editing it afterwards.
    * Every ordinary domain named must be one the caller holds ``glossary_rw`` in, not merely one
      some role of theirs can reach. Declaring a term into a domain you cannot curate is how a
      member would widen their own scope by hand.

    ``current`` is the term's existing declared scope — empty for a create.

    ``raw`` is a value straight out of the request's JSON body, hence ``Any``: its type is what
    the client sent, which is exactly what this function exists to check.
    """
    from provisa.core import domain_policy
    from provisa.core.glossary import ENTERPRISE_DOMAIN

    if raw is None:
        declared: set[str] = set()
    elif isinstance(raw, list):
        declared = {str(d) for d in raw}
    else:
        raise ApiError(400, "glossary.invalid", "domains must be a list of domain ids")
    if ENTERPRISE_DOMAIN in declared and len(declared) > 1:
        raise ApiError(
            400,
            "glossary.invalid",
            f"{ENTERPRISE_DOMAIN!r} is the whole org and cannot be combined with a domain",
        )
    crossing_enterprise = (ENTERPRISE_DOMAIN in declared) != (ENTERPRISE_DOMAIN in current)
    if crossing_enterprise and not _is_org_curator(request):
        raise ApiError(
            403,
            "auth.missing_capability",
            "org_glossary_rw is required to make a term enterprise-wide or to narrow one",
        )
    if domain_policy.single_domain():
        return declared
    if not declared:
        raise ApiError(400, "glossary.invalid", "at least one domain is required")
    if ENTERPRISE_DOMAIN in declared or _is_org_curator(request):
        # REQ-1592: the override is org-wide, so it covers WHERE a term may be placed as well as
        # WHICH terms may be changed. Without this the org's glossary owner could take a term out
        # of enterprise scope but not put it anywhere — the one move the override exists to allow.
        return declared
    allowed = _curate_authority(request)
    if allowed is not None:
        outside = sorted(declared - allowed)
        if outside:
            raise ApiError(403, "auth.domain_denied", f"No access to domain {outside[0]!r}")
    return declared


@router.get("/terms")
async def list_terms(
    request: Request,
    q: str | None = None,
    include_deprecated: bool = True,
    domains: "list[str] | None" = Query(None),
) -> list[dict]:
    _require_glossary_read(request)
    require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        return await glossary_repo.list_terms(
            cast("Connection", conn),
            q=q,
            include_deprecated=include_deprecated,
            domains=_view_scope(request, domains),
        )


@router.get("/terms/{term_id}")
async def get_term(request: Request, term_id: int) -> dict:
    _require_glossary_read(request)
    require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        term = await glossary_repo.get_term(_conn, term_id)
        if term is None:
            raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
        await _require_term_readable(_conn, term_id, request)
    return term


@router.post("/terms")
async def create_abstract_term(request: Request) -> dict:
    """Create an abstract term — a user concept with no physical refs."""
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    domains = _declared_domains(request, body.get("domains"), current=set())
    pool = await _pool()
    async with pool.acquire() as conn:
        try:
            term_id = await glossary_repo.create_abstract_term(
                cast("Connection", conn),
                str(body.get("name", "")),
                definition=body.get("definition"),
                domains=domains,
            )
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    await _notify(org_id, "glossary term created")
    return {"id": term_id}


@router.patch("/terms/{term_id}")
async def update_term(request: Request, term_id: int) -> dict:
    """Rename, set the definition, or flip the export_excluded / retired flags."""
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        await _require_term_curatable(_conn, term_id, request)
        found = False
        try:
            if "domains" in body:
                # REQ-1591: DECLARED domains, so only an abstract term takes them. A rooted term's
                # domains are its refs' — writing rows for one would be a second answer that drifts
                # the moment a column moves; the way to rescope it is to move the refs.
                existing = await glossary_repo.get_term(_conn, term_id)
                if existing is None:
                    raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
                if not existing["is_abstract"]:
                    raise ApiError(
                        400,
                        "glossary.invalid",
                        "a rooted term's domains come from its refs and cannot be set",
                    )
                await glossary_repo.set_declared_domains(
                    _conn,
                    term_id,
                    # REQ-1592: the term's CURRENT scope decides whether this write crosses the
                    # enterprise boundary in either direction, so it is passed rather than re-read.
                    _declared_domains(request, body["domains"], current=set(existing["domains"])),
                )
                found = True
            if "name" in body:
                found = await glossary_repo.rename_term(_conn, term_id, str(body["name"]))
            if "definition" in body:
                found = await glossary_repo.set_definition(_conn, term_id, body["definition"])
            if "export_excluded" in body:
                found = await glossary_repo.set_export_excluded(
                    _conn, term_id, bool(body["export_excluded"])
                )
            if "retired" in body:
                found = await glossary_repo.set_retired(_conn, term_id, bool(body["retired"]))
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    if not found:
        raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
    await _notify(org_id, "glossary term updated")
    return {"ok": True}


@router.delete("/terms/{term_id}")
async def delete_term(request: Request, term_id: int) -> dict:
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        await _require_term_curatable(_conn, term_id, request)
        try:
            deleted = await glossary_repo.delete_term(_conn, term_id)
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    if not deleted:
        raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
    await _notify(org_id, "glossary term deleted")
    return {"ok": True}


def _require_table_registration(request: Request) -> None:
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
        _conn = cast("Connection", conn)
        # REQ-1591: the column's own table decides this one — the caller is asking about a column
        # it can already see on the Tables surface, and that surface is gated by the table's domain.
        from provisa.api.admin.domain_guard import table_domain

        _require_domain(request, await table_domain(_conn, table_id))
        term = await glossary_repo.get_term_by_ref(_conn, table_id, column_name)
    if term is None:
        raise ApiError(404, "glossary.ref_not_found", "no term for that column")
    return term


@router.post("/terms/{term_id}/definition/generate")
async def generate_definition(request: Request, term_id: int) -> dict:
    """Draft a definition for the term with the org's AI model.

    Generation only — nothing persists until the user saves the draft through the
    PATCH endpoint, so a bad draft costs nothing.
    """
    _require_glossary_rw(request)
    require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        term = await glossary_repo.get_term(_conn, term_id)
        if term is None:
            raise ApiError(404, "glossary.term_not_found", f"term {term_id} not found")
        await _require_term_curatable(_conn, term_id, request)
    refs = ", ".join(f"{r['alias'] or r['table_name']}.{r['column_name']}" for r in term["refs"])
    related = ", ".join(
        f"{e['rel_type']} {e['name']}" for e in term["edges_out"] + term["edges_in"]
    )
    kind = "an abstract business concept" if term["is_abstract"] else "a business term"
    prompt = (
        f"You are a data catalog assistant. Write a concise one-to-two sentence business "
        f"definition for {kind} named '{term['name']}' in an enterprise data glossary. "
        f"Define the business concept itself, in plain business language a non-technical "
        f"reader would use — never describe it as 'this field', 'this column', or any other "
        f"database-schema wording, even when physical columns are listed below for context. "
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
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    from provisa.api.admin.schema_helpers import _call_llm

    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        # REQ-1591: the bulk fill writes definitions, so it may only reach the terms this caller
        # may curate — narrowed here rather than skipped per-term, so the count it reports is the
        # count of what it was allowed to touch.
        terms = await glossary_repo.list_terms(_conn, domains=_authority(request))
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
                f"data glossary. Define the business concept itself, in plain business "
                f"language a non-technical reader would use — never describe it as 'this "
                f"field', 'this column', or any other database-schema wording, even when "
                f"physical columns are listed below for context. "
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
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    import json

    from provisa.api.admin.schema_helpers import _call_llm
    from provisa.core.glossary import TERM_EDGE_TYPES

    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        terms = await glossary_repo.list_terms(
            _conn, include_deprecated=False, domains=_authority(request)
        )  # REQ-1591: propose edges only within the caller's own domains
        if len(terms) < 2:
            return {"added": 0}
        by_name = {t["name"]: t["id"] for t in terms}
        term_lines = "\n".join(
            f"- {t['name']}" + (f": {t['definition']}" if t["definition"] else "") for t in terms
        )
        prompt = (
            "You are a data catalog assistant. Given this business-glossary term list, "
            "propose semantic relationships between terms. Allowed relationship types: "
            "KIND_OF (from is a kind of to), PART_OF (from is a part of to), "
            "SYNONYM_OF (interchangeable terms), RELATED_TO (loosely associated), "
            "VALID_VALUE_OF (from is an allowed value of the to enumeration/domain), "
            "DERIVED_FROM (from is computed or sourced from to), "
            "REPLACES (from supersedes the deprecated to), "
            "PREFERRED_TERM_FOR (from is the preferred term over the discouraged to), "
            "TRANSLATION_OF (from is a language/locale translation of to), "
            "ANTONYM_OF (from is the semantic opposite of to). "
            "Only propose relationships you are confident in; "
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
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        # REQ-1591: three checks, because a move touches three things — the table the ref belongs
        # to, the term losing it, and the term gaining it. The table's domain is the one that
        # decides whether this caller may re-point that column at all; the two terms are gated by
        # the same ANY rule every other curation act uses.
        from provisa.api.admin.domain_guard import table_domain

        _require_domain(request, await table_domain(_conn, int(body["table_id"])))
        losing = await glossary_repo.get_term_by_ref(
            _conn, int(body["table_id"]), str(body["column_name"])
        )
        if losing is None:
            raise ApiError(404, "glossary.ref_not_found", "physical ref not found")
        await _require_term_curatable(_conn, losing["id"], request)
        await _require_term_curatable(_conn, int(body["to_term_id"]), request)
        try:
            moved = await glossary_repo.move_ref(
                _conn,
                int(body["table_id"]),
                str(body["column_name"]),
                int(body["to_term_id"]),
            )
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    if moved is None:
        raise ApiError(404, "glossary.ref_not_found", "physical ref not found")
    await _notify(org_id, "glossary ref moved")
    # Moving a term's last ref retires it, so the losing term may no longer exist. Report that
    # here: the admin UI has it open and would otherwise re-read it and surface a bare 404.
    return {"ok": True, **moved}


@router.post("/terms/{term_id}/edges")
async def add_edge(request: Request, term_id: int) -> dict:
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        await _require_term_curatable(_conn, term_id, request)
        await _require_term_curatable(_conn, int(body["to_term_id"]), request)
        try:
            await glossary_repo.add_edge(
                _conn,
                term_id,
                int(body["to_term_id"]),
                str(body["rel_type"]),
            )
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    await _notify(org_id, "glossary edge added")
    return {"ok": True}


@router.patch("/terms/{term_id}/edges")
async def retype_edge(request: Request, term_id: int) -> dict:
    """Correct an existing relationship's type in place.

    The type is part of the edge's identity, so this is its own endpoint rather than a
    field on the add: retyping is one curation act on one statement, and the UI would
    otherwise have to delete and re-add, which is two publishes and a window where the
    relationship does not exist.
    """
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        await _require_term_curatable(_conn, term_id, request)
        try:
            changed = await glossary_repo.retype_edge(
                _conn,
                term_id,
                int(body["to_term_id"]),
                str(body["rel_type"]),
                str(body["new_rel_type"]),
            )
        except ValueError as exc:
            raise ApiError(400, "glossary.invalid", str(exc)) from exc
    if not changed:
        raise ApiError(404, "glossary.edge_not_found", "edge not found")
    await _notify(org_id, "glossary edge retyped")
    return {"ok": True}


@router.delete("/terms/{term_id}/edges")
async def remove_edge(request: Request, term_id: int, to_term_id: int, rel_type: str) -> dict:
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        await _require_term_curatable(_conn, term_id, request)
        removed = await glossary_repo.remove_edge(_conn, term_id, to_term_id, rel_type)
    if not removed:
        raise ApiError(404, "glossary.edge_not_found", "edge not found")
    await _notify(org_id, "glossary edge removed")
    return {"ok": True}


@router.post("/terms/{term_id}/experts")
async def add_expert(request: Request, term_id: int) -> dict:
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    body = await request.json()
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        await _require_term_curatable(_conn, term_id, request)
        try:
            await glossary_repo.add_expert(
                _conn,
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
    _require_glossary_rw(request)
    org_id = require_active_org_id(request)
    pool = await _pool()
    async with pool.acquire() as conn:
        _conn = cast("Connection", conn)
        await _require_term_curatable(_conn, term_id, request)
        removed = await glossary_repo.remove_expert(_conn, term_id, user_id)
    if not removed:
        raise ApiError(404, "glossary.expert_not_found", "expert not found")
    await _notify(org_id, "glossary expert removed")
    return {"ok": True}
