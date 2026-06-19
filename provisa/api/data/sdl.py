# Copyright (c) 2026 Kenneth Stott
# Canary: cdba9e0f-f70d-4401-9655-786a35ca0b5e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/sdl and /data/introspection endpoints — role-aware GraphQL SDL and introspection."""

from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from graphql import graphql_sync, print_schema

router = APIRouter()


def _reachable_table_ids(domain_id: str, tables: list[dict], relationships: list[dict]) -> set[int]:
    """Directed BFS from tables in domain_id, following relationships source→target only.

    Undirected traversal would follow cross-domain relationships backward (e.g. pet_store→shelter
    traversed in reverse from shelter), pulling in unrelated domains. Directed BFS ensures we only
    reach tables that the seed domain's tables point *to*, not tables that point *at* them.

    Intra-domain relationships are also built bidirectionally so navigation within a domain works
    regardless of which end was designated source/target at registration time.
    """
    table_domain: dict[int, str] = {t["id"]: t["domain_id"] for t in tables}
    domain_ids = {tid for tid, did in table_domain.items() if did == domain_id}
    reachable = set(domain_ids)

    # Forward edges for all relationships; also reverse edges for intra-domain pairs.
    adj: dict[int, set[int]] = {}
    for rel in relationships:
        src, tgt = rel["source_table_id"], rel["target_table_id"]
        adj.setdefault(src, set()).add(tgt)
        if table_domain.get(src) == table_domain.get(tgt):
            adj.setdefault(tgt, set()).add(src)

    frontier = set(domain_ids)
    while frontier:
        next_f: set[int] = set()
        for tid in frontier:
            for nb in adj.get(tid, set()):
                if nb not in reachable:
                    reachable.add(nb)
                    next_f.add(nb)
        frontier = next_f
    return reachable


_ALWAYS_VISIBLE_DOMAINS = {"meta", "ops"}


def _build_domain_schema(role: dict, domain_ids: list[str], cache: dict):
    from provisa.api.app import state
    from provisa.compiler.schema_gen import SchemaInput, generate_schema

    tables = cache["tables"]
    relationships = cache["relationships"]
    always_ids = {t["id"] for t in tables if t["domain_id"] in _ALWAYS_VISIBLE_DOMAINS}
    reachable: set[int] = set(always_ids)
    seed_ids: set[int] = set()
    for domain_id in domain_ids:
        reachable |= _reachable_table_ids(domain_id, tables, relationships)
        seed_ids |= {t["id"] for t in tables if t["domain_id"] == domain_id}
    reachable |= seed_ids
    filtered_tables = [t for t in tables if t["id"] in reachable]
    root_ids = seed_ids
    # Ensure always-visible domains and the requested domains bypass per-role
    # domain_access check in _build_visible_tables (which skips inaccessible domains).
    existing = role.get("domain_access") or []
    if "*" not in existing:
        role = {
            **role,
            "domain_access": list(set(existing) | _ALWAYS_VISIBLE_DOMAINS | set(domain_ids)),
        }
    si = SchemaInput(
        tables=filtered_tables,
        root_table_ids=root_ids,
        relationships=relationships,
        column_types=cache["column_types"],
        naming_rules=cache["naming_rules"],
        role=role,
        domains=cache["domains"],
        source_types=state.source_types,
        domain_prefix=cache["domain_prefix"],
        physical_table_map=cache["physical_table_map"],
        functions=cache["functions"],
        webhooks=cache["webhooks"],
        enum_types=cache["enum_types"],
        governed_gql_types={
            tbl.get("gql_type_name")
            for reg in getattr(state, "graphql_remote_sources", {}).values()
            for tbl in reg.get("tables", [])
            if tbl.get("gql_type_name")
        },
    )
    return generate_schema(si)


@router.get("/data/schema-version")
async def get_schema_version():
    """Return the current schema version. Combines a per-boot nonce with the rebuild counter so
    sessionStorage cache entries are always invalidated after a server restart."""
    from provisa.api.app import state

    version = (
        f"{state.schema_boot_id}-{state.schema_version}"
        if state.schema_boot_id
        else str(state.schema_version)
    )
    return JSONResponse({"version": version})


@router.get("/data/domains")
async def get_domains(request: Request, x_role: str = Header(None, alias="X-Role")):
    """Return domain IDs accessible to the requesting role."""
    from provisa.api.app import state

    auth_role = getattr(request.state, "role", None)
    role_id = auth_role or x_role
    if role_id is None:
        raise HTTPException(status_code=422, detail="Missing X-Role header")
    role = state.roles.get(role_id)
    if role is None:
        raise HTTPException(status_code=404, detail=f"No role {role_id!r}")
    all_domains = [
        d["id"] for d in (state.schema_build_cache.get("domains") or []) if d["id"] != ""
    ]
    access = role.get("domain_access") or []
    if "*" in access:
        return JSONResponse(all_domains)
    return JSONResponse([d for d in all_domains if d in set(access)])


@router.get("/data/sdl", response_class=PlainTextResponse)
async def get_sdl(
    request: Request,
    x_role: str = Header(None, alias="X-Role"),
    domain: str | None = Query(None),
):
    """Return the GraphQL SDL for the requesting role's schema, optionally filtered to a domain."""
    from provisa.api.app import state

    auth_role = getattr(request.state, "role", None)
    role_id = auth_role or x_role
    if role_id is None:
        raise HTTPException(status_code=422, detail="Missing X-Role header")

    domain_list = [d for d in (domain or "").split(",") if d and d != "all"]
    if domain_list:
        role = state.roles.get(role_id)
        if role is None:
            raise HTTPException(status_code=404, detail=f"No role {role_id!r}")
        if not state.schema_build_cache:
            raise HTTPException(status_code=503, detail="Schema build cache not ready")
        schema = _build_domain_schema(role, domain_list, state.schema_build_cache)
    else:
        schema = state.schemas.get(role_id)
        if schema is None:
            raise HTTPException(status_code=404, detail=f"No schema for role {role_id!r}")
    return print_schema(schema)


_INTROSPECTION_QUERY = """
query IntrospectionQuery {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types { ...FullType }
    directives { name description locations args { ...InputValue } }
  }
}
fragment FullType on __Type {
  kind name description
  fields(includeDeprecated: true) {
    name description args { ...InputValue }
    type { ...TypeRef }
    isDeprecated deprecationReason
  }
  inputFields { ...InputValue }
  interfaces { ...TypeRef }
  enumValues(includeDeprecated: true) { name description isDeprecated deprecationReason }
  possibleTypes { ...TypeRef }
}
fragment InputValue on __InputValue {
  name description type { ...TypeRef } defaultValue
}
fragment TypeRef on __Type {
  kind name
  ofType { kind name ofType { kind name ofType { kind name ofType {
    kind name ofType { kind name ofType { kind name ofType { kind name } } }
  } } } }
}
"""


@router.get("/data/introspection")
async def get_introspection(
    request: Request,
    x_role: str = Header(None, alias="X-Provisa-Role"),
    domain: str | None = Query(None),
):
    """Return GraphQL introspection JSON, optionally filtered to a domain + reachable tables."""
    from provisa.api.app import state

    auth_role = getattr(request.state, "role", None)
    role_id = auth_role or x_role
    if role_id is None:
        raise HTTPException(status_code=422, detail="Missing X-Provisa-Role header")

    domain_list = [d for d in (domain or "").split(",") if d and d != "all"]
    if domain_list:
        role = state.roles.get(role_id)
        if role is None:
            raise HTTPException(status_code=404, detail=f"No role {role_id!r}")
        if not state.schema_build_cache:
            raise HTTPException(status_code=503, detail="Schema build cache not ready")
        schema = _build_domain_schema(role, domain_list, state.schema_build_cache)
    else:
        schema = state.schemas.get(role_id)
        if schema is None:
            raise HTTPException(status_code=404, detail=f"No schema for role {role_id!r}")

    result = graphql_sync(schema, _INTROSPECTION_QUERY)
    if result.errors:
        raise HTTPException(status_code=500, detail=str(result.errors[0]))
    return JSONResponse({"data": result.data})
