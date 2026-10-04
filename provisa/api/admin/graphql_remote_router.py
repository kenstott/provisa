# Copyright (c) 2026 Kenneth Stott
# Canary: dbd213fd-531e-44d6-941b-179405293d2c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Admin routes for GraphQL Remote Schema Connector (Phase AP).

Endpoints:
  POST /admin/sources/graphql-remote          — register source (introspect + auto-register),
                                                or add a branded source (registers no tables)
  POST /admin/sources/graphql-remote/{id}/refresh — re-introspect, update registrations
  GET  /admin/sources/graphql-remote/brands   — the branded sources this build carries
"""

# Requirements: REQ-307, REQ-308, REQ-310, REQ-311, REQ-312, REQ-313, REQ-597, REQ-598, REQ-599, REQ-600, REQ-602, REQ-1923

from __future__ import annotations
import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import and_, select

from provisa.api.errors import ApiError
from provisa.core.schema_org import domains, registered_tables, sources, table_columns
from provisa.api.admin.capabilities import require_capability_request
from provisa.api.admin.schema_common import remote_source_counts

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/sources/graphql-remote", tags=["admin", "graphql-remote"])


class GraphQLRemoteSourceRequest(BaseModel):
    source_id: str
    # REQ-1923: a branded source names its brand and supplies only a credential; the endpoint
    # is the brand's, and the namespace is the brand's unless one is given.
    brand: str | None = None
    url: str = ""
    namespace: str = ""
    domain_id: str = ""
    auth: dict | None = None
    cache_ttl: int = 300
    description: str = ""
    field_overrides: dict[str, str] = {}  # {"fieldName": "query" | "mutation"}
    relationships: list[dict] = []


class GraphQLRemoteRegistration(BaseModel):
    source_id: str
    url: str
    namespace: str
    domain_id: str = ""
    auth: dict | None = None
    cache_ttl: int = 300
    tables: list[dict] = []
    functions: list[dict] = []
    relationships: list[dict] = []


async def _verify_live_auth(url: str, auth: dict | None, verify_query: str) -> None:
    """Confirm the credential works against ``url`` with a minimal query. A branded source's
    schema ships with Provisa, so this is the only thing its registration asks of the remote."""
    import httpx

    from provisa.graphql_remote.introspect import _build_headers

    headers = {"Content-Type": "application/json", **_build_headers(auth)}
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(url, json={"query": verify_query}, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    if "errors" in data:
        raise ValueError(f"{data['errors']}")
    answered = (data.get("data") or {}).values()
    if not answered or any(value is None for value in answered):
        # A remote that also serves anonymous callers answers a who-am-I query with null
        # rather than an error when it does not recognize the credential.
        raise ValueError("the remote did not recognize the credential")


async def _resolved_credential(value: str) -> str:
    """A credential as entered: itself, or what its ``${env:...}``/``${secret:...}`` reference
    names (a vault reference resolves in the requesting org's vault)."""
    if "${" not in value:
        return value
    from provisa.core.secrets import resolve_secrets

    if "${secret:" in value or "${user:" in value:
        from provisa.core.secrets_store import bound_to_request_org

        async with bound_to_request_org():
            return resolve_secrets(value)
    return resolve_secrets(value)


_AUTH_SECRET_KEYS = frozenset({"token", "password"})


def _auth_without_secret(auth: dict | None) -> dict | None:
    """A source's auth as it may leave the server: its scheme and user name, never the
    credential. A caller that changes the source supplies the credential again."""
    if not auth:
        return None
    return {k: v for k, v in auth.items() if k not in _AUTH_SECRET_KEYS}


def _auth_hints(auth: dict | None) -> dict[str, str]:
    """What of a source's auth is kept on its row beside the vault reference, so a process that
    did not take the registration can rebuild it (app_loaders._graphql_remote_auth)."""
    if not auth or auth.get("type", "none") == "none":
        return {}
    return {"auth_type": auth["type"]}


def _declared_cache_ttl(body: "GraphQLRemoteSourceRequest") -> int | None:
    """The cache TTL the caller sent, or None when it sent none: the request model's default
    (300) is the remote-call cache, not a landing TTL anyone declared (REQ-1907)."""
    return body.cache_ttl if "cache_ttl" in body.model_fields_set else None


async def _persist_source(  # REQ-307, REQ-1923
    request: Request,
    source_id: str,
    url: str,
    description: str,
    namespace: str,
    auth: dict | None,
    brand_id: str | None,
    conn,
    *,
    cache_ttl: int | None,
) -> None:
    """Write the ``sources`` row. The credential goes to the org's vault and the row carries the
    reference (REQ-1695); the namespace, the auth scheme and the brand ride in
    ``federation_hints``. ``cache_ttl`` is the source's cache TTL when the caller declared one: the
    clock its landed tables refresh on (REQ-1907); None leaves the row's as it is."""
    from provisa.api.admin.schema_common import store_source_password
    from provisa.graphql_remote.brands import BRAND_HINT, NAMESPACE_HINT

    identity = getattr(request.state, "identity", None)
    secret = (auth or {}).get("token") or (auth or {}).get("password") or ""
    hints = {NAMESPACE_HINT: namespace, **_auth_hints(auth)}
    if brand_id:
        hints[BRAND_HINT] = brand_id
    row = {
        "origin": "admin",  # REQ-1919: written when the row is created
        "id": source_id,
        "type": "graphql_remote",
        "host": "",
        "port": 0,
        "database": "",
        "username": (auth or {}).get("username", ""),
        "dialect": "",
        "path": url,
        "description": description,
        "federation_hints": hints,
        "password_ref": await store_source_password(
            getattr(identity, "user_id", None), source_id, secret
        ),
    }
    updated = ["path", "description", "username", "federation_hints", "password_ref"]
    if cache_ttl is not None:
        row["cache_ttl"] = cache_ttl
        updated.append("cache_ttl")
    await conn.upsert(sources, row, index_elements=["id"], update_columns=updated)


async def _register_branded_source(request: Request, body: "GraphQLRemoteSourceRequest") -> dict:
    """Add a branded source (REQ-1923): check the credential, record the source, register no
    tables. Its tables are available to the Register Table picker from the shipped schema."""
    from provisa.api.app import _rebuild_schemas, state
    from provisa.graphql_remote.brands import BRANDS, available_tables, offered_mutation_count

    brand = BRANDS.get(body.brand or "")
    if brand is None:
        raise ApiError(
            422,
            "graphql_remote.unknown_brand",
            f"Unknown branded source {body.brand!r}",
            brand=body.brand,
        )
    token = (body.auth or {}).get("token")
    if not token:
        raise ApiError(
            422,
            "graphql_remote.credential_required",
            f"{brand.label} needs an access token",
            brand=brand.id,
        )
    auth = brand.auth(await _resolved_credential(token))
    try:
        await _verify_live_auth(brand.url, auth, brand.verify_query)
    except Exception as exc:
        raise ApiError(
            422,
            "graphql_remote.credential_rejected",
            f"{brand.label} did not accept the access token: {exc}",
            brand=brand.id,
            error=str(exc),
        ) from exc
    namespace = body.namespace or brand.namespace
    if state.model_db is None:
        raise ApiError(503, "graphql_remote.database_not_connected", "Database not connected")
    async with state.model_db.acquire() as conn:
        # The row keeps the credential as entered: a literal goes to the vault, a reference
        # stays a reference.
        await _persist_source(
            request,
            body.source_id,
            brand.url,
            body.description,
            namespace,
            brand.auth(token),
            brand.id,
            conn,
            cache_ttl=_declared_cache_ttl(body),
        )
    state.graphql_remote_sources[body.source_id] = {
        "source_id": body.source_id,
        "url": brand.url,
        "namespace": namespace,
        "domain_id": body.domain_id,
        "auth": auth,
        "cache_ttl": body.cache_ttl,
        "brand": brand.id,
        "error_policy": brand.error_policy,
        "tables": [],
        "functions": [],
        "relationships": [],
    }
    await _rebuild_schemas()
    log.info("Added %s source %s", brand.label, body.source_id)
    return {
        "source_id": body.source_id,
        "brand": brand.id,
        **remote_source_counts(
            0, len(available_tables(brand, namespace)), offered_mutation_count(brand.schema())
        ),
        "relationships": 0,
        "table_names": [],
    }


async def _introspect_and_map(  # REQ-307, REQ-308, REQ-312, REQ-597, REQ-600
    source_id: str,
    url: str,
    namespace: str,
    domain_id: str,
    auth: dict | None,
    field_overrides: dict[str, str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    from provisa.api.app import state
    from provisa.graphql_remote.introspect import introspect_schema
    from provisa.graphql_remote.mapper import map_schema

    max_depth = state.config.graphql_remote.max_object_depth
    max_list_depth = state.config.graphql_remote.max_list_depth
    max_list_items = state.config.graphql_remote.max_list_items
    schema = await introspect_schema(url, auth)
    tables, functions, relationships = map_schema(
        schema,
        namespace,
        source_id,
        domain_id,
        max_depth,
        max_list_depth,
        max_list_items,
        field_overrides=field_overrides,
    )
    return tables, functions, relationships


def _build_object_fields(raw_fields: list) -> list:
    """Recursively build ObjectField instances from structured gql_object_fields dicts."""
    from provisa.core.models import ObjectField

    result = []
    for f in raw_fields or []:
        if isinstance(f, str):
            result.append(ObjectField(name=f, type="string"))
        else:
            result.append(
                ObjectField(
                    name=f["name"],
                    type=f.get("type", "string"),
                    fields=_build_object_fields(f.get("fields") or []),
                )
            )
    return result


# Mapper provisa type → the engine type. Stored as table_columns.data_type so the type
# is valid for BOTH the graphql synth path (introspect._GQL_TYPE_MAP keys on these
# the engine names) and the SQL-catalog path used when a view references these columns.
_PROVISA_TO_PHYSICAL_TYPE = {
    "text": "varchar",
    "integer": "integer",
    "numeric": "double",
    "boolean": "boolean",
    "jsonb": "json",
}


async def _upsert_tables_to_semantic_layer(  # REQ-308, REQ-599, REQ-602
    source_id: str,
    domain_id: str,
    tables: list[dict],
    model_db,
) -> list[dict]:
    """Write discovered GraphQL tables into registered_tables with descriptions.

    Each table is upserted by its identity (source, schema, name), so one that is still in the
    remote schema keeps its id and what refers to it. One the remote no longer has is deleted
    through the model store; when something depends on it, it is kept, and returned here with
    what still refers to it (REQ-1918)."""
    from provisa.core.models import Column, Table
    from provisa.core.repositories import table as table_repo

    from provisa.compiler.naming import apply_sql_name

    async with model_db.acquire() as conn:
        # Introspection is not the authority on governance grants — preserve any
        # visible_to already set (e.g. by config apply): the upsert below replaces a table's
        # columns wholesale.
        _existing_rows = await conn.execute_core(
            select(
                registered_tables.c.table_name,
                table_columns.c.column_name,
                table_columns.c.visible_to,
            )
            .select_from(
                registered_tables.join(
                    table_columns, table_columns.c.table_id == registered_tables.c.id
                )
            )
            .where(
                and_(
                    registered_tables.c.source_id == source_id,
                    registered_tables.c.schema_name == "graphql",
                )
            )
        )
        _existing_grants: dict[tuple[str, str], list] = {
            (r.table_name, r.column_name): list(r.visible_to or [])
            for r in _existing_rows.fetchall()
        }
        # REQ-1591: a term's domains are derived by joining its refs to registered_tables, so the
        # snapshot has to be taken while the rows the wipe below removes are still there.
        from provisa.core.repositories import glossary as glossary_repo

        domains_before = await glossary_repo.term_domains(conn)
        unchanged: list[dict] = []
        for t in tables:
            # A table already registered keeps the name it is registered under.
            _sql_name = t.get("registered_name") or apply_sql_name(t["name"])
            tbl = Table(
                source_id=source_id,
                # A table registered on its own keeps the domain it was registered into.
                domain_id=t.get("domain_id") or domain_id or "",
                schema_name="graphql",
                table_name=_sql_name,
                description=t.get("description"),
                alias=None,
                columns=[
                    Column(
                        name=apply_sql_name(c["name"]),
                        visible_to=_existing_grants.get((_sql_name, apply_sql_name(c["name"])), []),
                        description=c.get("description"),
                        data_type=_PROVISA_TO_PHYSICAL_TYPE.get(c.get("type") or "text", "varchar"),
                        object_fields=_build_object_fields(c.get("gql_object_fields") or []),
                        # The selection carries arguments (a list's row limit, a connection's
                        # page bound) that the object-field shape alone cannot rebuild.
                        gql_selection=c.get("gql_selection"),
                    )
                    for c in t.get("columns", [])
                ]
                + [
                    Column(
                        name=f"_nf_{apply_sql_name(a['name'])}",
                        visible_to=[],
                        native_filter_type="query_param",
                        # REQ-1426: a native-filter column is a column — it carries the argument's
                        # resolved type like any other. Omitting it wrote NULL data_type rows the
                        # catalog then rendered as "unknown". _gql_to_provisa_type always yields a
                        # key of this map, so an unmapped type is a defect and must raise.
                        data_type=_PROVISA_TO_PHYSICAL_TYPE[a["provisa_type"]],
                    )
                    for a in t.get("required_args", [])
                ],
            )
            try:
                await table_repo.upsert(conn, tbl, origin="admin")
            except table_repo.ColumnDropRefused as refused:
                # The remote dropped a field something here still refers to: the table is left
                # as it was and reported.
                held = await table_repo.get_by_name(conn, source_id, "graphql", _sql_name)
                unchanged.append(
                    table_repo.kept_columns_report(refused, held["id"] if held else None)
                )

        kept = await table_repo.retire_generated(
            conn,
            source_id,
            "graphql",
            {t.get("registered_name") or apply_sql_name(t["name"]) for t in tables},
        )
        # REQ-1387: settle only the terms whose fields truly departed.
        await glossary_repo.sweep_refless_terms(conn, domains_before=domains_before)
        return [*table_repo.kept_report(kept), *unchanged]


async def _upsert_relationships_to_semantic_layer(  # REQ-313, REQ-598
    relationships: list[dict],
    model_db,
    state=None,
) -> None:
    """Upsert detected intra-source relationships, then retry any config relationships deferred at startup."""
    from provisa.core.models import Cardinality, Relationship
    from provisa.core.repositories import relationship as rel_repo

    async with model_db.acquire() as conn:
        for r in relationships or []:
            try:
                await rel_repo.upsert(
                    conn,
                    Relationship(
                        id=r["id"],
                        source_table_id=r["source_table_id"],
                        target_table_id=r["target_table_id"],
                        source_column=r["source_column"],
                        target_column=r["target_column"],
                        cardinality=Cardinality(r.get("cardinality", "many-to-one")),
                        source_json_key=r.get("source_json_key") or None,
                    ),
                    origin="admin",
                )
            except Exception:
                log.warning("Failed to upsert relationship %s", r["id"], exc_info=True)
        # Retry config relationships deferred at startup (tables may now exist)
        cfg = getattr(state, "config", None) if state is not None else None
        if cfg is not None:
            for rel in cfg.relationships:
                try:
                    await rel_repo.upsert(conn, rel, origin="admin")
                except ValueError:
                    pass


@router.post("")
async def register_graphql_remote_source(
    request: Request,  # REQ-307, REQ-308, REQ-311, REQ-312, REQ-597, REQ-598, REQ-599
    body: GraphQLRemoteSourceRequest,
):
    """Add a remote GraphQL source. Its schema is read to confirm the endpoint and the
    credential, and no table is registered: every table the schema offers is listed by the
    Register Table picker, and the steward registers the ones wanted (REQ-308)."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import _rebuild_schemas, state
    from provisa.graphql_remote.brands import offered_mutation_count
    from provisa.graphql_remote.introspect import introspect_schema
    from provisa.graphql_remote.mapper import map_schema

    if body.brand:
        return await _register_branded_source(request, body)
    if not body.url:
        raise ApiError(422, "graphql_remote.url_required", "A GraphQL endpoint URL is required")

    # The row keeps the endpoint as written; a ``${env:...}`` reference is read at what it names,
    # as boot resolves a config-declared path (app_loaders).
    url = await _resolved_credential(body.url)
    try:
        schema = await introspect_schema(url, body.auth)
        offered, _, _ = map_schema(
            schema, body.namespace, body.source_id, body.domain_id, only=set()
        )
    except Exception as exc:
        raise ApiError(
            422,
            "graphql_remote.introspection_failed",
            f"Introspection failed: {exc}",
            error=str(exc),
        ) from exc

    if state.model_db is None:
        raise ApiError(503, "graphql_remote.database_not_connected", "Database not connected")
    async with state.model_db.acquire() as _conn:
        await _persist_source(
            request,
            body.source_id,
            body.url,
            body.description,
            body.namespace,
            body.auth,
            None,
            _conn,
            cache_ttl=_declared_cache_ttl(body),
        )
        if body.domain_id:
            await _conn.upsert(
                domains,
                {"id": body.domain_id, "origin": "admin"},  # REQ-1919: written when created
                index_elements=["id"],
                update_columns=[],
            )
    state.graphql_remote_sources[body.source_id] = {
        "source_id": body.source_id,
        "url": url,
        "namespace": body.namespace,
        "domain_id": body.domain_id,
        "auth": body.auth,
        "cache_ttl": body.cache_ttl,
        "field_overrides": body.field_overrides or {},
        # Kept so the picker and each table registration read it without asking the remote again.
        "schema": schema,
        "tables": [],
        "functions": [],
        "relationships": body.relationships or [],
    }
    await _rebuild_schemas()
    log.info("Added GraphQL remote source %s (%d tables on offer)", body.source_id, len(offered))
    return {
        "source_id": body.source_id,
        **remote_source_counts(0, len(offered), offered_mutation_count(schema)),
        "relationships": 0,
        "table_names": [],
    }


@router.post("/{source_id}/refresh")
async def refresh_graphql_remote_source(request: Request, source_id: str):  # REQ-311, REQ-598
    """Read a remote source's schema again and bring its REGISTERED tables up to date with it.
    Nothing new is registered: a table the schema has gained is on offer, and one it has lost is
    retired unless something still refers to it."""
    require_capability_request(request, "source_registration")
    from provisa.api.admin._graphql_table_registration import (
        refreshed_registered_tables,
        remember_table,
        source_offer,
    )
    from provisa.graphql_remote.brands import offered_mutation_count
    from provisa.api.app import _rebuild_schemas, state

    sources = getattr(state, "graphql_remote_sources", {})
    if source_id not in sources:
        raise ApiError(
            404,
            "graphql_remote.source_not_found",
            f"GraphQL remote source {source_id!r} not found",
            source_id=source_id,
        )

    reg = sources[source_id]
    if reg.get("brand"):
        # REQ-1923: a branded source's schema ships with Provisa and its tables are registered
        # one at a time; there is nothing here to re-introspect.
        raise ApiError(
            409,
            "graphql_remote.branded_source_not_refreshed",
            f"Source {source_id!r} uses a schema that ships with Provisa",
            source_id=source_id,
        )
    if state.model_db is None:
        raise ApiError(503, "graphql_remote.database_not_connected", "Database not connected")
    try:
        reg["schema"] = None  # read again, not answered from what is kept
        offered = await source_offer(state, source_id)
        assert offered is not None
        offer, _ = offered
        tables = await refreshed_registered_tables(state, offer, reg)
    except Exception as exc:
        raise ApiError(
            422,
            "graphql_remote.reintrospection_failed",
            f"Re-introspection failed: {exc}",
            error=str(exc),
        ) from exc

    # REQ-1918: tables the remote schema no longer has that something still refers to.
    kept_tables = await _upsert_tables_to_semantic_layer(
        source_id, reg.get("domain_id", ""), tables, state.model_db
    )
    reg["tables"] = []
    for table in tables:
        await remember_table(state, reg, table["sql_name"], table)
    await _rebuild_schemas()

    log.info("Refreshed GraphQL remote source %s (%d tables)", source_id, len(tables))
    return {
        "source_id": source_id,
        **remote_source_counts(
            len(tables),
            len(offer.table_index(reg.get("namespace", ""))),
            offered_mutation_count(offer.schema()),
        ),
        "relationships": len(reg.get("relationships", [])),
        "table_names": [t["name"] for t in tables],
        "kept_tables": kept_tables,
    }


@router.get("/brands")
async def list_graphql_remote_brands(request: Request):  # REQ-1923
    """The branded sources this build carries, for the source picker."""
    require_capability_request(request, "source_registration")
    from provisa.graphql_remote.brands import BRANDS

    return [{"id": b.id, "label": b.label, "namespace": b.namespace} for b in BRANDS.values()]


@router.get("")
async def list_graphql_remote_sources(request: Request):  # REQ-598
    """List all registered GraphQL remote sources."""
    require_capability_request(request, "source_registration")
    from provisa.api.app import state

    sources = getattr(state, "graphql_remote_sources", {})
    return [{**reg, "auth": _auth_without_secret(reg.get("auth"))} for reg in sources.values()]
