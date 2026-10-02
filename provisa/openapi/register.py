# Copyright (c) 2026 Kenneth Stott
# Canary: e6b1702e-7879-4727-ac41-ec12b02a5845
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Auto-register OpenAPI operations as Provisa tables and tracked functions."""

from __future__ import annotations
import json
import logging
import re
from typing import TYPE_CHECKING


from provisa.core.schema_org import api_endpoints, api_sources
from provisa.openapi.mapper import OpenAPIQuery, OpenAPIMutation, parse_spec

if TYPE_CHECKING:
    pass

# Requirements: REQ-314, REQ-316, REQ-317, REQ-319, REQ-320, REQ-321

log = logging.getLogger(__name__)

_VERB_PREFIXES = (
    "get",
    "list",
    "fetch",
    "search",
    "find",
    "query",
    "create",
    "post",
    "add",
    "insert",
    "update",
    "put",
    "patch",
    "edit",
    "delete",
    "remove",
    "destroy",
)


def _singularize(word: str) -> str:
    """Best-effort English singularization for the noun segment of an alias."""
    if word.endswith("ies") and len(word) > 3:
        return word[:-3] + "y"
    if word.endswith("ses") or word.endswith("xes") or word.endswith("zes"):
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss") and len(word) > 2:
        return word[:-1]
    return word


def _operation_id_to_alias(op_id: str) -> str:
    """Convert camelCase/PascalCase/snake_case operationId to a snake_case alias.

    Format: {noun_singular}_{modifiers} (e.g. findPetsByStatus → pet_by_status).
    """
    # camelCase / PascalCase → snake_case (intermediate normalization for verb-stripping parse)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", op_id)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s).lower()
    # strip leading verb segment
    for verb in _VERB_PREFIXES:
        if s.startswith(verb + "_"):
            s = s[len(verb) + 1 :]
            break
        if s == verb:
            return s
    # singularize the first (noun) segment
    parts = s.split("_", 1)
    parts[0] = _singularize(parts[0])
    return "_".join(parts) or op_id.lower()


_OPENAPI_TYPE_MAP = {
    "string": "string",
    "integer": "integer",
    "number": "number",
    "boolean": "boolean",
    "array": "jsonb",
    "object": "jsonb",
}


def _openapi_to_provisa_type(t: str | None) -> str:
    return _OPENAPI_TYPE_MAP.get(t or "string", "string")


def _schema_to_columns(schema: dict | None) -> list[dict]:
    """Extract column list from a JSON Schema object or array-of-objects schema."""
    if not schema:
        return []
    # Unwrap array wrapper
    if schema.get("type") == "array" and "items" in schema:
        schema = schema["items"]
    assert schema is not None
    props = schema.get("properties", {})
    if not props and isinstance(schema.get("additionalProperties"), dict):
        # Map-shaped response (e.g. {"available": 3, "sold": 12}) has no fixed property
        # names — api_source.flattener.flatten_response flattens it to {"status": k, "count": v} rows,
        # so the registered columns must match.
        value_type = schema["additionalProperties"].get("type")
        return [
            {"name": "status", "type": "string"},
            {"name": "count", "type": _openapi_to_provisa_type(value_type)},
        ]
    cols = []
    for name, prop in props.items():
        col: dict = {"name": name, "type": _openapi_to_provisa_type(prop.get("type"))}
        desc = prop.get("description") or prop.get("title")
        if desc:
            col["description"] = desc
        if prop.get("type") == "object" and prop.get("properties"):
            sub_fields = []
            for sub_name, sub_prop in prop["properties"].items():
                sf: dict = {
                    "name": sub_name,
                    "type": _openapi_to_provisa_type(sub_prop.get("type")),
                }
                sub_desc = sub_prop.get("description") or sub_prop.get("title")
                if sub_desc:
                    sf["description"] = sub_desc
                sub_fields.append(sf)
            col["object_fields"] = sub_fields
        cols.append(col)
    return cols


async def upsert_table(  # REQ-316, REQ-320
    source_id: str,
    query: OpenAPIQuery,
    conn,
    domain_id: str = "",
    base_url: str = "",
    auth_config: dict | None = None,
    cache_ttl: int = 300,
) -> None:
    """Register an OpenAPI GET operation as a virtual table and api_endpoint."""
    from provisa.core.models import Column, Table
    from provisa.core.repositories import table as table_repo

    columns = _schema_to_columns(query.response_schema)
    # Add path/query params as native-filter columns with _nf_ prefix
    existing_names = {c["name"] for c in columns}
    for p in query.path_params:
        if p["name"] not in existing_names:
            columns.append(
                {
                    "name": f"_nf_{p['name']}",
                    "type": _openapi_to_provisa_type(p.get("type")),
                    "native_filter_type": "path_param",
                }
            )
    for p in query.query_params:
        if p["name"] not in existing_names:
            columns.append(
                {
                    "name": f"_nf_{p['name']}",
                    "type": _openapi_to_provisa_type(p.get("type")),
                    "native_filter_type": "query_param",
                }
            )

    table_name = _operation_id_to_alias(query.operation_id)

    from provisa.core.models import ObjectField

    tbl = Table(
        source_id=source_id,
        domain_id=domain_id or "",
        schema_name="openapi",
        table_name=table_name,
        alias=None,
        columns=[
            Column(
                name=c["name"],
                data_type=c["type"],
                visible_to=[],
                native_filter_type=c.get("native_filter_type"),
                object_fields=[ObjectField(**f) for f in c.get("object_fields", [])],
                description=c.get("description"),
            )
            for c in columns
        ],
        description=query.summary,
    )
    await table_repo.upsert(conn, tbl, origin="admin")
    log.debug("Upserted table %s for operation %s", table_name, query.operation_id)

    # Upsert api_sources so api_endpoints FK is satisfied
    from provisa.encryption import encryption_service  # REQ-686

    # REQ-686: encrypt the API auth (keys/tokens) at rest.
    _auth_enc = (
        encryption_service().encrypt(json.dumps(auth_config).encode("utf-8"))
        if auth_config
        else None
    )
    await conn.upsert(
        api_sources,
        {"id": source_id, "type": "openapi", "base_url": base_url, "auth": _auth_enc},
        index_elements=["id"],
        update_columns=["base_url", "auth"],
    )

    # Build ApiColumn JSON for api_endpoints
    response_col_names = {c["name"] for c in _schema_to_columns(query.response_schema)}
    api_columns = []
    for c in _schema_to_columns(query.response_schema):
        entry: dict = {"name": c["name"], "type": c["type"], "filterable": True}
        if c.get("object_fields"):
            entry["object_fields"] = c["object_fields"]
        api_columns.append(entry)
    for p in query.path_params:
        if p["name"] in response_col_names:
            # Param is also a response field — merge param metadata into the existing entry.
            for col in api_columns:
                if col["name"] == p["name"]:
                    col["param_type"] = "path"
                    col["param_name"] = p["name"]
                    break
        else:
            api_columns.append(
                {
                    "name": p["name"],
                    "type": _openapi_to_provisa_type(p.get("type")),
                    "filterable": False,
                    "param_type": "path",
                    "param_name": p["name"],
                    "param_only": True,
                }
            )
    for p in query.query_params:
        if p["name"] in response_col_names:
            for col in api_columns:
                if col["name"] == p["name"]:
                    col["param_type"] = "query"
                    col["param_name"] = p["name"]
                    break
        else:
            api_columns.append(
                {
                    "name": p["name"],
                    "type": _openapi_to_provisa_type(p.get("type")),
                    "filterable": False,
                    "param_type": "query",
                    "param_name": p["name"],
                    "param_only": True,
                }
            )

    await conn.upsert(
        api_endpoints,
        {
            "source_id": source_id,
            "path": query.path,
            "method": "GET",
            "table_name": table_name,
            # JSON column takes the Python list directly.
            "columns": api_columns,
            "ttl": cache_ttl,
        },
        index_elements=["table_name"],
        update_columns=["source_id", "path", "columns", "ttl"],
    )


async def upsert_tracked_function(  # REQ-317
    source_id: str,
    mutation: OpenAPIMutation,
    conn,
    domain_id: str = "",
) -> None:
    """Register an OpenAPI non-GET operation as a tracked function."""
    from provisa.core.models import Function, FunctionArgument
    from provisa.core.repositories import function as function_repo

    input_cols = _schema_to_columns(mutation.input_schema)
    return_cols = _schema_to_columns(mutation.response_schema)

    fn_name = mutation.operation_id
    # The operation's own JSON Schema, stored as an object into a JSON column: the compiler reads
    # ``return_schema["properties"]`` to build the GraphQL return type (REQ-885), so a serialized
    # column list would be both the wrong shape and double-encoded. ``return_cols`` decides only
    # WHETHER there is a shape to project.
    return_schema = mutation.response_schema if return_cols else None

    func = Function(
        name=fn_name,
        source_id=source_id,
        schema_name="openapi",
        function_name=mutation.operation_id,
        returns="",
        arguments=[FunctionArgument(name=c["name"], type=c["type"]) for c in input_cols],
        visible_to=[],
        writable_by=[],
        domain_id=domain_id or "",
        description=mutation.summary,
        kind="mutation",
    )
    await function_repo.upsert_function(conn, func, return_schema=return_schema)
    log.debug("Upserted tracked function %s for operation %s", fn_name, mutation.operation_id)


class CommandsNeedDomain(ValueError):
    """An OpenAPI registration whose spec declares commands and names no domain to put them in."""

    def __init__(self, source_id: str, commands: int) -> None:
        self.source_id = source_id
        self.commands = commands
        super().__init__(
            f"OpenAPI source {source_id!r} declares {commands} command(s) and names no domain: "
            "a command sits in a domain, so the registration needs one"
        )


async def auto_register_openapi_source(  # REQ-314, REQ-316, REQ-317, REQ-321
    source_id: str,
    spec: dict,
    conn,
    domain_id: str = "",
    base_url: str = "",
    auth_config: dict | None = None,
    cache_ttl: int = 300,
) -> tuple[int, int, list[dict]]:
    """Parse spec and upsert virtual tables + tracked functions.

    Returns (n_tables, n_mutations, kept). Each table is upserted by its identity (source,
    schema, name), so one the spec still has keeps its id and what refers to it. One the spec no
    longer has is deleted through the model store; when something depends on it, it is kept, and
    ``kept`` lists it with what still refers to it (REQ-1918)."""
    # REQ-1591: a term's domains are derived by joining its refs to registered_tables, so the
    # snapshot the sweep needs has to be taken while those rows still exist.
    from provisa.core.repositories import glossary as glossary_repo
    from provisa.core.repositories import table as table_repo

    queries, mutations = parse_spec(spec)
    if mutations:
        # REQ-1531: the spec's mutations become commands, and a command sits in a domain.
        # Refused before anything is written, so a registration never lands its tables and then
        # fails on its commands.
        from provisa.core import domain_policy

        try:
            domain_policy.command_domain_id(domain_id, source_id)
        except ValueError as refused:
            raise CommandsNeedDomain(source_id, len(mutations)) from refused
    domains_before = await glossary_repo.term_domains(conn)
    unchanged: list[dict] = []
    for q in queries:
        try:
            await upsert_table(source_id, q, conn, domain_id, base_url, auth_config, cache_ttl)
        except table_repo.ColumnDropRefused as refused:
            # The spec dropped a field something here still refers to: the table is left as it
            # was and reported.
            held = await table_repo.get_by_name(
                conn, source_id, "openapi", _operation_id_to_alias(q.operation_id)
            )
            unchanged.append(table_repo.kept_columns_report(refused, held["id"] if held else None))
    for m in mutations:
        await upsert_tracked_function(source_id, m, conn, domain_id)
    kept = await table_repo.retire_generated(
        conn, source_id, "openapi", {_operation_id_to_alias(q.operation_id) for q in queries}
    )
    # REQ-1387: settle only the terms whose fields truly departed.
    await glossary_repo.sweep_refless_terms(conn, domains_before=domains_before)
    return len(queries), len(mutations), [*table_repo.kept_report(kept), *unchanged]
