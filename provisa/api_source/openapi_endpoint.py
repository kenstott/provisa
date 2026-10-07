# Copyright (c) 2026 Kenneth Stott
# Canary: 7572a59e-4598-4775-90d1-31f4b1a7f27d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The endpoint a registered OpenAPI table is served from (REQ-316, REQ-318).

Every OpenAPI table is read through ``api_source.caller`` — paging, max_pages, the answer cut —
from its ``api_endpoints`` row. That row is derived from the table's registration by
:func:`register_openapi_endpoint`, whichever way the table was registered (the config, or the
admin's one-at-a-time registration): one function, one row, so the two origins cannot drift.
"""

# Requirements: REQ-316, REQ-318, REQ-320

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from sqlalchemy import select, update

from provisa.core.paging import PaginationConfig, paging_row
from provisa.core.schema_org import api_endpoints, api_sources, registered_tables, table_columns

if TYPE_CHECKING:
    from provisa.core.database import Connection
    from provisa.core.models import Table
    from provisa.openapi.mapper import OpenAPIQuery


class NoOperation(LookupError):
    """A registered OpenAPI table whose spec has no GET operation of its name: it has nothing to
    be read from."""

    def __init__(self, source_id: str, table_name: str) -> None:
        super().__init__(
            f"OpenAPI source {source_id!r} has no GET operation for table {table_name!r}"
        )


def normalize_op_id(s: str) -> str:
    return re.sub(r"[_-]", "", s).lower()


def _operation(queries: list[OpenAPIQuery], source_id: str, table_name: str) -> OpenAPIQuery:
    wanted = normalize_op_id(table_name)
    for query in queries:
        if normalize_op_id(query.operation_id) == wanted:
            return query
    raise NoOperation(source_id, table_name)


def openapi_operation(
    spec: dict, source_id: str, table_name: str, pagination: PaginationConfig | None
) -> OpenAPIQuery:
    """The GET operation ``table_name`` is read from (matched on its operation id), its rows
    read where the table's ``pagination`` says they are (REQ-316): the property it names, or the
    answer itself when it names none."""
    from provisa.openapi.mapper import parse_spec

    query = _operation(parse_spec(spec)[0], source_id, table_name)
    rows_field = None if pagination is None else pagination.rows_field
    if rows_field == query.rows_field:
        return query
    queries, _ = parse_spec(spec, rows_fields={query.operation_id: rows_field})
    return _operation(queries, source_id, table_name)


def default_params_from_spec(spec: dict, path: str) -> dict:
    """Extract enum/default values for GET query params at path for pre-population."""
    from provisa.openapi.mapper import operation_parameters

    defaults: dict = {}
    for p in operation_parameters(spec, path):
        if p.get("in") != "query":
            continue
        name = p.get("name", "")
        if not name:
            continue
        schema = p.get("schema") or {}
        if "enum" in schema:
            defaults[name] = schema["enum"]
        elif "default" in schema:
            defaults[name] = schema["default"]
    return defaults


def endpoint_columns(match: OpenAPIQuery) -> list[dict]:
    """The endpoint's columns: the response's fields, then a param-only column for each path
    parameter and each query parameter that is not also a response field."""
    from provisa.openapi.register import _openapi_to_provisa_type, _schema_to_columns

    response = _schema_to_columns(match.response_schema)
    resp_col_names: set[str] = {c["name"] for c in response}
    api_columns: list[dict] = [
        {
            "name": c["name"],
            "type": c["type"],
            "filterable": True,
            **({"object_fields": c["object_fields"]} if c.get("object_fields") else {}),
        }
        for c in response
    ]
    for p in match.path_params:
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
    for p in match.query_params:
        if p["name"] in resp_col_names:
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
    return api_columns


def api_auth(auth_config: dict | None) -> dict | None:
    """An OpenAPI source's auth (``{type, token | username+password | header_name+api_key}``)
    as the typed auth the caller applies (core.auth_models.ApiAuth). Secret references are kept
    as written; the caller resolves them at each call."""
    if not auth_config or auth_config.get("type", "none") == "none":
        return None
    kind = auth_config["type"]
    if kind == "bearer":
        return {"type": "bearer", "token": auth_config["token"]}
    if kind == "basic":
        return {
            "type": "basic",
            "username": auth_config["username"],
            "password": auth_config["password"],
        }
    if kind == "api_key":
        return {
            "type": "api_key",
            "key": auth_config["api_key"],
            "name": auth_config["header_name"],
            "location": "header",
        }
    raise ValueError(f"OpenAPI auth type {kind!r} is not one the caller applies")


async def register_openapi_source(conn: Connection, source_id: str, base_url: str) -> None:
    """The ``api_sources`` row an OpenAPI source's tables are called through: its base URL. The
    row's auth is the source registration's (:func:`store_openapi_auth`) and is not touched."""
    await conn.upsert(
        api_sources,
        {"id": source_id, "type": "openapi", "base_url": base_url, "auth": None},
        index_elements=["id"],
        update_columns=["base_url"],
    )


async def store_openapi_auth(conn: Connection, source_id: str, auth: dict | None) -> None:
    """Store the typed auth (:func:`api_auth`) the caller applies to ``source_id``'s calls,
    encrypted at rest (REQ-686); None clears it."""
    from provisa.encryption import encryption_service

    stored = None if auth is None else encryption_service().encrypt(json.dumps(auth).encode())
    await conn.execute_core(
        update(api_sources).where(api_sources.c.id == source_id).values(auth=stored)
    )


async def register_openapi_endpoint(
    conn: Connection, table: Table, *, spec: dict, ttl: int
) -> None:
    """Write the ``api_endpoints`` row ``table`` (a registered table of an OpenAPI source) is
    served from, derived from its registration: the operation of its name, its columns and
    default params, and a copy of the table's own paging. The source's ``api_sources`` row
    (:func:`register_openapi_source`) is written before it."""
    match = openapi_operation(spec, table.source_id, table.table_name, table.pagination)
    columns = endpoint_columns(match)
    defaults = default_params_from_spec(spec, match.path)
    await conn.upsert(
        api_endpoints,
        {
            "source_id": table.source_id,
            "path": match.path,
            "method": "GET",
            "table_name": table.table_name,
            "columns": columns,
            "ttl": ttl,
            "default_params": defaults or None,
            "promotions": table.promotions,
            # REQ-318: a copy of the table's own paging, the one place it is authored.
            "pagination": paging_row(table.pagination),
            "response_root": match.rows_field,  # REQ-316: where the table's paging says its rows are
        },
        index_elements=["table_name"],
        update_columns=[
            "source_id",
            "path",
            "columns",
            "ttl",
            "default_params",
            "promotions",
            "pagination",
            "response_root",
        ],
    )
    for col in columns:
        if col.get("object_fields"):
            await conn.execute_core(
                update(table_columns)
                .where(
                    table_columns.c.table_id
                    == select(registered_tables.c.id)
                    .where(
                        registered_tables.c.source_id == table.source_id,
                        registered_tables.c.table_name == table.table_name,
                    )
                    .scalar_subquery(),
                    table_columns.c.column_name == col["name"],
                )
                .values(object_fields=col["object_fields"])
            )
