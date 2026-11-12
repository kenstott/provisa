# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7c5e93-8f16-4b3d-a0e4-71d9c3b5f286
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ways to reach a registered table, each ready to copy (REQ-2234, SEE THE DATA RIGHT AFTER
REGISTERING): the GraphQL query, the SQL a client runs over pgwire and the connection string it
connects with, the REST read and the MCP call.

Every name is the one the role's compiled schema gives the table, read where the MCP tools read
it (``graphql_field_names``): nothing here derives a name of its own. A listener not started or a
surface not mounted is not offered, as a data contract does not name it (``odps._servers``).
"""

# Requirements: REQ-2234

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from provisa.api.admin.types import TableAccessType

if TYPE_CHECKING:
    from provisa.api.metadata_export.model import ServingEndpoints

#: The rows each ready query reads: a first look, not the table.
FIRST_ROWS = 10


async def table_access(
    state: Any, role: str, table_id: int, endpoints: "ServingEndpoints | None"
) -> TableAccessType:
    """How ``role`` reaches table ``table_id``. Refused when the role's schema does not serve it.

    A draft is served to a role holding table registration in its domain (REQ-1921): its
    ways are named as that role's surface names it -- the field its schema offers, the REST path
    its path map answers, the name its statements read it by -- and marked as a draft."""
    from provisa.api.mcp.tools import graphql_field_names
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_table_name

    meta = _served(state.contexts.get(role), table_id)
    if meta is None:
        return _draft_access(state, role, table_id, endpoints)
    names = await graphql_field_names(
        state, role, domain_to_sql_name(meta.domain_id), semantic_table_name(meta)
    )
    return _ways(
        meta,
        names["table_field"],
        [c["graphql_name"] for c in names["columns"]],
        names["openapi_path"],
        endpoints,
        draft=False,
    )


def _served(ctx: Any, table_id: int) -> Any:
    """The table ``table_id`` as context ``ctx`` serves it, or None."""
    return next((m for m in (ctx.tables.values() if ctx else ()) if m.table_id == table_id), None)


def _draft_access(
    state: Any, role: str, table_id: int, endpoints: "ServingEndpoints | None"
) -> TableAccessType:
    """The ways ``role`` reaches the draft ``table_id``, when it is served it (table registration
    in its domain); refused as a table not served otherwise. The catalog the MCP tools name a table
    from lists no draft, so the names are read off the role's own surface, which offers it."""
    from provisa.api.admin.table_read import draft_served_to, surface_with_drafts
    from provisa.api.role_surface import role_surface

    draft = draft_served_to(state, role, table_id)
    surface = role_surface(state, role)
    if draft is None or surface is None:
        raise ValueError(f"table {table_id} is not served to role {role!r}")
    ctx = surface_with_drafts(state, role, [draft]).context
    meta = _served(ctx, table_id)
    offered = [
        (field, path)
        for field, path in surface.table_path_map.items()
        if path["table_id"] == table_id
    ]
    if meta is None or len(offered) != 1:
        raise ValueError(f"table {table_id} is not served to role {role!r}")
    ((field, path),) = offered
    # Its columns the role reads, as its schema names them (in the order registered).
    columns = [
        ctx.physical_to_gql[key]
        for c in draft["columns"]
        if (key := (table_id, c["column_name"])) in ctx.physical_to_gql
    ]
    # The REST read the path map answers (rest/generator), as graphql_field_names words it.
    rest = f"GET /data/rest/{path['domain_id']}/{path['table_name']}?limit=20"
    return _ways(meta, field, columns, rest, endpoints, draft=True)


def _ways(
    meta: Any,
    table_field: str,
    columns: list[str],
    openapi_path: str,
    endpoints: "ServingEndpoints | None",
    *,
    draft: bool,
) -> TableAccessType:
    """Each way to reach the table ``meta``, from the names its role's surface gives it."""
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_ref

    schema = domain_to_sql_name(meta.domain_id)
    fields = "\n".join(f"    {c}" for c in columns)
    sql = f"SELECT * FROM {semantic_ref(meta)} LIMIT {FIRST_ROWS}"
    pgwire = None
    rest = None
    if endpoints is not None:
        if endpoints.pgwire_port is not None:
            pgwire = (
                f"postgresql://{endpoints.hostname}:{endpoints.pgwire_port}/"
                f"{endpoints.database}?options=-csearch_path%3D{schema}"
            )
        if "REST" in endpoints.http_paths:
            method, path = openapi_path.split(" ", 1)
            rest = f"{method} {endpoints.http_origin}{path}"
    return TableAccessType(
        graphql=f"query {{\n  {table_field}(limit: {FIRST_ROWS}) {{\n{fields}\n  }}\n}}",
        sql=sql,
        pgwire=pgwire,
        rest=rest,
        mcp=json.dumps({"tool": "run_sql", "arguments": {"sql": sql}}, indent=2),
        draft=draft,
    )
