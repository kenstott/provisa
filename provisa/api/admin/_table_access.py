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
    """How ``role`` reaches table ``table_id``. Refused when the role's schema does not serve it."""
    from provisa.api.mcp.tools import graphql_field_names
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.compiler.sql_rewrite import semantic_ref, semantic_table_name

    ctx = state.contexts.get(role)
    meta = next(
        (m for m in (ctx.tables.values() if ctx else ()) if m.table_id == table_id),
        None,
    )
    if meta is None:
        raise ValueError(f"table {table_id} is not served to role {role!r}")
    schema = domain_to_sql_name(meta.domain_id)
    names = await graphql_field_names(state, role, schema, semantic_table_name(meta))
    fields = "\n".join(f"    {c['graphql_name']}" for c in names["columns"])
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
            method, path = names["openapi_path"].split(" ", 1)
            rest = f"{method} {endpoints.http_origin}{path}"
    return TableAccessType(
        graphql=f"query {{\n  {names['table_field']}(limit: {FIRST_ROWS}) {{\n{fields}\n  }}\n}}",
        sql=sql,
        pgwire=pgwire,
        rest=rest,
        mcp=json.dumps({"tool": "run_sql", "arguments": {"sql": sql}}, indent=2),
    )
