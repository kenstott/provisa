# Copyright (c) 2026 Kenneth Stott
# Canary: 6a2d8f31-4c7e-4b9a-9d5e-1f3b7c9a2e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Register Table on a neo4j source (REQ-1670): the Cypher preview behind the form and the
endpoint persistence lives in ``_query_api_registration`` (shared with sparql).

A neo4j source has no tables to list — its table IS a Cypher projection — so the form previews
the Cypher (rows + inferred column types), the operator names the table, and registration writes
the same ``api_sources``/``api_endpoints`` rows config load writes (REQ-1668).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from provisa.api.admin.types import QueryPreviewType
    from provisa.core.database import Connection


async def preview_neo4j(conn: "Connection", source_id: str, cypher: str) -> "QueryPreviewType":
    """Run the Cypher (LIMIT 5) against the source and infer the registered column types."""
    import httpx

    from provisa.api.admin.types import QueryPreviewColumnType, QueryPreviewType
    from provisa.api_source.persist import data_type_for_api_column
    from provisa.neo4j.persist import neo4j_config_from_source
    from provisa.neo4j.preview import Neo4jNodeObjectError, preview_query, validate_shape
    from provisa.neo4j.source import infer_columns

    from provisa.api.admin._query_api_registration import source_row

    row = await source_row(conn, source_id)
    if row is None or row["type"] != "neo4j":
        return QueryPreviewType(error=f"{source_id!r} is not a registered neo4j source")
    cfg, api_source = neo4j_config_from_source(
        source_id=source_id,
        host=row["host"],
        port=row["port"],
        database=row["database"],
        base_url=None,
    )
    try:
        rows = await preview_query(
            base_url=api_source.base_url, database=cfg.database, cypher=cypher
        )
        validate_shape(rows)
    except Neo4jNodeObjectError as exc:
        return QueryPreviewType(error=str(exc))
    except (httpx.HTTPError, ValueError) as exc:
        return QueryPreviewType(error=f"{type(exc).__name__}: {exc}")
    columns = [
        QueryPreviewColumnType(name=c.name, data_type=data_type_for_api_column(c.type))
        for c in infer_columns(rows)
    ]
    return QueryPreviewType(rows=list(rows), columns=columns)
