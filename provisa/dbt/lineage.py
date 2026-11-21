# Copyright (c) 2026 Kenneth Stott
# Canary: 91ae42c7-0d5f-4c38-a6b2-7e13f8d05c1a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A dbt model's lineage: its compiled SQL, as a view's definition is read (REQ-2228, REQ-1160).

A model the steward has registered takes its registered ``<domain>.<table>`` relation, so the graph
continues to what consumes it. One not registered keeps ``<schema>.<relation>``, so a chain through
models nobody registered stays whole. A manifest written by ``dbt parse`` holds no compiled SQL, and
such a model has no lineage to add.
"""

# Requirements: REQ-2228

from __future__ import annotations

import sqlglot
from sqlglot.errors import SqlglotError

from provisa.dbt.manifest import DbtManifest

#: The sqlglot dialect a dbt adapter's compiled SQL is written in.
_ADAPTER_DIALECT = {
    "athena": "athena",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "duckdb": "duckdb",
    "fabric": "fabric",
    "mysql": "mysql",
    "oracle": "oracle",
    "postgres": "postgres",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "sqlserver": "tsql",
    "trino": "trino",
}

#: The dialect the federation graph reads every definition in.
_GRAPH_DIALECT = "postgres"


def _in_graph_dialect(sql: str, adapter: str) -> str | None:
    """``sql`` as the graph reads it, through the AST; None when it will not parse."""
    try:
        tree = sqlglot.parse_one(sql, read=_ADAPTER_DIALECT.get(adapter, adapter))
    except SqlglotError:
        return None
    return tree.sql(dialect=_GRAPH_DIALECT)


def compiled_views(
    manifest: DbtManifest, registered: dict[tuple[str, str], str]
) -> list[tuple[str, str]]:
    """Each compiled model as ``(relation, sql)``. ``registered`` gives the relation a model the
    steward registered is served as, by its ``(schema, relation)`` in the warehouse."""
    views: list[tuple[str, str]] = []
    for model in manifest.models:
        if model.compiled_sql is None:
            continue
        sql = _in_graph_dialect(model.compiled_sql, manifest.adapter)
        if sql is None:
            continue
        key = (model.schema, model.relation)
        views.append((registered.get(key, f"{model.schema}.{model.relation}"), sql))
    return views
