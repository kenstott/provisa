# Copyright (c) 2026 Kenneth Stott
# Canary: 2b3c4d5e-6f70-8192-b3c4-d5e6f7081920
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""GovData JDBC query executor via jaydebeapi.

Each GovDataSource is backed by the Calcite/GovData fat JAR.  Connections are
opened per-request (no persistent pool — GovData uses DuckDB internally which
manages its own connection lifecycle).

JDBC URL construction (in priority order):
  model_file set:       jdbc:calcite:model=<path>
  Single-schema inline: jdbc:govdata:source=<schema>&<params>
  Multi-schema inline:  jdbc:calcite:model=inline:<json>

The inline model injects ``operatingDirectory`` and ``s3Config`` into each
schema operand so the GovData driver can locate its .aperio cache and download
data from Cloudflare R2.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from provisa.core.models import GovDataSource

log = logging.getLogger(__name__)

_GOVDATA_DRIVER = "org.apache.calcite.jdbc.Driver"


def _build_operand(source: GovDataSource, schema: str) -> dict[str, Any]:
    operand: dict[str, Any] = {"dataSource": schema}
    if source.start_year:
        operand["startYear"] = source.start_year
    if source.end_year:
        operand["endYear"] = source.end_year
    if source.ciks:
        operand["ciks"] = source.ciks
    if source.auto_download:
        operand["autoDownload"] = True
    if source.operating_directory:
        operand["operatingDirectory"] = source.operating_directory
    elif source.operating_directory is None:
        # Default: <cwd>/.aperio/<schema> — mirrors GovDataSchemaFactory behaviour
        operand["operatingDirectory"] = os.path.join(os.getcwd(), ".aperio", schema.lower())
    if source.s3_config:
        operand["s3Config"] = source.s3_config
    return operand


def _build_jdbc_url(source: GovDataSource) -> str:
    if source.model_file:
        return f"jdbc:calcite:model={source.model_file}"

    schemas = source.govdata_schemas

    if len(schemas) == 1:
        operand = _build_operand(source, schemas[0])
        # Single-schema shorthand URL; operand fields not expressible in the URL
        # are passed as inline model instead to preserve s3Config / operatingDirectory.
        model = json.dumps(
            {
                "version": "1.0",
                "schemas": [
                    {
                        "name": schemas[0].upper(),
                        "type": "custom",
                        "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
                        "operand": operand,
                    }
                ],
            }
        )
        return f"jdbc:calcite:model=inline:{model}"

    schema_entries = [
        {
            "name": schema.upper(),
            "type": "custom",
            "factory": "org.apache.calcite.adapter.govdata.GovDataSchemaFactory",
            "operand": _build_operand(source, schema),
        }
        for schema in schemas
    ]
    model = json.dumps({"version": "1.0", "schemas": schema_entries})
    return f"jdbc:calcite:model=inline:{model}"


def _env_props(source: GovDataSource) -> dict[str, str]:
    props = {"lex": "ORACLE", "unquotedCasing": "TO_LOWER"}
    for key, val in source.api_keys.items():
        os.environ.setdefault(key, val)
    return props


def connect(source: GovDataSource):
    """Open a jaydebeapi connection to the GovData JDBC adapter.

    Caller is responsible for closing the returned connection.
    """
    import jaydebeapi  # optional dependency

    url = _build_jdbc_url(source)
    log.debug("GovData JDBC connect: %s (jar=%s)", url, source.jar_path)
    return jaydebeapi.connect(
        _GOVDATA_DRIVER,
        url,
        _env_props(source),
        source.jar_path,
    )


def execute_query(source: GovDataSource, sql: str) -> list[dict[str, Any]]:
    """Execute *sql* against *source* and return rows as dicts."""
    conn = connect(source)
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        cols = [desc[0].lower() for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    finally:
        conn.close()
