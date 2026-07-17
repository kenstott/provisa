# Copyright (c) 2026 Kenneth Stott
# Canary: e0d62002-4bc1-4d47-a253-a2e9e24c04e2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Snowflake object/lake ATTACH connectors — zero-copy external links (REQ-988).

Snowflake reads object/lake data on cloud storage IN PLACE via an EXTERNAL STAGE + EXTERNAL TABLE
(``CREATE STAGE … URL=… CREDENTIALS=…`` then ``CREATE EXTERNAL TABLE … LOCATION=@stage FILE_FORMAT=…``),
and Iceberg via native Iceberg tables. So parquet/csv/json/iceberg/delta sources attach as a live
``ATTACH_R`` → SCAN — no landing. Credentials + the cloud location come from the source config; the
runtime INSTALLS the stage and VALIDATES the attach (reads a row) before exposing it.

Not live-verified: no Snowflake account is available in this environment. The connector shape, DDL,
and validation mirror the Databricks/BigQuery external-link connectors, which are live-verified.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.federation.connector_base import Capability, Connector, Mechanism

if TYPE_CHECKING:
    from provisa.core.models import Source

# source_type → Snowflake FILE_FORMAT TYPE.
_SF_FORMAT = {
    "parquet": "PARQUET",
    "csv": "CSV",
    "json": "JSON",
    "iceberg": "ICEBERG",
    "delta_lake": "DELTA",
}


class _SnowflakeObjectLinkConnector(Connector):
    """A Snowflake external-table link over cloud object/lake storage (ATTACH_R → SCAN)."""

    engine = "snowflake"
    mechanism = Mechanism.SCAN  # external table reads the object/lake in place — no copy (REQ-951)

    def capability(self) -> Capability:
        return Capability(predicate_pushdown=True, aggregate_pushdown=True, write=False)

    def details(self, source: "Source") -> dict:
        hints = getattr(source, "federation_hints", {}) or {}
        return {
            "format": _SF_FORMAT[self.source_type],
            "location": getattr(source, "path", None),  # s3:// / gcs:// / azure:// URI
            "credential": {
                "access_key_id": hints.get("access_key_id"),
                "secret_access_key": hints.get("secret_access_key"),
                "endpoint": hints.get("endpoint"),  # S3-compatible (e.g. R2) endpoint, if any
            },
        }


class SnowflakeParquetLinkConnector(_SnowflakeObjectLinkConnector):
    source_type = "parquet"
    key = "snowflake_parquet_link"


class SnowflakeCsvLinkConnector(_SnowflakeObjectLinkConnector):
    source_type = "csv"
    key = "snowflake_csv_link"


class SnowflakeJsonLinkConnector(_SnowflakeObjectLinkConnector):
    source_type = "json"
    key = "snowflake_json_link"


class SnowflakeIcebergLinkConnector(_SnowflakeObjectLinkConnector):
    source_type = "iceberg"
    key = "snowflake_iceberg_link"


class SnowflakeDeltaLinkConnector(_SnowflakeObjectLinkConnector):
    source_type = "delta_lake"
    key = "snowflake_delta_link"


def snowflake_object_link_connectors() -> list[Connector]:
    return [
        SnowflakeParquetLinkConnector(),
        SnowflakeCsvLinkConnector(),
        SnowflakeJsonLinkConnector(),
        SnowflakeIcebergLinkConnector(),
        SnowflakeDeltaLinkConnector(),
    ]


def stage_and_external_table_ddl(
    database: str, schema: str, table: str, stage: str, details: dict
) -> list[str]:
    """The SQL to INSTALL an external stage + EXTERNAL TABLE over the source's cloud location, plus a
    validation SELECT. A single-file URI is split into its directory (the stage URL) and file name
    (an external-table PATTERN). Credentials ride the stage; an S3-compatible ENDPOINT (e.g. R2) is
    passed when present. A source with no ``location`` is a config error (never a guessed URI)."""
    location = details.get("location")
    if not location:
        raise ValueError("snowflake external-link source has no 'path' (cloud URI)")
    fmt = details["format"]
    cred = details.get("credential") or {}
    # dir vs file: a URI ending in a file name stages the directory and PATTERN-matches the file.
    last = location.rsplit("/", 1)[-1]
    if "." in last:
        url, pattern = location.rsplit("/", 1)[0] + "/", last
    else:
        url, pattern = location.rstrip("/") + "/", None
    cred_clause = ""
    if cred.get("access_key_id"):
        cred_clause = (
            f" CREDENTIALS = (AWS_KEY_ID = '{cred['access_key_id']}' "
            f"AWS_SECRET_KEY = '{cred['secret_access_key']}')"
        )
    endpoint_clause = f" ENDPOINT = '{cred['endpoint']}'" if cred.get("endpoint") else ""
    fq = f'"{database}"."{schema}"."{table}"'
    stage_fq = f'"{database}"."{schema}"."{stage}"'
    pattern_clause = f" PATTERN = '.*{pattern}'" if pattern else ""
    return [
        f'CREATE DATABASE IF NOT EXISTS "{database}"',
        f'CREATE SCHEMA IF NOT EXISTS "{database}"."{schema}"',
        f"CREATE STAGE IF NOT EXISTS {stage_fq} URL = '{url}'{cred_clause}{endpoint_clause}",
        f"CREATE OR REPLACE EXTERNAL TABLE {fq} LOCATION = @{stage_fq}{pattern_clause} "
        f"FILE_FORMAT = (TYPE = {fmt}) AUTO_REFRESH = FALSE",
        f"SELECT * FROM {fq} LIMIT 1",  # validate the external attach (creds + reachability)
    ]
