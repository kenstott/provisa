# Copyright (c) 2026 Kenneth Stott
# Canary: bd2bb81e-7680-4750-94cd-1b5bc0a9a0c4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BigQuery object/lake ATTACH connectors — zero-copy external links (REQ — BigQuery engine).

BigQuery reads object/lake data on cloud storage IN PLACE via external / BigLake tables
(``CREATE EXTERNAL TABLE … OPTIONS(format=…, uris=[…])``). So parquet/csv/json/iceberg/delta sources
on GCS (and, via BigLake + a cross-cloud Connection, S3/Azure) attach as a live ``ATTACH_R`` → SCAN —
no landing, no copy. A plain in-project GCS external table needs no connection; BigLake / cross-cloud
sources reference a BigQuery ``Connection`` carried in ``federation_hints``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.federation.connector_base import Capability, Connector, Mechanism

if TYPE_CHECKING:
    from provisa.core.models import Source

# source_type → BigQuery external-table OPTIONS format keyword.
_BQ_FORMAT = {
    "parquet": "PARQUET",
    "csv": "CSV",
    "json": "NEWLINE_DELIMITED_JSON",
    "iceberg": "ICEBERG",
    "delta_lake": "DELTA_LAKE",
}


class _BigQueryObjectLinkConnector(Connector):
    """A BigQuery external-table link over cloud object/lake storage (ATTACH_R → SCAN)."""

    engine = "bigquery"
    mechanism = Mechanism.SCAN  # external table reads the object/lake in place — no copy (REQ-951)

    def capability(self) -> Capability:
        # BigQuery pushes predicates/aggregates into the external-table scan; read-only (no upstream write).
        return Capability(predicate_pushdown=True, aggregate_pushdown=True, write=False)

    def details(self, source: "Source") -> dict:
        hints = getattr(source, "federation_hints", {}) or {}
        return {
            "format": _BQ_FORMAT[self.source_type],
            "location": getattr(source, "path", None),  # a gs:// (or, with a connection, s3://) URI
            "connection": hints.get("connection"),  # BigLake / cross-cloud Connection (optional)
        }


class BigQueryParquetLinkConnector(_BigQueryObjectLinkConnector):
    source_type = "parquet"
    key = "bigquery_parquet_link"


class BigQueryCsvLinkConnector(_BigQueryObjectLinkConnector):
    source_type = "csv"
    key = "bigquery_csv_link"


class BigQueryJsonLinkConnector(_BigQueryObjectLinkConnector):
    source_type = "json"
    key = "bigquery_json_link"


class BigQueryIcebergLinkConnector(_BigQueryObjectLinkConnector):
    source_type = "iceberg"
    key = "bigquery_iceberg_link"


class BigQueryDeltaLinkConnector(_BigQueryObjectLinkConnector):
    source_type = "delta_lake"
    key = "bigquery_delta_link"


def bigquery_object_link_connectors() -> list[Connector]:
    return [
        BigQueryParquetLinkConnector(),
        BigQueryCsvLinkConnector(),
        BigQueryJsonLinkConnector(),
        BigQueryIcebergLinkConnector(),
        BigQueryDeltaLinkConnector(),
    ]


def external_table_ddl(project: str, dataset: str, table: str, details: dict) -> str:
    """``CREATE OR REPLACE EXTERNAL TABLE`` DDL for a BigQuery external link. A ``gs://…/dir/file`` URI
    is expanded to a ``dir/*`` glob (BigQuery external tables scan a URI prefix). A ``connection`` (for
    BigLake / cross-cloud) is referenced with ``WITH CONNECTION`` when present; a plain in-project GCS
    external table needs none. A source with no ``location`` is a config error (never a guessed URI)."""
    location = details.get("location")
    if not location:
        raise ValueError("bigquery external-link source has no 'path' (gs:// URI)")
    fmt = details["format"]
    # A single-file URI scans fine; a directory URI needs a trailing glob.
    uri = (
        location
        if location.endswith("*") or "." in location.rsplit("/", 1)[-1]
        else location.rstrip("/") + "/*"
    )
    conn = details.get("connection")
    with_conn = f"WITH CONNECTION `{conn}` " if conn else ""
    return (
        f"CREATE OR REPLACE EXTERNAL TABLE `{project}`.`{dataset}`.`{table}` "
        f"{with_conn}OPTIONS (format = '{fmt}', uris = ['{uri}'])"
    )
