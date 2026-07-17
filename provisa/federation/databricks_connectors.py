# Copyright (c) 2026 Kenneth Stott
# Canary: 65fe0ebc-e3c4-4147-9b7a-59e40cfcfae4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Databricks object/lake ATTACH connectors — zero-copy external links (REQ-987).

Databricks reads object/lake data on cloud storage IN PLACE via Unity Catalog external tables
(``CREATE TABLE … USING <format> LOCATION '<url>'``). So parquet/csv/iceberg/delta sources on S3 (or
S3-compatible stores like Cloudflare R2) attach as a live ``ATTACH_R`` → SCAN — no landing, no copy.

Each connector declares its Databricks storage FORMAT and projects a ``details`` payload the runtime
uses to (1) INSTALL + VALIDATE the required Unity Catalog storage credential + external location via
the UC REST API, then (2) create the external table at the compiler's physical name. Credentials and
the cloud location come from the source config (``path`` + ``federation_hints``); nothing is guessed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from provisa.federation.connector_base import Capability, Connector, Mechanism

if TYPE_CHECKING:
    from provisa.core.models import Source


class _DatabricksObjectLinkConnector(Connector):
    """A Databricks external-table link over cloud object/lake storage (ATTACH_R → SCAN)."""

    engine = "databricks"
    mechanism = Mechanism.SCAN  # external table reads the object/lake in place — no copy (REQ-951)
    _format = ""  # the Databricks USING <format> keyword (PARQUET / CSV / ICEBERG / DELTA)

    def capability(self) -> Capability:
        # Databricks (Photon + Delta/Parquet data skipping) pushes predicates and aggregates into the
        # scan; the external link is read-only (no upstream write).
        return Capability(predicate_pushdown=True, aggregate_pushdown=True, write=False)

    def details(self, source: "Source") -> dict:
        """The attach payload: storage format, the cloud location, and the credential the runtime must
        install + validate in Unity Catalog before creating the external table. A source with no
        ``path`` is a config error surfaced at attach time (never a guessed location)."""
        hints = getattr(source, "federation_hints", {}) or {}
        return {
            "format": self._format,
            "location": getattr(source, "path", None),
            "credential": {
                "access_key_id": hints.get("access_key_id"),
                "secret_access_key": hints.get("secret_access_key"),
                "account_id": hints.get("account_id"),  # Cloudflare R2 account (S3-compatible)
            },
        }


class DatabricksParquetLinkConnector(_DatabricksObjectLinkConnector):
    source_type = "parquet"
    key = "databricks_parquet_link"
    _format = "PARQUET"


class DatabricksCsvLinkConnector(_DatabricksObjectLinkConnector):
    source_type = "csv"
    key = "databricks_csv_link"
    _format = "CSV"


class DatabricksIcebergLinkConnector(_DatabricksObjectLinkConnector):
    source_type = "iceberg"
    key = "databricks_iceberg_link"
    _format = "ICEBERG"


class DatabricksDeltaLinkConnector(_DatabricksObjectLinkConnector):
    source_type = "delta_lake"
    key = "databricks_delta_link"
    _format = "DELTA"


def databricks_object_link_connectors() -> list[Connector]:
    """The Databricks object/lake external-link connectors (zero-copy SCAN over cloud storage)."""
    return [
        DatabricksParquetLinkConnector(),
        DatabricksCsvLinkConnector(),
        DatabricksIcebergLinkConnector(),
        DatabricksDeltaLinkConnector(),
    ]
