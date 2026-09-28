# Copyright (c) 2026 Kenneth Stott
# Canary: 760cf319-f864-425d-b805-c409ce4e9b53
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Resolve a Databricks Unity Catalog table's live Iceberg metadata pointer (REQ-1867).

Unity Catalog exposes an Apache Iceberg REST catalog endpoint
(``/api/2.1/unity-catalog/iceberg/v1/...``) for any table that is a native Iceberg table or has
UniForm Iceberg enabled. ``LoadTable`` on that endpoint answers with the table's CURRENT
``metadata-location`` — the same pointer DuckDB's ``iceberg_scan`` needs to read the table's live
snapshot in place, zero-copy. Auth follows the same convention as
``provisa.executor.drivers.databricks.DatabricksDriver``: a bearer token carried as the source's
``password``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import httpx

if TYPE_CHECKING:
    from provisa.core.models import Source


def resolve_databricks_iceberg_metadata_location(source: "Source") -> str:
    """Return the table's current Iceberg ``metadata-location`` via Unity Catalog's Iceberg REST API.

    ``source.database`` is the Unity Catalog catalog name; the schema/table live in
    ``federation_hints`` (there is no standard ``Source`` field for a schema-qualified table name).
    Every required field is validated up front and raises loud — none has a sane default, so
    guessing one would silently point ``iceberg_scan`` at the wrong table.
    """
    if not source.host:
        raise ValueError(f"databricks source {source.id!r} requires 'host' (workspace hostname)")
    if not source.password:
        raise ValueError(
            f"databricks source {source.id!r} requires 'password' (Databricks personal access token)"
        )
    if not source.database:
        raise ValueError(
            f"databricks source {source.id!r} requires 'database' (Unity Catalog catalog name)"
        )
    schema = source.federation_hints.get("schema")
    table = source.federation_hints.get("table")
    if not schema or not table:
        raise ValueError(
            f"databricks source {source.id!r} requires 'schema' and 'table' in federation_hints"
        )

    url = (
        f"https://{source.host}/api/2.1/unity-catalog/iceberg/v1/"
        f"catalogs/{source.database}/namespaces/{schema}/tables/{table}"
    )
    resp = httpx.get(
        url,
        headers={"Authorization": f"Bearer {source.password}"},
        timeout=10.0,
    )
    resp.raise_for_status()
    body = resp.json()
    metadata_location = body.get("metadata-location")
    if not metadata_location:
        raise ValueError(
            f"databricks source {source.id!r}: Iceberg REST response has no 'metadata-location'"
        )
    return metadata_location
