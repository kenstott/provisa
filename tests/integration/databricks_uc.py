# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7f5c91-e4b8-4d63-9c1a-6f0e3b8d7a25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Remove the Unity Catalog objects a Databricks test left: catalogs, schemas, external locations,
storage credentials (REQ-254).

An external link (``provisa/federation/databricks_uc.py``) creates an external location at the
linked object's parent directory and a storage credential named for the bucket. A run seeds its
objects under an R2 prefix of its own, so the locations over that prefix are the run's and are
removed. The credential is named by the product for the bucket alone, so a test reads whether it
exists before it attaches and removes it only if the run created it. Each removal is read back.
"""

from __future__ import annotations

from typing import Any

from tests.integration.databricks_warehouse import _client

_UC = "/api/2.1/unity-catalog"


def _base() -> str:
    import os

    return f"https://{os.environ['DATABRICKS_SERVER_HOSTNAME']}{_UC}"


def _listed(client: Any, kind: str) -> list[dict]:
    """Every entry of a paged Unity Catalog listing (``external-locations``, ...)."""
    entries: list[dict] = []
    token: str | None = None
    while True:
        params = {"page_token": token} if token else {}
        body = client.get(f"{_base()}/{kind}", params=params).raise_for_status().json()
        entries.extend(body.get(kind.replace("-", "_"), []))
        token = body.get("next_page_token")
        if not token:
            return entries


def credential_name(bucket: str) -> str:
    """The storage credential the product creates for ``bucket`` (databricks_uc.ensure_external_link)."""
    from provisa.federation.databricks_uc import _sanitize

    return f"provisa_cred_{_sanitize(bucket)}"


def credential_exists(bucket: str) -> bool:
    with _client() as client:
        return any(
            c["name"] == credential_name(bucket) for c in _listed(client, "storage-credentials")
        )


def remove_locations_under(url_prefix: str) -> None:
    """Delete every external location whose URL lies under the run's ``url_prefix``, then list
    again and raise if any is still there."""
    with _client() as client:
        for loc in _listed(client, "external-locations"):
            if (loc.get("url") or "").startswith(url_prefix):
                client.delete(f"{_base()}/external-locations/{loc['name']}").raise_for_status()
        left = [
            loc["name"]
            for loc in _listed(client, "external-locations")
            if (loc.get("url") or "").startswith(url_prefix)
        ]
    if left:
        raise RuntimeError(f"Unity Catalog external locations left under {url_prefix}: {left}")


def remove_credential(bucket: str, *, existed: bool) -> None:
    """Delete the bucket's storage credential if the run created it (``existed`` is False), then
    read back that it is gone. One that existed before the run is another's and stays."""
    if existed:
        return
    name = credential_name(bucket)
    with _client() as client:
        if not any(c["name"] == name for c in _listed(client, "storage-credentials")):
            return
        client.delete(f"{_base()}/storage-credentials/{name}").raise_for_status()
        if any(c["name"] == name for c in _listed(client, "storage-credentials")):
            raise RuntimeError(f"Unity Catalog storage credential {name} still listed after delete")


def drop_catalog(cursor_of: Any, catalog: str) -> None:
    """``DROP CATALOG ... CASCADE`` (its schemas and tables with it), then read back that it is
    gone. ``cursor_of`` is the open connection to drop it through."""
    cur = cursor_of.cursor()
    try:
        cur.execute(f"DROP CATALOG IF EXISTS `{catalog}` CASCADE")
        cur.execute(f"SHOW CATALOGS LIKE '{catalog}'")
        left = [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
    if left:
        raise RuntimeError(f"Unity Catalog catalog {catalog} still listed after its drop: {left}")


def drop_schema(cursor_of: Any, catalog: str, schema: str) -> None:
    """``DROP SCHEMA ... CASCADE``, then read back that it is gone."""
    cur = cursor_of.cursor()
    try:
        cur.execute(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")
        cur.execute(f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'")
        left = [r[0] for r in cur.fetchall()]
    finally:
        cur.close()
    if left:
        raise RuntimeError(f"Databricks schema {catalog}.{schema} still listed after its drop")
