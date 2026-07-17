# Copyright (c) 2026 Kenneth Stott
# Canary: a1adafad-2c95-4f28-b0ca-4cf1784893d4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fabric OneLake shortcut auto-provisioning for external-data links (Fabric engine).

Microsoft Fabric's native external-data mechanism is a OneLake **shortcut**: it virtualizes external
object storage (S3-compatible / R2, ADLS, GCS) into OneLake, where the warehouse reads it. Attaching an
external source therefore auto-creates ALL its prerequisites via the Fabric REST API — exactly like the
Databricks connector auto-creates its UC credential + external location:

  1. an ``AmazonS3Compatible`` **connection** holding the store's endpoint + access key/secret,
  2. a **Lakehouse** (once) to host shortcuts,
  3. a **shortcut** ``Files/<name>`` → the external bucket/subpath, referencing the connection.

It returns the OneLake path (workspace-GUID / lakehouse-item-GUID form — friendly names are rejected)
the warehouse then reads via ``OPENROWSET``. All steps are idempotent (reused by name). Azure AD auth
via ``azure-identity`` (the ``az login`` / managed identity), a Fabric API token."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

_FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"
_API = "https://api.fabric.microsoft.com/v1"


class FabricShortcutError(RuntimeError):
    """A Fabric connection / shortcut install failed — surfaced, never swallowed."""


def _sanitize(s: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in s).strip("_")


class _Fabric:
    def __init__(self) -> None:
        from azure.identity import DefaultAzureCredential

        self._token = DefaultAzureCredential().get_token(_FABRIC_SCOPE).token

    def _req(self, method: str, path: str, body: dict | None = None) -> tuple[int, Any]:
        import requests

        r = requests.request(
            method,
            _API + path,
            headers={"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"},
            json=body,
            timeout=120,
        )
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, r.text

    def ensure_connection(self, endpoint: str, access_key: str, secret: str) -> str:
        """An ``AmazonS3Compatible`` connection for the S3-compatible endpoint (idempotent by name).
        The URL is the ENDPOINT only (no bucket); credentials are Basic (key id = user, secret = pw)."""
        name = f"provisa_{_sanitize(endpoint.split('//', 1)[-1])}"[:64]
        st, payload = self._req("GET", "/connections")
        if st == HTTPStatus.OK:
            for c in payload.get("value", []):
                if c.get("displayName") == name:
                    return c["id"]
        body = {
            "connectivityType": "ShareableCloud",
            "displayName": name,
            "connectionDetails": {
                "type": "AmazonS3Compatible",
                "creationMethod": "AmazonS3Compatible.Storage",
                "parameters": [{"dataType": "Text", "name": "url", "value": endpoint}],
            },
            "credentialDetails": {
                "singleSignOnType": "None",
                "connectionEncryption": "NotEncrypted",
                "skipTestConnection": False,  # Fabric tests the endpoint/creds on create
                "credentials": {
                    "credentialType": "Basic",
                    "username": access_key,
                    "password": secret,
                },
            },
        }
        st, payload = self._req("POST", "/connections", body)
        if st not in (HTTPStatus.OK, HTTPStatus.CREATED):
            raise FabricShortcutError(
                f"Fabric S3-compatible connection create failed ({st}): {payload}"
            )
        return payload["id"]

    def ensure_lakehouse(self, workspace_id: str, name: str = "provisa_shortcuts") -> str:
        """A Lakehouse to host shortcuts (idempotent by name)."""
        st, payload = self._req("GET", f"/workspaces/{workspace_id}/items")
        if st == HTTPStatus.OK:
            for it in payload.get("value", []):
                if it.get("type") == "Lakehouse" and it.get("displayName") == name:
                    return it["id"]
        st, payload = self._req(
            "POST", f"/workspaces/{workspace_id}/lakehouses", {"displayName": name}
        )
        if st in (HTTPStatus.OK, HTTPStatus.CREATED):
            return payload["id"]
        # long-running create → poll the item list
        if st == HTTPStatus.ACCEPTED:
            import time

            for _ in range(20):
                time.sleep(3)
                st2, p2 = self._req("GET", f"/workspaces/{workspace_id}/items")
                if st2 == HTTPStatus.OK:
                    for it in p2.get("value", []):
                        if it.get("type") == "Lakehouse" and it.get("displayName") == name:
                            return it["id"]
        raise FabricShortcutError(f"Fabric lakehouse create failed ({st}): {payload}")

    def ensure_shortcut(
        self,
        workspace_id: str,
        lakehouse_id: str,
        name: str,
        connection_id: str,
        endpoint: str,
        bucket: str,
        subpath: str,
    ) -> None:
        """A OneLake shortcut ``Files/<name>`` → the S3-compatible bucket/subpath (idempotent)."""
        st, _ = self._req(
            "GET", f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts/Files/{name}"
        )
        if st == HTTPStatus.OK:
            return
        body = {
            "path": "Files",
            "name": name,
            "target": {
                "type": "S3Compatible",
                "s3Compatible": {
                    "connectionId": connection_id,
                    "location": endpoint,
                    "subpath": subpath if subpath.startswith("/") else "/" + subpath,
                    "bucket": bucket,
                },
            },
        }
        st, payload = self._req(
            "POST", f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts", body
        )
        if st not in (HTTPStatus.OK, HTTPStatus.CREATED):
            raise FabricShortcutError(f"Fabric shortcut create failed ({st}): {payload}")


def ensure_external_shortcut(
    *,
    workspace_id: str,
    endpoint: str,
    bucket: str,
    subpath: str,
    filename: str,
    access_key: str,
    secret: str,
    name: str,
) -> str:
    """Auto-provision the full external-data chain (connection → lakehouse → shortcut) for an
    S3-compatible source, and return the OneLake ``BULK`` path the warehouse reads via OPENROWSET.

    ``subpath`` is the bucket-relative directory; ``filename`` the object within it. The returned path
    uses the workspace + lakehouse-item GUIDs (Fabric rejects friendly names)."""
    if not (endpoint and access_key and secret):
        raise FabricShortcutError(
            "Fabric external link needs endpoint + access_key_id + secret_access_key in federation_hints"
        )
    fab = _Fabric()
    conn_id = fab.ensure_connection(endpoint, access_key, secret)
    lakehouse_id = fab.ensure_lakehouse(workspace_id)
    fab.ensure_shortcut(workspace_id, lakehouse_id, name, conn_id, endpoint, bucket, subpath)
    return f"https://onelake.dfs.fabric.microsoft.com/{workspace_id}/{lakehouse_id}/Files/{name}/{filename}"
