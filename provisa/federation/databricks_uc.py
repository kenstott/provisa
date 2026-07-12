# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unity Catalog credential + external-location provisioning for Databricks external links (REQ-987).

A Databricks external table over cloud storage needs a UC **storage credential** + **external
location** — and those can't be created from the SQL warehouse (``CREATE STORAGE CREDENTIAL`` is not
parseable on serverless); they are account-admin operations via the Unity Catalog REST API. This
module INSTALLS them (idempotently) and lets Databricks VALIDATE them on create (``skip_validation``
False → UC pings the store and the path), so the ATTACH connector proves the credentials work before
any table is created. Credentials come from the source config — never guessed.

Cloudflare R2 (S3-compatible) uses a ``cloudflare_api_token`` credential; native AWS S3 uses an IAM
role. The location URL scheme selects which.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any
from urllib.parse import urlparse


class ExternalLinkError(RuntimeError):
    """A UC credential/external-location install or validation failed — surfaced, never swallowed."""


def parse_location(url: str) -> tuple[str, str, str]:
    """Split an object-store URL into ``(scheme, bucket, root_url)``.

    ``r2://pubs@acct.r2.cloudflarestorage.com/dir/file.parquet`` → ``("r2", "pubs",
    "r2://pubs@acct.r2.cloudflarestorage.com/")``. The root URL is the external-location prefix that
    contains the table, so many sources under one bucket share a single external location."""
    u = urlparse(url)
    if not u.scheme or not u.netloc:
        raise ExternalLinkError(f"external-link source needs a cloud URL (got {url!r})")
    bucket = u.netloc.split("@", 1)[0]
    root = f"{u.scheme}://{u.netloc}/"
    return u.scheme, bucket, root


def _sanitize(s: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in s).strip("_").lower()


class _UC:
    """Thin Unity Catalog REST client (bearer token). Uses the ambient CA bundle via requests env."""

    def __init__(self, host: str, token: str) -> None:
        self._base = f"https://{host}"
        self._h = {"Authorization": f"Bearer {token}"}

    def _call(self, method: str, path: str, body: dict | None = None) -> tuple[int, Any]:
        import requests

        r = requests.request(method, self._base + path, headers=self._h, json=body, timeout=60)
        try:
            payload = r.json()
        except ValueError:
            payload = r.text
        return r.status_code, payload

    def get(self, path: str) -> tuple[int, Any]:
        return self._call("GET", path)

    def post(self, path: str, body: dict) -> tuple[int, Any]:
        return self._call("POST", path, body)


def _credential_body(name: str, scheme: str, cred: dict) -> dict:
    """The UC storage-credential create body for the cloud scheme. R2 → cloudflare_api_token."""
    if scheme == "r2":
        missing = [
            k for k in ("access_key_id", "secret_access_key", "account_id") if not cred.get(k)
        ]
        if missing:
            raise ExternalLinkError(
                f"R2 external link needs {missing} in the source federation_hints"
            )
        return {
            "name": name,
            "cloudflare_api_token": {
                "access_key_id": cred["access_key_id"],
                "secret_access_key": cred["secret_access_key"],
                "account_id": cred["account_id"],
            },
            "comment": "provisa external link",
            "skip_validation": False,  # UC validates the credential against R2 on create
        }
    raise ExternalLinkError(
        f"external-link scheme {scheme!r} not supported yet (R2 supported; AWS S3 uses an IAM role)"
    )


def _covering_location(uc: "_UC", location: str) -> str | None:
    """An existing UC external location whose URL is a prefix of ``location`` (already governs it), or
    None. UC permits only one external location per path, so coverage — not name — is what matters."""
    st, payload = uc.get("/api/2.1/unity-catalog/external-locations")
    if st != HTTPStatus.OK or not isinstance(payload, dict):
        return None
    for loc in payload.get("external_locations", []):
        url = loc.get("url") or ""
        if url and location.startswith(url):
            return url
    return None


def ensure_external_link(host: str, token: str, *, location: str, credential: dict) -> str:
    """Install (idempotent) + validate the UC storage credential and external location that governs
    ``location``, so a Databricks external table over it can be created. Returns the governing
    external-location URL.

    If an existing external location already COVERS the path, it is reused. Otherwise a new one is
    created at the source's PARENT DIRECTORY (narrow — so sibling sources under the same bucket don't
    collide on overlapping locations). Databricks validates the credential + path access on create, so
    a bad credential or unreachable path raises ``ExternalLinkError`` here — before any table DDL, and
    never a guess."""
    if not location:
        raise ExternalLinkError("external-link source has no 'path' (cloud location)")
    scheme, bucket, _ = parse_location(location)
    uc = _UC(host, token)

    # 1) already governed by an existing external location? reuse it (its credential is what reads).
    covered = _covering_location(uc, location)
    if covered is not None:
        return covered

    # 2) storage credential — create if absent (a name conflict means it already exists → reuse).
    cred_name = f"provisa_cred_{_sanitize(bucket)}"
    status, _ = uc.get(f"/api/2.1/unity-catalog/storage-credentials/{cred_name}")
    if status == HTTPStatus.NOT_FOUND:
        st, payload = uc.post(
            "/api/2.1/unity-catalog/storage-credentials",
            _credential_body(cred_name, scheme, credential),
        )
        if st != HTTPStatus.OK:
            raise ExternalLinkError(
                f"UC storage-credential install/validate failed ({st}): {payload}"
            )
    elif status != HTTPStatus.OK:
        raise ExternalLinkError(f"UC storage-credential lookup failed ({status})")

    # 3) external location at the source's PARENT DIRECTORY — validated on create.
    parent = location.rsplit("/", 1)[0] + "/"
    loc_name = f"provisa_loc_{_sanitize(parent)}"[:200]
    st, payload = uc.post(
        "/api/2.1/unity-catalog/external-locations",
        {
            "name": loc_name,
            "url": parent,
            "credential_name": cred_name,
            "comment": "provisa external link",
            "skip_validation": False,  # UC validates read access to the path on create
        },
    )
    if st != HTTPStatus.OK:
        # A concurrent create or a covering location appeared — re-check coverage before failing.
        covered = _covering_location(uc, location)
        if covered is not None:
            return covered
        raise ExternalLinkError(f"UC external-location install/validate failed ({st}): {payload}")
    return parent
