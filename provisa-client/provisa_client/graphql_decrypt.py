# Copyright (c) 2026 Kenneth Stott
# Canary: 8a2f3c91-6d5e-4b7a-8c0f-2e9d1a4b6c88
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Client-side decryption wrapper for Provisa GraphQL (REQ-692).

The Provisa GraphQL schema marks encrypted fields with an ``@encrypted`` directive.
This thin wrapper discovers the flagged fields (from the schema SDL) and decrypts
them in a response before returning it to the caller — the backend only ever
returns the ciphertext blob. Scoped to non-browser clients: browser-side KMS
access weakens the threat model, so this ships in the Python client library only.

Decrypt failure is loud: a flagged field that cannot be decrypted raises
``DecryptionError`` up through the wrapper.
"""

from __future__ import annotations

import re
from typing import Any

import httpx

from provisa_client.encryption import (
    ClientEncryptionService,
    DecryptionError,
    build_client_encryption,
)

_ENCRYPTED_FIELD_RE = re.compile(
    r"^\s*(\w+)\s*(?:\([^)]*\))?\s*:\s*[\[\]\w!]+\s*@encrypted\b",
    re.MULTILINE,
)

ENCRYPTED_DIRECTIVE_SDL = (
    "directive @encrypted on FIELD_DEFINITION  "
    "# REQ-692: field value is a client-decryptable envelope blob"
)


def encrypted_fields_from_sdl(sdl: str) -> set[str]:
    """Return the set of field names carrying the ``@encrypted`` directive in an SDL string."""
    return set(_ENCRYPTED_FIELD_RE.findall(sdl))


def decrypt_response(
    data: Any,
    encrypted_fields: set[str],
    svc: ClientEncryptionService,
) -> Any:
    """Recursively decrypt every ``@encrypted``-flagged field in a GraphQL response.

    Walks dicts and lists; any key in ``encrypted_fields`` has its value decrypted
    via the client EncryptionService. Raises ``DecryptionError`` on failure (loud).
    """
    if isinstance(data, dict):
        out: dict[str, Any] = {}
        for key, value in data.items():
            if key in encrypted_fields and not isinstance(value, (dict, list)):
                out[key] = svc.decrypt_field(value)
            else:
                out[key] = decrypt_response(value, encrypted_fields, svc)
        return out
    if isinstance(data, list):
        return [decrypt_response(item, encrypted_fields, svc) for item in data]
    return data


class GraphQLDecryptClient:
    """GraphQL client that transparently decrypts ``@encrypted`` fields (REQ-692).

    Construct with the server URL and the client's KMS params. On first use it
    fetches the role-scoped SDL (``/data/sdl``) to learn which fields are
    ``@encrypted``, then decrypts those fields in every query response.
    """

    def __init__(
        self,
        url: str = "http://localhost:8001",
        *,
        role: str = "admin",
        token: str | None = None,
        kms_provider: str | None = None,
        kms_key_arn: str | None = None,
        dek_cache_ttl: float = 300.0,
        _kms_client: Any = None,
        _http: httpx.Client | None = None,
    ) -> None:
        self._base = url.rstrip("/")
        self._role = role
        self._token = token
        self._kms_key_arn = kms_key_arn  # REQ-693: high-security gate proof-of-client-decrypt
        self._http = _http or httpx.Client(timeout=10.0)
        self._encryption = build_client_encryption(
            kms_provider=kms_provider,
            kms_key_arn=kms_key_arn,
            dek_cache_ttl=dek_cache_ttl,
            _client=_kms_client,
        )
        self._encrypted_fields: set[str] | None = None

    def _headers(self) -> dict[str, str]:
        h = {"Content-Type": "application/json", "X-Role": self._role}
        if self._token:
            h["Authorization"] = f"Bearer {self._token}"
        if self._kms_key_arn:
            h["X-Provisa-KMS-Key"] = self._kms_key_arn
        return h

    def _load_encrypted_fields(self) -> set[str]:
        if self._encrypted_fields is None:
            r = self._http.get(f"{self._base}/data/sdl", headers=self._headers())
            r.raise_for_status()
            self._encrypted_fields = encrypted_fields_from_sdl(r.text)
        return self._encrypted_fields

    def query(self, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
        """Run a GraphQL query and decrypt any ``@encrypted`` fields in the response."""
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        r = self._http.post(f"{self._base}/data/graphql", json=payload, headers=self._headers())
        r.raise_for_status()
        body = r.json()
        data = body.get("data")
        if data is None:
            return body
        fields = self._load_encrypted_fields()
        if not fields or self._encryption is None:
            if fields and self._encryption is None:
                raise DecryptionError(
                    "schema marks @encrypted fields but no kms_provider/kms_key_arn "
                    "configured on this GraphQL client (REQ-692)"
                )
            return body
        body["data"] = decrypt_response(data, fields, self._encryption)
        return body

    def close(self) -> None:
        self._http.close()
