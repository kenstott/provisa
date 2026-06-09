# Copyright (c) 2026 Kenneth Stott
# Canary: 81c145bc-e238-4939-a509-fa64e81d2c3c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Firebase ID token validation provider."""

from __future__ import annotations

from provisa.auth.models import AuthIdentity, AuthProvider
from provisa.core.secrets import resolve_secrets

firebase_admin = None
credentials = None
firebase_auth = None
try:
    import firebase_admin
    from firebase_admin import credentials, auth as firebase_auth

    _HAS_FIREBASE = True
except ImportError:
    _HAS_FIREBASE = False


class FirebaseAuthProvider(AuthProvider):
    """Validates Firebase ID tokens via firebase-admin SDK."""

    provider_name: str = "firebase"

    def __init__(self, firebase_config: dict) -> None:
        if not _HAS_FIREBASE:
            raise ImportError("firebase-admin is required: pip install provisa[firebase]")
        project_id = firebase_config.get("project_id", "")
        # Resolve ${env:...} placeholders; an unset FIREBASE_SERVICE_ACCOUNT_KEY
        # yields "" (no key file) rather than a literal path that would be opened.
        service_account_key = resolve_secrets(firebase_config.get("service_account_key") or "")
        self._project_id = project_id
        if not firebase_admin._apps:  # type: ignore[union-attr]
            cred = (
                credentials.Certificate(service_account_key)  # type: ignore[union-attr]
                if service_account_key
                else credentials.ApplicationDefault()  # type: ignore[union-attr]
            )
            firebase_admin.initialize_app(cred, {"projectId": project_id})  # type: ignore[union-attr]

    async def validate_token(self, token: str) -> AuthIdentity:
        decoded = firebase_auth.verify_id_token(token)  # type: ignore[union-attr]
        return AuthIdentity(
            user_id=decoded["uid"],
            email=decoded.get("email"),
            display_name=decoded.get("name"),
            roles=decoded.get("roles", []),
            raw_claims=decoded,
        )
