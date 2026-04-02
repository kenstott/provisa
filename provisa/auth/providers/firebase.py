# Copyright (c) 2025 Kenneth Stott
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

try:
    import firebase_admin
    from firebase_admin import auth as firebase_auth, credentials

    _HAS_FIREBASE = True
except ImportError:
    _HAS_FIREBASE = False


class FirebaseAuthProvider(AuthProvider):
    """Validates Firebase ID tokens via firebase-admin SDK."""

    def __init__(self, project_id: str, service_account_key: str | None = None) -> None:
        if not _HAS_FIREBASE:
            raise ImportError(
                "firebase-admin is required: pip install provisa[firebase]"
            )
        self._project_id = project_id
        if not firebase_admin._apps:
            cred = (
                credentials.Certificate(service_account_key)
                if service_account_key
                else credentials.ApplicationDefault()
            )
            firebase_admin.initialize_app(cred, {"projectId": project_id})

    async def validate_token(self, token: str) -> AuthIdentity:
        decoded = firebase_auth.verify_id_token(token)
        return AuthIdentity(
            user_id=decoded["uid"],
            email=decoded.get("email"),
            display_name=decoded.get("name"),
            roles=decoded.get("roles", []),
            raw_claims=decoded,
        )
