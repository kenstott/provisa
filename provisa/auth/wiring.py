# Copyright (c) 2025 Kenneth Stott
# Canary: f364d766-4f15-4260-b1f3-9232f7676ef3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Wire auth middleware and routes into the FastAPI application."""

from __future__ import annotations

import os

from fastapi import FastAPI

from provisa.auth.models import AuthProvider


def build_auth_provider(auth_config: dict) -> AuthProvider:
    """Instantiate the configured auth provider from the auth config section."""
    provider_name = auth_config["provider"]
    if provider_name == "simple":
        from provisa.auth.providers.simple import SimpleAuthProvider
        simple_cfg = auth_config.get("simple", {})
        jwt_secret = auth_config.get("jwt_secret", "")
        if jwt_secret.startswith("${env:"):
            env_key = jwt_secret[6:-1]
            jwt_secret = os.environ[env_key]
        return SimpleAuthProvider(
            users=simple_cfg.get("users", []),
            jwt_secret=jwt_secret,
        )
    if provider_name == "firebase":
        from provisa.auth.providers.firebase import FirebaseAuthProvider
        return FirebaseAuthProvider(auth_config.get("firebase", {}))
    if provider_name == "keycloak":
        from provisa.auth.providers.keycloak import KeycloakAuthProvider
        return KeycloakAuthProvider(auth_config.get("keycloak", {}))
    if provider_name == "oauth":
        from provisa.auth.providers.oauth import OAuthProvider
        return OAuthProvider(auth_config.get("oauth", {}))
    raise ValueError(f"Unknown auth provider: {provider_name!r}")


def wire_auth(app: FastAPI, auth_config: dict | None) -> None:
    """Conditionally register AuthMiddleware and auth routes based on config."""
    if auth_config is None:
        return

    provider = build_auth_provider(auth_config)
    mapping_rules = auth_config.get("role_mapping", [])
    default_role = auth_config.get("default_role", "analyst")

    from provisa.auth.middleware import AuthMiddleware
    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=mapping_rules,
        default_role=default_role,
    )

    # Mount simple auth login route when provider=simple
    if auth_config["provider"] == "simple":
        from provisa.auth.providers import simple as simple_mod
        simple_mod._provider_instance = provider
        app.include_router(simple_mod.router)
