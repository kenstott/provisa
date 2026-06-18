# Copyright (c) 2026 Kenneth Stott
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


# auth.provider: basic — uses local_users table; db_pool injected at startup
def build_auth_provider(auth_config: dict, db_pool=None) -> AuthProvider:
    """Instantiate the configured auth provider from the auth config section."""
    provider_name = auth_config["provider"]
    if provider_name == "basic":
        from provisa.auth.providers.basic import BasicAuthProvider

        return BasicAuthProvider(db_pool=db_pool)
    if provider_name == "simple":
        # REQ-124: simple username/password auth is for testing only and must be
        # explicitly opted into. Refuse to build it in the absence of the flag.
        if not auth_config.get("allow_simple_auth", False):
            raise ValueError(
                "auth.provider 'simple' requires auth.allow_simple_auth: true "
                "(simple username/password auth is not for production)"
            )
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

        kc = auth_config.get("keycloak", {})
        return KeycloakAuthProvider(
            server_url=kc["server_url"],
            realm=kc["realm"],
            client_id=kc["client_id"],
            client_secret=kc.get("client_secret"),
        )
    if provider_name == "oauth":
        from provisa.auth.providers.oauth import OAuthProvider

        oa = auth_config.get("oauth", {})
        return OAuthProvider(
            discovery_url=oa["discovery_url"],
            client_id=oa["client_id"],
            audience=oa.get("audience"),
            role_claim=oa.get("role_claim", "roles"),
        )
    raise ValueError(f"Unknown auth provider: {provider_name!r}")


def wire_auth(app: FastAPI, auth_config: dict | None, db_pool=None) -> None:
    """Conditionally register AuthMiddleware and auth routes based on config."""
    if auth_config is None:
        return

    provider = build_auth_provider(auth_config, db_pool=db_pool)
    mapping_rules = auth_config.get("role_mapping", [])
    default_role = auth_config.get("default_role", "analyst")

    from provisa.api.app import state as _app_state

    cfg = getattr(_app_state, "config", None)
    multitenancy = getattr(cfg, "multitenancy", False) if cfg else False
    default_org_id = getattr(cfg, "default_org_id", "root") if cfg else "root"

    _app_state.auth_middleware_active = True

    from provisa.auth.middleware import AuthMiddleware

    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=mapping_rules,
        default_role=default_role,
        db_pool=db_pool,
        assignments_source=auth_config.get("assignments_source", "claims"),
        default_assignments=auth_config.get("default_assignments", []),
        multitenancy=multitenancy,
        default_org_id=default_org_id,
    )

    # Mount simple auth login route when provider=simple
    if auth_config["provider"] == "simple":
        from provisa.auth.providers import simple as simple_mod

        simple_mod._provider_instance = provider
        app.include_router(simple_mod.router)
