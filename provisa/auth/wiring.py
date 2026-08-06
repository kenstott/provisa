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
from provisa.auth.superuser import resolve_superuser_config

# Requirements: REQ-120, REQ-121, REQ-122, REQ-123, REQ-124, REQ-125


# auth.provider: basic — reads the local_users table, which lives in the
# platform control plane; the platform pool is injected at startup.
def build_auth_provider(
    auth_config: dict, admin_pool=None
) -> AuthProvider:  # REQ-120, REQ-121, REQ-122, REQ-123, REQ-124
    """Instantiate the configured auth provider from the auth config section.

    REQ-1263: the personal-access-token store is attached to whichever provider is built, so a
    PAT is accepted under every provider and on every surface. It needs the platform control
    plane; without one there is nowhere to read tokens from and the attribute stays None,
    leaving a PAT-shaped credential to fail as an unknown provider token.
    """
    # REQ-1393: every surface reaches its provider through here, so this is where the shared
    # login throttle learns the deployment's settings. Identical settings keep the existing
    # counters — the wire surfaces build a provider per connection.
    from provisa.auth.throttle import configure_login_throttle

    configure_login_throttle(auth_config)

    provider = _construct_provider(auth_config, admin_pool)
    if admin_pool is not None:
        from provisa.auth.pat import PersonalAccessTokenStore

        provider.pat_store = PersonalAccessTokenStore(admin_pool)
    return provider


def _construct_provider(auth_config: dict, admin_pool) -> AuthProvider:
    provider_name = auth_config["provider"]
    if provider_name == "basic":
        from provisa.auth.providers.basic import BasicAuthProvider

        # Session-token signing key for the browser login route. Absent means API clients
        # still work over HTTP Basic and /auth/login answers 503 — never a default key.
        session_secret = auth_config.get("jwt_secret")
        if session_secret and session_secret.startswith("${env:"):
            session_secret = os.environ[session_secret[6:-1]]
        return BasicAuthProvider(db_pool=admin_pool, session_secret=session_secret)
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
        # An empty HMAC signing key silently accepts forged tokens — require it.
        jwt_secret = auth_config.get("jwt_secret")
        if not jwt_secret:
            raise ValueError("auth.jwt_secret is required for provider 'simple'")
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
    if provider_name in ("oauth", "oidc"):  # REQ-890: "oidc" is the generic OIDC selector
        from provisa.auth.providers.oauth import OAuthProvider

        oa = auth_config.get("oidc" if provider_name == "oidc" else "oauth", {})
        return OAuthProvider(
            discovery_url=oa["discovery_url"],
            client_id=oa["client_id"],
            audience=oa.get("audience"),
            role_claim=oa.get("role_claim", "roles"),
        )
    raise ValueError(f"Unknown auth provider: {provider_name!r}")


def _resolve_default_org_id(cfg) -> str:  # REQ-1286
    """The org an authenticated user is bound to when no other org applies.

    ONE source of truth: the control plane's resolved ``org_id`` (``state.org_id``, set by
    ``_init_control_planes``), which is also what names the tenant schema ``org_<id>`` and the
    registry's default org row. ``config.default_org_id`` overrides it only when explicitly set.
    Before the control planes come up ``state.org_id`` is absent — the middleware resolves its
    settings lazily on the first request, by which point the lifespan has run, so an absent value
    there is a real misconfiguration and must raise rather than resolve to a guessed literal."""
    from provisa.api.app import state

    explicit = getattr(cfg, "default_org_id", None) if cfg else None
    if explicit:
        return explicit
    org_id = getattr(state, "org_id", None)
    if not org_id:
        raise RuntimeError(
            "default org id unresolved: control-plane org_id is unset and no explicit "
            "config.default_org_id was provided"
        )
    return org_id


def _resolve_auth_settings() -> dict:  # REQ-120, REQ-125
    """Resolve AuthMiddleware settings from live app ``state``.

    Called lazily on the middleware's first request. The middleware is installed at create_app time,
    but its inputs — ``auth_config`` and the control-plane pools — are only populated later, in the
    lifespan (``_load_and_build``). Starlette builds the middleware stack on the lifespan ASGI call,
    so the middleware must be *added* at create_app; its configuration is resolved here, on the first
    request, by which point the lifespan has run.

    ``auth_config is None`` (no ``auth`` section, or ``provider: none``) → unsecured: no provider, so
    the middleware honors ``X-Provisa-Role`` at face value (REQ-273)."""
    from provisa.api.app import state
    from provisa.api.org_runtime import ActiveOrgPool

    auth_config = getattr(state, "auth_config", None)
    cfg = getattr(state, "config", None)
    multitenancy = getattr(cfg, "multitenancy", False) if cfg else False
    default_org_id = _resolve_default_org_id(cfg)
    base = {
        "mapping_rules": [],
        "default_role": "analyst",
        "db_pool": None,
        "admin_pool": None,
        "assignments_source": "claims",
        "default_assignments": [],
        "multitenancy": multitenancy,
        "default_org_id": default_org_id,
        "superuser": None,
        "bootstrap_superadmin": False,
    }
    if auth_config is None:
        return {**base, "provider": None}

    admin_pool = getattr(state, "admin_db", None)
    provider = build_auth_provider(auth_config, admin_pool=admin_pool)
    if auth_config["provider"] == "simple":
        from provisa.auth.providers import simple as simple_mod

        simple_mod._provider_instance = provider
    return {
        **base,
        "provider": provider,
        "mapping_rules": auth_config.get("role_mapping", []),
        "default_role": auth_config.get("default_role", "analyst"),
        "db_pool": ActiveOrgPool(),
        "admin_pool": admin_pool,
        "assignments_source": auth_config.get("assignments_source", "claims"),
        "default_assignments": auth_config.get("default_assignments", []),
        "superuser": resolve_superuser_config(auth_config.get("superuser")),
        "bootstrap_superadmin": auth_config.get("bootstrap_superadmin", False),
    }


def wire_auth(
    app: FastAPI, auth_config: dict | None, db_pool=None, admin_pool=None
) -> None:  # REQ-120, REQ-125
    """Register AuthMiddleware (and auth routes) on the application.

    ``db_pool`` is the tenant control plane (``user_role_assignments``);
    ``admin_pool`` is the platform control plane (``local_users``,
    ``user_profiles``, ``user_org_memberships``).

    Called from create_app before the config + control-plane pools exist, so the middleware is
    always installed and resolves its settings lazily from ``state`` (see ``_resolve_auth_settings``).
    An explicit ``auth_config`` (tests / eager callers) takes the original eager path unchanged."""
    from provisa.api.app import state as _app_state

    if auth_config is None:
        # Unsecured (no auth section, or provider: none): the middleware runs in passthrough
        # mode (honors X-Provisa-Role at face value). No REAL provider is active, so the flag is
        # False — bolt/pgwire read it to allow their no-auth path (a real provider whose config
        # is absent, by contrast, stays fail-closed there).
        _app_state.auth_middleware_active = False
        from provisa.auth.middleware import AuthMiddleware

        app.add_middleware(AuthMiddleware, config_resolver=_resolve_auth_settings)
        return

    _app_state.auth_middleware_active = True

    provider = build_auth_provider(auth_config, admin_pool=admin_pool)
    mapping_rules = auth_config.get("role_mapping", [])
    default_role = auth_config.get("default_role", "analyst")

    from provisa.api.app import state as _app_state

    cfg = getattr(_app_state, "config", None)
    multitenancy = getattr(cfg, "multitenancy", False) if cfg else False
    default_org_id = _resolve_default_org_id(cfg)

    _app_state.auth_middleware_active = True

    from provisa.auth.middleware import AuthMiddleware

    app.add_middleware(
        AuthMiddleware,
        provider=provider,
        mapping_rules=mapping_rules,
        default_role=default_role,
        db_pool=db_pool,
        admin_pool=admin_pool,
        assignments_source=auth_config.get("assignments_source", "claims"),
        default_assignments=auth_config.get("default_assignments", []),
        multitenancy=multitenancy,
        default_org_id=default_org_id,
        superuser=resolve_superuser_config(auth_config.get("superuser")),
        bootstrap_superadmin=auth_config.get("bootstrap_superadmin", False),
    )

    # Mount simple auth login route when provider=simple
    if auth_config["provider"] == "simple":
        from provisa.auth.providers import simple as simple_mod

        simple_mod._provider_instance = provider
        app.include_router(simple_mod.router)

    # REQ-124: the browser signs in against the basic provider through the same /auth/login
    # exchange; without this route the SPA's login POST 404s and no one can reach the UI.
    if auth_config["provider"] == "basic":
        from provisa.auth.providers import basic as basic_mod

        basic_mod._provider_instance = provider
        app.include_router(basic_mod.router)
