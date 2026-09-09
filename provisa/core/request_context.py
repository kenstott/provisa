# Copyright (c) 2026 Kenneth Stott
# Canary: 2c7f4a9e-8b3d-4e6a-9f1c-5d0b8a7e3c21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The request context every layer reads: which org and environment this task serves (REQ-1678).

These two ContextVars are set by the API layer when a request arrives (auth middleware binds the
org from the authenticated identity; the ``x-provisa-env`` header selects the environment) and by
the background entrypoints that act on behalf of an org. They live in ``core`` because the
compiler, the naming layer, secrets resolution and the federation engine all read them to answer
for the request they are inside — and none of those layers may import the API (the importlinter
contracts in pyproject.toml). The org RUNTIME registry that the values select stays in
:mod:`provisa.api.org_runtime`; only the request-scoped selection is here.

No-fallback rule: :func:`require_current_org` RAISES when no org is bound. ``active_env`` answers
``prod`` for an unbound environment because REQ-1487 settles that a request naming none is served
by prod — an answer the requirement gives, not a value invented to fill a hole.

The two providers at the bottom let the engine layer ask the running application for the active
org's own engine DSN (REQ-1418) and the persisted platform config without importing it: the API
registers them once at import, and a process with no app (desktop profile, tooling) has none
registered, which is the documented "no org has claimed an engine of its own" answer.
"""

# Requirements: REQ-1418, REQ-1487, REQ-1529, REQ-1678

from __future__ import annotations

from collections.abc import Callable
from contextvars import ContextVar, Token

from provisa.core.environments import PROD

# The org selected for the current request/task. Unset (None) at startup, on
# background-boot paths, and in single-org tests — the AppState shims then
# resolve the default-org runtime. A tenant-data entrypoint that sees None must
# raise (see require_current_org); it must never silently pick an org.
current_org: ContextVar[str | None] = ContextVar("current_org", default=None)

# REQ-1487/REQ-1529: the ENVIRONMENT selected for the current request/task, alongside the org. Unset
# (None) means prod — the environment a request naming none is served by (REQ-1487) — so every
# pre-environment path resolves exactly the runtime it resolved before there were environments.
#
# It is a SECOND ContextVar rather than an env baked into ``current_org`` because the two are set by
# different authorities at different moments: the org comes from the authenticated identity and is
# bound by the auth middleware, the environment comes from the ``x-provisa-env`` header and is a
# request-scoped selection within an org the caller already proved they belong to.
current_env: ContextVar[str | None] = ContextVar("current_env", default=None)


def set_current_org(org_id: str) -> Token[str | None]:
    """Bind the active org for the current context; returns a reset token."""
    return current_org.set(org_id)


def reset_current_org(token: Token[str | None]) -> None:
    current_org.reset(token)


def set_current_env(env: str | None) -> Token[str | None]:
    """Bind the active environment for the current context; returns a reset token.

    ``None`` binds prod explicitly, which is the same thing an unbound ContextVar resolves to.
    """
    return current_env.set(env)


def reset_current_env(token: Token[str | None]) -> None:
    current_env.reset(token)


def active_env() -> str:
    """The environment bound for this context, ``prod`` when none is (REQ-1487).

    Not a fallback: REQ-1487 settles that a request naming no environment is served by prod, so
    this is the answer the requirement gives rather than a value invented to fill a hole.
    """
    return current_env.get() or PROD


def require_current_org() -> str:
    """The active org id, or raise if none is bound.

    A tenant-data path that reaches this with no org selected is a routing bug
    (or an unauthenticated request that slipped past the org gate) — never a
    case to paper over with a default. Callers on the default-org fast path use
    the AppState shims instead, which fall back explicitly to the default org.
    """
    org_id = current_org.get()
    if org_id is None:
        raise RuntimeError(
            "No active org bound (current_org unset). A tenant-data path must "
            "set_current_org before use; this is a routing defect, not a "
            "condition to default around."
        )
    return org_id


# --- providers the API layer registers so lower layers never import it (REQ-1678) ---

_active_engine_url_provider: Callable[[], str | None] | None = None
_platform_config_provider: Callable[[], dict] | None = None


def register_active_engine_url_provider(fn: Callable[[], str | None]) -> None:
    global _active_engine_url_provider
    _active_engine_url_provider = fn


def register_platform_config_provider(fn: Callable[[], dict]) -> None:
    global _platform_config_provider
    _platform_config_provider = fn


def active_org_engine_url() -> str | None:
    """REQ-1418: the DSN of the engine the ACTIVE org operates itself, or ``None``.

    ``None`` when no application registered a provider: a process with no app (desktop profile,
    tooling) has no active org, and "no org has claimed an engine of its own" is the answer
    REQ-1418 gives for it, not a value gone missing.
    """
    if _active_engine_url_provider is None:
        return None
    return _active_engine_url_provider()


def platform_config() -> dict:
    """The persisted platform config, for engine selection. Empty when no application registered
    a provider (very early boot before a config file exists, or a process with no app)."""
    if _platform_config_provider is None:
        return {}
    return _platform_config_provider()
