# Copyright (c) 2026 Kenneth Stott
# Canary: bd8471ba-cce3-4543-9149-7ba9c63914c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Loading the central redaction rules (REQ-2087): the deployment's, each org's, and the secrets
each org's model can reach. Run at start, when an org's runtime is built, when its model is
reloaded, when a secret of it is stored or removed, and when the rules are saved."""

# Requirements: REQ-2087

from __future__ import annotations

import json
import logging
from typing import Any

from provisa.security import redaction

_log = logging.getLogger(__name__)


def _rules_of(raw: Any, source: str) -> list[redaction.Rule]:
    if raw is None:
        return []
    items = json.loads(raw) if isinstance(raw, str) else raw
    return redaction.parse_rules(items, source)


async def load_deployment_rules() -> None:
    """The deployment's rules, their references resolved with the deployment org's vault bound."""
    from provisa.api.app import state
    from provisa.core import deployment_settings, secrets_store
    from provisa.core.secrets import resolve_secrets

    rules = _rules_of(deployment_settings.get(redaction.DEPLOYMENT_KEY), "deployment")
    assert state.admin_db is not None  # the platform plane is brought up before this runs
    async with secrets_store.bound(state.admin_db, state.org_id):
        redaction.set_deployment_rules(rules, resolve_secrets)


#: Orgs whose rules are being loaded: building the org's runtime for the load re-enters
#: :func:`ensure_loaded` through ensure_org_runtime.
_loading: set[str] = set()


async def ensure_loaded(org_id: str) -> None:
    """Load the org's rules once, when its runtime is first built (ensure_org_runtime)."""
    if redaction.is_loaded(org_id) or org_id in _loading:
        return
    _loading.add(org_id)
    try:
        await refresh_org(org_id)
    finally:
        _loading.discard(org_id)


async def refresh_org(org_id: str, *, every_environment: bool = False) -> None:
    """The org's own rules and the secrets its model can reach: the names Provisa's vault holds
    for it (when Provisa's vault is the backend) and every ``${secret:...}`` its stored model and
    settings name, on any backend -- each resolved with the org's vault bound. A name that does
    not resolve is logged at ERROR by name and left out; the org's other rules stand."""
    from provisa.api.app import ensure_org_runtime, state
    from provisa.core import secret_references, secrets_store
    from provisa.core.env_store import list_envs
    from provisa.core.org_settings import read_org_overrides
    from provisa.core.secrets import resolve_secrets
    from provisa.core.secrets_runtime import secrets_backend, secrets_backend_spec

    admin_db = state.admin_db
    assert admin_db is not None  # the platform plane is brought up before any org runtime
    runtime = await ensure_org_runtime(org_id)
    assert runtime.model_db is not None
    overrides = await read_org_overrides(runtime.model_db)
    rules = _rules_of((overrides.get(redaction.ORG_BLOCK) or {}).get("rules"), "org")
    # The prod model is scanned; with ``every_environment`` (a secret stored or removed, the rules
    # saved) every environment the org holds is. A secret an unscanned environment references is
    # matched from its first resolution (core.secrets), as a central backend's is.
    planes: dict[str, Any] = {"prod": runtime.model_db}
    if every_environment:
        for env in await list_envs(admin_db, org_id):
            env_runtime = await ensure_org_runtime(org_id, env["name"])
            if env_runtime.model_db is not None:
                planes[env["name"]] = env_runtime.model_db
    names = await secret_references.referenced_names(admin_db, org_id, environments=planes)
    spec = secrets_backend_spec()
    held: dict[str, str] = {}
    if spec is not None and spec.key == "provisa":
        held = await secrets_store.vault_values(admin_db, org_id)
    secrets: dict[str, str] = {name: value for name, value in held.items()}
    async with secrets_store.bound(admin_db, org_id):
        backend = secrets_backend()
        for name in sorted(names - set(secrets)):
            try:
                secrets[name] = backend.resolve(name)
            except (KeyError, ValueError) as exc:
                _log.error("redaction: secret %r of org %s does not resolve: %s", name, org_id, exc)
        redaction.set_org(org_id, rules, secrets, resolve_secrets)
