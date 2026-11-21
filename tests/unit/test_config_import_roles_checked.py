# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6e2f7d-93a4-4c1e-8d52-7f1a9c3e6b40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A role defined through config import passes what the role forms ask (REQ-1327, REQ-1337).

The import's ``roles`` section is opened by ``user_management``. That right is over an org's
people, so it must not let an importer define, or rewrite into, a role carrying rights they do
not hold: a platform right (``role_definition_problem``) or an org administrator's role
(``rights.check_role_grant``). Also pinned: the two ways such a role could reach a person.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest
import yaml

from provisa.api.admin import import_router as ir
from provisa.api.errors import ApiError
from provisa.core.db import standard_role_dicts

#: A role an org administrator made earlier, held by the importer: the one the triage rewrites.
HELD_ROLE = {
    "id": "reporting",
    "description": "Reports.",
    "capabilities": ["query_development"],
    "parents": [],
    "domain_access": ["default"],
    "residency_values": [],
}


class _Conn:
    pass


class _Pool:
    def acquire(self):
        @asynccontextmanager
        async def _cm():
            yield _Conn()

        return _cm()


def _request() -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(identity=SimpleNamespace(user_id="member@example.com", roles=[]))
    )


def _wire(monkeypatch, caps: set[str]) -> dict:
    """The importer holds ``caps``; the org holds the standard roles, HELD_ROLE and one domain.
    Records whether the apply ran."""
    import provisa.api.admin.capabilities as capmod
    import provisa.api.app as app_mod
    import provisa.api.app_loaders as loaders
    import provisa.core.repositories.domain as domain_repo
    import provisa.core.repositories.role as role_repo
    import provisa.security.sensitive as sensitive

    seen: dict = {"applied": False}
    monkeypatch.setattr(capmod, "_resolved_capabilities", lambda _identity, _state: caps)
    # Domains gate nothing here: reach has its own tests (REQ-1531).
    monkeypatch.setattr(capmod, "allowed_domains_request", lambda _request: None)
    monkeypatch.setattr(capmod, "_rights_scope", lambda _identity, _state, _rights: None)

    async def _roles(_conn):
        return [*standard_role_dicts(), dict(HELD_ROLE)]

    async def _domains(_conn, retired: bool = False):  # noqa: ARG001
        return [{"id": "default", "parent_domain_id": None}]

    monkeypatch.setattr(role_repo, "list_all", _roles)
    monkeypatch.setattr(domain_repo, "list_all", _domains)

    async def _no_changes(_conn, _config):
        return []

    monkeypatch.setattr(sensitive, "config_changes", _no_changes)

    async def _residency(_request, _conn, _config):
        return None

    monkeypatch.setattr(ir, "_require_residency_of_import", _residency)

    async def _apply(_config):
        seen["applied"] = True

    monkeypatch.setattr(loaders, "apply_configuration", _apply)
    monkeypatch.setattr(app_mod.state, "model_db", _Pool(), raising=False)
    return seen


def _config(*roles: dict) -> str:
    return yaml.dump(
        {"sources": [], "tables": [], "domains": [{"id": "default"}], "roles": list(roles)}
    )


def _role(role_id: str, capabilities: list[str], **extra) -> dict:
    return {
        "id": role_id,
        "description": "Imported.",
        "capabilities": capabilities,
        "domain_access": ["default"],
        **extra,
    }


async def _apply(yaml_text: str):
    return await ir.apply_import(ir.ImportApplyRequest(config_yaml=yaml_text), _request())


@pytest.mark.asyncio
async def test_a_user_management_holder_cannot_import_a_role_carrying_platform_rights(monkeypatch):
    seen = _wire(monkeypatch, {"user_management", "org_settings"})
    with pytest.raises(ApiError) as exc:
        await _apply(_config(_role("ops", ["platform_settings", "cross_org"])))
    assert exc.value.status_code == 403
    assert exc.value.code == "roles.platform_right_requires_platform_admin"
    assert exc.value.params == {"rights": ["cross_org", "platform_settings"]}
    assert seen["applied"] is False


@pytest.mark.asyncio
async def test_rewriting_a_held_role_into_platform_rights_is_refused(monkeypatch):
    """The triage's path: no assignment is needed when the importer already holds the role."""
    seen = _wire(monkeypatch, {"user_management", "org_settings", "query_development"})
    rewritten = _role("reporting", ["query_development", "platform_settings", "cross_org"])
    with pytest.raises(ApiError) as exc:
        await _apply(_config(rewritten))
    assert exc.value.code == "roles.platform_right_requires_platform_admin"
    assert seen["applied"] is False


@pytest.mark.asyncio
async def test_a_dry_run_asks_the_same(monkeypatch):
    seen = _wire(monkeypatch, {"user_management", "org_settings"})
    with pytest.raises(ApiError) as exc:
        await ir.apply_import(
            ir.ImportApplyRequest(
                config_yaml=_config(_role("ops", ["platform_settings"])), dry_run=True
            ),
            _request(),
        )
    assert exc.value.code == "roles.platform_right_requires_platform_admin"
    assert seen["applied"] is False


@pytest.mark.asyncio
async def test_an_org_administrators_role_needs_every_right_it_carries(monkeypatch):
    """check_role_grant: user_management alone does not hand out a role carrying user_management
    with rights its importer lacks."""
    seen = _wire(monkeypatch, {"user_management", "org_settings"})
    with pytest.raises(ApiError) as exc:
        await _apply(_config(_role("deputy", ["user_management", "table_registration"])))
    assert exc.value.status_code == 403 and exc.value.code == "roles.rights_not_held"
    assert exc.value.params == {"role": "deputy", "rights": "table_registration"}
    assert seen["applied"] is False


@pytest.mark.asyncio
async def test_a_role_within_the_importers_rights_is_applied(monkeypatch):
    seen = _wire(monkeypatch, {"user_management", "org_settings", "table_registration"})
    resp = await _apply(
        _config(
            _role("deputy", ["user_management", "table_registration"]),
            _role("reader", ["query_development"]),
        )
    )
    assert seen["applied"] is True
    assert resp.summary.role_ids == ["deputy", "reader"]


@pytest.mark.asyncio
async def test_a_platform_administrator_may_import_a_platform_role(monkeypatch):
    seen = _wire(monkeypatch, {"user_management", "org_settings", "platform_settings", "cross_org"})
    await _apply(_config(_role("ops", ["platform_settings", "cross_org"])))
    assert seen["applied"] is True


@pytest.mark.asyncio
async def test_an_unknown_capability_is_refused_by_name(monkeypatch):
    _wire(monkeypatch, {"user_management", "org_settings"})
    with pytest.raises(ApiError) as exc:
        await _apply(_config(_role("odd", ["no_such_right"])))
    assert exc.value.code == "roles.unknown_capability"


@pytest.mark.asyncio
async def test_a_standard_role_is_not_redefined_by_import(monkeypatch):
    _wire(monkeypatch, {"user_management", "org_settings", "query_development"})
    with pytest.raises(ApiError) as exc:
        await _apply(_config(_role("analyst", ["query_development"])))
    assert exc.value.code == "roles.standard_locked"


@pytest.mark.asyncio
async def test_a_child_of_a_held_role_narrows_it(monkeypatch):
    """REQ-2155 against the org's stored roles, not only the config's own."""
    _wire(monkeypatch, {"user_management", "org_settings", "table_registration"})
    with pytest.raises(ApiError) as exc:
        await _apply(_config(_role("child", ["table_registration"], parents=["reporting"])))
    assert exc.value.code == "roles.right_not_in_parents"


@pytest.mark.asyncio
async def test_a_parent_named_nowhere_is_refused(monkeypatch):
    _wire(monkeypatch, {"user_management", "org_settings"})
    with pytest.raises(ApiError) as exc:
        await _apply(_config(_role("child", [], parents=["missing_parent"])))
    assert exc.value.code == "roles.parent_invalid"


# --- the two ways a role could reach a person ---------------------------------------------------


def test_a_member_cannot_assign_a_platform_role_to_themselves(monkeypatch):
    """Question 1: had such a role been imported, the users surface's grant still asks
    check_role_grant, so a user_management holder cannot assign it (to anyone, themselves too).
    Rewriting a role the importer already holds needs no assignment, which the import refuses."""
    import provisa.api.admin.capabilities as capmod
    from provisa.api.admin._platform_guard import require_role_grantable

    monkeypatch.setattr(
        capmod, "_resolved_capabilities", lambda _i, _s: {"user_management", "org_settings"}
    )
    with pytest.raises(ApiError) as exc:
        require_role_grantable(_request(), "ops", ["platform_settings", "cross_org"])
    assert exc.value.code == "users.platform_role_requires_platform_admin"


@pytest.mark.asyncio
async def test_a_platform_role_takes_effect_only_outside_a_tenant_org():
    """Question 2: a tenant org on a many-organization server drops a held role carrying a
    platform right; a single-organization install (no tenant org) keeps it, which is why the
    import itself must refuse it."""
    from provisa.auth.held_assignments import settle
    from provisa.auth.role_mapping import RoleAssignment

    roles = {"ops": {"capabilities": ["platform_settings", "cross_org"]}}
    held = [RoleAssignment("ops", "*")]
    identity = SimpleNamespace(user_id="member@example.com")

    in_tenant = await settle(
        held,
        identity=identity,
        claimant=False,
        org_id="tenant-1",
        root_org_id="root",
        tenant_roles=roles,
        roles_now=lambda: roles,
    )
    single = await settle(
        held,
        identity=identity,
        claimant=False,
        org_id=None,
        root_org_id=None,
        roles_now=lambda: roles,
    )
    assert in_tenant == []
    assert single == held


def test_the_ui_has_both_refusals_in_every_locale():
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[2] / "provisa-ui/src/i18n/locales"
    locales = sorted(root.glob("*/serverErrors.json"))
    assert len(locales) == 13
    for path in locales:
        roles = json.loads(path.read_text(encoding="utf-8"))["serverErrors"]["roles"]
        assert "{{rights}}" in roles["platform_right_requires_platform_admin"], path
        held = roles["rights_not_held"]
        assert "{{role}}" in held and "{{rights}}" in held, path
