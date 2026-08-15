# Copyright (c) 2026 Kenneth Stott
# Canary: 9f3c581e-2740-4b6a-8d15-c0e79b42a635
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1182: a cloud deploy picks its identity provider without anyone opening the wizard.

Terraform sets ``auth_provider``, the node receives it as ``PROVISA_IDP``, and the server writes
the matching auth section on first boot. Two halves have to agree for that to work, and nothing
else in the suite compares them: the provider names Terraform accepts and the names the server
knows what to do with. A value that passes ``terraform apply`` and then means nothing to the
server produces a deployment that boots with no auth and no error.

The other property is that auto-configuration must never overwrite a deployment someone already
configured — a redeploy would silently reset how everyone signs in.
"""

# Requirements: REQ-1182, REQ-1266

from __future__ import annotations

import re

from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_VARIABLES_TF = _REPO_ROOT / "terraform/gcp/variables.tf"


def _terraform_providers() -> list[str]:
    """The provider names ``terraform apply`` will accept for auth_provider."""
    block = _VARIABLES_TF.read_text().split('variable "auth_provider"')[1].split("\nvariable ")[0]
    condition = re.search(r"contains\(\[([^\]]+)\]", block)
    assert condition is not None, "auth_provider lost its validation list"
    return [v.strip().strip('"') for v in condition.group(1).split(",")]


def test_terraform_constrains_the_provider_to_a_known_list():
    """Without the validation an unknown provider reaches the node and the server boots unauthed."""
    providers = _terraform_providers()

    assert providers == ["none", "firebase", "basic", "keycloak", "oauth", "oidc"]


@pytest.mark.parametrize("provider", [p for p in _terraform_providers() if p != "none"])
@pytest.mark.asyncio
async def test_every_provider_terraform_accepts_is_written_as_an_auth_section(
    provider, tmp_path, monkeypatch
):
    written = await _auto_configure(provider, tmp_path, monkeypatch)

    assert written["auth"]["provider"] == provider
    assert written["auth"]["assignments_source"] == "provisa"


@pytest.mark.asyncio
async def test_firebase_grants_nobody_by_default_and_bootstraps_the_first_user(
    tmp_path, monkeypatch
):
    """A blanket default assignment would make every Firebase account in the project an admin of
    this deployment. The first authenticated user claims the slot instead."""
    monkeypatch.setenv("FIREBASE_PROJECT_ID", "provisa-cloud")

    written = await _auto_configure("firebase", tmp_path, monkeypatch)

    assert written["auth"]["bootstrap_superadmin"] is True
    assert written["auth"]["default_assignments"] == []
    assert written["auth"]["firebase"]["project_id"] == "provisa-cloud"


@pytest.mark.asyncio
async def test_a_non_firebase_provider_seats_its_configured_principal_as_admin(
    tmp_path, monkeypatch
):
    written = await _auto_configure("oidc", tmp_path, monkeypatch)

    assert written["auth"]["default_assignments"] == [{"role_id": "admin", "domain_id": "*"}]


@pytest.mark.asyncio
async def test_multitenancy_is_off_unless_the_deploy_asks_for_it(tmp_path, monkeypatch):
    written = await _auto_configure("oidc", tmp_path, monkeypatch)
    assert "multitenancy" not in written

    monkeypatch.setenv("PROVISA_MULTITENANCY", "true")
    written = await _auto_configure("oidc", tmp_path, monkeypatch)
    assert written["multitenancy"] is True


@pytest.mark.asyncio
async def test_an_already_configured_deployment_is_left_alone(tmp_path, monkeypatch):
    """A redeploy re-runs this. Rewriting the auth section would reset how everyone signs in."""
    existing = {
        "auth": {
            "provider": "keycloak",
            "assignments_source": "provisa",
            "jwt_secret": "already-set",
        }
    }

    written = await _auto_configure("firebase", tmp_path, monkeypatch, existing=existing)

    assert written is None, "auto-configuration overwrote a configured deployment"


@pytest.mark.asyncio
async def test_a_configured_deployment_without_a_signing_key_gets_one(tmp_path, monkeypatch):
    """REQ-1472: additive reconcile — the break-glass browser session has nothing to sign with."""
    existing = {"auth": {"provider": "keycloak", "assignments_source": "provisa"}}

    written = await _auto_configure("firebase", tmp_path, monkeypatch, existing=existing)

    assert written["auth"]["provider"] == "keycloak", "reconcile changed how everyone signs in"
    assert written["auth"]["assignments_source"] == "provisa"
    assert written["auth"]["jwt_secret"]


async def _auto_configure(provider, tmp_path, monkeypatch, existing=None):
    """Run the auto-configuration against a recorded config file; returns what it wrote, or None."""
    import provisa.api.setup_router as mod

    cfg_path = tmp_path / "provisa.yaml"
    captured: dict = {}

    def _write_config(path, cfg):
        captured["cfg"] = cfg

    async def _load_and_build(path):
        captured["rebuilt"] = True

    monkeypatch.setattr("provisa.api.admin._config_io.config_path", lambda: cfg_path)
    monkeypatch.setattr(
        "provisa.api.admin._config_io.read_config_for_setup", lambda: dict(existing or {})
    )
    monkeypatch.setattr("provisa.api.admin._config_io.write_config", _write_config)
    monkeypatch.setattr("provisa.api.app._load_and_build", _load_and_build)

    await mod._auto_configure_idp(provider, None)
    return captured.get("cfg")
