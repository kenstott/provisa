# Copyright (c) 2026 Kenneth Stott
# Canary: e4b70c95-1a3d-4f28-9c6e-b85d02f7a149
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A multi-tenant deployment does not read role assignments from sign-in claims.

It grants roles itself, per org — an invitation, auto-join, the org_admin of a new org — and
``auth.assignments_source: claims`` would ignore every one of them. The combination is refused
wherever a configuration is taken in: the model file at load (so the deployment does not boot on
it), and a settings save, before it is written. Single-tenant with claims stays legal.
"""

from __future__ import annotations

import pytest
import yaml

from provisa.core.assignments_source import ClaimsWithMultitenancy, require_assignments_source
from provisa.core.config_loader import parse_config_dict

_BOTH = "multitenancy: true cannot be combined with auth.assignments_source: claims"
_FIX = "Set auth.assignments_source: provisa"


def _config(**over) -> dict:
    return {"sources": [], "domains": [], "tables": [], "roles": [], **over}


def _oidc(**over) -> dict:
    return {"provider": "oidc", **over}


# --- the rule -------------------------------------------------------------------------------------


def test_multi_tenant_with_claims_is_refused_naming_both_settings_and_the_value_to_use():
    with pytest.raises(ClaimsWithMultitenancy) as refused:
        require_assignments_source(True, _oidc(assignments_source="claims"))
    assert _BOTH in str(refused.value) and _FIX in str(refused.value)


def test_leaving_the_setting_at_its_default_is_claims_and_is_refused_the_same():
    with pytest.raises(ClaimsWithMultitenancy):
        require_assignments_source(True, _oidc())


@pytest.mark.parametrize(
    "multitenancy, auth",
    [
        (True, _oidc(assignments_source="provisa")),  # what setup writes
        (False, _oidc(assignments_source="claims")),  # single-tenant may read claims
        (False, _oidc()),
        (True, None),  # no auth section: nobody signs in, no assignment is read
        (True, {"provider": "none"}),
        (True, {}),
    ],
)
def test_every_other_combination_is_legal(multitenancy, auth):
    require_assignments_source(multitenancy, auth)


# --- the model file, and so the boot --------------------------------------------------------------


def test_a_model_file_with_the_combination_does_not_load():
    """parse_config_dict is what startup loads the configuration with (api/app.py): a
    configuration it refuses is one the deployment does not start on."""
    with pytest.raises(ValueError, match=_BOTH):
        parse_config_dict(_config(multitenancy=True, auth=_oidc(assignments_source="claims")))
    with pytest.raises(ValueError, match="Set auth.assignments_source: provisa"):
        parse_config_dict(_config(multitenancy=True, auth=_oidc()))


def test_the_legal_model_files_load():
    assert parse_config_dict(
        _config(multitenancy=True, auth=_oidc(assignments_source="provisa"))
    ).multitenancy
    assert not parse_config_dict(_config(auth=_oidc(assignments_source="claims"))).multitenancy
    assert parse_config_dict(_config(multitenancy=True)).multitenancy  # no auth provider


# --- the settings paths ---------------------------------------------------------------------------


def test_the_config_writer_refuses_it_before_replacing_the_file(tmp_path):
    """Every settings path writes the configuration through this one function."""
    from provisa.api.admin._config_io import write_config

    path = tmp_path / "provisa.yaml"
    good = _config(multitenancy=True, auth=_oidc(assignments_source="provisa"))
    write_config(path, good)
    with pytest.raises(ClaimsWithMultitenancy):
        write_config(path, _config(multitenancy=True, auth=_oidc(assignments_source="claims")))
    assert yaml.safe_load(path.read_text()) == good, "the working configuration is still there"


async def test_the_auth_settings_save_answers_a_400_and_writes_nothing(monkeypatch, tmp_path):
    from provisa.api.admin import settings_router
    from provisa.api.errors import ApiError

    path = tmp_path / "provisa.yaml"
    stored = _config(multitenancy=True, auth=_oidc(assignments_source="provisa"))
    path.write_text(yaml.dump(stored))
    monkeypatch.setattr(settings_router, "config_path", lambda: path)
    monkeypatch.setattr(settings_router, "read_config", lambda: yaml.safe_load(path.read_text()))
    monkeypatch.setattr(settings_router, "require_deployment_settings", lambda request: None)

    class _Request:
        async def json(self):
            return {"provider": "keycloak", "common": {"assignments_source": "claims"}}

    with pytest.raises(ApiError) as refused:
        await settings_router.set_auth(_Request())  # type: ignore[arg-type]
    assert refused.value.status_code == 400
    assert refused.value.code == "settings.claims_with_multitenancy"
    assert yaml.safe_load(path.read_text()) == stored


async def test_the_same_save_on_a_single_tenant_deployment_is_accepted(monkeypatch, tmp_path):
    from provisa.api.admin import settings_router

    path = tmp_path / "provisa.yaml"
    path.write_text(yaml.dump(_config(auth=_oidc(assignments_source="provisa"))))
    monkeypatch.setattr(settings_router, "config_path", lambda: path)
    monkeypatch.setattr(settings_router, "read_config", lambda: yaml.safe_load(path.read_text()))
    monkeypatch.setattr(settings_router, "require_deployment_settings", lambda request: None)

    class _Request:
        async def json(self):
            return {"provider": "keycloak", "common": {"assignments_source": "claims"}}

    assert (await settings_router.set_auth(_Request()))["success"] is True  # type: ignore[arg-type]
    assert yaml.safe_load(path.read_text())["auth"]["assignments_source"] == "claims"


def test_first_run_setup_writes_the_legal_value():
    """Setup is the path that makes a deployment multi-tenant, and it sets the source to
    ``provisa`` in the same write (api/setup_router.py)."""
    from pathlib import Path

    import provisa.api.setup_router as setup

    source = Path(setup.__file__).read_text(encoding="utf-8")
    assert source.count('"assignments_source": "provisa"') == 2
    assert '"assignments_source": "claims"' not in source
