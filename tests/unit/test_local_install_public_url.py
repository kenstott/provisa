# Copyright (c) 2026 Kenneth Stott
# Canary: 8d4f2c17-3a9e-4b61-9e05-7c2a1f6d8b43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A local install sets server.public_url (REQ-2135, A LOCAL INSTALL SETS THE PUBLIC URL).

A local start names the address its server listens on in PROVISA_LOCAL_ADDRESS. When
server.public_url is unset, the install stores that address as the operator setting, so it shows
in Deployment settings and can be changed there. A start that names no local address (a hosted or
cluster install) stores nothing, and SPARQL still refuses by name."""

# Requirements: REQ-2135

from __future__ import annotations

from pathlib import Path

import pytest

from provisa.core import deployment_settings, local_install, settings_registry
from provisa.gql.iri import IriBaseUnset, iri_base

_REPO = Path(__file__).resolve().parents[2]
_LOCAL = "http://localhost:8001"


@pytest.fixture
def plane(control_plane, monkeypatch):
    """The test's platform plane, bound as the deployment's, with no public address anywhere."""
    monkeypatch.setattr(deployment_settings, "_db", None)
    monkeypatch.setattr(deployment_settings, "_held", None)
    monkeypatch.delenv("PROVISA_PUBLIC_URL", raising=False)
    monkeypatch.setattr(settings_registry, "_config", {})
    deployment_settings.bind(control_plane.platform)
    return control_plane.platform


def test_a_local_start_stores_the_address_it_listens_on_as_the_public_url(plane, monkeypatch):
    monkeypatch.setenv(local_install.LOCAL_ADDRESS_ENV, _LOCAL)
    local_install.set_public_url(plane)
    assert settings_registry.resolve("server.public_url") == (_LOCAL, "stored")
    assert iri_base() == _LOCAL


def test_a_public_url_already_set_is_left_as_it_is(plane, monkeypatch):
    monkeypatch.setenv(local_install.LOCAL_ADDRESS_ENV, _LOCAL)
    monkeypatch.setenv("PROVISA_PUBLIC_URL", "https://data.example.com")
    local_install.set_public_url(plane)
    assert deployment_settings.get("server.public_url") is None
    assert iri_base() == "https://data.example.com"


def test_an_address_the_operator_stored_is_never_replaced(plane, monkeypatch):
    settings_registry.store(
        plane, {"server.public_url": "https://kept.example.com"}, updated_by="operator"
    )
    monkeypatch.setenv(local_install.LOCAL_ADDRESS_ENV, _LOCAL)
    local_install.set_public_url(plane)
    assert deployment_settings.get("server.public_url") == "https://kept.example.com"


def test_a_start_naming_no_local_address_stores_nothing_and_sparql_still_refuses(
    plane, monkeypatch
):
    monkeypatch.delenv(local_install.LOCAL_ADDRESS_ENV, raising=False)
    local_install.set_public_url(plane)
    assert deployment_settings.get("server.public_url") is None
    with pytest.raises(IriBaseUnset):
        iri_base()


def test_the_refusal_names_the_field_and_the_address_to_put_in_it(monkeypatch):
    monkeypatch.delenv("PROVISA_PUBLIC_URL", raising=False)
    monkeypatch.setenv(local_install.LOCAL_ADDRESS_ENV, _LOCAL)
    with pytest.raises(IriBaseUnset) as refused:
        iri_base()
    told = str(refused.value)
    assert f"Set it to {_LOCAL}" in told
    assert "Deployment settings > Network and TLS > General and HTTP > Public address" in told
    assert "(server.public_url)" in told


def test_a_hosted_refusal_names_the_field_and_the_address_it_takes(monkeypatch):
    monkeypatch.delenv("PROVISA_PUBLIC_URL", raising=False)
    monkeypatch.delenv(local_install.LOCAL_ADDRESS_ENV, raising=False)
    with pytest.raises(IriBaseUnset) as refused:
        iri_base()
    told = str(refused.value)
    assert "Set it to the address clients reach this deployment at" in told
    assert "Public address (server.public_url)" in told


def test_the_place_the_refusal_names_is_the_one_the_settings_page_shows():
    import json

    en = _REPO / "provisa-ui" / "src" / "i18n" / "locales" / "en"
    page = json.loads((en / "adminPage.json").read_text())["adminPage"]["setting"]
    field = json.loads((en / "settings.json").read_text())["settings"]["server"]["public_url"]
    shown = " > ".join((page["card"]["network"], page["group"]["general"], field["label"]))
    assert shown == "Network and TLS > General and HTTP > Public address"
    assert settings_registry.setting("server.public_url").card == "network"


async def test_the_dev_mode_caller_is_served_the_public_address_field(monkeypatch):
    """A local start with no auth provider: its one user is the anonymous caller the catalog
    admits, and the field is in the network card, editable."""
    import types

    from provisa.api.admin import settings_catalog_router as catalog

    monkeypatch.setattr(catalog, "_audit", lambda: {})
    request = types.SimpleNamespace(
        state=types.SimpleNamespace(
            identity=types.SimpleNamespace(user_id="anonymous", roles=["org_admin"])
        )
    )
    payload = await catalog.get_catalog(request)
    network = next(c for c in payload["cards"] if c["id"] == "network")
    field = next(s for s in network["settings"] if s["key"] == "server.public_url")
    assert field["editable"] is True


@pytest.mark.parametrize(
    "launcher",
    ["start-ui-install.sh", "scripts/provisa", "packaging/windows/provisa-native.ps1"],
)
def test_every_local_launcher_names_the_address_its_server_listens_on(launcher):
    assert local_install.LOCAL_ADDRESS_ENV in (_REPO / launcher).read_text()


def test_provisa_run_names_the_address_its_api_listens_on(monkeypatch):
    from provisa import cli

    monkeypatch.delenv(local_install.LOCAL_ADDRESS_ENV, raising=False)
    cli._name_local_address("127.0.0.1", 8000)
    import os

    assert os.environ[local_install.LOCAL_ADDRESS_ENV] == "http://127.0.0.1:8000"
