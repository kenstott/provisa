# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8b1e93-2f07-4d6a-b5e8-7a3d0c9f2b61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Go source's install (REQ-2200): its code fetched from the organisation's Go module proxy (or
the public one), from Git, an upload or a directory; every module it needs fetched through the
proxy and checked against its go.sum; its host compiled with it; all cached by content hash. The
installs, which run the pinned Go against the public proxy, are tests/integration/test_go_install_live.py."""

# Requirements: REQ-2200
from __future__ import annotations

import base64
import io
import zipfile

import pytest

from provisa.codesource.bundle import BundleRefused
from provisa.core.package_registries import Resolved
from provisa.golang.install import (
    PUBLIC_PROXY,
    GoModuleCode,
    check_lockfile,
    escaped,
    fetch_module,
    netrc,
    proxy_address,
)

pytestmark = pytest.mark.unit

UUID_SUM = (
    "github.com/google/uuid v1.6.0 h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0=\n"
    "github.com/google/uuid v1.6.0/go.mod h1:TIyPZe4MgqvfeYDBFedMoGGpEw/LqOeaOT+nhxU+yHo=\n"
)


def _proxy(**changed) -> Resolved:
    given = {
        "id": "p1",
        "kind": "goproxy",
        "address": "https://goproxy.acme.test/",
        "scope": None,
        "credential": "ada:made-up-proxy-token",
        "mirror": True,
    }
    given.update(changed)
    return Resolved(**given)


def _module_zip(module: str, version: str, files: dict[str, str]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as archive:
        for name, text in files.items():
            archive.writestr(f"{module}@{version}/{name}", text)
    return out.getvalue()


# -- the code from a module proxy ------------------------------------------------------------------


def test_a_module_is_fetched_from_the_default_proxy_with_its_login(tmp_path):
    asked = []

    def http(url, headers, ca_bundle=None):
        asked.append((url, headers, ca_bundle))
        return _module_zip(
            "github.com/Acme/Rates", "v1.2.0", {"go.mod": "module github.com/Acme/Rates\n"}
        )

    proxy = _proxy(ca_bundle="-----BEGIN CERTIFICATE-----\nmade-up\n")
    root = fetch_module(GoModuleCode("github.com/Acme/Rates", "v1.2.0"), [proxy], http, tmp_path)
    assert root == (tmp_path / "github.com" / "Acme" / "Rates@v1.2.0").resolve()
    [(url, headers, ca_bundle)] = asked
    # A module path's upper case is escaped as the module proxy protocol has it.
    assert url == "https://goproxy.acme.test/github.com/!acme/!rates/@v/v1.2.0.zip"
    assert headers == {
        "Authorization": "Basic " + base64.b64encode(b"ada:made-up-proxy-token").decode()
    }
    assert ca_bundle == proxy.ca_bundle


def test_with_no_default_proxy_a_module_comes_from_the_public_one(tmp_path):
    asked = []

    def http(url, headers):
        asked.append((url, headers))
        return _module_zip("example.com/rows", "v0.1.0", {"go.mod": "module example.com/rows\n"})

    not_default = _proxy(mirror=False)
    fetch_module(GoModuleCode("example.com/rows", "v0.1.0"), [not_default], http, tmp_path)
    assert asked == [(f"{PUBLIC_PROXY}/example.com/rows/@v/v0.1.0.zip", {})]
    assert proxy_address([not_default]) == PUBLIC_PROXY
    assert proxy_address([_proxy()]) == "https://goproxy.acme.test"


@pytest.mark.parametrize("version", ["latest", "1.2.0", "v1.2", "master"])
def test_a_module_is_named_at_an_exact_version(tmp_path, version):
    with pytest.raises(BundleRefused) as refused:
        fetch_module(GoModuleCode("example.com/rows", version), [], lambda *a: b"", tmp_path)
    assert refused.value.code == "codesource.go_version"


def test_a_module_without_a_go_mod_is_refused_by_name(tmp_path):
    def http(url, headers):
        return _module_zip("example.com/rows", "v0.1.0", {"rows.go": "package rows\n"})

    with pytest.raises(BundleRefused, match="example.com/rows@v0.1.0 .* holds no go.mod"):
        fetch_module(GoModuleCode("example.com/rows", "v0.1.0"), [], http, tmp_path)


def test_the_proxys_login_reaches_the_go_command_in_a_netrc():
    assert netrc([_proxy(), _proxy(kind="pypi", credential="token")]) == (
        "machine goproxy.acme.test login ada password made-up-proxy-token\n"
    )
    with pytest.raises(BundleRefused) as refused:
        netrc([_proxy(credential="a-bare-token")])
    assert refused.value.code == "codesource.goproxy_credential"


def test_a_module_path_is_escaped_as_the_proxy_protocol_has_it():
    assert escaped("github.com/BurntSushi/toml") == "github.com/!burnt!sushi/toml"


# -- the lockfile ----------------------------------------------------------------------------------


def test_code_that_requires_modules_carries_its_go_sum(tmp_path):
    (tmp_path / "go.mod").write_text(
        "module example.com/rows\n\ngo 1.23\n\nrequire github.com/google/uuid v1.6.0\n"
    )
    with pytest.raises(BundleRefused) as refused:
        check_lockfile(tmp_path)
    assert refused.value.code == "codesource.lockfile_missing"
    (tmp_path / "go.sum").write_text(UUID_SUM)
    check_lockfile(tmp_path)
    (tmp_path / "go.sum").unlink()
    (tmp_path / "go.mod").write_text("module example.com/rows\n\ngo 1.23\n")
    check_lockfile(tmp_path)  # no requirement, no go.sum
