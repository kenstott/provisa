# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e9a41-5d3b-4f86-a0c1-9e8b2d6f4a17
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1997, REQ-1557: the own-disk rule for a Logs source's files (REQ-1996), a code source's
directory (REQ-2121) and a file source's download_from mount path (REQ-2113) is decided by
``sandbox_egress.held``, as a source's locations are: a sandbox org on an install of one
organization is refused a place on the server's own disk outside its folder, even inside the
operator's allowed directories; inside its folder, or a place its demo declares, it reads; the
deployment's own org on an install of one reads what the operator's directories allow."""

# Requirements: REQ-1997, REQ-1557, REQ-1996, REQ-2121, REQ-2113

from __future__ import annotations

from types import SimpleNamespace

import pytest

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.codesource import source as code_source
from provisa.codesource.bundle import BundleRefused
from provisa.core import org_files, secrets_providers, source_downloads
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.logs.roots import LogPathRefused, checked_file, checked_glob
from provisa.security import sandbox_egress as egress

pytestmark = pytest.mark.unit

_SANDBOX = "sbx0123456789ab"
_DEPLOYMENT = "default"


@pytest.fixture
def disk(tmp_path, monkeypatch):
    """An install of one organization whose operator allows the whole of ``tmp_path`` for log
    files, code directories and downloads: a place of the server's own (``server``) with a log,
    a code directory and a file, and the same in each org's folder."""
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("PROVISA_LOGS_ALLOWED_ROOTS", str(tmp_path))
    monkeypatch.setenv("PROVISA_SOURCES_DOWNLOAD_ROOTS", str(tmp_path))
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)
    monkeypatch.setattr(egress, "_deployment_org", lambda: _DEPLOYMENT)
    places = {}
    for name, base in (
        ("server", tmp_path / "server"),
        (_SANDBOX, org_files.folder(_SANDBOX) / "files"),
        (_DEPLOYMENT, org_files.folder(_DEPLOYMENT) / "files"),
    ):
        (base / "logs").mkdir(parents=True)
        (base / "logs" / "app.log").write_text("x\n")
        (base / "code").mkdir()
        (base / "code" / "rates.py").write_text("def rate(): return 1\n")
        (base / "supplies.csv").write_text("a\n1\n")
        places[name] = base
    return SimpleNamespace(server=places["server"], places=places)


@pytest.fixture
def bound(monkeypatch):
    tokens = []

    def bind(org: str):
        monkeypatch.setattr("provisa.core.org_files.current_org_segment", lambda: org)
        tokens.append(set_current_org(org))

    yield bind
    for token in reversed(tokens):
        reset_current_org(token)
    egress.forget(_SANDBOX)


def _code(path) -> dict:
    return {"runtime": "python", "code": {"kind": "directory", "path": str(path)}}


def _refused_everywhere(base, org: str) -> None:
    with pytest.raises(LogPathRefused, match="outside its own folder"):
        checked_glob(f"{base}/logs/*.log")
    with pytest.raises(LogPathRefused, match="outside its own folder"):
        checked_file(str(base / "logs" / "app.log"))
    with pytest.raises(BundleRefused) as code:
        code_source.config_of("python", _code(base / "code"))
    assert code.value.code == "codesource.directory_refused"
    assert code.value.params["path"] == str(base / "code")
    with pytest.raises(source_downloads.DownloadRefused) as download:
        source_downloads.checked_mount(str(base / "supplies.csv"), org)
    assert download.value.code == "files.download_mount_refused"
    assert download.value.params["address"] == str(base / "supplies.csv")


def _read_everywhere(base, org: str) -> None:
    assert checked_glob(f"{base}/logs/*.log") == f"{base}/logs/*.log"
    assert checked_file(str(base / "logs" / "app.log"))
    config = code_source.config_of("python", _code(base / "code"))
    assert config.code["path"] == str((base / "code").resolve())
    assert source_downloads.checked_mount(str(base / "supplies.csv"), org).is_file()


def test_a_sandbox_org_on_an_install_of_one_is_refused_a_place_of_the_servers_own(disk, bound):
    bound(_SANDBOX)
    _refused_everywhere(disk.server, _SANDBOX)


def test_a_sandbox_org_is_refused_another_orgs_folder_and_a_link_out_of_its_own(disk, bound):
    bound(_SANDBOX)
    _refused_everywhere(disk.places[_DEPLOYMENT], _SANDBOX)
    link = disk.places[_SANDBOX] / "out"
    link.symlink_to(disk.server, target_is_directory=True)
    _refused_everywhere(link, _SANDBOX)


def test_a_sandbox_org_reads_inside_its_own_folder(disk, bound):
    bound(_SANDBOX)
    _read_everywhere(disk.places[_SANDBOX], _SANDBOX)


def test_a_sandbox_org_reads_a_code_directory_and_a_download_its_demo_declares(disk, bound):
    """The demo's own sources name the server's places they read (set_demo_destinations), as
    they name the hosts they reach; another place of the server's stays refused."""
    bound(_SANDBOX)
    declared = disk.server
    egress.set_demo_destinations(
        _SANDBOX,
        [
            {"id": "rates", "type": "python", "mapping": _code(declared / "code")},
            {"id": "supplies", "type": "csv", "download_from": str(declared / "supplies.csv")},
        ],
    )
    config = code_source.config_of("python", _code(declared / "code"))
    assert config.code["path"] == str((declared / "code").resolve())
    assert source_downloads.checked_mount(str(declared / "supplies.csv"), _SANDBOX).is_file()
    (declared / "other.csv").write_text("a\n")
    with pytest.raises(source_downloads.DownloadRefused) as download:
        source_downloads.checked_mount(str(declared / "other.csv"), _SANDBOX)
    assert download.value.code == "files.download_mount_refused"


def test_a_sandbox_org_reads_its_own_demos_logs(disk, bound, monkeypatch):
    """REQ-2049: a demo's logs are read live from its directory by its sandboxes."""
    bound(_SANDBOX)
    monkeypatch.setattr(
        "provisa.core.demo_bundles.own_demo_directory", lambda _org: disk.server / "logs"
    )
    assert checked_glob(f"{disk.server}/logs/*.log") == f"{disk.server}/logs/*.log"
    with pytest.raises(LogPathRefused, match="outside its own folder"):
        checked_glob(f"{disk.server}/*.log")


def test_the_deployments_own_org_on_an_install_of_one_reads_what_the_operator_allows(disk, bound):
    bound(_DEPLOYMENT)
    _read_everywhere(disk.server, _DEPLOYMENT)


def test_an_ordinary_org_on_a_server_of_many_is_held_and_the_deployments_own_is_not(
    disk, bound, monkeypatch
):
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)
    org_files.folder("acme").mkdir(parents=True)
    bound("acme")
    _refused_everywhere(disk.server, "acme")
    bound(_DEPLOYMENT)
    _read_everywhere(disk.server, _DEPLOYMENT)
