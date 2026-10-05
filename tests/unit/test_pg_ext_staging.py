# Copyright (c) 2026 Kenneth Stott
# Canary: 303f9d2a-2fb5-4f04-93e5-f9b6f40cb359
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Offline staging of the bundled PG extensions (provisa-pg-ext PyPI package, REQ-1158).

stage_bundled_pg_extensions must copy the running platform's FDW modules + control/SQL files out of
the wheel into a pgserver's pginstall WITHOUT touching github.com/releases, and fail loud
(BundledPgExtensionsMissing) when the wheel carries no bundle for this platform. The package is mocked
so the test is hermetic (no dependency on the provisa-pg-ext wheel being installed).
"""

from __future__ import annotations

import hashlib
import json
import sys
import types

import pytest

from provisa.pg_extensions.staging import (
    BundledPgExtensionsMissing,
    bundle_platform,
    stage_bundled_pg_extensions,
)


def _install_fake_package(monkeypatch, ext_root):
    mod = types.ModuleType("provisa_pg_ext")
    mod.ext_root = lambda: ext_root  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "provisa_pg_ext", mod)


def _seed_bundle(pkg, platform, suffix, module=b"FAKE-SO"):
    plat = pkg / platform
    (plat / "lib").mkdir(parents=True, exist_ok=True)
    (plat / "share" / "extension").mkdir(parents=True, exist_ok=True)
    (plat / "lib" / f"sqlite_fdw.{suffix}").write_bytes(module)
    row = {"name": "sqlite_fdw", "key": "sqlite_fdw", "file": f"lib/sqlite_fdw.{suffix}"}
    row["sha256"] = hashlib.sha256(module).hexdigest()
    (plat / "manifest.json").write_text(json.dumps({"artifacts": [row]}))
    (plat / "share" / "extension" / "sqlite_fdw.control").write_text("comment = 'x'")


def test_stage_copies_current_platform_bundle(tmp_path, monkeypatch):
    platform = bundle_platform()
    suffix = "dylib" if platform.startswith("darwin") else "so"
    pkg = tmp_path / "pkg"
    _seed_bundle(pkg, platform, suffix)
    _install_fake_package(monkeypatch, pkg)

    pginstall = tmp_path / "pginstall"
    pkglibdir = stage_bundled_pg_extensions(pginstall)

    module = pkglibdir / f"sqlite_fdw.{suffix}"
    control = pginstall / "share" / "postgresql" / "extension" / "sqlite_fdw.control"
    assert pkglibdir == pginstall / "lib" / "postgresql"
    assert module.exists() and module.read_bytes() == b"FAKE-SO"
    assert control.exists()
    # Idempotent — a second call over the existing files does not raise and returns the same pkglibdir.
    assert stage_bundled_pg_extensions(pginstall) == pkglibdir


def test_missing_platform_bundle_fails_loud(tmp_path, monkeypatch):
    # Package present but no bundle for this platform → packaging defect, never a silent network fetch.
    _install_fake_package(monkeypatch, tmp_path / "empty_pkg")
    with pytest.raises(BundledPgExtensionsMissing):
        stage_bundled_pg_extensions(tmp_path / "pginstall")


def test_package_absent_raises_module_not_found(tmp_path, monkeypatch):
    # A dev checkout without the provisa-pg-ext wheel: caller decides whether a network fetch is ok.
    monkeypatch.setitem(sys.modules, "provisa_pg_ext", None)  # force ModuleNotFoundError on import
    with pytest.raises(ModuleNotFoundError):
        stage_bundled_pg_extensions(tmp_path / "pginstall")


def test_an_upgrade_replaces_a_staged_module_the_wheel_no_longer_ships(tmp_path, monkeypatch):
    """pgserver's install dir outlives a package upgrade: a module staged by an earlier release is
    replaced when its sha256 is not the manifest's, never kept because a file of that name exists."""
    platform = bundle_platform()
    suffix = "dylib" if platform.startswith("darwin") else "so"
    pkg = tmp_path / "pkg"
    _seed_bundle(pkg, platform, suffix, module=b"OLD-RELEASE")
    _install_fake_package(monkeypatch, pkg)
    pginstall = tmp_path / "pginstall"
    staged = stage_bundled_pg_extensions(pginstall) / f"sqlite_fdw.{suffix}"
    assert staged.read_bytes() == b"OLD-RELEASE"

    _seed_bundle(pkg, platform, suffix, module=b"NEW-RELEASE")  # the upgraded wheel
    stage_bundled_pg_extensions(pginstall)

    assert staged.read_bytes() == b"NEW-RELEASE"


def test_a_module_without_a_manifest_row_is_a_packaging_defect(tmp_path, monkeypatch):
    platform = bundle_platform()
    suffix = "dylib" if platform.startswith("darwin") else "so"
    pkg = tmp_path / "pkg"
    _seed_bundle(pkg, platform, suffix)
    (pkg / platform / "lib" / f"unlisted.{suffix}").write_bytes(b"X")
    _install_fake_package(monkeypatch, pkg)
    with pytest.raises(BundledPgExtensionsMissing, match="without a manifest row"):
        stage_bundled_pg_extensions(tmp_path / "pginstall")
