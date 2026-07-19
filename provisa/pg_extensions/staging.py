# Copyright (c) 2026 Kenneth Stott
# Canary: 2f78d56e-df45-406a-9164-58adb3c67ca1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Offline staging of the prebuilt PG extension/FDW bundle into a pgserver (REQ-1152).

The ``provisa-pg-ext`` PyPI wheel carries the pinned per-platform bundle
(``<os>-<arch>/{manifest.json, lib/<name>.<suf>, share/extension/*}`` — the exact tree
scripts/ci/build_pg_extensions.sh emits and scripts/ci/smoke_pg_extensions.py installs). This module
copies the running platform's bundle out of that wheel into a pgserver's ``pginstall`` so the FDW
modules load without the github.com/kenstott/provisa release round trip — the firewall-safe analog of
``provisa.federation.duckdb_extensions.stage_bundled_extensions`` for the DuckDB side.

Consumption call site is deliberately absent: no live pgserver federation engine stages FDWs yet. When
one exists it calls ``stage_bundled_pg_extensions(pginstall)`` at bootstrap and then registers what
landed via ``provisa.federation.fdw_artifact_catalog.discover_bundled_artifacts(<returned pkglibdir>)``.
"""

from __future__ import annotations

import platform as _platform
import shutil
import sys
from pathlib import Path


class BundledPgExtensionsMissing(RuntimeError):
    """The provisa-pg-ext package is installed but lacks a bundle for this platform.

    A packaging defect (the wheel was built without this ``<os>-<arch>``), NOT a runtime condition to
    paper over: an air-gapped/enterprise install must get every FDW through PyPI, so fail loud with a
    precise remediation rather than silently reaching github.com/releases.
    """


def bundle_platform() -> str:
    """This host's ``<os>-<arch>`` bundle tag, matching scripts/ci/build_pg_extensions.sh's ``OUT`` dir
    (``darwin``/``linux`` + ``arm64``/``x64``) — the name the wheel's per-platform subdir uses."""
    os_tag = {"darwin": "darwin", "linux": "linux"}.get(str(sys.platform))
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "x64", "amd64": "x64"}.get(
        _platform.machine().lower()
    )
    if os_tag is None or arch is None:
        raise BundledPgExtensionsMissing(
            f"no PG-extension bundle target for platform {sys.platform!r} / {_platform.machine()!r} "
            f"(the wheel ships darwin|linux × arm64|x64)"
        )
    return f"{os_tag}-{arch}"


def _module_suffix(platform: str) -> str:
    """The compiled-module suffix for a bundle tag (``darwin-…`` → ``dylib``, ``linux-…`` → ``so``)."""
    return "dylib" if platform.startswith("darwin") else "so"


def stage_bundled_pg_extensions(pginstall: str | Path) -> Path:
    """Copy the running platform's PG extension bundle from the ``provisa-pg-ext`` wheel into a
    pgserver's ``pginstall`` (idempotent) and return the ``pkglibdir`` the modules landed in — the
    offline, firewall-safe alternative to fetching the bundle from github.com/releases.

    ``lib/<name>.<suf>`` copies into ``<pginstall>/lib/postgresql/`` and ``share/extension/*`` into
    ``<pginstall>/share/postgresql/extension/`` (the layout scripts/ci/smoke_pg_extensions.py proves
    loads). Pass the returned pkglibdir to
    ``provisa.federation.fdw_artifact_catalog.discover_bundled_artifacts`` to register what landed.

    Raises ``ModuleNotFoundError`` when the package isn't installed (the caller decides whether a
    network fetch is acceptable — a dev checkout, never an enterprise embedded install), and
    ``BundledPgExtensionsMissing`` when the package is present but ships no bundle for this platform.
    """
    from provisa_pg_ext import ext_root  # type: ignore[import-not-found]  # ModuleNotFoundError propagates by design

    platform = bundle_platform()
    src = ext_root() / platform
    lib_src = src / "lib"
    modules = sorted(lib_src.glob(f"*.{_module_suffix(platform)}")) if lib_src.is_dir() else []
    if not modules:
        raise BundledPgExtensionsMissing(
            f"provisa-pg-ext has no PG-extension bundle for {platform} (looked in {lib_src}); "
            f"rebuild the package with build-pg-extensions.yml covering {platform}."
        )

    base = Path(pginstall)
    pkglibdir = base / "lib" / "postgresql"
    extdir = base / "share" / "postgresql" / "extension"
    pkglibdir.mkdir(parents=True, exist_ok=True)
    extdir.mkdir(parents=True, exist_ok=True)

    for module in modules:
        out = pkglibdir / module.name
        if not out.exists():
            shutil.copy2(module, out)
    ext_share = src / "share" / "extension"
    if ext_share.is_dir():
        for control in sorted(ext_share.iterdir()):
            out = extdir / control.name
            if not out.exists():
                shutil.copy2(control, out)
    return pkglibdir
