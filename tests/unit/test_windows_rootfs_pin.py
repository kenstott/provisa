# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1d7a52-9c64-4e08-b1a7-5d2e8c40f6b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Structure tests: the Windows Container-tier WSL rootfs is pinned and cached."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[2]
_PIN = _ROOT / "packaging" / "windows" / "rootfs.pin"
_SCRIPT = _ROOT / "packaging" / "windows" / "build-container.ps1"
_WORKFLOW = _ROOT / ".github" / "workflows" / "release-windows.yml"


def _pin() -> dict[str, str]:
    pairs = re.findall(r"^(url|sha256)=(\S+)$", _PIN.read_text(), re.M)
    return dict(pairs)


def _container_steps() -> list[dict]:
    wf = yaml.safe_load(_WORKFLOW.read_text())
    return wf["jobs"]["build-windows-container"]["steps"]


def test_pin_file_exists_with_dated_url_and_sha256():
    assert _PIN.is_file()
    pin = _pin()
    assert re.fullmatch(
        r"https://cloud-images\.ubuntu\.com/wsl/jammy/\d{8}/\S+\.rootfs\.tar\.gz", pin["url"]
    )
    assert "/current/" not in pin["url"]
    assert re.fullmatch(r"[0-9a-f]{64}", pin["sha256"])


def test_script_reads_pin_and_has_no_moving_url():
    text = _SCRIPT.read_text()
    assert "rootfs.pin" in text
    assert "/current/" not in text
    assert "ROOTFS_URL" not in text


def test_workflow_reads_pin():
    steps = _container_steps()
    readers = [s for s in steps if "rootfs.pin" in s.get("run", "")]
    assert len(readers) == 1
    assert readers[0]["id"] == "rootfs"


def test_cache_key_built_from_pin_url_and_sha256():
    steps = _container_steps()
    caches = [s for s in steps if str(s.get("uses", "")).startswith("actions/cache@")]
    assert len(caches) == 1
    key = caches[0]["with"]["key"]
    assert "steps.rootfs.outputs.url" in key
    assert "steps.rootfs.outputs.sha256" in key
    assert caches[0]["with"]["path"] == "packaging/windows/rootfs-cache"
    assert "restore-keys" not in caches[0]["with"]
    # the cache step runs before the build step that consumes it
    names = [s.get("name") for s in steps]
    assert names.index(caches[0]["name"]) < names.index(
        "Build container-tier installer (WSL2, Inno Setup)"
    )


def test_script_verifies_checksum_on_every_use_without_fallback():
    text = _SCRIPT.read_text()
    assert "Get-FileHash -Algorithm SHA256" in text
    verify = text.index("Get-FileHash -Algorithm SHA256")
    # verification sits after both the cached and downloaded paths, before the copy
    assert text.index("Invoke-WebRequest -Uri $RootfsUrl") < verify
    assert verify < text.index("Copy-Item $RootfsCached")
    assert re.search(r"throw \"sha256 mismatch for \$RootfsCached", text)
