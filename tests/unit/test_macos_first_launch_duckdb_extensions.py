# Copyright (c) 2026 Kenneth Stott
# Canary: 8e2f4c61-0b7a-4d93-a5e8-3c19d7f0b42a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""First launch of the macOS Core DMG stages the DuckDB extensions from their own release asset.

The Core DMG ships none (codesign cannot sign them, and Apple rejected the DMG that held them;
maintainer decision 2026-11-11, "Separate release asset"). build-dmg.sh pins the asset's sha256 in
Resources/duckdb-extensions.sha256. first-launch.sh restores the asset into the staged runtime: the
copy beside the installer (air-gapped), else this version's release download, either way only when
its checksum matches the pin. With neither, setup stops, naming the asset and where to put it.
"""

from __future__ import annotations

import hashlib
import io
import subprocess
import tarfile
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_FIRST_LAUNCH = (_REPO / "packaging/macos/first-launch.sh").read_text()
_ASSET = "provisa-duckdb-extensions-v9-macOS-arm64.tar.gz"
_BLOB = (
    "lib/python3.12/site-packages/duckdb_extension_httpfs/extensions/v2.0.0/httpfs.duckdb_extension"
)
_VOLUME = "/Volumes/Provisa Test"


def _asset_bytes(blob: bytes = b"\xcf\xfa\xed\xfe httpfs") -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        info = tarfile.TarInfo(f"./{_BLOB}")
        info.size = len(blob)
        tar.addfile(info, io.BytesIO(blob))
    return buf.getvalue()


def _functions() -> str:
    """first-launch.sh's DuckDB extension staging, without the rest of the script."""
    start = _FIRST_LAUNCH.index('DUCKDB_EXTENSIONS_PIN="')
    end = _FIRST_LAUNCH.index("# ── Native tier: stage the prebuilt runtime")
    return _FIRST_LAUNCH[start:end]


def _setup(tmp_path: Path, *, pinned: bytes, serves: bytes | None = None) -> dict[str, Path]:
    """A pinned .app, an empty staged runtime, a fake curl (serving ``serves``, else failing) and a
    fake hdiutil reporting the DMG at ``<tmp>/installer/Provisa-v9-macOS.dmg`` on _VOLUME."""
    paths = {
        "resources": tmp_path / "Provisa.app/Contents/Resources",
        "venv": tmp_path / "home/.provisa/venv",
        "installer": tmp_path / "installer",
        "bin": tmp_path / "bin",
        "home": tmp_path / "home",
    }
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(pinned).hexdigest()
    (paths["resources"] / "duckdb-extensions.sha256").write_text(f"{digest}  {_ASSET}\n")
    served = tmp_path / "served"
    if serves is not None:
        served.write_bytes(serves)
    (paths["bin"] / "curl").write_text(
        "#!/bin/bash\n"
        f'echo "${{*: -1}}" >> "{tmp_path}/curl.log"\n'
        f'[ -f "{served}" ] || exit 22\n'
        'while [ $# -gt 0 ]; do [ "$1" = "-o" ] && out="$2"; shift; done\n'
        f'cp "{served}" "$out"\n'
    )
    (paths["bin"] / "hdiutil").write_text(
        "#!/bin/bash\n"
        "printf '%s\\n' '================================================'\n"
        "printf 'framework       : 683\\n'\n"
        "printf '%s\\n' '================================================'\n"
        f"printf 'image-path      : {paths['installer']}/Provisa-v9-macOS.dmg\\n'\n"
        "printf '/dev/disk4\\tGUID_partition_scheme\\t\\n'\n"
        f"printf '/dev/disk4s1\\tApple_HFS\\t{_VOLUME}\\n'\n"
    )
    for tool in ("curl", "hdiutil"):
        (paths["bin"] / tool).chmod(0o755)
    return paths


def _stage(
    paths: dict[str, Path], *, version: str = "v9", bundle: str = ""
) -> subprocess.CompletedProcess:
    bundle_dir = bundle or f"{_VOLUME}/Provisa.app/Contents"
    program = (
        "set -euo pipefail\n"
        'info() { echo "INFO $*"; }; ok() { echo "OK $*"; }; err() { echo "$*" >&2; }\n'
        f'BUNDLE_DIR="{bundle_dir}"\n'
        f'RESOURCES="{paths["resources"]}"\n'
        f'PROVISA_HOME="{paths["venv"].parent}"\n'
        f'PROVISA_VERSION="{version}"\n'
        f"{_functions()}\n"
        f'stage_duckdb_extensions "{paths["venv"]}"\n'
    )
    return subprocess.run(
        ["bash", "-c", program],
        capture_output=True,
        text=True,
        env={"PATH": f"{paths['bin']}:/usr/bin:/bin", "HOME": str(paths["home"])},
    )


def _curl_calls(tmp_path: Path) -> list[str]:
    log = tmp_path / "curl.log"
    return log.read_text().split() if log.exists() else []


def test_air_gapped_it_stages_the_asset_beside_the_installer_without_the_network(tmp_path):
    asset = _asset_bytes()
    paths = _setup(tmp_path, pinned=asset)
    (paths["installer"] / _ASSET).write_bytes(asset)
    result = _stage(paths)
    assert result.returncode == 0, result.stderr
    assert (paths["venv"] / _BLOB).read_bytes() == b"\xcf\xfa\xed\xfe httpfs"
    assert _curl_calls(tmp_path) == []
    assert "sha256 verified" in result.stdout


def test_online_it_downloads_the_asset_pinned_to_this_version_and_verifies_it(tmp_path):
    asset = _asset_bytes()
    paths = _setup(tmp_path, pinned=asset, serves=asset)
    result = _stage(paths)
    assert result.returncode == 0, result.stderr
    assert _curl_calls(tmp_path) == [
        f"https://github.com/kenstott/provisa/releases/download/v9/{_ASSET}"
    ]
    assert (paths["venv"] / _BLOB).is_file()
    # The download is not left behind.
    assert not (paths["venv"].parent / _ASSET).exists()


def test_a_downloaded_asset_whose_checksum_does_not_match_is_an_error_naming_it(tmp_path):
    paths = _setup(tmp_path, pinned=_asset_bytes(), serves=_asset_bytes(b"tampered"))
    result = _stage(paths)
    assert result.returncode == 1
    assert f"{paths['venv'].parent / _ASSET}: sha256 " in result.stderr
    assert f"the checksum this Provisa pins for {_ASSET}" in result.stderr
    assert not (paths["venv"] / _BLOB).exists()
    assert not (paths["venv"].parent / _ASSET).exists()


def test_an_asset_beside_the_installer_whose_checksum_does_not_match_is_an_error(tmp_path):
    paths = _setup(tmp_path, pinned=_asset_bytes(), serves=_asset_bytes())
    (paths["installer"] / _ASSET).write_bytes(_asset_bytes(b"tampered"))
    result = _stage(paths)
    assert result.returncode == 1
    assert f"{paths['installer'] / _ASSET}: sha256 " in result.stderr
    assert not (paths["venv"] / _BLOB).exists()
    # The mismatched local copy is not passed over for the download.
    assert _curl_calls(tmp_path) == []


def test_with_neither_it_fails_naming_the_asset_and_where_to_put_it(tmp_path):
    paths = _setup(tmp_path, pinned=_asset_bytes())
    result = _stage(paths)
    assert result.returncode == 1
    assert f"DuckDB extensions not found: {_ASSET} is not beside the installer" in result.stderr
    assert f"Place {_ASSET} next to the Provisa installer (the .dmg) or in ~/Downloads" in (
        result.stderr
    )
    assert not (paths["venv"] / _BLOB).exists()


def test_a_build_without_a_release_version_does_not_download_and_says_where_to_put_it(tmp_path):
    asset = _asset_bytes()
    paths = _setup(tmp_path, pinned=asset, serves=asset)
    result = _stage(paths, version="dev")
    assert result.returncode == 1
    assert _curl_calls(tmp_path) == []
    assert f"DuckDB extensions not found: {_ASSET}. Place it next to the Provisa installer" in (
        result.stderr
    )


def test_it_reads_the_asset_from_downloads_when_the_app_is_not_on_a_disk_image(tmp_path):
    asset = _asset_bytes()
    paths = _setup(tmp_path, pinned=asset)
    downloads = paths["home"] / "Downloads"
    downloads.mkdir()
    (downloads / _ASSET).write_bytes(asset)
    result = _stage(paths, bundle=str(tmp_path / "Provisa.app/Contents"))
    assert result.returncode == 0, result.stderr
    assert (paths["venv"] / _BLOB).is_file()
    assert _curl_calls(tmp_path) == []


def test_an_app_without_the_pin_cannot_stage_the_extensions(tmp_path):
    asset = _asset_bytes()
    paths = _setup(tmp_path, pinned=asset, serves=asset)
    (paths["resources"] / "duckdb-extensions.sha256").unlink()
    result = _stage(paths)
    assert result.returncode == 1
    assert "has no duckdb-extensions.sha256" in result.stderr
    assert _curl_calls(tmp_path) == []


def test_the_runtime_is_stamped_up_to_date_only_after_its_extensions_are_staged():
    body = _FIRST_LAUNCH.split("stage_native_runtime() {")[1].split("\n}\n")[0]
    copy = body.index('cp -R "$src"/. "${venv}/"')
    unstamp = body.index('rm -f "${venv}/.runtime-version"')
    stage = body.index('stage_duckdb_extensions "$venv"')
    stamp = body.index('printf \'%s\' "$bundle_ver" > "${venv}/.runtime-version"')
    assert copy < unstamp < stage < stamp
