# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The core images ship as release assets each under GitHub's 2 GiB limit, and every installer
rebuilds each image from them.

alpha.513's one core images archive per architecture outgrew the limit (`gh release upload`: HTTP
422 "size must be less than 2147483648"). scripts/ci/package_core_images.sh now writes one asset
per image, an image of the part size or more as numbered parts, and a sha256 manifest listing them
in order. The Linux AppImage's and the macOS DMG's first launch are run here as the scripts run
them, on assets the packaging script wrote."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

_ROOT = Path(__file__).parents[2]
_PACKAGE = _ROOT / "scripts/ci/package_core_images.sh"
_TAG = "v0.1.0-alpha.513"
_PROVISA = bytes(range(256)) * 10  # 2560 bytes: three parts at a 1000-byte part size
_ZAYCHIK = b"the zaychik image"


def _package(tmp_path: Path, arch: str, part_bytes: int = 1000) -> Path:
    images = tmp_path / "images"
    images.mkdir()
    (images / "provisa-local.tar.gz").write_bytes(_PROVISA)
    (images / "zaychik-local.tar.gz").write_bytes(_ZAYCHIK)
    out = tmp_path / "assets"
    subprocess.run(
        ["bash", str(_PACKAGE), str(images), arch, _TAG, str(out), str(part_bytes)],
        check=True,
        capture_output=True,
    )
    return out


def test_each_image_is_its_own_asset_and_a_large_one_is_split_into_ordered_parts(tmp_path):
    out = _package(tmp_path, "arm64")
    provisa = f"provisa-core-image-provisa-local-arm64-{_TAG}.tar.gz"
    zaychik = f"provisa-core-image-zaychik-local-arm64-{_TAG}.tar.gz"
    manifest = f"provisa-core-images-arm64-{_TAG}.sha256"
    assert sorted(p.name for p in out.iterdir()) == sorted(
        [f"{provisa}.part1", f"{provisa}.part2", f"{provisa}.part3", zaychik, manifest]
    )
    listed = [line.split("  ", 1)[1] for line in (out / manifest).read_text().splitlines()]
    assert listed == [f"{provisa}.part1", f"{provisa}.part2", f"{provisa}.part3", zaychik]
    assert b"".join((out / name).read_bytes() for name in listed[:3]) == _PROVISA
    check = subprocess.run(["sha256sum", "-c", manifest], cwd=out, capture_output=True, text=True)
    assert check.returncode == 0, check.stdout + check.stderr


def test_packaging_fails_when_an_asset_would_be_2_gib_or_more(tmp_path):
    """A part size at the limit leaves a sparse 2 GiB image whole: the script's own size check
    refuses it, so the dry run fails before anything could be uploaded."""
    images = tmp_path / "images"
    images.mkdir()
    with (images / "provisa-local.tar.gz").open("wb") as f:
        f.truncate(2147483648)
    done = subprocess.run(
        ["bash", str(_PACKAGE), str(images), "amd64", _TAG, str(tmp_path / "out"), "4294967296"],
        capture_output=True,
        text=True,
    )
    assert done.returncode == 1
    assert "is 2147483648 bytes; a GitHub release asset must be under 2147483648" in done.stdout


def _function(script: Path, name: str) -> str:
    text = script.read_text()
    start = text.index(f"{name}() {{")
    return text[start : text.index("\n}\n", start) + 3]


_CURL_STUB = """
curl() {
  local out="" url=""
  while [ $# -gt 0 ]; do
    case "$1" in
      -o) out="$2"; shift ;;
      -*) ;;
      *) url="$1" ;;
    esac
    shift
  done
  printf '%s\\n' "$url" >> "$FETCHED"
  [ -f "$SERVED/${url##*/}" ] || return 22
  cp "$SERVED/${url##*/}" "$out"
}
"""


def _linux(tmp_path: Path, appdir_parent: Path, served: Path) -> subprocess.CompletedProcess:
    script = _ROOT / "packaging/linux/first-launch.sh"
    body = "\n".join(
        [
            'info() { printf "%s\\n" "$*"; }',
            'err() { printf "%s\\n" "$*" >&2; }',
            _CURL_STUB,
            _function(script, "_find_core_asset"),
            _function(script, "_assemble_core_images"),
            f'_assemble_core_images "{tmp_path}/staged" "{appdir_parent}"',
        ]
    )
    (tmp_path / "staged").mkdir(exist_ok=True)
    (tmp_path / "home").mkdir(exist_ok=True)
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", body],
        cwd=tmp_path,
        env={
            "PATH": os.environ["PATH"],
            "HOME": str(tmp_path / "home"),
            "PROVISA_HOME": str(tmp_path / "provisa-home"),
            "PROVISA_VERSION": _TAG,
            "SERVED": str(served),
            "FETCHED": str(tmp_path / "fetched"),
        },
        capture_output=True,
        text=True,
    )


def test_the_appimage_rebuilds_every_image_from_assets_beside_it(tmp_path):
    assets = _package(tmp_path, "amd64")
    done = _linux(tmp_path, assets, tmp_path / "nothing-served")
    assert done.returncode == 0, done.stderr
    staged = tmp_path / "staged"
    assert (staged / "provisa-local.tar.gz").read_bytes() == _PROVISA
    assert (staged / "zaychik-local.tar.gz").read_bytes() == _ZAYCHIK
    assert not (tmp_path / "fetched").exists()


def test_the_appimage_downloads_the_manifest_and_every_asset_it_lists(tmp_path):
    served = _package(tmp_path, "amd64")
    appdir_parent = tmp_path / "appdir-parent"
    appdir_parent.mkdir()
    done = _linux(tmp_path, appdir_parent, served)
    assert done.returncode == 0, done.stderr
    base = f"https://github.com/kenstott/provisa/releases/download/{_TAG}"
    provisa = f"provisa-core-image-provisa-local-amd64-{_TAG}.tar.gz"
    assert (tmp_path / "fetched").read_text().splitlines() == [
        f"{base}/provisa-core-images-amd64-{_TAG}.sha256",
        f"{base}/{provisa}.part1",
        f"{base}/{provisa}.part2",
        f"{base}/{provisa}.part3",
        f"{base}/provisa-core-image-zaychik-local-amd64-{_TAG}.tar.gz",
    ]
    assert (tmp_path / "staged" / "provisa-local.tar.gz").read_bytes() == _PROVISA
    # The downloads are removed once assembled.
    assert not (tmp_path / "provisa-home" / "core-images-download").exists()


def test_the_appimage_refuses_an_asset_that_does_not_match_its_sha256(tmp_path):
    assets = _package(tmp_path, "amd64")
    part = assets / f"provisa-core-image-provisa-local-amd64-{_TAG}.tar.gz.part2"
    part.write_bytes(b"truncated")
    done = _linux(tmp_path, assets, tmp_path / "nothing-served")
    assert done.returncode == 1
    assert f"Core image asset {part} does not match its sha256" in done.stderr


def test_the_appimage_names_a_listed_asset_it_cannot_find_or_download(tmp_path):
    assets = _package(tmp_path, "amd64")
    zaychik = f"provisa-core-image-zaychik-local-amd64-{_TAG}.tar.gz"
    (assets / zaychik).unlink()
    done = _linux(tmp_path, assets, tmp_path / "nothing-served")
    assert done.returncode == 1
    assert f"Core image asset {zaychik} (listed in provisa-core-images-amd64-{_TAG}.sha256)" in (
        done.stderr
    )


def _macos(tmp_path: Path, bundle_parent: Path, served: Path) -> subprocess.CompletedProcess:
    script = _ROOT / "packaging/macos/first-launch.sh"
    body = "\n".join(
        [
            'info() { printf "%s\\n" "$*"; }',
            'ok() { printf "%s\\n" "$*"; }',
            'err() { printf "%s\\n" "$*" >&2; }',
            "_lc() { printf '%s' \"$1\" | tr '[:upper:]' '[:lower:]'; }",
            _CURL_STUB,
            _function(script, "_core_asset_local"),
            _function(script, "_core_asset_download"),
            _function(script, "acquire_core_images"),
            f'acquire_core_images "{_TAG}" "{tmp_path}/staged"',
        ]
    )
    (tmp_path / "staged").mkdir(exist_ok=True)
    (tmp_path / "home").mkdir(exist_ok=True)
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", body],
        env={
            "PATH": os.environ["PATH"],
            "HOME": str(tmp_path / "home"),
            "BUNDLE_DIR": str(bundle_parent / "Provisa.app"),
            "SCRIPT_DIR": str(tmp_path / "script-dir"),
            "PROVISA_HOME": str(tmp_path / "provisa-home"),
            "PROVISA_NONINTERACTIVE": "1",
            "SERVED": str(served),
            "FETCHED": str(tmp_path / "fetched"),
        },
        capture_output=True,
        text=True,
    )


def test_the_dmg_rebuilds_every_image_from_assets_beside_the_installer(tmp_path):
    assets = _package(tmp_path, "arm64")
    done = _macos(tmp_path, assets, tmp_path / "nothing-served")
    assert done.returncode == 0, done.stderr
    assert (tmp_path / "staged" / "provisa-local.tar.gz").read_bytes() == _PROVISA
    assert (tmp_path / "staged" / "zaychik-local.tar.gz").read_bytes() == _ZAYCHIK
    assert not (tmp_path / "fetched").exists()


def test_the_dmg_downloads_what_is_not_beside_the_installer(tmp_path):
    served = _package(tmp_path, "arm64")
    bundle_parent = tmp_path / "Applications"
    bundle_parent.mkdir()
    done = _macos(tmp_path, bundle_parent, served)
    assert done.returncode == 0, done.stderr
    assert len((tmp_path / "fetched").read_text().splitlines()) == 5
    assert (tmp_path / "staged" / "provisa-local.tar.gz").read_bytes() == _PROVISA
    assert not (tmp_path / "provisa-home" / "core-images-download").exists()


def test_the_dmg_refuses_an_asset_that_does_not_match_its_sha256(tmp_path):
    assets = _package(tmp_path, "arm64")
    (assets / f"provisa-core-image-zaychik-local-arm64-{_TAG}.tar.gz").write_bytes(b"wrong")
    done = _macos(tmp_path, assets, tmp_path / "nothing-served")
    assert done.returncode == 1
    assert "does not match its sha256" in done.stderr
    assert list((tmp_path / "staged").iterdir()) == []
