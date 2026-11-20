# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The provisa server's PEP 503 index over GitHub release assets (scripts/release_wheel_index.py)."""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "release_wheel_index.py"
_spec = importlib.util.spec_from_file_location("release_wheel_index", _SCRIPT)
assert _spec and _spec.loader
index = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(index)

_DL = "https://github.com/kenstott/provisa/releases/download"


def _asset(tag: str, name: str, sha: str | None = "ab" * 32) -> dict:
    return {
        "name": name,
        "browser_download_url": f"{_DL}/{tag}/{name}",
        "digest": None if sha is None else f"sha256:{sha}",
    }


def _release(tag: str, names: list[str], *, draft: bool = False) -> dict:
    return {"tag_name": tag, "draft": draft, "assets": [_asset(tag, n) for n in names]}


_W509 = "provisa-0.1.0a509-cp312-cp312-manylinux_2_28_x86_64.whl"
_W510 = "provisa-0.1.0a510-cp312-cp312-macosx_11_0_arm64.whl"


@pytest.mark.parametrize(
    ("name", "normalized"),
    [
        ("provisa", "provisa"),
        ("Provisa", "provisa"),
        ("provisa_client", "provisa-client"),
        ("Provisa.Notebook", "provisa-notebook"),
        ("provisa__-.client", "provisa-client"),
    ],
)
def test_names_are_pep503_normalized(name, normalized):
    assert index.normalize(name) == normalized


def test_only_the_projects_wheels_of_published_releases_are_listed(tmp_path):
    releases = [
        _release("v0.1.0-alpha.511", ["provisa-0.1.0a511-cp312-cp312-win_amd64.whl"], draft=True),
        _release(
            "v0.1.0-alpha.510",
            [
                _W510,
                "provisa_client-0.1.0a510-py3-none-any.whl",
                "provisa_notebook-0.1.0a510-py3-none-any.whl",
                "provisa-0.1.0a510.tar.gz",
                "Provisa-0.1.0-alpha.510.dmg",
            ],
        ),
        _release("v0.1.0-alpha.509", [_W509, "Provisa-0.1.0a509-1-cp312-cp312-win_amd64.whl"]),
    ]
    listed = index.write_index(releases, tmp_path)
    assert listed == sorted([_W509, _W510, "Provisa-0.1.0a509-1-cp312-cp312-win_amd64.whl"])
    page = (tmp_path / "simple" / "provisa" / "index.html").read_text()
    hrefs = re.findall(r'href="([^"]+)"', page)
    assert all(h.startswith(f"{_DL}/v0.1.0-alpha.5") for h in hrefs)
    assert not [h for h in hrefs if "alpha.511" in h or "client" in h or "notebook" in h]
    root = (tmp_path / "simple" / "index.html").read_text()
    assert re.findall(r'href="([^"]+)"', root) == ["provisa/"]


def test_every_link_carries_its_assets_sha256_fragment(tmp_path):
    sha = "0123456789abcdef" * 4
    release = {"draft": False, "assets": [_asset("v0.1.0-alpha.510", _W510, sha)]}
    index.write_index([release], tmp_path)
    page = (tmp_path / "simple" / "provisa" / "index.html").read_text()
    assert f'<a href="{_DL}/v0.1.0-alpha.510/{_W510}#sha256={sha}">{_W510}</a>' in page


def test_a_wheel_without_a_sha256_digest_is_refused(tmp_path):
    release = {"draft": False, "assets": [_asset("v0.1.0-alpha.510", _W510, None)]}
    with pytest.raises(ValueError, match="no sha256 digest"):
        index.write_index([release], tmp_path)


def test_no_wheel_at_all_is_refused(tmp_path):
    with pytest.raises(ValueError, match="no published release"):
        index.write_index([_release("v1", [_W510], draft=True)], tmp_path)


def test_the_same_wheel_on_two_releases_is_refused(tmp_path):
    with pytest.raises(ValueError, match="more than one release"):
        index.write_index([_release("a", [_W510]), _release("b", [_W510])], tmp_path)


def test_main_reads_the_slurped_paginated_listing(tmp_path, capsys):
    pages = [[_release("v0.1.0-alpha.510", [_W510])], [_release("v0.1.0-alpha.509", [_W509])]]
    listing = tmp_path / "releases.json"
    listing.write_text(json.dumps(pages))
    assert index.main([str(listing), str(tmp_path / "site")]) == 0
    assert "2 provisa wheel(s) indexed" in capsys.readouterr().out
    assert (tmp_path / "site" / "simple" / "provisa" / "index.html").is_file()
