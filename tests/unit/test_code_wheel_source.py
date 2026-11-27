# Copyright (c) 2026 Kenneth Stott
# Canary: 7a2e4c19-8b6d-4e03-b5f1-2d9c6e8a4b70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The build image's provisa wheel from its ordered sources (REQ-2194): the wheelhouse, the
GitHub release of the running version, then the next source."""

# Requirements: REQ-2194
from __future__ import annotations

import hashlib
import io
import zipfile
from pathlib import Path
from typing import Any

import pytest

from provisa.codesource import image_build, wheel_source
from provisa.runtime_deps.code_runtimes import RuntimeUnavailable

pytestmark = pytest.mark.unit

VERSION = "0.1.0a510"
TAG = "v0.1.0-alpha.510"
DOWNLOAD = f"https://github.com/kenstott/provisa/releases/download/{TAG}"


@pytest.fixture(autouse=True)
def _build_cache_wheelhouse(monkeypatch):
    """The wheelhouse is the build cache's, not one the environment names."""
    monkeypatch.delenv(image_build.WHEELHOUSE_ENV, raising=False)


def _wheel(version: str = VERSION, machine: str = "x86_64", inside: str | None = None) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("provisa/_encoders.cpython-312-linux.so", f"encoders {machine}".encode())
        archive.writestr(
            f"provisa-{version}.dist-info/METADATA",
            f"Metadata-Version: 2.1\nName: provisa\nVersion: {inside or version}\n",
        )
    return buf.getvalue()


def _name(version: str = VERSION, machine: str = "x86_64") -> str:
    return f"provisa-{version}-cp312-cp312-manylinux_2_28_{machine}.whl"


def _listing(wheels: dict[str, bytes], digest: bool = True) -> dict[str, Any]:
    """The release as the GitHub API lists it (GET /repos/kenstott/provisa/releases/tags/<tag>),
    recorded and trimmed to the fields read, with the release's other assets beside the wheels."""
    others = ["provisa_client-0.1.0a510-py3-none-any.whl", _name(machine="win_amd64")]
    return {
        "tag_name": TAG,
        "draft": False,
        "prerelease": False,
        "assets": [
            *(
                {"name": n, "browser_download_url": f"{DOWNLOAD}/{n}", "digest": "sha256:00"}
                for n in others
            ),
            *(
                {
                    "name": name,
                    "browser_download_url": f"{DOWNLOAD}/{name}",
                    "digest": f"sha256:{hashlib.sha256(data).hexdigest()}" if digest else None,
                }
                for name, data in wheels.items()
            ),
        ],
    }


class _Release:
    """A recorded release served to the release source: the listing and its assets' bytes."""

    def __init__(self, listing: dict[str, Any] | None, files: dict[str, bytes]):
        self.listing = listing
        self.files = files
        self.listed: list[str] = []
        self.fetched: list[str] = []

    def list(self, tag: str) -> dict[str, Any] | None:
        self.listed.append(tag)
        return self.listing

    def fetch(self, url: str) -> bytes:
        self.fetched.append(url)
        name = url.rsplit("/", 1)[1]
        if name not in self.files:
            raise RuntimeUnavailable(f"failed to download {url}: HTTP 404")
        return self.files[name]

    def source(self) -> wheel_source.Source:
        return wheel_source.from_release(self.list, self.fetch)


def _published(**listing: Any) -> _Release:
    files = {_name(machine=m): _wheel(machine=m) for m in ("x86_64", "aarch64")}
    return _Release(_listing(files, **listing), files)


def test_release_tags_are_the_inverse_of_the_release_metadata():
    assert wheel_source.release_tag("0.1.0a510") == "v0.1.0-alpha.510"
    assert wheel_source.release_tag("0.2.0b3") == "v0.2.0-beta.3"
    assert wheel_source.release_tag("1.0.0rc1") == "v1.0.0-rc.1"
    assert wheel_source.release_tag("1.0.0") == "v1.0.0"
    for unreleased in ("0.1.0a7+local", "0.1.0a510.dev3", "0.1.0.post1", "0.1", "not a version"):
        assert wheel_source.release_tag(unreleased) is None


def test_a_wheel_in_the_wheelhouse_is_taken_without_the_release(tmp_path: Path):
    house = tmp_path / "wheels"
    house.mkdir()
    (house / _name()).write_bytes(_wheel())
    release = _published()
    found = wheel_source.provisa_wheel(
        VERSION, "amd64", house, (wheel_source.cached, release.source())
    )
    assert found == house / _name()
    assert release.listed == [] and release.fetched == []


@pytest.mark.parametrize("arch,machine", [("amd64", "x86_64"), ("arm64", "aarch64")])
def test_a_miss_downloads_the_wheel_of_the_running_version_from_its_release(
    tmp_path: Path, arch: str, machine: str
):
    house = tmp_path / "wheels"
    release = _published()
    sources = (wheel_source.cached, release.source())
    found = wheel_source.provisa_wheel(VERSION, arch, house, sources)
    assert found == house / _name(machine=machine)
    assert found.read_bytes() == release.files[_name(machine=machine)]
    assert release.listed == [TAG]
    assert release.fetched == [f"{DOWNLOAD}/{_name(machine=machine)}"]
    # Nothing staged is left beside it; the next lookup is a cache hit.
    assert sorted(p.name for p in house.iterdir()) == [_name(machine=machine)]
    assert wheel_source.provisa_wheel(VERSION, arch, house, sources) == found
    assert release.listed == [TAG]


def test_the_image_is_built_with_the_encoders_of_the_downloaded_wheel(tmp_path: Path):
    release = _published()
    plan = _plan(tmp_path)
    entries = {
        e.path: e
        for e in image_build._encoders(plan, "arm64", (wheel_source.cached, release.source()))
    }
    module = entries["/opt/provisa/frontlib/provisa/_encoders.cpython-312-linux.so"]
    assert module.data == b"encoders aarch64"


def test_a_release_without_a_checksum_is_taken_by_its_version(tmp_path: Path):
    release = _published(digest=False)
    found = wheel_source.provisa_wheel(VERSION, "amd64", tmp_path, (release.source(),))
    assert found == tmp_path / _name()


def test_a_release_wheel_of_another_version_is_refused_by_name(tmp_path: Path):
    files = {_name("0.1.0a509"): _wheel("0.1.0a509")}
    release = _Release(_listing(files), files)
    with pytest.raises(wheel_source.WheelRefused) as refused:
        wheel_source.provisa_wheel(VERSION, "amd64", tmp_path, (release.source(),))
    assert refused.value.code == "codesource.image_wheel_version"
    assert _name("0.1.0a509") in str(refused.value) and VERSION in str(refused.value)
    assert release.fetched == []


def test_a_wheel_whose_metadata_names_another_version_is_refused_and_not_kept(tmp_path: Path):
    files = {_name(): _wheel(inside="0.1.0a509")}
    release = _Release(_listing(files), files)
    with pytest.raises(wheel_source.WheelRefused) as refused:
        wheel_source.provisa_wheel(VERSION, "amd64", tmp_path, (release.source(),))
    assert refused.value.code == "codesource.image_wheel_version"
    assert "0.1.0a509" in str(refused.value)
    assert list(tmp_path.iterdir()) == []


def test_a_wheel_whose_checksum_is_not_the_releases_is_refused_and_not_kept(tmp_path: Path):
    release = _published()
    release.files[_name()] = _wheel() + b"tampered"
    with pytest.raises(wheel_source.WheelRefused) as refused:
        wheel_source.provisa_wheel(VERSION, "amd64", tmp_path, (release.source(),))
    assert refused.value.code == "codesource.image_wheel_checksum"
    assert _name() in str(refused.value)
    assert list(tmp_path.iterdir()) == []


def test_a_published_release_missing_the_wheel_is_refused_by_name(tmp_path: Path):
    files = {_name(machine="aarch64"): _wheel(machine="aarch64")}
    release = _Release(_listing(files), files)
    with pytest.raises(wheel_source.WheelRefused) as refused:
        wheel_source.provisa_wheel(VERSION, "amd64", tmp_path, (release.source(),))
    assert refused.value.code == "codesource.image_wheel_release"
    assert TAG in str(refused.value) and "manylinux_2_28_x86_64" in str(refused.value)


def test_a_failed_download_is_refused_not_handed_on(tmp_path: Path):
    release = _published()
    del release.files[_name()]
    later = _Later(tmp_path)
    with pytest.raises(wheel_source.WheelRefused) as refused:
        wheel_source.provisa_wheel(VERSION, "amd64", tmp_path, (release.source(), later))
    assert refused.value.code == "codesource.image_wheel_fetch"
    assert later.asked == []


class _Later:
    """The next source (the source build): records that it was asked, and builds the wheel."""

    def __init__(self, house: Path):
        self.house = house
        self.asked: list[tuple[str, str]] = []

    def __call__(self, version: str, arch: str, house: Path) -> Path:
        self.asked.append((version, arch))
        path = house / _name(version, wheel_source.WHEEL_ARCH[arch])
        house.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_wheel(version, wheel_source.WHEEL_ARCH[arch]))
        return path


def test_a_version_with_no_published_release_is_handed_to_the_next_source(tmp_path: Path):
    release = _Release(None, {})
    later = _Later(tmp_path)
    found = wheel_source.provisa_wheel(
        VERSION, "arm64", tmp_path, (wheel_source.cached, release.source(), later)
    )
    assert release.listed == [TAG] and later.asked == [(VERSION, "arm64")]
    assert found == tmp_path / _name(machine="aarch64")


def test_an_unreleased_version_is_handed_on_without_asking_for_a_release(tmp_path: Path):
    release = _Release(None, {})
    later = _Later(tmp_path)
    wheel_source.provisa_wheel("0.1.0a7+local", "amd64", tmp_path, (release.source(), later))
    assert release.listed == [] and later.asked == [("0.1.0a7+local", "amd64")]


def test_when_no_source_has_the_wheel_the_build_is_refused_naming_each(tmp_path: Path):
    plan = _plan(tmp_path)
    release = _Release(None, {})
    with pytest.raises(image_build.ImageBuildRefused) as refused:
        image_build._encoders(plan, "amd64", (wheel_source.cached, release.source()))
    assert refused.value.code == "codesource.image_encoders"
    text = str(refused.value)
    assert f"no provisa {VERSION} wheel for linux/amd64" in text
    assert "cache: no provisa" in text and f"release: no published release {TAG}" in text


def _plan(tmp_path: Path) -> image_build.Plan:
    return image_build.Plan(
        source_id="Rates Source",
        language="python",
        runtime="python",
        code_hash="0" * 64,
        entry=None,
        exports=[],
        names={},
        manifest={},
        registries=[],
        cache_root=tmp_path / "cache",
        version=VERSION,
    )
