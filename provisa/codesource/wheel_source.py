# Copyright (c) 2026 Kenneth Stott
# Canary: 3c9e7b21-5d4a-4f18-9a62-e0b7c4d81f56
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a build image's Linux provisa wheel comes from (REQ-2194): an ordered set of sources.

The maintainer decided the order (2026-11-26): the wheelhouse cache, then the GitHub release of
the running Provisa's version (never PyPI: every package ships as a GitHub release asset), then a
build from source. Each source either puts the wheel in the wheelhouse and returns its path, or
returns a :class:`Miss` saying why it has none, which hands the lookup to the next source. A
source that finds something wrong (a wheel of another version, a checksum that does not match, a
release that cannot be read) refuses by name; it never hands that on.

A source is ``source(version, arch, house) -> Path | Miss``: ``version`` the running Provisa's
(PEP 440), ``arch`` the image's (``amd64`` or ``arm64``), ``house`` the wheelhouse.
"""

# Requirements: REQ-2194
from __future__ import annotations

import hashlib
import os
import tempfile
import zipfile
from dataclasses import dataclass
from email.parser import HeaderParser
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable, Sequence

from provisa.codesource import wheel_build
from provisa.codesource.bundle import BundleRefused
from provisa.runtime_deps.code_runtimes import Fetcher, RuntimeUnavailable, fetch_https

#: The repository whose releases carry the provisa wheels (release-publish.yml).
GITHUB_REPO = "kenstott/provisa"
WHEEL_ARCH = {"amd64": "x86_64", "arm64": "aarch64"}
#: The release listing's timeout: a small JSON document (the wheel itself is fetched by
#: fetch_https, under its own).
LISTING_TIMEOUT = 60.0
#: A PEP 440 pre-release stage as a release tag spells it (release-metadata/action.yml).
_STAGE = {"a": "alpha", "b": "beta", "rc": "rc"}


@dataclass(frozen=True)
class Miss:
    """A source has no wheel for this version and architecture, and why: the next source is tried."""

    source: str
    reason: str


class WheelRefused(BundleRefused):
    """A wheel a source found but cannot use, named with why."""


Source = Callable[[str, str, Path], "Path | Miss"]
#: The release of a tag as the GitHub API lists it, or None when no release of it is published.
Listing = Callable[[str], "dict[str, Any] | None"]


def platform_tag(arch: str) -> str:
    return f"manylinux_2_28_{WHEEL_ARCH[arch]}"


def wheel_version(name: str) -> str | None:
    """The version in a wheel's filename, or None when the name is not a wheel's."""
    from packaging.utils import InvalidWheelFilename, parse_wheel_filename

    try:
        return str(parse_wheel_filename(name)[1])
    except InvalidWheelFilename:
        return None


def _same(found: str | None, version: str) -> bool:
    from packaging.version import InvalidVersion, Version

    if found is None:
        return False
    try:
        return Version(found) == Version(version)
    except InvalidVersion:
        return False


def cached(version: str, arch: str, house: Path) -> Path | Miss:
    """The wheel already in the wheelhouse: a released one by its version alone, a source-built
    one only while it carries the source hash of this checkout's rust/ (wheel_build: a stale
    source-built wheel is handed on, to be rebuilt)."""
    tag = platform_tag(arch)
    found = sorted(house.glob(f"provisa-*-{tag}.whl")) if house.is_dir() else []
    wheels = [w for w in found if _same(wheel_version(w.name), version)]
    if not wheels:
        return Miss("cache", f"no provisa {version} wheel for {tag} in {house}")
    released = [w for w in wheels if wheel_build.built_from(w) is None]
    if released:
        return released[-1]
    current = wheel_build.checkout_hash()
    fresh = [w for w in wheels if wheel_build.built_from(w) == current]
    if fresh:
        return fresh[-1]
    names = ", ".join(w.name for w in wheels)
    return Miss(
        "cache", f"{names} in {house} was built from rust/ sources other than this checkout's"
    )


def release_tag(version: str) -> str | None:
    """The release tag of a PEP 440 release version (the inverse of release-metadata's
    py_version: ``0.1.0a510`` -> ``v0.1.0-alpha.510``), or None for a version no release is cut
    as (a development, post or local version)."""
    from packaging.version import InvalidVersion, Version

    try:
        parsed = Version(version)
    except InvalidVersion:
        return None
    if parsed.local or parsed.is_devrelease or parsed.is_postrelease or len(parsed.release) != 3:
        return None
    tag = "v" + ".".join(str(part) for part in parsed.release)
    if parsed.pre is not None:
        stage, number = parsed.pre
        tag += f"-{_STAGE[stage]}.{number}"
    return tag


def github_release(tag: str) -> dict[str, Any] | None:
    """The published release ``tag`` of GITHUB_REPO from the GitHub API, or None when there is
    none (404: a draft is not served). Any other failure is raised."""
    import httpx

    from provisa.core.http_failure import describe_http_failure

    url = f"https://api.github.com/repos/{GITHUB_REPO}/releases/tags/{tag}"
    try:
        response = httpx.get(
            url,
            headers={"Accept": "application/vnd.github+json"},
            follow_redirects=True,
            timeout=LISTING_TIMEOUT,
        )
    except httpx.HTTPError as exc:
        raise WheelRefused(
            "image_wheel_release",
            f"the release {tag} could not be read: {describe_http_failure(exc)}",
        ) from exc
    if response.status_code == HTTPStatus.NOT_FOUND:
        return None
    if response.status_code != HTTPStatus.OK:
        raise WheelRefused(
            "image_wheel_release",
            f"the release {tag} could not be read: HTTP {response.status_code}",
        )
    return response.json()


def _metadata_version(path: Path) -> str | None:
    with zipfile.ZipFile(path) as wheel:
        names = [n for n in wheel.namelist() if n.endswith(".dist-info/METADATA")]
        if len(names) != 1:
            return None
        return HeaderParser().parsestr(wheel.read(names[0]).decode()).get("Version")


def from_release(listing: Listing = github_release, fetch: Fetcher = fetch_https) -> Source:
    """The source that downloads the wheel from the release of the running version into the
    wheelhouse: its version checked (filename and metadata), and its SHA-256 when the release
    publishes one."""

    def source(version: str, arch: str, house: Path) -> Path | Miss:
        tag = release_tag(version)
        if tag is None:
            return Miss("release", f"{version} is not a release version")
        release = listing(tag)
        if release is None:
            return Miss("release", f"no published release {tag}")
        ptag = platform_tag(arch)
        assets = [
            a for a in release.get("assets", []) if a["name"].startswith("provisa-")
            and a["name"].endswith(f"-{ptag}.whl")
        ]  # fmt: skip
        # release-publish.yml refuses to publish a release without both manylinux_2_28 provisa
        # wheels, so a published release lacking one is a defect, refused rather than skipped.
        if len(assets) != 1:
            raise WheelRefused(
                "image_wheel_release",
                f"the release {tag} carries {len(assets)} provisa wheels for {ptag}, not one",
                arch=arch,
            )
        (asset,) = assets
        name = asset["name"]
        if not _same(wheel_version(name), version):
            raise WheelRefused(
                "image_wheel_version",
                f"the release {tag} carries {name}, not a provisa {version} wheel",
                arch=arch,
            )
        try:
            data = fetch(asset["browser_download_url"])
        except RuntimeUnavailable as exc:
            raise WheelRefused("image_wheel_fetch", f"{name} from {tag}: {exc}", arch=arch) from exc
        # The maintainer's decision (2026-11-26): the checksum is checked when the release
        # publishes one. GitHub lists an asset's ``digest`` only for assets uploaded since it
        # began computing them; an older asset has none.
        digest = asset.get("digest")
        if digest is not None:
            algorithm, _, expected = digest.partition(":")
            actual = hashlib.sha256(data).hexdigest()
            if algorithm != "sha256" or actual != expected:
                raise WheelRefused(
                    "image_wheel_checksum",
                    f"{name} from {tag} has SHA-256 {actual}; the release publishes {digest}",
                    arch=arch,
                )
        house.mkdir(parents=True, exist_ok=True)
        fd, staged = tempfile.mkstemp(prefix=f".{name}.", suffix=".download", dir=house)
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)
            inside = _metadata_version(Path(staged))
            if not _same(inside, version):
                raise WheelRefused(
                    "image_wheel_version",
                    f"{name} from {tag} says it is provisa {inside}, not {version}",
                    arch=arch,
                )
            target = house / name
            os.replace(staged, target)
        except BaseException:
            Path(staged).unlink(missing_ok=True)
            raise
        return target

    return source


#: The maintainer's order (2026-11-26): cache, then the release of the running version, then a
#: build from this checkout's rust/ sources. The only fallbacks are this order; PyPI, when that
#: channel reopens, becomes the first remote source (before the release).
SOURCES: tuple[Source, ...] = (cached, from_release(), wheel_build.build_wheel)


def provisa_wheel(
    version: str, arch: str, house: Path, sources: Sequence[Source] = SOURCES
) -> Path | list[Miss]:
    """The wheel from the first source that has it, or every source's miss when none does."""
    misses: list[Miss] = []
    for source in sources:
        found = source(version, arch, house)
        if isinstance(found, Miss):
            misses.append(found)
            continue
        return found
    return misses
