# Copyright (c) 2026 Kenneth Stott
# Canary: b7d08db5-6aee-4f66-b500-cf9bb1f3597f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On-demand, pinned, hash-checked runtimes for code sources (REQ-1984, REQ-1993).

A code source names its runtime: Node, Bun or Deno for JavaScript; Python (with uv, which installs its
dependencies) for Python. None of them ships with Provisa. Each is
fetched on first use from its project's own release, at the version pinned here, checked against
the SHA-256 recorded here (the project's published sum) BEFORE anything is extracted, and cached
under the runtime-deps cache root beside the pgwire bundles, version-namespaced. A cache hit never
downloads.

Resolution fails loud: an unknown runtime, a platform with no pinned archive, a download that
fails, an archive whose hash is not the recorded one, or an archive without the runtime's
executable raises ``RuntimeUnavailable``. There is no unpinned or unchecked fallback.
"""

# Requirements: REQ-1984, REQ-1993
from __future__ import annotations

import hashlib
import io
import os
import platform
import shutil
import stat
import tarfile
import zipfile
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Callable

from provisa.runtime_deps.pgwire_bundles import default_cache_root

#: The release platforms Provisa runs on (the same three the pgwire bundles are built for).
DARWIN_ARM64 = ("Darwin", "arm64")
LINUX_X64 = ("Linux", "x86_64")
#: REQ-2098: a remote host's image may be built for 64-bit ARM Linux; Provisa itself does not run
#: there.
LINUX_ARM64 = ("Linux", "aarch64")
WINDOWS_X64 = ("Windows", "AMD64")


class RuntimeUnavailable(Exception):  # REQ-1984
    """A code source's runtime could not be resolved; never a fallback."""


@dataclass(frozen=True)
class Asset:
    """One platform's archive of a pinned runtime release."""

    filename: str
    #: The project's published SHA-256 of ``filename``.
    sha256: str
    #: The runtime's executable, relative to the extracted archive's root.
    executable: str


@dataclass(frozen=True)
class RuntimePin:
    name: str
    version: str
    #: ``{version}`` and ``{filename}`` are filled in.
    url_template: str
    assets: dict[tuple[str, str], Asset]

    def asset(self, key: tuple[str, str] | None = None) -> Asset:
        key = key or (platform.system(), platform.machine())
        found = self.assets.get(key)
        if found is None:
            raise RuntimeUnavailable(
                f"no {self.name} {self.version} is pinned for {key[0]}/{key[1]}"
            )
        return found

    def url(self, asset: Asset) -> str:
        return self.url_template.format(version=self.version, filename=asset.filename)


# The pinned releases. A bump changes the version and every sum together; each sum is the one the
# project publishes (Node: SHASUMS256.txt; Bun: the release's SHASUMS256.txt; Deno: the asset's
# .sha256sum).
RUNTIMES: dict[str, RuntimePin] = {
    "node": RuntimePin(
        "node",
        "24.21.0",
        "https://nodejs.org/dist/v{version}/{filename}",
        {
            DARWIN_ARM64: Asset(
                "node-v24.21.0-darwin-arm64.tar.gz",
                "bed7eea5325e1108f32ce5228ddd6a5f0f08a499ee42aa7442aea583702f6057",
                "node-v24.21.0-darwin-arm64/bin/node",
            ),
            LINUX_X64: Asset(
                "node-v24.21.0-linux-x64.tar.gz",
                "6e1db87ef58b8819e5d5402eff1536491b18edd8eb7bee5ef7897876e88dc5ff",
                "node-v24.21.0-linux-x64/bin/node",
            ),
            LINUX_ARM64: Asset(
                "node-v24.21.0-linux-arm64.tar.gz",
                "724282c3b43aec998aa9527380465b45d229e021b58035f5f4f63095eabfe5d5",
                "node-v24.21.0-linux-arm64/bin/node",
            ),
            WINDOWS_X64: Asset(
                "node-v24.21.0-win-x64.zip",
                "158f7685b44de51f6c0df1d153526cbcd3e1bc739a8dfc607721cef75de9e541",
                "node-v24.21.0-win-x64/node.exe",
            ),
        },
    ),
    "bun": RuntimePin(
        "bun",
        "1.4.2",
        "https://github.com/oven-sh/bun/releases/download/bun-v{version}/{filename}",
        {
            DARWIN_ARM64: Asset(
                "bun-darwin-aarch64.zip",
                "90987a3a16d7db556d886ac3d551e7b6d3edf0a1cf43acaed622e8676be1d12f",
                "bun-darwin-aarch64/bun",
            ),
            LINUX_X64: Asset(
                "bun-linux-x64.zip",
                "36368faef7527875d5ffa52e53cd48021741f2a83eb6208a8dd64068d422a913",
                "bun-linux-x64/bun",
            ),
            LINUX_ARM64: Asset(
                "bun-linux-aarch64.zip",
                "54328bbc2d9c8e0c9f892c544d66c57a83b84139e34909e5ee81758f1ac8fda7",
                "bun-linux-aarch64/bun",
            ),
            WINDOWS_X64: Asset(
                "bun-windows-x64.zip",
                "ce4c17497b2f29712a99d3d53f028de28cd42e3bacb8589599e7f000e49b6405",
                "bun-windows-x64/bun.exe",
            ),
        },
    ),
    "deno": RuntimePin(
        "deno",
        "2.9.7",
        "https://github.com/denoland/deno/releases/download/v{version}/{filename}",
        {
            DARWIN_ARM64: Asset(
                "deno-aarch64-apple-darwin.zip",
                "5cd46d6268f6f78f5d88bdc7159d20bd44cdaa4b3303474839f87ec6fe7ae25c",
                "deno",
            ),
            LINUX_X64: Asset(
                "deno-x86_64-unknown-linux-gnu.zip",
                "c6527f24f4b16031d3ae4fa9f658d5f11534c8d84ce7dc8502420280919c3490",
                "deno",
            ),
            LINUX_ARM64: Asset(
                "deno-aarch64-unknown-linux-gnu.zip",
                "c832298b1ad4422481334855f6003e0f54145762c5a134f20a489511d2f65bbf",
                "deno",
            ),
            WINDOWS_X64: Asset(
                "deno-x86_64-pc-windows-msvc.zip",
                "a0c3101b4158d1dfb7d6a78a7bf0f3de80c96bb423c152beec8beb22786f2238",
                "deno.exe",
            ),
        },
    ),
    # REQ-1993: the Python a Python source runs under (python-build-standalone's install_only
    # build; SHA256SUMS in the release) and uv, which installs its dependencies (the asset's
    # .sha256). One CPython version per release of Provisa (decision P3).
    "python": RuntimePin(
        "python",
        "3.12.14+20260924",
        "https://github.com/astral-sh/python-build-standalone/releases/download/20260924/"
        "{filename}",
        {
            DARWIN_ARM64: Asset(
                "cpython-3.12.14%2B20260924-aarch64-apple-darwin-install_only.tar.gz",
                "9763f43db2481a6af36af82ec40302aab7a73632f880129d07a6e81aec846277",
                "python/bin/python3.12",
            ),
            LINUX_X64: Asset(
                "cpython-3.12.14%2B20260924-x86_64-unknown-linux-gnu-install_only.tar.gz",
                "5eae8cf79dd47fc2496a4fccc892936be831ce7a84d984b2299dfb1cdb592682",
                "python/bin/python3.12",
            ),
            LINUX_ARM64: Asset(
                "cpython-3.12.14%2B20260924-aarch64-unknown-linux-gnu-install_only.tar.gz",
                "c0574c9c9fec561eb192ad510e4e79aaad039929b3531b3bb873e2f630fb14a9",
                "python/bin/python3.12",
            ),
            WINDOWS_X64: Asset(
                "cpython-3.12.14%2B20260924-x86_64-pc-windows-msvc-install_only.tar.gz",
                "c5303174bc29f5205decf6721ac549d4eb41c448f9b8c46cbc562d00348865bb",
                "python/python.exe",
            ),
        },
    ),
    "uv": RuntimePin(
        "uv",
        "0.12.18",
        "https://github.com/astral-sh/uv/releases/download/{version}/{filename}",
        {
            DARWIN_ARM64: Asset(
                "uv-aarch64-apple-darwin.tar.gz",
                "cf40e0c6a202190ccd9e0406dcfdd5b2d6668a9a5c779b17948963df32aafe5b",
                "uv-aarch64-apple-darwin/uv",
            ),
            LINUX_X64: Asset(
                "uv-x86_64-unknown-linux-gnu.tar.gz",
                "89eadd7c76fc063887959510d5ba0ab1264dfd5f1143b925ddb73021a40acf16",
                "uv-x86_64-unknown-linux-gnu/uv",
            ),
            WINDOWS_X64: Asset(
                "uv-x86_64-pc-windows-msvc.zip",
                "cae6a3bc25239f83dffb467a4b180508d9da23986c04639ebfa44e43e6a84bff",
                "uv.exe",
            ),
        },
    ),
    # REQ-2098: the Arrow library a Python source's host program sends its rows with, for the
    # pinned CPython above; a wheel is a zip, unpacked onto the host's path (python/source.py).
    # Each sum is the one PyPI publishes for the file.
    "pyarrow": RuntimePin(
        "pyarrow",
        "24.0.0",
        "https://files.pythonhosted.org/packages/{filename}",
        {
            DARWIN_ARM64: Asset(
                "b4/a9/9686d9f07837f91f775e8932659192e02c74f9d8920524b480b85212cc68/"
                "pyarrow-24.0.0-cp312-cp312-macosx_12_0_arm64.whl",
                "6233c9ed9ab9d1db47de57d9753256d9dcffbf42db341576099f0fd9f6bf4810",
                "pyarrow/__init__.py",
            ),
            LINUX_X64: Asset(
                "b3/7a/829f7d9dfd37c207206081d6dad474d81dde29952401f07f2ba507814818/"
                "pyarrow-24.0.0-cp312-cp312-manylinux_2_28_x86_64.whl",
                "6165461f55ef6314f026de6638d661188e3455d3ec49834556a0ebbdbace18bb",
                "pyarrow/__init__.py",
            ),
            LINUX_ARM64: Asset(
                "7c/3b/926382efe8ce27ba729071d3566ade6dfb86bdf112f366000196b2f5780a/"
                "pyarrow-24.0.0-cp312-cp312-manylinux_2_28_aarch64.whl",
                "1617043b99bd33e5318ae18eb2919af09c71322ef1ca46566cdafc6e6712fb66",
                "pyarrow/__init__.py",
            ),
            WINDOWS_X64: Asset(
                "66/1c/e3e72c8014ad2743ca64a701652c733cc5cbcee15c0463a32a8c55518d9e/"
                "pyarrow-24.0.0-cp312-cp312-win_amd64.whl",
                "295f0a7f2e242dabd513737cf076007dc5b2d59237e3eca37b05c0c6446f3826",
                "pyarrow/__init__.py",
            ),
        },
    ),
    # REQ-2098: the Arrow library a Node source's host program sends its rows with: the npm
    # package's self-contained build, the same file on every platform and runtime. The sum is
    # the SHA-256 of the registry's tarball.
    "apache-arrow": RuntimePin(
        "apache-arrow",
        "21.2.0",
        "https://registry.npmjs.org/apache-arrow/-/{filename}",
        {
            DARWIN_ARM64: Asset(
                "apache-arrow-21.2.0.tgz",
                "dd63a3a4c380eddeb00c718740414ff33c4ded015c8720798afdfcc61b5dac41",
                "package/Arrow.es2015.min.js",
            ),
            LINUX_X64: Asset(
                "apache-arrow-21.2.0.tgz",
                "dd63a3a4c380eddeb00c718740414ff33c4ded015c8720798afdfcc61b5dac41",
                "package/Arrow.es2015.min.js",
            ),
            LINUX_ARM64: Asset(
                "apache-arrow-21.2.0.tgz",
                "dd63a3a4c380eddeb00c718740414ff33c4ded015c8720798afdfcc61b5dac41",
                "package/Arrow.es2015.min.js",
            ),
            WINDOWS_X64: Asset(
                "apache-arrow-21.2.0.tgz",
                "dd63a3a4c380eddeb00c718740414ff33c4ded015c8720798afdfcc61b5dac41",
                "package/Arrow.es2015.min.js",
            ),
        },
    ),
}

#: Fetch an archive's bytes. Injected so tests never touch the network.
Fetcher = Callable[[str], bytes]


def fetch_https(url: str) -> bytes:
    """The archive at ``url``, over https only. Fails loud on any error."""
    import httpx

    if not url.startswith("https://"):
        raise RuntimeUnavailable(f"refusing a non-https runtime URL: {url}")
    try:
        response = httpx.get(url, follow_redirects=True, timeout=300.0)
    except httpx.HTTPError as exc:
        raise RuntimeUnavailable(f"failed to download {url}: {exc}") from exc
    if response.status_code != HTTPStatus.OK:
        raise RuntimeUnavailable(f"failed to download {url}: HTTP {response.status_code}")
    return response.content


def _extract(filename: str, data: bytes, dest: Path) -> None:
    """Expand ``data`` under ``dest``, refusing any entry that escapes it."""
    if filename.endswith((".zip", ".whl")):  # a wheel is a zip
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            root = dest.resolve()
            for info in archive.infolist():
                target = (dest / info.filename).resolve()
                if not target.is_relative_to(root):
                    raise RuntimeUnavailable(
                        f"{filename}: entry escapes the archive: {info.filename}"
                    )
            # Every entry was checked above to land inside dest.
            archive.extractall(dest)  # nosec B202
        return
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        archive.extractall(dest, filter="data")  # nosec B202 - the "data" filter refuses every unsafe member


class CodeRuntimeResolver:  # REQ-1984, REQ-1993
    """Resolves a runtime name to its cached executable, downloading and checking on a miss.

    Caches under ``<cache_root>/code/<name>/<version>/``; the executable's presence there is the
    marker of a complete resolution, because a resolution is extracted to a staging directory and
    moved into place whole."""

    def __init__(
        self,
        *,
        cache_root: str | Path | None = None,
        fetcher: Fetcher | None = None,
        host: tuple[str, str] | None = None,
    ) -> None:
        self._root = Path(cache_root) if cache_root is not None else default_cache_root()
        self._fetch = fetcher if fetcher is not None else fetch_https
        self._host = host

    def pin(self, name: str) -> RuntimePin:
        pin = RUNTIMES.get(name)
        if pin is None:
            raise RuntimeUnavailable(f"{name!r} is not a runtime (known: {sorted(RUNTIMES)})")
        return pin

    def home(self, name: str) -> Path:
        """The cached directory of runtime ``name`` at its pinned version."""
        pin = self.pin(name)
        return self._root / "code" / pin.name / pin.version

    def executable(self, name: str) -> Path:
        pin = self.pin(name)
        return self.home(name) / pin.asset(self._host).executable

    def archive(self, name: str) -> tuple[Asset, bytes]:
        """The runtime's archive for this resolver's platform, downloaded and checked, its bytes
        cached unextracted (REQ-2098: an image's layer is written from the archive's own entries,
        never from files extracted here, which another platform's file names may not suit)."""
        pin = self.pin(name)
        asset = pin.asset(self._host)
        cached = self._root / "code" / "archives" / asset.sha256
        if cached.is_file():
            return asset, cached.read_bytes()
        url = pin.url(asset)
        data = self._fetch(url)
        found = hashlib.sha256(data).hexdigest()
        if found != asset.sha256:
            raise RuntimeUnavailable(
                f"{asset.filename} from {url} has SHA-256 {found}, not the pinned {asset.sha256}"
            )
        cached.parent.mkdir(parents=True, exist_ok=True)
        staged = cached.with_name(f".{cached.name}.{os.getpid()}")
        staged.write_bytes(data)
        os.replace(staged, cached)
        return asset, data

    def resolve(self, name: str) -> Path:
        """The runtime's executable, downloaded, checked and cached on a miss."""
        pin = self.pin(name)
        asset = pin.asset(self._host)
        executable = self.executable(name)
        if executable.exists():
            return executable
        url = pin.url(asset)
        data = self._fetch(url)
        found = hashlib.sha256(data).hexdigest()
        if found != asset.sha256:
            raise RuntimeUnavailable(
                f"{asset.filename} from {url} has SHA-256 {found}, not the pinned {asset.sha256}"
            )
        home = self.home(name)
        staging = home.parent / f".{home.name}.{os.getpid()}.staging"
        shutil.rmtree(staging, ignore_errors=True)
        staging.mkdir(parents=True)
        try:
            try:
                _extract(asset.filename, data, staging)
            except (OSError, tarfile.TarError, zipfile.BadZipFile) as exc:
                raise RuntimeUnavailable(f"{asset.filename} could not be extracted: {exc}") from exc
            staged = staging / asset.executable
            if not staged.is_file():
                raise RuntimeUnavailable(f"{asset.filename} holds no {asset.executable}")
            # A zip keeps no permission bits: the executable is made executable here.
            staged.chmod(staged.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
            if executable.exists():  # another process resolved it meanwhile
                return executable
            home.parent.mkdir(parents=True, exist_ok=True)
            os.replace(staging, home)
        finally:
            shutil.rmtree(staging, ignore_errors=True)
        return executable
