# Copyright (c) 2026 Kenneth Stott
# Canary: fcef5ae4-416c-4dcd-bfbe-7ca798c2e331
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On-demand resolution + local caching of the Calcite pgwire connector bundles (REQ-956).

The pgwire bundles (``pgwire-file``, ``pgwire-sharepoint``, ``pgwire-splunk``) are NOT bundled with
Provisa. They are fetched on demand from the PINNED github.com/kenstott/calcite release
(``engine-v0.28.0``) at runtime and cached under the runtime-deps cache root, so Provisa release
cycles are decoupled from the connector releases and the distribution stays lean.

Resolution FAILS LOUD (project rule): an unknown source type, a failed download, or a download that
does not produce the expected ``bin/pgwire-<connector>`` launcher raises ``BundleUnavailable`` — never
a silent fallback to an unpinned version, a partial bundle, or an empty snapshot. A cache HIT (the
launcher already present under the pinned version) never re-downloads.
"""

from __future__ import annotations

import os
import tarfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

# The pinned upstream release the whole bundle set is fetched from (REQ-956). One version knob — a
# bundle path is always namespaced by this tag, so a version bump caches side by side, never in place.
RELEASE_TAG = "engine-v0.28.0"
GITHUB_REPO = "kenstott/calcite"

# Provisa source type -> the connector's pgwire bundle base name. A type absent here has no pgwire
# bundle and fails closed at ``bundle_spec_for`` (never guessed from the type string).
BUNDLE_CONNECTOR: dict[str, str] = {
    "files": "file",
    "sharepoint": "sharepoint",
    "splunk": "splunk",
}


class BundleUnavailable(Exception):  # REQ-956
    """A pgwire bundle could not be resolved — unknown source type, download failure, or a download
    that did not produce the launcher. Raised on fail-loud resolution; never a fallback."""


@dataclass(frozen=True)
class BundleSpec:  # REQ-956 — a pinned (connector, version) coordinate in the upstream release
    connector: str  # "file" | "sharepoint" | "splunk"
    version: str = RELEASE_TAG
    repo: str = GITHUB_REPO

    @property
    def artifact_name(self) -> str:
        """The bundle / launcher base name (``pgwire-file`` …) — also the ``bin/`` launcher filename."""
        return f"pgwire-{self.connector}"

    @property
    def asset_filename(self) -> str:
        """The release asset filename fetched from GitHub (version-stamped tarball)."""
        return f"{self.artifact_name}-{self.version}.tar.gz"

    @property
    def download_url(self) -> str:
        """The pinned GitHub release-asset URL this bundle is fetched from (REQ-956)."""
        return (
            f"https://github.com/{self.repo}/releases/download/{self.version}/{self.asset_filename}"
        )


def bundle_spec_for(source_type: str, *, version: str = RELEASE_TAG) -> BundleSpec:
    """The ``BundleSpec`` for a Provisa source type, pinned to ``version`` (default the release tag).
    A source type with no pgwire bundle fails loud (REQ-956) — never a guessed connector name."""
    connector = BUNDLE_CONNECTOR.get(source_type)
    if connector is None:
        raise BundleUnavailable(
            f"source type {source_type!r} has no pgwire connector bundle "
            f"(known: {sorted(BUNDLE_CONNECTOR)})"
        )
    return BundleSpec(connector, version)


def default_cache_root() -> Path:
    """The runtime-deps cache root: ``$PROVISA_RUNTIME_DEPS_CACHE`` if set, else
    ``~/.cache/provisa/runtime_deps``. Bundles cache under ``<root>/<version>/<artifact_name>``."""
    env = os.environ.get("PROVISA_RUNTIME_DEPS_CACHE")
    if env:
        return Path(env)
    return Path.home() / ".cache" / "provisa" / "runtime_deps"


# Fetch a bundle's release asset and lay it out under ``dest``. Injected at the resolver so tests
# never touch the network; the default is the real GitHub download.
Downloader = Callable[[BundleSpec, Path], None]


def download_release_asset(spec: BundleSpec, dest: Path) -> None:
    """Download ``spec``'s release-asset tarball and extract it into ``dest`` (REQ-956). Fails loud on
    any network / archive error — no partial or silent success. The tarball is expected to contain the
    bundle tree (``bin/pgwire-<connector>``, ``model/`` …); resolution verifies the launcher after."""
    dest.mkdir(parents=True, exist_ok=True)
    if not spec.download_url.startswith("https://"):
        raise BundleUnavailable(f"refusing non-https bundle URL: {spec.download_url}")
    try:
        # nosec B310 - scheme is validated to https just above (no file:/custom-scheme surface).
        with urllib.request.urlopen(spec.download_url) as resp:  # noqa: S310  # nosec B310
            with tarfile.open(fileobj=resp, mode="r|gz") as tar:
                # filter="data" rejects absolute paths, ".." traversal and unsafe links/specials,
                # so a malicious archive cannot escape dest (Python 3.12 safe-extraction filter).
                tar.extractall(dest, filter="data")
    except (OSError, tarfile.TarError) as exc:
        raise BundleUnavailable(
            f"failed to download pgwire bundle {spec.artifact_name} from {spec.download_url}: {exc}"
        ) from exc


class BundleResolver:  # REQ-956
    """Resolves a ``BundleSpec`` to a local, cached bundle directory, downloading on a cache miss.

    Caches under ``<cache_root>/<version>/<artifact_name>``; a HIT (the launcher already present)
    returns immediately with no download. A MISS downloads via the injected ``downloader`` and then
    verifies the launcher exists — a download that does not produce it fails loud (REQ-956)."""

    def __init__(
        self, *, cache_root: str | Path | None = None, downloader: Downloader | None = None
    ) -> None:
        self._cache_root = Path(cache_root) if cache_root is not None else default_cache_root()
        self._download = downloader if downloader is not None else download_release_asset

    def cached_path(self, spec: BundleSpec) -> Path:
        """The version-namespaced bundle directory for ``spec`` (whether or not it exists yet)."""
        return self._cache_root / spec.version / spec.artifact_name

    def launcher_path(self, spec: BundleSpec) -> Path:
        """The bundle's ``bin/pgwire-<connector>`` launcher — the presence marker for a cache hit."""
        return self.cached_path(spec) / "bin" / spec.artifact_name

    def is_cached(self, spec: BundleSpec) -> bool:
        """Whether ``spec`` is already resolved locally (its launcher is present)."""
        return self.launcher_path(spec).exists()

    def bundled_path(self, spec: BundleSpec) -> Path | None:
        """The offline wheel-carried bundle dir for ``spec``, or None (REQ-1153).

        When the optional ``provisa-pgwire-bundles`` wheel is installed it carries the pinned bundles
        under ``<bundle_root>/<version>/<artifact_name>`` — the same layout the cache uses — so an
        enterprise host behind a PyPI/Maven/npm/NuGet-only proxy resolves the connector without the
        github.com/kenstott/calcite release round trip. Returns the dir only if its launcher exists."""
        try:
            from provisa_pgwire_bundles import bundle_root  # type: ignore[import-not-found]
        except ImportError:
            return None
        cand = bundle_root() / spec.version / spec.artifact_name
        return cand if (cand / "bin" / spec.artifact_name).is_file() else None

    def resolve(self, spec: BundleSpec) -> Path:
        """Return the local bundle directory for ``spec``, downloading + caching on a miss (REQ-956).

        A cache hit returns without any download. The offline ``provisa-pgwire-bundles`` wheel is
        preferred over the network next (REQ-1153). Only then does a miss download via the injected
        downloader; if the launcher is still absent afterwards the bundle is unusable and fails loud."""
        dest = self.cached_path(spec)
        if self.is_cached(spec):
            return dest
        bundled = self.bundled_path(spec)
        if bundled is not None:
            return bundled
        self._download(spec, dest)
        if not self.is_cached(spec):
            raise BundleUnavailable(
                f"pgwire bundle {spec.artifact_name} download did not produce launcher "
                f"{self.launcher_path(spec)}"
            )
        return dest
