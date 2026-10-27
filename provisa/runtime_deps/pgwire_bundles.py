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
import platform
import shutil
import tarfile
import tempfile
import threading
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import IO, Callable, cast

# The pinned upstream release the whole bundle set is fetched from (REQ-956). One version knob — a
# bundle path is always namespaced by this tag, so a version bump caches side by side, never in place.
RELEASE_TAG = "engine-v0.122.2"
# Connectors whose bundle comes from a release other than RELEASE_TAG. The one table of such
# overrides; a connector absent here is at RELEASE_TAG. Each entry is removed when RELEASE_TAG
# moves to (or past) its release. Empty: from engine-v0.117.0 no connector's catalog counts the
# rows of every table on the first pg_catalog query (pg_class.reltuples is 0), which held a first
# Salesforce connection for minutes over its 1,110 sObjects, so every bundle is at RELEASE_TAG.
CONNECTOR_RELEASE: dict[str, str] = {}
GITHUB_REPO = "kenstott/calcite"

# The release ships one tarball per OS/arch (REQ-1690): ``pgwire-<connector>-<ver>-<variant>.tar.gz``
# where ``<ver>`` is the tag without its ``engine-v`` prefix. The variant names are the release
# workflow's matrix; a platform absent here has no bundle and fails closed.
BUNDLE_VARIANTS: dict[tuple[str, str], str] = {
    ("Darwin", "arm64"): "macos-arm64",
    ("Linux", "x86_64"): "linux-x86_64",
    ("Windows", "AMD64"): "windows-x86_64",
}

# Provisa source type -> the connector's pgwire bundle base name. A type absent here has no pgwire
# bundle and fails closed at ``bundle_spec_for`` (never guessed from the type string).
BUNDLE_CONNECTOR: dict[str, str] = {
    "files": "file",
    "sharepoint": "sharepoint",
    "salesforce": "salesforce",
    "servicenow": "servicenow",  # REQ-1954
    "jira": "jira",  # REQ-2069
    "cloudops": "cloudops",
    "splunk": "splunk",
    "govdata": "govdata",  # AskAmerica (REQ-540)
}


def bundle_dir_key(connector: str) -> str:
    """The operator setting that names a connector's bundle folder (``connectors.servicenow.
    bundle_dir``): a bundle built for this host, used as it is in place of the pinned release
    (REQ-1954). Declared in ``core/settings_catalog_connectors``."""
    return f"connectors.{connector}.bundle_dir"


def bundle_dir_setting(connector: str) -> str:
    """The environment variable of :func:`bundle_dir_key` (``PROVISA_PGWIRE_BUNDLE_SERVICENOW``)."""
    return f"PROVISA_PGWIRE_BUNDLE_{connector.upper()}"


def named_bundle_dir(connector: str) -> str | None:
    """The bundle folder the operator named for ``connector``, or None for the pinned release."""
    from provisa.core import settings_registry

    return settings_registry.value(bundle_dir_key(connector)) or None


class BundleUnavailable(Exception):  # REQ-956
    """A pgwire bundle could not be resolved — unknown source type, download failure, or a download
    that did not produce the launcher. Raised on fail-loud resolution; never a fallback."""


class BundleDownloading(BundleUnavailable):
    """The bundle is not here yet and is being downloaded off any request: a bundle is hundreds of
    megabytes, and a download inside a request was cut off by the request's deadline every time,
    so it never arrived. The request is answered now; a later one finds the bundle."""

    def __init__(self, spec: "BundleSpec") -> None:
        super().__init__(
            f"the {spec.artifact_name} connector is being downloaded ({spec.version}); "
            "try again shortly"
        )
        self.spec = spec


#: Downloads in flight off any request, by destination, and the failure of the last one that
#: ended badly (reported to the next request, which then starts another).
_downloads: dict[Path, threading.Thread] = {}
_download_failures: dict[Path, BundleUnavailable] = {}
_downloads_lock = threading.Lock()


@dataclass(frozen=True)
class BundleSpec:  # REQ-956 — a pinned (connector, version) coordinate in the upstream release
    connector: str  # "file" | "sharepoint" | "splunk" | "salesforce"
    version: str = RELEASE_TAG
    repo: str = GITHUB_REPO
    variant: str = ""  # the OS/arch tarball variant; "" ⇒ this host's (see bundle_variant)

    @property
    def artifact_name(self) -> str:
        """The bundle / launcher base name (``pgwire-file`` …) — also the ``bin/`` launcher filename."""
        return f"pgwire-{self.connector}"

    @property
    def asset_stem(self) -> str:
        """The tarball's top-level directory — ``pgwire-file-0.82.1-macos-arm64`` — which is also
        the asset filename without ``.tar.gz``."""
        ver = self.version.removeprefix("engine-v")
        return f"{self.artifact_name}-{ver}-{self.variant or bundle_variant()}"

    @property
    def asset_filename(self) -> str:
        """The release asset filename fetched from GitHub (version- and platform-stamped tarball)."""
        return f"{self.asset_stem}.tar.gz"

    @property
    def download_url(self) -> str:
        """The pinned GitHub release-asset URL this bundle is fetched from (REQ-956)."""
        return (
            f"https://github.com/{self.repo}/releases/download/{self.version}/{self.asset_filename}"
        )


def bundle_variant() -> str:
    """This host's release tarball variant (REQ-1690). A platform the release does not build for
    has no bundle — fail loud rather than fetch a tarball that cannot run here."""
    key = (platform.system(), platform.machine())
    variant = BUNDLE_VARIANTS.get(key)
    if variant is None:
        raise BundleUnavailable(
            f"no pgwire bundle is published for {key[0]}/{key[1]} (built: {sorted(BUNDLE_VARIANTS.values())})"
        )
    return variant


def connector_release(connector: str) -> str:
    """The release ``connector``'s bundle is fetched from: its entry in ``CONNECTOR_RELEASE``,
    else ``RELEASE_TAG``."""
    return CONNECTOR_RELEASE.get(connector, RELEASE_TAG)


def bundle_spec_for(source_type: str, *, version: str | None = None) -> BundleSpec:
    """The ``BundleSpec`` for a Provisa source type, pinned to ``version`` (default the connector's
    own release, ``connector_release``). A source type with no pgwire bundle fails loud (REQ-956)
    — never a guessed connector name."""
    connector = BUNDLE_CONNECTOR.get(source_type)
    if connector is None:
        raise BundleUnavailable(
            f"source type {source_type!r} has no pgwire connector bundle "
            f"(known: {sorted(BUNDLE_CONNECTOR)})"
        )
    return BundleSpec(connector, version if version is not None else connector_release(connector))


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
    import httpx

    if not spec.download_url.startswith("https://"):
        raise BundleUnavailable(f"refusing non-https bundle URL: {spec.download_url}")
    # The tarball nests everything under ``<asset_stem>/``; extract beside ``dest`` into a staging
    # directory of this download's own and rename that directory into place whole, so the bundle
    # root is ``dest`` (``dest/bin/pgwire-<connector>``) and a reader never sees it half laid down.
    dest.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{dest.name}.", suffix=".download", dir=dest.parent))
    try:
        with httpx.stream("GET", spec.download_url, follow_redirects=True, timeout=120.0) as resp:
            if resp.status_code != HTTPStatus.OK:
                raise BundleUnavailable(
                    f"failed to download pgwire bundle {spec.artifact_name} from "
                    f"{spec.download_url}: HTTP {resp.status_code}"
                )
            with tarfile.open(
                fileobj=cast("IO[bytes]", _StreamReader(resp.iter_bytes())), mode="r|gz"
            ) as tar:
                # filter="data" rejects absolute paths, ".." traversal and unsafe links/specials,
                # so a malicious archive cannot escape staging (Python 3.12 safe-extraction filter).
                tar.extractall(staging, filter="data")
        root = staging / spec.asset_stem
        if not root.is_dir():
            raise BundleUnavailable(
                f"pgwire bundle {spec.asset_filename} does not contain a {spec.asset_stem}/ directory"
            )
        if dest.exists():
            # Left by a download that did not finish (it has no launcher, or this one would not
            # run): the resolver holds the bundle's lock, so nothing reads it.
            shutil.rmtree(dest)
        os.rename(root, dest)
    except (OSError, httpx.HTTPError, tarfile.TarError) as exc:
        raise BundleUnavailable(
            f"failed to download pgwire bundle {spec.artifact_name} from {spec.download_url}: {exc}"
        ) from exc
    finally:
        shutil.rmtree(staging, ignore_errors=True)


class _StreamReader:
    """A read()-only file object over an httpx byte iterator, for streaming tarfile extraction."""

    def __init__(self, chunks) -> None:
        self._chunks = iter(chunks)
        self._buf = b""

    def read(self, n: int = -1) -> bytes:
        while n < 0 or len(self._buf) < n:
            chunk = next(self._chunks, None)
            if chunk is None:
                break
            self._buf += chunk
        if n < 0:
            out, self._buf = self._buf, b""
        else:
            out, self._buf = self._buf[:n], self._buf[n:]
        return out


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
        """The version-namespaced bundle directory for ``spec`` (whether or not it exists yet), or
        the directory its connector's setting names (:func:`bundle_dir_key`)."""
        named = named_bundle_dir(spec.connector)
        if named:
            return Path(named)
        return self._cache_root / spec.version / spec.artifact_name

    def launcher_path(self, spec: BundleSpec) -> Path:
        """The bundle's ``bin/pgwire-<connector>`` launcher — the presence marker for a cache hit."""
        return self.cached_path(spec) / "bin" / spec.artifact_name

    def is_cached(self, spec: BundleSpec) -> bool:
        """Whether ``spec`` is already resolved locally (its launcher is present)."""
        return self.launcher_path(spec).exists()

    def resolve(self, spec: BundleSpec) -> Path:
        """Return the local bundle directory for ``spec``, downloading + caching on a miss (REQ-956).
        A bundle its connector's setting names (:func:`bundle_dir_key`) is returned as it is,
        or refused when it has no launcher; the pinned release is then never fetched.

        A cache hit returns without any download. A miss downloads via the injected downloader; if the
        launcher is still absent afterwards the bundle is unusable and this fails loud."""
        dest = self.cached_path(spec)
        if self.is_cached(spec):
            return dest
        if named_bundle_dir(spec.connector):
            # A bundle named explicitly is used as it is: nothing is downloaded in its place.
            raise BundleUnavailable(
                f"setting {bundle_dir_key(spec.connector)} names {dest}, which has no launcher "
                f"{self.launcher_path(spec)}"
            )
        from provisa.core import request_deadline

        if request_deadline.remaining() is not None:
            self._download_off_request(spec, dest)
        self._fetch(spec, dest)
        return dest

    def _fetch(self, spec: BundleSpec, dest: Path) -> None:
        """Download ``spec`` into ``dest`` once among every thread and process of this host: under
        the bundle's own lock file, and only when it is still missing once the lock is held. A
        fetch that waited for another's finds the bundle that one laid down."""
        from provisa.core.host_lock import FileLock

        lock = FileLock(dest.parent / f".{dest.name}.lock")
        lock.acquire()
        try:
            if not self.is_cached(spec):
                self._download(spec, dest)
        finally:
            lock.release()
        if not self.is_cached(spec):
            raise BundleUnavailable(
                f"pgwire bundle {spec.artifact_name} download did not produce launcher "
                f"{self.launcher_path(spec)}"
            )

    def _download_off_request(self, spec: BundleSpec, dest: Path) -> None:
        """Inside a request: start the download on a thread of its own (one per bundle), outside
        the request's deadline, and answer :class:`BundleDownloading` now. A download that failed
        is reported once, to the request after it, and the next request starts another."""
        with _downloads_lock:
            failed = _download_failures.pop(dest, None)
            if failed is not None:
                raise failed
            if dest not in _downloads:

                def run() -> None:
                    try:
                        self._fetch(spec, dest)
                    except BundleUnavailable as exc:
                        with _downloads_lock:
                            _download_failures[dest] = exc
                    finally:
                        with _downloads_lock:
                            _downloads.pop(dest, None)

                # A new thread starts with an empty context: no request deadline is bound in it.
                thread = threading.Thread(
                    target=run, daemon=True, name=f"pgwire-bundle:{spec.artifact_name}"
                )
                _downloads[dest] = thread
                thread.start()
        raise BundleDownloading(spec)
