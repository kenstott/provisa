# Copyright (c) 2026 Kenneth Stott
# Canary: fcef5ae4-416c-4dcd-bfbe-7ca798c2e331
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Local resolution of the Calcite pgwire connector bundles (REQ-956, REQ-294).

The pgwire bundles are NOT bundled with Provisa and a running Provisa never fetches one: AIR-GAPPED
IS LOCAL FIRST. A bundle is located locally (the folder its connector's setting names, else the
runtime-deps cache root) and was staged there at build or packaging time, by
``python -m provisa.runtime_deps.stage_pgwire_bundles``, from the PINNED github.com/kenstott/calcite
release (``RELEASE_TAG``), so Provisa release cycles stay decoupled from the connector releases.

Resolution FAILS LOUD (project rule): an unknown source type, or a bundle that is not staged,
raises ``BundleUnavailable`` (``BundleNotStaged`` for the latter, naming the bundle and the fix) --
never a download, a fallback to an unpinned version, a partial bundle, or an empty snapshot.
``python -m provisa.runtime_deps.pgwire_bundles`` is the install-time check that every bundle is staged;
``--except <connector>`` names a bundle deliberately left to a later staging step.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import platform
import shutil
import sys
import tarfile
import tempfile
import threading
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path
from typing import Callable, Iterable

# The pinned upstream release the whole bundle set is fetched from (REQ-956). One version knob — a
# bundle path is always namespaced by this tag, so a version bump caches side by side, never in place.
RELEASE_TAG = "engine-v0.130.0"
# Connectors whose bundle comes from a release other than RELEASE_TAG. The one table of such
# overrides; a connector absent here is at RELEASE_TAG. Each entry is removed when RELEASE_TAG
# moves to (or past) its release. Empty: from engine-v0.117.0 no connector's catalog counts the
# rows of every table on the first pg_catalog query (pg_class.reltuples is 0), which held a first
# Salesforce connection for minutes over its 1,110 sObjects, so every bundle is at RELEASE_TAG.
CONNECTOR_RELEASE: dict[str, str] = {}
GITHUB_REPO = "kenstott/calcite"

# The release ships one tarball per OS/arch (REQ-1690): ``pgwire-<connector>-<ver>-<variant>.tar.gz``
# where ``<ver>`` is the tag without its ``engine-v`` prefix. The variant names are the release
# workflow's matrix; a platform absent here has no bundle and fails closed. The supported platforms
# (REQ-294, amended 2026-11-10): macOS arm64, Linux x86_64, Linux arm64 and Windows x86_64. The
# calcite engine release builds, signs and publishes every bundle; Provisa only reads them.
BUNDLE_VARIANTS: dict[tuple[str, str], str] = {
    ("Darwin", "arm64"): "macos-arm64",
    ("Linux", "x86_64"): "linux-x86_64",
    ("Linux", "aarch64"): "linux-aarch64",
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


#: How to put a bundle in place: the one stage command. Named in every refusal.
STAGE_FIX = (
    "run `python -m provisa.runtime_deps.stage_assets` once on a machine that reaches your Maven "
    "repository (PROVISA_RUNTIME_DEPS_CACHE names the directory, and a directory already filled "
    "is used without fetching), or name a built bundle with the connector's bundle_dir setting"
)


class BundleNotStaged(BundleUnavailable):
    """A pgwire bundle is not where the runtime looks, and the runtime never fetches one (REQ-294).
    An install defect, named with the bundle, where it was looked for, and the fix."""

    def __init__(self, spec: "BundleSpec", where: Path) -> None:
        super().__init__(
            f"the {spec.artifact_name} bundle ({spec.version}) is not staged: no launcher under "
            f"{where}. {STAGE_FIX}"
        )
        self.spec = spec


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

    @property
    def checksum_filename(self) -> str:
        """The release asset holding the tarball's SHA-256 (``<asset_filename>.sha256``)."""
        return f"{self.asset_filename}.sha256"

    @property
    def checksum_url(self) -> str:
        """The URL of :attr:`checksum_filename` in the same pinned release."""
        return (
            f"https://github.com/{self.repo}/releases/download/{self.version}/"
            f"{self.checksum_filename}"
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


def prune_releases(cache_root: Path | None = None) -> list[str]:
    """Delete from the cache each engine release no connector here is pinned to, and return their
    names. Each RELEASE_TAG bump caches beside the last: six releases held 13 GB of the sandbox
    VM's 49 GB disk (2026-10-31). Only a deployment with one Provisa on its cache runs this: a
    developer's is shared by worktrees at other releases."""
    root = cache_root if cache_root is not None else default_cache_root()
    if not root.is_dir():
        return []
    kept = {RELEASE_TAG, *CONNECTOR_RELEASE.values()}
    gone = sorted(p for p in root.glob("engine-v*") if p.is_dir() and p.name not in kept)
    for release in gone:
        shutil.rmtree(release)
    return [release.name for release in gone]


# Fetch a bundle's release asset and lay it out under ``dest``. Injected at the resolver so tests
# never touch the network; the default is the real GitHub download.
Downloader = Callable[[BundleSpec, Path], None]


def download_release_asset(spec: BundleSpec, dest: Path) -> None:
    """Download ``spec``'s release-asset tarball, verify it against the release's ``.sha256``
    asset, and extract it into ``dest`` (REQ-956). Fails loud on any network / archive error, a
    missing ``.sha256`` asset, or a checksum mismatch — no partial or silent success, and no path
    that skips verification. The tree is laid down beside ``dest`` and renamed into place whole,
    only once it is fully extracted and holds the ``bin/pgwire-<connector>`` launcher."""
    import httpx

    if not spec.download_url.startswith("https://"):
        raise BundleUnavailable(f"refusing non-https bundle URL: {spec.download_url}")
    # Everything happens in a staging directory of this download's own beside ``dest``: the
    # tarball, its extraction, and the launcher check. A cut-off download, a tampered tarball or an
    # interrupted extraction leaves nothing at ``dest`` and no staging behind.
    dest.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{dest.name}.", suffix=".download", dir=dest.parent))
    try:
        expected = _expected_sha256(spec)
        tarball = staging / spec.asset_filename
        found = _download_to(spec.download_url, tarball, spec)
        if found != expected:
            raise BundleUnavailable(
                f"pgwire bundle {spec.asset_filename} from {spec.download_url} has SHA-256 "
                f"{found}, not the {expected} its release publishes in {spec.checksum_url}"
            )
        extracted = staging / "extract"
        with tarfile.open(tarball, mode="r:gz") as tar:
            # filter="data" rejects absolute paths, ".." traversal and unsafe links/specials,
            # so a malicious archive cannot escape staging (Python 3.12 safe-extraction filter).
            tar.extractall(extracted, filter="data")
        root = extracted / spec.asset_stem
        if not root.is_dir():
            raise BundleUnavailable(
                f"pgwire bundle {spec.asset_filename} does not contain a {spec.asset_stem}/ directory"
            )
        if not (root / "bin" / spec.artifact_name).is_file():
            raise BundleUnavailable(
                f"pgwire bundle {spec.asset_filename} does not contain launcher "
                f"{spec.asset_stem}/bin/{spec.artifact_name}"
            )
        if dest.exists():
            # Left by an earlier install with no launcher: the resolver holds the bundle's lock,
            # so nothing reads it.
            shutil.rmtree(dest)
        os.rename(root, dest)
    except (OSError, httpx.HTTPError, tarfile.TarError, EOFError) as exc:
        raise BundleUnavailable(
            f"failed to download pgwire bundle {spec.artifact_name} from {spec.download_url}: {exc}"
        ) from exc
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def _expected_sha256(spec: BundleSpec) -> str:
    """The SHA-256 the release publishes for ``spec``'s tarball, from its ``.sha256`` asset (the
    hex digest, optionally followed by the filename). A missing or malformed asset is refused by
    name: a bundle is never installed unverified."""
    import httpx

    with httpx.stream("GET", spec.checksum_url, follow_redirects=True, timeout=120.0) as resp:
        if resp.status_code != HTTPStatus.OK:
            raise BundleUnavailable(
                f"pgwire bundle {spec.asset_filename} has no checksum asset "
                f"{spec.checksum_filename} in release {spec.version} ({spec.checksum_url}: "
                f"HTTP {resp.status_code}); refusing to install it unverified"
            )
        text = b"".join(resp.iter_bytes()).decode("ascii", errors="replace")
    return sha256_digest(text, f"checksum asset {spec.checksum_filename} in release {spec.version}")


def sha256_digest(text: str, where: str) -> str:
    """The hex digest a ``.sha256`` file holds (the digest, optionally followed by the filename).
    Anything else -- an empty body, an HTML error page -- is refused, naming ``where``."""
    fields = text.split()
    digest = fields[0].lower() if fields else ""
    if len(digest) != _SHA256_HEX_LEN or any(c not in "0123456789abcdef" for c in digest):
        raise BundleUnavailable(f"{where} does not hold a SHA-256 digest: {text[:100]!r}")
    return digest


def _download_to(url: str, path: Path, spec: BundleSpec) -> str:
    """Stream ``url`` into ``path`` and return the SHA-256 hex digest of what arrived."""
    import httpx

    digest = hashlib.sha256()
    with httpx.stream("GET", url, follow_redirects=True, timeout=120.0) as resp:
        if resp.status_code != HTTPStatus.OK:
            raise BundleUnavailable(
                f"failed to download pgwire bundle {spec.artifact_name} from {url}: "
                f"HTTP {resp.status_code}"
            )
        with path.open("wb") as out:
            for chunk in resp.iter_bytes():
                digest.update(chunk)
                out.write(chunk)
    return digest.hexdigest()


_SHA256_HEX_LEN = 64


class BundleResolver:  # REQ-956
    """Resolves a ``BundleSpec`` to a local, staged bundle directory.

    Looks under ``<cache_root>/<version>/<artifact_name>``; a HIT (the launcher present) returns
    immediately. A MISS raises ``BundleNotStaged`` (REQ-294): nothing is downloaded, unless a
    ``downloader`` is injected, which only the staging step (and a test) does, then verifies the
    launcher exists — a download that does not produce it fails loud (REQ-956)."""

    def __init__(
        self, *, cache_root: str | Path | None = None, downloader: Downloader | None = None
    ) -> None:
        self._cache_root = Path(cache_root) if cache_root is not None else default_cache_root()
        self._download = downloader

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
        """Return the local bundle directory for ``spec`` (REQ-956). A bundle its connector's
        setting names (:func:`bundle_dir_key`) is returned as it is, or refused when it has no
        launcher; the pinned release is then never fetched.

        A hit returns at once. A miss raises ``BundleNotStaged`` (REQ-294) when no downloader was
        injected; with one, it downloads, and if the launcher is still absent afterwards the bundle
        is unusable and this fails loud."""
        dest = self.cached_path(spec)
        if self.is_cached(spec):
            return dest
        if named_bundle_dir(spec.connector):
            # A bundle named explicitly is used as it is: nothing is downloaded in its place.
            raise BundleUnavailable(
                f"setting {bundle_dir_key(spec.connector)} names {dest}, which has no launcher "
                f"{self.launcher_path(spec)}"
            )
        if self._download is None:
            raise BundleNotStaged(spec, dest)
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
            if not self.is_cached(spec) and self._download is not None:
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


def checked_connectors(excepted: Iterable[str] = ()) -> list[str]:
    """Every connector with a bundle, less ``excepted``, in order. A name that is no connector is
    refused: a typo must not read as a passing check."""
    connectors = set(BUNDLE_CONNECTOR.values())
    unknown = sorted(set(excepted) - connectors)
    if unknown:
        raise BundleUnavailable(
            f"--except names no connector: {', '.join(unknown)} (known: {sorted(connectors)})"
        )
    return sorted(connectors - set(excepted))


def missing_bundles(
    cache_root: str | Path | None = None, excepted: Iterable[str] = ()
) -> list[BundleSpec]:
    """The bundle of every connector, less ``excepted``, that is not staged (its launcher is
    absent), once each, in connector order. Honors a connector's ``bundle_dir`` setting as the
    resolver does."""
    resolver = BundleResolver(cache_root=cache_root)
    connectors = checked_connectors(excepted)
    source_types = {c: t for t, c in BUNDLE_CONNECTOR.items()}
    specs = [bundle_spec_for(source_types[c]) for c in connectors]
    return [spec for spec in specs if not resolver.is_cached(spec)]


def verify_staged(cache_root: str | Path | None = None, excepted: Iterable[str] = ()) -> None:
    """REQ-294, the install-time check: every connector's bundle but ``excepted`` is staged. Raises
    ``BundleNotStaged`` naming each one that is not."""
    root = Path(cache_root) if cache_root is not None else default_cache_root()
    absent = missing_bundles(root, excepted)
    if absent:
        refused = BundleNotStaged(absent[0], root)
        names = ", ".join(f"{s.artifact_name} ({s.version})" for s in absent)
        refused.args = (f"pgwire bundles not staged under {root}: {names}. {STAGE_FIX}",)
        raise refused


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check that the pgwire bundles are staged.")
    parser.add_argument(
        "--except",
        dest="excepted",
        action="append",
        default=[],
        metavar="CONNECTOR",
        help="a connector whose bundle is left to a later staging step; repeat for more",
    )
    excepted: list[str] = parser.parse_args(argv).excepted
    try:
        verify_staged(excepted=excepted)
        count = len(checked_connectors(excepted))
    except BundleUnavailable as refused:
        print(f"pgwire bundle check failed: {refused}", file=sys.stderr)
        return 1
    print(
        f"{count} pgwire bundles staged"
        + (f"; excepted: {', '.join(excepted)}" if excepted else "")
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
