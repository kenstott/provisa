# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8a1c0d-7b3f-4d29-9e51-6a2f8c3b1d70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Fetch a pgwire connector bundle from the configured Maven repository (REQ-294).

Each connector is a Maven Central artifact per platform, ``io.simpleishard:pgwire-<connector>-<variant>``,
holding what is that connector's alone. The files the bundles have in common (the JRE, the Calcite
jars) are one artifact per platform, ``pgwire-shared-<variant>``, stored by content: every connector
tarball carries a ``.provisa-shared.json`` naming the shared files it needs and where. A connector's
files are then laid out with hard links into the shared store, so a machine holds each file once.
The artifacts are built, signed and published by the calcite engine release (REQ-294, amended
2026-11-10); Provisa builds none and reads them at the engine version it pins.

The repository is the machine's: ``PROVISA_MAVEN_REPOSITORY``, else the mirror of ``central`` in
``~/.m2/settings.xml`` (an enterprise Artifactory), else Maven Central.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from http import HTTPStatus
from pathlib import Path, PurePosixPath

from defusedxml.ElementTree import parse as parse_xml

from provisa.core.host_lock import FileLock
from provisa.runtime_deps.pgwire_bundles import BundleSpec, BundleUnavailable, bundle_variant

GROUP_PATH = "io/simpleishard"
CENTRAL = "https://repo1.maven.org/maven2"
REPOSITORY_ENV = "PROVISA_MAVEN_REPOSITORY"
#: The manifest in a connector tarball: [{"path", "object", "mode"}], the shared files it needs.
SHARED_MANIFEST = ".provisa-shared.json"
SHARED_ARTIFACT = "pgwire-shared"
#: The directory under ``<cache>/<version>`` that holds the shared files of one platform.
_SHARED_STORE = ".shared-{variant}"
_M2 = "{http://maven.apache.org/SETTINGS/1.0.0}"


def repository(settings: Path | None = None) -> str:
    """The Maven repository this machine fetches from, without a trailing slash."""
    named = os.environ.get(REPOSITORY_ENV)
    if named:
        return named.rstrip("/")
    path = settings if settings is not None else Path.home() / ".m2" / "settings.xml"
    if path.is_file():
        root = parse_xml(path).getroot()
        if root is None:
            raise BundleUnavailable(f"{path} is an empty Maven settings file")
        for mirror in root.iter(f"{_M2}mirror"):
            of = (mirror.findtext(f"{_M2}mirrorOf") or "").split(",")
            url = mirror.findtext(f"{_M2}url")
            if url and {"*", "central"} & {entry.strip() for entry in of}:
                return url.strip().rstrip("/")
    return CENTRAL


def artifact_url(artifact: str, version: str, repo: str | None = None) -> str:
    """The URL of ``<artifact>-<version>.tar.gz`` in the repository."""
    root = repo if repo is not None else repository()
    return f"{root}/{GROUP_PATH}/{artifact}/{version}/{artifact}-{version}.tar.gz"


def _download(url: str, out: Path) -> None:
    """Fetch ``url`` into ``out`` and check it against the ``.sha256`` published beside it."""
    import httpx

    try:
        digest = httpx.get(f"{url}.sha256", follow_redirects=True, timeout=120.0)
        if digest.status_code != HTTPStatus.OK:
            raise BundleUnavailable(f"{url}.sha256: HTTP {digest.status_code}")
        expected = digest.text.split()[0].strip()
        sha = hashlib.sha256()
        with httpx.stream("GET", url, follow_redirects=True, timeout=120.0) as resp:
            if resp.status_code != HTTPStatus.OK:
                raise BundleUnavailable(f"{url}: HTTP {resp.status_code}")
            with out.open("wb") as sink:
                for chunk in resp.iter_bytes():
                    sha.update(chunk)
                    sink.write(chunk)
    except httpx.HTTPError as exc:
        raise BundleUnavailable(f"could not fetch {url}: {exc}") from exc
    if sha.hexdigest() != expected:
        raise BundleUnavailable(f"{url} does not match its published sha256")


def _extract(archive: Path, into: Path) -> None:
    with tarfile.open(archive, "r:gz") as tar:
        # filter="data" rejects paths and links outside ``into`` (Python 3.12 safe extraction).
        tar.extractall(into, filter="data")


def _shared_store(spec: BundleSpec, dest: Path, repo: str) -> Path:
    """The shared files of ``spec``'s platform, fetched once per version however many connectors
    need them: under a lock of their own, and only when still missing once it is held."""
    variant = spec.variant or bundle_variant()
    store = dest.parent / _SHARED_STORE.format(variant=variant)
    version = spec.version.removeprefix("engine-v")
    lock = FileLock(dest.parent / f"{store.name}.lock")
    lock.acquire()
    try:
        if store.is_dir():
            return store
        with tempfile.TemporaryDirectory(dir=dest.parent, prefix=f"{store.name}.") as work:
            archive = Path(work) / "shared.tar.gz"
            _download(artifact_url(f"{SHARED_ARTIFACT}-{variant}", version, repo), archive)
            unpacked = Path(work) / "store"
            unpacked.mkdir()
            _extract(archive, unpacked)
            os.rename(unpacked, store)
    finally:
        lock.release()
    return store


def _link_shared(root: Path, store: Path) -> None:
    """Lay out the shared files ``root``'s manifest names, each a hard link into ``store``."""
    manifest = root / SHARED_MANIFEST
    if not manifest.is_file():
        raise BundleUnavailable(f"{root.name} has no {SHARED_MANIFEST}")
    for entry in json.loads(manifest.read_text()):
        rel = PurePosixPath(entry["path"])
        if rel.is_absolute() or ".." in rel.parts:
            raise BundleUnavailable(f"{root.name}: unsafe shared path {entry['path']!r}")
        source = store / "objects" / str(entry["object"])
        if not source.is_file():
            raise BundleUnavailable(f"{root.name}: shared file {entry['object']} is not in {store}")
        out = root.joinpath(*rel.parts)
        out.parent.mkdir(parents=True, exist_ok=True)
        os.link(source, out)
    manifest.unlink()


def download_maven_bundle(spec: BundleSpec, dest: Path) -> None:
    """Fetch ``spec`` from the Maven repository and lay it out under ``dest``, whole or not at all."""
    variant = spec.variant or bundle_variant()
    version = spec.version.removeprefix("engine-v")
    repo = repository()
    dest.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{dest.name}.", suffix=".download", dir=dest.parent))
    try:
        store = _shared_store(spec, dest, repo)
        archive = staging / "bundle.tar.gz"
        _download(artifact_url(f"{spec.artifact_name}-{variant}", version, repo), archive)
        _extract(archive, staging)
        root = staging / spec.asset_stem
        if not root.is_dir():
            raise BundleUnavailable(f"{archive.name} does not contain {spec.asset_stem}/")
        _link_shared(root, store)
        if dest.exists():
            shutil.rmtree(dest)  # left by a fetch that did not finish; the resolver holds its lock
        os.rename(root, dest)
    except (OSError, tarfile.TarError) as exc:
        raise BundleUnavailable(f"could not stage {spec.artifact_name}: {exc}") from exc
    finally:
        shutil.rmtree(staging, ignore_errors=True)
