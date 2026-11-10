#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 2a7d0f64-93c1-4e5b-8c47-f16b3a9e0d52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Lay out the pgwire connector bundles of one engine release for Maven Central (REQ-294).

The per-platform release tarballs are 7+ bundles sharing most of their bytes (the JRE, the Calcite
jars). A file identical in two or more bundles of a platform goes, once, into that platform's
``pgwire-shared-<variant>`` artifact, stored by content; each connector artifact keeps what is its own
and a ``.provisa-shared.json`` naming the shared files to link back in
(provisa/runtime_deps/maven_bundles.py is the reader). Publishes nothing: it writes one Central
bundle directory per artifact and ``sizes.json``; .github/workflows/pgwire-maven-bundles.yml signs and
zips them, and the Maven Central promote is a separate, confirmed step.

Usage: pgwire_maven_bundles.py <tarball_dir> <out_dir> <version>
  <tarball_dir> holds ``pgwire-<connector>-<version>-<variant>.tar.gz``.
"""

from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

GROUP = "io.simpleishard"
SHARED = "pgwire-shared"
MANIFEST = ".provisa-shared.json"
#: Below this a shared file saves less than its manifest entry costs.
MIN_SHARED_BYTES = 4096
_POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>{group}</groupId>
  <artifactId>{artifact}</artifactId>
  <version>{version}</version>
  <packaging>tar.gz</packaging>
  <name>{artifact}</name>
  <description>{description}</description>
  <url>https://github.com/kenstott/calcite</url>
  <licenses>
    <license>
      <name>Apache License, Version 2.0</name>
      <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <developers>
    <developer><name>Kenneth Stott</name></developer>
  </developers>
  <scm>
    <url>https://github.com/kenstott/calcite</url>
    <connection>scm:git:https://github.com/kenstott/calcite.git</connection>
  </scm>
</project>
"""


def _digest(path: Path) -> str:
    with path.open("rb") as content:
        return hashlib.file_digest(content, "sha256").hexdigest()


def _object_name(path: Path) -> str:
    """The file's name in the shared store: content and mode, since a hard link shares its mode."""
    return f"{_digest(path)}-{path.stat().st_mode & 0o777:o}"


def _sharable(root: Path):
    for path in sorted(root.rglob("*")):
        if path.is_file() and not path.is_symlink() and path.stat().st_size >= MIN_SHARED_BYTES:
            yield path


def split(unpacked: dict[str, Path]) -> dict[str, list[dict[str, str]]]:
    """connector -> the shared files its bundle needs, each named by its object: those identical in
    two or more of the ``unpacked`` bundle trees."""
    seen: dict[str, set[str]] = {}
    for connector, root in unpacked.items():
        for path in _sharable(root):
            seen.setdefault(_object_name(path), set()).add(connector)
    entries: dict[str, list[dict[str, str]]] = {c: [] for c in unpacked}
    for connector, root in unpacked.items():
        for path in _sharable(root):
            name = _object_name(path)
            if len(seen[name]) >= 2:
                entries[connector].append(
                    {"path": path.relative_to(root).as_posix(), "object": name}
                )
    return entries


def write_shared(unpacked: dict[str, Path], entries: dict[str, list[dict[str, str]]], out: Path):
    """The shared artifact: ``objects/<name>``, one file for each name any bundle needs."""
    written: set[str] = set()
    with tarfile.open(out, "w:gz", compresslevel=6) as tar:
        for connector, root in unpacked.items():
            for entry in entries[connector]:
                if entry["object"] not in written:
                    written.add(entry["object"])
                    tar.add(root / entry["path"], arcname=f"objects/{entry['object']}")


def write_connector(root: Path, stem: str, entries: list[dict[str, str]], out: Path) -> None:
    """A connector's artifact: its tree under ``<stem>/`` without the shared files, plus the manifest."""
    shared = {entry["path"] for entry in entries}
    with tarfile.open(out, "w:gz", compresslevel=6) as tar:
        for path in sorted(root.rglob("*")):
            rel = path.relative_to(root).as_posix()
            if rel not in shared:
                tar.add(path, arcname=f"{stem}/{rel}", recursive=False)
        body = json.dumps(entries, indent=0).encode()
        info = tarfile.TarInfo(f"{stem}/{MANIFEST}")
        info.size = len(body)
        tar.addfile(info, io.BytesIO(body))


def _placeholder_jar(out: Path, artifact: str, kind: str) -> None:
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as jar:
        jar.writestr(
            "README.txt",
            f"{artifact} is a packaged runtime bundle with no Java API of its own; its {kind} are "
            "those of the engine release it is built from: https://github.com/kenstott/calcite\n",
        )


def central_dir(artifact: str, version: str, tarball: Path, description: str, out: Path) -> Path:
    """One Central bundle directory: the tarball, its pom, sources and javadoc jars, sha256."""
    base = out / artifact / GROUP.replace(".", "/") / artifact / version
    base.mkdir(parents=True)
    stem = f"{artifact}-{version}"
    shutil.move(tarball, base / f"{stem}.tar.gz")
    (base / f"{stem}.pom").write_text(
        _POM.format(group=GROUP, artifact=artifact, version=version, description=description)
    )
    _placeholder_jar(base / f"{stem}-sources.jar", artifact, "sources")
    _placeholder_jar(base / f"{stem}-javadoc.jar", artifact, "javadoc")
    for name in (f"{stem}.tar.gz", f"{stem}.pom", f"{stem}-sources.jar", f"{stem}-javadoc.jar"):
        (base / f"{name}.sha256").write_text(_digest(base / name) + "\n")
    return base / f"{stem}.tar.gz"


def build_variant(tarballs: dict[str, Path], version: str, variant: str, out: Path) -> dict:
    """Split one platform's bundles and lay out their Central directories; return their sizes."""
    work = Path(tempfile.mkdtemp(prefix=".split.", dir=out))
    try:
        unpacked: dict[str, Path] = {}
        for connector, tarball in sorted(tarballs.items()):
            stem = f"pgwire-{connector}-{version}-{variant}"
            with tarfile.open(tarball, "r:gz") as tar:
                tar.extractall(work / connector, filter="data")
            unpacked[connector] = work / connector / stem
        entries = split(unpacked)
        sizes: dict[str, int] = {}
        shared_out = work / "shared.tar.gz"
        write_shared(unpacked, entries, shared_out)
        artifact = f"{SHARED}-{variant}"
        sizes[artifact] = (
            central_dir(
                artifact,
                version,
                shared_out,
                f"Files shared by the pgwire bundles ({variant})",
                out,
            )
            .stat()
            .st_size
        )
        for connector, root in unpacked.items():
            artifact = f"pgwire-{connector}-{variant}"
            built = work / f"{connector}.tar.gz"
            write_connector(
                root, f"pgwire-{connector}-{version}-{variant}", entries[connector], built
            )
            sizes[artifact] = (
                central_dir(
                    artifact,
                    version,
                    built,
                    f"The {connector} pgwire connector bundle ({variant})",
                    out,
                )
                .stat()
                .st_size
            )
        return sizes
    finally:
        shutil.rmtree(work, ignore_errors=True)


def main(tarball_dir: Path, out: Path, version: str) -> int:
    pattern = re.compile(rf"pgwire-(?P<c>[a-z]+)-{re.escape(version)}-(?P<v>[a-z0-9_-]+)\.tar\.gz")
    by_variant: dict[str, dict[str, Path]] = {}
    for tarball in sorted(tarball_dir.glob("pgwire-*.tar.gz")):
        match = pattern.fullmatch(tarball.name)
        if match is None or match["c"] == "calcite":  # pgwire-calcite is not a connector bundle
            continue
        by_variant.setdefault(match["v"], {})[match["c"]] = tarball
    if not by_variant:
        print(f"no pgwire bundles of {version} under {tarball_dir}", file=sys.stderr)
        return 1
    out.mkdir(parents=True, exist_ok=True)
    report = {v: build_variant(t, version, v, out) for v, t in sorted(by_variant.items())}
    (out / "sizes.json").write_text(json.dumps(report, indent=1) + "\n")
    for variant, sizes in report.items():
        print(f"{variant}: {sum(sizes.values()) / 1e6:.0f} MB")
        for artifact, size in sorted(sizes.items()):
            print(f"  {artifact}: {size / 1e6:.0f} MB")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(__doc__)
        raise SystemExit(2)
    raise SystemExit(main(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3]))
