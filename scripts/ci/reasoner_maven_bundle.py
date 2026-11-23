#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Build the sparql reasoner's jar and lay it out for Maven Central (REQ-2138).

provisa/sparql/reasoner/Reasoner.java is compiled by the pinned Temurin JDK's javac
(``code_runtimes.RUNTIMES["temurin"]``) against the pinned HermiT and OWL API jars
(``reasoner_bundle.JARS``) and stored in a jar whose entries are sorted, uncompressed and dated
one fixed time, so its bytes depend only on the source and that compiler. The jar's SHA-256 must
be the one ``reasoner_bundle.REASONER`` pins, or this exits non-zero naming the sum it built: what
is promoted is what installs fetch and check.

Publishes nothing: it writes one Central bundle directory (the jar, its pom, sources and javadoc
jars) and ``sizes.json``; .github/workflows/reasoner-maven-bundle.yml signs and zips it, and the
Maven Central promote is a separate, confirmed step.

Usage: reasoner_maven_bundle.py <out_dir>
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver  # noqa: E402
from provisa.sparql.reasoner_bundle import (  # noqa: E402
    JARS,
    MAIN_CLASS,
    PROGRAM,
    REASONER,
    ReasonerBundle,
)

#: The bytecode level: the oldest Java the reasoner's dependencies run on is below it, and jdk4py's
#: is above.
RELEASE = "17"
#: Every entry's time: a fixed one, so the jar's bytes do not depend on when it was built.
_DATE = (2026, 1, 1, 0, 0, 0)
_MANIFEST = f"Manifest-Version: 1.0\r\nMain-Class: {MAIN_CLASS}\r\nCreated-By: provisa\r\n\r\n"
_POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>{group}</groupId>
  <artifactId>{artifact}</artifactId>
  <version>{version}</version>
  <packaging>jar</packaging>
  <name>{artifact}</name>
  <description>The OWL reasoner a Provisa sparql source runs as its own program: HermiT over the
    OWL API, which Provisa puts on its class path at pinned versions.</description>
  <url>https://github.com/kenstott/provisa</url>
  <licenses>
    <license>
      <name>Business Source License 1.1</name>
      <url>https://github.com/kenstott/provisa/blob/main/LICENSE</url>
    </license>
  </licenses>
  <developers>
    <developer><name>Kenneth Stott</name></developer>
  </developers>
  <scm>
    <url>https://github.com/kenstott/provisa</url>
    <connection>scm:git:https://github.com/kenstott/provisa.git</connection>
  </scm>
</project>
"""


def _digest(path: Path) -> str:
    with path.open("rb") as content:
        return hashlib.file_digest(content, "sha256").hexdigest()


def write_jar(files: dict[str, bytes], out: Path) -> None:
    """A jar of ``files`` (entry name -> bytes) after its manifest: sorted, stored, one date."""
    with zipfile.ZipFile(out, "w", zipfile.ZIP_STORED) as jar:
        for name, data in [("META-INF/MANIFEST.MF", _MANIFEST.encode()), *sorted(files.items())]:
            info = zipfile.ZipInfo(name, date_time=_DATE)
            info.create_system = 3
            info.external_attr = 0o644 << 16
            jar.writestr(info, data)


def compile_classes(javac: Path, classpath: str, work: Path) -> dict[str, bytes]:
    """The reasoner's class files, by entry name, compiled from PROGRAM."""
    classes = work / "classes"
    classes.mkdir()
    subprocess.run(  # nosec B603 - the pinned javac, arguments built here
        [
            str(javac),
            "--release",
            RELEASE,
            "-encoding",
            "UTF-8",
            "-cp",
            classpath,
            "-d",
            str(classes),
            str(PROGRAM),
        ],
        check=True,
    )
    return {p.relative_to(classes).as_posix(): p.read_bytes() for p in classes.rglob("*.class")}


def build_jar(out: Path, *, cache_root: Path | None = None) -> str:
    """Build the reasoner's jar at ``out`` and return its SHA-256."""
    java = CodeRuntimeResolver(cache_root=cache_root).resolve("temurin")
    javac = java.with_name("javac.exe" if java.suffix == ".exe" else "javac")
    bundle = ReasonerBundle(cache_root=cache_root)
    bundle.keep(JARS)
    with tempfile.TemporaryDirectory() as work:
        write_jar(compile_classes(javac, bundle.classpath(JARS), Path(work)), out)
    return _digest(out)


def central_dir(jar: Path, out: Path) -> Path:
    """The Central bundle directory of REASONER: the jar, its pom, sources and javadoc jars."""
    base = out / REASONER.artifact
    version_dir = base / REASONER.group.replace(".", "/") / REASONER.artifact / REASONER.version
    version_dir.mkdir(parents=True)
    stem = f"{REASONER.artifact}-{REASONER.version}"
    jar.replace(version_dir / f"{stem}.jar")
    (version_dir / f"{stem}.pom").write_text(
        _POM.format(group=REASONER.group, artifact=REASONER.artifact, version=REASONER.version)
    )
    write_jar({PROGRAM.name: PROGRAM.read_bytes()}, version_dir / f"{stem}-sources.jar")
    write_jar(
        {
            "README.txt": (
                f"{REASONER.artifact} is a program, run by its main class {MAIN_CLASS}, with no "
                "Java API of its own; its source is in the sources jar.\n"
            ).encode()
        },
        version_dir / f"{stem}-javadoc.jar",
    )
    for name in (f"{stem}.jar", f"{stem}.pom", f"{stem}-sources.jar", f"{stem}-javadoc.jar"):
        (version_dir / f"{name}.sha256").write_text(_digest(version_dir / name) + "\n")
    return base


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    out = Path(argv[1])
    out.mkdir(parents=True, exist_ok=True)
    jar = out / REASONER.filename
    found = build_jar(jar)
    if found != REASONER.sha256:
        print(
            f"the built {REASONER.filename} has SHA-256 {found}, not the {REASONER.sha256} "
            "provisa/sparql/reasoner_bundle.py pins: pin the new sum with a new version",
            file=sys.stderr,
        )
        return 1
    base = central_dir(jar, out)
    sizes = {REASONER.artifact: sum(p.stat().st_size for p in base.rglob("*") if p.is_file())}
    (out / "sizes.json").write_text(json.dumps(sizes, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
