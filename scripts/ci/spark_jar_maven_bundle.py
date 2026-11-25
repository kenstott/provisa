#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Build the provisa data source's Spark jar and lay it out for Maven Central (REQ-2270).

provisa-spark-jar/pom.xml builds ``io.simpleishard:provisa-spark_2.13``: Maven compiles it, runs
its JUnit tests, and shades Arrow Flight, gRPC, Netty, protobuf and Guava under
``io.provisa.spark.shaded``. The jar must hold nothing outside ``io/provisa/`` and ``META-INF/``,
or this exits non-zero naming what leaked: a bundled class at its own package would collide with
Spark's copy of it.

Publishes nothing: it writes one Central bundle directory (the jar, its pom, sources and javadoc
jars, each with its SHA-256) and ``sizes.json``, as scripts/ci/reasoner_maven_bundle.py does for
the reasoner; .github/workflows/spark-jar-maven-bundle.yml keeps it as a workflow artifact, and
signing and the Maven Central promote are a separate, confirmed step.

Usage: spark_jar_maven_bundle.py <out_dir>
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2] / "provisa-spark-jar"
_NS = {"m": "http://maven.apache.org/POM/4.0.0"}
#: Where a shaded jar's entries may sit: its own package and the jar's metadata.
ALLOWED = ("io/provisa/", "META-INF/")
_POM = """<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <modelVersion>4.0.0</modelVersion>
  <groupId>{group}</groupId>
  <artifactId>{artifact}</artifactId>
  <version>{version}</version>
  <packaging>jar</packaging>
  <name>{artifact}</name>
  <description>Spark data source for Provisa: read governed Provisa tables as DataFrames over Arrow
    Flight SQL, spark.read.format("provisa"). Spark 4 / Scala 2.13; Arrow Flight, gRPC, Netty,
    protobuf and Guava are shaded into the jar, and Spark supplies the rest.</description>
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


def coordinates(pom: Path) -> tuple[str, str, str]:
    """The project's group, artifact and version, as its pom states them."""
    root = ET.parse(pom).getroot()  # nosec B314 - the repository's own pom

    def text(tag: str) -> str:
        node = root.find(f"m:{tag}", _NS)
        if node is None or not node.text:
            raise SystemExit(f"{pom} states no {tag}")
        return node.text.strip()

    return text("groupId"), text("artifactId"), text("version")


def _digest(path: Path) -> str:
    with path.open("rb") as content:
        return hashlib.file_digest(content, "sha256").hexdigest()


def leaked(jar: Path) -> list[str]:
    """The jar's entries outside ALLOWED: classes the shade left at their own packages."""
    with zipfile.ZipFile(jar) as content:
        return sorted(
            name
            for name in content.namelist()
            if not name.endswith("/") and not name.startswith(ALLOWED)
        )


def build(mvn: str) -> None:
    """``mvn package``: compile, the JUnit tests, then the shaded, sources and javadoc jars."""
    subprocess.run([mvn, "-B", "-f", str(PROJECT / "pom.xml"), "package"], check=True)  # nosec B603 - fixed arguments


def central_dir(out: Path, group: str, artifact: str, version: str) -> Path:
    """The Central bundle directory: the jar, its pom, sources and javadoc jars, each summed."""
    base = out / artifact
    version_dir = base / group.replace(".", "/") / artifact / version
    version_dir.mkdir(parents=True)
    stem = f"{artifact}-{version}"
    target = PROJECT / "target"
    for suffix in ("", "-sources", "-javadoc"):
        shutil.copyfile(target / f"{stem}{suffix}.jar", version_dir / f"{stem}{suffix}.jar")
    (version_dir / f"{stem}.pom").write_text(
        _POM.format(group=group, artifact=artifact, version=version)
    )
    for name in (f"{stem}.jar", f"{stem}.pom", f"{stem}-sources.jar", f"{stem}-javadoc.jar"):
        (version_dir / f"{name}.sha256").write_text(_digest(version_dir / name) + "\n")
    return base


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    mvn = shutil.which("mvn")
    if mvn is None:
        print("mvn is not on the PATH", file=sys.stderr)
        return 2
    out = Path(argv[1])
    out.mkdir(parents=True, exist_ok=True)
    group, artifact, version = coordinates(PROJECT / "pom.xml")
    build(mvn)
    jar = PROJECT / "target" / f"{artifact}-{version}.jar"
    stray = leaked(jar)
    if stray:
        print(
            f"{jar.name} holds {len(stray)} entries outside {', '.join(ALLOWED)} (relocate or "
            f"exclude them in provisa-spark-jar/pom.xml): {', '.join(stray[:20])}",
            file=sys.stderr,
        )
        return 1
    base = central_dir(out, group, artifact, version)
    sizes = {artifact: sum(p.stat().st_size for p in base.rglob("*") if p.is_file())}
    (out / "sizes.json").write_text(json.dumps(sizes, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
