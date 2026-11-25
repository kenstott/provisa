# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2aecaf-994a-4cb1-821b-175de186e72e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Spark jar's Central bundle (REQ-2270): built and tested, its shade checked, kept as a
workflow artifact and never published, as the reasoner jar's bundle is."""

# Requirements: REQ-2270

from __future__ import annotations

import importlib.util
import zipfile
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]
_SCRIPT = REPO / "scripts" / "ci" / "spark_jar_maven_bundle.py"


def _module():
    spec = importlib.util.spec_from_file_location("spark_jar_maven_bundle", _SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_bundle_is_built_tested_and_kept_never_published():
    path = REPO / ".github" / "workflows" / "spark-jar-maven-bundle.yml"
    workflow = yaml.safe_load(path.read_text())
    assert set(workflow[True]) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read"}
    steps = workflow["jobs"]["bundle"]["steps"]
    runs = [str(s.get("run") or "") for s in steps]
    build = [i for i, r in enumerate(runs) if "scripts/ci/spark_jar_maven_bundle.py bundles" in r]
    keep = [
        i
        for i, s in enumerate(steps)
        if str(s.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    assert len(build) == len(keep) == 1 and build[0] < keep[0]
    text = path.read_text()
    for publish in ("central.sonatype", "oss.sonatype", "curl", "mvn deploy", "gh release", "gpg"):
        assert publish not in text, publish


def test_the_coordinates_are_the_poms():
    group, artifact, version = _module().coordinates(REPO / "provisa-spark-jar" / "pom.xml")
    assert (group, artifact) == ("io.simpleishard", "provisa-spark_2.13")
    assert version


def test_a_class_the_shade_left_at_its_own_package_is_named(tmp_path):
    module = _module()
    jar = tmp_path / "x.jar"
    with zipfile.ZipFile(jar, "w") as content:
        content.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n")
        content.writestr("io/provisa/spark/ProvisaDataSource.class", b"")
        content.writestr("io/provisa/spark/shaded/grpc/Status.class", b"")
        content.writestr("io/grpc/Status.class", b"")
        content.writestr("com/fasterxml/jackson/core/JsonParser.class", b"")
    assert module.leaked(jar) == [
        "com/fasterxml/jackson/core/JsonParser.class",
        "io/grpc/Status.class",
    ]


def test_a_jar_that_leaks_a_class_is_not_bundled(tmp_path, monkeypatch):
    module = _module()
    group, artifact, version = module.coordinates(module.PROJECT / "pom.xml")
    project = tmp_path / "project"
    target = project / "target"
    target.mkdir(parents=True)
    (project / "pom.xml").write_text((module.PROJECT / "pom.xml").read_text())
    stem = f"{artifact}-{version}"
    with zipfile.ZipFile(target / f"{stem}.jar", "w") as content:
        content.writestr("io/netty/Leaked.class", b"")
    monkeypatch.setattr(module, "PROJECT", project)
    monkeypatch.setattr(module, "build", lambda mvn: None)
    monkeypatch.setattr(module.shutil, "which", lambda name: "/usr/bin/mvn")
    assert module.main(["spark_jar_maven_bundle.py", str(tmp_path / "out")]) == 1
    assert not (tmp_path / "out" / "sizes.json").exists()

    with zipfile.ZipFile(target / f"{stem}.jar", "w") as content:
        content.writestr("io/provisa/spark/ProvisaDataSource.class", b"")
    for suffix in ("-sources", "-javadoc"):
        (target / f"{stem}{suffix}.jar").write_bytes(b"j")
    assert module.main(["spark_jar_maven_bundle.py", str(tmp_path / "ok")]) == 0
    version_dir = tmp_path / "ok" / artifact / Path(*group.split(".")) / artifact / version
    for name in (f"{stem}.jar", f"{stem}.pom", f"{stem}-sources.jar", f"{stem}-javadoc.jar"):
        assert (version_dir / name).is_file() and (version_dir / f"{name}.sha256").is_file(), name
    assert (tmp_path / "ok" / "sizes.json").is_file()
