# Copyright (c) 2026 Kenneth Stott
# Canary: 0c94f7e2-6d1b-4a83-b2c5-91e7d3f40a68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""REQ-294: every installer works the same: one stage command, assets from package repositories."""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import subprocess
import sys
import tarfile
import types
from pathlib import Path

import pytest

from provisa.runtime_deps import maven_bundles, pgwire_bundles, stage_assets, stage_pgwire_bundles
from provisa.runtime_deps.pgwire_bundles import BundleResolver, BundleSpec, BundleUnavailable

_REPO = Path(__file__).resolve().parents[2]
_LAUNCHERS = (
    "packaging/macos/first-launch.sh",
    "packaging/linux/first-launch.sh",
    "packaging/windows/first-launch-native.ps1",
)
_BUILDS = (
    "packaging/macos/build-dmg.sh",
    "packaging/linux/build-appimage.sh",
    "packaging/windows/build-sfx.ps1",
)


def _split_module() -> types.ModuleType:
    path = _REPO / "scripts/ci/pgwire_maven_bundles.py"
    spec = importlib.util.spec_from_file_location("pgwire_maven_bundles", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["pgwire_maven_bundles"] = module
    spec.loader.exec_module(module)
    return module


# ---- the stage command ---------------------------------------------------------------------------


def test_the_stage_command_stages_both_and_checks_the_extensions(tmp_path):
    ran: list[tuple[tuple[str, ...], dict]] = []
    stage_assets.stage(tmp_path, runner=lambda args, env: ran.append((args, dict(env))))
    assert [args[-1] for args, _ in ran] == [
        "provisa.federation.stage_duckdb_extensions",
        "provisa.federation.duckdb_extensions",
        "provisa.runtime_deps.stage_pgwire_bundles",
    ]
    for _args, env in ran:
        assert env["PROVISA_DUCKDB_EXT_DIR"] == str(tmp_path / "duckdb-ext")
        assert env["PROVISA_RUNTIME_DEPS_CACHE"] == str(tmp_path / "runtime-deps")


def test_a_failed_step_fails_the_command_naming_it(monkeypatch, capsys, tmp_path):
    def fail(_home):
        args = ("-m", "provisa.federation.stage_duckdb_extensions")
        raise subprocess.CalledProcessError(1, [sys.executable, *args])

    monkeypatch.setattr(stage_assets, "stage", fail)
    assert stage_assets.main([str(tmp_path)]) == 1
    assert "stage_duckdb_extensions" in capsys.readouterr().err


def test_without_a_home_the_environment_names_the_directories(monkeypatch):
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", "/x")
    assert stage_assets.environment(None) == {}
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR")
    assert stage_assets.environment(None)["PROVISA_DUCKDB_EXT_DIR"].endswith(
        ".provisa/native/duckdb-ext"
    )


def test_every_connector_is_staged_govdata_included(tmp_path, monkeypatch):
    fetched: list[str] = []

    def downloader(spec: BundleSpec, dest: Path) -> None:
        fetched.append(spec.connector)
        (dest / "bin").mkdir(parents=True)
        (dest / "bin" / spec.artifact_name).write_text("#!/bin/sh\n")

    monkeypatch.setattr(pgwire_bundles, "named_bundle_dir", lambda _connector: None)
    stage_pgwire_bundles.stage(tmp_path, downloader=downloader)
    assert "govdata" in fetched and len(fetched) == len(
        set(pgwire_bundles.BUNDLE_CONNECTOR.values())
    )
    assert (
        stage_pgwire_bundles.stage(tmp_path, downloader=downloader) == []
    )  # filled: nothing fetched


def test_only_names_a_real_connector(capsys, monkeypatch, tmp_path):
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))
    assert stage_pgwire_bundles.main(["--only", "gomdata"]) == 1
    assert "gomdata" in capsys.readouterr().err


def test_nothing_the_server_runs_imports_a_staging_step():
    # The staging modules live in the package for an install to run; the server must not reach them.
    steps = {"stage_duckdb_extensions", "stage_pgwire_bundles", "stage_assets", "maven_bundles"}
    offenders: list[str] = []
    for path in sorted((_REPO / "provisa").rglob("*.py")):
        if "_ui" in path.parts or path.stem in steps or path.name == "cli.py":
            continue  # cli.py is first launch of a pip install, not the server
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or "", *(f"{node.module}.{a.name}" for a in node.names)]
            if any(n.rsplit(".", 1)[-1] in steps for n in names):
                offenders.append(f"{path.relative_to(_REPO)}:{node.lineno}")
    assert offenders == []


def test_a_refusal_names_the_stage_command():
    from provisa.core.engine_memory import STAGE_FIX

    assert "python -m provisa.runtime_deps.stage_assets" in STAGE_FIX
    assert "python -m provisa.runtime_deps.stage_assets" in pgwire_bundles.STAGE_FIX


# ---- the installers ------------------------------------------------------------------------------


@pytest.mark.parametrize("builder", _BUILDS)
def test_no_installer_carries_extensions_or_bundles(builder):
    text = (_REPO / builder).read_text()
    assert "installer_assets" not in text and "provisa-assets.tar.gz" not in text
    assert "stage_assets" not in text  # the installer stays small: staging is at first launch


@pytest.mark.parametrize("launcher", _LAUNCHERS)
def test_each_first_launch_runs_the_one_stage_command(launcher):
    text = (_REPO / launcher).read_text()
    assert "-I -m provisa.runtime_deps.stage_assets" in text
    assert "unpack" not in text.split("stage_assets")[0].rsplit("\n", 3)[-1]


def test_the_native_launchers_export_the_directories_the_command_fills():
    posix = (_REPO / "scripts/provisa").read_text()
    windows = (_REPO / "packaging/windows/provisa-native.ps1").read_text()
    assert f"PROVISA_DUCKDB_EXT_DIR=${{PROVISA_HOME}}/{stage_assets.DUCKDB_DIR}" in posix
    assert f"PROVISA_RUNTIME_DEPS_CACHE=${{PROVISA_HOME}}/{stage_assets.BUNDLES_DIR}" in posix
    assert f"'{stage_assets.DUCKDB_DIR}'" in windows and f"'{stage_assets.BUNDLES_DIR}'" in windows


@pytest.mark.parametrize("dockerfile", ["Dockerfile", "Dockerfile.dev"])
def test_the_images_are_built_with_the_assets_inside_them(dockerfile):
    """provisa is /app in the final image, not site-packages: -I could not import it."""
    text = (_REPO / dockerfile).read_text()
    assert "RUN python -m provisa.runtime_deps.stage_assets" in text
    assert "--except" not in text and "-I -m provisa.runtime_deps" not in text


def test_the_steps_inherit_isolation_from_the_command():
    # Only as isolated as the command: the image's provisa is on the working directory's path.
    assert ("-I" in stage_assets._PY) == bool(sys.flags.isolated)


def test_the_cloud_deploy_stages_every_bundle_before_recreating_the_stack():
    text = (_REPO / "scripts/deploy-cloud.sh").read_text()
    stage = text.index('  stage_runtime_deps\n  echo "== host overlay: installing overlay"')
    assert stage < text.index('echo "== host overlay: recreating stack"')
    assert 'provisa.runtime_deps.stage_pgwire_bundles"' in text  # govdata too
    assert ":/root/.cache/provisa/runtime_deps" not in text


def test_the_ci_lanes_stage_what_they_read():
    text = (_REPO / ".github/workflows/integration-suite-lanes.yml").read_text()
    assert "stage_pgwire_bundles --only salesforce --github-release" in text
    assert "stage_pgwire_bundles --only servicenow --github-release" in text


def test_the_embedded_extra_holds_no_extension_wheels_and_pip():
    import tomllib

    extras = tomllib.loads((_REPO / "pyproject.toml").read_text())["project"][
        "optional-dependencies"
    ]
    embedded = extras["embedded"]
    assert "pip" in embedded
    assert not [d for d in embedded if "duckdb-extension" in d or "provisa-duckdb-ext" in d]


def test_there_is_one_extension_project():
    assert not (_REPO / "packaging" / "duckdb-ext-warehouse").exists()
    assert not (_REPO / "packaging" / "duckdb-ext-saas").exists()
    assert "provisa-duckdb-ext-" not in (_REPO / "packaging/duckdb-ext/pyproject.toml").read_text()


# ---- the Maven bundles ----------------------------------------------------------------------------


def _tree(root: Path, files: dict[str, bytes]) -> Path:
    for rel, body in files.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        if rel.startswith("bin/"):
            path.chmod(0o755)
    return root


_JRE = b"J" * 8192
_CALCITE = b"C" * 8192


def _unpacked(tmp_path: Path) -> dict[str, Path]:
    return {
        "file": _tree(
            tmp_path / "file",
            {
                "jre/libjvm": _JRE,
                "lib/calcite.jar": _CALCITE,
                "bin/pgwire-file": b"#!" * 4096,
                "x": b"s",
            },
        ),
        "jira": _tree(
            tmp_path / "jira",
            {"jre/libjvm": _JRE, "lib/calcite.jar": _CALCITE, "bin/pgwire-jira": b"#?" * 4096},
        ),
    }


def test_a_file_in_two_bundles_is_shared_and_one_alone_stays(tmp_path):
    split = _split_module()
    unpacked = _unpacked(tmp_path)
    entries = split.split(unpacked)
    assert sorted(e["path"] for e in entries["file"]) == ["jre/libjvm", "lib/calcite.jar"]
    assert entries["file"] == entries["jira"]


def test_shared_objects_keep_their_mode_in_their_name(tmp_path):
    # A hard link shares its inode, so a mode differing across bundles must be a separate object.
    split = _split_module()
    file = _tree(tmp_path / "file", {"a": _JRE})
    jira = _tree(tmp_path / "jira", {"a": _JRE})
    (file / "a").chmod(0o755)
    (jira / "a").chmod(0o644)
    assert split.split({"file": file, "jira": jira}) == {"file": [], "jira": []}


def _published(tmp_path: Path, version: str) -> Path:
    """The Central layout of two connectors and the shared files, as the repository serves it."""
    split = _split_module()
    unpacked = _unpacked(tmp_path / "src")
    entries = split.split(unpacked)
    repo = tmp_path / "repo" / "io" / "simpleishard"
    variant = pgwire_bundles.bundle_variant()

    def publish(artifact: str, built: Path) -> None:
        base = repo / artifact / version
        base.mkdir(parents=True)
        out = base / f"{artifact}-{version}.tar.gz"
        built.rename(out)
        (base / f"{out.name}.sha256").write_text(
            hashlib.sha256(out.read_bytes()).hexdigest() + "\n"
        )

    shared = tmp_path / "shared.tar.gz"
    split.write_shared(unpacked, entries, shared)
    publish(f"pgwire-shared-{variant}", shared)
    for connector, root in unpacked.items():
        built = tmp_path / f"{connector}.tar.gz"
        split.write_connector(
            root, f"pgwire-{connector}-{version}-{variant}", entries[connector], built
        )
        publish(f"pgwire-{connector}-{variant}", built)
    return tmp_path / "repo"


def _serve(monkeypatch, repo: Path) -> None:
    def download(url: str, out: Path) -> None:
        local = repo / url.removeprefix("https://maven.test/")
        if not local.is_file():
            raise BundleUnavailable(f"{url}: HTTP 404")
        out.write_bytes(local.read_bytes())

    monkeypatch.setenv(maven_bundles.REPOSITORY_ENV, "https://maven.test")
    monkeypatch.setattr(maven_bundles, "_download", download)


def test_a_bundle_fetched_from_maven_is_whole_and_shares_its_files_on_disk(tmp_path, monkeypatch):
    version = "0.1.0"
    _serve(monkeypatch, _published(tmp_path, version))
    monkeypatch.setattr(pgwire_bundles, "named_bundle_dir", lambda _connector: None)
    cache = tmp_path / "cache"
    resolver = BundleResolver(cache_root=cache, downloader=maven_bundles.download_maven_bundle)
    for connector in ("file", "jira"):
        resolver.resolve(BundleSpec(connector, f"engine-v{version}"))
    first, second = (
        cache / f"engine-v{version}" / "pgwire-file",
        cache / f"engine-v{version}" / "pgwire-jira",
    )
    assert (first / "jre/libjvm").read_bytes() == _JRE
    assert (first / "bin/pgwire-file").stat().st_mode & 0o777 == 0o755
    assert not (first / maven_bundles.SHARED_MANIFEST).exists()
    assert (first / "jre/libjvm").stat().st_ino == (
        second / "jre/libjvm"
    ).stat().st_ino  # stored once


def test_a_bundle_absent_from_the_repository_fails_naming_it(tmp_path, monkeypatch):
    _serve(monkeypatch, tmp_path / "empty")
    with pytest.raises(BundleUnavailable, match="HTTP 404"):
        maven_bundles.download_maven_bundle(
            BundleSpec("file", "engine-v0.1.0", variant="linux-x86_64"),
            tmp_path / "out" / "pgwire-file",
        )
    assert list((tmp_path / "out").glob(".pgwire-file.*")) == []  # nothing left by halves


def test_a_manifest_escaping_the_bundle_is_refused(tmp_path):
    root = tmp_path / "bundle"
    root.mkdir()
    (root / maven_bundles.SHARED_MANIFEST).write_text(
        json.dumps([{"path": "../evil", "object": "x"}])
    )
    with pytest.raises(BundleUnavailable, match="unsafe"):
        maven_bundles._link_shared(root, tmp_path)


def test_a_download_is_refused_when_it_does_not_match_its_sha256(tmp_path, monkeypatch):
    import httpx

    class _Stream:
        status_code = 200

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def iter_bytes(self):
            yield b"not what was published"

    monkeypatch.setattr(
        httpx, "get", lambda *_a, **_k: types.SimpleNamespace(status_code=200, text="abc  f")
    )
    monkeypatch.setattr(httpx, "stream", lambda *_a, **_k: _Stream())
    with pytest.raises(BundleUnavailable, match="sha256"):
        maven_bundles._download("https://m/x.tar.gz", tmp_path / "x")


def test_the_repository_is_the_machines(tmp_path, monkeypatch):
    settings = tmp_path / "settings.xml"
    settings.write_text(
        '<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"><mirrors><mirror>'
        "<id>corp</id><mirrorOf>central</mirrorOf><url>https://artifactory.corp/maven/</url>"
        "</mirror></mirrors></settings>"
    )
    monkeypatch.delenv(maven_bundles.REPOSITORY_ENV, raising=False)
    assert maven_bundles.repository(settings) == "https://artifactory.corp/maven"
    assert maven_bundles.repository(tmp_path / "absent.xml") == maven_bundles.CENTRAL
    monkeypatch.setenv(maven_bundles.REPOSITORY_ENV, "https://named/")
    assert maven_bundles.repository(settings) == "https://named"


def test_the_coordinates_are_per_connector_and_platform():
    assert maven_bundles.artifact_url("pgwire-file-macos-arm64", "0.1.0", "https://r") == (
        "https://r/io/simpleishard/pgwire-file-macos-arm64/0.1.0/pgwire-file-macos-arm64-0.1.0.tar.gz"
    )


def test_a_central_directory_is_complete(tmp_path):
    split = _split_module()
    tarball = tmp_path / "x.tar.gz"
    with tarfile.open(tarball, "w:gz"):
        pass
    split.central_dir("pgwire-file-macos-arm64", "0.1.0", tarball, "d", tmp_path / "out")
    base = tmp_path / "out/pgwire-file-macos-arm64/io/simpleishard/pgwire-file-macos-arm64/0.1.0"
    names = {p.name for p in base.iterdir()}
    stem = "pgwire-file-macos-arm64-0.1.0"
    assert {f"{stem}.tar.gz", f"{stem}.pom", f"{stem}-sources.jar", f"{stem}-javadoc.jar"} <= names
    assert f"{stem}.pom.sha256" in names
    pom = (base / f"{stem}.pom").read_text()
    assert "<packaging>tar.gz</packaging>" in pom and "<groupId>io.simpleishard</groupId>" in pom


def test_the_bundle_workflow_never_uploads_to_central():
    text = (_REPO / ".github/workflows/pgwire-maven-bundles.yml").read_text()
    assert "workflow_dispatch" in text
    assert "central.sonatype.com" not in text and "SONATYPE" not in text
    assert "scripts/ci/pgwire_maven_bundles.py" in text
