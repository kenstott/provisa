# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""REQ-294: what the native installers carry, and how it reaches an install."""

from __future__ import annotations

import ast
import importlib.util
import os
import stat
import subprocess
import sys
import tarfile
import types
from collections.abc import Mapping
from pathlib import Path

import pytest

from provisa.federation import stage_duckdb_extensions as stage_ext
from provisa.runtime_deps import installer_assets as ia
from provisa.runtime_deps import pgwire_bundles as pb
from provisa.runtime_deps import stage_pgwire_bundles as stage_pg

_REPO = Path(__file__).resolve().parents[2]


def _lay_down(spec: pb.BundleSpec, dest: Path) -> None:
    (dest / "bin").mkdir(parents=True)
    (dest / "bin" / spec.artifact_name).write_text("#!/bin/sh\n")


def _collect() -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(
        "collect", _REPO / "packaging" / "duckdb-ext" / "collect.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---- the archive ------------------------------------------------------------------------------


def _staged_tree(root: Path) -> None:
    (root / ia.BUNDLES_DIR / "a" / "bin").mkdir(parents=True)
    (root / ia.BUNDLES_DIR / "b" / "bin").mkdir(parents=True)
    (root / ia.DUCKDB_DIR).mkdir()
    launcher = root / ia.BUNDLES_DIR / "a" / "bin" / "run"
    launcher.write_text("same jre")
    launcher.chmod(0o755)
    (root / ia.BUNDLES_DIR / "b" / "bin" / "run").write_text("same jre")
    (root / ia.BUNDLES_DIR / "b" / "bin" / "run").chmod(0o755)
    (root / ia.BUNDLES_DIR / "a" / "data").write_text("same jre")  # same bytes, another mode
    (root / ia.BUNDLES_DIR / "b" / "only").write_text("its own")
    (root / ia.DUCKDB_DIR / "ext.duckdb_extension").write_text("blob")
    (root / ia.BUNDLES_DIR / ".a.lock").write_text("")


def test_pack_stores_a_shared_file_once_and_unpack_restores_every_path(tmp_path):
    staged = tmp_path / "staged"
    staged.mkdir()
    _staged_tree(staged)
    archive = tmp_path / "assets.tar.gz"
    ia.pack(staged, archive)

    with tarfile.open(archive) as tar:
        names = {m.name: m for m in tar.getmembers()}
    assert names["runtime-deps/b/bin/run"].islnk()
    assert names["runtime-deps/b/bin/run"].linkname == "runtime-deps/a/bin/run"
    assert names["runtime-deps/a/bin/run"].isreg()
    assert not any(n.endswith(".lock") for n in names)

    home = tmp_path / "home"
    assert ia.unpack(archive, home, runner=lambda *_: None) is True
    assert (home / "runtime-deps/b/bin/run").read_text() == "same jre"
    assert (home / "runtime-deps/b/only").read_text() == "its own"
    assert (home / "duckdb-ext/ext.duckdb_extension").read_text() == "blob"
    assert (home / "runtime-deps/a/bin/run").stat().st_mode & stat.S_IXUSR
    assert not (home / "runtime-deps/a/data").stat().st_mode & stat.S_IXUSR
    assert os.path.samefile(home / "runtime-deps/a/bin/run", home / "runtime-deps/b/bin/run")


def test_build_stages_then_checks_excepting_govdata_by_name(tmp_path):
    calls: list[tuple[tuple[str, ...], dict[str, str]]] = []

    def runner(args: tuple[str, ...], env: Mapping[str, str]) -> None:
        calls.append((args, dict(env)))
        if args[2].endswith("stage_pgwire_bundles"):
            Path(env["PROVISA_RUNTIME_DEPS_CACHE"], "x").write_text("bundle")
        if args[2].endswith("stage_duckdb_extensions"):
            Path(env["PROVISA_DUCKDB_EXT_DIR"], "y").write_text("ext")

    out = tmp_path / "dist" / "assets.tar.gz"
    ia.build(out, runner=runner)

    modules = [c[0][2] for c in calls]
    assert modules == [
        "provisa.federation.stage_duckdb_extensions",
        "provisa.runtime_deps.stage_pgwire_bundles",
        "provisa.federation.duckdb_extensions",
        "provisa.runtime_deps.pgwire_bundles",
    ]
    assert all(c[0][:2] == ("-I", "-m") for c in calls)
    assert calls[1][0][3:] == ("--except", "govdata")
    assert calls[3][0][3:] == ("--except", "govdata")
    with tarfile.open(out) as tar:
        assert {"runtime-deps/x", "duckdb-ext/y"} <= set(tar.getnames())
    assert not list(out.parent.glob(".provisa-assets.*"))  # the staging directory is gone


def test_build_stops_when_a_stage_step_fails(tmp_path):
    def failing(args: tuple[str, ...], env: Mapping[str, str]) -> None:
        raise subprocess.CalledProcessError(1, [sys.executable, *args])

    with pytest.raises(subprocess.CalledProcessError):
        ia.build(tmp_path / "out.tar.gz", runner=failing)
    assert not (tmp_path / "out.tar.gz").exists()


def test_unpack_checks_the_install_and_does_nothing_the_second_time(tmp_path):
    staged = tmp_path / "staged"
    staged.mkdir()
    _staged_tree(staged)
    archive = tmp_path / "assets.tar.gz"
    ia.pack(staged, archive)
    home = tmp_path / "home"
    checked: list[tuple[str, ...]] = []

    def runner(args: tuple[str, ...], env: Mapping[str, str]) -> None:
        checked.append(args)
        assert env["PROVISA_RUNTIME_DEPS_CACHE"] == str(home / "runtime-deps")
        assert env["PROVISA_DUCKDB_EXT_DIR"] == str(home / "duckdb-ext")

    assert ia.unpack(archive, home, runner=runner) is True
    assert [a[2] for a in checked] == [
        "provisa.federation.duckdb_extensions",
        "provisa.runtime_deps.pgwire_bundles",
    ]
    checked.clear()
    assert ia.unpack(archive, home, runner=runner) is False
    assert checked == []


def test_a_failed_check_leaves_no_marker_so_the_next_launch_tries_again(tmp_path):
    staged = tmp_path / "staged"
    staged.mkdir()
    _staged_tree(staged)
    archive = tmp_path / "assets.tar.gz"
    ia.pack(staged, archive)
    home = tmp_path / "home"

    def failing(args: tuple[str, ...], env: Mapping[str, str]) -> None:
        raise subprocess.CalledProcessError(1, [sys.executable, *args])

    with pytest.raises(subprocess.CalledProcessError):
        ia.unpack(archive, home, runner=failing)
    assert not (home / ".provisa-assets").exists()


def test_unpack_refuses_a_member_outside_home(tmp_path):
    archive = tmp_path / "evil.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        info = tarfile.TarInfo("../escaped")
        tar.addfile(info)
    with pytest.raises(tarfile.FilterError):
        ia.unpack(archive, tmp_path / "home", runner=lambda *_: None)
    assert not (tmp_path / "escaped").exists()


# ---- the named exception ------------------------------------------------------------------------


def _staged_but(tmp_path: Path, *absent: str) -> None:
    resolver = pb.BundleResolver(cache_root=tmp_path)
    for connector in set(pb.BUNDLE_CONNECTOR.values()) - set(absent):
        spec = pb.BundleSpec(connector, pb.connector_release(connector))
        _lay_down(spec, resolver.cached_path(spec))


def test_the_check_passes_only_with_the_exception_named(tmp_path, monkeypatch, capsys):
    _staged_but(tmp_path, "govdata")
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))

    assert pb.main([]) == 1
    assert "pgwire-govdata" in capsys.readouterr().err

    assert pb.main(["--except", "govdata"]) == 0
    assert "excepted: govdata" in capsys.readouterr().out


def test_the_check_still_refuses_another_missing_bundle_under_the_exception(
    tmp_path, monkeypatch, capsys
):
    _staged_but(tmp_path, "govdata", "jira")
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))
    assert pb.main(["--except", "govdata"]) == 1
    assert "pgwire-jira" in capsys.readouterr().err


def test_a_mistyped_exception_is_refused(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))
    assert pb.main(["--except", "govdta"]) == 1
    assert "govdta" in capsys.readouterr().err


def test_staging_leaves_out_what_it_is_told_and_fetches_only_what_it_is_told(tmp_path, monkeypatch):
    laid: list[str] = []

    def dl(spec: pb.BundleSpec, dest: Path) -> None:
        laid.append(spec.connector)
        _lay_down(spec, dest)

    stage_pg.stage(tmp_path, downloader=dl, excepted=["govdata"])
    assert "govdata" not in laid and set(laid) == set(pb.BUNDLE_CONNECTOR.values()) - {"govdata"}

    laid.clear()
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))
    real_stage = stage_pg.stage
    monkeypatch.setattr(
        stage_pg, "stage", lambda root, excepted: real_stage(root, downloader=dl, excepted=excepted)
    )
    assert stage_pg.main(["--only", "govdata"]) == 0
    assert laid == ["govdata"]


def test_nothing_the_server_runs_imports_a_staging_step():
    # The two staging modules live in the package for a pip user to run; the server must not reach them.
    runtime = _REPO / "provisa"
    build_steps = {"stage_duckdb_extensions", "stage_pgwire_bundles"}
    offenders: list[str] = []
    for path in sorted(runtime.rglob("*.py")):
        if "_ui" in path.parts or path.stem in build_steps:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module or "", *(f"{node.module}.{a.name}" for a in node.names)]
            if any(n.rsplit(".", 1)[-1] in build_steps for n in names):
                offenders.append(f"{path.relative_to(_REPO)}:{node.lineno}")
    assert offenders == []


# ---- the pip route ------------------------------------------------------------------------------


def test_the_packaged_command_stages_where_provisa_run_looks_and_the_script_where_duckdb_does(
    monkeypatch, tmp_path
):
    seen: list[str | None] = []

    async def fake_stage() -> int:
        seen.append(os.environ.get("PROVISA_DUCKDB_EXT_DIR"))
        return 0

    monkeypatch.setattr(stage_ext, "_stage", fake_stage)
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    assert stage_ext.main(default_directory=tmp_path / "run" / "duckdb-ext") == 0
    assert seen == [str(tmp_path / "run" / "duckdb-ext")]
    assert (tmp_path / "run" / "duckdb-ext").is_dir()

    seen.clear()
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    assert stage_ext.main(default_directory=None) == 0
    assert seen == [None]

    seen.clear()
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "named"))
    assert stage_ext.main(default_directory=tmp_path / "unused") == 0
    assert seen == [str(tmp_path / "named")]


def test_the_run_directory_is_the_one_provisa_run_uses():
    from provisa.cli import _DEFAULT_DATA_DIR

    assert stage_ext.RUN_DIRECTORY == _DEFAULT_DATA_DIR / "duckdb-ext"


def test_a_refusal_names_the_packaged_command():
    from provisa.core.engine_memory import STAGE_FIX

    assert "python -m provisa.federation.stage_duckdb_extensions" in STAGE_FIX
    assert "python -m provisa.runtime_deps.stage_pgwire_bundles" in pb.STAGE_FIX


# ---- the wheel ----------------------------------------------------------------------------------


def _blob(directory: Path, name: str, size: int) -> Path:
    path = directory / f"{name}.duckdb_extension"
    path.write_bytes(os.urandom(size))  # random: it compresses to its own size
    return path


def test_the_wheel_takes_what_fits_in_order_and_skips_what_does_not(tmp_path, monkeypatch):
    collect = _collect()
    monkeypatch.setattr(collect, "WHEEL_BUDGET_BYTES", 1000)
    found = [
        _blob(tmp_path, n, s)
        for n, s in [("postgres_scanner", 600), ("sqlite_scanner", 700), ("httpfs", 300)]
    ]
    carried, left_out = collect._select(found)
    assert [f.name.split(".")[0] for f in carried] == ["postgres_scanner", "httpfs"]
    assert left_out == ["sqlite_scanner"]


def test_a_group_travels_together_or_not_at_all(tmp_path, monkeypatch):
    collect = _collect()
    monkeypatch.setattr(collect, "WHEEL_BUDGET_BYTES", 1000)
    found = [_blob(tmp_path, n, 600) for n in ["motherduck", "motherduck_impl"]]
    carried, left_out = collect._select(found)
    assert carried == []
    assert sorted(left_out) == ["motherduck", "motherduck_impl"]


def test_an_extension_the_priority_does_not_list_stops_the_build(tmp_path):
    collect = _collect()
    message = collect._select([_blob(tmp_path, "brand_new", 10)])
    assert isinstance(message, str) and "brand_new" in message


def test_check_wheels_needs_one_wheel_per_platform_under_the_limit(tmp_path, capsys):
    collect = _collect()
    for _, tag in collect.PLATFORMS.values():
        (tmp_path / f"provisa_duckdb_ext-0.1.2-py3-none-{tag}.whl").write_bytes(b"x")
    assert collect.check_wheels(tmp_path) == 0
    victim = tmp_path / f"provisa_duckdb_ext-0.1.2-py3-none-{collect.PLATFORMS['linux-x64'][1]}.whl"
    with victim.open("wb") as handle:
        handle.truncate(collect.MAX_WHEEL_BYTES)
    assert collect.check_wheels(tmp_path) == 1
    assert "past PyPI" in capsys.readouterr().err


def test_there_is_one_extension_project():
    assert not (_REPO / "packaging" / "duckdb-ext-warehouse").exists()
    assert not (_REPO / "packaging" / "duckdb-ext-saas").exists()
    assert "provisa-duckdb-ext-" not in (_REPO / "packaging/duckdb-ext/pyproject.toml").read_text()


# ---- the installers -----------------------------------------------------------------------------


@pytest.mark.parametrize(
    "builder",
    [
        "packaging/macos/build-dmg.sh",
        "packaging/linux/build-appimage.sh",
        "packaging/windows/build-sfx.ps1",
    ],
)
def test_each_installer_build_stages_and_checks_its_assets(builder):
    text = (_REPO / builder).read_text()
    assert "-I -m provisa.runtime_deps.installer_assets build" in text
    assert "provisa-assets.tar.gz" in text


@pytest.mark.parametrize(
    "launcher",
    [
        "packaging/macos/first-launch.sh",
        "packaging/linux/first-launch.sh",
        "packaging/windows/first-launch-native.ps1",
    ],
)
def test_each_first_launch_unpacks_the_assets(launcher):
    text = (_REPO / launcher).read_text()
    assert "-I -m provisa.runtime_deps.installer_assets" in text and "unpack" in text


def test_the_native_launchers_export_the_directories_the_archive_fills():
    posix = (_REPO / "scripts/provisa").read_text()
    windows = (_REPO / "packaging/windows/provisa-native.ps1").read_text()
    assert f"PROVISA_DUCKDB_EXT_DIR=${{PROVISA_HOME}}/{ia.DUCKDB_DIR}" in posix
    assert f"PROVISA_RUNTIME_DEPS_CACHE=${{PROVISA_HOME}}/{ia.BUNDLES_DIR}" in posix
    assert f"'{ia.DUCKDB_DIR}'" in windows and f"'{ia.BUNDLES_DIR}'" in windows


@pytest.mark.parametrize("dockerfile", ["Dockerfile", "Dockerfile.dev"])
def test_the_images_stage_the_same_list_and_check_it_without_dash_i(dockerfile):
    """provisa is /app in the final image, not site-packages: -I could not import it."""
    text = (_REPO / dockerfile).read_text()
    assert "provisa.runtime_deps.stage_pgwire_bundles --except govdata" in text
    assert "provisa.runtime_deps.pgwire_bundles --except govdata" in text
    assert "-I -m provisa.runtime_deps" not in text


def test_the_cloud_deploy_stages_every_bundle_before_recreating_the_stack():
    text = (_REPO / "scripts/deploy-cloud.sh").read_text()
    stage = text.index('  stage_runtime_deps\n  echo "== host overlay: installing overlay"')
    assert stage < text.index('echo "== host overlay: recreating stack"')
    assert 'provisa.runtime_deps.stage_pgwire_bundles"' in text  # no --except: govdata too
    assert ":/root/.cache/provisa/runtime_deps" not in text


def test_the_ci_lanes_stage_what_they_read():
    text = (_REPO / ".github/workflows/integration-suite-lanes.yml").read_text()
    assert "stage_pgwire_bundles.py --only salesforce" in text
    assert "stage_pgwire_bundles.py --only servicenow" in text
