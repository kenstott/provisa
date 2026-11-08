# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e61d4-8a27-4c05-b1f3-6d2a7c4e9f58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""REQ-294: a pgwire bundle is located locally, staged at build or packaging; a runtime never fetches."""

from __future__ import annotations

import ast
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

from provisa.core import request_deadline
from provisa.runtime_deps import pgwire_bundles as pb

_REPO = Path(__file__).resolve().parents[2]


def _no_network(monkeypatch) -> None:
    import httpx

    def _refuse(*args, **kwargs):
        raise AssertionError("a runtime resolution reached for the network")

    monkeypatch.setattr(httpx, "stream", _refuse)


def _lay_down(spec: pb.BundleSpec, dest: Path) -> None:
    (dest / "bin").mkdir(parents=True)
    (dest / "bin" / spec.artifact_name).write_text("")


def _stage_script():
    spec = importlib.util.spec_from_file_location(
        "stage_pgwire_bundles", _REPO / "scripts" / "stage_pgwire_bundles.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_a_missing_bundle_is_refused_naming_it_and_the_fix(tmp_path, monkeypatch):
    _no_network(monkeypatch)
    spec = pb.bundle_spec_for("files")
    with pytest.raises(pb.BundleNotStaged) as refused:
        pb.BundleResolver(cache_root=tmp_path).resolve(spec)
    message = str(refused.value)
    assert "pgwire-file" in message and spec.version in message
    assert "stage_pgwire_bundles.py" in message


def test_a_missing_bundle_is_refused_inside_a_request_too(tmp_path, monkeypatch):
    # Not the background download the resolver started for a request: nothing is fetched at all.
    _no_network(monkeypatch)
    with request_deadline.within(30):
        with pytest.raises(pb.BundleNotStaged):
            pb.BundleResolver(cache_root=tmp_path).resolve(pb.bundle_spec_for("files"))
    assert not list(tmp_path.rglob("*.download"))


def test_a_staged_bundle_resolves_with_no_network(tmp_path, monkeypatch):
    _no_network(monkeypatch)
    spec = pb.bundle_spec_for("files")
    resolver = pb.BundleResolver(cache_root=tmp_path)
    _lay_down(spec, resolver.cached_path(spec))
    assert resolver.resolve(spec) == resolver.cached_path(spec)


def test_no_runtime_code_fetches_a_bundle():
    # download_release_asset is a build step: the resolver's own default, and every runtime caller of
    # it, would fetch from github.com at the first query that needs a bundle.
    offenders: list[str] = []
    runtime = _REPO / "provisa"
    for path in sorted(runtime.rglob("*.py")):
        if path == runtime / "runtime_deps" / "pgwire_bundles.py" or "_ui" in path.parts:
            continue
        if path == runtime / "runtime_deps" / "__init__.py":
            continue
        if path == runtime / "runtime_deps" / "stage_pgwire_bundles.py":
            continue  # the staging step, packaged so an install can run it; the server never imports it
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (isinstance(node, ast.Name) and node.id == "download_release_asset") or (
                isinstance(node, ast.Attribute) and node.attr == "download_release_asset"
            ):
                offenders.append(f"{path.relative_to(_REPO)}:{node.lineno}")
    assert offenders == []
    tree = ast.parse((runtime / "runtime_deps" / "pgwire_bundles.py").read_text(encoding="utf-8"))
    resolver = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "BundleResolver"
    )
    init = next(n for n in resolver.body if isinstance(n, ast.FunctionDef) and n.name == "__init__")
    assert "download_release_asset" not in ast.unparse(init)


def test_the_check_names_each_unstaged_bundle(tmp_path):
    resolver = pb.BundleResolver(cache_root=tmp_path)
    every = [pb.bundle_spec_for(t) for t in sorted(pb.BUNDLE_CONNECTOR)]
    assert {s.artifact_name for s in pb.missing_bundles(tmp_path)} == {
        s.artifact_name for s in every
    }
    for spec in every:
        if spec.connector not in ("splunk", "jira"):
            _lay_down(spec, resolver.cached_path(spec))
    assert sorted(s.connector for s in pb.missing_bundles(tmp_path)) == ["jira", "splunk"]
    with pytest.raises(pb.BundleNotStaged) as refused:
        pb.verify_staged(tmp_path)
    assert "pgwire-jira" in str(refused.value) and "pgwire-splunk" in str(refused.value)


def test_the_check_command_exits_non_zero_naming_what_is_missing(tmp_path):
    done = subprocess.run(
        [sys.executable, "-I", "-m", "provisa.runtime_deps.pgwire_bundles"],
        capture_output=True,
        text=True,
        cwd=_REPO,
        env={"PROVISA_RUNTIME_DEPS_CACHE": str(tmp_path), "PATH": ""},
        timeout=120,
    )
    assert done.returncode != 0
    assert "pgwire-file" in done.stdout + done.stderr


def test_staging_lays_down_every_bundle_and_leaves_none_missing(tmp_path):
    stage = _stage_script()
    laid: list[str] = []

    def _dl(spec: pb.BundleSpec, dest: Path) -> None:
        laid.append(spec.artifact_name)
        _lay_down(spec, dest)

    stage.stage(tmp_path, downloader=_dl)
    assert sorted(laid) == sorted(f"pgwire-{c}" for c in set(pb.BUNDLE_CONNECTOR.values()))
    assert pb.missing_bundles(tmp_path) == []
    pb.verify_staged(tmp_path)
    laid.clear()
    stage.stage(tmp_path, downloader=_dl)  # a staged bundle is not fetched again
    assert laid == []
