# Copyright (c) 2026 Kenneth Stott
# Canary: 4e0b7c1a-93d2-4f6e-8a15-c2d7f0b9e361
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python source's runtime on the server (REQ-1993; maintainer 2026-11-22, a pip-installed Provisa
fetches only from PyPI, Maven Central and npm): the base CPython Provisa runs on -- the one pip
installed Provisa into, or an installer's bundled python-build-standalone -- and uv from its PyPI
wheel. No python-build-standalone or uv release is fetched from GitHub; the pinned CPython is only
for an image built for a remote host's platform."""

# Requirements: REQ-1993, REQ-2098
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from provisa.codesource.source import CodeConfig, Context
from provisa.runtime_deps import code_runtimes
from provisa.runtime_deps.code_runtimes import (
    RuntimeUnavailable,
    running_interpreter,
    uv_executable,
)

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]
_EXE = "python.exe" if os.name == "nt" else f"bin/python{sys.version_info[0]}.{sys.version_info[1]}"


def test_the_interpreter_is_the_base_of_the_one_provisa_runs_on():
    runtime = running_interpreter()
    assert runtime.home == Path(sys.base_prefix).resolve()
    assert runtime.executable == runtime.home / _EXE
    assert runtime.executable.is_file()


def test_an_installers_bundled_interpreter_is_the_one_taken(tmp_path, monkeypatch):
    """An installer runs Provisa on its python-build-standalone (``python/bin/python3.12``):
    that prefix's interpreter is the one a source runs on, with nothing fetched."""
    bundled = tmp_path / "runtime" / "python"
    (bundled / _EXE).parent.mkdir(parents=True)
    (bundled / _EXE).write_text("")
    monkeypatch.setattr(sys, "base_prefix", str(bundled))
    monkeypatch.setattr(code_runtimes, "fetch_https", _no_fetch)
    runtime = running_interpreter()
    assert runtime.executable == bundled.resolve() / _EXE
    assert runtime.home == bundled.resolve()


def test_a_prefix_without_its_interpreter_fails_loud(tmp_path, monkeypatch):
    monkeypatch.setattr(sys, "base_prefix", str(tmp_path))
    with pytest.raises(RuntimeUnavailable, match="has no"):
        running_interpreter()


def test_uv_is_the_binary_of_its_pypi_wheel():
    from uv import find_uv_bin

    uv = uv_executable()
    assert uv == Path(find_uv_bin())
    assert uv.is_file()


def test_no_uv_release_is_pinned_and_no_server_code_resolves_the_pinned_python():
    """uv comes from PyPI, so no uv release is pinned; the pinned CPython is taken only by the
    image builds, for the image's platform."""
    assert "uv" not in code_runtimes.RUNTIMES
    resolving = [
        path.relative_to(REPO).as_posix()
        for path in sorted((REPO / "provisa").rglob("*.py"))
        if 'resolve("python")' in path.read_text(encoding="utf-8")
        or 'pin("python")' in path.read_text(encoding="utf-8")
    ]
    assert resolving == ["provisa/codesource/image_build.py", "provisa/python/image.py"]


class _Stop(Exception):
    pass


def _no_fetch(url: str) -> bytes:
    raise AssertionError(f"fetched {url}")


def test_preparing_a_source_builds_no_url_and_installs_from_the_running_interpreter(
    tmp_path, monkeypatch
):
    from provisa.python import install
    from provisa.python import prepare as python_prepare

    fetched: list[str] = []

    def _record(url: str) -> bytes:
        fetched.append(url)
        raise AssertionError(f"fetched {url}")

    taken: dict = {}

    def _ensure(**kwargs):
        taken.update(kwargs)
        raise _Stop

    monkeypatch.setattr(code_runtimes, "fetch_https", _record)
    monkeypatch.setattr(install, "ensure_installed", _ensure)
    config = CodeConfig("python", "python", {"kind": "upload"}, None, None, None)
    ctx = Context(tmp_path, [], [], None, "org-1")  # type: ignore[arg-type]
    with pytest.raises(_Stop):
        python_prepare.prepare("rates-src", config, object(), None, ctx)
    assert fetched == []
    assert not list(tmp_path.rglob("*"))  # nothing downloaded or cached
    assert taken["python"] == running_interpreter().executable
    assert taken["uv"] == uv_executable()
