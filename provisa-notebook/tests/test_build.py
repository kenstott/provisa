# Copyright (c) 2026 Kenneth Stott
# Canary: 4b658145-957d-4a2d-82fe-2b26a6f735ad
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What provisa-notebook ships: the named packages and everything they depend on, and nothing
served when it was built without its assets."""

# Requirements: REQ-2071

import pytest

import provisa_notebook
from provisa_notebook.build import closure

LOCK = {
    "packages": {
        "pandas": {"depends": ["numpy", "python_dateutil"]},
        "numpy": {"depends": []},
        "python-dateutil": {"depends": ["six"]},
        "six": {"depends": []},
        "scipy": {"depends": ["numpy", "openblas"]},
        "openblas": {"depends": []},
        "unused": {"depends": []},
    }
}


def test_the_shipped_packages_are_the_named_ones_and_their_dependencies():
    assert closure(LOCK, ("pandas", "SciPy")) == [
        "numpy",
        "openblas",
        "pandas",
        "python-dateutil",
        "scipy",
        "six",
    ]


def test_a_package_pyodide_does_not_have_is_refused_by_name():
    with pytest.raises(KeyError, match="no package 'polars'"):
        closure(LOCK, ("polars",))


def test_a_package_built_without_its_assets_says_so(tmp_path, monkeypatch):
    monkeypatch.setattr(provisa_notebook, "ASSETS", tmp_path)
    with pytest.raises(FileNotFoundError, match="python -m provisa_notebook.build"):
        provisa_notebook.pyodide_dir()
    (tmp_path / "pyodide").mkdir()
    (tmp_path / "pyodide" / "pyodide-lock.json").write_text("{}")
    assert provisa_notebook.pyodide_dir() == tmp_path / "pyodide"


def test_the_fingerprint_follows_what_the_assets_are_built_from(tmp_path, monkeypatch):
    from provisa_notebook import build

    notebooks = tmp_path / "notebooks"
    notebooks.mkdir()
    (notebooks / "Provisa.ipynb").write_text("{}")
    monkeypatch.setattr(build, "NOTEBOOKS", notebooks)
    first = build.fingerprint()
    assert build.fingerprint() == first
    (notebooks / "Provisa.ipynb").write_text('{"changed": true}')
    assert build.fingerprint() != first


def test_built_assets_are_current_only_when_their_manifest_records_this_fingerprint(tmp_path):
    import json

    from provisa_notebook import build

    assert not build.is_current(tmp_path)
    (tmp_path / "pyodide").mkdir()
    (tmp_path / "lite").mkdir()
    manifest = tmp_path / "pyodide" / "manifest.json"
    manifest.write_text(json.dumps({"build": build.fingerprint()}))
    assert not build.is_current(tmp_path)  # no notebook site: built before it existed
    (tmp_path / "lite" / "jupyter-lite.json").write_text("{}")
    assert build.is_current(tmp_path)
    manifest.write_text(json.dumps({"build": "an older build"}))
    assert not build.is_current(tmp_path)


# The splash plugin's removal as jupyterlite-core 0.5.1's jlab_core bundle minifies it.
_SPLASH_JS = (
    's.classList.add("splash-fade"),window.setTimeout((()=>{document.body.removeChild(s)}),200)'
    '))})))}}},te={id:"@jupyterlab/apputils-extension:print"'
)


def test_the_splash_is_removed_without_throwing_once_it_is_gone():
    from provisa_notebook.build import detach_splash_safely

    out = detach_splash_safely(_SPLASH_JS, "jlab_core.x.js")
    assert "window.setTimeout((()=>{s.remove()}),200)" in out
    assert "removeChild" not in out
    assert out.replace("s.remove()", "document.body.removeChild(s)") == _SPLASH_JS


@pytest.mark.parametrize("js", ["no splash here", _SPLASH_JS + _SPLASH_JS])
def test_a_bundle_without_exactly_one_splash_removal_is_refused(js):
    from provisa_notebook.build import detach_splash_safely

    with pytest.raises(RuntimeError, match="jlab_core.x.js: expected one splash removal"):
        detach_splash_safely(js, "jlab_core.x.js")


def test_the_build_patches_its_one_core_bundle(tmp_path):
    from provisa_notebook.build import _patch_splash

    (tmp_path / "build").mkdir()
    bundle = tmp_path / "build" / "jlab_core.abc123.js"
    bundle.write_text(_SPLASH_JS)
    _patch_splash(tmp_path)
    assert "s.remove()" in bundle.read_text()


def test_the_built_lock_and_wheels_carry_networkx_and_its_dependencies(tmp_path, monkeypatch):
    import json
    import subprocess

    from provisa_notebook import PACKAGES, build

    # Pyodide's lock as 0.27.7 has it for networkx; every other shipped package stands alone.
    depends = {"networkx": ["decorator", "setuptools", "matplotlib", "numpy"]}
    names = {*PACKAGES, "decorator", "setuptools", "unshipped"}
    lock = {
        "info": {"version": "0.27.7"},
        "packages": {
            n: {"name": n, "file_name": f"{n}.whl", "sha256": n, "depends": depends.get(n, [])}
            for n in names
        },
    }
    fetched: list[str] = []

    def fetch(name, sha256):  # each file's bytes, as build._fetch returns them once checked
        fetched.append(name)
        return json.dumps(lock).encode() if name == build.LOCK else b"file"

    monkeypatch.setattr(build, "_fetch", fetch)
    monkeypatch.setattr(build, "build_lite", lambda target: None)
    monkeypatch.setattr(build, "fingerprint", lambda: "fp")

    def wheel(cmd, check):  # the provisa-client wheel pip would build
        out = tmp_path / "pyodide" / "wheels"
        out.mkdir(parents=True)
        (out / "provisa_client-1.0.0-py3-none-any.whl").write_bytes(b"whl")
        return subprocess.CompletedProcess(cmd, 0)

    monkeypatch.setattr(build.subprocess, "run", wheel)
    manifest = build.build(tmp_path)
    written = json.loads((tmp_path / "pyodide" / "pyodide-lock.json").read_text())["packages"]
    for name in ("networkx", "decorator", "setuptools"):
        assert name in manifest["packages"]
        assert name in written
        assert f"{name}.whl" in fetched
    assert "unshipped" not in written


def _windows_default_text_encoding(monkeypatch):
    """Makes an unnamed text encoding mean cp1252, as it does on a Windows build runner."""
    import pathlib

    read_text, write_text = pathlib.Path.read_text, pathlib.Path.write_text

    def read(self, encoding=None, errors=None, **kw):
        return read_text(self, encoding or "cp1252", errors, **kw)

    def write(self, data, encoding=None, errors=None, newline=None):
        return write_text(self, data, encoding or "cp1252", errors, newline)

    monkeypatch.setattr(pathlib.Path, "read_text", read)
    monkeypatch.setattr(pathlib.Path, "write_text", write)


def test_the_core_bundle_is_patched_as_utf8_whatever_the_os_default(tmp_path, monkeypatch):
    from provisa_notebook.build import _patch_splash

    _windows_default_text_encoding(monkeypatch)
    (tmp_path / "build").mkdir()
    bundle = tmp_path / "build" / "jlab_core.abc123.js"
    # U+03CF encodes as CF 8F; 0x8f has no cp1252 mapping, as in the failing Windows build.
    bundle.write_bytes(("const k='Ϗ';" + _SPLASH_JS).encode("utf-8"))
    _patch_splash(tmp_path)
    out = bundle.read_bytes().decode("utf-8")
    assert out.startswith("const k='Ϗ';")
    assert "s.remove()" in out


def test_every_text_read_and_write_in_the_build_names_its_encoding():
    import ast
    import inspect

    from provisa_notebook import build

    calls = [
        node
        for node in ast.walk(ast.parse(inspect.getsource(build)))
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Attribute) and node.func.attr in ("read_text", "write_text"))
            or (isinstance(node.func, ast.Name) and node.func.id == "open")
        )
    ]
    assert calls
    unnamed = [node.lineno for node in calls if not any(k.arg == "encoding" for k in node.keywords)]
    assert unnamed == []


# ── what a build downloads: every file pinned, and kept so it is downloaded once ──────────────
#
# An image build that asked Pyodide's CDN for each of its files was failed by one refused request
# (run 43879947072: "HTTP Error 503: No healthy backends" inside `docker build`).


def _sha(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def _no_network(monkeypatch):
    from provisa_notebook import build

    def refuse(url):
        raise AssertionError(f"asked the network for {url}")

    monkeypatch.setattr(build, "_download", refuse)


def test_the_lock_and_every_core_file_have_a_recorded_sha256():
    from provisa_notebook import build

    assert set(provisa_notebook.PYODIDE_SHA256) == {build.LOCK, *build.CORE}
    for name, digest in provisa_notebook.PYODIDE_SHA256.items():
        assert len(digest) == 64 and set(digest) <= set("0123456789abcdef"), name


def test_a_build_with_no_downloads_directory_downloads_and_keeps_nothing(tmp_path, monkeypatch):
    from provisa_notebook import build

    monkeypatch.delenv(build.DOWNLOADS_ENV, raising=False)
    monkeypatch.setattr(build, "_download", lambda url: b"core")
    assert build.downloads_dir() is None
    assert build._fetch("pyodide.js", _sha(b"core")) == b"core"
    assert list(tmp_path.iterdir()) == []


def test_a_downloaded_file_is_kept_under_the_pyodide_version_and_not_downloaded_again(
    tmp_path, monkeypatch
):
    from provisa_notebook import build

    monkeypatch.setenv(build.DOWNLOADS_ENV, str(tmp_path))
    asked = []

    def download(url):
        asked.append(url)
        return b"wheel"

    monkeypatch.setattr(build, "_download", download)
    assert build._fetch("numpy.whl", _sha(b"wheel")) == b"wheel"
    kept = tmp_path / provisa_notebook.PYODIDE_VERSION
    assert [p.name for p in kept.iterdir()] == ["numpy.whl"]
    assert asked == [build.CDN + "numpy.whl"]
    _no_network(monkeypatch)
    assert build._fetch("numpy.whl", _sha(b"wheel")) == b"wheel"


def test_a_download_that_is_not_the_pinned_file_is_refused_and_not_kept(tmp_path, monkeypatch):
    from provisa_notebook import build

    monkeypatch.setenv(build.DOWNLOADS_ENV, str(tmp_path))
    monkeypatch.setattr(build, "_download", lambda url: b"something else")
    with pytest.raises(ValueError, match="numpy.whl: sha256 does not match"):
        build._fetch("numpy.whl", _sha(b"wheel"))
    assert not (tmp_path / provisa_notebook.PYODIDE_VERSION).exists()


def test_a_kept_file_that_is_not_the_pinned_one_is_refused_by_path(tmp_path, monkeypatch):
    from provisa_notebook import build

    monkeypatch.setenv(build.DOWNLOADS_ENV, str(tmp_path))
    kept = tmp_path / provisa_notebook.PYODIDE_VERSION
    kept.mkdir()
    (kept / "numpy.whl").write_bytes(b"cut short")
    _no_network(monkeypatch)
    with pytest.raises(ValueError, match=r"numpy\.whl: sha256 is not Pyodide"):
        build._fetch("numpy.whl", _sha(b"wheel"))


def _a_pyodide(monkeypatch):
    """A stand-in release: a lock of two packages (one depending on the other) and the core
    files, with the sums a build checks them against. Returns {url: bytes}."""
    import json

    from provisa_notebook import build

    files = {name: f"core {name}".encode() for name in build.CORE}
    files["a.whl"] = b"package a"
    files["b.whl"] = b"package b"
    lock = {
        "packages": {
            "a": {"file_name": "a.whl", "sha256": _sha(files["a.whl"]), "depends": ["b"]},
            "b": {"file_name": "b.whl", "sha256": _sha(files["b.whl"]), "depends": []},
            "unshipped": {"file_name": "c.whl", "sha256": _sha(b"c"), "depends": []},
        }
    }
    files[build.LOCK] = json.dumps(lock).encode()
    monkeypatch.setattr(build, "PACKAGES", ("a",))
    monkeypatch.setattr(
        build,
        "PYODIDE_SHA256",
        {name: _sha(files[name]) for name in (build.LOCK, *build.CORE)},
    )
    return {build.CDN + name: data for name, data in files.items()}


def test_download_only_keeps_every_file_a_build_fetches_and_a_second_asks_for_nothing(
    tmp_path, monkeypatch
):
    from provisa_notebook import build

    served = _a_pyodide(monkeypatch)
    monkeypatch.setenv(build.DOWNLOADS_ENV, str(tmp_path))
    asked = []

    def download(url):
        asked.append(url)
        return served[url]

    monkeypatch.setattr(build, "_download", download)
    names = build.download_only()
    assert sorted(names) == sorted([build.LOCK, *build.CORE, "a.whl", "b.whl"])
    kept = tmp_path / provisa_notebook.PYODIDE_VERSION
    assert sorted(p.name for p in kept.iterdir()) == sorted(names)
    assert sorted(asked) == sorted(build.CDN + name for name in names)
    _no_network(monkeypatch)
    assert sorted(build.download_only()) == sorted(names)


def test_download_only_without_a_directory_says_which_variable_names_it(monkeypatch):
    from provisa_notebook import build

    monkeypatch.delenv(build.DOWNLOADS_ENV, raising=False)
    _no_network(monkeypatch)
    with pytest.raises(RuntimeError, match="PROVISA_NOTEBOOK_DOWNLOADS"):
        build.download_only()
