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
    monkeypatch.setattr(build, "_download", lambda url: json.dumps(lock).encode())
    monkeypatch.setattr(build, "_fetch", lambda name, dest, sha256=None: fetched.append(name))
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
