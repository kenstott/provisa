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
