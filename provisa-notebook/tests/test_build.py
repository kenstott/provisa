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
        "pandas": {"depends": ["numpy", "python-dateutil"]},
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
