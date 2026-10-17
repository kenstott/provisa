# Copyright (c) 2026 Kenneth Stott
# Canary: 8b80fb01-d40f-4c36-8498-b5adc6bc74ec
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The in-browser Python Provisa's UI serves from this package (``python -m
provisa_notebook.build`` fills ``assets/`` when the package is built)."""

from __future__ import annotations

from pathlib import Path

#: The one Pyodide every in-browser Python in Provisa runs on.
PYODIDE_VERSION = "0.27.7"
#: The packages shipped beside it (their dependencies come with them). The notebook's kernel
#: (jupyterlite-pyodide-kernel) loads ssl, sqlite3 and ipython itself when it starts.
PACKAGES = (
    "numpy",
    "pandas",
    "pyarrow",
    "scikit-learn",
    "matplotlib",
    "micropip",
    "ssl",
    "sqlite3",
    "ipython",
    "jedi",
)

ASSETS = Path(__file__).resolve().parent / "assets"


def lite_dir() -> Path:
    """The directory served at ``/lite``: the JupyterLite site the Jupyter Notebook tab embeds.
    Raises when the package was built without its assets."""
    path = ASSETS / "lite"
    if not (path / "jupyter-lite.json").is_file():
        raise FileNotFoundError(
            f"{path} has no JupyterLite: build the package with `python -m provisa_notebook.build`"
        )
    return path


def pyodide_dir() -> Path:
    """The directory served at ``/pyodide``: Pyodide, its packages and the provisa-client wheel.
    Raises when the package was built without its assets."""
    path = ASSETS / "pyodide"
    if not (path / "pyodide-lock.json").is_file():
        raise FileNotFoundError(
            f"{path} has no Pyodide: build the package with `python -m provisa_notebook.build`"
        )
    return path
