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
#: The sha256 of that release's lock file and core files, as jsDelivr and the npm package
#: ``pyodide`` both serve them. The lock carries the sha256 of every package, so every file a build
#: fetches is checked against a sum recorded here or reached from one. A bump of PYODIDE_VERSION
#: changes every sum with it.
PYODIDE_SHA256 = {
    "pyodide-lock.json": "9c45b916001a750f4102fc287494f3eab215909c7535c626a20db80fd6333e2c",
    "pyodide.js": "b4cb23a53aba19c221659b9fb40a2f18281d685691dc06647fb0afd0681baf98",
    "pyodide.mjs": "04c052c815abb0bf2981fa38c2dfa58e3120ee5288c956df2b2a15e301782c5f",
    "pyodide.asm.js": "6b4c90de5b7172873f04f21884d0e9d2274e305fe32116558fe3e4fbe3618d51",
    "pyodide.asm.wasm": "a50dd1843f805a0b7c45b61037ee0d7b26dfe85efe0e18ef95a34ad24e401f5f",
    "python_stdlib.zip": "16611534726e5d8ac2bd8f926410b2dcb8d6f49aa24913463533b457a2115c16",
}
#: The packages shipped beside it (their dependencies come with them). The notebook's kernel
#: (jupyterlite-pyodide-kernel) loads ssl, sqlite3 and ipython itself when it starts.
PACKAGES = (
    "numpy",
    "pandas",
    "pyarrow",
    "scikit-learn",
    "matplotlib",
    "networkx",
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
