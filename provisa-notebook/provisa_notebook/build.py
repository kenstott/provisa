# Copyright (c) 2026 Kenneth Stott
# Canary: 174f9ffe-a994-499b-a2f8-1468c5d6ea6e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fill ``assets/pyodide`` for a build of provisa-notebook: ``python -m provisa_notebook.build``.

Pyodide's core files and the packages in :data:`PACKAGES` with their dependencies are downloaded
from Pyodide's release CDN and checked against the lock file's sha256. The lock written beside
them lists only what is shipped, so an import of anything else fails here, by name, rather than
reaching for a network the deployment may not have. The provisa-client wheel is built from this
repository's ``provisa-client`` and recorded in ``manifest.json``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

from provisa_notebook import ASSETS, PACKAGES, PYODIDE_VERSION

CDN = f"https://cdn.jsdelivr.net/pyodide/v{PYODIDE_VERSION}/full/"
#: The JupyterLite the Jupyter Notebook tab embeds, built in a virtualenv of its own: nothing of it
#: is a dependency of provisa or of this package.
LITE = ("jupyterlite-core==0.5.1", "jupyterlite-pyodide-kernel==0.5.2", "jupyter-server")
#: What the notebook's kernel installs at start that neither Pyodide nor the kernel's own index
#: carries; added to the site's own wheel index so the kernel never reaches for PyPI.
KERNEL_WHEELS = ("comm==0.2.2",)
#: The notebook's kernel runs on the Pyodide served at /pyodide (never a CDN or PyPI), starts with
#: provisa-client loaded, and the app is reachable from the Provisa page that embeds it
#: (exposeAppInBrowser), which hands each kernel the signed-in user.
LITE_CONFIG = {
    "jupyter-lite-schema-version": 0,
    "jupyter-config-data": {
        "appName": "Provisa notebook",
        "exposeAppInBrowser": True,
        "litePluginSettings": {
            "@jupyterlite/pyodide-kernel-extension:kernel": {
                "pyodideUrl": "/pyodide/pyodide.js",
                "disablePyPIFallback": True,
                "loadPyodideOptions": {"packages": ["provisa-client"]},
            }
        },
    },
}
NOTEBOOKS = Path(__file__).resolve().parent / "notebooks"
CORE = ("pyodide.js", "pyodide.mjs", "pyodide.asm.js", "pyodide.asm.wasm", "python_stdlib.zip")
REPO = Path(__file__).resolve().parents[2]


def _canonical(name: str) -> str:
    """A package name as the lock keys it (PEP 503): its depends lists spell some differently."""
    return re.sub(r"[-_.]+", "-", name).lower()


def closure(lock: dict, names: tuple[str, ...]) -> list[str]:
    """``names`` and every package they depend on, by the lock's names, sorted."""
    packages = lock["packages"]
    seen: set[str] = set()
    todo = [_canonical(n) for n in names]
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        if name not in packages:
            raise KeyError(f"Pyodide {PYODIDE_VERSION} has no package {name!r}")
        seen.add(name)
        todo.extend(_canonical(d) for d in packages[name]["depends"])
    return sorted(seen)


def _fetch(name: str, dest: Path, sha256: str | None = None) -> None:
    with urllib.request.urlopen(CDN + name) as r:  # noqa: S310 -- the pinned release CDN
        data = r.read()
    if sha256 is not None and hashlib.sha256(data).hexdigest() != sha256:
        raise ValueError(f"{name}: sha256 does not match Pyodide {PYODIDE_VERSION}'s lock")
    dest.write_bytes(data)


def build(out: Path = ASSETS) -> dict:
    target = out / "pyodide"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    with urllib.request.urlopen(CDN + "pyodide-lock.json") as r:  # noqa: S310
        lock = json.load(r)
    for name in CORE:
        _fetch(name, target / name)
    shipped = closure(lock, PACKAGES)
    for name in shipped:
        entry = lock["packages"][name]
        _fetch(entry["file_name"], target / entry["file_name"], entry["sha256"])
    lock["packages"] = {name: lock["packages"][name] for name in shipped}
    wheels = target / "wheels"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--quiet",
            "--no-deps",
            "--wheel-dir",
            str(wheels),
            str(REPO / "provisa-client"),
        ],
        check=True,
    )
    [client] = sorted(wheels.glob("provisa_client-*.whl"))
    # provisa-client is a package of this Pyodide like the others: `import provisa_client` (and
    # `%pip install provisa-client` in the notebook) load it, with pandas and pyarrow, from here.
    version = client.name.split("-")[1]
    lock["packages"]["provisa-client"] = {
        "name": "provisa-client",
        "version": version,
        "file_name": f"wheels/{client.name}",
        "install_dir": "site",
        "sha256": hashlib.sha256(client.read_bytes()).hexdigest(),
        "package_type": "package",
        "imports": ["provisa_client"],
        "depends": ["pandas", "pyarrow"],
        "unvendored_tests": False,
    }
    (target / "pyodide-lock.json").write_text(json.dumps(lock))
    build_lite(out / "lite")
    manifest = {
        "pyodide": PYODIDE_VERSION,
        "packages": shipped,
        "client_wheel": f"wheels/{client.name}",
    }
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def build_lite(target: Path) -> None:
    """The JupyterLite site served at /lite, its contents the starter notebooks."""
    if target.exists():
        shutil.rmtree(target)
    with tempfile.TemporaryDirectory() as tmp:
        venv = Path(tmp) / "venv"
        subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        bindir = venv / ("Scripts" if os.name == "nt" else "bin")
        python = bindir / ("python.exe" if os.name == "nt" else "python")
        subprocess.run([str(python), "-m", "pip", "install", "--quiet", *LITE], check=True)
        wheels = Path(tmp) / "wheels"
        subprocess.run(
            [
                str(python),
                "-m",
                "pip",
                "download",
                "--quiet",
                "--no-deps",
                "--only-binary=:all:",
                "--dest",
                str(wheels),
                *KERNEL_WHEELS,
            ],
            check=True,
        )
        src = Path(tmp) / "src"
        shutil.copytree(NOTEBOOKS, src / "files")
        (src / "jupyter-lite.json").write_text(json.dumps(LITE_CONFIG, indent=2))
        subprocess.run(
            [
                str(python),
                "-m",
                "jupyterlite_core",
                "build",
                "--contents",
                "files",
                *[f"--piplite-wheels={wheel}" for wheel in sorted(wheels.glob("*.whl"))],
                "--output-dir",
                str(target),
            ],
            cwd=src,
            check=True,
            stdout=subprocess.DEVNULL,
        )


if __name__ == "__main__":
    json.dump(build(), sys.stdout, indent=2)
