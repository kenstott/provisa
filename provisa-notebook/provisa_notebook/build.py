# Copyright (c) 2026 Kenneth Stott
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
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

from provisa_notebook import ASSETS, PACKAGES, PYODIDE_VERSION

CDN = f"https://cdn.jsdelivr.net/pyodide/v{PYODIDE_VERSION}/full/"
CORE = ("pyodide.js", "pyodide.mjs", "pyodide.asm.js", "pyodide.asm.wasm", "python_stdlib.zip")
REPO = Path(__file__).resolve().parents[2]


def closure(lock: dict, names: tuple[str, ...]) -> list[str]:
    """``names`` and every package they depend on, by the lock's names, sorted."""
    packages = lock["packages"]
    seen: set[str] = set()
    todo = [n.lower() for n in names]
    while todo:
        name = todo.pop()
        if name in seen:
            continue
        if name not in packages:
            raise KeyError(f"Pyodide {PYODIDE_VERSION} has no package {name!r}")
        seen.add(name)
        todo.extend(d.lower() for d in packages[name]["depends"])
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
    (target / "pyodide-lock.json").write_text(json.dumps(lock))
    wheels = target / "wheels"
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(wheels), str(REPO / "provisa-client")],
        check=True,
    )
    [client] = sorted(wheels.glob("provisa_client-*.whl"))
    manifest = {
        "pyodide": PYODIDE_VERSION,
        "packages": shipped,
        "client_wheel": f"wheels/{client.name}",
    }
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


if __name__ == "__main__":
    json.dump(build(), sys.stdout, indent=2)
