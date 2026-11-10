# Copyright (c) 2026 Kenneth Stott
# Canary: b2e8d1f4-6a73-4c09-9e5b-1f7a3c8d2e60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python source's part of its image (REQ-2098): the pinned Python and pyarrow for the image's
platform, the code's wheels installed for it (wheels only), the host and the code."""

# Requirements: REQ-2098
from __future__ import annotations

import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from provisa.codesource import oci
from provisa.codesource.image_build import (
    ROOT,
    ImageParts,
    Plan,
    files,
    runtime_layer,
    uv_install,
    uv_install_env,
)
from provisa.codesource.install import Http, Runner
from provisa.python import install
from provisa.python.install import LOCKFILES
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

__all__ = ["LOCKFILES", "fetch", "layers"]


def fetch(code: Any, registries: Sequence[Any], http: Http, dest: Path) -> Path:
    """The code, fetched by the language's install (looked up when called)."""
    return install.fetch(code, registries, http, dest)


def layers(
    plan: Plan, root: Path, target: CodeRuntimeResolver, arch: str, workdir: Path, run: Runner
) -> ImageParts:
    from provisa.python import install as python_install
    from provisa.python.source import HOST, code_path, entry_module

    server = CodeRuntimeResolver(cache_root=plan.cache_root)
    uv = server.resolve("uv")
    site = workdir / "site-packages"
    site.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="provisa-image-python-") as scratch:
        home = Path(scratch)
        env = uv_install_env(plan, home)
        install = [str(uv), *uv_install(arch, site)]
        lock = python_install.lockfile(root)
        if lock is not None:
            requirements = lock
            if lock.name == "uv.lock":
                requirements = home / "requirements.txt"
                run(
                    [str(uv), "export", "--frozen", "--no-emit-project", "--format",
                     "requirements-txt", "--output-file", str(requirements)],
                    root,
                    env,
                )  # fmt: skip
            hashed = ["--require-hashes"] if requirements.name.endswith(".txt") else []
            run([*install, *hashed, "-r", str(requirements)], root, env)
    version = target.pin("python").version.split("+", 1)[0]
    venv = [
        oci.Entry(
            f"{ROOT}/venv/pyvenv.cfg",
            data=(
                f"home = {ROOT}/runtime/python/bin\ninclude-system-site-packages = false\n"
                f"version = {version}\n"
            ).encode(),
        ),
        oci.Entry(f"{ROOT}/venv/bin/python", link=f"{ROOT}/runtime/python/bin/python3.12"),
        *oci.tree(site, f"{ROOT}/venv/lib/python3.12/site-packages"),
    ]
    code = code_path(root)
    entry = entry_module(code, plan.entry)
    layers = [
        runtime_layer(target, "python", f"{ROOT}/runtime"),
        runtime_layer(target, "pyarrow", f"{ROOT}/hostlib"),
        # REQ-2225: reader.py beside the host, for a script connector's tags read at join.
        files(HOST.parent, f"{ROOT}/host", ["host.py", "reader.py"]),
        # REQ-2106: Provisa's services module, beside the host as it is on the server.
        oci.tree(HOST.parent / "services", f"{ROOT}/host/services"),
        venv,
        oci.tree(code, f"{ROOT}/code"),
    ]
    worker = [
        f"{ROOT}/venv/bin/python",
        "-I",
        f"{ROOT}/host/host.py",
        "{fd}",
        f"{ROOT}/code",
        entry,
    ]
    env = {"PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"}
    # The code's own Python runs the front end too.
    return ImageParts(layers, worker, env, runs_front=True)
