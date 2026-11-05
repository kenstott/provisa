# Copyright (c) 2026 Kenneth Stott
# Canary: e4c71a39-0d58-4b26-8f13-6a9e2b5d7c81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Node source's part of its image (REQ-2098): the pinned runtime and Arrow library for the
image's platform, the code's packages installed for it (no install scripts), the host and the
code.

Deno installs npm packages for the machine it runs on and has no way to name another platform, so
a Deno source's image is built from the server's own install, and is refused by name when the
server's platform is not the image's and the code's lockfile holds packages for a platform."""

# Requirements: REQ-2098
from __future__ import annotations

import json
import platform
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from provisa.codesource import oci
from provisa.codesource.image_build import (
    CPU,
    PLATFORM,
    ROOT,
    ImageBuildRefused,
    ImageParts,
    Plan,
    files,
    runtime_layer,
    scratch_env,
)
from provisa.codesource.install import Http, Runner
from provisa.node import install
from provisa.node.install import LOCKFILES as _BY_RUNTIME
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

__all__ = ["LOCKFILES", "fetch", "layers"]

#: Every lockfile a Node, Bun or Deno install reads.
LOCKFILES = tuple(n for names in _BY_RUNTIME.values() for n in names)


def fetch(code: Any, registries: Sequence[Any], http: Http, dest: Path) -> Path:
    """The code, fetched by the language's install (looked up when called)."""
    return install.fetch(code, registries, http, dest)


def _deno_platform_packages(root: Path) -> list[str]:
    """The npm packages a Deno lockfile holds for one platform only."""
    lock = root / "deno.lock"
    if not lock.is_file():
        return []
    packages = (json.loads(lock.read_text()).get("npm") or {}).items()
    return sorted(name for name, entry in packages if entry.get("os") or entry.get("cpu"))


def layers(
    plan: Plan, root: Path, target: CodeRuntimeResolver, arch: str, workdir: Path, run: Runner
) -> ImageParts:
    from provisa.node import install as node_install
    from provisa.node.worker import HOST, entry_module

    runtime = plan.runtime
    if node_install.needs_install(root, runtime):
        server = CodeRuntimeResolver(cache_root=plan.cache_root)
        executable = server.resolve(runtime)
        with tempfile.TemporaryDirectory(prefix="provisa-image-node-") as scratch:
            home = Path(scratch)
            rc = home / ".npmrc"
            env = scratch_env(plan.registries, home)
            cafile = env.get("SSL_CERT_FILE")
            rc.write_text(node_install.npmrc(plan.registries, Path(cafile) if cafile else None))
            (root / ".npmrc").write_text(rc.read_text())
            env.update(
                {
                    "NPM_CONFIG_USERCONFIG": str(rc),
                    "npm_config_cache": str(home / "npm-cache"),
                    "BUN_INSTALL_CACHE_DIR": str(home / "bun-cache"),
                    "DENO_DIR": str(root / ".deno"),
                }
            )
            if runtime == "node":
                argv = [
                    *node_install.install_command("node", executable),
                    "--os=linux", f"--cpu={CPU[arch]}", "--libc=glibc",
                ]  # fmt: skip
            elif runtime == "bun":
                argv = [
                    *node_install.install_command("bun", executable),
                    "--os=linux",
                    f"--cpu={CPU[arch]}",
                ]
            else:
                here = (platform.system(), platform.machine().replace("arm64", "aarch64"))
                held = _deno_platform_packages(root)
                if here != PLATFORM[arch] and held:
                    raise ImageBuildRefused(
                        "deno_platform",
                        "Deno installs npm packages for the machine it runs on; this server is "
                        f"not {arch} Linux, and the lockfile holds packages for one platform: "
                        f"{', '.join(held[:5])}",
                    )
                argv = node_install.install_command("deno", executable)
            try:
                run(argv, root, env)
            finally:
                (root / ".npmrc").unlink(missing_ok=True)
    exe = f"{ROOT}/runtime/{target.pin(runtime).asset(PLATFORM[arch]).executable}"
    entry = entry_module(root).relative_to(root.resolve()).as_posix()
    host = [f"{ROOT}/host/host.mjs", "{fd}", f"{ROOT}/code/{entry}"]
    if runtime == "deno":
        worker = [
            exe, "run", "--no-prompt", "--allow-net", f"--allow-read={ROOT}", "--allow-env",
            f"--import-map={ROOT}/host/services-import-map.json", *host,
        ]  # fmt: skip
        env = {"DENO_DIR": f"{ROOT}/code/.deno", "NO_COLOR": "1"}
    else:
        worker = [*node_install.code_command(runtime, exe), *host]
        env = {"NO_COLOR": "1"}
    layers = [
        runtime_layer(target, runtime, f"{ROOT}/runtime"),
        runtime_layer(target, "apache-arrow", f"{ROOT}/hostlib"),
        files(
            HOST.parent,
            f"{ROOT}/host",
            # REQ-2106: "@provisa/services", and Deno's import map naming it.
            ["host-core.mjs", "host.mjs", "services.mjs", "services-import-map.json"],
        ),
        oci.tree(root, f"{ROOT}/code"),
    ]
    return ImageParts(layers, worker, env)
