# Copyright (c) 2026 Kenneth Stott
# Canary: 6d3f9a20-8c41-4e57-b1a6-2e0c7b5f9d38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Go source's part of its image (REQ-2098, REQ-2200): its host, compiled by the pinned Go for
the image's platform (``GOOS=linux``, the image's ``GOARCH``, no cgo) with the code linked in,
one static binary. The code's modules are fetched through the organisation's Go module proxy and
checked against its go.sum, as for an install on the server; nothing else of the code, and no
runtime, goes into the image. The binary reads the base's certificates; the front end runs on the
Python every image without one of its own carries."""

# Requirements: REQ-2098, REQ-2200
from __future__ import annotations

import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from provisa.codesource import oci
from provisa.codesource.image_build import ROOT, ImageParts, Plan
from provisa.codesource.install import Http, Runner
from provisa.golang import install
from provisa.golang.build import build_host
from provisa.golang.install import LOCKFILES, MODULES, check_lockfile, install_environment
from provisa.golang.reader import pattern, run_go
from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver

__all__ = ["LOCKFILES", "fetch", "layers"]


def fetch(code: Any, registries: Sequence[Any], http: Http, dest: Path) -> Path:
    """The code, fetched by the language's install (looked up when called)."""
    return install.fetch(code, registries, http, dest)


#: Where the host is in the image.
HOST = f"{ROOT}/host/host"


def layers(
    plan: Plan, root: Path, target: CodeRuntimeResolver, arch: str, workdir: Path, run: Runner
) -> ImageParts:
    go = CodeRuntimeResolver(cache_root=plan.cache_root).resolve("go")
    package = plan.entry or "."
    check_lockfile(root)
    gocache = plan.cache_root / "code" / "go-build"
    gocache.mkdir(parents=True, exist_ok=True)
    binary = workdir / "go-host" / "host"
    with tempfile.TemporaryDirectory(prefix="provisa-image-go-") as scratch:
        env = install_environment(go, root, plan.registries, Path(scratch), gocache)
        env.update(GOOS="linux", GOARCH=arch)
        # Every module the package needs on the image's platform, each checked against go.sum.
        run_go([str(go), "list", "-deps", pattern(package)], env, root)
        build_host(
            go=go,
            code=root,
            package=package,
            exports=plan.exports,
            gomodcache=root / MODULES,
            proxy=env["GOPROXY"],
            dest=binary,
            extra_env={
                k: env[k]
                for k in ("NETRC", "SSL_CERT_FILE", "GOCACHE", "GOOS", "GOARCH")
                if k in env
            },
        )
    host = [
        oci.Entry(HOST, source=binary, mode=0o755),
        # The image's working directory: the code is linked into the host, so nothing is here.
        oci.Entry(f"{ROOT}/code", directory=True, mode=0o755),
    ]
    return ImageParts([host], [HOST, "{fd}"], {})
