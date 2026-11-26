# Copyright (c) 2026 Kenneth Stott
# Canary: 98372727-e372-4ec0-b28a-805a0b324150
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Go author's module that requires Provisa's Go SDK (REQ-1984), built and read the way Provisa
reads a Go source: the pinned toolchain, no network, the SDK replaced by the repository's copy
(``provisa-sdk/go``) or by another directory standing for a different SDK version."""

# Requirements: REQ-1984
from __future__ import annotations

import subprocess  # nosec B404 - argv-list invocation of the pinned Go toolchain
from dataclasses import dataclass
from pathlib import Path

from provisa.codesource.manifest import ManifestRefused, manifest_json
from provisa.golang.reader import ensure_reader, go_environment, read_code, services_go_mod
from provisa.runtime_deps.code_runtimes import RUNTIMES, CodeRuntimeResolver

REPO = Path(__file__).resolve().parents[3]
SDK = REPO / "provisa-sdk" / "go"
SDK_MODULE = "provisa.dev/sdk/go"
#: The proxy the reader's own dependency (golang.org/x/tools) is fetched through.
PROXY = "https://proxy.golang.org"


def go_mod(sdk: Path = SDK) -> str:
    return (
        f"module example.com/source\n\ngo 1.23\n\nrequire {SDK_MODULE} v0.0.0\n\n"
        f"replace {SDK_MODULE} => {sdk}\n"
    )


@dataclass(frozen=True)
class GoToolchain:
    go: Path
    reader: Path
    cache: Path

    @classmethod
    def resolve(cls, cache: Path) -> GoToolchain:
        go = CodeRuntimeResolver(cache_root=cache).resolve("go")
        return cls(go, ensure_reader(go, RUNTIMES["go"].version, cache, PROXY), cache)

    def build(self, module: Path) -> subprocess.CompletedProcess[str]:
        """``go build ./...`` of the module, offline, as Provisa builds it: with Provisa's services
        module (provisa_services, whose ReadContext a doc-comment spelling takes) provided."""
        modfile = services_go_mod(module, module.parent / f".{module.name}-build")
        env = go_environment(
            self.go,
            gocache=self.cache / "gocache",
            gomodcache=self.cache / "gomod",
            GOFLAGS=f"-mod=readonly -modfile={modfile}",
        )
        return subprocess.run(  # nosec B603 - the pinned toolchain, no shell
            [str(self.go), "build", "./..."], capture_output=True, text=True, env=env, cwd=module
        )

    def read(self, module: Path) -> dict | tuple[str, ...]:
        """The module's manifest as JSON, or the manifest's problems when it is refused."""
        try:
            manifest = read_code(
                reader=self.reader,
                go=self.go,
                code=module,
                package=".",
                gomodcache=self.cache / "gomod",
                scratch=module.parent / f".{module.name}-scratch",
            )
        except ManifestRefused as refused:
            return tuple(refused.problems)
        return manifest_json(manifest)


def write_module(directory: Path, files: dict[str, str], sdk: Path = SDK) -> Path:
    """``directory`` holding ``files`` and a go.mod requiring the SDK at ``sdk``."""
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "go.mod").write_text(go_mod(sdk))
    for name, text in files.items():
        (directory / name).write_text(text)
    return directory
