# Copyright (c) 2026 Kenneth Stott
# Canary: 0c7e5a91-2b4d-4f63-9e18-7d5a3f6b2c40
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading a Go source's declarations (REQ-2200).

The reader (``reader/``, beside this module) is a Go program shipped as source with its own
``go.mod`` and ``go.sum``. It is compiled once by the pinned Go toolchain, its one dependency
(golang.org/x/tools, for go/packages) fetched through the module proxy it is given and checked
against the committed ``go.sum``, and the binary is cached by the reader's source and the
toolchain's version. It type-checks the code's package and prints what its exported functions
say; :mod:`provisa.codesource.manifest` applies the rules.

Reading types runs none of the code (Node's reader reads declarations the same way), so the
reader is not sandboxed. The go command it calls is held to what is installed: no network
(``GOPROXY=off``), no change to the code's ``go.mod`` (``-mod=readonly``), no other toolchain
(``GOTOOLCHAIN=local``), no workspace and no cgo.
"""

# Requirements: REQ-2200
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess  # nosec B404 - argv-list invocation only, never shell=True
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import GO_RULES, Manifest, read_manifest
from provisa.runtime_deps.code_runtimes import RUNTIMES

READER = Path(__file__).with_name("reader")
_READER_SOURCES = ("go.mod", "go.sum", "main.go")
_GO_LINE = re.compile(r"^go\s+(\d+(?:\.\d+){0,2})\s*$", re.MULTILINE)


def _version(text: str) -> tuple[int, ...]:
    return tuple(int(part) for part in text.split("."))


def check_go_line(code: Path, toolchain: str | None = None) -> None:
    """Refuse code whose ``go.mod`` asks for a newer Go than the pinned toolchain (``toolchain``,
    the pin's by default): the toolchain runs with GOTOOLCHAIN=local and fetches no other. Code
    without a ``go.mod`` is refused too: a Go source is a module."""
    pinned = toolchain or RUNTIMES["go"].version
    module = code / "go.mod"
    if not module.is_file():
        raise BundleRefused("no_go_mod", "the code has no go.mod: a Go source is a module")
    found = _GO_LINE.search(module.read_text())
    if found is not None and _version(found.group(1)) > _version(pinned):
        raise BundleRefused(
            "go_too_new",
            f"the code's go.mod asks for go {found.group(1)}, and this Provisa's Go is {pinned}",
            go=found.group(1),
            pinned=pinned,
        )


def go_environment(go: Path, *, gocache: Path, gomodcache: Path, **extra: str) -> dict[str, str]:
    """The environment every go command Provisa runs gets: exactly this, nothing inherited. The
    toolchain is ``go``'s own, its caches are the ones named, and it reaches no network, changes
    no ``go.mod`` and builds no C unless ``extra`` says otherwise."""
    return {
        "PATH": str(go.parent),
        "HOME": str(gocache.parent),
        "GOCACHE": str(gocache),
        "GOMODCACHE": str(gomodcache),
        "GOPATH": str(gomodcache.parent),
        "GOTOOLCHAIN": "local",
        "GOFLAGS": "-mod=readonly",
        "GOPROXY": "off",
        "GOSUMDB": "off",
        "GOWORK": "off",
        "GOENV": "off",
        "CGO_ENABLED": "0",
        **extra,
    }


def reader_key(go_version: str) -> str:
    """The reader's cache key: its own source and the toolchain that compiles it."""
    digest = hashlib.sha256(go_version.encode())
    for name in _READER_SOURCES:
        digest.update(b"\0" + name.encode() + b"\0" + (READER / name).read_bytes())
    return digest.hexdigest()[:16]


#: Run a go command's argv with exactly ``env`` in ``cwd``; its stdout, refused by name on failure.
GoRunner = Callable[[Sequence[str], dict[str, str], Path], str]


def run_go(argv: Sequence[str], env: dict[str, str], cwd: Path) -> str:
    # The pinned toolchain or Provisa's own reader, with a fixed argv and no shell.
    done = subprocess.run(  # nosec B603
        list(argv), capture_output=True, text=True, env=env, cwd=cwd
    )
    if done.returncode != 0:
        raise BundleRefused(
            "go_failed",
            f"{Path(argv[0]).name} {' '.join(argv[1:2])} failed: {done.stderr.strip()[-2000:]}",
        )
    return done.stdout


def ensure_reader(
    go: Path, go_version: str, cache_root: Path, proxy: str, run: GoRunner = run_go
) -> Path:
    """The compiled reader for this toolchain, built and cached on a miss. ``proxy`` is the
    GOPROXY its dependency is fetched through (the organisation's, else the public one)."""
    home = cache_root / "code" / "go-reader" / reader_key(go_version)
    binary = home / "reader"
    if binary.is_file():
        return binary
    home.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=home.parent))
    try:
        source = staging / "src"
        source.mkdir()
        for name in _READER_SOURCES:
            shutil.copyfile(READER / name, source / name)
        env = go_environment(
            go,
            gocache=staging / "cache",
            gomodcache=staging / "mod",
            GOPROXY=proxy,
            GOFLAGS="-mod=readonly -modcacherw",
        )
        built = staging / "out"
        built.mkdir()
        run([str(go), "build", "-trimpath", "-o", str(built / "reader"), "."], env, source)
        if not binary.is_file():
            os.replace(built, home)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return binary


def read_code(
    *,
    reader: Path,
    go: Path,
    code: Path,
    package: str,
    gomodcache: Path,
    scratch: Path,
    run: GoRunner = run_go,
) -> Manifest:
    """The code's tables, commands and streams, read from its exported functions; refused with
    every problem by name (:func:`provisa.codesource.manifest.read_manifest`). ``gomodcache``
    holds the code's installed modules; ``scratch`` is where the go command keeps its build
    cache for this read."""
    code = code.resolve()  # the reader runs in it, and the go command it starts runs there too
    check_go_line(code)
    env = go_environment(go, gocache=scratch / "gocache", gomodcache=gomodcache)
    out = run([str(reader), str(code), package], env, code)
    return read_manifest(json.loads(out), GO_RULES)
