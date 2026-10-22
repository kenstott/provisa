# Copyright (c) 2026 Kenneth Stott
# Canary: 9ee30cdc-2987-45dc-a01e-0b5fdd0b2e4d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading a Node bundle's declarations (REQ-1984).

The reader (reader.mjs, beside this module) runs under the source's own runtime with the
TypeScript compiler, which Provisa does not ship: it is fetched as the npm package at the version
pinned here, checked against the integrity recorded here, through the organisation's registries,
and cached under the runtime-deps root. The reader reads only the bundle's declaration files,
never its JavaScript, and prints what they say; :mod:`provisa.codesource.manifest` applies the rules.
"""

# Requirements: REQ-1984
from __future__ import annotations

import json
import os
import shutil
import subprocess  # nosec B404 - argv-list invocation only, never shell=True
import tempfile
from collections.abc import Sequence
from pathlib import Path

from provisa.core.package_registries import Resolved
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.install import Http, http_get
from provisa.node.install import NpmCode, code_command, fetch_npm
from provisa.codesource.manifest import Manifest, read_manifest

#: The TypeScript compiler the reader runs, and the npm integrity it must have.
TYPESCRIPT = NpmCode("typescript", "6.0.3")
TYPESCRIPT_INTEGRITY = "sha512-y2TvuxSZPDyQakkFRPZHKFm+KKVqIisdg9/CZwm9ftvKXLP8NRWj38/ODjNbr43SsoXqNuAisEf1GdCxqWcdBw=="

READER = Path(__file__).with_name("reader.mjs")


def typescript_home(cache_root: Path) -> Path:
    return cache_root / "code" / "typescript" / TYPESCRIPT.version


def ensure_typescript(
    cache_root: Path, registries: Sequence[Resolved], http: Http = http_get
) -> Path:
    """The pinned TypeScript compiler's ``lib/typescript.js``, fetched and cached on a miss."""
    home = typescript_home(cache_root)
    compiler = home / "lib" / "typescript.js"
    if compiler.is_file():
        return compiler
    home.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=home.parent))
    try:
        root = fetch_npm(
            TYPESCRIPT, registries, http, staging / "pkg", pinned_integrity=TYPESCRIPT_INTEGRITY
        )
        if not (root / "lib" / "typescript.js").is_file():
            raise BundleRefused(
                "no_compiler", f"typescript@{TYPESCRIPT.version} holds no lib/typescript.js"
            )
        if not compiler.is_file():
            os.replace(root, home)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    return compiler


def entry_declarations(bundle: Path) -> Path:
    """The declaration file the bundle's exports are read from: its package's ``types`` (or
    ``typings``), else ``index.d.ts``. A bundle with neither is refused by name."""
    manifest = bundle / "package.json"
    if manifest.is_file():
        package = json.loads(manifest.read_text())
        named = package.get("types") or package.get("typings")
        if named:
            path = (bundle / named).resolve()
            if not path.is_relative_to(bundle.resolve()) or not path.is_file():
                raise BundleRefused(
                    "no_declarations",
                    f"the package names {named} as its declarations, and there is no such file",
                    declarations=str(named),
                )
            return path
    index = bundle / "index.d.ts"
    if index.is_file():
        return index
    raise BundleRefused(
        "no_declarations",
        "the bundle carries no TypeScript declarations (a package.json types field or index.d.ts)",
    )


def reader_command(runtime: str, executable: Path, compiler: Path, entry: Path) -> list[str]:
    """How the runtime runs the reader. Deno is given read access to the reader, the compiler and
    the bundle, and the environment (the compiler reads its own settings there; the reader's
    environment is only PATH, :func:`run_reader`), and nothing else."""
    if runtime in ("node", "bun"):
        return [*code_command(runtime, executable), str(READER), str(compiler), str(entry)]
    if runtime == "deno":
        readable = ",".join(str(p) for p in (READER.parent, compiler.parent.parent, entry.parent))
        return [
            str(executable),
            "run",
            f"--allow-read={readable}",
            "--allow-env",
            str(READER),
            str(compiler),
            str(entry),
        ]
    raise BundleRefused("unknown_runtime", f"{runtime!r} is not a runtime", runtime=runtime)


def run_reader(argv: Sequence[str]) -> dict:
    done = subprocess.run(  # nosec B603 - the pinned runtime and Provisa's own reader, no shell
        list(argv),
        capture_output=True,
        text=True,
        env={"PATH": os.environ.get("PATH", ""), "NO_COLOR": "1"},
    )
    if done.returncode != 0:
        raise BundleRefused(
            "reader_failed",
            f"the bundle's declarations could not be read: {done.stderr.strip()[-2000:]}",
        )
    return json.loads(done.stdout)


def read_bundle(
    *,
    runtime: str,
    executable: Path,
    compiler: Path,
    bundle: Path,
    run=run_reader,
) -> Manifest:
    """The bundle's tables, commands and streams, read from its declarations; refused with every
    problem by name (:func:`provisa.codesource.manifest.read_manifest`)."""
    entry = entry_declarations(bundle)
    return read_manifest(run(reader_command(runtime, executable, compiler, entry)))
