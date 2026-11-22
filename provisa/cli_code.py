# Copyright (c) 2026 Kenneth Stott
# Canary: 8b1e4c37-5d02-4a96-b3f1-7c90a2e6d845
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa code check`` -- what Provisa would serve from a code source, read on the author's machine.

It runs the server's own readers (the Python reader under the author's interpreter, ``reader.mjs``
under the pinned TypeScript) and ``manifest.py`` at this release's version, installs nothing and
sandboxes nothing: it is the author's own code on the author's own machine. The SDK launchers run
it as ``uvx provisa==<version> code check``.

THE CONTRACT WITH A SCRIPT: stdout is the manifest (``--json``: exactly ``manifest_json``), stderr
carries warnings and refusals.

* ``0`` -- read, no problems (warnings allowed).
* ``1`` -- refused: every problem of the manifest, or a reader diagnostic, one per line.
* ``2`` -- usage or environment: no entry, no declarations, no interpreter or runtime.
"""

# Requirements: REQ-1984
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from provisa.codesource.bundle import BundleRefused
from provisa.codesource.manifest import (
    NODE_RULES,
    PROVISA_TAGS,
    PYTHON_RULES,
    Manifest,
    ManifestRefused,
    manifest_hash,
    manifest_json,
    read_manifest,
)

#: The Python the server runs a source under (code-sources.md).
PINNED_PYTHON = (3, 12)


class _Usage(Exception):
    """A usage or environment error: exit 2."""


def _language(code: Path, named: str | None) -> str:
    if named is not None:
        return named
    if (code / "package.json").is_file() or any(code.glob("*.d.ts")):
        return "typescript"
    return "python"


def _python_version(python: Path) -> tuple[int, int]:
    done = subprocess.run(  # nosec B603 - the author's own interpreter, fixed argv, no shell
        [str(python), "-c", "import sys; print(sys.version_info[0], sys.version_info[1])"],
        capture_output=True,
        text=True,
    )
    if done.returncode != 0:
        raise _Usage(f"{python} did not run: {done.stderr.strip()[-500:]}")
    major, minor = done.stdout.split()
    return int(major), int(minor)


def _read_python(code: Path, entry: str | None, python: Path) -> tuple[dict, list[str]]:
    from provisa.python.source import READER, entry_module, run_reader

    if not python.is_file():
        raise _Usage(f"no interpreter at {python}; pass --python")
    root = code / "src" if (code / "src").is_dir() else code
    module = entry_module(root, entry)
    warnings: list[str] = []
    version = _python_version(python)
    if version != PINNED_PYTHON:
        warnings.append(
            f"{python} is Python {version[0]}.{version[1]}; Provisa runs code under "
            f"{PINNED_PYTHON[0]}.{PINNED_PYTHON[1]}"
        )
    env = {"PATH": _path(), "PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1"}
    raw = run_reader([str(python), "-I", str(READER), str(root), module], env)
    return raw, warnings


def _path() -> str:
    return os.environ["PATH"]


def _read_typescript(code: Path) -> tuple[dict, list[str]]:
    from provisa.node.reader import (
        ensure_typescript,
        entry_declarations,
        reader_command,
        run_reader,
    )
    from provisa.runtime_deps.code_runtimes import CodeRuntimeResolver, RuntimeUnavailable
    from provisa.runtime_deps.pgwire_bundles import default_cache_root

    entry = entry_declarations(code)
    cache_root = default_cache_root()
    try:
        node = CodeRuntimeResolver(cache_root=cache_root).resolve("node")
    except RuntimeUnavailable as exc:
        raise _Usage(str(exc)) from exc
    compiler = ensure_typescript(cache_root, [])
    return run_reader(reader_command("node", node, compiler, entry)), []


def _untagged(raw: dict) -> list[str]:
    out = []
    for entry in raw["exports"]:
        stated = {t["tag"] for t in entry.get("tag_arguments", [])}
        if not PROVISA_TAGS.intersection(entry["tags"]) and not PROVISA_TAGS.intersection(stated):
            out.append(f"{entry['name']}: a public function with no Provisa tag; it is not served")
    return out


def check(
    code: Path, language: str | None, entry: str | None, python: Path | None
) -> tuple[Manifest, dict, list[str]]:
    """The manifest the server would read from ``code``, its raw reader output and the warnings.
    Raises :class:`ManifestRefused` or :class:`BundleRefused` on a refusal, ``_Usage`` on a usage
    or environment error."""
    if not code.is_dir():
        raise _Usage(f"{code} is not a directory")
    lang = _language(code, language)
    if lang == "python":
        raw, warnings = _read_python(code, entry, python or code / ".venv" / "bin" / "python")
        rules = PYTHON_RULES
    elif lang == "typescript":
        rules = NODE_RULES
        raw, warnings = _read_typescript(code)
    else:
        raise _Usage(f"{lang!r} is not a language `code check` reads (python, typescript)")
    warnings = [*warnings, *_untagged(raw)]
    return read_manifest(raw, rules), raw, warnings


#: Refusals that are the environment's or the invocation's, not the code's declarations.
_ENVIRONMENT = ("no_entry", "entry_ambiguous", "no_declarations", "no_compiler", "unknown_runtime")


def _print_block(manifest: Manifest, digest: str) -> None:
    from provisa.codesource.manifest import ir_of

    for e in manifest.exports:
        print(f"{e.kind} {e.name}")
        for p in e.parameters:
            req = "required" if p.required else "optional"
            print(f"  param {p.name}: {ir_of(p.shape)} ({req})")
        shape = e.returns
        if shape is not None:
            print(f"  returns {ir_of(shape)}{' (many)' if e.many else ''}")
        if e.loader:
            print(f"  loader batch={e.loader_batch}")
        if e.binds:
            print("  binds " + ", ".join(f"{op} {table}" for op, table in e.binds))
        if e.takes:
            print("  takes " + ", ".join(e.takes))
    for r in manifest.requires:
        print(f"requires {r.name}{' (secret)' if r.secret else ''}")
    print(f"manifest_hash {digest}")


def _cmd_check(args: argparse.Namespace) -> int:
    try:
        manifest, _raw, warnings = check(
            Path(args.dir), args.language, args.entry, Path(args.python) if args.python else None
        )
    except _Usage as exc:
        print(f"code: {exc}", file=sys.stderr)
        return 2
    except ManifestRefused as exc:
        for problem in exc.problems:
            print(problem, file=sys.stderr)
        return 1
    except BundleRefused as exc:
        print(f"code: {exc}", file=sys.stderr)
        return 2 if exc.code.removeprefix("codesource.") in _ENVIRONMENT else 1
    for warning in warnings:
        print(f"warning: {warning}", file=sys.stderr)
    document: dict[str, Any] = manifest_json(manifest)
    if args.json:
        print(json.dumps(document, sort_keys=True))
    else:
        _print_block(manifest, manifest_hash(document))
    return 0


def register(sub) -> None:
    parser = sub.add_parser("code", help="Read a code source the way Provisa does")
    actions = parser.add_subparsers(dest="code_command", required=True)
    check_parser = actions.add_parser(
        "check", help="Print what Provisa would serve from a code source's working tree"
    )
    check_parser.add_argument("dir", nargs="?", default=".", help="The project (default: .)")
    check_parser.add_argument("--language", choices=("python", "typescript"), default=None)
    check_parser.add_argument("--entry", default=None, help="The entry module (Python)")
    check_parser.add_argument(
        "--python", default=None, metavar="PATH", help="The interpreter (default: .venv/bin/python)"
    )
    check_parser.add_argument("--json", action="store_true", help="Print manifest_json alone")
    check_parser.set_defaults(func=_cmd_check)
