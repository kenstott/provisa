# Copyright (c) 2026 Kenneth Stott
# Canary: 7d4e0a92-3b61-4f85-9c27-e5a8b1f60d34
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The languages a code source can be written in (REQ-1984, REQ-1993, REQ-2200).

Nothing in the shared layer names a language (docs/arch/code-sources.md, section 1): a language
is a :class:`CodeLanguage`, its source type, the runtimes a source of it may name, the package
kind its packages come from, whether a source names an entry into its code, and how a source of
it is prepared -- its install, its reader and its host -- which is the language's own module.
Everything that asks whether a source type is code asks :data:`CODE_SOURCE_TYPES`.

This module imports nothing of any language's, so any layer may ask it.
"""

# Requirements: REQ-1984, REQ-1993, REQ-2200
from __future__ import annotations

import importlib
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CodeLanguage:
    #: The source type a source of this language is registered as.
    source_type: str
    #: The runtimes a source may name; with only one, a source that names none runs on it.
    runtimes: tuple[str, ...]
    #: The kind of package reference its code may be (npm, pypi, gomod), beside the Git,
    #: upload and directory references every language shares.
    package_kind: str
    #: Whether a source names where its code is entered (Python's module, Go's package), when the
    #: code has more than one.
    named_entry: bool
    #: The language's module of how a source of it is prepared (provisa/codesource/source.py):
    #: its ``prepare(source_id, config, code, lockfile, ctx)`` and its ``package_code(package,
    #: version)``, the reference of one of its packages.
    module: str

    def prepare(self) -> Callable[..., Any]:
        return importlib.import_module(self.module).prepare

    def package_code(self, package: str, version: str) -> Any:  # the language's package reference
        return importlib.import_module(self.module).package_code(package, version)


LANGUAGES: dict[str, CodeLanguage] = {
    language.source_type: language
    for language in (
        # REQ-1984: JavaScript (TypeScript declarations), on Node, Bun or Deno.
        CodeLanguage("node", ("node", "bun", "deno"), "npm", False, "provisa.node.prepare"),
        # REQ-1993: Python, on the pinned CPython.
        CodeLanguage("python", ("python",), "pypi", True, "provisa.python.prepare"),
        # REQ-2200: Go, compiled by the pinned toolchain into one static host per code hash.
        CodeLanguage("go", ("go",), "gomod", True, "provisa.golang.prepare"),
    )
}

#: Every code source type.
CODE_SOURCE_TYPES: frozenset[str] = frozenset(LANGUAGES)


def language_of(source_type: str) -> CodeLanguage | None:
    return LANGUAGES.get(source_type)
