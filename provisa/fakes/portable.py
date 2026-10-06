# Copyright (c) 2026 Kenneth Stott
# Canary: 8b974398-7601-4d62-aa39-009266aabf6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The portable definition of stable fakes (REQ-1494, A STABLE FAKE): one algorithm and versioned
word lists and templates shipped with Provisa, computed alike by every engine -- this module on
the embedded engine and PostgreSQL (PL/Python), the Trino plugin's PortableFakes from the same
JSON -- so the same real value shows the same stable fake on every engine, in every region and
across an engine's replacement.

The algorithm, which the Trino plugin mirrors exactly:

- The draws are splitmix64 seeded by the value's seed -- its keyed digest mixed with the
  definition hash of the column's fake (provisa.fakes.digest.seed) -- all in unsigned 64-bit
  arithmetic; ``below(n)`` is the next draw modulo ``n``.
- A method is one of its formats, chosen by ``below(len(formats))``, filled left to right:
  ``{name}`` is a value of the list ``name`` when the version has that list, else the method
  ``name`` filled in turn; ``#`` is a digit, ``%`` a digit from 1 to 9, ``?`` a letter A-Z; every
  other character is kept.
- The ``user`` transform lowers the result and keeps only a-z, 0-9, ``.`` and ``_``.
- ``sentence`` is 4 to 9 words (``4 + below(6)``), the first capitalised, ending in a full stop;
  ``uuid4`` is two draws as 32 hex digits with the version-4 and RFC 4122 variant bits set.

A version's files are frozen: a change to the lists or templates is a new version beside it.
"""

# Requirements: REQ-1494

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from provisa.fakes.digest import seed

_M = (1 << 64) - 1
_DIR = Path(__file__).parent / "portable"

#: The version a stable fake is pinned to when it is declared, or its fake changes (REQ-1494: a
#: release never changes an existing stable column's fakes silently; it keeps its version).
CURRENT_VERSION = 1

#: Methods the algorithm computes itself rather than from a version's formats.
_COMPUTED = frozenset({"sentence", "uuid4"})


class _Draws:
    def __init__(self, seed: int) -> None:
        self._s = seed & _M

    def next(self) -> int:
        self._s = (self._s + 0x9E3779B97F4A7C15) & _M
        z = self._s
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _M
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _M
        return z ^ (z >> 31)

    def below(self, n: int) -> int:
        return self.next() % n


@lru_cache(maxsize=None)
def definition(version: int) -> dict[str, Any]:
    """Version ``version`` of the portable definition; an unknown version is refused by number."""
    path = _DIR / f"v{version}.json"
    if not path.is_file():
        raise ValueError(f"there is no version {version} of the portable fake definition")
    doc = json.loads(path.read_text())
    if doc["version"] != version:
        raise ValueError(f"{path.name} holds version {doc['version']}, not {version}")
    return doc


def methods(version: int) -> frozenset[str]:
    """The methods version ``version`` computes."""
    return frozenset(definition(version)["formats"]) | _COMPUTED


def _fill(doc: dict[str, Any], method: str, d: _Draws) -> str:
    formats = doc["formats"][method]
    template = formats[d.below(len(formats))]
    out: list[str] = []
    i = 0
    while i < len(template):
        ch = template[i]
        if ch == "{":
            end = template.index("}", i)
            name = template[i + 1 : end]
            words = doc["lists"].get(name)
            out.append(words[d.below(len(words))] if words is not None else _fill(doc, name, d))
            i = end + 1
            continue
        if ch == "#":
            out.append(str(d.below(10)))
        elif ch == "%":
            out.append(str(1 + d.below(9)))
        elif ch == "?":
            out.append(chr(ord("A") + d.below(26)))
        else:
            out.append(ch)
        i += 1
    text = "".join(out)
    if doc["transforms"].get(method) == "user":
        text = "".join(c for c in text.lower() if c.isascii() and (c.isalnum() or c in "._"))
    return text


def stable_fake(method: str, version: int, digest: int | None, def_hash: int) -> str | None:
    """The stable fake of ``method`` for the value whose keyed digest is ``digest``, under the
    definition hash ``def_hash`` (provisa.fakes.digest.definition_hash); NULL for NULL."""
    if digest is None:
        return None
    return compute(method, version, seed(digest, def_hash))


def compute(method: str, version: int, value_seed: int) -> str:
    """The stable fake of ``method`` for one seed (provisa.fakes.digest.seed)."""
    doc = definition(version)
    d = _Draws(value_seed)
    if method == "uuid4":
        hi, lo = d.next(), d.next()
        hi = (hi & ~(0xF << 12) & _M) | (0x4 << 12)
        lo = (lo & ~(0x3 << 62) & _M) | (0x2 << 62)
        h = f"{hi:016x}{lo:016x}"
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
    if method == "sentence":
        words = doc["lists"]["word"]
        picked = [words[d.below(len(words))] for _ in range(4 + d.below(6))]
        text = " ".join(picked)
        return text[0].upper() + text[1:] + "."
    if method not in doc["formats"]:
        raise ValueError(f"version {version} of the portable fake definition has no {method}()")
    return _fill(doc, method, d)


def pinned_version(
    fake: str | None, stable: bool, stored: tuple[str | None, bool, int | None] | None
) -> int | None:
    """The portable definition version a column's stable fake is computed by: the version it is
    pinned to while it stays stable with the same fake, else the current one; None when it is not
    stable. ``stored`` is the column's (fake, stable, version) as saved before, None for a new
    column."""
    if not stable:
        return None
    if stored is not None and stored[1] and stored[0] == fake and stored[2] is not None:
        return stored[2]
    return CURRENT_VERSION
