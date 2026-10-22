# Copyright (c) 2026 Kenneth Stott
# Canary: 7e3a1f9c-4b2d-4e8a-9c5f-1d6b3e7a2c4f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MapProjectionMixin — Cypher map projection rewrite.

  n { .name, .age }            →  MAP(ARRAY['name','age'], ARRAY[n."name",n."age"])
  n { .* }                     →  MAP(ARRAY[...all props...], ARRAY[n."col",...])
  n { .*, extra: expr }        →  MAP(ARRAY[...,..'extra'], ARRAY[...,..(expr)])
  n { key: expr }              →  MAP(ARRAY['key'], ARRAY[(expr)])

Mixed into _Translator; relies on _var_table and _lm.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

# Requirements: REQ-347, REQ-349, REQ-571

# Matches bare {key: val, ...} blocks — no leading identifier, innermost only (no nested {})
_BARE_MAP_RE = re.compile(r"(?<![A-Za-z_\w])\{([^{}]+:[^{}]*)\}")


def _split_top_level_commas(text: str) -> list[str]:
    """Split by commas at depth 0 (not inside parentheses or brackets)."""
    parts: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(text):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return parts


_IDENT_RE = re.compile(r"^[A-Za-z_]\w*$")


def _expand_bare_map(m: re.Match) -> str:
    """Replace {key: val, ...} with MAP(ARRAY['key',...], ARRAY[val,...])."""
    body = m.group(1)
    keys: list[str] = []
    vals: list[str] = []
    for part in _split_top_level_commas(body):
        colon = part.find(":")
        if colon < 0:
            return m.group(0)
        k = part[:colon].strip()
        v = part[colon + 1 :].strip()
        if not k or not v or not _IDENT_RE.match(k):
            return m.group(0)
        keys.append(f"'{k}'")
        vals.append(v)
    if not keys:
        return m.group(0)
    # Encode each value as a JSON value with to_json, NOT CAST(v AS JSON): in the IR dialect
    # (Postgres) CAST AS JSON *parses* the input as JSON text, so a bare string like 'Siamese' fails
    # ("Malformed JSON"); to_json *encodes* any scalar/struct into a JSON value. Keeps the
    # heterogeneous map as one JSON array type.
    json_vals = [f"to_json({v})" for v in vals]
    return f"MAP(ARRAY[{', '.join(keys)}], ARRAY[{', '.join(json_vals)}])"


def rewrite_bare_map_literals(text: str) -> str:  # REQ-571
    """Rewrite bare {key: val, ...} map literals to MAP(ARRAY[...], ARRAY[...]).

    Applies bottom-up to handle nested maps: innermost {key: val} blocks are
    replaced first, then the next level up, until stable.
    """
    prev = None
    while prev != text:
        prev = text
        text = _BARE_MAP_RE.sub(_expand_bare_map, text)
    return text
