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
    from provisa.cypher.label_map import CypherLabelMap

# Matches: varname { ... }  — does NOT match bare { } blocks (no leading ident)
_MAP_PROJ_RE = re.compile(
    r'\b([A-Za-z_]\w*)\s*\{([^{}]+)\}',
)


class MapProjectionMixin:
    """Mixin for _Translator: rewrites map projection expressions."""

    _var_table: dict
    _lm: "CypherLabelMap"

    def _rewrite_map_projections(self, text: str) -> str:
        """Rewrite all map projection expressions in *text* to SQL MAP(...)."""
        return _MAP_PROJ_RE.sub(self._expand_map_proj, text)

    def _expand_map_proj(self, m: re.Match) -> str:
        var = m.group(1)
        body = m.group(2).strip()

        # Only rewrite if var is a known node variable
        info = self._var_table.get(var)
        if info is None:
            return m.group(0)
        nm = info[1]  # NodeMapping | None

        keys: list[str] = []
        vals: list[str] = []

        for raw in body.split(","):
            item = raw.strip()
            if not item:
                continue
            # Normalise: remove internal whitespace (parser may emit ". *" for ".*")
            item_norm = re.sub(r'\s+', '', item)
            if item_norm == ".*":
                # Expand all known properties
                if nm and nm.properties:
                    for prop in sorted(nm.properties.keys()):
                        keys.append(f"'{prop}'")
                        vals.append(f'{var}."{prop}"')
            elif item_norm.startswith("."):
                prop = item_norm[1:].strip()
                keys.append(f"'{prop}'")
                vals.append(f'{var}."{prop}"')
            elif ":" in item:
                key, _, val_expr = item.partition(":")
                keys.append(f"'{key.strip()}'")
                vals.append(val_expr.strip())
            else:
                # bare property name without dot — treat as .prop
                keys.append(f"'{item}'")
                vals.append(f'{var}."{item}"')

        if not keys:
            return m.group(0)

        keys_sql = ", ".join(keys)
        vals_sql = ", ".join(vals)
        return f"MAP(ARRAY[{keys_sql}], ARRAY[{vals_sql}])"
