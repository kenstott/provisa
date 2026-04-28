# Copyright (c) 2026 Kenneth Stott
# Canary: 4e1b7f3a-2c9d-4a5e-8f1b-6d3a9c7e2b5f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Translate Cypher $param names to ordered positional parameter lists.

Cypher uses named parameters ($name). The executor expects positional params.
This module resolves the binding order and validates all referenced params.
"""

from __future__ import annotations

import re
from typing import Any


class CypherParamError(Exception):
    """Raised for unbound or missing parameters."""


_PARAM_RE = re.compile(r"\$([A-Za-z_]\w*)")


def collect_param_names(query: str) -> list[str]:
    """Return unique $param names in order of first appearance."""
    seen: list[str] = []
    for m in _PARAM_RE.finditer(query):
        name = m.group(1)
        if name not in seen:
            seen.append(name)
    return seen


def bind_params(
    param_names: list[str],
    provided: dict[str, Any],
) -> list[Any]:
    """Resolve param_names against provided dict → ordered value list.

    Raises CypherParamError for any name not present in provided.
    """
    values: list[Any] = []
    missing = [n for n in param_names if n not in provided]
    if missing:
        raise CypherParamError(
            f"Unbound Cypher parameters: {missing!r}. "
            "Provide values in the 'params' request field."
        )
    for name in param_names:
        values.append(provided[name])
    return values


def rewrite_params(query: str, param_names: list[str]) -> str:
    """Replace $name with positional $1, $2, ... in order of appearance."""
    idx: dict[str, int] = {name: i + 1 for i, name in enumerate(param_names)}

    def _replace(m: re.Match) -> str:
        name = m.group(1)
        if name in idx:
            return f"${idx[name]}"
        return m.group(0)

    return _PARAM_RE.sub(_replace, query)
