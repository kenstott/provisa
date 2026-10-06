# Copyright (c) 2026 Kenneth Stott
# Canary: 5932a4f6-f46d-433b-83a9-14c9389731c1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The kinds of fake and fake methods a column may declare, as the table editor offers them
(REQ-1494): each by name and category, with the arguments it takes."""

# Requirements: REQ-1494

from __future__ import annotations

import inspect
from functools import lru_cache
from typing import Any

from provisa.fakes.kinds import RULE_ONLY
from provisa.fakes.methods import STABLE_METHODS, UNSUPPORTED, _generator, method_names


def _arg(name: str, kind: str, required: bool = True) -> dict[str, Any]:
    """``kind``: number, date (a date or time), interval (``30 days``), text, list (``(a, b)``),
    column (a column of the table), expression (SQL), or point (a number, or a date or time)."""
    return {"name": name, "kind": kind, "required": required}


#: Provisa's own kinds, by category: (name, positional, arguments).
KINDS: list[dict[str, Any]] = [
    {"category": "values", "name": "categories", "positional": True,
     "args": [_arg("values", "list", False), _arg("shares", "list", False)]},
    {"category": "values", "name": "bool", "positional": True,
     "args": [_arg("share", "number", False)]},
    {"category": "distribution", "name": "uniform", "positional": False,
     "args": [_arg("min", "point"), _arg("max", "point")]},
    {"category": "distribution", "name": "normal", "positional": False,
     "args": [_arg("mean", "point"), _arg("sd", "interval"), _arg("min", "point", False),
              _arg("max", "point", False)]},
    {"category": "distribution", "name": "lognormal", "positional": False,
     "args": [_arg("median", "point", False), _arg("p95", "point", False),
              _arg("mu", "number", False), _arg("sigma", "number", False),
              _arg("min", "point", False), _arg("max", "point", False)]},
    {"category": "distribution", "name": "triangular", "positional": False,
     "args": [_arg("min", "point"), _arg("mode", "point"), _arg("max", "point")]},
    {"category": "distribution", "name": "percentiles", "positional": False,
     "args": [_arg(k, "point", False) for k in ("min", "p5", "p25", "p50", "p75", "p95", "max")]},
    {"category": "distribution", "name": "poisson", "positional": False,
     "args": [_arg("mean", "number")]},
    {"category": "distribution", "name": "profile", "positional": False,
     "args": [_arg("run", "text", False)]},
    {"category": "transform", "name": "pattern", "positional": True, "args": []},
    {"category": "transform", "name": "bucket", "positional": True,
     "args": [_arg("width_or_edges", "list")]},
    {"category": "transform", "name": "truncate", "positional": True,
     "args": [_arg("unit", "text")]},
    {"category": "transform", "name": "prefix", "positional": True,
     "args": [_arg("length", "number")]},
    {"category": "transform", "name": "hash", "positional": True, "args": []},
    {"category": "transform", "name": "encrypt", "positional": True, "args": []},
    {"category": "relative", "name": "after", "positional": True,
     "args": [_arg("column", "column"), _arg("distance", "interval", False)]},
    {"category": "relative", "name": "before", "positional": True,
     "args": [_arg("column", "column"), _arg("distance", "interval", False)]},
    {"category": "relative", "name": "greater_than", "positional": True,
     "args": [_arg("column", "column"), _arg("distance", "number", False)]},
    {"category": "relative", "name": "less_than", "positional": True,
     "args": [_arg("column", "column"), _arg("distance", "number", False)]},
    {"category": "relative", "name": "sql", "positional": True,
     "args": [_arg("expression", "expression")]},
    {"category": "rule", "name": "sql_group", "positional": True,
     "args": [_arg("expression", "expression")]},
    {"category": "rule", "name": "sequence", "positional": True,
     "args": [_arg("states", "list"), _arg("entity", "column"), _arg("order", "column")]},
]  # fmt: skip


def _category(name: str) -> str:
    """The provider a method belongs to, by its module: person, address, internet and so on."""
    for provider in _generator().providers:
        for cls in type(provider).__mro__:
            parts = cls.__module__.split(".")
            if name in cls.__dict__ and len(parts) > 2:
                return parts[2]
    return "pattern"  # inherited by every provider from the base: bothify, numerify and the like


@lru_cache(maxsize=1)
def catalog() -> dict[str, Any]:
    """Every kind and every declarable method, by category, with its arguments."""
    methods = []
    for name in method_names():
        if name in UNSUPPORTED:
            continue
        signature = inspect.signature(getattr(_generator(), name))
        params = [
            {
                "name": p.name,
                "required": p.default is p.empty,
                "default": None if p.default is p.empty else repr(p.default),
            }
            for p in signature.parameters.values()
            if p.kind not in (p.VAR_POSITIONAL, p.VAR_KEYWORD)
        ]
        methods.append(
            {
                "name": name,
                "category": _category(name),
                "params": params,
                "stable": name in STABLE_METHODS,
            }
        )
    return {
        "kinds": [{**k, "ruleOnly": k["name"] in RULE_ONLY} for k in KINDS],
        "methods": methods,
    }
