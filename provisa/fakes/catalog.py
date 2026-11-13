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
(REQ-1494): each by name and category, with the arguments it takes, the column families it can
fill and whether it can be stable -- each answer the one the checks a save runs give, so the editor
offers what the save accepts."""

# Requirements: REQ-1494

from __future__ import annotations

import inspect
from functools import lru_cache
from typing import Any

from provisa.fakes.checks import DeclaredColumn, check_stable, check_table
from provisa.fakes.kinds import RULE_ONLY, FakeRefused, Method, parse
from provisa.fakes.methods import UNSUPPORTED, _generator, check_method, method_names


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


#: A column type of each family of :func:`provisa.fakes.checks.family`: the column an entry of the
#: catalog is checked against.
FAMILY_TYPES: dict[str, str] = {
    "text": "varchar",
    "integer": "integer",
    "numeric": "double",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "other": "json",
}

#: Each kind's argument choices, as declarations: the families a kind can fill are those any of
#: them fits, and its stability is read off the ones a stable save accepts. A relative kind names
#: ``c_<family>``, a column of that family beside the one checked. A kind's family depends on its
#: arguments where its values come from them: categories and sequence (the values named),
#: the distributions (numbers or dates and times as their points).
KIND_CHOICES: dict[str, tuple[str, ...]] = {
    "categories": (
        "categories()",
        "categories((a, b))",
        "categories((1, 2))",
        "categories((1.5, 2.5))",
        "categories((true, false))",
        "categories(('2026-01-01', '2026-02-01'))",
    ),
    "bool": ("bool()", "bool(0.5)"),
    "uniform": ("uniform(min=1, max=2)", "uniform(min='2026-01-01', max='2026-02-01')"),
    "normal": ("normal(mean=1, sd=1)", "normal(mean='2026-01-01', sd=30 days)"),
    "lognormal": (
        "lognormal(median=10, p95=100)",
        "lognormal(median='2026-02-01', p95='2026-06-01', min='2026-01-01')",
    ),
    "triangular": (
        "triangular(min=1, mode=2, max=3)",
        "triangular(min='2026-01-01', mode='2026-02-01', max='2026-03-01')",
    ),
    "percentiles": (
        "percentiles(min=1, p50=2, max=3)",
        "percentiles(min='2026-01-01', p50='2026-02-01', max='2026-03-01')",
    ),
    "poisson": ("poisson(mean=3)",),
    "profile": ("profile()", "profile(run=1)"),
    "pattern": ("pattern()",),
    "bucket": ("bucket(10)",),
    "truncate": ("truncate(month)",),
    "prefix": ("prefix(3)",),
    "hash": ("hash()",),
    "encrypt": ("encrypt()",),
    "after": ("after(c_date)", "after(c_date, 1 day)"),
    "before": ("before(c_date)", "before(c_date, 1 day)"),
    "greater_than": ("greater_than(c_integer)", "greater_than(c_integer, 1)"),
    "less_than": ("less_than(c_integer)", "less_than(c_integer, 1)"),
    "sql": ("sql(1)",),
    "sql_group": ("sql_group(sum(self))",),
    "sequence": (
        "sequence((a, b), c_text, c_integer)",
        "sequence((1, 2), c_text, c_integer)",
        "sequence((1.5, 2.5), c_text, c_integer)",
        "sequence((true, false), c_text, c_integer)",
        "sequence(('2026-01-01', '2026-02-01'), c_text, c_integer)",
    ),
}

#: The methods whose value is an argument given -- random_element returns one of its ``elements``
#: -- and their argument choices beyond the default call, one of each family a declaration can
#: write. Every other portable argument sets a range, format, length or count, which leaves the
#: value's type as the default call makes it (tests/unit/test_fake_catalog_families.py).
METHOD_CHOICES: dict[str, tuple[str, ...]] = {
    "random_element": (
        "random_element(elements=(a, b))",
        "random_element(elements=(1, 2))",
        "random_element(elements=(1.5, 2.5))",
        "random_element(elements=(true, false))",
    ),
}


def _kind_fits(declaration: str, fam: str, rule: bool) -> bool:
    """The save's own check: a column of ``fam`` declares it, beside a column of every family."""
    others = [DeclaredColumn(f"c_{f}", t) for f, t in FAMILY_TYPES.items()]
    data_type = FAMILY_TYPES[fam]
    column = (
        DeclaredColumn("x", data_type, rule=declaration)
        if rule
        else DeclaredColumn("x", data_type, fake=declaration)
    )
    try:
        check_table("t", [column, *others], {})
    except FakeRefused:
        return False
    return True


def _method_fits(method: Method, fam: str) -> bool:
    try:
        check_method(method, fam)
    except FakeRefused:
        return False
    return True


def _stable(declaration: str) -> bool:
    """Whether a save accepts the declaration stable (provisa.fakes.checks.check_stable)."""
    try:
        check_stable(parse(declaration, rule=True))
    except FakeRefused:
        return False
    return True


def _given(declaration: str, args: list[dict[str, Any]]) -> set[str]:
    kind = parse(declaration, rule=True)
    return {a["name"] for a in args if getattr(kind, a["name"], None) is not None}


def _kind_entry(kind: dict[str, Any]) -> dict[str, Any]:
    name = kind["name"]
    rule = name in RULE_ONLY
    choices = KIND_CHOICES[name]
    families = [f for f in FAMILY_TYPES if any(_kind_fits(d, f, rule) for d in choices)]
    stable = [d for d in choices if _stable(d)]
    unstable = [d for d in choices if d not in stable]
    # The arguments every stable choice gives and some other choice lacks: those a stable fake of
    # this kind must declare.
    needs = (
        set.intersection(*(_given(d, kind["args"]) for d in stable))
        - set.intersection(*(_given(d, kind["args"]) for d in unstable))
        if stable and unstable
        else set()
    )
    return {
        **kind,
        "ruleOnly": rule,
        "families": families,
        "stable": bool(stable),
        "stableNeeds": [a["name"] for a in kind["args"] if a["name"] in needs],
    }


def _method_families(name: str) -> list[str]:
    calls = [Method(name, ())] + [parse(d) for d in METHOD_CHOICES.get(name, ())]
    return [
        f for f in FAMILY_TYPES if any(_method_fits(m, f) for m in calls if isinstance(m, Method))
    ]


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
    """Every kind and every declarable method, by category, with its arguments, the families it
    can fill, and whether it can be stable (``stableNeeds``: the arguments a stable one declares;
    a stable method takes none)."""
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
                "families": _method_families(name),
                "stable": _stable(f"{name}()"),
                "stableNeeds": [],
            }
        )
    return {"kinds": [_kind_entry(k) for k in KINDS], "methods": methods}
