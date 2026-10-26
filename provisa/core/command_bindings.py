# Copyright (c) 2026 Kenneth Stott
# Canary: 4c8e2a71-5d93-4b06-a1f7-8e3b9d0c6f25
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Command bindings: a command's (or function's) name in each language (REQ-2168).

A command is offered in every language under its generated name, the command's own. Its
``language_bindings`` rename it in a language or exclude it from one; SQL, which every language
compiles to, cannot be excluded. A binding is a name only: arguments, governance and grants are
the command's, shared by every binding.

Two commands bound to one name in a language, or a binding that would shadow one of the
language's built-ins, are refused naming both.
"""

from __future__ import annotations

from collections.abc import Iterable
from functools import cache

LANGUAGES = ("sql", "cypher", "graphql", "rest", "mcp", "sparql")

# Neo4j's own procedures live under these namespaces; Provisa answers some of them itself
# (db.labels, dbms.components, provisa.metric), so a command bound under one would be shadowed.
_CYPHER_RESERVED_NAMESPACES = ("db.", "dbms.", "provisa.")

# The openCypher built-in functions.
_CYPHER_FUNCTIONS = frozenset(
    {
        "abs",
        "acos",
        "all",
        "any",
        "asin",
        "atan",
        "atan2",
        "avg",
        "ceil",
        "coalesce",
        "collect",
        "cos",
        "cot",
        "count",
        "degrees",
        "e",
        "endnode",
        "exists",
        "exp",
        "floor",
        "haversin",
        "head",
        "id",
        "isempty",
        "keys",
        "labels",
        "last",
        "left",
        "length",
        "log",
        "log10",
        "ltrim",
        "max",
        "min",
        "nodes",
        "none",
        "percentilecont",
        "percentiledisc",
        "pi",
        "point",
        "properties",
        "radians",
        "rand",
        "range",
        "reduce",
        "relationships",
        "replace",
        "reverse",
        "right",
        "round",
        "rtrim",
        "sign",
        "sin",
        "single",
        "size",
        "split",
        "sqrt",
        "startnode",
        "stdev",
        "stdevp",
        "substring",
        "sum",
        "tail",
        "tan",
        "timestamp",
        "toboolean",
        "tofloat",
        "tointeger",
        "tolower",
        "tostring",
        "toupper",
        "trim",
        "type",
        "date",
        "datetime",
        "localdatetime",
        "localtime",
        "time",
        "duration",
    }
)


class CommandBindingRefused(ValueError):
    """A command binding that cannot be honoured: an unknown language, an excluded SQL binding,
    or a name another command or a built-in already holds."""


@cache
def _sql_builtins() -> frozenset[str]:
    from sqlglot.parser import Parser

    return frozenset(k.lower() for k in Parser.FUNCTIONS)


def check_declared(command: str, bindings: dict[str, dict]) -> None:
    """One command's declared bindings: each for a known language, SQL never excluded."""
    for language, binding in bindings.items():
        if language not in LANGUAGES:
            raise CommandBindingRefused(
                f"command {command!r}: {language!r} is not a language a command binding names "
                f"({', '.join(LANGUAGES)})"
            )
        if language == "sql" and binding.get("exclude"):
            raise CommandBindingRefused(
                f"command {command!r}: its SQL command binding cannot be excluded; every "
                "language compiles to SQL"
            )


def binding_name(command: dict, language: str) -> str | None:
    """``command``'s name in ``language``: the declared one, the generated one (the command's own
    name) when it declares none there, or None when it is excluded from the language."""
    declared = (command.get("language_bindings") or {}).get(language)
    if declared is None:
        return command["name"]
    if declared.get("exclude"):
        return None
    return declared.get("name") or command["name"]


def bound(commands: Iterable[dict], language: str) -> dict[str, dict]:
    """``commands`` offered in ``language``, by their name there."""
    out: dict[str, dict] = {}
    for command in commands:
        name = binding_name(command, language)
        if name is not None:
            out[name] = command
    return out


def _builtin(language: str, name: str, *, function: bool) -> str | None:
    """The built-in of ``language`` that ``name`` would shadow, or None."""
    low = name.lower()
    if language == "cypher":
        if any(low.startswith(ns) for ns in _CYPHER_RESERVED_NAMESPACES):
            return f"procedure namespace {low.split('.')[0]}"
        if function and low in _CYPHER_FUNCTIONS:
            return f"function {low}"
    if language == "sql" and low in _sql_builtins():
        return f"function {low}"
    return None


def check_names(commands: Iterable[dict]) -> None:
    """Refuse two commands bound to one name in a language, and a binding that shadows one of
    the language's built-ins, naming both."""
    held: dict[tuple[str, str], str] = {}
    for command in commands:
        function = command.get("impl_kind") == "sql_expression"
        for language in LANGUAGES:
            name = binding_name(command, language)
            if name is None:
                continue
            shadowed = _builtin(language, name, function=function)
            if shadowed is not None:
                raise CommandBindingRefused(
                    f"command {command['name']!r}: its {language} command binding {name!r} "
                    f"would shadow the built-in {shadowed}"
                )
            other = held.setdefault((language, name.lower()), command["name"])
            if other != command["name"]:
                raise CommandBindingRefused(
                    f"commands {other!r} and {command['name']!r} are both bound to {name!r} "
                    f"in {language}"
                )
