# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8a6c21-9e47-4d0b-a5c3-7b2e1d9f6a85
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A command argument's default (REQ-2168): the value a call that omits the argument passes.

An argument that declares a default may be left out of a call on every surface; one that
declares none is required, and a call without it is refused naming it. A default is checked
against the argument's type when the command is saved.
"""

from __future__ import annotations

from typing import Any

# An argument's type -- an IR type name, or the GraphQL scalar a command form writes -- to what
# a default of it may be. JSON takes any value.
_TEXT = {"text", "varchar", "string", "uuid", "date", "timestamp", "timestamptz", "time"}
_TEXT |= {"datetime", "id", "char", "bytea"}
_INTEGER = {"smallint", "integer", "int", "bigint"}
_NUMBER = {"float", "double", "numeric", "decimal", "real"}
_BOOLEAN = {"boolean", "bool"}
_ANY = {"json", "jsonb"}


class ArgumentDefaultRefused(ValueError):
    """A default that is not a value of its argument's type, or a call missing an argument."""


def check_default(argument: str, type_name: str, value: Any) -> None:
    """``value`` is a value of ``type_name`` (null is one of every type)."""
    kind = type_name.strip().lower()
    if value is None or kind in _ANY:
        return
    if kind in _TEXT:
        fits = isinstance(value, str)
    elif kind in _INTEGER:
        fits = isinstance(value, int) and not isinstance(value, bool)
    elif kind in _NUMBER:
        fits = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif kind in _BOOLEAN:
        fits = isinstance(value, bool)
    else:
        raise ArgumentDefaultRefused(
            f"argument {argument!r}: a default is checked against its type, and {type_name!r} "
            "is not a type a default is read for"
        )
    if not fits:
        raise ArgumentDefaultRefused(
            f"argument {argument!r}: its default {value!r} is not a {type_name} value"
        )


def with_defaults(command: dict, args: dict) -> dict:
    """``args`` with each declared argument the call left out taking its default. Positional
    values (``a0``, ``a1`` …, as SQL and Cypher pass them) stay positional. An argument left out
    that declares no default stays left out: whether a call may omit it is the surface's binding
    (action_exec.bind_*), or the implementation's -- a remote operation's optional argument. A
    value no argument takes is refused naming it."""
    declared = [a for a in command.get("arguments") or [] if a.get("name")]
    if not declared:
        return args  # a source procedure registered without its signature takes what it is given
    names = [a["name"] for a in declared]
    keys = list(args)
    if keys and all(k == f"a{i}" for i, k in enumerate(keys)):
        # Kept by position: a SQL surface's values are SQL literals, read as such by position
        # (function_dispatch); each left out takes its default after them.
        if len(keys) > len(names):
            raise ArgumentDefaultRefused(
                f"{command['name']} takes {len(names)} argument(s); {len(keys)} given"
            )
        positional = dict(args)
        for i, a in enumerate(declared[len(keys) :], start=len(keys)):
            if "default" not in a:
                break  # a position after one left out cannot be filled
            positional[f"a{i}"] = a["default"]
        return positional
    unknown = [k for k in args if k not in names]
    if unknown:
        raise ArgumentDefaultRefused(
            f"{command['name']}: argument {unknown[0]!r} is not one it declares"
        )
    bound = dict(args)
    for a in declared:
        if a["name"] not in bound and "default" in a:
            bound[a["name"]] = a["default"]
    return bound
