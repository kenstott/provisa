# Copyright (c) 2026 Kenneth Stott
# Canary: d40cd447-468d-48b6-8f99-c7a9b96b7352
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An entity-set request's system query options (REQ-2193)."""

# Requirements: REQ-2193

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from provisa.api.odata.errors import QueryOptionRefused
from provisa.api.odata.expression import Node, OrderKey, parse_filter, parse_orderby

# Options not served. Answered 501, never ignored: a read that ignores one answers rows without
# what the client asked to see, as though it had applied it.
NOT_IMPLEMENTED = frozenset({"$search", "$apply", "$compute", "$levels"})
_SERVED = ("$select", "$top", "$skip", "$count", "$format", "$filter", "$orderby", "$expand")
_JSON_FORMATS = ("json", "application/json")
# Levels of $expand: each bound guards a recursion, and each level is a subquery per row above it.
MAX_EXPAND_DEPTH = 4
_NAVIGATION = re.compile(r"[A-Za-z_]\w*")


@dataclass(frozen=True)
class QueryOptions:
    select: tuple[str, ...] | None = None  # None: every property
    top: int | None = None
    skip: int = 0
    count: bool = False
    filter: Node | None = None
    order_by: tuple[OrderKey, ...] = ()
    expand: tuple[Expansion, ...] = ()


@dataclass(frozen=True)
class Expansion:
    """One navigation property of a ``$expand``. ``raw`` is its options as the client wrote them,
    which a nested collection's nextLink carries."""

    name: str
    options: QueryOptions
    raw: tuple[tuple[str, str], ...] = ()


def _refused(message: str) -> QueryOptionRefused:
    return QueryOptionRefused(400, "invalid_query_option", f"$expand: {message}")


def _count_of_rows(name: str, value: str) -> int:
    if not value.isascii() or not value.isdigit():
        raise QueryOptionRefused(
            400, "invalid_query_option", f"{name} must be a non-negative integer: {value!r}"
        )
    return int(value)


def _boolean(name: str, value: str) -> bool:
    if value not in ("true", "false"):
        raise QueryOptionRefused(
            400, "invalid_query_option", f"{name} must be true or false: {value!r}"
        )
    return value == "true"


def _select(value: str) -> tuple[str, ...] | None:
    names = tuple(name.strip() for name in value.split(","))
    if not all(names):
        raise QueryOptionRefused(400, "invalid_query_option", f"$select names nothing: {value!r}")
    return None if "*" in names else tuple(dict.fromkeys(names))


def check_format(value: str | None, *allowed: str) -> None:
    """Refuse a ``$format`` the document is not served in."""
    if value is not None and value.split(";")[0].strip().lower() not in allowed:
        raise QueryOptionRefused(406, "unsupported_format", f"$format {value!r} is not served")


def _split(text: str, separator: str) -> list[str]:
    """``text`` cut at each ``separator`` outside a quoted string and outside parentheses."""
    parts, start, depth, quoted = [], 0, 0, False
    for at, character in enumerate(text):
        if character == "'":
            quoted = not quoted  # a doubled quote closes and opens again
        elif quoted:
            continue
        elif character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth < 0:
                raise _refused(f"unbalanced parentheses: {text!r}")
        elif character == separator and depth == 0:
            parts.append(text[start:at])
            start = at + 1
    if depth or quoted:
        raise _refused(f"unbalanced text: {text!r}")
    return [*parts, text[start:]]


def _expansion(text: str, depth: int) -> Expansion:
    text = text.strip()
    head, opening, _ = text.partition("(")
    name = head.strip()
    if name == "*" or "/" in name:
        raise QueryOptionRefused(501, "not_implemented", f"$expand of {name} is not served")
    if not _NAVIGATION.fullmatch(name):
        raise _refused(f"no navigation property in {text!r}")
    if not opening:
        return Expansion(name, QueryOptions())
    if not text.endswith(")"):
        raise _refused(f"unbalanced text: {text!r}")
    pairs = []
    for option in _split(text[len(head) + 1 : -1], ";"):
        key, equals, value = option.partition("=")
        if not equals:
            raise _refused(f"an option of {name} has no value: {option!r}")
        pairs.append((key.strip(), value.strip()))
    return Expansion(name, _parsed(_once_each(pairs), depth), tuple(pairs))


def _expand(value: str, depth: int) -> tuple[Expansion, ...]:
    if depth >= MAX_EXPAND_DEPTH:
        raise _refused(f"nested deeper than {MAX_EXPAND_DEPTH}")
    expansions = tuple(_expansion(item, depth + 1) for item in _split(value, ","))
    names = [e.name for e in expansions]
    repeated = {name for name in names if names.count(name) > 1}
    if repeated:
        raise _refused(f"names {sorted(repeated)[0]!r} twice")
    return expansions


def _once_each(items: Iterable[tuple[str, str]]) -> dict[str, str]:
    options: dict[str, str] = {}
    for name, value in items:
        if name in options:
            raise QueryOptionRefused(400, "invalid_query_option", f"{name} is given twice")
        options[name] = value
    return options


def _parsed(options: dict[str, str], depth: int) -> QueryOptions:
    """``options`` at ``depth`` levels of $expand. In an $expand only the options of a collection
    are read, and $count is not served there."""
    for name in options:
        if name in NOT_IMPLEMENTED:
            raise QueryOptionRefused(501, "not_implemented", f"{name} is not served yet")
        if name not in _SERVED or (depth and name == "$format"):
            raise QueryOptionRefused(400, "invalid_query_option", f"unknown option {name}")
        if depth and name == "$count":
            raise QueryOptionRefused(501, "not_implemented", "$count in $expand is not served")
    check_format(options.get("$format"), *_JSON_FORMATS)
    return QueryOptions(
        select=_select(options["$select"]) if "$select" in options else None,
        top=_count_of_rows("$top", options["$top"]) if "$top" in options else None,
        skip=_count_of_rows("$skip", options["$skip"]) if "$skip" in options else 0,
        count=_boolean("$count", options["$count"]) if "$count" in options else False,
        filter=parse_filter(options["$filter"]) if "$filter" in options else None,
        order_by=parse_orderby(options["$orderby"]) if "$orderby" in options else (),
        expand=_expand(options["$expand"], depth) if "$expand" in options else (),
    )


def parse_query_options(items: Iterable[tuple[str, str]]) -> QueryOptions:
    """The options of a read's query string. A name without ``$`` is the client's own custom
    option, which the service does not read."""
    return _parsed(_once_each((n, v) for n, v in items if n.startswith("$")), 0)
