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

from collections.abc import Iterable
from dataclasses import dataclass

# Options a later job serves. Answered 501, never ignored: a read that ignores $filter answers
# rows the client asked not to see, as though it had applied it.
NOT_IMPLEMENTED = frozenset(
    {"$filter", "$orderby", "$expand", "$search", "$apply", "$compute", "$levels"}
)
_JSON_FORMATS = ("json", "application/json")


class QueryOptionRefused(ValueError):
    """A system query option the service does not take; ``status`` is the answer."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = code


@dataclass(frozen=True)
class QueryOptions:
    select: tuple[str, ...] | None = None  # None: every property
    top: int | None = None
    skip: int = 0
    count: bool = False


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


def parse_query_options(items: Iterable[tuple[str, str]]) -> QueryOptions:
    """The options of a read's query string. A name without ``$`` is the client's own custom
    option, which the service does not read."""
    options: dict[str, str] = {}
    for name, value in items:
        if not name.startswith("$"):
            continue
        if name in options:
            raise QueryOptionRefused(400, "invalid_query_option", f"{name} is given twice")
        options[name] = value
    for name in options:
        if name in NOT_IMPLEMENTED:
            raise QueryOptionRefused(501, "not_implemented", f"{name} is not served yet")
        if name not in ("$select", "$top", "$skip", "$count", "$format"):
            raise QueryOptionRefused(400, "invalid_query_option", f"unknown option {name}")
    check_format(options.get("$format"), *_JSON_FORMATS)
    return QueryOptions(
        select=_select(options["$select"]) if "$select" in options else None,
        top=_count_of_rows("$top", options["$top"]) if "$top" in options else None,
        skip=_count_of_rows("$skip", options["$skip"]) if "$skip" in options else 0,
        count=_boolean("$count", options["$count"]) if "$count" in options else False,
    )
