# Copyright (c) 2026 Kenneth Stott
# Canary: f3dc9125-a7de-4cb2-aca5-5567b3981c00
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The endpoints API tables are served from, as this process holds them (REQ-314, REQ-1668).

An endpoint belongs to one registered table, and a table is named within its source: two sources
may each register a table of the same name (two OpenAPI specs with a ``getInventory``). The map is
therefore keyed by ``(source_id, table_name)``, never by the name alone — keyed by name, the
second registration replaced the first source's endpoint.

A statement names its tables by the names its SQL carries; which endpoint each name is, is decided
through the registered tables the pipeline resolved for that statement (its ``table_ids``), as
the hot tier does (``materialization._StatementHot``).
"""

from __future__ import annotations

from typing import Any, Iterable

from provisa.api_source.models import ApiEndpoint

EndpointKey = tuple[str, str]


class AmbiguousApiTable(ValueError):
    """A statement reads two API tables under one name; which endpoint the name is, is unknown."""

    def __init__(self, name: str, sources: list[str]) -> None:
        self.name = name
        self.sources = sources
        super().__init__(
            f"the statement reads API table {name!r} of more than one source ({', '.join(sources)})"
        )


def _map(state: Any) -> dict[EndpointKey, ApiEndpoint]:
    return getattr(state, "api_endpoints", None) or {}


def endpoint_of(state: Any, source_id: str, table_name: str) -> ApiEndpoint | None:
    """The endpoint ``source_id``'s table ``table_name`` is served from, or None."""
    return _map(state).get((source_id, table_name))


def put_endpoint(state: Any, endpoint: ApiEndpoint) -> None:
    """Hold ``endpoint`` as the one its table is served from."""
    if not isinstance(getattr(state, "api_endpoints", None), dict):
        state.api_endpoints = {}
    state.api_endpoints[(endpoint.source_id, endpoint.table_name)] = endpoint


def statement_endpoints(state: Any, table_ids: Iterable[int]) -> dict[str, ApiEndpoint]:
    """The endpoints of the API tables a statement reads, by every name its SQL may carry for
    each (the registered name and its SQL spelling). Raises :class:`AmbiguousApiTable` when two
    of them share a name."""
    from provisa.compiler.naming import apply_sql_name

    read = {int(t) for t in table_ids}
    endpoints = _map(state)
    named: dict[str, dict[EndpointKey, ApiEndpoint]] = {}
    for row in getattr(state, "tables", None) or []:
        if int(row["id"]) not in read:
            continue
        key = (row["source_id"], row["table_name"])
        endpoint = endpoints.get(key)
        if endpoint is None:
            continue
        for name in {row["table_name"], apply_sql_name(row["table_name"])}:
            named.setdefault(name, {})[key] = endpoint
    result: dict[str, ApiEndpoint] = {}
    for name, by_key in named.items():
        if len(by_key) > 1:
            raise AmbiguousApiTable(name, sorted(source for source, _ in by_key))
        result[name] = next(iter(by_key.values()))
    return result


def endpoint_named(state: Any, table: str, domain_sql: str | None) -> ApiEndpoint | None:
    """The endpoint of the API table a semantic name ``domain_sql.table`` addresses, or None when
    no API table has that name. Two sources' tables of one name are told apart by the domain each
    is registered in; raises :class:`AmbiguousApiTable` when the name still addresses more than
    one."""
    from provisa.compiler.naming import apply_sql_name, domain_to_sql_name

    hits = {
        key: endpoint
        for key, endpoint in _map(state).items()
        if table in {key[1], apply_sql_name(key[1])}
    }
    if len(hits) > 1 and domain_sql is not None:
        in_domain = {
            (row["source_id"], row["table_name"])
            for row in getattr(state, "tables", None) or []
            if domain_to_sql_name(row["domain_id"]) == domain_sql
        }
        hits = {key: endpoint for key, endpoint in hits.items() if key in in_domain}
    if len(hits) > 1:
        raise AmbiguousApiTable(table, sorted(source for source, _ in hits))
    return next(iter(hits.values()), None)
