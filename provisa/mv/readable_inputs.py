# Copyright (c) 2026 Kenneth Stott
# Canary: 8c3e5f17-4a92-4d6b-b1e0-7f2a9c6d3e54
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A materialized view reads only inputs the engine can read whole.

A view is built by the engine running the view's SQL over its inputs. That is the view's contents
only when each input is all of its table: a live-attached table, a whole-table replica, an API
endpoint that takes no arguments (replicated whole), or another view. Three kinds of input are
not that, and a view over one would be built from whatever rows requests happened to leave
behind:

* a row-level replicated table — it holds only the rows requests have fetched;
* an API table that needs arguments — it has no rows until a request supplies them;
* a table in a per-request cache schema — it holds what one request last cached.

A view over such an input is refused when it is saved (admin mutation, config load) and when it
is refreshed, with an error naming the view, the input and the reason.

The inputs are the view's SQL lineage (``provisa.events.lineage.extract_inputs``) and each kind
is recognised from the registry — the registered tables, the API endpoints, the cache schemas —
never by a second reading of the SQL."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from provisa.mv.models import MVDefinition

_ROW_LEVEL = "row-level replicated table"
_PARAMETERIZED = "API table that needs arguments"
_REQUEST_CACHE = "per-request cache table"

_PATH_PLACEHOLDER = re.compile(r"\{[^{}]+\}")

# The per-request cache stores among the schemas an org derives (provisa.core.environments
# SCHEMA_SUFFIXES): the API result cache and the GraphQL result cache. ``_mv_cache`` holds views.
_REQUEST_CACHE_SUFFIXES = ("_api_cache", "_gql_cache")


@dataclass(frozen=True)
class UnreadableInput:
    name: str
    kind: str
    reason: str

    def __str__(self) -> str:
        return f"{self.name!r} is a {self.kind}: {self.reason}"


class ViewInputNotReadable(ValueError):
    """A materialized view's definition reads an input the engine cannot read whole."""

    def __init__(self, view: str, inputs: list[UnreadableInput]) -> None:
        self.view = view
        self.inputs = inputs
        super().__init__(
            f"materialized view {view!r} cannot be built: "
            + "; ".join(str(i) for i in inputs)
            + ". A view reads only live-attached tables, whole-table replicas and other views."
        )


def _needed_path_arguments(endpoint: Any) -> list[str]:
    """The arguments an API endpoint cannot be called without: its path's placeholders."""
    named = [m.group(0)[1:-1] for m in _PATH_PLACEHOLDER.finditer(endpoint.path)]
    declared = [
        c.param_name or c.name
        for c in endpoint.columns
        if c.param_type is not None and c.param_type.value == "path"
    ]
    return sorted(set(named) | set(declared))


def _needed_remote_arguments(state: Any, table: str) -> list[str]:
    """The required arguments of the remote GraphQL field registered as ``table``."""
    for registration in state.graphql_remote_sources.values():
        for remote in registration["tables"]:
            if remote["sql_name"] == table:
                return sorted(str(a) for a in remote["required_args"])
    return []


async def _request_cache_schemas(state: Any) -> set[str]:
    """The cache schemas API sources write a request's result into: the default one and any a
    source with endpoints declares. (An org's derived ``…_api_cache`` / ``…_gql_cache`` schemas
    are recognised by their suffix.)"""
    from provisa.api_source.engine_cache import _DEFAULT_CACHE_SCHEMA  # noqa: PLC0415
    from provisa.federation.registry_view import registered_sources  # noqa: PLC0415

    with_endpoints = {endpoint.source_id for endpoint in state.api_endpoints.values()}
    declared = {s.cache_schema for s in await registered_sources(state) if s.id in with_endpoints}
    return {_DEFAULT_CACHE_SCHEMA} | declared


def view_inputs(mv: MVDefinition) -> list[str]:
    """The inputs ``mv`` reads: the lineage of its SQL, or — for a join-pattern view, which has
    no SQL of its own — the tables it joins."""
    from provisa.events.lineage import extract_inputs  # noqa: PLC0415

    if mv.sql:
        return sorted(extract_inputs(mv.sql, "postgres"))
    return sorted(mv.source_tables)


async def unreadable_inputs(inputs: list[str], state: Any) -> list[UnreadableInput]:
    """Those of ``inputs`` a materialized view may not read, each with its kind and reason.
    Empty when every input is readable."""
    from provisa.compiler.naming import apply_sql_name  # noqa: PLC0415
    from provisa.federation.query_residency import (  # noqa: PLC0415
        row_materialized_tables_by_name,
    )

    row_level = await row_materialized_tables_by_name(state)
    cache_schemas = await _request_cache_schemas(state)
    found: list[UnreadableInput] = []
    for name in inputs:
        parts = name.split(".")
        table = parts[-1]
        schema = parts[-2] if len(parts) > 1 else None
        if schema is not None and (
            schema in cache_schemas or schema.endswith(_REQUEST_CACHE_SUFFIXES)
        ):
            found.append(
                UnreadableInput(
                    name, _REQUEST_CACHE, f"schema {schema!r} holds what a request last cached"
                )
            )
            continue
        if table in row_level or apply_sql_name(table) in row_level:
            found.append(
                UnreadableInput(name, _ROW_LEVEL, "it holds only the rows requests have fetched")
            )
            continue
        endpoint = state.api_endpoints.get(table)
        needed = (
            _needed_path_arguments(endpoint)
            if endpoint is not None
            else _needed_remote_arguments(state, table)
        )
        if needed:
            found.append(
                UnreadableInput(
                    name,
                    _PARAMETERIZED,
                    f"it has no rows without the arguments a request supplies ({', '.join(needed)})",
                )
            )
    return found


async def require_readable_inputs(mv: MVDefinition, state: Any) -> None:
    """Raise :class:`ViewInputNotReadable` when ``mv`` reads an input a view may not read."""
    found = await unreadable_inputs(view_inputs(mv), state)
    if found:
        raise ViewInputNotReadable(mv.id, found)


async def register_view(state: Any, mv: MVDefinition) -> None:
    """Register ``mv`` — the one way a view enters the registry. A view whose definition reads an
    unreadable input is refused and nothing is registered."""
    await require_readable_inputs(mv, state)
    state.mv_registry.register(mv)


async def require_views_readable(state: Any, views: list[MVDefinition]) -> None:
    """Refuse the first of ``views`` that reads an unreadable input. A config load calls this
    once the registry its views are checked against (tables, API endpoints) is loaded."""
    for mv in views:
        await require_readable_inputs(mv, state)
