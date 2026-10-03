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
_UNRESOLVED = "reference that names no table or view"
_AMBIGUOUS = "reference more than one table or view answers to"
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


def read_table_names(sql: str) -> frozenset[str]:
    """The bare names of the tables a view's semantic SQL reads (``domain.table`` → ``table``)."""
    from provisa.events.lineage import extract_inputs  # noqa: PLC0415

    return frozenset(name.rsplit(".", 1)[-1] for name in extract_inputs(sql, "postgres"))


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


def unresolved_inputs(mv: MVDefinition, state: Any) -> list[UnreadableInput]:
    """Those of ``mv``'s inputs the model cannot resolve to exactly one registered table or
    materialized view (``provisa.mv.view_inputs`` — the same resolution the event graph's edges
    are built from). Empty when every input resolves."""
    from provisa.mv.view_inputs import InputUnresolved, ModelIndex, view_refs  # noqa: PLC0415

    index = ModelIndex(state)
    found: list[UnreadableInput] = []
    for parts in view_refs(mv):
        try:
            index.resolve(mv.id, parts)
        except InputUnresolved as unresolved:
            if unresolved.candidates:
                found.append(
                    UnreadableInput(
                        unresolved.ref,
                        _AMBIGUOUS,
                        f"it names {', '.join(unresolved.candidates)}; qualify it",
                    )
                )
            else:
                found.append(
                    UnreadableInput(
                        unresolved.ref,
                        _UNRESOLVED,
                        "no registered table or materialized view has that name",
                    )
                )
    return found


async def require_readable_inputs(mv: MVDefinition, state: Any) -> None:
    """Raise :class:`ViewInputNotReadable` when ``mv`` reads an input a view may not read, or one
    the model cannot resolve to exactly one table or view (REQ-939: a view's edges are resolved
    from the model, so a view whose input does not resolve is refused here, when it is declared,
    and never reaches the event graph)."""
    unreadable = await unreadable_inputs(view_inputs(mv), state)
    named = {i.name for i in unreadable}
    # An input already refused as unreadable is named once, for that.
    found = [i for i in unresolved_inputs(mv, state) if i.name not in named] + unreadable
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


def config_table_views(state: Any, raw_config: dict) -> list[MVDefinition]:
    """The materialized views a config declares as TABLE entries (``view_sql`` with
    ``materialize: true``), as the schema build registered them. A config declares a view in
    this spelling or under ``views:``; the load checks both the same way."""
    views: list[MVDefinition] = []
    for table in raw_config.get("tables") or []:
        if not (table.get("view_sql") and table.get("materialize")):
            continue
        name = table.get("table") or table["table_name"]
        mv = state.mv_registry.get(f"view-{name}")
        if mv is None:
            raise RuntimeError(
                f"config view {name!r} (view_sql, materialize: true) was not registered by the "
                "schema build"
            )
        views.append(mv)
    return views


class ViewsReadTable(ValueError):
    """A table cannot become an input no view may read while materialized views read it."""

    def __init__(self, table: str, kind: str, reason: str, views: list[str]) -> None:
        self.table = table
        self.views = views
        named = ", ".join(repr(v) for v in views)
        super().__init__(
            f"table {table!r} cannot become a {kind}: materialized view(s) {named} read it, "
            f"and {reason}. Remove or change those views first."
        )


async def _views_reading(conn: Any, names: set[str]) -> list[str]:
    """The materialized views — saved in the control plane, or held in memory from the config —
    whose inputs include one of ``names``."""
    from sqlalchemy import select  # noqa: PLC0415

    from provisa.api.app import state  # noqa: PLC0415
    from provisa.compiler.naming import apply_sql_name  # noqa: PLC0415
    from provisa.core.schema_org import registered_tables  # noqa: PLC0415
    from provisa.events.lineage import extract_inputs  # noqa: PLC0415

    wanted = names | {apply_sql_name(n) for n in names}

    def _reads(inputs: list[str]) -> bool:
        return any(i.split(".")[-1] in wanted for i in inputs)

    saved = await conn.execute_core(
        select(registered_tables.c.table_name, registered_tables.c.view_sql).where(
            registered_tables.c.materialize, registered_tables.c.view_sql.is_not(None)
        )
    )
    reading = {
        f"view-{row.table_name}"
        for row in saved.fetchall()
        if _reads(sorted(extract_inputs(row.view_sql, "postgres")))
    }
    reading |= {mv.id for mv in state.mv_registry.all() if _reads(view_inputs(mv))}
    return sorted(reading)


async def require_row_level_switch_allowed(conn: Any, table: Any) -> None:
    """Refuse storing ``table`` as row-level replicated when it was not stored so before and a
    materialized view reads it: that view's next refresh could only fail. Called where a table
    is persisted, so every way of changing a table is covered.

    A table the bound engine attaches live is not row-level whatever its flag says
    (``query_residency.active_row_materialize_tables``), so its switch is not one."""
    from sqlalchemy import select  # noqa: PLC0415

    from provisa.api.app import state  # noqa: PLC0415
    from provisa.core.schema_org import registered_tables  # noqa: PLC0415
    from provisa.federation.registry_view import registered_sources  # noqa: PLC0415
    from provisa.federation.strategy import engine_attaches  # noqa: PLC0415

    stored = await conn.execute_core(
        select(registered_tables.c.row_materialize).where(
            registered_tables.c.source_id == table.source_id,
            registered_tables.c.schema_name == table.schema_name,
            registered_tables.c.table_name == table.table_name,
        )
    )
    row = stored.fetchone()
    if row is not None and row.row_materialize:
        return  # already row-level: not a switch
    names = {n for n in (table.table_name, table.alias) if n}
    views = await _views_reading(conn, names)
    if not views:
        return
    sources = {s.id: s for s in await registered_sources(state, conn)}
    if engine_attaches(state.federation_engine, sources[table.source_id].type.value):
        return
    raise ViewsReadTable(
        table.table_name, _ROW_LEVEL, "it would hold only the rows requests have fetched", views
    )


async def require_argument_switch_allowed(conn: Any, endpoint: Any) -> None:
    """Refuse storing an API endpoint that needs arguments over one that did not while a
    materialized view reads its table."""
    from sqlalchemy import select  # noqa: PLC0415

    from provisa.core.schema_org import api_endpoints  # noqa: PLC0415

    needed = _needed_path_arguments(endpoint)
    if not needed:
        return
    stored = await conn.execute_core(
        select(api_endpoints.c.path).where(api_endpoints.c.table_name == endpoint.table_name)
    )
    row = stored.fetchone()
    if row is None or _PATH_PLACEHOLDER.search(row.path):
        return  # a new endpoint, or one that already needed arguments: not a switch
    views = await _views_reading(conn, {endpoint.table_name})
    if views:
        raise ViewsReadTable(
            endpoint.table_name,
            _PARAMETERIZED,
            f"it would have no rows without the arguments a request supplies ({', '.join(needed)})",
            views,
        )
