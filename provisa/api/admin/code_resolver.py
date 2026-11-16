# Copyright (c) 2026 Kenneth Stott
# Canary: 0d6f3a91-7b2e-4c58-a1e4-93f5b8c2d7e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code resolver set (REQ-2249): code decides, a bridge table stores, an ordinary relationship
serves.

A person picks one code resolver (``provisa.codesource.resolver``) to relate a source table to a
target table. Provisa keeps that as four objects of kinds that already exist, saved by the
mutations that save each kind, with every right and check those ask:

* the bridge, a materialized view over the resolver's call (``call_sql``), named by the set's id,
  in the source table's domain, worked out again when either table changes. It is always landed:
  a command call is run by the statement that names it, and a view read inline is expanded after
  that step, so a bridge that is not landed would never call its resolver;
* ``<id>``, the junction relationship from the source table to the target table through the
  bridge, named in Cypher by its fixed label (the id in upper case), each match's other columns
  (a score) its properties;
* ``<id>_matches``, the source table to the bridge, one-to-many, on the source table's key and the
  match's column carrying it, served under the set's id;
* ``<id>_target``, the bridge to the target table, many-to-one, on the match's column carrying the
  target's key and the target table's key, served under the target table's name.

GraphQL nests the two plain relationships; Cypher serves the junction as one edge and leaves the
plain ones out, as a junction's table is no node.

Each carries the set's id (``code_resolver``), so the set is found again from its objects. They
are changed and retired together, through the set: the ordinary pages refuse to change or retire
one of them apart from it (:func:`held_by_set`). Retiring the set retires all four (REQ-2248).

A bridge column is visible to the roles that see both tables' keys: a role that cannot see one
end's key does not see which rows of it match."""

# Requirements: REQ-2249, REQ-2248, REQ-2226

from __future__ import annotations

import re
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import select, update

from provisa.core.schema_org import registered_tables, relationships

if TYPE_CHECKING:
    from strawberry.types.info import Info as StrawberryInfo

    from provisa.api.admin.types import (
        CodeResolverInput,
        IncrementalOptionType,
        MutationResult,
    )
    from provisa.core.database import Connection

#: The set whose objects the running save or retirement may change (:func:`held_by_set`).
_CHANGING: ContextVar[str | None] = ContextVar("code_resolver_changing", default=None)

#: A set's id names its bridge view: a table name.
_ID = re.compile(r"[a-z][a-z0-9_]*")

#: The schema a view is registered in (core/config_loader.views_as_tables).
_VIEW_SCHEMA = "views"


@dataclass(frozen=True)
class SetNames:
    """The names of a set's objects: its bridge view and its three relationships."""

    bridge: str
    edge: str
    matches: str
    target: str

    @property
    def relationships(self) -> list[str]:
        return [self.edge, self.matches, self.target]


def names(set_id: str) -> SetNames:
    return SetNames(
        bridge=set_id, edge=set_id, matches=f"{set_id}_matches", target=f"{set_id}_target"
    )


@contextmanager
def _changing(set_id: str) -> Generator[None]:
    token = _CHANGING.set(set_id)
    try:
        yield
    finally:
        _CHANGING.reset(token)


def _result(success: bool, message: str, code: str, **params: Any) -> "MutationResult":
    from provisa.api.admin.types import MutationResult

    return MutationResult(success=success, message=message, code=code, params=params)


def held_by_set(row: dict | None, kind: str, name: str) -> "MutationResult | None":
    """The refusal of a change to ``row`` (a relationship or a registered table) made apart from
    the code resolver set it belongs to, or None when it belongs to none or that set is the one
    being changed."""
    held = (row or {}).get("code_resolver")
    if not held or _CHANGING.get() == held:
        return None
    return _result(
        False,
        f"{kind} {name!r} is part of code resolver {held!r}: change or retire it there",
        "schema.code_resolver_part",
        kind=kind,
        name=name,
        resolver=held,
    )


async def _table(conn: "Connection", name: str) -> dict | None:
    from provisa.core.repositories import table as table_repo

    return await table_repo.find_by_table_name(conn, name)


def _statement_name(row: dict) -> str:
    """A table as a statement and the minted listener name it: ``domain.table``."""
    from provisa.compiler.naming import domain_to_sql_name

    return f"{domain_to_sql_name(row['domain_id'])}.{row['table_name']}"


def _column(row: dict, name: str) -> dict | None:
    return next((c for c in row.get("columns") or [] if c["column_name"] == name), None)


async def _ends(
    conn: "Connection", input: "CodeResolverInput"
) -> "tuple[MutationResult | None, dict, dict, list[str]]":
    """The set's two tables, and the roles its matches are visible to, or the refusal naming
    what does not hold. Checked before anything is saved."""
    from provisa.core.repositories import table as table_repo

    found: dict[str, dict] = {}
    for end, name, key in (
        ("source", input.source_table, input.source_column),
        ("target", input.target_table, input.target_column),
    ):
        row = await _table(conn, name)
        if row is None:
            return (
                _result(
                    False,
                    f"{end.capitalize()} table {name!r} is not registered",
                    f"schema.relationship_unknown_{end}",
                    table=name,
                ),
                {},
                {},
                [],
            )
        full = await table_repo.get(conn, row["id"])
        assert full is not None  # just found by name
        if _column(full, key) is None:
            return (
                _result(
                    False,
                    f"{end.capitalize()} table {name!r} has no column {key!r}",
                    "schema.code_resolver_no_key",
                    end=end,
                    table=name,
                    column=key,
                ),
                {},
                {},
                [],
            )
        found[end] = full
    from provisa.api.app import state
    from provisa.security.rights import served_roles

    def readers(end: str, key: str) -> frozenset[str]:
        column = _column(found[end], key)
        assert column is not None  # checked above
        return served_roles(state.roles, state.role_chains, found[end]["domain_id"], column)

    # The rule: the roles each key is served to (security.rights.column_served), both of them.
    seen = sorted(readers("source", input.source_column) & readers("target", input.target_column))
    if not seen:
        return (
            _result(
                False,
                f"no role sees both {input.source_table}.{input.source_column} and "
                f"{input.target_table}.{input.target_column}, so no role could see a match",
                "schema.code_resolver_no_reader",
            ),
            {},
            {},
            [],
        )
    return None, found["source"], found["target"], seen


def _resolver(input: "CodeResolverInput") -> "tuple[MutationResult | None, dict]":
    from provisa.api.app import state
    from provisa.codesource.resolver import ResolverRefused, check_resolver

    command = state.tracked_functions.get(input.resolver)
    if command is None:
        return (
            _result(
                False,
                f"No command {input.resolver!r} is registered",
                "schema.code_resolver_unknown",
                function=input.resolver,
            ),
            {},
        )
    try:
        check_resolver(command, input.source_match, input.target_match)
    except ResolverRefused as refused:
        return _result(False, str(refused), refused.code, **refused.params), {}
    return None, command


def _bridge_input(
    input: "CodeResolverInput",
    command: dict,
    source: dict,
    target: dict,
    readers: list[str],
    schema_name: str = _VIEW_SCHEMA,
) -> Any:
    from provisa.api.admin.types import ColumnInput, TableInput
    from provisa.codesource.resolver import call_sql
    from provisa.core.models import DERIVED_SOURCE_ID

    return TableInput(
        source_id=DERIVED_SOURCE_ID,
        domain_id=source["domain_id"],
        schema_name=schema_name,
        table_name=names(input.id).bridge,
        description=(
            f"The matches of {input.source_table} to {input.target_table} that "
            f"{input.resolver} decides."
        ),
        columns=[
            ColumnInput(name=c["name"], visible_to=list(readers), data_type=c["type"])
            for c in command.get("output_columns") or []
        ],
        view_sql=call_sql(command, _statement_name(source), _statement_name(target)),
        materialize=True,
        mv_refresh_interval=input.refresh_interval,
        mv_incremental=input.incremental,  # kept by the resolver's @change function
        mv_rebuild_every=input.rebuild_every,
    )


def _edges(input: "CodeResolverInput", target: dict) -> list[Any]:
    """The set's three relationships. The junction is the one edge Cypher serves, its matches'
    other columns (a score) its properties; the two plain ones are the levels GraphQL nests, and
    are left out of Cypher, where a junction's table is no node."""
    from provisa.api.admin.types import RelationshipInput

    set_names = names(input.id)
    return [
        RelationshipInput(
            id=set_names.edge,
            source_table_id=input.source_table,
            target_table_id=input.target_table,
            source_column=input.source_column,
            target_column=input.target_column,
            cardinality="one-to-many",
            alias=input.id.upper(),
            via_table=set_names.bridge,
            via_source_column=input.source_match,
            via_target_column=input.target_match,
            via_label_source="fixed",
        ),
        RelationshipInput(
            id=set_names.matches,
            source_table_id=input.source_table,
            target_table_id=set_names.bridge,
            source_column=input.source_column,
            target_column=input.source_match,
            cardinality="one-to-many",
            graphql_alias=input.id,
            disable_cypher=True,
        ),
        RelationshipInput(
            id=set_names.target,
            source_table_id=set_names.bridge,
            target_table_id=input.target_table,
            source_column=input.target_match,
            target_column=input.target_column,
            cardinality="many-to-one",
            graphql_alias=target["alias"] or target["table_name"],
            disable_cypher=True,
        ),
    ]


async def _mark(conn: "Connection", set_id: str, bridge_id: int) -> None:
    set_names = names(set_id)
    await conn.execute_core(
        update(registered_tables)
        .where(registered_tables.c.id == bridge_id)
        .values(code_resolver=set_id)
    )
    await conn.execute_core(
        update(relationships)
        .where(relationships.c.id.in_(set_names.relationships))
        .values(code_resolver=set_id)
    )


async def _discard(conn: "Connection", set_id: str, bridge_id: int | None) -> None:
    """Remove what a save that did not complete made. Nothing of it was ever in service: the
    bridge was still a draft and its relationships were never served, so no person's object is
    removed (REQ-2248 retires a person's objects)."""
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import table as table_repo

    set_names = names(set_id)
    await rel_repo.remove_where(conn, relationships.c.id.in_(set_names.relationships))
    if bridge_id is not None:
        await table_repo.discard(conn, bridge_id)


async def save(
    info: "StrawberryInfo", input: "CodeResolverInput", *, creating: bool
) -> "MutationResult":
    """Save the set ``input`` describes: a new set, or a change to one (its resolver, its keys,
    its refresh interval). Each object is saved by its own kind's mutation."""
    from provisa.api.admin.capabilities import require_domain
    from provisa.api.admin.schema_helpers import _get_pool
    from provisa.api.admin.schema_mutation import _save_relationship_as_caller, _single
    from provisa.api.admin import schema_mutation_ops as ops
    from provisa.api.admin.domain_guard import require_domains

    if not _ID.fullmatch(input.id):
        return _result(
            False,
            f"Code resolver id {input.id!r} is not a name: lower-case letters, digits and "
            "underscores, starting with a letter",
            "schema.code_resolver_bad_id",
            resolver=input.id,
        )
    refused, command = _resolver(input)
    if refused is not None:
        return refused
    pool = await _get_pool()
    async with pool.acquire() as raw:
        conn = cast("Connection", raw)
        refused, source, target, readers = await _ends(conn, input)
        if refused is not None:
            return refused
        bridge = await _table(conn, names(input.id).bridge)
    # The set reads both tables: its author acts in both their domains.
    require_domain(info, source["domain_id"])
    require_domains(info, {target["domain_id"]})
    if creating and bridge is not None:
        return _result(
            False,
            f"Code resolver {input.id!r} already exists",
            "schema.code_resolver_taken",
            resolver=input.id,
        )
    if not creating and (bridge is None or bridge.get("code_resolver") != input.id):
        return _result(
            False,
            f"Code resolver {input.id!r} not found",
            "schema.code_resolver_not_found",
            resolver=input.id,
        )
    # A change is saved over the bridge as it is registered: under the schema it was stored in.
    view = _bridge_input(
        input,
        command,
        source,
        target,
        readers,
        _VIEW_SCHEMA if bridge is None else bridge["schema_name"],
    )
    with _changing(input.id):
        if creating:
            saved = await ops.register_table(info, view)
        else:
            saved = await _single().update_table(info, view)
        if not saved.success:
            return saved
        async with pool.acquire() as raw:
            made = await _table(cast("Connection", raw), names(input.id).bridge)
        assert made is not None  # just saved
        for edge in _edges(input, target):
            saved = await _save_relationship_as_caller(info, edge, creating=creating)
            if not saved.success:
                if creating:
                    async with pool.acquire() as raw:
                        conn = cast("Connection", raw)
                        async with conn.transaction():
                            await _discard(conn, input.id, made["id"])
                return saved
        async with pool.acquire() as raw:
            await _mark(cast("Connection", raw), input.id, made["id"])
        if creating:
            released = await _single().set_table_draft(info, made["id"], False)
            if not released.success:
                return released
    return _result(
        True,
        f"Code resolver {input.id!r} saved",
        "schema.code_resolver_saved",
        resolver=input.id,
    )


async def retire(
    info: "StrawberryInfo", set_id: str, reason: str | None = None
) -> "MutationResult":
    """Retire the set: its three relationships and its bridge, together (REQ-2248). Their names
    stay taken. Refused as a whole when any of them may not be retired."""
    from provisa.api.admin.capabilities import require_capability
    from provisa.api.admin.domain_guard import require_domains
    from provisa.api.admin.schema_helpers import _get_pool, _rebuild_schemas
    from provisa.core.repositories import relationship as rel_repo
    from provisa.core.repositories import table as table_repo

    require_capability(info, "create_relationship")
    require_capability(info, "table_registration")
    set_names = names(set_id)
    pool = await _get_pool()
    async with pool.acquire() as raw:
        conn = cast("Connection", raw)
        bridge = await _table(conn, set_names.bridge)
        if bridge is None or bridge.get("code_resolver") != set_id or bridge["retired"]:
            return _result(
                False,
                f"Code resolver {set_id!r} not found",
                "schema.code_resolver_not_found",
                resolver=set_id,
            )
        domains = {bridge["domain_id"]}
        for rel_id in set_names.relationships:
            rel = await rel_repo.get(conn, rel_id)
            assert rel is not None  # a set holds both its relationships
            for table_id in (rel["source_table_id"], rel["target_table_id"]):
                end = await table_repo.get(conn, table_id)
                assert end is not None
                domains.add(end["domain_id"])
        require_domains(info, domains)
        try:
            async with conn.transaction():
                for rel_id in set_names.relationships:
                    await rel_repo.delete(conn, rel_id, reason=reason)
                await table_repo.delete(conn, bridge["id"], reason=reason)
        except (rel_repo.RelationshipDeleteRefused, table_repo.TableDeleteRefused) as refused:
            from provisa.api.admin.read_rights import reader_for, refusal

            said, named = await refusal(conn, reader_for(info), refused)
            return _result(
                False,
                said,
                "schema.code_resolver_has_dependents",
                resolver=set_id,
                dependents=named,
            )
    await _rebuild_schemas()
    return _result(
        True,
        f"Code resolver {set_id!r} retired",
        "schema.code_resolver_retired",
        resolver=set_id,
    )


async def listed(conn: "Connection") -> list[dict]:
    """Every code resolver set in service, as the form shows and edits it: read back from its
    objects."""
    from provisa.codesource.resolver import called

    out: list[dict] = []
    bridges = (
        await conn.execute_core(
            select(registered_tables).where(
                registered_tables.c.code_resolver.is_not(None),
                registered_tables.c.retired.is_(False),
            )
        )
    ).fetchall()
    for bridge in bridges:
        set_id = bridge.code_resolver
        set_names = names(set_id)
        edges = {
            r.id: r
            for r in (
                await conn.execute_core(
                    select(relationships).where(
                        relationships.c.id.in_([set_names.matches, set_names.target])
                    )
                )
            ).fetchall()
        }
        matches, to_target = edges[set_names.matches], edges[set_names.target]
        tables = {
            r.id: r
            for r in (
                await conn.execute_core(
                    select(registered_tables).where(
                        registered_tables.c.id.in_(
                            [matches.source_table_id, to_target.target_table_id]
                        )
                    )
                )
            ).fetchall()
        }
        source, target = tables[matches.source_table_id], tables[to_target.target_table_id]
        out.append(
            {
                "id": set_id,
                "resolver": called(bridge.view_sql),
                "source_table": source.alias or source.table_name,
                "source_column": matches.source_column,
                "source_match": matches.target_column,
                "target_table": target.alias or target.table_name,
                "target_column": to_target.target_column,
                "target_match": to_target.source_column,
                "refresh_interval": int(bridge.mv_refresh_interval),
                "incremental": bool(bridge.mv_incremental),
                "rebuild_every": bridge.mv_rebuild_every,
            }
        )
    return out


async def incremental_option(resolver: str, set_id: str) -> "IncrementalOptionType":
    """Whether the set ``set_id`` over ``resolver`` may keep its matches incrementally: only when
    the resolver's source declares a @change function for its bridge (REQ-2249). Asked by the
    form before the set exists, and of a set in service."""
    from provisa.api.admin.types import IncrementalOptionType
    from provisa.api.app import state
    from provisa.mv.change_refresh import incremental_option as option_of

    command = state.tracked_functions.get(resolver)
    if command is None:
        return IncrementalOptionType(
            available=False,
            code="schema.code_resolver_unknown",
            reason=f"No command {resolver!r} is registered",
        )
    option = await option_of(state, command, names(set_id).bridge)
    return IncrementalOptionType(available=option.available, code=option.code, reason=option.reason)
