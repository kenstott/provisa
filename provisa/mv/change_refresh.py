# Copyright (c) 2026 Kenneth Stott
# Canary: 5e2b7c90-3d14-4a8f-9b61-0c7e2f4d8a35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A landed code resolver's result kept fresh by its ``@change`` function (REQ-2249).

A view whose definition calls a code command (a code resolver's bridge) is always landed
(``view_read.lands_stored_read``). With its incremental setting on (``mv_incremental``) and a
``@change`` function in the command's source naming the view's table, a refresh calls that
function instead of recomputing the whole result:

* with no cursor on the first build and on every whole rebuild -- its stored cursor is none, an
  operator asks for a full refresh, or ``rebuild_every`` seconds have passed since the last whole
  rebuild (``federation.delta.delta_build_reason``, the replicas' rule) -- and every match it
  returns replaces the landed rows;
* otherwise with the cursor stored after the last successful run, and the matches it returns
  applied by the existing incremental apply (``apply_cdc``): upsert by key, and a row flagged as
  removed removes its key.

The function is given the resolver's input tables (REQ-2226), as the resolver's call names them,
and the cursor; it never learns where its result is stored. Its rows are refused, with nothing
applied and no cursor stored, when one lacks a key column, a key repeats within the result, or a
cursor goes backwards. The cursor is stored on the view's build state only after the apply
succeeded.

With the incremental setting off, a refresh recomputes the whole result with the resolver's
ordinary function, and ``@change`` is never called. A view whose resolver's source declares no
``@change`` for it cannot have the setting on (:func:`incremental_option`), and one kept
incrementally needs ``rebuild_every``.
"""

# Requirements: REQ-2249, REQ-874, REQ-2226

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import sqlglot
import sqlglot.expressions as exp
from sqlalchemy import select, update

from provisa.core.schema_org import mv_build_state

if TYPE_CHECKING:
    from provisa.codesource.manifest import Export, Manifest
    from provisa.core.database import Database
    from provisa.mv.models import MVDefinition

#: The setting is refused: the resolver's source declares no @change function for the table.
NO_CHANGE = "schema.incremental_no_change"
#: The setting is refused: a view kept incrementally rebuilds whole on a clock, which it names.
NEEDS_REBUILD_EVERY = "schema.incremental_needs_rebuild_every"
#: A refresh is refused: a returned row lacks a key column.
KEY_MISSING = "mv.change_key_missing"
#: A refresh is refused: a key repeats within one result.
KEY_REPEATED = "mv.change_key_repeated"
#: A refresh is refused: a returned cursor is before the stored one.
CURSOR_BACKWARDS = "mv.change_cursor_backwards"

#: The argument kind of a table parameter (models.FunctionArgument.arg_kind).
_TABLE_REF = "table_ref"


class ChangeRefused(ValueError):
    """A refusal said with its stable ``code`` and what it names (``params``)."""

    def __init__(self, code: str, message: str, **params: Any) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


@dataclass(frozen=True)
class IncrementalOption:
    """Whether a view's incremental setting may be on; when not, its refusal's code and reason."""

    available: bool
    code: str | None = None
    reason: str | None = None


@dataclass(frozen=True)
class Kept:
    """A view kept by a ``@change`` function: the resolver's command, the function, the manifest
    it was read in, and the tables the resolver's call names, in its parameters' order."""

    command: dict
    export: "Export"
    manifest: "Manifest"
    tables: tuple[str, ...]


def code_call(sql: str, commands: Mapping[str, dict]) -> tuple[dict, tuple[str, ...]] | None:
    """The registered code command ``sql`` calls in its FROM and the table names its call gives
    the command's table parameters, in order; None when it calls none. ``commands`` are the
    registered commands by the name a statement calls them by (``state.tracked_functions``)."""
    from provisa.codesource.commands import CODE_COMMAND

    for table in sqlglot.parse_one(sql, read="postgres").find_all(exp.Table):
        call = table.this
        if not isinstance(call, exp.Anonymous):
            continue
        command = commands.get(call.name)
        if command is None or command.get("impl_kind") != CODE_COMMAND:
            continue
        values = list(call.expressions)
        names = tuple(
            values[i].this
            for i, argument in enumerate(command.get("arguments") or [])
            if argument.get("arg_kind") == _TABLE_REF and i < len(values)
        )
        return command, names
    return None


async def incremental_option(state: Any, command: dict, table: str) -> IncrementalOption:
    """Whether the view ``table``, calling the code command ``command``, may be kept
    incrementally: only when ``command``'s source declares a ``@change`` function naming it."""
    from provisa.codesource.commands import change_of

    if await change_of(state, command["source_id"], table) is not None:
        return IncrementalOption(True)
    return IncrementalOption(
        False,
        NO_CHANGE,
        f"{command['function_name']}'s source has no @change function for {table}: declare one "
        f"with @change table={table}",
    )


async def incremental_refusal(
    state: Any, table: str, view_sql: str | None, incremental: bool, rebuild_every: int | None
) -> ChangeRefused | None:
    """Why the view ``table`` defined by ``view_sql`` cannot be saved with its incremental setting
    ``incremental`` and ``rebuild_every``; None when it can. Only a view calling a code command
    is judged here."""
    if not incremental or not view_sql:
        return None
    called = code_call(view_sql, state.tracked_functions)
    if called is None:
        return None
    option = await incremental_option(state, called[0], table)
    if not option.available:
        assert option.code is not None and option.reason is not None
        return ChangeRefused(option.code, option.reason, table=table)
    if rebuild_every is None:
        return ChangeRefused(
            NEEDS_REBUILD_EVERY,
            f"{table} is kept incrementally, so it is rebuilt whole on a clock: set how often "
            "(rebuild every, in seconds)",
            table=table,
        )
    return None


async def kept_by_change(state: Any, mv: "MVDefinition") -> Kept | None:
    """``mv`` kept by a ``@change`` function: None when its incremental setting is off or its
    definition calls no code command. Refused when it is on and cannot be kept so."""
    from provisa.codesource.commands import change_of

    if not mv.incremental or mv.semantic_sql is None:
        return None
    called = code_call(mv.semantic_sql, state.tracked_functions)
    if called is None:
        return None
    command, tables = called
    assert mv.view_table is not None  # a view's build names the view it builds
    refused = await incremental_refusal(
        state, mv.view_table, mv.semantic_sql, True, mv.rebuild_every
    )
    if refused is not None:
        raise refused
    found = await change_of(state, command["source_id"], mv.view_table)
    assert found is not None  # incremental_refusal found it
    export, manifest = found
    return Kept(command, export, manifest, tables)


def _plain(value: Any) -> Any:
    """``value`` as it is stored and passed back: its JSON form read back."""
    return json.loads(json.dumps(value, default=str))


def checked(rows: list[dict], kept: Kept, cursor: Any) -> tuple[list[SimpleNamespace], Any]:
    """The apply events of ``rows`` (a delete for a row flagged as removed, else an upsert) and
    the cursor they advance ``cursor`` to; refused when a row lacks a key column, a key repeats,
    or a row's cursor is before ``cursor``."""
    change = kept.export.change
    assert change is not None  # a @change export carries its declaration
    function = kept.export.name
    seen: set[str] = set()
    events: list[SimpleNamespace] = []
    advanced = cursor
    for row in rows:
        missing = [k for k in change.key if k not in row]
        if missing:
            raise ChangeRefused(
                KEY_MISSING,
                f"{function} returned a row without its key column {missing[0]}",
                function=function,
                column=missing[0],
            )
        key = json.dumps([_plain(row[k]) for k in change.key])
        if key in seen:
            raise ChangeRefused(
                KEY_REPEATED,
                f"{function} returned the key {key} twice in one result",
                function=function,
                key=key,
            )
        seen.add(key)
        at = _plain(row[change.cursor])
        if cursor is not None and at < cursor:
            raise ChangeRefused(
                CURSOR_BACKWARDS,
                f"{function}'s cursor went backwards: it returned {at}, before the stored {cursor}",
                function=function,
                returned=str(at),
                stored=str(cursor),
            )
        advanced = at if advanced is None or at > advanced else advanced
        events.append(
            SimpleNamespace(operation="delete" if row[change.removed] else "upsert", row=row)
        )
    return events, advanced


def _columns(kept: Kept) -> list[tuple[str, str]]:
    """The landed table's columns: the resolver's, which the view over its call reads."""
    return [(c["name"], c["type"]) for c in kept.command.get("output_columns") or []]


def _landed(kept: Kept, row: dict) -> dict:
    """``row`` as the landed table holds it: its columns, which the row must carry."""
    missing = [name for name, _ in _columns(kept) if name not in row]
    if missing:
        raise ValueError(
            f"{kept.export.name} returned a row without the column {missing[0]} the landed "
            "table holds"
        )
    return {name: row[name] for name, _ in _columns(kept)}


async def call_change(state: Any, kept: Kept, cursor: Any) -> list[dict]:
    """Call ``kept``'s ``@change`` function with the resolver's tables and ``cursor`` (None: no
    cursor), as the build calls the resolver: as the region's administrator."""
    from provisa.codesource.commands import contract
    from provisa.codesource.manifest import TableOf
    from provisa.core import region_admin
    from provisa.core.request_context import reset_session_vars, set_session_vars
    from provisa.executor.function_dispatch import dispatch_function

    arguments, outputs = contract(kept.manifest, kept.export)
    function = {
        **kept.command,
        "name": kept.export.name,
        "function_name": kept.export.name,
        "arguments": arguments,
        "output_columns": outputs,
        "binding": {},
        "materialize": False,
        "writes_table": None,
    }
    tables = [p.name for p in kept.export.parameters if isinstance(p.shape, TableOf)]
    (cursor_parameter,) = [
        p.name for p in kept.export.parameters if not isinstance(p.shape, TableOf)
    ]
    args: dict[str, Any] = dict(zip(tables, kept.tables, strict=True))
    args[cursor_parameter] = cursor
    token = set_session_vars(region_admin.session_vars(state))
    try:
        return await dispatch_function(function, args, state, region_admin.ROLE)
    finally:
        reset_session_vars(token)


async def _stored(ledger: "Database", mv_id: str) -> tuple[Any, datetime | None]:
    """The view's stored cursor (None: none yet) and when it was last rebuilt whole."""
    t = mv_build_state
    async with ledger.acquire() as conn:
        row = (
            await conn.execute_core(
                select(t.c.change_cursor, t.c.change_rebuilt_at).where(t.c.mv_id == mv_id)
            )
        ).first()
    if row is None:
        return None, None
    cursor = None if row.change_cursor is None else json.loads(row.change_cursor)
    return cursor, row.change_rebuilt_at


async def _store(
    ledger: "Database", mv: "MVDefinition", cursor: Any, *, rebuilt_at: datetime | None
) -> None:
    """Store the cursor after a successful apply, and when a whole rebuild was."""
    from provisa.mv.coordination import ensure_build_row

    await ensure_build_row(ledger, mv)
    values: dict[str, Any] = {
        "change_cursor": None if cursor is None else json.dumps(cursor, default=str)
    }
    if rebuilt_at is not None:
        values["change_rebuilt_at"] = rebuilt_at
    async with ledger.acquire() as conn:
        await conn.execute_core(
            update(mv_build_state).where(mv_build_state.c.mv_id == mv.id).values(**values)
        )


@dataclass(frozen=True)
class Applied:
    """What a refresh by ``@change`` did: rebuilt whole (and why) or applied by cursor, and the
    rows it applied."""

    whole: str | None
    rows: list[dict]
    landed: str | None


async def refresh_by_change(
    state: Any, engine: Any, mv: "MVDefinition", kept: Kept, *, full: bool = False
) -> Applied:
    """Refresh ``mv`` through its ``@change`` function: whole when the replicas' rule says so
    (no cursor, an operator's ``full`` refresh, ``rebuild_every`` elapsed), else by the stored
    cursor. ``engine`` is the EngineRuntime it lands through."""
    from provisa.federation import replica_state as rs
    from provisa.federation.delta import delta_build_reason

    change = kept.export.change
    assert change is not None
    ledger = state.tenant_db
    cursor, rebuilt_at = await _stored(ledger, mv.id)
    now = datetime.now(UTC)
    due = (
        mv.rebuild_every is not None
        and rebuilt_at is not None
        and (now - rebuilt_at).total_seconds() >= mv.rebuild_every
    )
    whole = delta_build_reason(
        has_delta=True,
        has_cursor=cursor is not None,
        reason=rs.REASON_OPERATOR if full else "",
        definition_changed=False,
        store_applies_delta=True,
        rebuild_due=due,
    )
    given = None if whole is not None else cursor
    rows = await call_change(state, kept, given)
    events, advanced = checked(rows, kept, given)
    key = list(change.key)
    columns = _columns(kept)
    landed: str | None = None
    if whole is not None:
        kept_rows = [_landed(kept, e.row) for e in events if e.operation == "upsert"]
        landed = await engine.persist_mv_table(
            schema=mv.target_schema,
            table=mv.target_table,
            columns=columns,
            rows=kept_rows,
            persist="replace",
            pk_columns=key,
        )
    elif events:
        applied = [
            SimpleNamespace(
                operation=e.operation,
                row=_landed(kept, e.row) if e.operation == "upsert" else {k: e.row[k] for k in key},
            )
            for e in events
        ]
        await engine.apply_cdc_events(
            schema=mv.target_schema,
            table=mv.target_table,
            columns=columns,
            pk_columns=key,
            events=applied,
        )
    await _store(ledger, mv, advanced, rebuilt_at=now if whole is not None else None)
    return Applied(whole, [e.row for e in events], landed)


def change_generate(state: Any, engine: Any, mv: "MVDefinition") -> Any:
    """The event loop's generate handler of ``mv`` when it is kept by a ``@change`` function (its
    incremental setting on and its definition calling a code command); None otherwise. Each fire
    refreshes it through the function and emits the rows it applied; nothing applied, nothing
    emitted. ``engine`` is the EngineRuntime it lands through."""
    if not mv.incremental or mv.semantic_sql is None:
        return None
    if code_call(mv.semantic_sql, state.tracked_functions) is None:
        return None

    async def generate(
        pending: list[dict],
        *,
        prior_hash: str | None = None,
        ctx: Any = None,
        preprocess: Any = None,
        forced: bool = False,
    ) -> tuple[str, dict, str | None] | None:
        # A forced fire refreshes as any other does: the whole rebuilds are the rule's.
        del pending, prior_hash, ctx, preprocess, forced
        kept = await kept_by_change(state, mv)
        assert kept is not None  # its setting and its definition were judged above
        applied = await refresh_by_change(state, engine, mv, kept)
        if applied.whole is None and not applied.rows:
            return None
        return (
            "delta",
            {"rows": len(applied.rows), "delta": applied.rows, "landed": applied.landed},
            None,
        )

    return generate
