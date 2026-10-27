# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6c3f0e-8b7a-4f4e-9d1a-2f5b7e1c9a43
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Time travel as an argument of the relation that has versions (REQ-1163, amended 2026-10-25).

A statement says when it reads a bitemporal table or view where it names it, with the call form
SQL already has for a relation in ``FROM`` (docs/commands.md)::

    FROM "banking"."view_eod_totals"(as_recorded => '2026-10-02T17:00:00') AS e

``as_of`` is valid time and ``as_recorded`` recorded time (REQ-2109), validated as
:func:`provisa.mv.bitemporal.parse_as_of` / :func:`parse_as_recorded` validate them. An Iceberg
or Delta table travels through its snapshots (REQ-372): a snapshot is a commit time, so
``as_recorded`` selects it, and ``as_of`` is refused on it. Any other relation refuses both, by
name. A reference without arguments reads as the request's default (a pgwire session's ``SET
provisa.as_of``, a Flight ticket's options), else current state.

:func:`lift` runs before anything else reads the statement: each such reference becomes the plain
reference governance resolves, and its time is kept by the reference's alias, which governance
and every later rewrite keep (:class:`Travel`). The readers of versions -- bitemporal tables
(``bitemporal_tables.read_versions``), tables updated in place (``bitemporal_capture.read_logs``),
bitemporal views (``view_read``) and snapshots (:func:`read_snapshots`) -- ask it, per reference,
when to read.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

#: The arguments time travel takes, by the names GraphQL's fields take them.
ARGS = ("as_of", "as_recorded")


@dataclass(frozen=True)
class Travel:
    """When a statement reads each relation that keeps versions: ``by_alias`` (the canonical
    ``AsOf.key`` each reference with arguments said, by its alias, lower case), else ``default``
    (the request's; None: current state)."""

    default: str | None = None
    by_alias: Mapping[str, str] = field(default_factory=dict)

    def at(self, alias: str | None) -> str | None:
        """The time the reference ``alias`` is read at."""
        if alias is not None:
            said = self.by_alias.get(alias.lower())
            if said is not None:
                return said
        return self.default

    def travels(self) -> bool:
        """Whether any reference reads other than current state."""
        return self.default is not None or bool(self.by_alias)

    def single(self) -> str | None:
        """The one time the statement reads at, for its audit record's as_of and as_recorded:
        the request's when no reference says its own, else the one its references all say (and
        the request's, if it set one). None when it reads at several -- its text names each."""
        said = set(self.by_alias.values())
        if not said:
            return self.default
        if len(said) == 1 and self.default in (None, *said):
            return next(iter(said))
        return None

    def cache_sql(self, sql: str) -> str:
        """``sql`` as the response cache keys it: a governed statement keeps a bitemporal view's
        or a logged table's reference and is read at its time after, so the times its references
        said are part of what it answers (the request's own time is keyed beside it)."""
        if not self.by_alias:
            return sql
        said = ", ".join(f"{alias}={at}" for alias, at in sorted(self.by_alias.items()))
        return f"{sql} /* provisa travel: {said} */"


def plain(default: str | None) -> Travel:
    """A statement's travel when no reference carries arguments."""
    return Travel(default=default)


def _travel_call(node: Any) -> tuple[Any, dict[str, Any]] | None:
    """``(name, {argument: value})`` when ``node`` (a ``FROM`` relation) is a call carrying only
    named arguments, at least one of them a time-travel argument; else None (a plain reference, or
    a command's call)."""
    from sqlglot import exp  # noqa: PLC0415

    call = node.this
    if not isinstance(call, exp.Anonymous) or not call.expressions:
        return None
    if not all(isinstance(a, exp.Kwarg) for a in call.expressions):
        return None
    named = {a.this.name.lower(): a.expression for a in call.expressions}
    if not named.keys() & set(ARGS):
        return None
    # An unquoted name is held as text, a quoted one as an identifier.
    name = call.this if isinstance(call.this, exp.Identifier) else exp.to_identifier(call.this)
    return name, named


def _literal(name: str, relation: str, value: Any) -> str:
    from sqlglot import exp  # noqa: PLC0415

    if not isinstance(value, exp.Literal) or not value.is_string:
        raise ValueError(f"{relation}: {name} takes a quoted timestamp, not {value.sql()}")
    return value.this


def lift(
    sql: str,
    kind_of: Any,
    default: str | None = None,
    *,
    commands: frozenset[str] = frozenset(),
) -> tuple[str, Travel]:
    """``sql`` with each relation's time-travel arguments lifted off it, and the :class:`Travel`
    they say. ``kind_of(table)`` (a sqlglot ``Table`` naming the relation) is ``"bitemporal"``,
    ``"snapshot"`` (Iceberg/Delta), ``"plain"`` (any other relation) or None (not a relation the
    model has). A call naming one of ``commands`` is the command's and is left alone. A refusal is
    a ``ValueError`` naming the relation."""
    if "=>" not in sql:
        return sql, plain(default)
    import sqlglot  # noqa: PLC0415
    from sqlglot import exp  # noqa: PLC0415

    from provisa.mv.bitemporal import parse_as_of, parse_as_recorded, request_as_of  # noqa: PLC0415

    tree = sqlglot.parse_one(sql, read="postgres")
    by_alias: dict[str, str] = {}
    for node in list(tree.find_all(exp.Table)):
        found = _travel_call(node)
        if found is None:
            continue
        name, named = found
        if name.name in commands:
            continue
        reference = exp.Table(
            this=name.copy(),
            db=node.args.get("db"),
            catalog=node.args.get("catalog"),
            alias=node.args.get("alias"),
        )
        relation = ".".join(p for p in (reference.db, reference.name) if p)
        kind = kind_of(reference)
        if kind is None:
            raise ValueError(f"{relation} is not a table or view")
        extra = sorted(set(named) - set(ARGS))
        if extra:
            raise ValueError(f"{relation} takes no argument {extra[0]}: only as_of and as_recorded")
        if kind == "plain":
            raise ValueError(
                f"{relation} keeps no versions: as_of and as_recorded apply to bitemporal "
                "tables and views, and as_recorded to Iceberg and Delta tables"
            )
        as_of = _literal("as_of", relation, named["as_of"]) if "as_of" in named else None
        as_recorded = (
            _literal("as_recorded", relation, named["as_recorded"])
            if "as_recorded" in named
            else None
        )
        if kind == "snapshot" and as_of is not None:
            raise ValueError(
                f"{relation} maps no valid time: it travels by its snapshots, with as_recorded"
            )
        for value, parse, arg in (
            (as_of, parse_as_of, "as_of"),
            (as_recorded, parse_as_recorded, "as_recorded"),
        ):
            if value is None:
                continue
            try:
                parse(value)
            except ValueError as exc:
                raise ValueError(f"{relation}: invalid {arg}: {exc}") from exc
        alias = (reference.alias or reference.name).lower()
        at = request_as_of(as_of, as_recorded)
        if by_alias.get(alias, at) != at:
            raise ValueError(
                f"{relation}: two references at two times share the alias {alias}; "
                "give each its own"
            )
        if not reference.alias:
            reference.set("alias", exp.TableAlias(this=exp.to_identifier(reference.name)))
        by_alias[alias] = at  # type: ignore[assignment]
        node.replace(reference)
    if not by_alias:
        return sql, plain(default)
    return tree.sql(dialect="postgres"), Travel(default=default, by_alias=by_alias)


def as_travel(travel: Travel | str | None) -> Travel:
    """``travel`` as a :class:`Travel`: one time (``AsOf.key``) or none reads every reference at
    it."""
    return travel if isinstance(travel, Travel) else plain(travel)


def relation_kind(table: Any, state: Any, gov: Any, ctx: Any) -> str | None:
    """How the relation ``table`` (a sqlglot ``Table``) travels in time: ``"bitemporal"`` (a
    bitemporal view, or a table mapping business key, valid time and recorded time, REQ-2109),
    ``"snapshot"`` (an Iceberg or Delta table, REQ-372), ``"plain"`` (any other relation the
    model has), or None (none it has)."""
    from provisa.compiler.naming import table_id_of  # noqa: PLC0415
    from provisa.core.source_registry import TIME_TRAVEL_SOURCES  # noqa: PLC0415
    from provisa.mv.bitemporal_capture import captured_table_rows  # noqa: PLC0415
    from provisa.mv.bitemporal_tables import versioned_table_rows  # noqa: PLC0415

    if table.name in (getattr(state, "bitemporal_view_reads", None) or {}):
        return "bitemporal"
    tid = table_id_of(table, gov.table_map)
    if tid is None:
        return "plain" if table.name in (getattr(state, "view_sql_map", None) or {}) else None
    rows = getattr(state, "tables", None) or []
    if tid in versioned_table_rows(rows) or tid in captured_table_rows(rows):
        return "bitemporal"
    meta = next((m for m in ctx.tables.values() if m.table_id == tid), None)
    if meta is not None and meta.source_type in TIME_TRAVEL_SOURCES:
        return "snapshot"
    return "plain"


def lift_statement(
    sql: str, state: Any, gov_of: Any, ctx: Any, default: str | None
) -> tuple[str, Travel]:
    """:func:`lift` of a statement a role sends, its relations resolved in the governance context
    ``gov_of()`` gives (asked for only when a relation carries arguments) and ``ctx``, and the
    registered commands left to themselves."""
    commands = frozenset(
        {
            **(getattr(state, "tracked_functions", None) or {}),
            **(getattr(state, "tracked_webhooks", None) or {}),
        }
    )
    return lift(
        sql,
        lambda table: relation_kind(table, state, gov_of(), ctx),
        default,
        commands=commands,
    )


def read_snapshots(sql: str, ctx: Any, travel: Any) -> str:
    """``sql`` (governed, catalog-physical) with each reference to an Iceberg or Delta table that
    is read at a recorded time read from the snapshot current then (REQ-372: a snapshot is a
    commit time), with the engine's ``FOR TIMESTAMP AS OF``. Unchanged when nothing travels."""
    travel = as_travel(travel)
    if not travel.travels():
        return sql
    from provisa.core.source_registry import TIME_TRAVEL_SOURCES  # noqa: PLC0415

    lake = {
        (m.catalog_name.lower(), m.schema_name.lower(), m.table_name.lower())
        for m in ctx.tables.values()
        if m.source_type in TIME_TRAVEL_SOURCES
    }
    if not lake:
        return sql
    import sqlglot  # noqa: PLC0415
    from sqlglot import exp  # noqa: PLC0415

    from provisa.mv.bitemporal import AsOf  # noqa: PLC0415

    tree = sqlglot.parse_one(sql, read="postgres")
    changed = False
    for node in tree.find_all(exp.Table):
        if (node.catalog.lower(), node.db.lower(), node.name.lower()) not in lake:
            continue
        at = travel.at(node.alias or node.name)
        if at is None:
            continue
        recorded = AsOf.from_key(at).recorded_sql
        if recorded is None:
            continue  # a request's valid time alone: no snapshot is chosen by it
        node.set(
            "version",
            exp.Version(this="TIMESTAMP", kind="AS OF", expression=sqlglot.parse_one(recorded)),
        )
        changed = True
    return tree.sql(dialect="postgres") if changed else sql


def relation_kinds(state: Any, tables: list[dict]) -> dict[int, str]:
    """How each of the registered ``tables`` travels in time, by id: ``"bitemporal"`` or
    ``"snapshot"`` (as :func:`relation_kind` decides for a statement's reference); a relation that
    keeps no versions is absent. A GraphQL field takes the arguments its relation's kind does."""
    from provisa.core.source_registry import TIME_TRAVEL_SOURCES  # noqa: PLC0415
    from provisa.mv.bitemporal_capture import captured_table_rows  # noqa: PLC0415
    from provisa.mv.bitemporal_tables import versioned_table_rows  # noqa: PLC0415

    views = getattr(state, "bitemporal_view_reads", None) or {}
    rows = getattr(state, "tables", None) or []
    keeps = set(versioned_table_rows(rows)) | set(captured_table_rows(rows))
    source_types = getattr(state, "source_types", None) or {}
    kinds: dict[int, str] = {}
    for table in tables:
        if table["id"] in keeps or (table.get("view_sql") and table["table_name"] in views):
            kinds[table["id"]] = "bitemporal"
        elif source_types.get(table["source_id"]) in TIME_TRAVEL_SOURCES:
            kinds[table["id"]] = "snapshot"
    return kinds


def bind_travel_params(sql: str, params: list | None) -> tuple[str, list | None]:
    """``sql`` with each time-travel argument given as a ``$n`` placeholder written as the time
    ``params`` binds to it, and ``params`` without those values (the rest renumbered). A relation's
    time decides which rows the statement reads before it is governed -- a bitemporal table's
    versions are picked in the governed statement -- so it is part of the statement, not a value
    bound after; it is still only ever a time, validated by :func:`lift` as a written one is."""
    if not params or "=>" not in sql:
        return sql, params
    import sqlglot  # noqa: PLC0415
    from sqlglot import exp  # noqa: PLC0415

    tree = sqlglot.parse_one(sql, read="postgres")
    taken: set[int] = set()
    for kwarg in tree.find_all(exp.Kwarg):
        value = kwarg.expression
        if kwarg.this.name.lower() not in ARGS or not isinstance(value, exp.Parameter):
            continue
        n = int(value.this.this)
        if not 1 <= n <= len(params):
            raise ValueError(f"{kwarg.this.name} => ${n} names no bound value")
        bound = params[n - 1]
        if not isinstance(bound, str):
            raise ValueError(f"{kwarg.this.name} takes an ISO 8601 time, not {bound!r}")
        value.replace(exp.Literal.string(bound))
        taken.add(n)
    if not taken:
        return sql, params
    renumbered = {
        old: new
        for new, old in enumerate((n for n in range(1, len(params) + 1) if n not in taken), 1)
    }
    for param in tree.find_all(exp.Parameter):
        n = int(param.this.this)
        param.set("this", exp.Literal.number(renumbered[n]))
    rest = [v for i, v in enumerate(params, 1) if i not in taken]
    # The relation is still in its call form, a function to the parser: its name is kept as
    # written, or a quoted lower-case name would be written upper-case.
    return tree.sql(dialect="postgres", normalize_functions=False), rest or None
