# Copyright (c) 2026 Kenneth Stott
# Canary: 3e7c1a94-5b2d-4f08-9a61-d84c2e70b513
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A reader's rules on a view applying a column transform (REQ-2131, amended 2026-11-16) or calling
a code command (REQ-2249): both are stored views, read only from their stored tables.

The view is built once and stored, never recomputed per reader, and no transform is called on a
read. So:

* The build keeps, beside the view's columns, every input column a role's row rule on one of the
  view's input tables reads (:func:`with_rule_inputs`), each under a name of its own
  (:func:`rule_input`). Those are columns of the stored table only: the view's registered columns
  are its definition's, so no reader, statement or catalog is offered them.
* A read (:func:`reader_rows`) filters the stored rows with the reader's row rule on each input,
  read over those kept columns, and masks each output whose lineage (:func:`output_lineage`)
  includes a column masked for the reader, with that column's mask. A reader with no row rule on
  an input and no mask on a column an output derives from reads the stored rows as they are.
* A code command's rows are computed from the whole of each table it is given, so each of its
  outputs derives from every column of those tables, and its rows carry no input row's values. A
  reader's rule on such a table is held through that table's registered relationship to the view
  (an EXISTS over the table under the rule, :func:`view_keys`), and an output that is the table's
  relationship key derives from that key column only (amended b0823d303a; REQ-2249: rules apply at
  each hop). A rule on a given table with no relationship to the view is refused by name.
* The kept columns a read filters over are those the stored table holds as built
  (:func:`stored_rule_inputs`), never those the rules name now: a row rule changed since the build
  is refused by name (:class:`ViewAwaitsRuleRebuild`) until the build that follows the change has
  published (:func:`rebuild_for_rules`), never read under the rules before it nor failed by the
  engine.
"""

# Requirements: REQ-2131, REQ-2249

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Mapping
from typing import TYPE_CHECKING, Any

import sqlglot
from sqlglot import exp

from provisa.compiler.view_expand import view_of, view_table
from provisa.core.read_refusal import ReadRefused

if TYPE_CHECKING:
    from provisa.compiler.stage2 import GovernanceContext
    from provisa.mv.models import MVDefinition

#: Where a kept input column is stored: the input's alias in the definition, then the column.
_RULE_INPUT = "__rule_input_{alias}__{column}"
RULE_INPUT_PREFIX = "__rule_input_"

#: An output's lineage: the (table id, column name as the definition reads it) it derives from.
Lineage = frozenset[tuple[int, str]]


def rule_input(alias: str, column: str) -> str:
    """The stored table's name for input ``column`` of the input read as ``alias``."""
    return _RULE_INPUT.format(alias=alias, column=column)


class ViewAwaitsRuleRebuild(ReadRefused):
    """A stored view read by a role whose row rule on one of its inputs reads an input column its
    stored rows do not hold yet: the rule changed since the view was built, and the build that
    follows the change has not published (REQ-2131)."""

    code = "data.view_awaits_rule_rebuild"

    def __init__(self, view: str) -> None:
        super().__init__(
            f"the view {view!r} is being built again for a change to the row rules on its "
            "inputs; this role can read it once that build has published"
        )
        self.params = {"view": view}


def _parse(sql: str) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    return sqlglot.parse_one(sql, read="postgres")


def _top_scope(node: exp.Expr) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """Whether ``node`` of a row rule is read against the rule's own table, not a subquery's."""
    return node.find_ancestor(exp.Query) is None


def rule_columns(rule: exp.Expr) -> set[str]:  # pyright: ignore[reportPrivateImportUsage]
    """The columns of its table a row rule reads: those outside any subquery of the rule."""
    return {c.name for c in rule.find_all(exp.Column) if _top_scope(c)}


def _projection(select: exp.Expr) -> bool:  # pyright: ignore[reportPrivateImportUsage]
    """Whether each row of ``select`` is one row of its inputs: a stored row then carries the
    input values a row rule reads. An aggregate, a DISTINCT or a set operation folds input rows
    together, so no input value belongs to its output row."""
    if not isinstance(select, exp.Select):
        return False
    if select.args.get("group") or select.args.get("having") or select.args.get("distinct"):
        return False
    return not any(e.find(exp.AggFunc) for e in select.expressions)


def _relations(select: exp.Select) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]
    origin = select.args.get("from_")
    found = [origin.this] if origin is not None else []
    return found + [j.this for j in select.args.get("joins") or []]


def rule_inputs(definition: str, table_map: Mapping[str, int]) -> list[tuple[str, int]]:
    """(alias, table id) of each registered table the definition's rows are rows of: a table its
    own FROM or a join reads, when it is a projection (:func:`_projection`). A table read anywhere
    else -- inside a derived table, a subquery, another view -- is none of these."""
    from provisa.compiler.naming import table_id_of  # noqa: PLC0415

    select = _parse(definition)
    if not _projection(select):
        return []
    assert isinstance(select, exp.Select)  # _projection admits only a SELECT
    found: list[tuple[str, int]] = []
    for relation in _relations(select):
        if not isinstance(relation, exp.Table):
            continue
        table_id = table_id_of(relation, table_map)
        if table_id is not None:
            found.append((relation.alias_or_name, table_id))
    return found


def roles_rules(state: Any) -> Callable[[int], list[exp.Expr]]:  # pyright: ignore[reportPrivateImportUsage]
    """Every role's row rule on a table, by its id: what a build keeps the inputs of.

    Read from the generation's stored rule rows (``role_build_inputs``), which name every role's
    rules model-wide, so no role's surface is built to collect them (REQ-1914). A stored role's
    rule on a table is its table rule (compiler.rls.build_rls_context); a meta-role's folds in a
    member's domain rule where the member has none on the table (security.meta_role._rls); every
    role holds a signals table to the reading org (compiler.rls.org_scoped_signals)."""
    from provisa.compiler.rls import (  # noqa: PLC0415
        ORG_SCOPED_SIGNALS_RULE,
        SIGNALS_SOURCE,
        build_rls_context,
    )

    inputs = state.role_build_inputs
    rows = inputs["rls_rules"]
    tables = {t["id"]: t for t in inputs["tables"]}
    contexts = [
        build_rls_context(rows, role_id) for role_id in sorted({r["role_id"] for r in rows})
    ]

    def of(table_id: int) -> list[exp.Expr]:  # pyright: ignore[reportPrivateImportUsage]
        table = tables[table_id]
        domain = table.get("domain_id") or ""
        found = [ctx.rules[table_id] for ctx in contexts if table_id in ctx.rules]
        found += [ctx.domain_rules[domain] for ctx in contexts if domain in ctx.domain_rules]
        if table["source_id"] == SIGNALS_SOURCE:
            found.append(ORG_SCOPED_SIGNALS_RULE)
        return found

    return of


def kept_inputs(
    definition: str,
    table_map: Mapping[str, int],
    rules_of: Callable[[int], Iterable[exp.Expr]],  # pyright: ignore[reportPrivateImportUsage]
) -> list[tuple[str, int, str]]:
    """(alias, table id, column) of each input column the build keeps: those any role's row rule
    on an input of the definition reads."""
    kept: list[tuple[str, int, str]] = []
    for alias, table_id in rule_inputs(definition, table_map):
        columns: set[str] = set()
        for rule in rules_of(table_id):
            columns |= rule_columns(rule)
        kept += [(alias, table_id, column) for column in sorted(columns)]
    return kept


def with_rule_inputs(definition: str, kept: Collection[tuple[str, int, str]]) -> str:
    """``definition`` with each of the ``kept`` input columns added to its select list under its
    :func:`rule_input` name: the statement a build lands."""
    if not kept:
        return definition
    select = _parse(definition)
    assert isinstance(select, exp.Select)  # kept only for a projection (rule_inputs)
    for alias, _, column in kept:
        select = select.select(
            exp.alias_(
                exp.column(column, table=alias, quoted=True), rule_input(alias, column), quoted=True
            ),
            copy=False,
        )
    # Function names as written: a column transform is matched by its registered name.
    return select.sql(dialect="postgres", normalize_functions=False)


# --- lineage --------------------------------------------------------------------------------------


class _Scope:
    """What each name a SELECT reads from stands for: a registered table (its id) or a derived
    relation (each of its outputs' lineage)."""

    def __init__(self) -> None:
        self.tables: dict[str, int] = {}
        self.derived: dict[str, dict[str, Lineage]] = {}

    def names(self) -> list[str]:
        return [*self.tables, *self.derived]


class _Lineage:
    def __init__(
        self,
        table_map: Mapping[str, int],
        columns_of: Callable[[int], list[str]],
        views: Mapping[str, str],
        transforms: Mapping[str, dict],
        commands: Mapping[str, dict],
    ) -> None:
        from provisa.mv.view_read import view_names  # noqa: PLC0415

        self.table_map = table_map
        self.columns_of = columns_of
        self.views = views
        # A semantic definition names a view by its domain as well as by its schema.
        self.view_names = view_names(views, table_map)
        self.transforms = {name.lower(): command for name, command in transforms.items()}
        self.commands = {name.lower(): command for name, command in commands.items()}

    def of_query(
        self, query: exp.Expr, ctes: Mapping[str, dict[str, Lineage]]
    ) -> dict[str, Lineage]:  # pyright: ignore[reportPrivateImportUsage]
        if isinstance(query, exp.Subquery):
            return self.of_query(query.this, ctes)
        if isinstance(query, exp.SetOperation):
            left = self.of_query(query.left, ctes)
            right = list(self.of_query(query.right, ctes).values())
            return {
                name: lineage | (right[i] if i < len(right) else frozenset())
                for i, (name, lineage) in enumerate(left.items())
            }
        if not isinstance(query, exp.Select):
            return {}
        local = dict(ctes)
        with_ = query.args.get("with_")
        for cte in with_.expressions if with_ is not None else []:
            local[cte.alias_or_name] = self.of_query(cte.this, local)
        scope = self._scope(query, local)
        out: dict[str, Lineage] = {}
        for item in query.expressions:
            out.update(self._item(item, scope, local))
        return out

    def _scope(self, select: exp.Select, ctes: Mapping[str, dict[str, Lineage]]) -> _Scope:
        from provisa.compiler.naming import table_id_of  # noqa: PLC0415

        scope = _Scope()
        for relation in _relations(select):
            alias = relation.alias_or_name
            if isinstance(relation, exp.Table):
                if (command := _command_of(relation, self.commands)) is not None:
                    scope.derived[alias] = self._command(relation, command)
                elif not relation.db and relation.name in ctes:
                    scope.derived[alias] = ctes[relation.name]
                elif (view := view_of(relation, self.views, self.view_names)) is not None:
                    scope.derived[alias] = self.of_query(_parse(self.views[view]), {})
                elif (table_id := table_id_of(relation, self.table_map)) is not None:
                    scope.tables[alias] = table_id
            elif isinstance(relation, exp.Subquery):
                scope.derived[alias] = self.of_query(relation.this, ctes)
        return scope

    def _command(self, call: exp.Table, command: dict) -> dict[str, Lineage]:
        """REQ-2249: a code command's rows are computed from the whole of each table it is
        given, so each of its output columns derives from every column of those tables."""
        from provisa.compiler.naming import table_id_of  # noqa: PLC0415
        from provisa.compiler.sql_validator import command_table_arguments  # noqa: PLC0415

        inputs: set[tuple[int, str]] = set()
        for table in command_table_arguments(call, command):
            if (view := view_of(table, self.views, self.view_names)) is not None:
                for lineage in self.of_query(_parse(self.views[view]), {}).values():
                    inputs |= lineage
            elif (table_id := table_id_of(table, self.table_map)) is not None:
                inputs |= {(table_id, column) for column in self.columns_of(table_id)}
        return {c["name"]: frozenset(inputs) for c in command.get("output_columns") or []}

    def _column(self, column: exp.Column, scope: _Scope) -> Lineage:
        holders = [column.table] if column.table else scope.names()
        found: set[tuple[int, str]] = set()
        for holder in holders:
            if holder in scope.tables:
                table_id = scope.tables[holder]
                if column.table or len(holders) == 1 or column.name in self.columns_of(table_id):
                    found.add((table_id, column.name))
            elif holder in scope.derived:
                found |= scope.derived[holder].get(column.name, frozenset())
        return frozenset(found)

    def _expression(
        self,
        node: exp.Expr,
        scope: _Scope,
        ctes: Mapping[str, dict[str, Lineage]],  # pyright: ignore[reportPrivateImportUsage]
    ) -> Lineage:
        found: set[tuple[int, str]] = set()
        for column in node.find_all(exp.Column):
            if column.find_ancestor(exp.Select) is node.find_ancestor(exp.Select):
                found |= self._column(column, scope)
        for nested in node.find_all(exp.Subquery):
            for lineage in self.of_query(nested.this, ctes).values():
                found |= lineage
        return frozenset(found)

    def _item(
        self,
        item: exp.Expr,
        scope: _Scope,
        ctes: Mapping[str, dict[str, Lineage]],  # pyright: ignore[reportPrivateImportUsage]
    ) -> dict[str, Lineage]:
        if isinstance(item, exp.Anonymous) and item.name.lower() in self.transforms:
            # REQ-2131: a transform's call stands for its output columns, each computed from all
            # of its inputs.
            inputs = self._expression(item, scope, ctes)
            command = self.transforms[item.name.lower()]
            return {c["name"]: inputs for c in command["output_columns"]}
        star = item if isinstance(item, exp.Star) else None
        if isinstance(item, exp.Column) and isinstance(item.this, exp.Star):
            star = item
        if star is not None:
            holders = [item.table] if isinstance(item, exp.Column) else scope.names()
            out: dict[str, Lineage] = {}
            for holder in holders:
                if holder in scope.tables:
                    table_id = scope.tables[holder]
                    out.update({c: frozenset({(table_id, c)}) for c in self.columns_of(table_id)})
                elif holder in scope.derived:
                    out.update(scope.derived[holder])
            return out
        return {item.alias_or_name: self._expression(item, scope, ctes)}


def _command_of(relation: exp.Table, commands: Mapping[str, dict]) -> dict | None:
    """The code command ``relation`` calls (REQ-2249), by ``commands``' lower-cased names."""
    target = relation.this
    if relation.db or not isinstance(target, exp.Anonymous):
        return None
    return commands.get(target.name.lower())


def output_lineage(
    definition: str,
    table_map: Mapping[str, int],
    columns_of: Callable[[int], list[str]],
    views: Mapping[str, str],
    transforms: Mapping[str, dict],
    commands: Mapping[str, dict],
) -> dict[str, Lineage]:
    """Each output column of ``definition`` with the registered columns it is computed from,
    through derived tables, common table expressions and the other ``views`` it reads; a column
    transform's outputs each from all of its inputs (REQ-2131); a code command's each from every
    column of the tables it is given (REQ-2249)."""
    lineage = _Lineage(table_map, columns_of, views, transforms, commands)
    return lineage.of_query(_parse(definition), {})


def tables_read(
    definition: str,
    table_map: Mapping[str, int],
    views: Mapping[str, str],
    commands: Mapping[str, dict],
) -> set[int]:
    """Every registered table ``definition`` reads, anywhere in it, through the other ``views``
    it reads and as the tables a code command it calls is given (REQ-2249)."""
    from provisa.compiler.naming import table_id_of  # noqa: PLC0415
    from provisa.compiler.sql_validator import command_table_arguments  # noqa: PLC0415

    from provisa.mv.view_read import view_names  # noqa: PLC0415

    lowered = {name.lower(): command for name, command in commands.items()}
    names = view_names(views, table_map)
    found: set[int] = set()
    pending, seen = [definition], set()
    while pending:
        tables = list(_parse(pending.pop()).find_all(exp.Table))
        while tables:
            table = tables.pop()
            if (command := _command_of(table, lowered)) is not None:
                tables += command_table_arguments(table, command)
            elif (view := view_of(table, views, names)) is not None:
                if view not in seen:
                    seen.add(view)
                    pending.append(views[view])
            elif (table_id := table_id_of(table, table_map)) is not None:
                found.add(table_id)
    return found


# --- the read -------------------------------------------------------------------------------------


def _reader_filter(
    rule: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    alias: str,
    table_id: int,
    held: str,
    gov: "GovernanceContext",
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """The reader's row ``rule`` on the input read as ``alias``, read over the stored table
    (under ``held``)'s kept columns of it."""
    from provisa.compiler.rls import cast_session_terms  # noqa: PLC0415

    pred = rule.copy()
    cast_session_terms(pred, {name: dtype for name, dtype in gov.all_columns.get(table_id, [])})
    for column in list(pred.find_all(exp.Column)):
        if _top_scope(column):
            column.replace(exp.column(rule_input(alias, column.name), table=held, quoted=True))
    return pred


def command_inputs(
    definition: str, table_map: Mapping[str, int], commands: Mapping[str, dict]
) -> dict[int, str]:
    """Each registered table a code command the definition calls is given (REQ-2249), by its id,
    with the name the definition gives it."""
    from provisa.compiler.naming import table_id_of  # noqa: PLC0415
    from provisa.compiler.sql_validator import command_table_arguments  # noqa: PLC0415

    lowered = {name.lower(): command for name, command in commands.items()}
    found: dict[int, str] = {}
    for call in _parse(definition).find_all(exp.Table):
        command = _command_of(call, lowered)
        if command is None:
            continue
        for table in command_table_arguments(call, command):
            table_id = table_id_of(table, table_map)
            if table_id is not None:
                found.setdefault(table_id, table.sql(dialect="postgres"))
    return found


def view_keys(
    view_table_id: int, given: Mapping[int, str], relationships: Iterable[dict]
) -> dict[int, tuple[str, str, str]]:
    """(name, its key column, the view's column) of each table in ``given`` that a registered
    relationship joins to the view on one column each, in either direction (a junction edge
    through the view is none of these)."""
    found: dict[int, tuple[str, str, str]] = {}
    for rel in relationships:
        if rel.get("via_table_id") is not None or rel.get("target_function_name"):
            continue
        ends = (rel["source_table_id"], rel["target_table_id"])
        if ends[1] == view_table_id and ends[0] in given:
            table_id, key, column = ends[0], rel["source_column"], rel["target_column"]
        elif ends[0] == view_table_id and ends[1] in given:
            table_id, key, column = ends[1], rel["target_column"], rel["source_column"]
        else:
            continue
        found.setdefault(table_id, (given[table_id], key, column))
    return found


def _related_filter(
    rule: exp.Expr,  # pyright: ignore[reportPrivateImportUsage]
    name: str,
    table_id: int,
    input_column: str,
    view_column: str,
    held: str,
    gov: "GovernanceContext",
) -> exp.Expr:  # pyright: ignore[reportPrivateImportUsage]
    """EXISTS a row of the table ``name`` under the reader's ``rule`` whose ``input_column`` is
    the stored row's ``view_column``: the reader's rule on that input, held through its
    relationship to the view."""
    from provisa.compiler.rls import cast_session_terms  # noqa: PLC0415

    inner = f"__rule_{table_id}"
    pred = rule.copy()
    cast_session_terms(pred, {n: dtype for n, dtype in gov.all_columns.get(table_id, [])})
    for column in list(pred.find_all(exp.Column)):
        if _top_scope(column):
            column.replace(exp.column(column.name, table=inner, quoted=True))
    joined = exp.EQ(
        this=exp.column(input_column, table=inner, quoted=True),
        expression=exp.column(view_column, table=held, quoted=True),
    )
    read = (
        exp.select(exp.Literal.number(1))
        .from_(exp.to_table(name, dialect="postgres").as_(inner))
        .where(exp.and_(joined, pred))
    )
    return exp.Exists(this=read)


def _table_name(table_id: int, gov: "GovernanceContext") -> str:
    """A name the reader's statements give table ``table_id``: its qualified one, else its bare."""
    names = sorted(n for n, tid in gov.table_map.items() if tid == table_id)
    qualified = [n for n in names if "." in n]
    return (qualified or names)[0]


def reader_rows(
    view: str,
    view_table_id: int,
    reference: exp.Table,
    definition: str,
    gov: "GovernanceContext",
    state: Any,
    views: Mapping[str, str],
    commands: Mapping[str, dict],
    stored: frozenset[str] | None,
) -> exp.Select:  # pyright: ignore[reportPrivateImportUsage]
    """The reader's read of stored view ``view``'s rows, through ``reference`` (the view's
    reference in the governed statement, which optimization answers from its stored table): the
    view's own columns, its row rules applied over the kept input columns and each output computed
    from a column masked for it masked with that column's mask. A reader whose rules narrow none
    of the view's inputs reads the stored rows as they are. The kept input columns are Provisa's
    own, offered to no reader: every read names the view's columns, so a star over the view
    never reaches them, whatever the reader may see (the build that kept them is governed as the
    administrator).

    ``stored`` is the kept input columns the stored table holds as built
    (:func:`stored_rule_inputs`; None when it is not built), read only when the reader has a row
    rule on an input (:func:`filters_rows`). A rule reading a column the rules keep but the
    stored table does not hold yet is refused by name until the build that keeps it publishes
    (:class:`ViewAwaitsRuleRebuild`).

    A row rule of the reader on a table a code command is given is held through that table's
    registered relationship to the view (:func:`view_keys`). Any other row rule on a table the
    stored rows carry no input values of -- one read inside a derived table or another view, given
    to a code command with no relationship to the view, or read by a view whose rows fold its
    inputs together -- is refused naming the table: those rows cannot be filtered by it."""
    from provisa.compiler.naming import apply_sql_name  # noqa: PLC0415
    from provisa.compiler.stage2 import registered_name  # noqa: PLC0415
    from provisa.security.masking import MaskType, mask_expression  # noqa: PLC0415

    held = reference.alias_or_name
    kept = kept_inputs(definition, gov.table_map, roles_rules(state))
    kept_set = {(alias, column) for alias, _, column in kept}
    filters: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    filtered: set[int] = set()
    for alias, table_id in rule_inputs(definition, gov.table_map):
        rule = gov.rls_rules.get(table_id)
        if rule is None:
            continue
        columns = sorted(rule_columns(rule))
        missing = [c for c in columns if (alias, c) not in kept_set]
        if missing:
            raise PermissionError(
                f"[V003] View {view!r} is stored without input {', '.join(missing)} that this "
                f"role's row rule on its input {alias!r} reads"
            )
        if stored is None or any(rule_input(alias, c) not in stored for c in columns):
            raise ViewAwaitsRuleRebuild(view)
        filters.append(_reader_filter(rule, alias, table_id, held, gov))
        filtered.add(table_id)
    given = command_inputs(definition, gov.table_map, commands)
    keys = view_keys(view_table_id, given, state.relationships)
    for table_id, (name, input_column, view_column) in sorted(keys.items()):
        # REQ-2131 (amended b0823d303a), REQ-2249: a bridge's rows are filtered by the reader's
        # rule on each table its command is given, through that table's relationship to the view.
        rule = gov.rls_rules.get(table_id)
        if rule is not None:
            filters.append(
                _related_filter(rule, name, table_id, input_column, view_column, held, gov)
            )
            filtered.add(table_id)
    for table_id in sorted(tables_read(definition, gov.table_map, views, commands) - filtered):
        if table_id in gov.rls_rules:
            raise PermissionError(
                f"[V003] This role's row rule on table {_table_name(table_id, gov)!r} cannot "
                f"filter view {view!r}: "
                "its stored rows carry no values of that table's columns"
            )

    def columns_of(table_id: int) -> list[str]:
        return [name for name, _ in gov.all_columns.get(table_id, [])]

    lineage = output_lineage(
        definition,
        gov.table_map,
        columns_of,
        views,
        getattr(state, "column_transforms", None) or {},
        commands,
    )
    for table_id, (_, input_column, view_column) in keys.items():
        # An output that is a given table's relationship key derives from that key column only.
        if (table_id, input_column) in lineage.get(view_column, frozenset()):
            lineage[view_column] = frozenset({(table_id, input_column)})
    items: list[exp.Expr] = []  # pyright: ignore[reportPrivateImportUsage]
    for physical, dtype in gov.all_columns.get(view_table_id, []):
        name = apply_sql_name(physical)
        value: exp.Expr = exp.column(name, table=held, quoted=True)  # pyright: ignore[reportPrivateImportUsage]
        derived_from = lineage.get(name, lineage.get(physical, frozenset()))
        for table_id, column in sorted(derived_from):
            entry = gov.masking_rules.get((table_id, registered_name(gov, table_id, column)))
            if entry is None:
                continue
            rule = entry[0]
            if rule.mask_type == MaskType.fake:
                raise PermissionError(
                    f"[V003] Column {name!r} of view {view!r} is computed from a column this "
                    "role reads faked, which its stored rows cannot fake"
                )
            value = mask_expression(rule, value, dtype)
            break
        items.append(exp.alias_(value, name, quoted=True))
    from provisa.compiler.rls import all_of  # noqa: PLC0415

    stored_table = reference.copy()
    stored_table.set("alias", exp.TableAlias(this=exp.to_identifier(held)))
    read = exp.select(*items).from_(stored_table)
    where = all_of(filters)
    return read if where is None else read.where(where)


def filters_rows(definition: str, gov: "GovernanceContext") -> bool:
    """Whether the reader has a row rule on an input whose values the stored rows of
    ``definition`` keep: the read filters over kept columns, so it reads what the stored table
    holds (:func:`stored_rule_inputs`)."""
    return any(table_id in gov.rls_rules for _, table_id in rule_inputs(definition, gov.table_map))


async def stored_rule_inputs(mv: "MVDefinition", state: Any) -> frozenset[str] | None:
    """The kept input columns ``mv``'s stored table holds, read from the table itself; None when
    it is not built. Held on ``mv`` for the build it read (its ``last_refresh_at``) under the rules
    of the model it read them for (``role_build_inputs``): a build published, or a rule changed,
    reads the table again."""
    key = (mv.last_refresh_at, id(state.role_build_inputs))
    held = mv.stored_rule_inputs
    if held is not None and held[0] == key:
        return held[1]
    from provisa.federation.execution_auth import SystemAuth, mint_system_token  # noqa: PLC0415
    from provisa.mv.refresh import _target_columns  # noqa: PLC0415

    columns = await _target_columns(
        state.federation_engine,
        mv,
        mv.stored_ref(),
        SystemAuth(mint_system_token(), reason=f"mv_rule_inputs:{mv.id}"),
    )
    if columns is None:
        return None
    kept = frozenset(c for c in columns if c.startswith(RULE_INPUT_PREFIX))
    mv.stored_rule_inputs = (key, kept)
    return kept


async def views_to_rebuild(state: Any) -> list["MVDefinition"]:
    """The stored views (REQ-2131, REQ-2249) built here whose stored tables hold other kept input
    columns than the row rules of the model now name: each must be built again. One not built yet
    is built with the rules in force by its first build."""
    from provisa.compiler.stage2 import context_table_map  # noqa: PLC0415
    from provisa.federation.replica_converge import builds_here  # noqa: PLC0415

    stored_reads = getattr(state, "stored_view_reads", None) or set()
    if not stored_reads:
        return []
    table_map = context_table_map(state.view_context)
    rules_of = roles_rules(state)
    found: list["MVDefinition"] = []
    for view in sorted(stored_reads):
        mv = state.mv_registry.get(f"view-{view_table(view)}")
        if mv is None or mv.semantic_sql is None:
            raise RuntimeError(f"stored view {view!r} has no definition registered")
        if not mv.enabled or not builds_here(mv.region):
            continue
        kept = kept_inputs(mv.semantic_sql, table_map, rules_of)
        needed = frozenset(rule_input(alias, column) for alias, _, column in kept)
        held = await stored_rule_inputs(mv, state)
        if held is not None and held != needed:
            found.append(mv)
    return found


async def rebuild_for_rules(state: Any) -> list[str]:
    """Start a build of each stored view whose kept input columns a change to the row rules
    changed (:func:`views_to_rebuild`), each on a background worker; the ids of those started.
    Until a build publishes, a reader whose rule reads a column it adds is refused by name
    (:class:`ViewAwaitsRuleRebuild`)."""
    from provisa.core.connection_loop import spawn_background  # noqa: PLC0415
    from provisa.mv.refresh import refresh_mv  # noqa: PLC0415

    started: list[str] = []
    for mv in await views_to_rebuild(state):
        spawn_background(
            refresh_mv(
                state.federation_engine,
                mv,
                state.mv_registry,
                store=state.model_db,
                ledger=state.tenant_db,
            ),
            name=f"rule-change build of {mv.id}",
        )
        started.append(mv.id)
    return started
