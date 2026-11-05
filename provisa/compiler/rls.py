# Copyright (c) 2026 Kenneth Stott
# Canary: 98081871-58f3-442f-911a-b2a85cbdf709
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Per-role row filters (RLS, REQ-040, REQ-041) and the predicate helpers that qualify them.

RLS filter expressions are stored per (table_id, role_id) in the config DB; the governed
pipeline (compiler/stage2) ANDs each table's filter into every SELECT that reads it.
"""

# Requirements: REQ-038, REQ-039, REQ-040, REQ-041, REQ-402, REQ-403

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import exp

#: A row rule: a predicate over one relation's columns, carried as an expression (REQ-913).
Rule = exp.Expr  # pyright: ignore[reportPrivateImportUsage]  # sqlglot omits __all__


def read_rule(text: str) -> Rule:  # REQ-913
    """A row rule as written -- by an administrator, a stored row, a config file -- read once,
    where it enters the pipeline; from there on it is carried as this expression."""
    # REQ-913: the one text step a row rule takes, reading what someone else wrote.
    return sqlglot.parse_one(text, read="postgres")


def as_rule(rule: str | Rule) -> Rule:
    """``rule`` as an expression: a rule entering as text is read (:func:`read_rule`)."""
    return read_rule(rule) if isinstance(rule, str) else rule


def read_rule_row(row: dict) -> dict:
    """A copy of a stored rule row (``filter_expr`` and its target) with its rule an expression:
    a row read from the store is where a rule enters (:func:`as_rule`)."""
    return {**row, "filter_expr": as_rule(row["filter_expr"])}


def every_row(rule: Rule | None) -> bool:
    """Whether ``rule`` is ``TRUE``: it admits every row."""
    return isinstance(rule, exp.Boolean) and rule.this is True


def _folded(rules: Iterable[Rule | None], connective: type[exp.Connector]) -> Rule | None:
    terms = list(dict.fromkeys(r for r in rules if r is not None))
    if not terms:
        return None
    if len(terms) == 1:
        return terms[0]
    folded: Rule = exp.paren(terms[0].copy())
    for term in terms[1:]:
        folded = connective(this=folded, expression=exp.paren(term.copy()))
    return folded


def all_of(rules: Iterable[Rule | None]) -> Rule | None:
    """The AND of ``rules``, each once and parenthesised; None stands for no rule and contributes
    nothing; one rule is returned as it is; None when there is none."""
    return _folded(rules, exp.And)


def any_of(rules: Iterable[Rule | None]) -> Rule | None:
    """The OR of ``rules``, as :func:`all_of` ANDs them."""
    return _folded(rules, exp.Or)


class RuleMap(dict):
    """Row rules by key (a table id, a domain id, an action name), each an expression. A rule
    stored into it as text is a rule entering the pipeline, read there once (:func:`as_rule`)."""

    def __init__(self, items: Any = (), /, **named: Any) -> None:
        super().__init__()
        self.update(items, **named)

    def __setitem__(self, key: Any, rule: str | Rule) -> None:
        super().__setitem__(key, as_rule(rule))

    def update(self, items: Any = (), /, **named: Any) -> None:  # pyright: ignore[reportIncompatibleMethodOverride]
        for key, rule in dict(items, **named).items():
            self[key] = rule

    def setdefault(self, key: Any, rule: Any = None) -> Any:  # pyright: ignore[reportIncompatibleMethodOverride]
        if key not in self:
            self[key] = rule
        return self[key]

    def copy(self) -> RuleMap:
        return RuleMap(self)


def read_grant(grant: dict) -> dict:  # REQ-913
    """A copy of a write grant with its rows and check -- and each holder's, a meta-role's --
    as expressions (None stays None: every row)."""

    def predicates(g: dict) -> dict:
        return {
            **g,
            **{k: as_rule(g[k]) for k in ("rows", "check") if g.get(k) is not None},
        }

    read = predicates(grant)
    if grant.get("holders"):
        read["holders"] = [predicates(h) for h in grant["holders"]]
    return read


class GrantMap(dict):
    """A role's write grants: table id -> operation -> grant, each grant's predicates
    expressions. A grant stored into it with text predicates is read there once (:func:`read_grant`)."""

    def __init__(self, items: Any = (), /) -> None:
        super().__init__()
        for key, grants in dict(items).items():
            self[key] = grants

    def __setitem__(self, key: Any, grants: dict) -> None:
        super().__setitem__(key, {op: read_grant(g) for op, g in grants.items()})


def rule_maps(by_operation: Any) -> dict[str, RuleMap]:
    """Write rules -- operation -> key -> rule -- with each operation's rules a :class:`RuleMap`."""
    return {operation: RuleMap(rules) for operation, rules in dict(by_operation or {}).items()}


#: A member's treatment of a column, ordered least restrictive first: read as it is (0), through
#: entry i of the column's mask list (1 + i), the complete mask of a reader holding no entry
#: (1 + the list's length), or not served (HIDDEN).
PLAIN = 0
HIDDEN = 1 << 30


@dataclass(frozen=True)
class MemberReads:  # REQ-1620
    """How the members of a meta-role read one table. ``flags``: each member's own resolved row
    filter, its constants inlined (``TRUE`` where it reads every row). ``columns``: per column,
    each member's treatment, in member order: (rank, mask rule or None, data type), rank as
    :data:`PLAIN` / ``1 + mask entry index`` / :data:`HIDDEN`."""

    flags: tuple[Rule, ...]
    columns: dict[str, tuple[tuple[int, Any, str], ...]]

    def __post_init__(self) -> None:
        # A flag given as text is a rule entering (REQ-913): read once, here.
        object.__setattr__(self, "flags", tuple(as_rule(f) for f in self.flags))

    def uniform(self, column: str) -> bool:
        return len({rank for rank, _, _ in self.columns[column]}) == 1

    def resolved(self, column: str) -> tuple[int, Any, str] | None:
        """The one treatment every row reads ``column`` through, or None when it depends on the
        row. A row reads a column through the least restrictive treatment of the members that
        admit it, so a column every member treats alike, or whose least restrictive treatment is
        held by a member that admits every row (its flag is TRUE), reads that treatment on every
        row: nothing about the row needs evaluating."""
        treatments = self.columns[column]
        least = min(rank for rank, _, _ in treatments)
        group = [i for i, t in enumerate(treatments) if t[0] == least]
        if self.uniform(column) or any(every_row(self.flags[i]) for i in group):
            return treatments[group[0]]
        return None


@dataclass
class RLSContext:
    """RLS rules for a specific role, keyed by table_id or domain_id."""

    # table_id → row rule (REQ-913: an expression; a rule given as text is read where it enters)
    rules: dict[int, Rule]
    # domain_id → row rule (applies to all tables in that domain)
    domain_rules: dict[str, Rule] = field(default_factory=dict)
    # REQ-1679: action name → row rule over the action's response contract
    action_rules: dict[str, Rule] = field(default_factory=dict)
    # REQ-1620: a meta-role's members, per table they read (MemberReads): each row is read as the
    # member(s) whose filter admitted it. Empty for a single role.
    member_reads: dict[int, MemberReads] = field(default_factory=dict)
    # REQ-2188: the role's U and D rules, resolved to one predicate per table: "update" or "delete"
    # -> table_id -> predicate. Reads never consult them. A write ANDs them in beside its grant.
    write_rules: dict[str, dict[int, Rule]] = field(default_factory=dict)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in ("rules", "domain_rules", "action_rules"):
            value = RuleMap(value)
        elif name == "write_rules":
            value = rule_maps(value)
        super().__setattr__(name, value)

    @staticmethod
    def empty() -> RLSContext:
        return RLSContext(rules={}, domain_rules={}, action_rules={})

    def has_rules(self) -> bool:
        return bool(self.rules) or bool(self.domain_rules) or bool(self.action_rules)


#: The source of the ops signals (traces, metrics, logs): one store every org's rows land in, each
#: row's org kept as ``tenant_id``.
SIGNALS_SOURCE = "provisa-otel"
#: A reader of the signals reads its own org's rows; a row of no org (a span outside any request)
#: is no org's, and ``tenant_id = NULL`` matches it for no reader.
ORG_SCOPED_SIGNALS_RULE = exp.EQ(
    this=exp.column("tenant_id"),
    expression=exp.Anonymous(
        this="current_setting", expressions=[exp.Literal.string("provisa.org")]
    ),
)


def org_scoped_signals(rls: RLSContext, tables: list[dict]) -> RLSContext:  # REQ-1349
    """``rls`` with every signals table held to the reading org, beside any rule of the role's
    own on it (both hold). Applied to every role: no role reads another org's signals."""
    rules = RuleMap(rls.rules)
    for table in tables:
        if table["source_id"] != SIGNALS_SOURCE:
            continue
        held = all_of([ORG_SCOPED_SIGNALS_RULE, rules.get(table["id"])])
        assert held is not None  # the org's rule is one of them
        rules[table["id"]] = held
    return RLSContext(
        rules=rules,
        domain_rules=dict(rls.domain_rules),
        action_rules=dict(rls.action_rules),
        member_reads=dict(rls.member_reads),
        write_rules=dict(rls.write_rules),
    )


#: REQ-2188: the write a U or D rule narrows.
_OPERATION_OF_RIGHT = {"U": "update", "D": "delete"}


def build_rls_context(rls_rules: list[dict], role_id: str) -> RLSContext:  # REQ-041, REQ-402
    """Build an RLSContext from DB rows for a specific role.

    Args:
        rls_rules: list of dicts with {table_id, domain_id, role_id, filter_expr, right}. A row
            with no right is an R rule, as a stored rule defaults to.
        role_id: the role to filter for.
    """
    rules = RuleMap()
    domain_rules = RuleMap()
    action_rules = RuleMap()
    write_rules: dict[str, dict[int, Rule]] = {}
    for rule in rls_rules:
        if rule["role_id"] != role_id:
            continue
        right = rule.get("right", "R")
        if right != "R":  # REQ-2188: folded per table by resolve_table_rules; never a read's
            if rule.get("table_id") is not None:
                write_rules.setdefault(_OPERATION_OF_RIGHT[right], RuleMap())[rule["table_id"]] = (
                    rule["filter_expr"]
                )
            continue
        if rule.get("action_name"):  # REQ-1679
            action_rules[rule["action_name"]] = rule["filter_expr"]
        elif rule.get("domain_id"):
            domain_rules[rule["domain_id"]] = rule["filter_expr"]
        elif rule.get("table_id") is not None:
            rules[rule["table_id"]] = rule["filter_expr"]
    return RLSContext(
        rules=rules, domain_rules=domain_rules, action_rules=action_rules, write_rules=write_rules
    )


def _qualified_predicate(rule: Rule, alias: str | None) -> Rule:
    """A copy of ``rule`` with every unqualified column qualified with ``alias`` (AST).

    Structural, not textual: string literals and keywords are their own node types, so only
    genuine Column nodes are touched — no regex, no keyword denylist. Already-qualified columns
    keep their table. Column identifiers are rendered quoted to match the compiler convention.
    A column inside a subquery of the rule is the subquery's to resolve: qualified to ``alias``,
    ``region IN (SELECT region FROM allowed)`` would compare each row's region with itself.
    """
    pred = rule.copy()
    if alias:
        for col in pred.find_all(exp.Column):
            if not col.table and col.find_ancestor(exp.Query) is None:
                col.set("table", exp.to_identifier(alias, quoted=True))
            if isinstance(col.this, exp.Identifier):
                col.this.set("quoted", True)
    return pred


def qualified_filter(
    rule: Rule, alias: str, column_types: dict[str, str] | None = None
) -> Rule:  # REQ-913
    """A copy of ``rule`` -- an administrator's rule, read once where it entered -- with its bare
    columns qualified by ``alias``.

    ``column_types`` (column name → IR type) casts every session-variable term compared with a
    typed column to that column's type (REQ-1686).
    """
    pred = _qualified_predicate(rule, alias)
    if column_types:
        cast_session_terms(pred, column_types)
    return pred


def _qualify_filter(
    rule: str | Rule, alias: str, column_types: dict[str, str] | None = None
) -> str:
    """:func:`qualified_filter` rendered as SQL text, for display."""
    return qualified_filter(as_rule(rule), alias, column_types).sql(dialect="postgres")


def _is_session_term(node: Rule) -> bool:
    return isinstance(node, exp.Anonymous) and node.name.lower() == "current_setting"


def cast_session_terms(pred: Rule, column_types: dict[str, str]) -> None:  # REQ-1686
    """Cast ``current_setting('provisa.<var>')`` to the type of the column it is compared with.

    A session variable is text (Postgres ``current_setting`` returns text; the pipeline's literal
    substitution quotes it), so ``id = current_setting(...)`` on an integer column fails with
    "operator does not exist: integer = text". Hasura casts a session variable to the column's
    type; so does this, on the AST, for every comparison whose other side is a column with a
    known type. A variable compared with an untyped column, or used outside a comparison, is left
    as it is. Mutates ``pred`` in place.
    """
    for node in list(pred.find_all(exp.Binary)):
        left, right = node.this, node.expression
        for col, term in ((left, right), (right, left)):
            if not isinstance(col, exp.Column) or not _is_session_term(term):
                continue
            col_type = column_types.get(col.name)
            if not col_type:
                continue
            term.replace(exp.Cast(this=term.copy(), to=exp.DataType.build(col_type)))
    for node in list(pred.find_all(exp.In)):
        col = node.this
        if not isinstance(col, exp.Column):
            continue
        col_type = column_types.get(col.name)
        if not col_type:
            continue
        for term in list(node.expressions):
            if _is_session_term(term):
                term.replace(exp.Cast(this=term.copy(), to=exp.DataType.build(col_type)))


def _inject_where(sql: str, rls_clause: str | Rule) -> str:
    """AND ``rls_clause`` into a query's WHERE (AST): parse, attach the predicate to the outer
    SELECT — sqlglot places it before ORDER BY/LIMIT and merges an existing WHERE — reserialize.
    A clause given as text is a rule entering, read once (:func:`as_rule`)."""
    # REQ-913: ``sql`` is the statement a previous stage handed on as text; the stages exchange
    # SQL text, so this stage reads it once here.
    tree = sqlglot.parse_one(sql, read="postgres")
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if select is None:
        raise ValueError("_inject_where: query has no SELECT to attach the predicate to")
    select.where(exp.paren(as_rule(rls_clause)), copy=False)
    return tree.sql(dialect="postgres")
