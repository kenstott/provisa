# Copyright (c) 2026 Kenneth Stott
# Canary: 34abc1c2-e527-4132-8834-cb80584d5d2a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The polyfill grid (REQ-2236): one table, keyed by feature and connector dialect, of how a
PostgreSQL client gets PostgreSQL's behaviour on a connector Provisa sends SQL to that is not
PostgreSQL: a serving engine, or a source a statement goes to directly.

A feature has a read side (how its values and operations are computed on the connector) and a write
side (how its values reach it). Each side holds its rules and, per dialect, its limitations. A rule
is a pattern and, per dialect, a form: the dialect's SQL, a refusal, or a rewrite function. A
dialect a rule gives no form for does the feature natively; a dialect with no limitation has none.

The translator takes its forms from here (provisa.transpiler.function_macros reads :data:`GRID`),
and the documentation is generated from here (scripts/gen_polyfill_docs.py)."""

# Requirements: REQ-2236

from __future__ import annotations

import enum
from collections.abc import Mapping
from dataclasses import dataclass, field

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import notation
from provisa.transpiler.polyfill.matching import Bindings, match
from provisa.transpiler.polyfill.notation import Form, Refusal, Rewrite, RuleError

#: PostgreSQL's own dialect: the reference. Every feature is native there, and no rule runs.
NATIVE = frozenset({"postgres"})


@dataclass(frozen=True)
class Connector:
    """A dialect Provisa sends SQL to itself: a serving engine's, or a source's a statement goes
    to directly (REQ-863). Its forms are written in ``written_in``."""

    title: str
    written_in: str


#: Each column of the grid: a connector dialect Provisa sends SQL to (REQ-2236, EVERY CONNECTOR
#: PROVISA SENDS SQL TO), each with its own rows and its own page. They are the dialects of
#: provisa.transpiler.dialect_capabilities but Qdrant's, whose statements are lowered to its API and
#: run no SQL (SQL CONNECTORS ONLY). Presto's and Athena's forms are written in Trino's SQL.
DIALECTS: Mapping[str, Connector] = {
    "postgres": Connector("PostgreSQL", "postgres"),
    "duckdb": Connector("DuckDB", "duckdb"),
    "trino": Connector("Trino", "trino"),
    "presto": Connector("Presto", "trino"),
    "athena": Connector("Athena", "trino"),
    "clickhouse": Connector("ClickHouse", "clickhouse"),
    "snowflake": Connector("Snowflake", "snowflake"),
    "databricks": Connector("Databricks", "databricks"),
    "bigquery": Connector("BigQuery", "bigquery"),
    "tsql": Connector("T-SQL (SQL Server, Fabric, Synapse)", "tsql"),
    "redshift": Connector("Redshift", "redshift"),
    "mysql": Connector("MySQL", "mysql"),
    "singlestore": Connector("SingleStore", "singlestore"),
    "sqlite": Connector("SQLite", "sqlite"),
    "oracle": Connector("Oracle", "oracle"),
    "hive": Connector("Hive", "hive"),
    "exasol": Connector("Exasol", "exasol"),
    "druid": Connector("Druid", "druid"),
}


def column_of(dialect: str) -> str | None:
    """The column of the grid ``dialect`` is (its own), or None for a dialect Provisa sends no SQL
    to: a rule matched there has no form, and the translator refuses the call by name."""
    return dialect if dialect in DIALECTS else None


def written_in(dialect: str) -> str:
    """The dialect the forms of ``dialect``'s column are written in."""
    return DIALECTS[dialect].written_in


@dataclass(frozen=True)
class Limitation:
    """A difference from PostgreSQL on a dialect, and the refusal a client receives for it."""

    text: str
    refusal: Refusal | None = None


@dataclass(frozen=True)
class Rule:
    """A pattern in PostgreSQL's SQL with holes, and each dialect's form. ``pattern`` is None only for
    a rule whose every form is a rewrite function: the function does its own matching."""

    pattern: str | None
    forms: Mapping[str, Form]

    def matched(self, node: exp.Expr) -> Bindings | None:
        """What ``node`` binds to the pattern's holes; None where it does not match."""
        if self.pattern is None:
            return None
        return match(notation.pattern(self.pattern), node)

    def holes(self) -> tuple[str, ...]:
        return () if self.pattern is None else notation.holes(notation.pattern(self.pattern))


@dataclass(frozen=True)
class Side:
    """One side of a feature: its rules, and each dialect's limitations."""

    rules: tuple[Rule, ...] = ()
    limitations: Mapping[str, tuple[Limitation, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class Feature:
    """A row of the grid: a PostgreSQL feature, its family, and its read and write sides."""

    name: str
    family: str
    summary: str
    read: Side = Side()
    write: Side = Side()


class Status(enum.Enum):
    """A feature on a dialect, as the summary across the dialects shows it."""

    NATIVE = "native"
    POLYFILLED = "polyfilled"
    DIFFERS = "differs"
    REFUSED = "refused"


@dataclass(frozen=True)
class Applied:
    """A rule that matched a node: the feature, the rule, the dialect's form and the bindings."""

    feature: Feature
    rule: Rule
    form: Form | None
    bindings: Bindings
    #: What a rewrite function answered, for a rule that is one.
    rewritten: exp.Expr | None = None


class Grid:
    """The grid: its features, validated, and the lookups the translator and the docs make."""

    def __init__(self, features: tuple[Feature, ...]) -> None:
        self.features = features
        self._by_root: dict[str, list[tuple[Feature, Rule]]] = {}
        self._rewrites: list[tuple[Feature, Rule]] = []
        #: The names of the typed nodes a pattern's root is (array_length is sqlglot's ArraySize).
        self._typed_names: set[str] = set()
        names = [f.name for f in features]
        duplicated = {n for n in names if names.count(n) > 1}
        if duplicated:
            raise RuleError(f"features named twice: {sorted(duplicated)}")
        for feature in features:
            for side in (feature.read, feature.write):
                self._check(feature, side)
        for feature in features:
            for rule in feature.read.rules:
                if rule.pattern is None:
                    self._rewrites.append((feature, rule))
                else:
                    root = notation.pattern(rule.pattern)
                    self._by_root.setdefault(_root_key(root), []).append((feature, rule))
                    if not isinstance(root, exp.Anonymous):
                        self._typed_names.update(_called(root))

    @staticmethod
    def _check(feature: Feature, side: Side) -> None:
        for dialect in side.limitations:
            if dialect not in DIALECTS:
                raise RuleError(f"{feature.name}: no dialect {dialect!r}")
        for rule in side.rules:
            for dialect, form in rule.forms.items():
                if dialect not in DIALECTS:
                    raise RuleError(f"{feature.name}: no dialect {dialect!r}")
                if rule.pattern is None and not isinstance(form, Rewrite):
                    raise RuleError(f"{feature.name}: a rule with no pattern is a function name")
                if isinstance(form, str):
                    assert rule.pattern is not None
                    notation.replacement(form, written_in(dialect), rule.pattern)

    @property
    def names(self) -> frozenset[str]:
        """The names of the functions the read rules' patterns call."""
        return frozenset(
            name
            for feature in self.features
            for rule in feature.read.rules
            if rule.pattern is not None
            for name in _called(notation.pattern(rule.pattern))
        )

    @property
    def mentions(self) -> tuple[str, ...]:
        """What a statement a rule of no pattern may rewrite holds, where it names no function."""
        return tuple(
            sorted(
                {
                    form.mentions
                    for _, rule in self._rewrites
                    for form in rule.forms.values()
                    if isinstance(form, Rewrite) and form.mentions
                }
            )
        )

    def read(self, node: exp.Expr, dialect: str) -> Applied | None:
        """The read rule ``node`` matches, with ``dialect``'s form (None: native there, or no
        column of the grid), or None where no rule matches. Rules of one function are tried in
        the order the grid lists them."""
        column = column_of(dialect)
        for subject in (node, self._typed(node)):
            if subject is None:
                continue
            for feature, rule in self._by_root.get(_root_key(subject), []):
                bindings = rule.matched(subject)
                if bindings is not None:
                    form = None if column is None else rule.forms.get(column)
                    return Applied(feature, rule, form, bindings)
        if column is None:
            return None
        for feature, rule in self._rewrites:
            form = rule.forms.get(column)
            if not isinstance(form, Rewrite):
                continue
            rewritten = form.function(node, dialect)
            if rewritten is not None:
                return Applied(feature, rule, form, {}, rewritten)
        return None

    def _typed(self, node: exp.Expr) -> exp.Expr | None:
        """A call written by a name sqlglot leaves a plain call (quoted, or by pg_catalog), as the
        node sqlglot reads that name as unquoted, where a pattern's root is that node; None
        otherwise, and where sqlglot reads no such node of these arguments."""
        if not isinstance(node, exp.Anonymous) or node.name.lower() not in self._typed_names:
            return None
        # Copies: the statement's arguments stay its own.
        arguments = [argument.copy() for argument in node.expressions]
        try:
            typed = exp.func(node.name.lower(), *arguments, dialect=notation.PATTERN_DIALECT)
        except ValueError:  # sqlglot's node of that name takes no such arguments
            return None
        return None if isinstance(typed, exp.Anonymous) else typed

    def status(self, feature: Feature, dialect: str) -> Status:
        """What the summary shows of ``feature`` on ``dialect``."""
        forms = [
            rule.forms.get(dialect)
            for side in (feature.read, feature.write)
            for rule in side.rules
            if dialect in rule.forms
        ]
        limited = any(side.limitations.get(dialect) for side in (feature.read, feature.write))
        if forms and all(isinstance(form, Refusal) for form in forms):
            return Status.REFUSED
        if limited or any(isinstance(form, Refusal) for form in forms):
            return Status.DIFFERS
        return Status.POLYFILLED if forms else Status.NATIVE

    def on(self, dialect: str) -> tuple[Feature, ...]:
        """The features with a polyfill or a limitation on ``dialect``: what its page lists."""
        return tuple(f for f in self.features if self.status(f, dialect) is not Status.NATIVE)


def _called(node: exp.Expr) -> tuple[str, ...]:
    """The names a pattern's root call is written by."""
    if isinstance(node, exp.Anonymous):
        return (node.name.lower(),)
    if isinstance(node, exp.Func):
        return tuple(name.lower() for name in type(node).sql_names())
    return ()


def _root_key(node: exp.Expr) -> str:
    """What a pattern's root and a statement's node are indexed by: a call's name."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower()
    return type(node).__name__
