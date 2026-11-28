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

# The dialects of the Db2 and SAP HANA columns, registered with sqlglot by their definition: the
# columns' forms are read in them. Neither module imports the transpiler where it is defined.
import provisa.db2.dialect  # noqa: F401
import provisa.saphana.dialect  # noqa: F401
from provisa.core.engine_kinds import ENGINE_KINDS
from provisa.core.source_registry import LAKE_ONLY_SOURCES, SOURCE_TO_DIALECT, engine_dialect
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
    "db2": Connector("Db2", "db2"),
    "saphana": Connector("SAP HANA", "saphana"),
}


#: The source types a hint may read: those that run SQL (each has a dialect) and the lakes an
#: engine attaches (REQ-2236, SQL CONNECTORS ONLY).
_ATTACHED_SOURCES = frozenset(SOURCE_TO_DIALECT) | frozenset(LAKE_ONLY_SOURCES)


def column_of(dialect: str) -> str | None:
    """The column of the grid ``dialect`` is (its own), or None for a dialect Provisa sends no SQL
    to: a rule matched there has no form, and the translator refuses the call by name."""
    return dialect if dialect in DIALECTS else None


def written_in(dialect: str) -> str:
    """The dialect the forms of ``dialect``'s column are written in."""
    return DIALECTS[dialect].written_in


#: The bound of a last-digits difference (REQ-2236, amended 2026-11-25): a float4 or float8 result an
#: engine computes with its own math library differs from PostgreSQL's by at most this part of
#: PostgreSQL's value. The widest measured is a ClickHouse server's ln(100), 2.1e-10 of it
#: (4.6051701869546955, PostgreSQL 4.605170185988092); exp(0.5) there is 8.3e-13. Anything a single
#: precision step or a wrong formula makes (1e-7 and more) is beyond it. A float4 has no difference
#: this small but none, so on a float4 the bound admits none.
LAST_DIGITS_BOUND = 1e-9


@dataclass(frozen=True)
class Limitation:
    """A difference from PostgreSQL on a dialect, and the refusal a client receives for it.
    ``last_digits``: the difference is a float result's last significant digits, within
    :data:`LAST_DIGITS_BOUND`, and nothing else (``last_digits``)."""

    text: str
    refusal: Refusal | None = None
    last_digits: bool = False


def last_digits(engine: str, computed: str) -> Limitation:
    """The limitation of a row whose float result ``engine`` computes with its own math library
    (REQ-2236, amended 2026-11-25): a documented difference within :data:`LAST_DIGITS_BOUND`.
    ``computed``: what it computes so, as the row's text says it."""
    return Limitation(
        f"{engine} computes {computed} with its own math library: a float result can differ from "
        f"PostgreSQL's in its last significant digits, by at most {LAST_DIGITS_BOUND:g} of "
        "PostgreSQL's value.",
        last_digits=True,
    )


@dataclass(frozen=True)
class Rule:
    """A pattern in PostgreSQL's SQL with holes, and each dialect's form. ``pattern`` is None only for
    a rule whose every form is a rewrite function: the function does its own matching."""

    pattern: str | None
    forms: Mapping[str, Form]
    #: Whether a dialect that is no column of the grid keeps what it had (the engine's own, or the
    #: old table's form) where the pattern matches: a row that polyfills a function only where an
    #: engine differs (array_agg on ClickHouse) refuses no call elsewhere. False: such a dialect
    #: has no form of the call, and it is refused by name.
    unlisted_keep: bool = False

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


@dataclass(frozen=True)
class Hint:
    """A hint (REQ-2236, A HINT IS A REWRITE FOR AN ENGINE AND SOURCE PAIR; REQ-2352): a row of the
    grid for a difference from PostgreSQL that one engine shows, on its own tables or reading one
    source it attaches, which its dialect's rows do not, and its fix: a rule in the grid's notation
    that replaces the dialect's rule there. ``dialect``: the column whose page lists it. ``engine``:
    the engine kind (provisa.core.engine_kinds) whose dialect that is. ``source``: the source type
    whose attached columns it applies to (a source hint), or None for the engine's own tables (an
    engine hint). ``setting``: the source's version or setting it depends on, where it does.
    ``features``: the grid rows it refines. ``test``: the test that reproduced the difference and
    checks the answer, from the repository's root; ``service``: the docker-compose.test.yml service
    that test runs the engine and source in, read for the hint's provenance."""

    name: str
    dialect: str
    engine: str
    source: str | None
    difference: str
    rule: Rule
    features: tuple[str, ...]
    test: str
    service: str
    setting: str | None = None

    @property
    def form(self) -> Form:
        """The hint's form on its dialect."""
        return self.rule.forms[self.dialect]


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


def _first(rule: Rule) -> bool:
    return any(isinstance(form, Rewrite) and form.first for form in rule.forms.values())


class Grid:
    """The grid: its features, validated, and the lookups the translator and the docs make."""

    def __init__(self, features: tuple[Feature, ...], hints: tuple[Hint, ...] = ()) -> None:
        self.features = features
        self.hints = hints
        self._by_root: dict[str, list[tuple[Feature, Rule]]] = {}
        self._rewrites: list[tuple[Feature, Rule]] = []
        #: The rules of no pattern read before any pattern (Rewrite.first).
        self._first: list[tuple[Feature, Rule]] = []
        #: The names of the typed nodes a pattern's root is (array_length is sqlglot's ArraySize).
        self._typed_names: set[str] = set()
        names = [f.name for f in features]
        duplicated = {n for n in names if names.count(n) > 1}
        if duplicated:
            raise RuleError(f"features named twice: {sorted(duplicated)}")
        for feature in features:
            for side in (feature.read, feature.write):
                self._check(feature, side)
        self._check_hints(frozenset(names))
        for feature in features:
            for rule in feature.read.rules:
                if rule.pattern is None and _first(rule):
                    self._first.append((feature, rule))
                elif rule.pattern is None:
                    self._rewrites.append((feature, rule))
                else:
                    root = notation.pattern(rule.pattern)
                    self._by_root.setdefault(_root_key(root), []).append((feature, rule))
                    if not isinstance(root, exp.Anonymous):
                        self._typed_names.update(_called(root))
                        # The name the pattern writes, where sqlglot reads it as a node of another
                        # name (log10 as log of base 10): a quoted call of it is that node too.
                        self._typed_names.add(_written_name(rule.pattern))
        # Each column's rewrite functions, in the grid's order, found once: the walk reads them at
        # every node of every statement, and most rules have no rewrite on a given column.
        self._rewrites_on = {c: _rewrites_of(self._rewrites, c) for c in DIALECTS}
        self._first_on = {c: _rewrites_of(self._first, c) for c in DIALECTS}
        #: Each column's first rules less those of the features skipped, by the ids of those
        #: features, the features kept with them (so no id is reused while it is a key).
        self._first_kept: dict[tuple[str, tuple[int, ...]], _Kept] = {}

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

    def _check_hints(self, features: frozenset[str]) -> None:
        """Each hint is of a dialect of the grid and an engine kind that writes it, reads a source
        type that runs SQL or a lake an engine attaches, refines rows the grid has, and has one form,
        its dialect's (REQ-2236, A HINT IS A REWRITE FOR AN ENGINE AND SOURCE PAIR)."""
        keys = [(h.dialect, h.engine, h.source, h.setting, h.name) for h in self.hints]
        twice = sorted({k[-1] for k in keys if keys.count(k) > 1})
        if twice:
            raise RuleError(f"hints named twice on one engine and source: {twice}")
        for hint in self.hints:
            if hint.dialect not in DIALECTS:
                raise RuleError(f"hint {hint.name}: no dialect {hint.dialect!r}")
            if hint.engine not in ENGINE_KINDS:
                raise RuleError(f"hint {hint.name}: no engine kind {hint.engine!r}")
            if engine_dialect(hint.engine) != hint.dialect:
                raise RuleError(
                    f"hint {hint.name}: engine {hint.engine!r} does not write {hint.dialect!r}"
                )
            if hint.source is not None and hint.source not in _ATTACHED_SOURCES:
                raise RuleError(f"hint {hint.name}: no SQL or lake source type {hint.source!r}")
            if set(hint.rule.forms) != {hint.dialect}:
                raise RuleError(f"hint {hint.name}: its one form is of {hint.dialect!r}")
            if not hint.features:
                raise RuleError(f"hint {hint.name}: names the rows it refines")
            unknown = sorted(set(hint.features) - features)
            if unknown:
                raise RuleError(f"hint {hint.name}: no rows {unknown}")
            if not hint.test or not hint.service:
                raise RuleError(f"hint {hint.name}: names its reproducing test and its service")
            self._check(Feature(hint.name, "hint", hint.difference), Side(rules=(hint.rule,)))

    def hints_on(self, dialect: str) -> tuple[Hint, ...]:
        """The hints ``dialect``'s page lists, by engine, then source (its own tables first)."""
        return tuple(
            sorted(
                (h for h in self.hints if h.dialect == dialect),
                key=lambda h: (h.engine, h.source or "", h.setting or "", h.name),
            )
        )

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

    def names_on(self, dialect: str) -> frozenset[str]:
        """The names of the functions the read rules' patterns call that a rule writes on
        ``dialect``: a form of its column, or, on a dialect with no column, every rule but one that
        keeps the dialect's own (Rule.unlisted_keep). A rule with no form of a dialect's column
        leaves the engine's own call there."""
        column = column_of(dialect)
        return frozenset(
            name
            for feature in self.features
            for rule in feature.read.rules
            if rule.pattern is not None
            and (
                rule.forms.get(column) is not None if column is not None else not rule.unlisted_keep
            )
            for name in _called(notation.pattern(rule.pattern))
        )

    def mentions_on(self, dialect: str) -> tuple[str, ...]:
        """What a statement a rule of no pattern may rewrite on ``dialect`` holds, where it names no
        function: the mentions of the rewrites in the dialect's column, or of every rewrite on a
        dialect with no column. A rewrite of another column leaves the statement as it is here."""
        column = column_of(dialect)
        return tuple(
            sorted(
                {
                    form.mentions
                    for _, rule in (*self._first, *self._rewrites)
                    for of, form in rule.forms.items()
                    if isinstance(form, Rewrite)
                    and form.mentions
                    and (column is None or of == column)
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
        for feature, rule, form in self._rewrites_on[column]:
            rewritten = form.function(node, dialect)
            if rewritten is not None:
                return Applied(feature, rule, form, {}, rewritten)
        return None

    def read_first(
        self, node: exp.Expr, dialect: str, skipped: tuple[Feature, ...]
    ) -> Applied | None:
        """The rule over every call (Rewrite.first) that rewrites ``node`` on ``dialect``, before
        any pattern is read; None where none does. A rule of a feature in ``skipped`` is not read
        (the statement's origin rules it out: function_macros.inline's ``generated``)."""
        column = column_of(dialect)
        if column is None:
            return None
        for feature, rule, form in self._first_not_skipped(column, skipped):
            rewritten = form.function(node, dialect)
            if rewritten is not None:
                return Applied(feature, rule, form, {}, rewritten)
        return None

    def _first_not_skipped(self, column: str, skipped: tuple[Feature, ...]) -> _Found:
        """``column``'s first rules but those of a feature in ``skipped``, found once per column
        and set of skipped features."""
        key = (column, tuple(map(id, skipped)))
        kept = self._first_kept.get(key)
        if kept is None:
            found = tuple(f for f in self._first_on[column] if f[0] not in skipped)
            kept = self._first_kept[key] = (skipped, found)
        return kept[1]

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


#: A column's rules of a rewrite function: each with its feature and the column's Rewrite.
_Found = tuple[tuple[Feature, Rule, Rewrite], ...]
#: The features skipped, and a column's first rules less theirs.
_Kept = tuple[tuple[Feature, ...], _Found]


def _rewrites_of(rules: list[tuple[Feature, Rule]], column: str) -> _Found:
    """The rules of ``rules`` whose form on ``column`` is a rewrite function, in their order; one
    that keeps the old table's form answers nothing, and is left out."""
    return tuple(
        (feature, rule, form)
        for feature, rule in rules
        if isinstance(form := rule.forms.get(column), Rewrite) and not form.keeps_old
    )


def _called(node: exp.Expr) -> tuple[str, ...]:
    """The names a pattern's root call is written by."""
    if isinstance(node, exp.Anonymous):
        return (node.name.lower(),)
    if isinstance(node, exp.Func):
        return tuple(name.lower() for name in type(node).sql_names())
    return ()


def _written_name(pattern: str) -> str:
    """The name of the function a pattern's text calls at its root."""
    return pattern.split("(", 1)[0].strip().lower()


def _root_key(node: exp.Expr) -> str:
    """What a pattern's root and a statement's node are indexed by: a call's name."""
    if isinstance(node, exp.Anonymous):
        return node.name.lower()
    return type(node).__name__
