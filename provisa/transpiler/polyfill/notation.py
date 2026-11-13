# Copyright (c) 2026 Kenneth Stott
# Canary: 4cbb917f-79f4-43f9-b96b-eab9c782fff3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The notation a polyfill is written in (REQ-2236, A POLYFILL IS A RULE THE TRANSLATOR RUNS).

A rule is a pattern in PostgreSQL's SQL with holes, and for each engine a replacement in that
engine's SQL with the same holes, or a refusal, or the name of a rewrite function (THE ESCAPE HATCH
IS A FUNCTION NAME). A hole is ``$`` and a name: ``$a``. In a pattern a hole may be typed by a
PostgreSQL cast, ``$a::anyarray``, and then matches only an argument whose type the statement says
fits that type, as provisa.pgwire.function_types reads it (an argument whose type is not said fits
any).

Both sides are parsed by sqlglot, each in its own dialect, so a rule is two trees, never text the
translator edits. Each side is read by its dialect with ``$`` read as a parameter's sign, so
``$a`` is the dialect's own parameter node (a named placeholder on DuckDB), and that node is the
hole."""

# Requirements: REQ-2236

from __future__ import annotations

import functools
from collections.abc import Callable
from dataclasses import dataclass

import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import SqlglotError
from sqlglot.tokens import TokenType

from provisa.pgwire import function_types

#: The dialect a pattern is written in.
PATTERN_DIALECT = "postgres"


class RuleError(ValueError):
    """A rule the notation does not read: a side that does not parse, or a replacement naming a
    hole its pattern does not have."""


@dataclass(frozen=True)
class Refusal:
    """A refused feature: the SQLSTATE and the message a client receives."""

    sqlstate: str
    message: str


@dataclass(frozen=True)
class Rewrite:
    """A rule that is the name of its rewrite function: the function matches and rewrites on the
    tree, answering the rewritten node, or None where the node is not its to rewrite."""

    function: Callable[[exp.Expr, str], exp.Expr | None]
    #: For a rule of no pattern, which names no function: a regular expression of what a statement
    #: it may rewrite holds (a type's name), so the translator reads a statement that calls no
    #: function of the grid's (provisa.transpiler.function_macros reads no other).
    mentions: str | None = None
    #: For a rule of no pattern: read before any rule's pattern, as a rule over every call, which
    #: holds whatever row a call's own name has (Grid.read_first).
    first: bool = False

    @property
    def name(self) -> str:
        return f"{self.function.__module__}.{self.function.__qualname__}"

    @property
    def purpose(self) -> str:
        """The first line of the function's docstring: what the docs print beside its name."""
        doc = (self.function.__doc__ or "").strip()
        return doc.splitlines()[0] if doc else ""


class Refused(Exception):  # noqa: N818 -- a refusal, as Refusal names what it carries
    """A rewrite function's refusal of a call its rule matched, where what refuses it is a value
    and not the call's shape: the client receives ``refusal``, which the row lists as a
    limitation."""

    def __init__(self, refusal: Refusal) -> None:
        super().__init__(refusal.message)
        self.refusal = refusal


#: A replacement: an engine's SQL with the pattern's holes, a refusal, or a rewrite function.
Form = str | Refusal | Rewrite


@functools.cache
def rule_dialect(dialect: str) -> Dialect:
    """``dialect`` with ``$`` read as a parameter's sign. A rule has no dollar-quoted string, so
    ``$`` never opens one: a dialect that reads ``$tag$`` would read ``$a[1:$n]`` as one."""
    base = type(Dialect.get_or_raise(dialect))
    tokenizer = base.tokenizer_class
    holes = type(
        f"{tokenizer.__name__}Holes",
        (tokenizer,),
        {
            "SINGLE_TOKENS": {**tokenizer.SINGLE_TOKENS, "$": TokenType.PARAMETER},
            "HEREDOC_STRINGS": [start for start in tokenizer.HEREDOC_STRINGS if start != "$"],
        },
    )
    name = f"_polyfill_rule_{base.__name__.lower()}"
    derived = type(name, (base,), {"tokenizer_class": holes, "Tokenizer": holes})
    # A rule's dialect reads rules only: no statement names it.
    Dialect.classes.pop(name, None)
    return derived()


def hole_name(node: exp.Expr) -> str | None:
    """The name of the hole ``node`` is, or None. A dialect reads ``$a`` as a parameter of the name
    or as a named placeholder; a positional one (``$1``) is no hole."""
    if isinstance(node, exp.Parameter) and isinstance(node.this, exp.Var):
        return node.this.name
    if isinstance(node, exp.Placeholder) and isinstance(node.this, str) and node.this:
        return None if node.this.isdigit() else node.this
    return None


def typed_hole(node: exp.Expr) -> tuple[str, str] | None:
    """A hole cast to a type in a pattern: its name and PostgreSQL's name of the type."""
    if not isinstance(node, exp.Cast):
        return None
    name = hole_name(node.this)
    if name is None:
        return None
    said = function_types.argument_type(node)
    if said is None:
        raise RuleError(
            f"${name}: the type {node.to.sql(PATTERN_DIALECT)} is not one PostgreSQL has"
        )
    return name, said


def holes(tree: exp.Expr) -> tuple[str, ...]:
    """The holes of ``tree`` in the order they are written, each once."""
    named = (hole_name(node) for node in tree.dfs())
    return tuple(dict.fromkeys(name for name in named if name is not None))


def _parsed(text: str, dialect: str) -> exp.Expr:
    try:
        return sqlglot.parse_one(text, dialect=rule_dialect(dialect))
    except SqlglotError as error:
        raise RuleError(f"{dialect}: {text!r} does not parse: {error}") from error


@functools.cache
def pattern(text: str) -> exp.Expr:
    """A pattern's tree, in PostgreSQL's dialect. The tree is shared: copy it to change it."""
    tree = _parsed(text, PATTERN_DIALECT)
    for node in tree.dfs():
        typed_hole(node)  # its type is one PostgreSQL has
    return tree


@functools.cache
def replacement(text: str, dialect: str, pattern_text: str) -> exp.Expr:
    """A replacement's tree, in ``dialect``, naming only holes ``pattern_text`` has. The tree is
    shared: copy it to change it."""
    tree = _parsed(text, dialect)
    missing = set(holes(tree)) - set(holes(pattern(pattern_text)))
    if missing:
        raise RuleError(f"{text!r} names {sorted(missing)}, which {pattern_text!r} does not have")
    return tree
