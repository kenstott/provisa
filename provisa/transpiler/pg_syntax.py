# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1e4a92-5b3d-4f08-9e61-d2a8b0c5f317
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's spellings that sqlglot's own parser misreads, read by its parser (REQ-2167). Every
rewrite of a statement is made on the tree, never on its text, so what sqlglot cannot read is
taught to its parser here:

- ``bit varying [(n)]`` is a type, ``varbit``: sqlglot reads it as ``bit`` and an alias.
- ``oid[]``, ``regtype[]``, ``cstring[]`` and each other type sqlglot holds as an object
  identifier or a pseudo-type is an array type, PostgreSQL's own name of it (``_oid``): sqlglot
  reads the brackets as an empty subscript.
- ``overlaps(a, b, c, d)``, bare or pg_catalog's, is the call pg_proc names ``overlaps_timestamp``:
  sqlglot reads the bare one as a column and an alias. A PostgreSQL target writes it back as
  ``pg_catalog."overlaps"`` (``overlaps_restored``).
- ``convert(bytes, from, to)`` is PostgreSQL's call, not the CONVERT of a type or a charset.
- ``json_object('{...}')`` and ``json_object(ARRAY[...], ...)`` are PostgreSQL's calls of an
  array, not SQL's JSON_OBJECT of key and value pairs.
- a keyword after ``AS`` is a column label (``AS select``): sqlglot refuses the reserved ones; the
  label is read as a quoted identifier, so the statement writes back as PostgreSQL reads it.
- pgvector's ``a <#> b`` is its negative inner product: it tokenizes as ``<`` and ``#>``.
- to_date's and to_timestamp's template is read whole (``TEMPLATE``): sqlglot's own reading leaves
  a month's or a day's name (``Mon``, ``Month``, ``Dy``), ``HH`` and the meridiem as literal text."""

# Requirements: REQ-2167, REQ-423

from __future__ import annotations

from typing import Any

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.time import format_time
from sqlglot.tokens import TokenType
from sqlglot.trie import new_trie

#: The routine pg_proc names for overlaps: what a call of it reads as (function_macros' form).
OVERLAPS = "overlaps_timestamp"
#: The array types sqlglot reads ``[]`` after as a subscript, and the name of each.
_ARRAYED = (exp.PseudoType, exp.ObjectIdentifier)


class NegativeInnerProduct(exp.Expression, exp.Binary):  # REQ-423
    """pgvector's ``a <#> b``."""


def _varbit(parser: Any) -> exp.DataType:
    """``varbit`` with the length ``bit varying`` gave, which follows it in parentheses."""
    sizes: list[exp.Expr] = []
    if parser._match(TokenType.L_PAREN):
        sizes = parser._parse_csv(lambda: exp.DataTypeParam(this=parser._parse_primary()))
        parser._match_r_paren()
    return exp.DataType(
        this=exp.DataType.Type.USERDEFINED, kind=exp.to_identifier("varbit"), expressions=sizes
    )


def _reads_types(parser: type) -> None:
    parse_types = parser._parse_types

    def _parse_types(self, *args: Any, **kwargs: Any) -> exp.Expr | None:
        this = parse_types(self, *args, **kwargs)
        if (
            isinstance(this, exp.DataType)
            and this.is_type(exp.DataType.Type.BIT)
            and not this.expressions
            and self._curr is not None
            and self._curr.token_type == TokenType.VAR
            and self._curr.text.upper() == "VARYING"
        ):
            self._advance()
            return _varbit(self)
        if isinstance(this, _ARRAYED) and self._match_pair(
            TokenType.L_BRACKET, TokenType.R_BRACKET
        ):
            while self._match_pair(TokenType.L_BRACKET, TokenType.R_BRACKET):
                pass
            return exp.DataType(
                this=exp.DataType.Type.USERDEFINED, kind=exp.to_identifier(f"_{this.name.lower()}")
            )
        return this

    parser._parse_types = _parse_types


def _overlaps(args: list[exp.Expr]) -> exp.Anonymous:
    return exp.Anonymous(this=OVERLAPS, expressions=args)


def _qualified(name: str, call: exp.Anonymous) -> exp.Expr | None:
    """pg_catalog's ``call``, as its bare call reads."""
    if name == "overlaps":
        return _overlaps(call.expressions)
    return None


def _reads_qualified_calls(parser: type) -> None:
    parse_column_ops = parser._parse_column_ops

    def _parse_column_ops(self, this: exp.Expr | None) -> exp.Expr | None:
        this = parse_column_ops(self, this)
        if (
            isinstance(this, exp.Dot)
            and isinstance(this.expression, exp.Anonymous)
            and isinstance(this.this, (exp.Column, exp.Identifier, exp.Var))
            and this.this.name.lower() == "pg_catalog"
        ):
            return _qualified(this.expression.name.lower(), this.expression) or this
        return this

    parser._parse_column_ops = _parse_column_ops


def _pg_json_object(self: Any) -> exp.Expr:
    """json_object of an array (``'{...}'``, ``ARRAY[...]``) as PostgreSQL's call; any other as
    sqlglot reads SQL's."""
    current, following = self._curr, self._next
    of_array = current is not None and (
        (current.token_type == TokenType.STRING and current.text.startswith("{"))
        or (
            current.token_type == TokenType.ARRAY
            and following is not None
            and following.token_type == TokenType.L_BRACKET
        )
    )
    if of_array:
        return exp.Anonymous(
            this="json_object", expressions=self._parse_csv(self._parse_assignment)
        )
    return _JSON_OBJECT(self)


def _reads_negative_inner_product(parser: type) -> None:
    """``a <#> b``, which tokenizes as ``<`` then ``#>``: sqlglot refused it."""
    parse_bitwise = parser._parse_bitwise

    def _parse_bitwise_with_inner_product(self) -> exp.Expr | None:
        this = parse_bitwise(self)
        while self._curr is not None and self._next is not None:
            if not (
                self._curr.token_type == TokenType.LT
                and self._next.token_type == TokenType.HASH_ARROW
                and self._curr.end + 1 == self._next.start
            ):
                return this
            self._advance(2)
            this = self.expression(NegativeInnerProduct(this=this, expression=parse_bitwise(self)))
        return this

    parser._parse_bitwise = _parse_bitwise_with_inner_product


def _reads_keyword_labels(parser: type) -> None:
    """``expr AS <keyword>``: PostgreSQL takes any keyword as a label after AS. A word token
    sqlglot does not take as an identifier becomes one, quoted and folded to lower case as
    PostgreSQL folds it."""
    parse_alias = parser._parse_alias

    def _parse_alias(self, this: exp.Expr | None, explicit: bool = False) -> exp.Expr | None:
        current, following = self._curr, self._next
        if (
            this is not None
            and current is not None
            and current.token_type == TokenType.ALIAS
            and following is not None
            and following.text.isalpha()
            and following.token_type not in self.ID_VAR_TOKENS
            and following.token_type != TokenType.STRING
        ):
            self._advance(2)
            label = exp.to_identifier(following.text.lower(), quoted=True)
            return self.expression(exp.Alias(this=this, alias=label))
        return parse_alias(self, this, explicit)

    parser._parse_alias = _parse_alias


#: PostgreSQL's template patterns, as to_date and to_timestamp read them, by the strptime
#: specifier of each (formatting.c's keywords): sqlglot's own, with the names, HH and the meridiem
#: it lacks, and its day names for parsing (its lower-case ones are to_char's).
TEMPLATE: dict[str, str] = {
    **Postgres.TIME_MAPPING,
    **{name: "%b" for name in ("Mon", "MON", "mon")},
    **{name: "%B" for name in ("Month", "MONTH", "month")},
    **{name: "%a" for name in ("Dy", "DY", "dy")},
    **{name: "%A" for name in ("Day", "DAY", "day")},
    **{name: "%I" for name in ("HH", "hh", "hh12")},
    "hh24": "%H",
    "us": "%f",
    **{name: "%p" for name in ("AM", "PM", "am", "pm")},
}
_TEMPLATE_TRIE = new_trie(TEMPLATE)


def template(written: str) -> str:
    """PostgreSQL's template ``written``, as the strptime format of it."""
    return format_time(written, TEMPLATE, _TEMPLATE_TRIE) or written


def _formatted(kind: type[exp.StrToDate] | type[exp.StrToTime]) -> Any:
    def build(args: list[exp.Expr]) -> exp.Expr:
        value, format_ = args[0], args[1] if len(args) > 1 else None
        if isinstance(format_, exp.Literal) and format_.is_string:
            format_ = exp.Literal.string(template(format_.this))
        return kind(this=value, format=format_)

    return build


_TO_TIMESTAMP = Postgres.Parser.FUNCTIONS["TO_TIMESTAMP"]


def _to_timestamp(args: list[exp.Expr], dialect: Any) -> exp.Expr:
    """to_timestamp: of one argument, the epoch's; of two, a text read by its template."""
    if len(args) == 1:
        return _TO_TIMESTAMP(args, dialect)
    return _formatted(exp.StrToTime)(args)


_TO_CHAR = Postgres.Parser.FUNCTIONS["TO_CHAR"]
#: The key of a to_char call's meta that holds its template as the statement wrote it.
TO_CHAR_TEMPLATE = "pg_template"


def _to_char(args: list[exp.Expr], dialect: Any) -> exp.Expr:
    """to_char as sqlglot reads it, its template as the statement wrote it kept in the call's meta:
    sqlglot's strftime text of it no longer tells PostgreSQL's YYYY from a literal %Y."""
    call = _TO_CHAR(args, dialect)
    written = args[1] if len(args) > 1 else None
    if isinstance(written, exp.Literal) and written.is_string:
        call.meta[TO_CHAR_TEMPLATE] = written.this
    return call


_TO_CHAR_WRITTEN = Postgres.Generator.TRANSFORMS[exp.TimeToStr]


def _to_char_written(self: Any, node: exp.TimeToStr) -> str:
    """to_char in PostgreSQL's SQL, its template the one the statement wrote: a step that writes
    the statement again (transpile's passes over its text) keeps it, where sqlglot's strftime
    text of it would be written back as another template ('%Y' as 'YYYY')."""
    template = node.meta.get(TO_CHAR_TEMPLATE)
    if template is None:
        return _TO_CHAR_WRITTEN(self, node)
    return self.func("TO_CHAR", node.this, exp.Literal.string(template))


_JSON_OBJECT = Postgres.Parser.FUNCTION_PARSERS["JSON_OBJECT"]

_reads_types(Postgres.Parser)
_reads_qualified_calls(Postgres.Parser)
_reads_negative_inner_product(Postgres.Parser)
_reads_keyword_labels(Postgres.Parser)
Postgres.Parser.FUNC_TOKENS = Postgres.Parser.FUNC_TOKENS | {TokenType.OVERLAPS}
Postgres.Parser.FUNCTIONS["OVERLAPS"] = _overlaps
Postgres.Parser.FUNCTIONS["TO_DATE"] = _formatted(exp.StrToDate)
Postgres.Parser.FUNCTIONS["TO_TIMESTAMP"] = _to_timestamp
Postgres.Parser.FUNCTIONS["TO_CHAR"] = _to_char
Postgres.Generator.TRANSFORMS[exp.TimeToStr] = _to_char_written
Postgres.Parser.FUNCTION_PARSERS.pop("CONVERT", None)
Postgres.Parser.FUNCTIONS["CONVERT"] = lambda args: exp.Anonymous(this="convert", expressions=args)
Postgres.Parser.FUNCTION_PARSERS["JSON_OBJECT"] = _pg_json_object
Postgres.Generator.TRANSFORMS[NegativeInnerProduct] = lambda self, e: self.binary(e, "<#>")


def _anonymous_sql(self: Any, expression: exp.Anonymous) -> str:
    """A call by a quoted name keeps the name's case: PostgreSQL resolves a quoted name as written,
    so ``pg_catalog."json_to_record"`` in FROM is not ``"JSON_TO_RECORD"``. Any other call is
    written as sqlglot writes it."""
    name = expression.this
    if isinstance(name, exp.Identifier) and name.quoted:
        return self.func(self.sql(expression, "this"), *expression.expressions, normalize=False)
    return _BASE_ANONYMOUS_SQL(self, expression)


_BASE_ANONYMOUS_SQL = Postgres.Generator.anonymous_sql
Postgres.Generator.anonymous_sql = _anonymous_sql


def overlaps_restored(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each call of overlaps PostgreSQL's own, ``pg_catalog."overlaps"``: it has no
    function of the routine's name."""

    def restored(node: exp.Expr) -> exp.Expr:
        if isinstance(node, exp.Anonymous) and node.name.lower() == OVERLAPS:
            called = exp.Anonymous(
                this=exp.to_identifier("overlaps", quoted=True), expressions=node.expressions
            )
            return exp.Dot(this=exp.to_identifier("pg_catalog"), expression=called)
        return node

    return tree.transform(restored)
