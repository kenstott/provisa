# Copyright (c) 2026 Kenneth Stott
# Canary: b0bac4ef-6df2-4879-9f54-7dfbf94069eb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The PostgreSQL types of a catalog statement's result columns (REQ-2167).

A column the statement reads from a pg_catalog or information_schema relation, as it is or
through an alias, a subquery or a CTE, has the type PostgreSQL 17 declares for it
(catalog_pg17.RELATIONS); a column that is a call of a pg_catalog function has the type PostgreSQL
17 says the function returns (catalog_pg17.FUNCTION_RETURNS); any other column has the type its
expression gives."""

from __future__ import annotations

import functools

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import ParseError
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.schema import MappingSchema

from provisa.pgwire.catalog_pg17 import DOMAIN_TYPMODS, FUNCTION_RETURNS, RELATIONS, TYPES
from provisa.pgwire.catalog_pg17_procs import PROCS
from provisa.pgwire.function_types import call_result_type

# The types whose text and binary forms are those of the wire type a catalog column is served
# with (text, a 4-byte integer, or a timestamp with time zone), named the way the statement's annotation names them; their
# OIDs are PostgreSQL 17's, as RELATIONS records them.
_SAME_WIRE_FORM = frozenset(
    {
        "name",
        '"char"',
        "pg_node_tree",
        "oid",
        "xid",
        "information_schema.sql_identifier",
        "information_schema.character_data",
        "information_schema.yes_or_no",
        "information_schema.cardinal_number",
        "information_schema.time_stamp",
        # REQ-2204: character_data's base type, which a UNION of it with another input resolves
        # to (Metabase's column listing), its value the domain's text.
        "character varying",
        # Held as text in the always-empty statistics relations (catalog_pg17_tables): described
        # as PostgreSQL's, though no value of one is ever sent.
        "anyarray",
        "pg_ndistinct",
        "pg_dependencies",
        "pg_mcv_list",
        "pg_statistic[]",
        # A record-returning catalog function answers a NULL record, and pg_sleep the void value
        # (empty text): no composite value is ever sent.
        "record",
        "void",
        # REQ-2167: a server function's value, held as what its wire form is: xid8 a bigint, the
        # object identifier types an oid, and the rest their text. A binary pg_snapshot or
        # aclitem is its text.
        "xid8",
        "pg_snapshot",
        "txid_snapshot",
        "aclitem",
        "tid",
        "regcollation",
        "regconfig",
        "regdictionary",
        "regoper",
        "regoperator",
        "regprocedure",
        "regrole",
    }
)


def described_type_oid(oid: int) -> int:
    """The OID PostgreSQL describes a column of type ``oid`` with: a domain's base type, as
    printtup.c SendRowDescriptionMessage sends it (getBaseTypeAndTypmod), so information_schema's
    character_data is described as varchar and sql_identifier as name (Grafana's column listing)."""
    entry = TYPES[oid]
    return described_type_oid(entry[6]) if entry[3] == "d" else oid


#: Each such type's name and the OID a column of it is described with.
CATALOG_TYPE_OIDS: dict[str, int] = {
    entry[-1]: described_type_oid(oid)
    for oid, entry in TYPES.items()
    if entry[-1] in _SAME_WIRE_FORM
}


def described_typmod(oid: int) -> int:
    """The typmod PostgreSQL describes a column of type ``oid`` with: a domain's own typmod, as
    printtup.c getBaseTypeAndTypmod gives it (yes_or_no, varchar(3), a 7; time_stamp,
    timestamptz(2), a 2), or -1, PostgreSQL's "no typmod", for a type that is not a domain."""
    return DOMAIN_TYPMODS[oid] if TYPES[oid][3] == "d" else -1


#: Each such type's name and the typmod a column of it is described with, where it has one.
CATALOG_TYPE_TYPMODS: dict[str, int] = {
    entry[-1]: described_typmod(oid)
    for oid, entry in TYPES.items()
    if entry[-1] in _SAME_WIRE_FORM and described_typmod(oid) != -1
}

_PSEUDO_TYPES = frozenset(entry[-1] for entry in TYPES.values() if entry[3] == "p")

# A function returning a polymorphic pseudo-type (anyelement, anyarray, ...) is left to its
# expression; one returning record or void is that type.
_FUNCTION_TYPES = {
    name: returns
    for name, returns in FUNCTION_RETURNS.items()
    if returns not in _PSEUDO_TYPES or returns in ("record", "void")
}


def _column_type(pg_type: str) -> str | exp.DataType:
    """A column's type for the annotation. sqlglot reads ``oid[]`` (and ``regtype[]``) as the
    element type, so an array of a type it knows is built as an array explicitly."""
    if not pg_type.endswith("[]"):
        return pg_type
    element = exp.DataType.build(pg_type[:-2], dialect="postgres", udt=True)
    if element.is_type(exp.DataType.Type.USERDEFINED):
        return pg_type
    return exp.DataType(this=exp.DataType.Type.ARRAY, expressions=[element], nested=True)


@functools.cache
def _schema() -> MappingSchema:
    nested: dict[str, object] = {
        schema: {
            relation: {name: _column_type(pg_type) for name, pg_type, _ in columns}
            for (in_schema, relation), columns in RELATIONS.items()
            if in_schema == schema
        }
        for schema, _ in RELATIONS
    }
    return MappingSchema(nested, dialect="postgres")


@functools.lru_cache(maxsize=4096)
def result_types(sql: str) -> tuple[tuple[str, str], ...]:
    """Each result column's name and PostgreSQL type, as PostgreSQL's format_type names it (an
    unknown type is the empty string). An unqualified relation is pg_catalog's, first on PostgreSQL's
    search path."""
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except ParseError:
        return ()  # a call sqlglot reads as syntax (convert): the engine's types describe it
    # A pg_catalog table function's columns are its OUT parameters: sqlglot will not resolve a
    # correlated subquery's column while a source's columns are unknown (psql's FDW options read
    # pg_options_to_table(c.reloptions)).
    qualified = qualify(
        _with_out_columns(tree),
        db="pg_catalog",
        schema=_schema(),
        dialect="postgres",
        validate_qualify_columns=False,
        quote_identifiers=False,
    )
    annotated = annotate_types(
        _search_path_types(qualified),
        schema=_schema(),
        expression_metadata=_expression_metadata(),
        dialect="postgres",
    )
    if not isinstance(annotated, exp.Query):
        return ()
    return tuple(zip((s.alias_or_name for s in annotated.selects), _query_types(annotated)))


def _select_types(select: exp.Query) -> list[str]:
    return [_function_type(s.unalias()) or _format_type(s.type) for s in select.selects]


def _query_types(query: exp.Expr) -> list[str]:
    """Each output column's type: a set operation's is its branches' common type."""
    if isinstance(query, exp.Subquery):
        return _query_types(query.this)
    if not isinstance(query, exp.SetOperation):
        return _select_types(query) if isinstance(query, exp.Query) else []
    left, right = _query_types(query.left), _query_types(query.right)
    if len(left) != len(right):
        return left
    return [_common_type((a, b)) for a, b in zip(left, right)]


def _base_type(pg_type: str) -> str:
    """A domain's base type (information_schema.sql_identifier is name); any other type itself."""
    entry = _TYPES_BY_NAME.get(pg_type)
    if entry is None or entry[3] != "d":
        return pg_type
    return TYPES[entry[6]][-1]


def _common_type(types: tuple[str, ...]) -> str:
    """The type PostgreSQL resolves a UNION's (or CASE's) inputs to (parse_coerce.c
    select_common_type): one type if all are it; otherwise each domain is its base type and an
    untyped NULL counts for nothing, and the type they then share. Where they share none, the
    first input's type, as before."""
    if len(set(types)) == 1:
        return types[0]
    known = {_base_type(t) for t in types if t not in _UNTYPED}
    if not known:
        return "text"
    if len(known) == 1:
        return known.pop()
    return types[0]


#: An untyped NULL literal, as _format_type names its type.
_UNTYPED = frozenset({"", "null"})

_TYPES_BY_NAME = {entry[-1]: entry for entry in TYPES.values()}

#: A keyword PostgreSQL reads as a function of the session, by the function it is.
_SESSION_KEYWORDS = {"current_catalog": "current_database"}


def _search_path_types(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each type named in pg_catalog (``pg_catalog.regclass``) by its bare name, as
    PostgreSQL's search path resolves it: sqlglot keeps a qualified user-defined type through a
    cast but not through a CASE."""
    for data_type in list(tree.find_all(exp.DataType)):
        kind = data_type.args.get("kind")
        if (
            data_type.this == exp.DataType.Type.USERDEFINED
            and isinstance(kind, exp.Dot)
            and kind.this.name.lower() == "pg_catalog"
        ):
            bare = exp.DataType.build(kind.expression.name, dialect="postgres", udt=True)
            data_type.replace(bare)
    return tree


_INTEGER_WIDTHS = (
    exp.DataType.Type.SMALLINT,
    exp.DataType.Type.INT,
    exp.DataType.Type.BIGINT,
)


def _integer_division(annotator, expression: exp.Div) -> exp.Div:
    """PostgreSQL's integer division (int2/int4/int8 operators): of two integers, the wider
    operand's type, an integer literal being int4; sqlglot's own annotation is bigint."""
    left, right = _integer_width(expression.left), _integer_width(expression.right)
    if expression.args.get("typed") and left is not None and right is not None:
        annotator._set_type(expression, _INTEGER_WIDTHS[max(left, right)])
        return expression
    return annotator._annotate_div(expression)


def _integer_width(side: exp.Expr) -> int | None:
    kind = side.type.this if side.type else None
    if kind in _INTEGER_WIDTHS:
        return _INTEGER_WIDTHS.index(kind)
    return None


@functools.cache
def _expression_metadata() -> dict:
    from sqlglot.dialects.postgres import Postgres

    return {**Postgres.EXPRESSION_METADATA, exp.Div: {"annotator": _integer_division}}


@functools.cache
def _out_columns() -> dict[str, tuple[str, ...]]:
    """Each pg_catalog table function's OUT parameter names, where every overload names the same."""
    named: dict[str, set[tuple[str, ...]]] = {}
    for proc in PROCS:
        modes, names = proc[20], proc[21]
        if not modes or not names:
            continue
        outs = tuple(n for n, m in zip(names, modes, strict=True) if m in ("o", "t", "b"))
        if outs:
            named.setdefault(proc[1], set()).add(outs)
    return {name: next(iter(outs)) for name, outs in named.items() if len(outs) == 1}


#: sqlglot's readings of PostgreSQL's one-column set-returning functions: unnest of one array, and
#: generate_series. string_to_array returns no set: in FROM it is one row
#: (:data:`_SCALAR_FROM_FUNCTIONS`), never a table function, which DuckDB has none of (REQ-2204).
_ONE_COLUMN_FUNCTIONS = (exp.Explode, exp.GenerateSeries)


def one_column_function(table: exp.Expr) -> exp.Table | None:  # REQ-2204
    """``table`` as PostgreSQL reads it where it is one of PostgreSQL's one-column set-returning
    functions in FROM, unqualified or in pg_catalog, aliased without column names: the alias names
    its one column (psql's ``pg_catalog.unnest(tc.reloptions) x`` reads column x), written
    ``unnest(...) AS x(x)`` with no schema, which every engine reads. None for any other FROM
    item."""
    if not isinstance(table, exp.Table) or type(table.this) not in _ONE_COLUMN_FUNCTIONS:
        return None
    alias = table.args.get("alias")
    if table.catalog or table.db.lower() not in ("", "pg_catalog"):
        return None
    if alias is None or alias.columns:
        return None
    named = exp.TableAlias(this=alias.this.copy(), columns=[exp.to_identifier(alias.name)])
    return exp.Table(this=table.this, alias=named)


#: sqlglot's readings of the PostgreSQL scalar functions a client's catalog SQL calls in FROM
#: (Grafana's ``string_to_array(current_setting('search_path'), ',') s``).
_SCALAR_FROM_FUNCTIONS = (exp.StringToArray,)


def scalar_function_row(table: exp.Expr) -> exp.Subquery | None:  # REQ-2204
    """``table`` as PostgreSQL reads it where it is one of :data:`_SCALAR_FROM_FUNCTIONS` in FROM,
    unqualified or in pg_catalog: a function that returns no set is one row of one column, the
    column named by the alias's column, else by the alias, else by the function. Written as a
    one-row subquery, which every engine reads and which reads no relation. None for any other
    FROM item."""
    if not isinstance(table, exp.Table) or type(table.this) not in _SCALAR_FROM_FUNCTIONS:
        return None
    if table.catalog or table.db.lower() not in ("", "pg_catalog"):
        return None
    alias = table.args.get("alias")
    relation = alias.name if alias is not None else type(table.this).sql_names()[0].lower()
    column = alias.columns[0].name if alias is not None and alias.columns else relation
    return exp.Subquery(
        this=exp.select(exp.alias_(table.this.copy(), column, quoted=True)),
        alias=exp.TableAlias(this=exp.to_identifier(relation, quoted=True)),
    )


def _with_out_columns(tree: exp.Expr) -> exp.Expr:
    """``tree`` with each pg_catalog table function in FROM given its OUT parameters as the
    alias's columns, as PostgreSQL names them; the alias defaults to the function's name. A
    one-column function's alias names its column (:func:`one_column_function`)."""
    for table in list(tree.find_all(exp.Table)):
        named = one_column_function(table) or scalar_function_row(table)
        if named is not None:
            table.replace(named)
    for table in tree.find_all(exp.Table):
        call = table.this
        if not isinstance(call, exp.Anonymous) or table.alias_column_names:
            continue
        if table.db and table.db.lower() != "pg_catalog":
            continue
        columns = _out_columns().get(call.name.lower())
        if columns is None:
            continue
        alias = table.args.get("alias") or exp.TableAlias(this=exp.to_identifier(call.name))
        alias.set("columns", [exp.to_identifier(c) for c in columns])
        table.set("alias", alias)
    return tree


def _function_type(expression: exp.Expr) -> str:
    """The type PostgreSQL 17 says a pg_catalog function call returns; empty when the expression
    is not such a call."""
    if isinstance(expression, exp.Dot):
        expression = expression.expression
    overload = call_result_type(expression)  # REQ-2167: the overload its arguments name
    if overload is not None and (overload not in _PSEUDO_TYPES or overload in ("record", "void")):
        return overload
    if isinstance(expression, exp.Anonymous):
        name = expression.name.lower()
    elif isinstance(expression, exp.Func):
        name = type(expression).sql_names()[0].lower()
    else:
        return ""
    return _FUNCTION_TYPES.get(_SESSION_KEYWORDS.get(name, name), "")


def _format_type(data_type) -> str:
    if data_type is None or data_type.is_type("unknown"):
        return ""
    # A type named in pg_catalog is the type of its bare name (pg_catalog.regclass is regclass).
    return data_type.sql("postgres").lower().removeprefix("pg_catalog.")
