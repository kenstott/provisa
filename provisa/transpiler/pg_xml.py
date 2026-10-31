# Copyright (c) 2026 Kenneth Stott
# Canary: 4e9a13c8-5f07-4b62-a3d9-0c86e7f1d524
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's XML syntax and calls on DuckDB (REQ-2167, REQ-2171): each written as the
functions of ``provisa.federation.duckdb_pg_xml``. Part of what PostgreSQL's parser does is done
here too, as it needs the statement rather than a value: an element's name is mapped to an XML
name, and an argument that is an xml value is told from one that is text, which is escaped."""

# Requirements: REQ-2167

from __future__ import annotations

import json
import re
import unicodedata
from typing import Callable

import sqlglot
import sqlglot.expressions as exp

from provisa.core.pg_error import MARK, raising
from provisa.transpiler import function_macros
from provisa.transpiler.xml_syntax import (
    RETURNS_XML,
    XML_VALUES,
    XmlExists,
    XmlParse,
    XmlPi,
    XmlRoot,
    XmlSerialize,
)

#: A statement ``typed_calls`` has XML work in.
MENTIONED = re.compile(r"\bxml|\bxpath|\bdocument\b|\bprovisa_xml\b", re.IGNORECASE)

_XML_TYPE = exp.DataType.Type.XML
#: The dialects that run PostgreSQL's XML as PostgreSQL's parser reads it.
NATIVE_DIALECTS = frozenset({"postgres", "postgresql"})


class XmlFunctionRefused(ValueError):
    """An XML function on a dialect with no way to run it: refused by name, never sent to fail
    there or answered over the value's text form."""

    code = "query.xml_function_unsupported"
    sqlstate = "0A000"

    def __init__(self, name: str, dialect: str) -> None:
        self.params = {"function": name, "dialect": dialect}
        super().__init__(f"{name} needs XML, which {dialect} has no SQL for")


#: Every XML function that is a call. A value already answered, provisa_xml, is none.
_CALLS = frozenset({"xmlattributes", "xpath", "xpath_exists", "xml_is_well_formed"}) | frozenset(
    {"xml_is_well_formed_content", "xml_is_well_formed_document", "xml_send", "xmlvalidate"}
)


def _xml_function(node: exp.Expr) -> str | None:
    """The name of the XML function a node is; None for any other."""
    if isinstance(node, (XmlParse, XmlPi, XmlRoot, XmlSerialize, XmlExists)):
        return type(node).sql_names()[0]
    if isinstance(node, exp.XMLElement):
        return "XMLELEMENT"
    if isinstance(node, exp.XMLTable):
        return "XMLTABLE"
    if isinstance(node, exp.Is) and isinstance(node.expression, exp.Column):
        return "IS DOCUMENT" if node.expression.name.upper() == "DOCUMENT" else None
    if isinstance(node, exp.Anonymous):
        name = node.name.lower()
        if name in _CALLS or (name in RETURNS_XML and name != "provisa_xml"):
            return name
    return None


def _has_form(node: exp.Expr, dialect: str) -> bool:
    """Whether the rule writes this XML call for ``dialect``."""
    if not isinstance(node, exp.Anonymous):
        return False  # syntax of XML's own, which only PostgreSQL's and DuckDB's forms read
    key = (node.name.lower(), len(node.expressions))
    return key in function_macros.MACROS and function_macros.outcome(*key, dialect) != "refused"


def for_dialect(pg_sql: str, dialect: str) -> str:
    """``pg_sql`` for a dialect other than DuckDB's: PostgreSQL runs XML as written, and any other
    runs the forms the rule has for it (function_macros), so an XML function with none is refused
    by name. A document already answered is a value there, as xml on PostgreSQL and its text
    elsewhere."""
    if MENTIONED.search(pg_sql) is None:
        return pg_sql
    tree = sqlglot.parse_one(pg_sql, read="postgres")
    if dialect not in NATIVE_DIALECTS:
        for node in tree.walk():
            name = _xml_function(node)
            if name is not None and not _has_form(node, dialect):
                raise XmlFunctionRefused(name, dialect)

    def value(node: exp.Expr) -> exp.Expr:
        if not (isinstance(node, exp.Anonymous) and node.name.lower() == "provisa_xml"):
            return node
        if dialect in NATIVE_DIALECTS:
            return exp.Cast(this=node.expressions[0], to=exp.DataType(this=_XML_TYPE))
        return node.expressions[0]

    if not any(
        isinstance(n, exp.Anonymous) and n.name.lower() == "provisa_xml" for n in tree.walk()
    ):
        return pg_sql
    return tree.transform(value).sql(dialect="postgres")


def _refused(sqlstate: str, message: str) -> Exception:
    return ValueError(f"{MARK} {sqlstate}: {message}")


def _quoted(text: str) -> str:
    return exp.Literal.string(text).sql(dialect="postgres")


def _names(items: list[str]) -> str:
    return _text_list([_quoted(i) for i in items])


def _text_list(items: list[str]) -> str:
    return "CAST(ARRAY[" + ", ".join(items) + "] AS TEXT[])" if items else "CAST(ARRAY[] AS TEXT[])"


def _is_name_start(char: str) -> bool:
    return char in "_:" or unicodedata.category(char)[0] == "L" or char.isalpha()


def _is_name_char(char: str) -> bool:
    return (
        _is_name_start(char)
        or char in ".-·"
        or unicodedata.category(char) in ("Nd", "Mn", "Mc", "Lm")
    )


def xml_name(ident: str, *, fully_escaped: bool = False, escape_period: bool = False) -> str:
    """map_sql_identifier_to_xml_name."""
    out: list[str] = []
    for position, char in enumerate(ident):
        if char == ":" and (position == 0 or fully_escaped):
            out.append("_x003A_")
        elif char == "_" and ident[position + 1 : position + 2] == "x":
            out.append("_x005F_")
        elif fully_escaped and position == 0 and ident[:3].lower() == "xml":
            out.append("_x0078_" if char == "x" else "_x0058_")
        elif escape_period and char == ".":
            out.append("_x002E_")
        elif _is_name_start(char) if position == 0 else _is_name_char(char):
            out.append(char)
        else:
            out.append(f"_x{ord(char):04X}_")
    return "".join(out)


def is_xml(node: exp.Expr) -> bool:
    """Whether an expression is an xml value, as far as the statement says."""
    if isinstance(node, exp.Paren):
        return is_xml(node.this)
    if isinstance(node, exp.Cast):
        return node.to.this == _XML_TYPE
    if isinstance(node, XML_VALUES):
        return True
    if isinstance(node, exp.Dot):
        return is_xml(node.expression)
    return isinstance(node, exp.Anonymous) and node.name.lower() in RETURNS_XML


def _as_xml(node: exp.Expr, sql: Callable[[exp.Expr], str]) -> str:
    """An argument PostgreSQL coerces to xml: an unknown-type literal is read as xml."""
    written = sql(node)
    return f"PROVISA_XML_IN({written})" if isinstance(node, exp.Literal) else written


def _content(node: exp.Expr, sql: Callable[[exp.Expr], str]) -> str:
    written = sql(node)
    return f"CAST({written} AS TEXT)" if is_xml(node) else f"PROVISA_XML_TEXT({written})"


def _attribute_of(node: exp.Expr, sql: Callable[[exp.Expr], str]) -> str:
    written = sql(node)
    return f"CAST({written} AS TEXT)" if is_xml(node) else f"PROVISA_XML_ATTRIBUTE({written})"


def _named(item: exp.Expr, unnamed: str) -> tuple[exp.Expr, str]:
    """A forest element's or an attribute's value and its name: an alias, or a column's own."""
    if isinstance(item, exp.Alias):
        return item.this, item.alias
    if isinstance(item, exp.Column):
        return item, item.name
    raise _refused("42601", unnamed)


def _element(node: exp.XMLElement, sql: Callable[[exp.Expr], str]) -> str:
    name = xml_name(node.this.name if isinstance(node.this, exp.Expr) else str(node.this))
    names: list[str] = []
    values: list[str] = []
    contents: list[str] = []
    for argument in node.expressions:
        if isinstance(argument, exp.Anonymous) and argument.name.lower() == "xmlattributes":
            for item in argument.expressions:
                value, attribute = _named(
                    item, "unnamed XML attribute value must be a column reference"
                )
                attribute = xml_name(attribute)
                if attribute in names:
                    raise _refused(
                        "42601", f'XML attribute name "{attribute}" appears more than once'
                    )
                names.append(attribute)
                values.append(_attribute_of(value, sql))
        else:
            contents.append(_content(argument, sql))
    return (
        f"PROVISA_XMLELEMENT({_quoted(name)}, {_names(names)}, {_text_list(values)}, "
        f"{_text_list(contents)})"
    )


def _forest(args: list[exp.Expr], sql: Callable[[exp.Expr], str]) -> str:
    names: list[str] = []
    values: list[str] = []
    for item in args:
        value, name = _named(item, "unnamed XML element value must be a column reference")
        names.append(xml_name(name))
        values.append(_content(value, sql))
    return f"PROVISA_XMLFOREST({_names(names)}, {_text_list(values)})"


def _shown(kind: exp.DataType) -> str:
    """A type as PostgreSQL's messages name it."""
    written = kind.sql(dialect="postgres")
    return {"INT": "integer", "BIGINT": "bigint", "SMALLINT": "smallint"}.get(
        written, written.lower()
    )


def _serialize(node: XmlSerialize, sql: Callable[[exp.Expr], str]) -> str:
    to: exp.DataType = node.args["to"]
    if not to.is_type(
        exp.DataType.Type.TEXT,
        exp.DataType.Type.VARCHAR,
        exp.DataType.Type.CHAR,
        exp.DataType.Type.BPCHAR,
    ):
        raise _refused(
            "42846",
            f"cannot cast XMLSERIALIZE result to {_shown(to)}",
        )
    document = "TRUE" if node.args["document"].this else "FALSE"
    indent = "TRUE" if node.args["indent"].this else "FALSE"
    written = f"PROVISA_XMLSERIALIZE({sql(node.this)}, {document}, {indent})"
    sizes = to.expressions
    if sizes and to.is_type(exp.DataType.Type.VARCHAR):
        size = int(sizes[0].name)
        long = raising("22001", _quoted(f"value too long for type character varying({size})"))
        written = f"CASE WHEN LENGTH({written}) > {size} THEN {long} ELSE {written} END"
    return f"CAST({written} AS {to.sql(dialect='postgres')})"


def _agg(args: list[exp.Expr], sql: Callable[[exp.Expr], str]) -> str:
    value = args[0]
    return f"PROVISA_XMLAGG(ARRAY_AGG({sql(value)}))"


def _column_definition(column: exp.ColumnDef) -> tuple[str | None, exp.Expr | None, bool]:
    """A column's PATH, DEFAULT and NOT NULL."""
    path = default = None
    not_null = False
    for constraint in column.args.get("constraints") or ():
        kind = constraint.kind
        if isinstance(kind, exp.PathColumnConstraint):
            path = kind.this.name
        elif isinstance(kind, exp.DefaultColumnConstraint):
            default = kind.this
        elif isinstance(kind, exp.NotNullColumnConstraint):
            not_null = True
    return path, default, not_null


def _column_read(  # noqa: PLR0913 -- a column's six facts, each from its own place
    index: int,
    name: str,
    kind: exp.DataType,
    default: exp.Expr | None,
    not_null: bool,
    sql: Callable,
) -> str:
    """One XMLTABLE column from the row's text: cast (PostgreSQL's input function, with its
    error), its default where it read nothing, refused where it may not be NULL."""
    cell = f"_r[{index}]"
    as_type = kind.sql(dialect="postgres")
    if kind.is_type(_XML_TYPE) or kind.is_type(exp.DataType.Type.TEXT):
        value = cell if kind.is_type(_XML_TYPE) else f"CAST({cell} AS TEXT)"
    else:
        invalid = raising(
            "22P02",
            f"{_quoted(f'invalid input syntax for type {_shown(kind)}: ' + chr(34))} "
            f"|| {cell} || '\"'",
        )
        # A numeric of no scale stays its text, as PostgreSQL's input keeps its digits.
        unsized = kind.is_type(exp.DataType.Type.DECIMAL) and not kind.expressions
        checked = "DOUBLE PRECISION" if unsized else as_type
        cast = cell if unsized else f"CAST({cell} AS {as_type})"
        value = (
            f"CASE WHEN {cell} IS NOT NULL AND TRY_CAST({cell} AS {checked}) IS NULL "
            f"THEN {invalid} ELSE {cast} END"
        )
    if default is not None:
        value = f"COALESCE({value}, CAST({sql(default)} AS {as_type}))"
    if not_null:
        missing = raising("22004", _quoted(f'null is not allowed in column "{name}"'))
        value = f"CASE WHEN ({value}) IS NULL THEN {missing} ELSE {value} END"
    # This runs after governance: a name the statement chose must stay a name.
    written_name = exp.to_identifier(name, quoted=True).sql(dialect="postgres")
    return f"{value} AS {written_name}"


def _table_rows(node: exp.XMLTable, sql: Callable[[exp.Expr], str]) -> str:
    """XMLTABLE as a subquery: the rows' texts, from provisa_xmltable, then each column's read."""
    spaces = [
        (space.this.alias, space.this.this.name)
        for space in node.args.get("namespaces") or ()
        if isinstance(space.this, exp.Alias)
    ]
    described: list[dict] = []
    reads: list[str] = []
    for index, column in enumerate(node.args["columns"], 1):
        name = column.name
        ordinality = bool(column.args.get("ordinality"))
        kind = exp.DataType.build("INT") if ordinality else column.args["kind"]
        path, default, not_null = _column_definition(column)
        described.append(
            {
                "path": name if path is None else path,
                "ordinality": ordinality,
                "xml": kind.is_type(_XML_TYPE),
                "numeric": kind.is_type(*exp.DataType.NUMERIC_TYPES),
            }
        )
        reads.append(_column_read(index, name, kind, default, not_null, sql))
    document = sql(node.args["passing"][0])
    rows = (
        f"PROVISA_XMLTABLE({document}, {sql(node.this)}, {_quoted(json.dumps(spaces))}, "
        f"{_quoted(json.dumps(described))})"
    )
    return f"(SELECT {', '.join(reads)} FROM (SELECT UNNEST({rows}) AS _r) AS _rows)"


def _table(node: exp.Expr, sql: Callable[[exp.Expr], str]) -> exp.Expr | None:
    """``FROM XMLTABLE(...)``, joined or lateral, as a derived table of that name."""
    table = node.this
    if not isinstance(table, exp.XMLTable):
        return None
    alias = node.args.get("alias")
    written = _table_rows(table, sql)
    tree = sqlglot.parse_one(f"SELECT * FROM {written} AS x", read="postgres")
    derived = tree.args["from_"].this
    derived.set("alias", alias.copy() if alias is not None else exp.TableAlias(this="xmltable"))
    if isinstance(node, exp.Lateral):
        return exp.Lateral(this=derived.this, alias=derived.args["alias"])
    return derived


def _exists(node: XmlExists, sql: Callable[[exp.Expr], str]) -> str:
    return f"PROVISA_XPATH_EXISTS({sql(node.this)}, {_as_xml(node.expression, sql)}, '[]')"


def _call(name: str, args: list[exp.Expr], sql: Callable[[exp.Expr], str]) -> str | None:
    if name == "xml" and len(args) == 1:
        return f"PROVISA_XML_IN({sql(args[0])})"
    if name == "xmlforest":
        return _forest(args, sql)
    if name == "xmlconcat":
        return f"PROVISA_XMLCONCAT({_text_list([_as_xml(a, sql) for a in args])})"
    if name == "xmlagg" and len(args) == 1:
        return _agg(args, sql)
    if name == "pg_typeof" and len(args) == 1:
        return _typeof(args[0])
    return None


def _typeof(value: exp.Expr) -> str | None:
    """pg_typeof of an xml value: the engine holds it as text, and would say so."""
    if is_xml(value):
        return "CAST('xml' AS REGTYPE)"
    called = value.expression if isinstance(value, exp.Dot) else value
    is_array = isinstance(called, exp.Anonymous) and called.name.lower() == "xpath"
    return "CAST('xml[]' AS REGTYPE)" if is_array else None


def _refused_aggregate(node: exp.Expr) -> None:
    """xml has no ordering, so max and min do not exist for it."""
    if isinstance(node, (exp.Max, exp.Min)) and is_xml(node.this):
        name = "max" if isinstance(node, exp.Max) else "min"
        raise _refused("42883", f"function {name}(xml) does not exist")


#: PostgreSQL's xml has no comparison: each operator a statement may try, as its error names it.
_COMPARED = {exp.EQ: "=", exp.NEQ: "<>", exp.LT: "<", exp.LTE: "<=", exp.GT: ">", exp.GTE: ">="}


def _is_xml_array(kind: exp.DataType) -> bool:
    return kind.is_type(exp.DataType.Type.ARRAY) and any(
        isinstance(element, exp.DataType) and element.this == _XML_TYPE
        for element in kind.expressions
    )


def _syntax(node: exp.Expr, sql: Callable[[exp.Expr], str]) -> str | None:  # noqa: PLR0911
    if isinstance(node, exp.Cast) and node.to.this == _XML_TYPE:
        return f"PROVISA_XML_IN({sql(node.this)})"
    if isinstance(node, exp.Cast) and _is_xml_array(node.to):
        return f"PROVISA_XML_ARRAY_IN({sql(node.this)})"
    operator = _COMPARED.get(type(node))
    if operator is not None and is_xml(node.this) and is_xml(node.expression):
        raise _refused("42883", f"operator does not exist: xml {operator} xml")
    if isinstance(node, exp.Is) and isinstance(node.expression, exp.Column):
        if node.expression.name.upper() == "DOCUMENT":
            return f"PROVISA_XML_IS_DOCUMENT({sql(node.this)})"
        return None
    if isinstance(node, exp.XMLElement):
        return _element(node, sql)
    if isinstance(node, XmlParse):
        reader = "PROVISA_XMLPARSE_DOCUMENT" if node.args["document"].this else "PROVISA_XML_IN"
        return f"{reader}({sql(node.this)})"
    if isinstance(node, XmlPi):
        target = xml_name(node.this.name)
        value = node.expression
        given = "FALSE" if value is None else "TRUE"
        written = "NULL" if value is None else sql(value)
        return f"PROVISA_XMLPI({_quoted(target)}, {written}, {given})"
    if isinstance(node, XmlRoot):
        version = node.args.get("version")
        standalone = node.args.get("standalone")
        return (
            f"PROVISA_XMLROOT({sql(node.this)}, {'NULL' if version is None else sql(version)}, "
            f"{'NULL' if standalone is None else _quoted(standalone)})"
        )
    if isinstance(node, XmlSerialize):
        return _serialize(node, sql)
    if isinstance(node, XmlExists):
        return _exists(node, sql)
    return None


def rewritten(node: exp.Expr, sql: Callable[[exp.Expr], str]) -> exp.Expr | None:
    """The DuckDB engine's form of an XML node, in PostgreSQL's dialect; None for any other."""
    if isinstance(node, (exp.Table, exp.Lateral)):
        return _table(node, sql)
    _refused_aggregate(node)
    written = _syntax(node, sql)
    if written is None and isinstance(node, exp.Anonymous):
        written = _call(node.name.lower(), node.expressions, sql)
    return None if written is None else sqlglot.parse_one(written, read="postgres")
