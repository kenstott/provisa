# Copyright (c) 2026 Kenneth Stott
# Canary: b6f20d71-94ce-4e3a-8a15-2c7d0e9f4b36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's XML syntax in the semantic SQL's dialect (REQ-2167, REQ-2171): sqlglot does not
read XMLPARSE, XMLPI, XMLROOT, XMLSERIALIZE or XMLEXISTS, so a statement holding one would never
reach governance. Each reads here as a node of its own and writes back as PostgreSQL's syntax, so
a statement crosses governance's rewrites with its columns visible, and a PostgreSQL source gets
what the client wrote. The DuckDB engine's form is ``provisa.transpiler.pg_xml``."""

# Requirements: REQ-2167

from __future__ import annotations

from typing import Any

from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sqlglot.tokens import TokenType


class XmlParse(exp.Expression, exp.Func):
    """XMLPARSE({DOCUMENT | CONTENT} value [{PRESERVE | STRIP} WHITESPACE])."""

    _sql_names = ["XMLPARSE"]
    arg_types = {"this": True, "document": True, "whitespace": False}


class XmlPi(exp.Expression, exp.Func):
    """XMLPI(NAME target [, value])."""

    _sql_names = ["XMLPI"]
    arg_types = {"this": True, "expression": False}


class XmlRoot(exp.Expression, exp.Func):
    """XMLROOT(value, VERSION {expression | NO VALUE} [, STANDALONE {YES | NO | NO VALUE}])."""

    _sql_names = ["XMLROOT"]
    arg_types = {"this": True, "version": False, "standalone": False}


class XmlSerialize(exp.Expression, exp.Func):
    """XMLSERIALIZE({DOCUMENT | CONTENT} value AS type [[NO] INDENT])."""

    _sql_names = ["XMLSERIALIZE"]
    arg_types = {"this": True, "document": True, "to": True, "indent": False}


class XmlExists(exp.Expression, exp.Func):
    """XMLEXISTS(path PASSING [BY {REF | VALUE}] value [BY {REF | VALUE}])."""

    _sql_names = ["XMLEXISTS"]
    arg_types = {"this": True, "expression": True}


#: The nodes that are an xml value.
XML_VALUES = (XmlParse, XmlPi, XmlRoot, exp.XMLElement)
#: SQL/XML's mappings of tables: each reads what its text argument names, so no statement holding
#: one can be governed by reading the statement (provisa.compiler.xml_mapping answers them first).
MAPPINGS = frozenset(
    {
        "cursor_to_xml",
        "cursor_to_xmlschema",
        "database_to_xml",
        "database_to_xml_and_xmlschema",
        "database_to_xmlschema",
        "query_to_xml",
        "query_to_xml_and_xmlschema",
        "query_to_xmlschema",
        "schema_to_xml",
        "schema_to_xml_and_xmlschema",
        "schema_to_xmlschema",
        "table_to_xml",
        "table_to_xml_and_xmlschema",
        "table_to_xmlschema",
    }
)
#: The calls that return one: pg_proc's, and those only PostgreSQL's parser knows.
RETURNS_XML = frozenset(
    {
        "xml",
        "xmlagg",
        "xmlcomment",
        "xmlconcat",
        "xmlconcat2",
        "xmlforest",
        "xmltext",
        "provisa_xml",
        *MAPPINGS,
    }
)


def _by(parser: Any) -> None:
    if parser._match_text_seq("BY"):
        if not (parser._match_text_seq("REF") or parser._match_text_seq("VALUE")):
            parser.raise_error("Expected REF or VALUE")


def _document(parser: Any) -> bool:
    if parser._match_text_seq("DOCUMENT"):
        return True
    if parser._match_text_seq("CONTENT"):
        return False
    parser.raise_error("Expected DOCUMENT or CONTENT")
    return False


def _name(parser: Any) -> exp.Expr:
    if not parser._match_text_seq("NAME"):
        parser.raise_error("Expected NAME")
    return parser._parse_id_var(any_token=True)


def _parse_xmlparse(self: Any) -> exp.Expr:
    document = _document(self)
    value = self._parse_assignment()
    whitespace = None
    if self._match_text_seq("PRESERVE", "WHITESPACE"):
        whitespace = "PRESERVE"
    elif self._match_text_seq("STRIP", "WHITESPACE"):
        whitespace = "STRIP"
    return self.expression(
        XmlParse(this=value, document=exp.convert(document), whitespace=whitespace)
    )


def _parse_xmlpi(self: Any) -> exp.Expr:
    target = _name(self)
    value = self._parse_assignment() if self._match(TokenType.COMMA) else None
    return self.expression(XmlPi(this=target, expression=value))


def _parse_xmlroot(self: Any) -> exp.Expr:
    value = self._parse_assignment()
    if not self._match(TokenType.COMMA) or not self._match_text_seq("VERSION"):
        self.raise_error("Expected VERSION")
    version = None if self._match_text_seq("NO", "VALUE") else self._parse_assignment()
    standalone = None
    if self._match(TokenType.COMMA):
        if not self._match_text_seq("STANDALONE"):
            self.raise_error("Expected STANDALONE")
        if self._match_text_seq("NO", "VALUE"):
            standalone = "no value"
        elif self._match_text_seq("YES"):
            standalone = "yes"
        elif self._match_text_seq("NO"):
            standalone = "no"
        else:
            self.raise_error("Expected YES, NO or NO VALUE")
    return self.expression(XmlRoot(this=value, version=version, standalone=standalone))


def _parse_xmlserialize(self: Any) -> exp.Expr:
    document = _document(self)
    value = self._parse_assignment()
    if not self._match(TokenType.ALIAS):
        self.raise_error("Expected AS")
    to = self._parse_types()
    indent = False
    if self._match_text_seq("NO", "INDENT"):
        indent = False
    elif self._match_text_seq("INDENT"):
        indent = True
    return self.expression(
        XmlSerialize(this=value, document=exp.convert(document), to=to, indent=exp.convert(indent))
    )


def _parse_xmlexists(self: Any) -> exp.Expr:
    path = self._parse_bitwise()
    if self._match(TokenType.COMMA):  # pg_proc's function, called as one
        return self.expression(XmlExists(this=path, expression=self._parse_bitwise()))
    if not self._match_text_seq("PASSING"):
        self.raise_error("Expected PASSING")
    _by(self)
    value = self._parse_bitwise()
    _by(self)
    return self.expression(XmlExists(this=path, expression=value))


def _kind(document: exp.Expr) -> str:
    return "DOCUMENT" if document.this else "CONTENT"


def _xmlparse_sql(self: Any, e: XmlParse) -> str:
    whitespace = f" {e.args['whitespace']} WHITESPACE" if e.args.get("whitespace") else ""
    return f"XMLPARSE({_kind(e.args['document'])} {self.sql(e, 'this')}{whitespace})"


def _xmlpi_sql(self: Any, e: XmlPi) -> str:
    value = self.sql(e, "expression")
    return f"XMLPI(NAME {self.sql(e, 'this')}{', ' + value if value else ''})"


def _xmlroot_sql(self: Any, e: XmlRoot) -> str:
    version = self.sql(e, "version") or "NO VALUE"
    standalone = e.args.get("standalone")
    said = f", STANDALONE {standalone.upper()}" if standalone else ""
    return f"XMLROOT({self.sql(e, 'this')}, VERSION {version}{said})"


def _xmlserialize_sql(self: Any, e: XmlSerialize) -> str:
    indent = " INDENT" if e.args["indent"].this else ""
    value = self.sql(e, "this")
    return f"XMLSERIALIZE({_kind(e.args['document'])} {value} AS {self.sql(e, 'to')}{indent})"


def _c_expr(self: Any, node: exp.Expr) -> str:
    """An expression where PostgreSQL's grammar takes a c_expr: in parentheses, as a cast needs."""
    written = self.sql(node)
    return written if isinstance(node, exp.Paren) else f"({written})"


def _xmlexists_sql(self: Any, e: XmlExists) -> str:
    return f"XMLEXISTS({self.sql(e, 'this')} PASSING {_c_expr(self, e.expression)})"


def _xmltable_column_sql(self: Any, column: exp.ColumnDef) -> str:
    name = self.sql(column, "this")
    if column.args.get("ordinality"):
        return f"{name} FOR ORDINALITY"
    return self.sql(column)


def _xmltable_sql(self: Any, e: exp.XMLTable) -> str:
    spaces = self.expressions(e, "namespaces", flat=True)
    prefix = f"XMLNAMESPACES({spaces}), " if spaces else ""
    passing = ", ".join(_c_expr(self, p) for p in e.args["passing"])
    columns = ", ".join(_xmltable_column_sql(self, c) for c in e.args["columns"])
    return f"XMLTABLE({prefix}{self.sql(e, 'this')} PASSING {passing} COLUMNS {columns})"


def install() -> None:
    """Teach PostgreSQL's dialect the syntax. The FUNCTION_PARSERS table is replaced rather than
    changed, so what the parser's own class declares stays as it was."""
    # sqlglot's own parser sets it on an XMLTABLE column; its class does not declare it.
    exp.ColumnDef.arg_types = {**exp.ColumnDef.arg_types, "ordinality": False}
    Postgres.Parser.FUNCTION_PARSERS = {
        **Postgres.Parser.FUNCTION_PARSERS,
        "XMLPARSE": _parse_xmlparse,
        "XMLPI": _parse_xmlpi,
        "XMLROOT": _parse_xmlroot,
        "XMLSERIALIZE": _parse_xmlserialize,
        "XMLEXISTS": _parse_xmlexists,
    }
    Postgres.Generator.TRANSFORMS[XmlParse] = _xmlparse_sql
    Postgres.Generator.TRANSFORMS[XmlPi] = _xmlpi_sql
    Postgres.Generator.TRANSFORMS[XmlRoot] = _xmlroot_sql
    Postgres.Generator.TRANSFORMS[XmlSerialize] = _xmlserialize_sql
    Postgres.Generator.TRANSFORMS[XmlExists] = _xmlexists_sql
    Postgres.Generator.TRANSFORMS[exp.XMLTable] = _xmltable_sql


install()
