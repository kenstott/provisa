# Copyright (c) 2026 Kenneth Stott
# Canary: e12b8f46-07da-4c39-b5e1-96ac3d7f2810
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL/XML's mapping of tables to XML (REQ-2167): query_to_xml, table_to_xml, schema_to_xml,
database_to_xml, cursor_to_xml and their xmlschema forms, by src/backend/utils/adt/xml.c.

These functions read data, so they read it as the session asking does: every read of theirs goes
through ``Reads``, which a surface implements with its own governed statements (masked,
row-filtered and audited as the client's own), and what a schema or a database holds is what that
role's catalog lists. Nothing here reaches a source or an engine, and a surface that offers no
``Reads`` does not serve them: governance refuses them (sql_validator V006). A call is answered before the statement that holds it is
governed (``fold``), and the statement then carries the document as a value: so its arguments
must be constants, the one thing here PostgreSQL does not ask of them (see ``_constant``)."""

# Requirements: REQ-2167

from __future__ import annotations

import base64
import dataclasses
import datetime
import decimal
import functools
import re
import sys
from dataclasses import dataclass
from typing import Any, Callable, Protocol, Sequence

import sqlglot
import sqlglot.expressions as exp
from sqlglot.errors import SqlglotError

from provisa.core.pg_error import MARK
from provisa.federation.duckdb_pg_scalars import parse_ident
from provisa.federation.duckdb_pg_xml import _escape, _float_out, xml_out
from provisa.transpiler.pg_xml import xml_name

NAMESPACE_XSD = "http://www.w3.org/2001/XMLSchema"
NAMESPACE_XSI = "http://www.w3.org/2001/XMLSchema-instance"

_BOOL, _BYTEA, _TEXT, _INT2, _INT4, _INT8, _FLOAT4, _FLOAT8 = 16, 17, 25, 21, 23, 20, 700, 701
_BPCHAR, _VARCHAR, _DATE, _TIME, _TIMETZ = 1042, 1043, 1082, 1083, 1266
_TIMESTAMP, _TIMESTAMPTZ, _NUMERIC, _XML = 1114, 1184, 1700, 142

#: The names map_sql_type_to_xml_name has for a type of no typmod.
_XML_TYPE_NAMES = {
    _BPCHAR: "CHAR",
    _VARCHAR: "VARCHAR",
    _NUMERIC: "NUMERIC",
    _INT4: "INTEGER",
    _INT2: "SMALLINT",
    _INT8: "BIGINT",
    _FLOAT4: "REAL",
    _FLOAT8: "DOUBLE",
    _BOOL: "BOOLEAN",
    _TIME: "TIME",
    _TIMETZ: "TIME_WTZ",
    _TIMESTAMP: "TIMESTAMP",
    _TIMESTAMPTZ: "TIMESTAMP_WTZ",
    _DATE: "DATE",
    _XML: "XML",
}

_SEMI_DIGITS = r"\p{Nd}"
_CLOCK = rf"{_SEMI_DIGITS}{{2}}:{_SEMI_DIGITS}{{2}}:{_SEMI_DIGITS}{{2}}"
_DAY = rf"{_SEMI_DIGITS}{{4}}-{_SEMI_DIGITS}{{2}}-{_SEMI_DIGITS}{{2}}"
_FRACTION = rf"(.{_SEMI_DIGITS}+)?"
_ZONE = rf"(\+|-){_SEMI_DIGITS}{{2}}:{_SEMI_DIGITS}{{2}}"


@dataclass(frozen=True)
class Column:
    """A result column: its name, its type's OID, and the writer of its text form."""

    name: str
    oid: int
    text: Callable[[Any], str]


class Reads(Protocol):
    """What these functions read, through the session asking."""

    @property
    def database(self) -> str:
        """The database the session is connected to."""
        ...

    def rows(self, sql: str) -> tuple[list[Column], list[Any]]:
        """The statement's rows, read as the session reads any statement."""
        ...

    def shape(self, sql: str) -> list[Column]:
        """The statement's columns, described as the session describes any statement."""
        ...

    def cursor_rows(self, name: str, count: int) -> tuple[list[Column], list[Any]]:
        """The next ``count`` rows of the session's cursor."""
        ...

    def cursor_shape(self, name: str) -> list[Column]:
        """The columns of the session's cursor."""
        ...

    def catalog(self, sql: str, parameters: list[Any]) -> list[tuple]:
        """A read of the role's catalog."""
        ...


def _refused(sqlstate: str, message: str) -> Exception:
    return ValueError(f"{MARK} {sqlstate}: {message}")


# --- a value --------------------------------------------------------------------------------------


def _timestamp(value: datetime.datetime) -> str:
    written = value.isoformat()
    if value.microsecond:
        head, zone = re.fullmatch(r"([^+-]*-\d\d-\d\dT[^+-]*)(.*)", written).groups()  # type: ignore[union-attr]
        written = head.rstrip("0") + zone
    return written


def _value(value: Any, column: Column) -> str:  # noqa: PLR0911 -- one return per kind of value
    """map_sql_value_to_xml_value."""
    if isinstance(value, (list, tuple)):
        return "".join(f"<element>{_element(item)}</element>" for item in value if item is not None)
    if column.oid == _XML:
        return xml_out(str(value))
    return _element(value, column)


def _element(value: Any, column: Column | None = None) -> str:  # noqa: PLR0911
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime.datetime):
        return _timestamp(value)
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode()
    if isinstance(value, float):
        return _float_out(value)
    if isinstance(value, decimal.Decimal):
        return str(value)
    return _escape(column.text(value) if column is not None else str(value))


def _row(  # noqa: PLR0913 -- SPI_sql_row_to_xmlelement's arguments
    values: Sequence[Any],
    columns: list[Column],
    table: str | None,
    nulls: bool,
    forest: bool,
    namespace: str,
    top_level: bool,
) -> str:
    """SPI_sql_row_to_xmlelement."""
    name = xml_name(table, fully_escaped=True) if table else "row" if forest else "table"
    out = [_root_start(name, None, namespace, top_level) if forest else "<row>\n"]
    for value, column in zip(values, columns):
        element = xml_name(column.name, fully_escaped=True)
        if value is None:
            if nulls:
                out.append(f'  <{element} xsi:nil="true"/>\n')
        else:
            out.append(f"  <{element}>{_value(value, column)}</{element}>\n")
    out.append(f"</{name}>\n\n" if forest else "</row>\n\n")
    return "".join(out)


def _root_start(name: str, schema: str | None, namespace: str, top_level: bool) -> str:
    """xmldata_root_element_start."""
    out = [f"<{name}"]
    if top_level:
        out.append(f' xmlns:xsi="{NAMESPACE_XSI}"')
        if namespace:
            out.append(f' xmlns="{namespace}"')
    if schema is not None:
        if namespace:
            out.append(f' xsi:schemaLocation="{namespace} #"')
        else:
            out.append(' xsi:noNamespaceSchemaLocation="#"')
    out.append(">\n")
    return "".join(out)


def _read(  # noqa: PLR0913 -- query_to_xml_internal's arguments
    columns: list[Column],
    rows: list[Sequence[Any]],
    table: str | None,
    schema: str | None,
    nulls: bool,
    forest: bool,
    namespace: str,
    top_level: bool,
) -> str:
    """query_to_xml_internal, over the rows it read."""
    name = xml_name(table, fully_escaped=True) if table else "table"
    out: list[str] = []
    if not forest:
        out.append(_root_start(name, schema, namespace, top_level) + "\n")
    if schema is not None:
        out.append(f"{schema}\n\n")
    for row in rows:
        out.append(_row(row, columns, table, nulls, forest, namespace, top_level))
    if not forest:
        out.append(f"</{name}>\n")
    return "".join(out)


# --- XML Schema -----------------------------------------------------------------------------------


def _multipart(*parts: str | None) -> str:
    """map_multipart_sql_identifier_to_xml_name."""
    return ".".join(
        xml_name(part, fully_escaped=True, escape_period=True) for part in parts if part is not None
    )


def _type_name(oid: int, database: str) -> str:
    """map_sql_type_to_xml_name of a column, which is asked with no typmod."""
    known = _XML_TYPE_NAMES.get(oid)
    if known is not None:
        return known
    from provisa.pgwire.catalog_pg17 import TYPES

    entry = TYPES[oid]
    return _multipart("UDT", database, entry[0], entry[1])


def _restriction(oid: int) -> str:  # noqa: PLR0911 -- a mapping, one case per type
    """The body of map_sql_type_to_xmlschema_type's simple type, of no typmod."""
    if oid in (_BPCHAR, _VARCHAR, _TEXT):
        return '  <xsd:restriction base="xsd:string">\n  </xsd:restriction>\n'
    if oid == _BYTEA:
        return '  <xsd:restriction base="xsd:base64Binary">\n  </xsd:restriction>\n'
    bounded = {
        _INT2: ("short", 32767, -32768),
        _INT4: ("int", 2147483647, -2147483648),
        _INT8: ("long", 9223372036854775807, -9223372036854775808),
    }.get(oid)
    if bounded is not None:
        base, high, low = bounded
        return (
            f'  <xsd:restriction base="xsd:{base}">\n'
            f'    <xsd:maxInclusive value="{high}"/>\n'
            f'    <xsd:minInclusive value="{low}"/>\n'
            "  </xsd:restriction>\n"
        )
    empty = {_FLOAT4: "float", _FLOAT8: "double", _BOOL: "boolean"}.get(oid)
    if empty is not None:
        return f'  <xsd:restriction base="xsd:{empty}"></xsd:restriction>\n'
    patterns = {
        _TIME: ("time", f"{_CLOCK}{_FRACTION}"),
        _TIMETZ: ("time", f"{_CLOCK}{_FRACTION}{_ZONE}"),
        _TIMESTAMP: ("dateTime", f"{_DAY}T{_CLOCK}{_FRACTION}"),
        _TIMESTAMPTZ: ("dateTime", f"{_DAY}T{_CLOCK}{_FRACTION}{_ZONE}"),
        _DATE: ("date", _DAY),
    }.get(oid)
    if patterns is not None:
        base, pattern = patterns
        return (
            f'  <xsd:restriction base="xsd:{base}">\n'
            f'    <xsd:pattern value="{pattern}"/>\n'
            "  </xsd:restriction>\n"
        )
    return ""


def _schema_type(oid: int, database: str) -> str:
    """map_sql_type_to_xmlschema_type."""
    if oid == _XML:
        return (
            '<xsd:complexType mixed="true">\n'
            "  <xsd:sequence>\n"
            '    <xsd:any name="element" minOccurs="0" maxOccurs="unbounded" '
            'processContents="skip"/>\n'
            "  </xsd:sequence>\n"
            "</xsd:complexType>\n"
        )
    name = _type_name(oid, database)
    return f'<xsd:simpleType name="{name}">\n{_restriction(oid)}</xsd:simpleType>\n'


def _schema_types(tables: Sequence[list[Column]], database: str) -> str:
    """map_sql_typecoll_to_xmlschema_types: each type used once, as it is first met."""
    unique = dict.fromkeys(column.oid for columns in tables for column in columns)
    return "".join(f"{_schema_type(oid, database)}\n" for oid in unique)


def _xsd_start(namespace: str) -> str:
    target = (
        f'\n    targetNamespace="{namespace}"\n    elementFormDefault="qualified"'
        if namespace
        else ""
    )
    return f'<xsd:schema\n    xmlns:xsd="{NAMESPACE_XSD}"{target}>\n\n'


def _table_schema(  # noqa: PLR0913 -- map_sql_table_to_xmlschema's arguments
    columns: list[Column],
    table: tuple[str, str] | None,
    nulls: bool,
    forest: bool,
    namespace: str,
    database: str,
) -> str:
    """map_sql_table_to_xmlschema; ``table`` its schema and name, or None for a query's."""
    if table is not None:
        schema, name = table
        element = xml_name(name, fully_escaped=True)
        table_type = _multipart("TableType", database, schema, name)
        row_type = _multipart("RowType", database, schema, name)
    else:
        element = "row" if forest else "table"
        table_type, row_type = "TableType", "RowType"
    out = [_xsd_start(namespace), _schema_types([columns], database)]
    out.append(f'<xsd:complexType name="{row_type}">\n  <xsd:sequence>\n')
    absent = ' nillable="true"' if nulls else ' minOccurs="0"'
    for column in columns:
        out.append(
            f'    <xsd:element name="{xml_name(column.name, fully_escaped=True)}" '
            f'type="{_type_name(column.oid, database)}"{absent}></xsd:element>\n'
        )
    out.append("  </xsd:sequence>\n</xsd:complexType>\n\n")
    if not forest:
        out.append(
            f'<xsd:complexType name="{table_type}">\n  <xsd:sequence>\n'
            f'    <xsd:element name="row" type="{row_type}" minOccurs="0" '
            'maxOccurs="unbounded"/>\n'
            "  </xsd:sequence>\n</xsd:complexType>\n\n"
            f'<xsd:element name="{element}" type="{table_type}"/>\n\n'
        )
    else:
        out.append(f'<xsd:element name="{element}" type="{row_type}"/>\n\n')
    out.append("</xsd:schema>")
    return "".join(out)


def _schema_elements(schema: str, tables: list[str], forest: bool, database: str) -> str:
    """map_sql_schema_to_xmlschema_types."""
    schema_type = _multipart("SchemaType", database, schema)
    out = [f'<xsd:complexType name="{schema_type}">\n']
    out.append("  <xsd:all>\n" if not forest else "  <xsd:sequence>\n")
    for table in tables:
        element = xml_name(table, fully_escaped=True)
        named = _multipart("RowType" if forest else "TableType", database, schema, table)
        unbounded = ' minOccurs="0" maxOccurs="unbounded"' if forest else ""
        out.append(f'    <xsd:element name="{element}" type="{named}"{unbounded}/>\n')
    out.append("  </xsd:all>\n" if not forest else "  </xsd:sequence>\n")
    out.append("</xsd:complexType>\n\n")
    named = xml_name(schema, fully_escaped=True)
    out.append(f'<xsd:element name="{named}" type="{schema_type}"/>\n\n')
    return "".join(out)


def _catalog_elements(schemas: list[str], database: str) -> str:
    """map_sql_catalog_to_xmlschema_types."""
    catalog_type = _multipart("CatalogType", database)
    out = [f'<xsd:complexType name="{catalog_type}">\n  <xsd:all>\n']
    for schema in schemas:
        element = xml_name(schema, fully_escaped=True)
        named = _multipart("SchemaType", database, schema)
        out.append(f'    <xsd:element name="{element}" type="{named}"/>\n')
    out.append("  </xsd:all>\n</xsd:complexType>\n\n")
    named = xml_name(database, fully_escaped=True)
    out.append(f'<xsd:element name="{named}" type="{catalog_type}"/>\n\n')
    return "".join(out)


# --- what the role may see ------------------------------------------------------------------------

_RELATIONS = (
    "SELECT c.relname FROM _pg_class c JOIN _pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = ? AND c.relkind IN ('r', 'm', 'v') ORDER BY c.relname"
)
_SCHEMAS = (
    "SELECT nspname FROM _pg_namespace "
    "WHERE NOT (nspname LIKE 'pg\\_%' ESCAPE '\\' OR nspname = 'information_schema') "
    "ORDER BY nspname"
)
_BY_OID = (
    "SELECT n.nspname, c.relname FROM _pg_class c JOIN _pg_namespace n "
    "ON n.oid = c.relnamespace WHERE c.oid = ?"
)
_BY_NAME = (
    "SELECT n.nspname, c.relname FROM _pg_class c JOIN _pg_namespace n "
    "ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relname = ?"
)
#: Where an unqualified relation is looked for: PostgreSQL's order for the session's search_path.
_SEARCH_PATH = ("pg_catalog", "public")


def _quoted(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _relation(reads: Reads, named: Any) -> tuple[str, str]:
    """The relation a regclass argument names, as the role's catalog lists it."""
    if isinstance(named, int):
        found = reads.catalog(_BY_OID, [named])
        if not found:
            raise _refused("42P01", f"relation with OID {named} does not exist")
        return found[0]
    text = str(named)
    parts = parse_ident(text, True)
    schemas = parts[-2:-1] or list(_SEARCH_PATH)
    for schema in schemas:
        found = reads.catalog(_BY_NAME, [schema, parts[-1]])
        if found:
            return found[0]
    raise _refused("42P01", f'relation "{text}" does not exist')


def _select(table: tuple[str, str]) -> str:
    return f"SELECT * FROM {_quoted(table[0])}.{_quoted(table[1])}"


def _tables_of(reads: Reads, schema: str) -> list[str]:
    return [row[0] for row in reads.catalog(_RELATIONS, [schema])]


def _schemas(reads: Reads) -> list[str]:
    return [row[0] for row in reads.catalog(_SCHEMAS, [])]


def _existing_schema(reads: Reads, schema: str) -> str:
    if not reads.catalog("SELECT 1 FROM _pg_namespace WHERE nspname = ?", [schema]):
        raise _refused("3F000", f'schema "{schema}" does not exist')
    return schema


# --- the functions --------------------------------------------------------------------------------

#: The kinds of statement a query may not be: PostgreSQL runs it read-only.
_WRITES = {
    exp.Insert: "INSERT",
    exp.Update: "UPDATE",
    exp.Delete: "DELETE",
    exp.Merge: "MERGE",
}


#: The statements that are no read, by the word they open with.
_COMMANDS = frozenset(
    {
        "ALTER", "ANALYZE", "CALL", "COPY", "CREATE", "DELETE", "DO", "DROP", "GRANT", "INSERT",
        "LOCK", "MERGE", "REFRESH", "REINDEX", "REVOKE", "SET", "TRUNCATE", "UPDATE", "VACUUM",
    }
)  # fmt: skip


def _read_only(query: str) -> None:
    """Refuse what is not one read: PostgreSQL's SPI_execute(query, read_only = true)."""
    if not query.strip():
        raise _refused("22000", "invalid query")
    words = query.split()
    try:
        statements = sqlglot.parse(query, read="postgres")
    except SqlglotError as exc:
        raise _refused("42601", f'syntax error at or near "{words[0]}"') from exc
    if len(statements) != 1 or statements[0] is None:
        raise _refused("0A000", "only one statement is supported in a query argument")
    tree = statements[0]
    for node in tree.walk():
        verb = _WRITES.get(type(node))
        if verb is not None:
            raise _refused("0A000", f"{verb} is not allowed in a non-volatile function")
        if isinstance(node, exp.Into):
            raise _refused("0A000", "SELECT INTO is not allowed in a non-volatile function")
    if not isinstance(tree, (exp.Query, exp.Values)):
        first = words[0].upper()
        if first not in _COMMANDS:  # sqlglot reads a bare expression; PostgreSQL's grammar does not
            raise _refused("42601", f'syntax error at or near "{words[0]}"')
        verb = " ".join(words[:2]) if first in ("CREATE", "DROP", "ALTER") else first
        raise _refused("0A000", f"{verb.upper()} is not allowed in a non-volatile function")


def _named_as_postgresql(query: str, columns: list[Column]) -> list[Column]:
    """The columns with PostgreSQL's name for an unnamed constant, which is ?column?: the
    engine's is the constant's text, and here the name is in the document."""
    tree = sqlglot.parse_one(query, read="postgres")
    selects = tree.selects if isinstance(tree, exp.Select) else []
    if len(selects) != len(columns):
        return columns
    return [
        dataclasses.replace(column, name="?column?") if isinstance(select, exp.Literal) else column
        for select, column in zip(selects, columns)
    ]


def _query_to_xml(reads: Reads, query: str, nulls: bool, forest: bool, namespace: str) -> str:
    _read_only(query)
    columns, rows = reads.rows(query)
    named = _named_as_postgresql(query, columns)
    return _read(named, rows, None, None, nulls, forest, namespace, True)


def _query_to_xmlschema(reads: Reads, query: str, nulls: bool, forest: bool, namespace: str) -> str:
    _read_only(query)
    columns = _named_as_postgresql(query, reads.shape(query))
    return _table_schema(columns, None, nulls, forest, namespace, reads.database)


def _query_to_both(reads: Reads, query: str, nulls: bool, forest: bool, namespace: str) -> str:
    schema = _query_to_xmlschema(reads, query, nulls, forest, namespace)
    columns, rows = reads.rows(query)
    named = _named_as_postgresql(query, columns)
    return _read(named, rows, None, schema, nulls, forest, namespace, True)


def _table_read(  # noqa: PLR0913 -- table_to_xml_internal's arguments
    reads: Reads,
    table: tuple[str, str],
    schema: str | None,
    nulls: bool,
    forest: bool,
    namespace: str,
    top_level: bool,
) -> str:
    columns, rows = reads.rows(_select(table))
    return _read(columns, rows, table[1], schema, nulls, forest, namespace, top_level)


def _table_to_xml(reads: Reads, named: Any, nulls: bool, forest: bool, namespace: str) -> str:
    return _table_read(reads, _relation(reads, named), None, nulls, forest, namespace, True)


def _a_table_schema(  # noqa: PLR0913 -- map_sql_table_to_xmlschema, on a named table
    reads: Reads, table: tuple[str, str], nulls: bool, forest: bool, namespace: str
) -> str:
    return _table_schema(
        reads.shape(_select(table)), table, nulls, forest, namespace, reads.database
    )


def _table_to_xmlschema(reads: Reads, named: Any, nulls: bool, forest: bool, namespace: str) -> str:
    return _a_table_schema(reads, _relation(reads, named), nulls, forest, namespace)


def _table_to_both(reads: Reads, named: Any, nulls: bool, forest: bool, namespace: str) -> str:
    table = _relation(reads, named)
    schema = _a_table_schema(reads, table, nulls, forest, namespace)
    return _table_read(reads, table, schema, nulls, forest, namespace, True)


def _schema_read(  # noqa: PLR0913 -- schema_to_xml_internal's arguments
    reads: Reads,
    name: str,
    schema: str | None,
    nulls: bool,
    forest: bool,
    namespace: str,
    top_level: bool,
) -> str:
    element = xml_name(name, fully_escaped=True)
    out = [_root_start(element, schema, namespace, top_level) + "\n"]
    if schema is not None:
        out.append(f"{schema}\n\n")
    for table in _tables_of(reads, name):
        out.append(_table_read(reads, (name, table), None, nulls, forest, namespace, False) + "\n")
    out.append(f"</{element}>\n")
    return "".join(out)


def _schema_to_xml(reads: Reads, name: str, nulls: bool, forest: bool, namespace: str) -> str:
    _existing_schema(reads, name)
    return _schema_read(reads, name, None, nulls, forest, namespace, True)


def _schema_to_xmlschema(reads: Reads, name: str, nulls: bool, forest: bool, namespace: str) -> str:
    _existing_schema(reads, name)
    tables = _tables_of(reads, name)
    shapes = [reads.shape(_select((name, table))) for table in tables]
    return "".join(
        (
            _xsd_start(namespace),
            _schema_types(shapes, reads.database),
            _schema_elements(name, tables, forest, reads.database),
            "</xsd:schema>",
        )
    )


def _schema_to_both(reads: Reads, name: str, nulls: bool, forest: bool, namespace: str) -> str:
    schema = _schema_to_xmlschema(reads, name, nulls, forest, namespace)
    return _schema_read(reads, name, schema, nulls, forest, namespace, True)


def _database_read(
    reads: Reads, schema: str | None, nulls: bool, forest: bool, namespace: str
) -> str:
    element = xml_name(reads.database, fully_escaped=True)
    out = [_root_start(element, schema, namespace, True) + "\n"]
    if schema is not None:
        out.append(f"{schema}\n\n")
    for name in _schemas(reads):
        out.append(_schema_read(reads, name, None, nulls, forest, namespace, False) + "\n")
    out.append(f"</{element}>\n")
    return "".join(out)


def _database_to_xml(reads: Reads, nulls: bool, forest: bool, namespace: str) -> str:
    return _database_read(reads, None, nulls, forest, namespace)


def _database_to_xmlschema(reads: Reads, nulls: bool, forest: bool, namespace: str) -> str:
    schemas = _schemas(reads)
    shapes = [
        reads.shape(_select((schema, table)))
        for schema in schemas
        for table in _tables_of(reads, schema)
    ]
    return "".join(
        (
            _xsd_start(namespace),
            _schema_types(shapes, reads.database),
            _catalog_elements(schemas, reads.database),
            "</xsd:schema>",
        )
    )


def _database_to_both(reads: Reads, nulls: bool, forest: bool, namespace: str) -> str:
    schema = _database_to_xmlschema(reads, nulls, forest, namespace)
    return _database_read(reads, schema, nulls, forest, namespace)


def _cursor_to_xml(  # noqa: PLR0913 -- PostgreSQL's five arguments
    reads: Reads, name: str, count: int, nulls: bool, forest: bool, namespace: str
) -> str:
    # A count of 0 is SPI's FETCH ALL, which PostgreSQL 17 answers with no rows: the cursor ends.
    columns, rows = reads.cursor_rows(name, count or sys.maxsize)
    rows = rows if count else []
    out: list[str] = []
    if not forest:
        out.append(_root_start("table", None, namespace, True) + "\n")
    for row in rows:
        out.append(_row(row, columns, None, nulls, forest, namespace, True))
    if not forest:
        out.append("</table>\n")
    return "".join(out)


def _cursor_to_xmlschema(reads: Reads, name: str, nulls: bool, forest: bool, namespace: str) -> str:
    return _table_schema(reads.cursor_shape(name), None, nulls, forest, namespace, reads.database)


_FUNCTIONS: dict[str, Callable[..., str]] = {
    "query_to_xml": _query_to_xml,
    "query_to_xmlschema": _query_to_xmlschema,
    "query_to_xml_and_xmlschema": _query_to_both,
    "table_to_xml": _table_to_xml,
    "table_to_xmlschema": _table_to_xmlschema,
    "table_to_xml_and_xmlschema": _table_to_both,
    "schema_to_xml": _schema_to_xml,
    "schema_to_xmlschema": _schema_to_xmlschema,
    "schema_to_xml_and_xmlschema": _schema_to_both,
    "database_to_xml": _database_to_xml,
    "database_to_xmlschema": _database_to_xmlschema,
    "database_to_xml_and_xmlschema": _database_to_both,
    "cursor_to_xml": _cursor_to_xml,
    "cursor_to_xmlschema": _cursor_to_xmlschema,
}

_NAMED = re.compile(
    r"\b(" + "|".join(sorted(_FUNCTIONS, key=len, reverse=True)) + r")\"?\s*\(", re.IGNORECASE
)


@functools.lru_cache(maxsize=4096)
def calls_one(sql: str) -> bool:
    """Whether a statement calls one of these functions (a string may only name one)."""
    if _NAMED.search(sql) is None:
        return False
    try:
        tree = sqlglot.parse_one(sql, read="postgres")
    except SqlglotError:
        return False
    return any(_call_of(node) is not None for node in tree.walk())


def _constant(node: exp.Expr, params: list[Any] | None) -> Any:  # noqa: PLR0911 -- one per node
    """The value of an argument the statement fixes: literals, casts of them, their
    concatenation and bound parameters. A call is answered before its statement is read, so it
    cannot be given a column: a read that depends on another row is one this statement cannot
    govern as a read of its own."""
    if isinstance(node, exp.Paren):
        return _constant(node.this, params)
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Literal):
        return node.this if node.is_string else int(node.this)
    if isinstance(node, exp.Cast):
        return _constant(node.this, params)
    if isinstance(node, exp.Parameter) and params is not None:
        return params[int(node.this.name) - 1]
    if isinstance(node, exp.DPipe):
        left, right = _constant(node.this, params), _constant(node.expression, params)
        return None if left is None or right is None else f"{left}{right}"
    raise _NotConstant


class _NotConstant(Exception):
    pass


def _as_bool(value: Any) -> Any:
    return (
        value if isinstance(value, bool) or value is None else str(value).lower() in ("t", "true")
    )


def _arguments(name: str, args: list[exp.Expr], params: list[Any] | None) -> list[Any]:
    try:
        values = [_constant(arg, params) for arg in args]
    except _NotConstant as exc:
        raise _refused(
            "0A000", f"{name}: an argument that is not a constant expression is not supported"
        ) from exc
    # In every overload the booleans are the two before the last argument.
    if len(values) >= 3:  # noqa: PLR2004
        values[-3:-1] = [_as_bool(values[-3]), _as_bool(values[-2])]
    return values


def _call_of(node: exp.Expr) -> exp.Anonymous | None:
    if isinstance(node, exp.Dot):
        node = node.expression
    if isinstance(node, exp.Anonymous) and node.name.lower() in _FUNCTIONS:
        return node
    return None


def _value_node(xml: str | None) -> exp.Expr:
    held = exp.null() if xml is None else exp.Literal.string(xml)
    return exp.Anonymous(this="PROVISA_XML", expressions=[held])


def _replaced(tree: exp.Expr, answer: Callable[[exp.Anonymous], str | None]) -> str:
    def replace(node: exp.Expr) -> exp.Expr:
        call = _call_of(node)
        if call is None or isinstance(node.parent, exp.Dot):
            return node
        written = _value_node(answer(call))
        if isinstance(node.parent, exp.Select):
            return exp.alias_(written, call.name.lower())
        return written

    return tree.transform(replace).sql(dialect="postgres")


def fold(sql: str, reads: Reads, params: list[Any] | None) -> str:
    """``sql`` with each call of these functions replaced by its answer. A NULL argument answers
    NULL without a read: each of them is strict."""
    tree = sqlglot.parse_one(sql, read="postgres")

    def answer(call: exp.Anonymous) -> str | None:
        name = call.name.lower()
        values = _arguments(name, call.expressions, params)
        if any(v is None for v in values):
            return None
        return _FUNCTIONS[name](reads, *values)

    return _replaced(tree, answer)


def undescribed(sql: str) -> str:
    """``sql`` with each call an xml value of no content, to describe it while a parameter it
    needs has no value yet."""
    return _replaced(sqlglot.parse_one(sql, read="postgres"), lambda _call: None)
