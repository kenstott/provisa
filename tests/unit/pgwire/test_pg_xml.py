# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e2f07-c3a6-4d19-8f71-a04d6e92c1b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's xml type on pgwire (REQ-2167, REQ-2171): every statement run as the conformance
probe runs it, through a real pgwire server and a real DuckDB engine, and compared with what
PostgreSQL 17.11 answered to it, type, text and binary (pg17_xml_answers). The functions that read
data are compared over the fixture PostgreSQL read, and shown to read as the session reads."""

# Requirements: REQ-2167

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from psycopg import pq

from provisa.federation import duckdb_pg_xml as xml
from provisa.pgwire import xml_publish
from provisa.pgwire.catalog_pg17_overloads import FUNCTION_OVERLOADS
from provisa.pgwire.xml_publish import Column
from provisa.transpiler.transpile import transpile
from tests.unit.pgwire.pg17_xml_answers import CURSOR, PUBLISHED, SCALAR
from tests.unit.pgwire.test_type_conversions import conn, pgwire  # noqa: F401

# ruff: noqa: F811 -- the fixtures are conn and pgwire of the module they are imported from


def _run(conn: Any, sql: str, fmt: int = 0) -> Any:
    return conn.conn.pgconn.exec_params(sql.encode(), [], result_format=fmt)


def _rows(res: Any, decode: Any) -> list[list[Any]]:
    return [
        [None if (v := res.get_value(r, f)) is None else decode(v) for f in range(res.nfields)]
        for r in range(res.ntuples)
    ]


def _listed(problems: dict[str, Any]) -> str:
    return "\n".join(f"{sql}\n    -> {str(problem)[:300]}" for sql, problem in problems.items())


def _problem(conn: Any, sql: str, answer: tuple) -> str | None:
    """How the server's answer differs from PostgreSQL's, in type, text and binary; None if not."""
    text = _run(conn, sql, 0)
    if answer[0] == "err":
        if text.status != pq.ExecStatus.FATAL_ERROR:
            return f"answered {_rows(text, bytes.decode)}"
        said = (
            text.error_field(pq.DiagnosticField.SQLSTATE).decode(),
            text.error_field(pq.DiagnosticField.MESSAGE_PRIMARY).decode(),
        )
        return None if said == answer[1:3] else f"said {said}"
    if text.status != pq.ExecStatus.TUPLES_OK:
        return (text.error_message or b"").decode()
    _, oids, _, rows, binary = answer
    types = [text.ftype(f) for f in range(text.nfields)]
    if types != oids:
        return f"types {types}"
    if _rows(text, bytes.decode) != rows:
        return f"text {_rows(text, bytes.decode)}"
    sent = _run(conn, sql, 1)
    if sent.status != pq.ExecStatus.TUPLES_OK:
        return (sent.error_message or b"").decode()
    return None if _rows(sent, bytes.hex) == binary else f"binary {_rows(sent, bytes.hex)}"


# One test over all of them: conn builds a DuckDB runtime per test, and hundreds of those in one
# worker exhaust it.
def test_every_xml_statement_answers_as_postgresql_17_answered(conn: Any):
    problems = {
        sql: problem for sql, answer in SCALAR.items() if (problem := _problem(conn, sql, answer))
    }
    assert not problems, _listed(problems)


#: The answers that need no relation of the fixture.
_TABLELESS = {
    sql: answer
    for sql, answer in PUBLISHED.items()
    if not any(name in sql for name in ("xs.", "tp", "'xs'", "public", "database_", "nope"))
    and "_to_xmlschema" not in sql
    and "xmlschema(" not in sql
    and "and_xmlschema" not in sql
}


def test_the_functions_that_read_answer_as_postgresql_answered_through_the_server(conn: Any):
    problems = {
        sql: problem
        for sql, answer in _TABLELESS.items()
        if (problem := _problem(conn, sql, answer))
    }
    assert not problems, _listed(problems)


#: The functions the conformance probe found unserved until REQ-2167 served them.
_PROBED = (
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
    "xml",
    "xml_is_well_formed",
    "xml_is_well_formed_content",
    "xml_is_well_formed_document",
    "xml_send",
    "xmlagg",
    "xmlcomment",
    "xmlconcat2",
    "xmlexists",
    "xmltext",
    "xmlvalidate",
    "xpath",
    "xpath_exists",
)


def _null_probe_problem(conn: Any, name: str, args: tuple[str, ...], returns: str) -> str | None:
    listed = ", ".join(f"NULL::{t}" for t in args)
    res = _run(conn, f'SELECT pg_catalog."{name}"({listed}) AS v')
    if res.status != pq.ExecStatus.TUPLES_OK:
        return (res.error_message or b"").decode()
    expected = {"xml": 142, "xml[]": 143, "boolean": 16, "bytea": 17, "text": 25}[returns]
    return None if res.ftype(0) == expected else f"type {res.ftype(0)}, PostgreSQL {expected}"


def test_every_probed_overload_is_served_with_postgresqls_type(conn: Any):
    overloads = [
        (name, args, returns) for name in _PROBED for args, returns in FUNCTION_OVERLOADS[name]
    ]
    problems = {
        f"{name}({', '.join(args)})": problem
        for name, args, returns in overloads
        if (problem := _null_probe_problem(conn, name, args, returns))
    }
    assert not problems, _listed(problems)
    assert len(overloads) == 29


# --- the fixture PostgreSQL read, for Reads ---------------------------------------------------


def _text(value: Any) -> str:
    return str(value)


def _int(name: str) -> Column:
    return Column(name, 23, _text)


_TA = [
    Column("id", 23, _text),
    Column("name", 25, _text),
    Column("amount", 1700, _text),
    Column("d", 1082, _text),
    Column("ts", 1114, _text),
    Column("b", 16, _text),
    Column("x", 142, _text),
    Column("f", 701, _text),
    Column("by", 17, _text),
    Column("arr", 1007, _text),
]
_TA_ROWS = [
    [
        1,
        "a<b",
        Decimal("1.50"),
        datetime.date(2024, 1, 2),
        datetime.datetime(2024, 1, 2, 3, 4, 5),
        True,
        "<q/>",
        1.5,
        b"\xde\xad",
        [1, 2],
    ],
    [2, None, None, None, None, None, None, None, None, None],
]
_RELATIONS: dict[tuple[str, str], tuple[list[Column], list[list[Any]]]] = {
    ("xs", "T b"): ([_int("a b"), Column("c:d", 25, _text)], [[1, "x"]]),
    ("xs", "ta"): (_TA, _TA_ROWS),
    ("xs", "v"): ([_int("id")], [[1], [2]]),
    ("public", "tp"): ([_int("a")], [[5]]),
}
_QUERIES: dict[str, tuple[list[Column], list[list[Any]]]] = {
    "SELECT id FROM xs.ta ORDER BY id": ([_int("id")], [[1], [2]]),
    "SELECT * FROM xs.ta ORDER BY id": (_TA, _TA_ROWS),
    "SELECT * FROM xs.ta": (_TA, _TA_ROWS),
}


class _FixtureReads:
    """Reads over the fixture: what a session reads, as PostgreSQL's columns."""

    database = "postgres"

    def __init__(self) -> None:
        self.read: list[str] = []

    def _answer(self, sql: str) -> tuple[list[Column], list[list[Any]]]:
        self.read.append(sql)
        for (schema, table), answer in _RELATIONS.items():
            if sql == f'SELECT * FROM "{schema}"."{table}"':
                return answer
        if sql in _QUERIES:
            return _QUERIES[sql]
        return self._table_less(sql)

    @staticmethod
    def _table_less(sql: str) -> tuple[list[Column], list[list[Any]]]:
        kinds = {
            "SELECT 1 AS a, ''x<'' AS b": None,
            "SELECT 1 AS a": ([_int("a")], [[1]]),
            "SELECT 1 AS a, 'x'::text AS b": (
                [_int("a"), Column("b", 25, _text)],
                [[1, "x"]],
            ),
        }
        if sql.startswith("SELECT 1::int2"):
            names = "abcdefghijklm"
            oids = (21, 20, 700, 701, 1700, 1043, 1042, 1184, 1083, 1266, 1186, 114, 2950)
            return [Column(n, o, _text) for n, o in zip(names, oids)], []
        found = kinds.get(sql)
        if found is None:
            raise AssertionError(f"no fixture answers {sql}")
        return found

    def rows(self, sql: str) -> tuple[list[Column], list[Any]]:
        return self._answer(sql)

    def shape(self, sql: str) -> list[Column]:
        return self._answer(sql)[0]

    def _cursor(self, name: str) -> None:
        if name != "cx":
            raise ValueError(f'postgresql-error 34000: cursor "{name}" does not exist')

    def cursor_rows(self, name: str, count: int) -> tuple[list[Column], list[Any]]:
        self._cursor(name)
        columns = [_int("a"), Column("b", 25, _text)]
        rows = self.__dict__.setdefault("left", [[1, "v1"], [2, "v2"], [3, "v3"]])
        taken, self.__dict__["left"] = rows[:count], rows[count:]
        return columns, taken

    def cursor_shape(self, name: str) -> list[Column]:
        self._cursor(name)
        return [_int("a"), Column("b", 25, _text)]

    def catalog(self, sql: str, parameters: list[Any]) -> list[tuple]:
        schemas = sorted({schema for schema, _ in _RELATIONS})
        if "c.oid = ?" in sql:
            return []
        if "n.nspname = ? AND c.relname = ?" in sql:
            return [tuple(parameters)] if tuple(parameters) in _RELATIONS else []
        if "c.relname FROM" in sql:
            return [(t,) for s, t in sorted(_RELATIONS) if s == parameters[0]]
        if sql.startswith("SELECT 1"):
            return [(1,)] if parameters[0] in schemas else []
        return [(s,) for s in schemas]


def _published_problem(sql: str, answer: tuple, reads: Any) -> str | None:
    try:
        folded = xml_publish.fold(sql, reads, None)
    except ValueError as exc:
        if answer[0] != "err":
            return f"refused: {exc}"
        expected = f"{answer[1]}: {answer[2]}"
        return None if expected in str(exc) else f"said {exc}"
    if answer[0] == "err":
        return f"answered {folded}"
    want = answer[3][0][0]
    tree = xml_publish.sqlglot.parse_one(folded, read="postgres")
    held = next(tree.find_all(xml_publish.exp.Anonymous)).expressions[0]
    got = None if isinstance(held, xml_publish.exp.Null) else held.this
    return None if got == want else f"answered {got!r}"


def _fixture_answers() -> dict[str, tuple]:
    """Those of PUBLISHED over the fixture. A relation that is not there is the pipeline's to
    refuse, and each surface's refusal of one is its own test."""
    return {
        sql: answer
        for sql, answer in PUBLISHED.items()
        if sql not in _TABLELESS and "FROM nope" not in sql
    }


def test_every_mapping_of_the_fixture_is_postgresqls_document():
    problems = {
        sql: problem
        for sql, answer in _fixture_answers().items()
        if (problem := _published_problem(sql, answer, _FixtureReads()))
    }
    assert not problems, _listed(problems)


def test_a_cursor_is_read_where_it_stands():
    reads = _FixtureReads()
    problems = {
        sql: problem
        for sql, answer in CURSOR.items()
        if sql.startswith("SELECT") and (problem := _published_problem(sql, answer, reads))
    }
    assert not problems, _listed(problems)


def test_a_cursor_is_read_through_the_server(conn: Any):
    client = conn.conn.pgconn
    problems: dict[str, Any] = {}
    for sql, answer in CURSOR.items():
        if not sql.startswith("SELECT"):
            client.exec_(sql.encode())
            continue
        res = _run(conn, sql)
        # A type outside SQL's names is named for its database: provisa here, postgres there.
        rows = [
            [cell.replace("UDT.provisa.", "UDT.postgres.") for cell in row]
            for row in _rows(res, bytes.decode)
        ]
        if res.status != pq.ExecStatus.TUPLES_OK or rows != answer[3]:
            problems[sql] = (res.error_message or b"").decode() or rows
    assert not problems, _listed(problems)


def test_a_null_argument_reads_nothing():
    reads = _FixtureReads()
    folded = xml_publish.fold(
        "SELECT table_to_xml(NULL::regclass, true, false, '') AS v", reads, None
    )
    assert reads.read == [] and "PROVISA_XML(NULL)" in folded


def test_a_query_argument_is_refused_unless_it_is_one_read():
    for query, said in (
        ("DELETE FROM t", "DELETE is not allowed in a non-volatile function"),
        ("WITH d AS (DELETE FROM t RETURNING a) SELECT a FROM d", "DELETE is not allowed"),
        ("SELECT 1 INTO t", "SELECT INTO is not allowed"),
        ("DROP TABLE t", "DROP TABLE is not allowed"),
        ("SELECT 1; SELECT 2", "only one statement"),
    ):
        reads = _FixtureReads()
        escaped = query.replace("'", "''")
        with pytest.raises(ValueError, match=said):
            xml_publish.fold(f"SELECT query_to_xml('{escaped}', true, false, '')", reads, None)
        assert reads.read == []


def test_a_call_over_a_column_is_refused_by_name():
    with pytest.raises(ValueError, match="0A000: query_to_xml: an argument that is not a constant"):
        xml_publish.fold("SELECT query_to_xml(q, true, false, '') FROM t", _FixtureReads(), None)


def test_a_parameter_is_the_argument_it_binds():
    reads = _FixtureReads()
    folded = xml_publish.fold("SELECT query_to_xml($1, true, false, '')", reads, ["SELECT 1 AS a"])
    assert reads.read == ["SELECT 1 AS a"] and "<a>1</a>" in folded


def test_a_string_that_only_names_a_function_is_no_call():
    assert xml_publish.calls_one("SELECT 'query_to_xml(' AS v") is False
    assert xml_publish.calls_one("SELECT query_to_xml('SELECT 1', true, false, '') AS v") is True
    assert xml_publish.calls_one("SELECT pg_catalog.\"table_to_xml\"(1, true, false, '')") is True


_STRING_COLUMN = '[{"path": "string(.)", "ordinality": false, "xml": false, "numeric": false}]'


# --- the document cannot reach out ------------------------------------------------------------


@pytest.mark.parametrize(
    "document",
    [
        '<!DOCTYPE a [<!ENTITY e SYSTEM "file:///etc/hosts">]><a>&e;</a>',
        '<!DOCTYPE a [<!ENTITY % p SYSTEM "file:///etc/hosts"> %p;]><a/>',
        '<!DOCTYPE a SYSTEM "file:///etc/hosts"><a/>',
        '<!DOCTYPE a [<!ENTITY e SYSTEM "http://127.0.0.1:1/x">]><a>&e;</a>',
    ],
)
def test_an_external_entity_reads_nothing(document):
    assert xml.xpath("string(/a)", document, None) == [""]


def test_no_entity_reads_a_file_that_is_there(tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("TOPSECRET")
    declared = tmp_path / "declared.dtd"
    declared.write_text('<!ENTITY fromfile "DTDSECRET">')
    general = f'<!DOCTYPE a [<!ENTITY e SYSTEM "file://{secret}">]><a>&e;</a>'
    assert xml.xpath("string(/a)", general, None) == [""]
    assert xml.xpath("/a", general, None) == ["<a/>"]
    assert "SECRET" not in xml.xmlserialize(general, True, True)
    for document in (
        f'<!DOCTYPE a [<!ENTITY % p SYSTEM "file://{declared}"> %p;]><a>&fromfile;</a>',
        f'<!DOCTYPE a SYSTEM "file://{declared}"><a>&fromfile;</a>',
    ):
        for read in (
            lambda d: xml.xpath("/a", d, None),
            lambda d: xml.xmlserialize(d, True, True),
            lambda d: xml.xmltable(d, "/a", "[]", _STRING_COLUMN),
        ):
            try:
                assert "SECRET" not in str(read(document))
            except ValueError as refused:  # an entity with no declaration is not a document
                assert "2200" in str(refused)


def test_an_entity_bomb_is_refused_as_ill_formed():
    entities = "".join(f'<!ENTITY e{n} "{("&e" + str(n - 1) + ";") * 10}">' for n in range(1, 12))
    bomb = f'<!DOCTYPE a [<!ENTITY e0 "x">{entities}]><a>&e11;</a>'
    assert xml.is_well_formed(bomb, True) is False


def test_content_cannot_close_what_holds_it():
    assert xml.is_well_formed("</h><x/><h>", False) is False
    assert xml._read_content("</nope>") is None


# --- values -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "text"),
    [
        (1.0, "1"),
        (1.5, "1.5"),
        (-0.25, "-0.25"),
        (1e15, "1e+15"),
        (123456789012345.0, "123456789012345"),
        (1e-5, "1e-05"),
        (0.0001, "0.0001"),
        (float("nan"), "NaN"),
        (float("-inf"), "-Infinity"),
    ],
)
def test_a_number_is_written_as_float8out_writes_it(value, text):
    assert xml._float_out(value) == text


@pytest.mark.parametrize(
    ("declaration", "code", "length", "version", "standalone"),
    [
        ("<a/>", 0, 0, None, -1),
        ('<?xml version="1.0"?>', 0, 21, "1.0", -1),
        ("<?xml version='1.1' standalone='no'?>", 0, 37, "1.1", 0),
        ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>', 0, 55, "1.0", 1),
        ("<?xml-stylesheet x?>", 0, 0, None, -1),
        ("<?xml bad?>", 1, 0, None, -1),
        ('<?xml version="1.0"encoding="UTF-8"?>', 1, 0, "1.0", -1),
    ],
)
def test_a_declaration_is_read_as_parse_xml_decl_reads_it(
    declaration, code, length, version, standalone
):
    assert xml.parse_declaration(declaration) == (code, length, version, standalone)


def test_an_xmltable_column_name_stays_a_name():
    """The engine's form is written after governance: a name must not become SQL."""
    from provisa.transpiler.transpile import transpile_to_duckdb

    name = """x" , (SELECT 1) AS "y"""
    quoted = name.replace('"', '""')
    written = transpile_to_duckdb(
        f"""SELECT * FROM XMLTABLE('/r' PASSING ('<r/>'::xml) COLUMNS "{quoted}" text) t"""
    )
    tree = xml_publish.sqlglot.parse_one(written, read="duckdb")
    derived = tree.find(xml_publish.exp.Subquery)
    assert derived is not None
    assert [s.alias_or_name for s in derived.this.selects] == [name]


# --- on a PostgreSQL source, the syntax is PostgreSQL's ---------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT XMLPARSE(DOCUMENT x) FROM t",
        "SELECT XMLPARSE(CONTENT x STRIP WHITESPACE) FROM t",
        "SELECT XMLPI(NAME php, x) FROM t",
        "SELECT XMLROOT(x, VERSION NO VALUE, STANDALONE YES) FROM t",
        "SELECT XMLSERIALIZE(CONTENT x AS TEXT INDENT) FROM t",
        "SELECT XMLEXISTS('//a' PASSING (x)) FROM t",
        "SELECT * FROM XMLTABLE('/r' PASSING (x) COLUMNS n FOR ORDINALITY, v TEXT PATH '.') AS y",
    ],
)
def test_a_statement_crosses_governance_and_reaches_postgresql_as_written(sql):
    assert transpile(sql, "postgres") == sql


# --- a role reads what it may: each read is a governed statement of the session ------------------


@pytest.fixture
def governed(pgwire):
    """A session on the real governance pipeline: the role analyst reads sales.orders' id, sees
    secret masked and never sees hidden, and reads no other domain. The statement's routing stage
    answers with the SQL governance made of it, which is what would have reached a source. A
    refusal's audit row needs the org's record database, which this state has none of."""
    import psycopg

    from provisa.compiler.sql_gen import CompilationContext
    from provisa.pgwire import _pipeline
    from provisa.pgwire.server import ProvisaSession
    from provisa.security.masking import MaskingRule, MaskType
    from tests.role_state import RoleState
    from tests.unit import test_governed_sql_engine_internals as internals

    columns = [
        {"column_name": "id", "data_type": "integer", "visible_to": ["analyst"]},
        {"column_name": "secret", "data_type": "text", "visible_to": ["analyst"]},
        {"column_name": "hidden", "data_type": "text", "visible_to": ["admin"]},
    ]

    class Unset(RoleState):
        """The real state, for what this path reads of it: nothing is set up that it does not
        ask for, and each such setting (a response cache, an audit store) is off."""

        def __getattr__(self, name: str) -> Any:
            try:
                return RoleState.__getattr__(self, name)  # type: ignore[attr-defined]
            except AttributeError:
                if name.startswith("__"):
                    raise
                return None

    state = internals._state(["sales"])
    state.__class__ = Unset
    state.tables = [{**internals._TABLES[0], "columns": columns}]
    state.masking_rules = {
        (1, "analyst"): {"secret": (MaskingRule(mask_type=MaskType.constant, value="***"), "text")}
    }
    state.auth_config = None
    state.auth_middleware_active = False
    state.multitenancy = False
    state.org_id = "org"
    state.schema_build_cache = {"column_types": {}}
    state.federation_engine = None
    assert isinstance(state.contexts["analyst"], CompilationContext)

    class Reached(Exception):
        pass

    async def reached(governed_statement: Any, **kwargs: Any) -> Any:
        raise Reached(f"REACHED: {governed_statement.governed_semantic}")

    with (
        patch("provisa.api.app.state", state),
        patch.object(_pipeline, "route_governed", reached),
        patch("provisa.audit.pipeline.write_audit", AsyncMock()),
        patch.object(ProvisaSession, "_finalize_audit", lambda self, governed, status, **kw: None),
    ):
        connection = psycopg.connect(
            host="127.0.0.1",
            port=pgwire,
            user="analyst",
            password="x",
            dbname="provisa",
            autocommit=True,
        )
        try:
            yield connection
        finally:
            connection.close()


def _said(connection: Any, sql: str) -> str:
    res = connection.pgconn.exec_params(sql.encode(), [], result_format=0)
    assert res.status == pq.ExecStatus.FATAL_ERROR, _rows(res, bytes.decode)
    return (res.error_message or b"").decode()


def test_query_to_xml_reads_a_masked_column_masked(governed: Any):
    said = _said(
        governed,
        "SELECT query_to_xml('SELECT o.id, o.secret FROM sales.orders o', true, false, '')",
    )
    assert "REACHED" in said and "'***'" in said
    assert 'o".secret' not in said.replace('"o"."secret" AS', "")


@pytest.mark.parametrize(
    "read",
    [
        "SELECT o.hidden FROM sales.orders o",
        "SELECT * FROM read_csv(''/etc/passwd'')",
        "SELECT * FROM duckdb_secrets()",
        "SELECT o.id FROM finance.ledger o",
    ],
)
def test_query_to_xml_refuses_what_the_role_may_not_read(governed: Any, read: str):
    said = _said(governed, f"SELECT query_to_xml('{read}', true, false, '')")
    assert "REACHED" not in said


def test_table_to_xml_reads_the_table_as_the_role_reads_it(governed: Any):
    said = _said(governed, "SELECT table_to_xml('sales.orders', true, false, '')")
    assert "REACHED" in said and "'***'" in said and "hidden" not in said


def test_table_to_xml_cannot_name_what_the_role_cannot_see(governed: Any):
    said = _said(governed, "SELECT table_to_xml('finance.ledger', true, false, '')")
    assert 'relation "finance.ledger" does not exist' in said and "REACHED" not in said


def test_query_to_xmlschema_describes_only_what_the_role_may_read(governed: Any):
    said = _said(
        governed,
        "SELECT query_to_xmlschema('SELECT CAST(o.id AS int) AS id FROM sales.orders o', "
        "true, false, '')",
    )
    assert "REACHED" in said and 'xsd:element name="id"' in said.replace('\\"', '"')
    refused = _said(
        governed,
        "SELECT query_to_xmlschema('SELECT o.hidden FROM sales.orders o', true, false, '')",
    )
    assert "xsd:schema" not in refused


def test_schema_to_xml_lists_only_what_the_role_sees(governed: Any):
    said = _said(governed, "SELECT schema_to_xml('finance', true, false, '')")
    assert 'schema "finance" does not exist' in said


def test_a_masked_column_is_not_xmltables_document(governed: Any):
    said = _said(
        governed,
        "SELECT x.v FROM sales.orders o, "
        "XMLTABLE('/r' PASSING (o.secret) COLUMNS v text PATH '.') x",
    )
    assert "V005" in said and "REACHED" not in said


@pytest.mark.parametrize(
    "columns",
    [
        "v text PATH o.secret",
        "v text PATH '.' DEFAULT o.secret",
    ],
)
def test_a_masked_column_reaches_xmltable_by_no_other_way(governed: Any, columns: str):
    said = _said(
        governed,
        f"SELECT x.v FROM sales.orders o, XMLTABLE('/r' PASSING (o.id) COLUMNS {columns}) x",
    )
    assert "REACHED" not in said


def test_a_column_the_role_may_read_is_xmltables_document(governed: Any):
    said = _said(
        governed,
        "SELECT x.v FROM sales.orders o, XMLTABLE('/r' PASSING (o.id) COLUMNS v text PATH '.') x",
    )
    assert "REACHED" in said


def test_a_hidden_column_is_not_xmltables_document(governed: Any):
    said = _said(
        governed,
        "SELECT x.v FROM sales.orders o, "
        "XMLTABLE('/r' PASSING (o.hidden) COLUMNS v text PATH '.') x",
    )
    assert "V003" in said and "REACHED" not in said


def test_xmlelement_over_a_masked_column_reads_it_masked(governed: Any):
    said = _said(governed, "SELECT xmlelement(name a, o.secret) FROM sales.orders o")
    assert "REACHED" in said and "'***'" in said


def test_a_read_cut_at_the_limit_warns_the_statement():
    from provisa.pgwire.server import _SessionReads

    class Result:
        warnings = ("first",)

        def column_count(self) -> int:
            return 0

        def rows(self) -> list[Any]:
            return []

        def late_warnings(self) -> list[Any]:
            return ["cut"]

        def close(self) -> None:
            pass

    class Session:
        def _execute_sql_bound(self, sql: str, params: Any, formats: Any) -> Result:
            return Result()

    reads = _SessionReads(Session())  # type: ignore[arg-type]
    reads.rows("SELECT 1")
    assert reads.warnings == ["first", "cut"]
