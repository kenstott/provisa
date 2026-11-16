# Copyright (c) 2026 Kenneth Stott
# Canary: 7c1b0a7e-2f55-4b0e-8a4b-0c61c8f1d2a7
#
# This source code is licensed under the Business Source License 1.1
"""No registered value reaches the DuckDB engine's DDL unquoted.

DuckDB runs every statement of a text it is handed, and a role holding source or table
registration writes a source's path, host, database, user and credentials, a table's schema and
name and a column's name. Each site that puts one of them into a statement is fed values built
to end the literal or the identifier it sits in; the statement must stay one statement and the
value must come back as data. Where the statement runs with no network it is run on an
in-memory DuckDB; elsewhere DuckDB's own parser counts the statements and the literal is read
back from the text.

The last tests hold the two modules to the helper: a statement text built from an interpolated
value that is not written by ``duckdb_quoting`` fails them.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import duckdb
import pytest
import sqlglot
from psycopg.conninfo import conninfo_to_dict
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.tokens import TokenType

import provisa.api.app  # noqa: F401 -- the API layer says whether the deployment holds many orgs, which resolving a source asks
from provisa.core.models import SourceType
from provisa.federation import connector_duckdb as cd
from provisa.federation.duckdb_quoting import ident, libpq_dsn, libpq_value, lit

INJECTION = "'); ATTACH ':memory:' AS pwned; --"
HOSTILE = [
    pytest.param("a'b", id="single-quote"),
    pytest.param('a"b', id="double-quote"),
    pytest.param("a;b", id="semicolon"),
    pytest.param("x')", id="quote-paren"),
    pytest.param("a\nb", id="newline"),
    pytest.param(INJECTION, id="stacked-attach"),
    pytest.param("\"); ATTACH ':memory:' AS pwned; --", id="stacked-attach-identifier"),
]


def _source(**fields) -> Any:
    base = {
        "id": "src-1",
        "type": SourceType.postgresql,
        "host": "db.internal",
        "port": 5432,
        "database": "shop",
        "username": "reader",
        "password": "pw",
        "path": None,
        "base_url": None,
        "federation_hints": {},
        "mapping": {},
        "max_live_concurrency": None,
    }
    return SimpleNamespace(**{**base, **fields})


def _strings(sql: str) -> list[str]:
    """The string literals of ``sql``, decoded, as the DuckDB dialect reads them."""
    return [t.text for t in DuckDB.Tokenizer().tokenize(sql) if t.token_type == TokenType.STRING]


def _one_statement(
    sql: str, value: str, *, statements: int = 1, verbatim: bool = True
) -> list[str]:
    """``sql`` is ``statements`` statements to DuckDB's parser and to sqlglot's, and ``value``
    is inside one of its string literals (``verbatim``; a value a second grammar reads is
    checked by its caller in that grammar); the literals."""
    con = duckdb.connect()
    try:
        assert len(con.extract_statements(sql)) == statements, sql
    finally:
        con.close()
    assert len(sqlglot.parse(sql, read="duckdb")) == statements, sql
    strings = _strings(sql)
    if verbatim:
        assert any(value in s for s in strings), (value, strings)
    return strings


# --- the helper pair -----------------------------------------------------------------------


@pytest.mark.parametrize("value", HOSTILE)
def test_a_literal_and_an_identifier_come_back_as_written(value):
    con = duckdb.connect()
    try:
        assert con.execute(f"SELECT {lit(value)} AS {ident(value)}").fetchall() == [(value,)]
        assert [d[0] for d in con.description] == [value]
        assert [
            r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()
        ] == [
            "memory",
            "system",
            "temp",
        ]
    finally:
        con.close()


def test_identifier_parts_are_joined_and_a_nul_is_refused():
    assert ident("c", 's"x', "t") == '"c"."s""x"."t"'
    with pytest.raises(ValueError, match="NUL"):
        ident("a\x00b")
    with pytest.raises(ValueError, match="NUL"):
        lit("a\x00b")


@pytest.mark.parametrize("value", [*HOSTILE, "", "a b", "back\\slash", "x passfile=/etc/passwd"])
def test_a_libpq_value_is_read_back_by_libpq(value):
    dsn = libpq_dsn(host=value, port=5432, dbname=value)
    assert conninfo_to_dict(dsn) == {"host": value, "port": "5432", "dbname": value}


def test_a_plain_libpq_value_is_written_bare():
    assert libpq_value("db.internal") == "db.internal"
    assert libpq_dsn(host="h", port=5, user="u") == "host=h port=5 user=u"


# --- connection strings: one statement, and the value is the field it was given for ----------


@pytest.mark.parametrize("value", HOSTILE)
@pytest.mark.parametrize("field", ["host", "database", "username", "password"])
def test_a_postgres_attach_keeps_each_field_a_libpq_value(field, value):
    details = cd.DuckDBPostgresConnector().details(_source(**{field: value}))
    (dsn,) = _one_statement(details["attach"], value, verbatim=False)
    keyword = {"database": "dbname", "username": "user", "host": "host", "password": "password"}[
        field
    ]
    parsed = conninfo_to_dict(dsn)
    assert parsed[keyword] == value
    assert set(parsed) == {"host", "port", "dbname", "user", "password"}


@pytest.mark.parametrize("value", HOSTILE)
@pytest.mark.parametrize("field", ["host", "database", "username", "password"])
def test_a_sql_server_attach_is_one_statement(field, value):
    source = _source(type=SourceType.sqlserver, **{field: value})
    _one_statement(cd.DuckDBMssqlConnector().details(source)["attach"], value)


@pytest.mark.parametrize("value", HOSTILE)
@pytest.mark.parametrize("field", ["host", "database"])
def test_a_fabric_attach_is_its_secret_and_its_attach(field, value):
    source = _source(type=SimpleNamespace(value="fabric"), **{field: value})
    _one_statement(cd.DuckDBMssqlConnector("fabric").details(source)["attach"], value, statements=2)


@pytest.mark.parametrize("value", HOSTILE)
@pytest.mark.parametrize("field", ["host", "username", "password", "path"])
def test_a_firebird_attach_is_one_statement(field, value):
    source = _source(**{field: value})
    _one_statement(cd.DuckDBFirebirdConnector().details(source)["attach"], value)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_bigquery_attach_is_one_statement(value):
    source = _source(federation_hints={"project": value})
    _one_statement(cd.DuckDBBigQueryConnector().details(source)["attach"], value)


@pytest.mark.parametrize("value", HOSTILE)
@pytest.mark.parametrize("field", ["base_url", "host"])
def test_an_airport_attach_is_one_statement(field, value):
    source = _source(**{field: value})
    _one_statement(cd.DuckDBAirportConnector().details(source)["attach"], value)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_motherduck_attach_is_one_statement(value):
    details = cd.DuckDBMotherDuckConnector().details(_source(database=value))
    assert _one_statement(details["attach"], value) == ["md:" + value]


@pytest.mark.parametrize("value", HOSTILE)
@pytest.mark.parametrize("field", ["host", "username", "password", "database"])
def test_a_snowflake_attach_is_its_secret_and_its_attach(field, value):
    source = _source(**{field: value})
    attach = cd.DuckDBSnowflakeConnector().details(source)["attach"]
    assert value in _one_statement(attach, value, statements=2)


# --- scanner views and secrets -------------------------------------------------------------


@pytest.mark.parametrize("value", HOSTILE)
def test_a_google_sheet_id_and_its_key_file_are_literals(value):
    source = _source(database=value, mapping={"credentials_json": value})
    details = cd.DuckDBGsheetsConnector().details(source)
    assert _one_statement(details["view_ddl"], value) == [value]
    assert _one_statement(details["secret_ddl"], value) == [value]


@pytest.mark.parametrize("value", HOSTILE)
@pytest.mark.parametrize("connector", [cd.DuckDBIcebergConnector, cd.DuckDBDeltaConnector])
def test_a_lake_table_location_and_its_s3_secret_are_literals(connector, value):
    hints = {"access_key_id": value, "secret_access_key": value, "endpoint": "https://" + value}
    source = _source(path="s3://bucket/" + value, federation_hints=hints)
    details = connector().details(source)
    assert _one_statement(details["view_ddl"], value) == ["s3://bucket/" + value]
    assert _one_statement(details["secret_ddl"], value) == [value, value, value, "path"]


@pytest.mark.parametrize("value", HOSTILE)
@pytest.mark.parametrize(
    "hints",
    [
        {"access_key_id": "{v}", "secret_access_key": "{v}", "region": "{v}", "endpoint": "{v}"},
        {"gcs_access_id": "{v}", "gcs_secret_key": "{v}"},
        {"azure_account_name": "{v}", "azure_account_key": "{v}"},
    ],
    ids=["s3", "gcs", "azure"],
)
def test_an_object_store_secret_is_one_statement(hints, value):
    scheme = {"access_key_id": "s3", "gcs_access_id": "gs", "azure_account_name": "az"}[
        next(iter(hints))
    ]
    source = _source(
        path=f"{scheme}://bucket/a.csv",
        federation_hints={k: v.format(v=value) for k, v in hints.items()},
    )
    _one_statement(cd.object_store_secret_ddl(source), value)


@pytest.mark.parametrize("value", HOSTILE)
def test_a_remote_looking_path_is_a_literal(value):
    """A path with a scheme is kept as written at registration (``file_source_path``): what makes
    it safe is that it is only ever a literal."""
    path = "https://example.com/a.csv" + value
    for connector in (cd.DuckDBCsvConnector(), cd.DuckDBParquetConnector()):
        assert _one_statement(connector.details(_source(path=path))["view_ddl"], value) == [path]
    for connector in (cd.DuckDBSqliteConnector(), cd.DuckDBDuckdbConnector()):
        assert _one_statement(connector.details(_source(path=path))["attach"], value) == [path]


# --- run on DuckDB: file paths, schema and table names -----------------------------------


def _databases(con) -> set[str]:
    return {r[0] for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall()}


@pytest.mark.parametrize("value", HOSTILE)
@pytest.mark.parametrize("fmt", ["csv", "parquet"])
def test_a_file_at_a_hostile_path_is_read_and_nothing_else_runs(tmp_path, fmt, value):
    path = tmp_path / f"f{value}.{fmt}"
    con = duckdb.connect()
    try:
        con.execute(f"COPY (SELECT 42 AS n) TO {lit(str(path))} (FORMAT {fmt})")
        connector = cd.DuckDBCsvConnector() if fmt == "csv" else cd.DuckDBParquetConnector()
        ddl = connector.details(_source(path=str(path)))["view_ddl"]
        scan = ddl.split(" AS ", 1)[1]
        con.execute(f"CREATE VIEW v AS {scan}")
        assert con.execute("SELECT n FROM v").fetchall() == [(42,)]
        assert "pwned" not in _databases(con)
    finally:
        con.close()
    assert cd.read_flat_file_sync(
        str(path), fmt, f"SELECT n FROM {ident(path.stem.replace('.', '_').replace('-', '_'))}"
    ) == [{"n": 42}]


@pytest.fixture
def runtime():
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    rt = DuckDBFederationRuntime()
    yield rt
    rt.close()


def _duckdb_file_source(path: Path, schema: str, table: str) -> Any:
    return _source(
        id="shop",
        type=SourceType.duckdb,
        host="",
        port=0,
        database="",
        username="",
        password="",
        path=str(path),
        schema_name=schema,
        table_name=table,
    )


@pytest.mark.parametrize("value", HOSTILE)
def test_an_attached_table_with_hostile_names_at_a_hostile_path_is_read(tmp_path, runtime, value):
    """The file's path, the registered schema and the table's name each carry the value: the
    engine attaches the file, makes the schema and the view, reads the row, and drops the view."""
    db = tmp_path / f"db{value}.duckdb"
    made = duckdb.connect(str(db))
    made.execute(f"CREATE TABLE {ident(value)} AS SELECT 7 AS n")
    made.close()
    source = _duckdb_file_source(db, value, value)

    runtime.attach_source(source)
    con = runtime.connection.cursor()
    try:
        assert con.execute(f"SELECT n FROM {ident('shop', value, value)}").fetchall() == [(7,)]
        views = con.execute(
            "SELECT schema_name, view_name FROM duckdb_views() WHERE database_name = 'shop'"
        ).fetchall()
        assert views == [(value, value)]
        assert "pwned" not in _databases(con)
        runtime.detach_source(source)
        assert con.execute(
            "SELECT count(*) FROM duckdb_views() WHERE database_name = 'shop'"
        ).fetchall() == [(0,)]
    finally:
        con.close()


@pytest.mark.parametrize("value", HOSTILE)
def test_a_scanned_file_with_hostile_names_is_read(tmp_path, runtime, value):
    path = tmp_path / f"f{value}.csv"
    con = runtime.connection.cursor()
    try:
        con.execute(f"COPY (SELECT 42 AS n) TO {lit(str(path))} (FORMAT csv)")
        source = _source(
            id="files", type=SourceType.csv, path=str(path), schema_name=value, table_name=value
        )
        runtime.attach_source(source)
        assert con.execute(f"SELECT n FROM {ident('files', value, value)}").fetchall() == [(42,)]
        assert "pwned" not in _databases(con)
    finally:
        con.close()


# --- the names a table and a column may carry -------------------------------------------


def _table(schema: str = "public", table: str = "orders", column: str = "id"):
    from provisa.core.models import Table

    return Table.model_validate(
        {
            "source_id": "shop",
            "domain_id": "sales",
            "schema": schema,
            "table": table,
            "columns": [{"name": column, "visible_to": []}],
        }
    )


@pytest.mark.parametrize(
    "name",
    [
        "orders",
        "Order Details",
        "dbo.orders",
        'say "hi"',
        "it's",
        "commandes_été",
        "注文",
        "a;b",
        "$tmp#1",
        "sales/2024-01",
        "x')",
    ],
)
def test_a_name_a_source_can_hold_is_kept(name):
    table = _table(schema=name, table=name, column=name)
    assert (table.schema_name, table.table_name, table.columns[0].name) == (name, name, name)


@pytest.mark.parametrize(
    "bad", ["a\x00b", "a\nb", "a\rb", "a\tb", "a\x1bb", "a\x7fb", "a\x85b"], ids=repr
)
@pytest.mark.parametrize("where", ["schema", "table", "column"])
def test_a_control_character_in_a_name_is_refused(where, bad):
    from pydantic import ValidationError

    with pytest.raises(ValidationError, match="control character"):
        _table(**{where: bad})


# --- structure: statement text is built through the helper -----------------------------------

FEDERATION = Path(cd.__file__).parent
#: The modules that give the DuckDB engine statements carrying registered values.
DUCKDB_DDL_MODULES = ("connector_duckdb.py", "duckdb_runtime.py")
#: What may write an interpolated value into statement text.
QUOTERS = frozenset({"lit", "ident", "libpq_dsn", "libpq_value", "sql_literal", "int"})
_SQL_TEXT = re.compile(
    r"\b(ATTACH|DETACH|CREATE|DROP|ALTER|SELECT|INSERT|UPDATE|DELETE|COPY|INSTALL|LOAD|FROM|CALL|"
    r"DESCRIBE|TYPE|SECRET|KEY_ID|ENDPOINT|REGION|SCOPE|FILEPATH|ACCOUNT|USER|PASSWORD|DATABASE|"
    r"WAREHOUSE|ROLE|TENANT_ID|CLIENT_ID|CLIENT_SECRET|CONNECTION_STRING)\b"
)

#: Interpolations that are not a registered value, each read and found to be one of: statement
#: text already built through the helper (a quoted name, a clause list, a whole scan), a constant
#: of the module, or a name of DuckDB's own. (module, function, expression).
REVIEWED: frozenset[tuple[str, str, str]] = frozenset(
    {
        # connector_duckdb.py
        ("connector_duckdb.py", "_attach_options", "kind"),  # "sqlite" or "duckdb", from the caller
        ("connector_duckdb.py", "details", "_attach_options(source, 'sqlite')"),  # option keywords
        ("connector_duckdb.py", "details", "_attach_options(source, 'duckdb')"),  # option keywords
        ("connector_duckdb.py", "_scan_view_ddl", "scanner"),  # the connector's table function
        ("connector_duckdb.py", "_s3_secret_ddl", "', '.join(clauses)"),  # clauses of lit()
        (
            "connector_duckdb.py",
            "object_store_secret_ddl",
            "', '.join(clauses)",
        ),  # clauses of lit()
        ("connector_duckdb.py", "object_store_secret_ddl", "name"),  # ident()
        ("connector_duckdb.py", "details", "', '.join(options)"),  # snowflake: options of lit()
        ("connector_duckdb.py", "details", "provider"),  # azure: keywords and lit()
        # duckdb_runtime.py
        ("duckdb_runtime.py", "_attach_databricks", "ext"),  # "httpfs" or "iceberg"
        ("duckdb_runtime.py", "_point_databricks_view", "options"),  # DuckDB's option names, lit()
        ("duckdb_runtime.py", "_point_databricks_view", "phys"),  # _phys_name: ident()
        ("duckdb_runtime.py", "_point_databricks_view", "scan_args"),  # the connector's constant
        ("duckdb_runtime.py", "attach_source", "phys"),  # _phys_name: ident()
        ("duckdb_runtime.py", "attach_source", "remote"),  # ident()
        ("duckdb_runtime.py", "attach_source", "renamed"),  # ident() pairs
        ("duckdb_runtime.py", "attach_source", "scan"),  # the connector's scan: _scan_view_ddl
        ("duckdb_runtime.py", "attach_region_ops", "select"),  # an ops view's own SELECT
        ("duckdb_runtime.py", "attach_region_store", "store_type"),  # _ATTACH_TYPE_BY_SCHEME
        ("duckdb_runtime.py", "ensure_materialize_attached", "store_type"),  # the same
        ("duckdb_runtime.py", "ensure_materialize_attached", "self._MAT_STORE"),  # "mat_store"
        ("duckdb_runtime.py", "_attach_motherduck_store", "self._MAT_STORE"),  # "mat_store"
        ("duckdb_runtime.py", "_copy_store_table", "target"),  # _local_store_name: ident()
        ("duckdb_runtime.py", "_copy_store_table", "_typed_back(arrow_tbl.schema)"),  # ident()
        ("duckdb_runtime.py", "_index_replica_vectors", "target"),  # _local_store_name: ident()
        ("duckdb_runtime.py", "_index_replica_vectors", "quoted"),  # ident()
        ("duckdb_runtime.py", "introspect_columns", "phys"),  # _phys_name: ident()
        ("duckdb_runtime.py", "introspect_columns", "target"),  # phys, or the ClickHouse read
        ("duckdb_runtime.py", "describe_sync", "duck_sql"),  # the governed statement itself
    }
)


def _postgres_engine_classes(tree: ast.Module) -> set[str]:
    """The classes of ``tree`` that are connectors of the PostgreSQL engine: their statements go
    to PostgreSQL, not DuckDB."""
    engine: dict[str, str | None] = {}
    bases: dict[str, list[str]] = {}
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        bases[node.name] = [b.id for b in node.bases if isinstance(b, ast.Name)]
        engine[node.name] = next(
            (
                stmt.value.value
                for stmt in node.body
                if isinstance(stmt, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "engine" for t in stmt.targets)
                and isinstance(stmt.value, ast.Constant)
            ),
            None,
        )

    def resolved(name: str) -> str | None:
        if engine.get(name) is not None:
            return engine[name]
        return next((e for e in map(resolved, bases.get(name, [])) if e is not None), None)

    return {name for name in engine if resolved(name) == "postgres"}


def _is_quoted(value: ast.expr) -> bool:
    if isinstance(value, ast.Constant):
        return True
    if not isinstance(value, ast.Call):
        return False
    func = value.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
    return name in QUOTERS


def _unquoted_interpolations(module: str, source: str) -> set[tuple[str, str, str]]:
    """Every value written into statement text of ``source`` by something other than a quoter:
    an f-string field, an operand of ``+`` or ``%``, an argument of ``.format``."""
    tree = ast.parse(source)
    skipped = _postgres_engine_classes(tree)
    found: set[tuple[str, str, str]] = set()

    def text_of(node: ast.expr) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            return "".join(
                v.value
                for v in node.values
                if isinstance(v, ast.Constant) and isinstance(v.value, str)
            )
        return None

    def visit(node: ast.AST, function: str) -> None:
        if isinstance(node, ast.ClassDef) and node.name in skipped:
            return
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            function = node.name
        values: list[ast.expr] = []
        if isinstance(node, ast.JoinedStr) and _SQL_TEXT.search(text_of(node) or ""):
            values = [v.value for v in node.values if isinstance(v, ast.FormattedValue)]
        elif isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Mod)):
            sides = [node.left, node.right]
            if any(_SQL_TEXT.search(text_of(s) or "") for s in sides):
                values = [s for s in sides if text_of(s) is None]
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "format"
            and _SQL_TEXT.search(text_of(node.func.value) or "")
        ):
            values = [*node.args, *(k.value for k in node.keywords)]
        for value in values:
            if not _is_quoted(value):
                found.add((module, function, ast.unparse(value)))
        for child in ast.iter_child_nodes(node):
            visit(child, function)

    visit(tree, "<module>")
    return found


def test_the_detector_sees_each_way_a_value_is_spliced():
    spliced = """
def f(source):
    a = f"ATTACH '{source.path}' AS x"
    b = "SELECT * FROM " + source.table_name
    c = "DROP VIEW %s" % source.table_name
    d = "CREATE SCHEMA {}".format(source.schema_name)
    e = f"ATTACH {lit(source.path)} AS {ident(source.id)}"
    return f"no statement here: {source.id}"
"""
    assert _unquoted_interpolations("m.py", spliced) == {
        ("m.py", "f", "source.path"),
        ("m.py", "f", "source.table_name"),
        ("m.py", "f", "source.schema_name"),
    }


def test_duckdb_sql_is_built_through_the_quoting_helper():
    """A new interpolation in these modules is written with ``lit`` or ``ident``; one that is
    not a registered value is added to ``REVIEWED`` with what it is."""
    found: set[tuple[str, str, str]] = set()
    for module in DUCKDB_DDL_MODULES:
        found |= _unquoted_interpolations(module, (FEDERATION / module).read_text())
    unreviewed = sorted(found - REVIEWED)
    assert not unreviewed, "statement text built from an unquoted value:\n" + "\n".join(
        f"  {m}:{f}: {{{e}}}" for m, f, e in unreviewed
    )
    stale = sorted(REVIEWED - found)
    assert not stale, f"REVIEWED names interpolations that are gone: {stale}"


def test_the_two_modules_keep_no_quoter_of_their_own():
    for module in DUCKDB_DDL_MODULES:
        tree = ast.parse((FEDERATION / module).read_text())
        own = [
            n.name
            for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name in ("_quoted", "_sql_text")
        ]
        assert not own, f"{module} defines {own}: use provisa.federation.duckdb_quoting"
