# Copyright (c) 2026 Kenneth Stott
# Canary: 245400fb-99ea-4b9a-88fb-e7a89a4d54f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1690: DuckDB attaches a Calcite connector's bundled pgwire server LIVE.

The real bundle (resolved and cached by the runtime_deps resolver, a real JVM), a real DuckDB
``ATTACH ... (TYPE postgres, READ_ONLY)`` through the connector's ``details()``, and real reads:
catalog introspection over pg_catalog, the binary COPY scan, and filter/projection pushdown. The
``pgwire-file`` bundle stands in for sharepoint/splunk — the same Calcite pgwire server, fed a
directory of CSVs instead of a SaaS API, so the test needs no credentials.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from provisa.compiler.context import build_context
from provisa.compiler.introspect import ColumnMetadata
from provisa.compiler.parser import parse_query
from provisa.compiler.rls import RLSContext
from provisa.compiler.schema_gen import SchemaInput, generate_schema
from provisa.compiler.sql_gen import compile_query
from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical
from provisa.compiler.stage2 import apply_governance, build_governance_context
from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.federation.connector_duckdb import _DuckDBPgwireConnector
from provisa.transpiler.transpile import transpile
from tests.helpers import ALL_DATA_CAPABILITIES, registry_write_ops, registry_write_returns_rows

_ADMIN = {"id": "admin", "capabilities": ALL_DATA_CAPABILITIES, "domain_access": ["*"]}

pytestmark = [pytest.mark.integration]


class _FilesOverPgwire(_DuckDBPgwireConnector):
    source_type = "files"


@pytest.fixture
def csv_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data"
    d.mkdir()
    (d / "items.csv").write_text("id,name,qty\n1,alpha,10\n2,beta,20\n3,gamma,30\n")
    return d


@pytest.fixture
def source(csv_dir: Path) -> Source:
    return Source(id="probe-files", type=SourceType.files, path=str(csv_dir))


@pytest.fixture
def attached(source: Source):
    details = _FilesOverPgwire().details(source)  # starts the server, waits for its listener
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(details["attach"])
    try:
        yield con, details
    finally:
        con.close()
        pr.stop_all_servers()


def test_attach_details_name_the_endpoint_and_schema(source: Source):
    details = _FilesOverPgwire().details(source)
    try:
        assert details["raw_alias"] == "_src_probe-files"
        assert details["remote_schema"] == "probe_files"
        assert details["attach"].endswith('AS "_src_probe-files" (TYPE postgres, READ_ONLY)')
        # the same source reuses its server: one endpoint, one port
        assert _FilesOverPgwire().details(source) == details
    finally:
        pr.stop_all_servers()


def test_duckdb_lists_the_connector_tables(attached):
    con, details = attached
    rows = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables WHERE table_catalog = ?",
        [details["raw_alias"]],
    ).fetchall()
    assert (details["remote_schema"], "items") in rows


def test_duckdb_reads_rows_and_pushes_filters_down(attached):
    con, details = attached
    rel = f'"{details["raw_alias"]}"."{details["remote_schema"]}"."items"'
    assert con.execute(f"SELECT id, name, qty FROM {rel} ORDER BY id").fetchall() == [
        (1, "alpha", 10),
        (2, "beta", 20),
        (3, "gamma", 30),
    ]
    assert con.execute(f"SELECT name FROM {rel} WHERE qty > 15 ORDER BY qty").fetchall() == [
        ("beta",),
        ("gamma",),
    ]
    assert con.execute(f"SELECT sum(qty) FROM {rel}").fetchone() == (60,)


def test_attach_is_read_only(attached):
    con, details = attached
    rel = f'"{details["raw_alias"]}"."{details["remote_schema"]}"."items"'
    with pytest.raises(duckdb.Error):
        con.execute(f"INSERT INTO {rel} VALUES (4, 'delta', 40)")


def _governed_sql(schema: str, gql: str, rls: RLSContext) -> tuple[str, list]:
    """The pipeline's SQL and parameters for ``gql`` over ``schema.items``, governed by ``rls``, in
    DuckDB's dialect."""
    col = lambda n, t: ColumnMetadata(column_name=n, data_type=t, is_nullable=True)  # noqa: E731
    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "probe-files",
                "domain_id": "files",
                "schema_name": schema,
                "table_name": "items",
                "write_ops": registry_write_ops("files"),
                "write_returns_rows": registry_write_returns_rows("files"),
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]} for c in ("id", "name", "qty")
                ],
            }
        ],
        relationships=[],
        column_types={1: [col("id", "integer"), col("name", "varchar"), col("qty", "integer")]},
        naming_rules=[],
        role=_ADMIN,
        domains=[{"id": "files", "description": "Files"}],
        source_types={"probe-files": "files"},
    )
    ctx = build_context(si)
    compiled = compile_query(parse_query(generate_schema(si), gql, {}, ctx=ctx), ctx)[0]
    gov = build_governance_context("admin", rls, {}, ctx, si.tables, role=_ADMIN)
    sql = transpile(
        rewrite_semantic_to_physical(apply_governance(compiled.sql, gov), ctx), "duckdb"
    )
    return sql, list(compiled.params)


def test_a_string_filter_and_a_string_row_rule_read_through_the_attach(attached):
    """DuckDB pushes every string comparison to the pgwire server as ``= 'v' COLLATE "C"``; the
    Calcite bundle must answer it with the filtered rows (it refused COLLATE: "Failed to prepare
    COPY ..."), so a string filter and a string row rule both read live through the attach."""
    con, details = attached
    rel = f'"{details["raw_alias"]}"."{details["remote_schema"]}"."items"'
    con.execute("CREATE SCHEMA files_src")
    con.execute(f"CREATE VIEW files_src.items AS SELECT * FROM {rel}")
    rls = RLSContext(rules={1: "name <> 'gamma'"}, domain_rules={})

    sql, params = _governed_sql(
        "files_src", '{ items(where: {name: {eq: "beta"}}) { id name qty } }', rls
    )
    assert params == ["beta"] and "gamma" in sql  # both string predicates reach the attached scan
    rows = con.execute(sql, params).fetchall()
    assert len(rows) == 1 and "beta" in str(rows[0]), rows

    governed, params = _governed_sql("files_src", "{ items { id name qty } }", rls)
    names = str(con.execute(governed, params).fetchall())
    assert "alpha" in names and "beta" in names and "gamma" not in names, names
