# Copyright (c) 2026 Kenneth Stott
# Canary: 4c1f7a92-8e3b-4d05-b6a1-7f2e9c0d3b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration (REQ-788): a File Crawler source created in the admin (Sources > New source), with
``complaints`` registered through the Register Table form's "Or files matching" + combine, reads
as ONE table over every matching file.

The maintainer's flow, call for call as the UI makes them: ``createSource`` (no configuration-file
source), the form's discovery (``availableSchemas``, ``availableTables``, ``fileGlobPreview``,
``availableColumnsMetadata`` of the reference file), then ``registerTables`` with the combined
table's ``fileGlob`` and source-file column. The read failed: "Table with name complaints does not
exist! Did you mean _src_test.test.complaints_2026_09" -- a source created in the admin is a
``sources`` row, never one of the configuration's sources, so its server was never started again
with the glob table. Read here through SQL and through the Tables page's data tab statement, and
again after a restart.

A real server (DuckDB engine, own control plane) over the bundled Calcite file adapter. Everything
here is the ``test`` instance."""

# Requirements: REQ-788

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_ROLE = "org_admin"
_HEADERS = {"X-Provisa-Role": _ROLE}
_SOURCE = "test"
_SRC_COL = "src_file"


def _config() -> dict:
    return {
        "federation_engine": "duckdb",
        "auth": {
            "provider": "none",
            "assignments_source": "provisa",
            "default_assignments": [{"domain_id": "*", "role_id": _ROLE}],
        },
        "naming": {"domain_prefix": False, "rules": []},
        "cache": {"enabled": False},
        "domains": [{"id": "ops", "description": "Ops"}],
        "roles": [],
        # No source here: the files source is created through the admin, as the UI creates it.
        "sources": [],
        "tables": [],
        "relationships": [],
        "rls_rules": [],
        "functions": [],
        "webhooks": [],
    }


@pytest.fixture
def served():
    from tests.integration.isolated_server import IsolatedServer

    workdir = tempfile.TemporaryDirectory()
    work = Path(workdir.name)
    data = work / "data"
    data.mkdir()
    (data / "complaints_2026_09.csv").write_text("id,amount\n1,10\n2,20\n")
    (data / "complaints_2026_10.csv").write_text("id,amount\n3,30\n")
    (data / "other.csv").write_text("id,amount\n99,990\n")  # not matched by the glob
    (work / "config.yaml").write_text(yaml.safe_dump(_config()))
    server = IsolatedServer(
        "file_glob_ui_source_duckdb",
        engine="duckdb",
        config=str(work / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'materialize.duckdb'}",
    )
    server.start()
    try:
        yield server, data
    finally:
        server.stop_process()
        workdir.cleanup()


def _gql(srv, query: str, variables: dict | None = None) -> dict:
    r = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={"query": query, "variables": variables or {}},
        headers=_HEADERS,
        timeout=120,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "errors" not in body, r.text
    return body["data"]


def _listed(srv, query: str, variables: dict, field: str, *, seconds: float = 90):
    """A discovery answer, asked again while the source's server is still starting."""
    deadline = time.monotonic() + seconds
    while True:
        r = httpx.post(
            f"{srv.base_url}/admin/graphql",
            json={"query": query, "variables": variables},
            headers=_HEADERS,
            timeout=120,
        )
        body = r.json() if r.status_code == 200 else {}
        if body.get("data") and body["data"].get(field) and "errors" not in body:
            return body["data"][field]
        assert time.monotonic() < deadline, f"{field}: {r.status_code} {r.text[:800]}"
        time.sleep(2)


_last: dict[str, str] = {}


def _sql_rows(srv, sql: str, key: str) -> list[dict] | None:
    r = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql, "role": _ROLE},
        headers=_HEADERS,
        timeout=60,
    )
    if r.status_code != 200 or "errors" in r.json():
        _last[key] = f"{r.status_code} {r.text[:800]}"
        return None
    rows = r.json()["data"]["sql"]
    if not rows:
        _last[key] = f"200 but no rows: {r.text[:300]}"
        return None
    return rows


def _data_tab_rows(srv, relation: str, key: str) -> list[dict] | None:
    """The Tables page's data tab (GovernedTableViewer): its first page, the statement
    nativeParams.pagedViewerSql writes over naming.tableSqlRef, sent as admin.runSql sends it --
    the acting role in X-Provisa-Role only, paged."""
    r = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": f"SELECT * FROM {relation} LIMIT 101 OFFSET 0", "paged": True},
        headers=_HEADERS,
        timeout=60,
    )
    if r.status_code != 200 or "errors" in r.json() or not r.json()["data"]["sql"]:
        _last[key] = f"{r.status_code} {r.text[:800]}"
        return None
    return r.json()["data"]["sql"]


def _wait(fn, *, seconds: float, key: str):
    deadline = time.monotonic() + seconds
    while True:
        got = fn()
        if got:
            return got
        assert time.monotonic() < deadline, f"timed out on {key}: {_last.get(key)}"
        time.sleep(2)


def _assert_union(rows: list[dict]) -> None:
    got = {int(r["id"]): int(r["amount"]) for r in rows}
    # Both matching files; other.csv is not matched.
    assert got == {1: 10, 2: 20, 3: 30}, got


def _read_both(srv, relation: str) -> None:
    _assert_union(
        _wait(
            lambda: _sql_rows(srv, 'SELECT "id", "amount" FROM "complaints" ORDER BY "id"', "sql"),
            seconds=90,
            key="sql",
        )
    )
    _assert_union(
        _wait(lambda: _data_tab_rows(srv, relation, "data_tab"), seconds=90, key="data_tab")
    )


def _register_combined(srv, data: Path, glob: str) -> tuple[list[dict], str | None]:
    """The maintainer's flow, call for call as the UI makes it: a files source created in the
    admin, its schema and listed files, the preview of ``glob`` typed in "Or files matching", the
    reference file's columns, the suggested alias, then ``registerTables`` with the combined
    table. Answers what each registration answered and the alias sent."""
    created = _gql(
        srv,
        "mutation($input: SourceInput!) { createSource(input: $input) { success message } }",
        {"input": {"id": _SOURCE, "type": "files", "path": str(data)}},
    )["createSource"]
    assert created["success"], created

    # The Register Table form: the source's schema, its listed files, then "Or files matching".
    schemas = _listed(
        srv,
        "query($s: String!) { availableSchemas(sourceId: $s) }",
        {"s": _SOURCE},
        "availableSchemas",
    )
    schema = schemas[0]
    tables = _listed(
        srv,
        "query($s: String!, $n: String!) { availableTables(sourceId: $s, schemaName: $n) "
        "{ name } }",
        {"s": _SOURCE, "n": schema},
        "availableTables",
    )
    offered = [t["name"] for t in tables]
    assert "complaints_2026_09" in offered, offered
    preview = _listed(
        srv,
        "query($s: String!, $g: String, $t: [String!]) { fileGlobPreview(sourceId: $s, glob: $g, "
        "tables: $t) { glob referenceTable files { file table } differing { file } } }",
        {"s": _SOURCE, "g": glob, "t": offered},
        "fileGlobPreview",
    )
    assert preview["glob"] == glob, preview
    assert not preview["differing"], preview
    reference = preview["referenceTable"]
    columns = _listed(
        srv,
        "query($s: String!, $n: String!, $t: String!) { availableColumnsMetadata(sourceId: $s, "
        "schemaName: $n, tableName: $t) { name dataType } }",
        {"s": _SOURCE, "n": schema, "t": reference},
        "availableColumnsMetadata",
    )

    # The alias the form suggests (useManyTables.registrationOfTable), sent with the table.
    alias = _gql(
        srv,
        "query($t: String!, $d: String!, $s: String!) "
        "{ suggestTableAlias(tableName: $t, domainId: $d, sourceId: $s) }",
        {"t": "complaints", "d": "ops", "s": _SOURCE},
    )["suggestTableAlias"]

    # The combined table, as useManyTables.registrationOfTable sends it through registerTables:
    # the reference file's columns, the glob, and the source-file column named in the form.
    outcomes = _gql(
        srv,
        "mutation($inputs: [TableInput!]!) { registerTables(inputs: $inputs) "
        "{ tableName result { success message code params } } }",
        {
            "inputs": [
                {
                    "sourceId": _SOURCE,
                    "domainId": "ops",
                    "schemaName": schema,
                    "tableName": "complaints",
                    **({"alias": alias} if alias else {}),
                    "fileGlob": preview["glob"],
                    "sourceFileColumn": _SRC_COL,
                    "discover": False,
                    "columns": [
                        {"name": c["name"], "visibleTo": [_ROLE], "dataType": c["dataType"]}
                        for c in columns
                    ],
                }
            ]
        },
    )["registerTables"]
    return outcomes, alias


def test_a_combined_glob_table_on_an_admin_created_files_source_reads_every_file(served):
    srv, data = served
    outcomes, alias = _register_combined(srv, data, "complaints_2026_*.csv")
    assert all(o["result"]["success"] for o in outcomes), json.dumps(outcomes)

    # naming.tableSqlRef: the domain's SQL name, then the alias or the table's name.
    relation = f'"ops"."{alias or "complaints"}"'
    _read_both(srv, relation)

    # After a restart, the registered glob table is still the union under its name.
    srv.restart()
    _read_both(srv, relation)


def test_a_pattern_naming_no_file_type_reads_as_the_type_its_files_share(served):
    """REQ-2234, A PATTERN WITH NO FILE TYPE: ``complaints*`` (no extension) over CSV files is
    read as CSV -- the maintainer's failure, where ``complaints*.csv`` worked."""
    srv, data = served
    outcomes, alias = _register_combined(srv, data, "complaints*")
    assert all(o["result"]["success"] for o in outcomes), json.dumps(outcomes)
    relation = f'"ops"."{alias or "complaints"}"'
    _read_both(srv, relation)
    srv.restart()
    _read_both(srv, relation)


def test_a_pattern_naming_no_file_type_over_two_types_is_refused_naming_both(served):
    """REQ-2234: files of more than one type cannot be one table; registration is refused by
    name, listing the types found, and no table is registered."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    srv, data = served
    pq.write_table(pa.table({"id": [4], "amount": [40]}), data / "complaints_2026_11.parquet")
    outcomes, _alias = _register_combined(srv, data, "complaints*")
    assert len(outcomes) == 1, outcomes
    result = outcomes[0]["result"]
    assert result["success"] is False, json.dumps(outcomes)
    assert result["code"] == "schema.file_types_differ", json.dumps(outcomes)
    assert result["params"]["types"] == "csv, parquet", json.dumps(outcomes)
    rows = _sql_rows(srv, 'SELECT "id" FROM "complaints"', "refused")
    assert rows is None, rows
