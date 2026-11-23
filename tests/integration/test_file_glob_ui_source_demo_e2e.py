# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3b5e17-2a6c-4f90-9e41-c7b02f6d1a83
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-788): on the default demo, a File Crawler source created in the admin with
``complaints`` registered through the Register Table form's "Or files matching" + combine reads as
ONE table over every matching file, right after it is registered. (A restart is covered by
test_file_glob_ui_source_e2e: the demo's boot does not keep a table registered in the admin.)

The maintainer's local-dev flow, as the ``test`` instance: the smoke check's isolated instance of
the default demo (PROVISA_DEMO, embedded PostgreSQL control plane, DuckDB engine, the install
configuration), driven call for call as the UI drives it: ``createSource``, the form's discovery
(``availableSchemas``, ``availableTables``, ``fileGlobPreview``, ``availableColumnsMetadata`` of
the reference file), ``suggestTableAlias``, ``registerTables`` with the combined table's
``fileGlob``; then the Tables page's data tab statement. The read failed: "Table with name
complaints does not exist! Did you mean _src_test.test.complaints_2026_09"."""

# Requirements: REQ-788

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
import yaml

from tests.smoke import test_main_is_startable as smoke
from tests.smoke.test_main_is_startable import REPO, Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.local_platform]

_ROLE = "org_admin"
_HEADERS = {"X-Provisa-Role": _ROLE}
_SOURCE = "test"
# A domain of the demo's own configuration.
_DOMAIN = "pet-store"


@pytest.fixture(scope="module")
def instance(tmp_path_factory) -> Iterator[Instance]:
    """The smoke check's instance of the default demo on the install configuration, as
    ``start-ui-install.sh --demo`` runs it."""
    from _pytest.monkeypatch import MonkeyPatch

    home = tmp_path_factory.mktemp("file_glob_demo")
    config = yaml.safe_load((REPO / smoke.CONFIG).read_text())
    config["includes"] = [str(REPO / "config" / "provisa-install.yaml")]
    path = home / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    mp = MonkeyPatch()
    mp.setattr(smoke, "CONFIG", str(path))
    try:
        yield from smoke.instance.__wrapped__()
    finally:
        mp.undo()


def _log_tail(inst: Instance) -> str:
    return (inst.log_dir / "backend.log").read_text()[-6000:]


def _gql(inst: Instance, query: str, variables: dict | None = None) -> dict[str, Any]:
    r = httpx.post(
        f"{inst.url}/admin/graphql",
        json={"query": query, "variables": variables or {}},
        headers=_HEADERS,
        timeout=300,
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert "errors" not in body, body
    return body["data"]


def _listed(inst: Instance, query: str, variables: dict, field: str) -> Any:
    """A discovery answer; a source whose server is still starting answers STARTING and is asked
    again, as the admin's pages ask it again."""
    deadline = time.monotonic() + 300
    while True:
        r = httpx.post(
            f"{inst.url}/admin/graphql",
            json={"query": query, "variables": variables},
            headers=_HEADERS,
            timeout=300,
        )
        assert r.status_code == 200, r.text
        body = r.json()
        errors = body.get("errors") or []
        if not errors and body["data"].get(field):
            return body["data"][field]
        assert all(e["message"].startswith("STARTING:") for e in errors), body
        assert time.monotonic() < deadline, body
        time.sleep(2)


_last: dict[str, str] = {}


def _data_tab_rows(inst: Instance, relation: str) -> list[dict] | None:
    """The Tables page's data tab (GovernedTableViewer): its first page, the statement
    nativeParams.pagedViewerSql writes over naming.tableSqlRef, sent as admin.runSql sends it."""
    r = httpx.post(
        f"{inst.url}/data/sql",
        json={"sql": f"SELECT * FROM {relation} LIMIT 101 OFFSET 0", "paged": True},
        headers=_HEADERS,
        timeout=120,
    )
    if r.status_code != 200 or "errors" in r.json() or not r.json()["data"]["sql"]:
        _last["read"] = f"{r.status_code} {r.text[:800]}"
        return None
    return r.json()["data"]["sql"]


def _assert_union(inst: Instance, relation: str) -> None:
    deadline = time.monotonic() + 120
    while (rows := _data_tab_rows(inst, relation)) is None:
        assert time.monotonic() < deadline, f"{_last['read']}\n{_log_tail(inst)}"
        time.sleep(2)
    got = {int(r["id"]): int(r["amount"]) for r in rows}
    # Both matching files; other.csv is not matched.
    assert got == {1: 10, 2: 20, 3: 30}, got


def test_a_combined_glob_table_on_the_demo_reads_every_file(instance: Instance, tmp_path):
    data = tmp_path / "drop"
    data.mkdir()
    (data / "complaints_2026_09.csv").write_text("id,amount\n1,10\n2,20\n")
    (data / "complaints_2026_10.csv").write_text("id,amount\n3,30\n")
    (data / "other.csv").write_text("id,amount\n99,990\n")
    created = _gql(
        instance,
        "mutation($input: SourceInput!) { createSource(input: $input) { success message } }",
        {"input": {"id": _SOURCE, "type": "files", "path": str(data)}},
    )["createSource"]
    assert created["success"], created

    schema = _listed(
        instance,
        "query($s: String!) { availableSchemas(sourceId: $s) }",
        {"s": _SOURCE},
        "availableSchemas",
    )[0]
    offered = [
        t["name"]
        for t in _listed(
            instance,
            "query($s: String!, $n: String!) { availableTables(sourceId: $s, schemaName: $n) "
            "{ name } }",
            {"s": _SOURCE, "n": schema},
            "availableTables",
        )
    ]
    preview = _listed(
        instance,
        "query($s: String!, $g: String, $t: [String!]) { fileGlobPreview(sourceId: $s, glob: $g, "
        "tables: $t) { glob referenceTable files { file table } differing { file } } }",
        {"s": _SOURCE, "g": "complaints_2026_*.csv", "t": offered},
        "fileGlobPreview",
    )
    assert len(preview["files"]) == 2 and not preview["differing"], preview
    columns = _listed(
        instance,
        "query($s: String!, $n: String!, $t: String!) { availableColumnsMetadata(sourceId: $s, "
        "schemaName: $n, tableName: $t) { name dataType } }",
        {"s": _SOURCE, "n": schema, "t": preview["referenceTable"]},
        "availableColumnsMetadata",
    )
    alias = _gql(
        instance,
        "query($t: String!, $d: String!, $s: String!) "
        "{ suggestTableAlias(tableName: $t, domainId: $d, sourceId: $s) }",
        {"t": "complaints", "d": _DOMAIN, "s": _SOURCE},
    )["suggestTableAlias"]
    outcomes = _gql(
        instance,
        "mutation($inputs: [TableInput!]!) { registerTables(inputs: $inputs) "
        "{ tableName result { success message } } }",
        {
            "inputs": [
                {
                    "sourceId": _SOURCE,
                    "domainId": _DOMAIN,
                    "schemaName": schema,
                    "tableName": "complaints",
                    **({"alias": alias} if alias else {}),
                    "fileGlob": preview["glob"],
                    "sourceFileColumn": "src_file",
                    "discover": False,
                    "columns": [
                        {"name": c["name"], "visibleTo": [_ROLE], "dataType": c["dataType"]}
                        for c in columns
                    ],
                }
            ]
        },
    )["registerTables"]
    assert all(o["result"]["success"] for o in outcomes), json.dumps(outcomes)

    # naming.tableSqlRef: the domain's SQL name, then the alias or the table's name.
    relation = f'"pet_store"."{alias or "complaints"}"'
    _assert_union(instance, relation)
