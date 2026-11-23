# Copyright (c) 2026 Kenneth Stott
# Canary: 9a7e2c54-1d36-4f0b-8b6e-2f4c8d1a7e95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration (REQ-788): a glob table registered through the admin API on a files source whose
server is already running is ONE table over every matching file, under its registered name.

The Register Table form's "Or files matching" registers ``complaints`` over
``complaints_2026_*.csv`` on a files source whose server was started by an earlier read (its
discovery does the same). The read of ``complaints`` failed: "Table with name complaints does not
exist! Did you mean _src_test.test.complaints_2026_09" -- the server's model, built when it
started, had only the per-file tables. Here the read returns the rows of both matching files (and
not the non-matching one) with each row's source-file column, and again after a restart.

A real server (DuckDB engine, own control plane) over the bundled Calcite file adapter. Everything
here is the ``test`` instance."""

# Requirements: REQ-788

from __future__ import annotations

import tempfile
import time
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

pytest.importorskip("duckdb")

_ROLE = "org_admin"
_SRC_COL = "src_file"


def _config(data_dir: str) -> dict:
    v = {"visible_to": [_ROLE]}
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
        # org_admin is the reserved administrative role (REQ-1349): a config may not declare it.
        "roles": [],
        "sources": [{"id": "test", "type": "files", "path": data_dir}],
        # One per-file table, read first: it starts the source's server before the glob table
        # is registered, as the source's discovery does in the UI.
        "tables": [
            {
                "source_id": "test",
                "domain_id": "ops",
                "schema": "test",
                "table": "complaints_2026_09",
                "columns": [
                    {"name": "id", "data_type": "integer", "is_primary_key": True, **v},
                    {"name": "amount", "data_type": "integer", **v},
                ],
            }
        ],
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
    (work / "config.yaml").write_text(yaml.safe_dump(_config(str(data))))
    server = IsolatedServer(
        "file_glob_registered_later_duckdb",
        engine="duckdb",
        config=str(work / "config.yaml"),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'materialize.duckdb'}",
    )
    server.start()
    try:
        yield server
    finally:
        server.stop_process()
        workdir.cleanup()


_last: dict[str, str] = {}


def _rows(srv, sql: str, key: str) -> list[dict] | None:
    r = httpx.post(
        f"{srv.base_url}/data/sql",
        json={"sql": sql, "role": _ROLE},
        headers={"X-Provisa-Role": _ROLE},
        timeout=60,
    )
    if r.status_code != 200 or "errors" in r.json():
        _last[key] = f"{r.status_code} {r.text[:500]}"
        return None
    rows = r.json()["data"]["sql"]
    if not rows:
        _last[key] = f"200 but no rows: {r.text[:300]}"
        return None
    return rows


def _wait(fn, *, seconds: float, key: str):
    deadline = time.monotonic() + seconds
    while True:
        got = fn()
        if got:
            return got
        assert time.monotonic() < deadline, f"timed out on {key}: {_last.get(key)}"
        time.sleep(2)


def _combined(srv) -> dict[int, str] | None:
    """{id -> source-file value} read through the registered glob table ``complaints``."""
    rows = _rows(srv, f'SELECT "id", "{_SRC_COL}" FROM "complaints" ORDER BY "id"', "complaints")
    return None if rows is None else {int(r["id"]): str(r[_SRC_COL]) for r in rows}


def _assert_union(got: dict[int, str]) -> None:
    assert set(got) == {1, 2, 3}, got  # both matching files; other.csv is not matched
    assert all("complaints_2026_09.csv" in got[i] for i in (1, 2)), got
    assert "complaints_2026_10.csv" in got[3], got


def test_a_glob_table_registered_on_a_running_source_reads_every_matching_file(served):
    srv = served
    # The per-file table is read: the source's server is running, its model without the glob.
    first = _wait(
        lambda: _rows(srv, 'SELECT "id" FROM "complaints_2026_09"', "per_file"),
        seconds=60,
        key="per_file",
    )
    assert sorted(int(r["id"]) for r in first) == [1, 2]

    r = httpx.post(
        f"{srv.base_url}/admin/graphql",
        json={
            "query": 'mutation { registerTable(input: {sourceId: "test", domainId: "ops", '
            'schemaName: "test", tableName: "complaints", fileGlob: "complaints_2026_*.csv", '
            f'sourceFileColumn: "{_SRC_COL}", columns: ['
            '{name: "id", visibleTo: ["org_admin"], dataType: "integer", isPrimaryKey: true}, '
            '{name: "amount", visibleTo: ["org_admin"], dataType: "integer"}, '
            f'{{name: "{_SRC_COL}", visibleTo: ["org_admin"], dataType: "varchar"}}'
            "]}) { success message code } }"
        },
        headers={"X-Provisa-Role": _ROLE},
        timeout=120,
    )
    assert r.status_code == 200, r.text
    assert "errors" not in r.json(), r.text
    result = r.json()["data"]["registerTable"]
    assert result["success"] is True, result

    _assert_union(_wait(lambda: _combined(srv), seconds=90, key="complaints"))
    # The per-file table still reads its own file alone.
    again = _rows(srv, 'SELECT "id" FROM "complaints_2026_09"', "per_file")
    assert again is not None, _last.get("per_file")
    assert sorted(int(r["id"]) for r in again) == [1, 2]

    # After a restart, the registered glob table is still the union under its name.
    srv.restart()
    _assert_union(_wait(lambda: _combined(srv), seconds=90, key="complaints"))
