# Copyright (c) 2026 Kenneth Stott
# Canary: 7b1e9c04-5a2d-4e6f-9c88-3d21a7f0e5b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: config-driven custom source connectors reach real sources through the native runtimes (REQ-1177).

The unit tests (tests/unit/test_custom_connectors.py) prove the descriptor emits the right DDL shape.
These tests prove the descriptor actually DRIVES a live attach — an operator declares a source_type in
config/custom_connectors.yaml (via PROVISA_CUSTOM_CONNECTORS) and the DuckDB federation runtime reaches
that source with NO code change, exercising both conformance branches the requirement names:

  * DuckDB ATTACH  → ducklake (duckdb/ducklake): a URI-scheme DSN + extra ATTACH option (DATA_PATH),
    over a local ducklake catalog + parquet — no server.
  * DuckDB SCAN    → excel / read_xlsx (duckdb/duckdb-excel): a scan table-function with NAMED ARGS
    (sheet/header), over a local .xlsx — no server.

Both are self-contained: the only external dependency is DuckDB autoloading the extension on first use
(proven present by the connector probe path). This is the engine primitive, not the HTTP/routing path.
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

duckdb = pytest.importorskip("duckdb")
openpyxl = pytest.importorskip("openpyxl")

from provisa.core.catalog import _to_catalog_name  # noqa: E402
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402


def _write_connectors_yaml(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "custom_connectors.yaml"
    p.write_text(textwrap.dedent(body))
    return p


def _src(sid: str, typ: str, table: str, *, path: str | None = None, hints: dict | None = None):
    return SimpleNamespace(
        id=sid,
        type=SimpleNamespace(value=typ),
        schema_name="main",
        table_name=table,
        path=path,
        federation_hints=hints or {},
    )


def _phys(source) -> str:
    return f'"{_to_catalog_name(source.id)}"."{source.schema_name}"."{source.table_name}"'


async def test_config_driven_duckdb_attach_federates_ducklake(tmp_path, monkeypatch):
    """DuckDB ATTACH branch: a config-declared `ducklake` source_type attaches a real local ducklake
    catalog and its rows come back through the runtime — the operator wrote zero code."""
    cat = tmp_path / "cat.ducklake"
    data = tmp_path / "lake_data"
    data.mkdir()

    # Seed the ducklake catalog out-of-band (a real writer), then close it before the runtime reads.
    seed = duckdb.connect()
    seed.execute(f"ATTACH 'ducklake:{cat}' AS lake (DATA_PATH '{data}')")
    seed.execute("CREATE TABLE lake.events(id INTEGER, kind VARCHAR)")
    seed.execute("INSERT INTO lake.events VALUES (1,'shipped'),(2,'placed'),(3,'shipped')")
    seed.close()

    cfg = _write_connectors_yaml(
        tmp_path,
        f"""
        connectors:
          - engine: duckdb
            source_type: ducklake
            kind: duckdb_attach
            extension: ducklake
            install_from_community: false
            probe_symbol: ducklake_snapshots
            mechanism: attach_rw
            attach_template: "ATTACH 'ducklake:{{path}}' AS \\"{{alias}}\\" (DATA_PATH '{{data_path}}')"
            remote_schema: main
        """,
    )
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))

    rt = DuckDBFederationRuntime()
    try:
        assert rt._engine.reachable("ducklake")  # descriptor made the new type reachable, no code
        src = _src("lake", "ducklake", "events", path=str(cat), hints={"data_path": str(data)})
        rt.attach_source(src)

        res = await rt.execute(f"SELECT id, kind FROM {_phys(src)} ORDER BY id")
        assert res.column_names == ["id", "kind"]
        assert [r[0] for r in res.rows] == [1, 2, 3]
        assert res.rows[0][1] == "shipped"
    finally:
        rt.close()


async def test_config_driven_duckdb_scan_federates_excel(tmp_path, monkeypatch):
    """DuckDB SCAN branch: a config-declared `excel` source_type reads a real .xlsx via read_xlsx with
    NAMED ARGS (sheet/header) and its rows come back through the runtime — no code, no server."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Q1"
    ws.append(["id", "name"])
    ws.append([1, "alice"])
    ws.append([2, "bob"])
    xlsx = tmp_path / "sales.xlsx"
    wb.save(xlsx)

    cfg = _write_connectors_yaml(
        tmp_path,
        f"""
        connectors:
          - engine: duckdb
            source_type: excel
            kind: duckdb_scan
            extension: excel
            install_from_community: false
            probe_symbol: read_xlsx
            scan_template: "read_xlsx('{{path}}', sheet='{{sheet}}', header=true)"
        """,
    )
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))

    rt = DuckDBFederationRuntime()
    try:
        assert rt._engine.reachable("excel")
        src = _src("sales", "excel", "sales", path=str(xlsx), hints={"sheet": "Q1"})
        rt.attach_source(src)

        res = await rt.execute(f"SELECT id, name FROM {_phys(src)} ORDER BY id")
        assert res.column_names == ["id", "name"]
        assert [r[0] for r in res.rows] == [1, 2]
        assert res.rows[1][1] == "bob"
    finally:
        rt.close()


async def test_unknown_kind_fails_loud(tmp_path, monkeypatch):
    """A descriptor typo (unknown kind) must fail loud at load — never a silent no-op that leaves a
    source_type quietly unreachable."""
    cfg = _write_connectors_yaml(
        tmp_path,
        """
        connectors:
          - engine: duckdb
            source_type: bogus
            kind: duckdb_teleport
            extension: nope
            probe_symbol: nope
            scan_template: "nope('{path}')"
        """,
    )
    monkeypatch.setenv("PROVISA_CUSTOM_CONNECTORS", str(cfg))
    with pytest.raises(ValueError, match="unknown kind"):
        DuckDBFederationRuntime()
