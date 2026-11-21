# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1e4f2b-6c3d-4e5f-9a7b-2d0c8e1f4a6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: a Google Sheets source on the DuckDB engine is a replica landed through the Google Sheets
API (REQ-1998, MOTHERDUCK LANDS THROUGH ITS POSTGRES ENDPOINT: "A Google Sheets source likewise
lands through the Google Sheets API where the engine has no gsheets extension"; REQ-1142,
ATTACHABLE MEANS STAGED; NOT ATTACHABLE MEANS REPLICA).

The engine runs DuckDB 2.0, and DuckDB's `gsheets` community extension has no 2.0 build: its
INSTALL is refused (community-extensions.duckdb.org answers HTTP 404 for v2.0.0), so the engine has
no live reach for a sheet and never attaches or loads gsheets. A sheet is landed instead: Provisa's
Sheets reader (provisa/events/sheets_reader.py, wired by build_adapter_loaders) reads it with
values.batchGet, signed in with the source's service account key, and the landing path writes the
rows as the replica, which queries read.

Runs live in the warehouse lane
-------------------------------
The Sheets API is enabled for the GCP project of GOOGLE_APPLICATION_CREDENTIALS /
GOOGLE_CLOUD_PROJECT (.env). The service account has zero Drive storage quota (a non-Workspace SA
cannot own Drive files, so spreadsheets.create is a hard 403), so both tests read a DURABLE fixture
sheet owned by a real Drive user and shared with the SA as reader -- header id,name + the _WIDGETS
rows -- whose id is GSHEETS_TEST_SHEET_ID (.env).
"""

from __future__ import annotations

import os
import types

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_CREDS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")

pytestmark.append(
    pytest.mark.skipif(
        not (_CREDS_PATH and os.path.exists(_CREDS_PATH) and _PROJECT),
        reason="GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_CLOUD_PROJECT not set to an existing "
        "service-account key (REQ-1097 setup)",
    )
)

_WIDGETS = [("w1", "Widget A"), ("w2", "Widget B"), ("w3", "Widget C")]

# REQ-1097: the durable fixture sheet (see the module docstring); the warehouse lane sets it.
_FIXTURE_SHEET_ID = os.environ.get("GSHEETS_TEST_SHEET_ID", "")


def _fixture_source(source_id: str):
    from provisa.core.models import Source, SourceType

    assert _FIXTURE_SHEET_ID, (
        "GSHEETS_TEST_SHEET_ID is the durable fixture sheet (header id,name + Widget A/B/C) shared "
        "with the service account; the warehouse lane sets it"
    )
    return Source(
        id=source_id,
        type=SourceType.google_sheets,
        database=_FIXTURE_SHEET_ID,
        mapping={"credentials_json": _CREDS_PATH},
    )


def _first_sheet_table(source):
    # The one table the Register Table picker offers, named after the source: the first sheet.
    return types.SimpleNamespace(
        table_name=source.id,
        schema_name="main",
        landing_filter=None,
        columns=[
            types.SimpleNamespace(name="id", data_type="varchar", native_filter_type=None),
            types.SimpleNamespace(name="name", data_type="varchar", native_filter_type=None),
        ],
    )


@pytest.mark.asyncio
async def test_google_sheets_read_through_duckdb_engine(tmp_path, monkeypatch):
    """DuckDB 2.0 has no gsheets build, so the DuckDB engine reads a sheet from its replica:
    federate() resolves it MATERIALIZED, the replica build reads it through the Sheets reader
    build_adapter_loaders wires, and the replica answers the query. gsheets is never loaded."""
    import json

    import duckdb
    import pyarrow as pa

    from provisa.events.app_wiring import build_adapter_loaders
    from provisa.events.source_loader import SourceRowLoader
    from provisa.federation.duckdb_extensions import (
        MANIFEST,
        _needs,
        install_statements,
        running_version_platform,
    )
    from provisa.federation.engine import build_engine, duckdb_engine_connectors
    from provisa.federation.strategy import Strategy, federate

    version, platform = running_version_platform()
    assert version.lstrip("v").startswith("2."), f"the engine's DuckDB is {version}, not 2.x"

    # The fact the route rests on: gsheets does not install on this DuckDB -- the statements the
    # server runs at start (install_extension_set) are refused, as community-extensions answers
    # HTTP 404 for a v2.0.0 gsheets build.
    probe = duckdb.connect()
    try:
        with pytest.raises(duckdb.Error):
            for statement in install_statements("gsheets", from_community=True):
                probe.execute(statement)
    finally:
        probe.close()

    # The server's extension set on this DuckDB: every extension the engine's connectors need,
    # gsheets aside, staged for the running version and platform (REQ-1142, ATTACHABLE MEANS
    # STAGED).
    needed = {need for connector in duckdb_engine_connectors() for need in _needs(connector)}
    staged = tmp_path / "ext" / version / platform
    staged.mkdir(parents=True)
    (staged / MANIFEST).write_text(
        json.dumps(
            {
                "duckdb": version,
                "platform": platform,
                "extensions": sorted(needed - {"gsheets"}),
            }
        )
    )
    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", str(tmp_path / "ext"))

    engine = build_engine("duckdb")
    source = _fixture_source("gsheets_itest")
    table = _first_sheet_table(source)
    assert engine.reachable("google_sheets")
    assert not engine.connectors["google_sheets"].reads_in_place
    assert federate(source, engine) is Strategy.MATERIALIZED

    state = types.SimpleNamespace(config=types.SimpleNamespace(sources=[]))
    loaders = build_adapter_loaders(state, engine)
    assert "google_sheets" in loaders  # the Sheets reader, not an engine attach

    columns = [("id", "text"), ("name", "text")]
    replica_source = SourceRowLoader(engine, adapter_loaders=loaders).replica_source(
        state, source, table, columns
    )
    landed = pa.Table.from_batches([b async for b in replica_source.batches(1000)])

    replica = duckdb.connect()
    try:
        replica.register("landed", landed)
        replica.execute(f'CREATE TABLE "{source.id}" AS SELECT * FROM landed')
        replica.unregister("landed")
        rows = replica.execute(f'SELECT id, name FROM "{source.id}" ORDER BY id').fetchall()
        gsheets_loaded = replica.execute(
            "SELECT count(*) FROM duckdb_extensions() WHERE extension_name = 'gsheets' AND loaded"
        ).fetchone()[0]
    finally:
        replica.close()
    assert rows == _WIDGETS
    assert gsheets_loaded == 0


@pytest.mark.asyncio
async def test_google_sheets_lands_through_the_sheets_api():
    """REQ-1998 (MOTHERDUCK LANDS THROUGH ITS POSTGRES ENDPOINT: a Google Sheets source lands
    through the Google Sheets API where the engine has no gsheets extension): the real loader
    build_adapter_loaders wires reads the durable fixture sheet with values.batchGet, signed in
    with the service account key as a source enters it (a path), into Arrow of the registered
    column types -- what the landing path writes as the replica. No DuckDB extension is loaded."""
    import pyarrow as pa

    from provisa.events.sheets_reader import make_google_sheets_loader

    source = _fixture_source("gsheets_land")
    table = _first_sheet_table(source)
    loader = make_google_sheets_loader()
    reader = loader.replica_source(source, table, [("id", "text"), ("name", "text")])
    landed = pa.Table.from_batches([b async for b in reader.batches(2)])
    assert landed.schema.types == [pa.string(), pa.string()]
    assert sorted(zip(landed.column("id").to_pylist(), landed.column("name").to_pylist())) == list(
        _WIDGETS
    )
    rows = await loader(source, table)
    assert sorted((r["id"], r["name"]) for r in rows) == list(_WIDGETS)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
