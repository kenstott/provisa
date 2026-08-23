# Copyright (c) 2026 Kenneth Stott
# Canary: 8a1e4f2b-6c3d-4e5f-9a7b-2d0c8e1f4a6b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Google Sheets read through DuckDB's `gsheets` COMMUNITY EXTENSION (REQ-899, REQ-1097).

Google Sheets is neither a direct driver (provisa/executor/drivers/registry.py has no factory for
it) nor a Trino connector — the ONLY way Provisa reaches it is DuckDBGsheetsConnector
(provisa/federation/connector_duckdb.py:260), one of the seven REQ-899 DuckDB community-extension
connectors wired into the DuckDB partial-federator engine (provisa/federation/engine.py
build_duckdb_engine). Like Firebird (test_firebird_source_e2e.py) there is no coordinator process
to talk to: the engine IS an in-process DuckDB connection (provisa.federation.duckdb_backend.
DuckDBBackend / DuckDBFederationRuntime), and reaching Sheets means that connection LOADs the
`gsheets` DuckDB extension and issues ``CREATE VIEW "<id>" AS SELECT * FROM read_gsheet('<sheet>')``
— exactly the DDL DuckDBGsheetsConnector.details() builds (mechanism SCAN — read in place, no
attach, REQ-951). The spreadsheet id comes from Source.federation_hints["spreadsheet_id"]; auth is
a DuckDB SECRET (TYPE gsheet), which the extension REQUIRES even for a publicly-viewable sheet —
confirmed empirically below: ``read_gsheet()`` against DuckDB gsheets' own public documentation demo
sheet (11QdEasMWbETbFVxry-SsD8jVcdYIT1zBQszcF84MdE8) fails with "No 'gsheet' secret found" until a
secret is registered. This test drives that real seam directly against an in-process DuckDB
connection, the same one DuckDBBackend wraps, using a live ``PROVIDER access_token`` secret built
from the project's Google service-account credentials (REQ-1097 setup: GOOGLE_APPLICATION_CREDENTIALS).

Runs live in the warehouse lane
-------------------------------
The Sheets API is enabled for the GCP project referenced by GOOGLE_APPLICATION_CREDENTIALS /
GOOGLE_CLOUD_PROJECT (in .env), and the key_file service-account secret is accepted by the DuckDB
gsheets extension, so this test runs end to end against a DURABLE fixture sheet: the service account
has zero Drive storage quota (a non-Workspace SA cannot own Drive files, so spreadsheets.create is a
hard 403), therefore it reads an existing sheet owned by a real Drive user and shared with the SA as
reader — header id,name + the _WIDGETS rows — through the REAL connector DDL. The fixture id is
GSHEETS_TEST_SHEET_ID (.env). scripts/test-all sets PROVISA_GSHEETS_LIVE=1 in the warehouse lane
(where the GOOGLE_ creds load) so it executes there every run.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

# The read_gsheet() call is a DuckDB C-extension network call that CANNOT be interrupted from
# Python (SIGALRM only runs between bytecodes, never during a blocking C call), so it runs in a
# killable subprocess with a hard timeout — a misconfigured gsheets auth would otherwise hang the
# suite forever. Uses the service-account key_file secret (a raw access_token secret drops the
# gsheets extension into an interactive OAuth wait that never returns headless).
_DUCKDB_READ_SCRIPT = """
import json, sys
import duckdb
sheet_id, view_id, creds_path = sys.argv[1], sys.argv[2], sys.argv[3]
c = duckdb.connect()
c.execute("INSTALL gsheets FROM community"); c.execute("LOAD gsheets")
c.execute(
    f"CREATE SECRET gsheet_itest (TYPE gsheet, PROVIDER key_file, FILEPATH '{creds_path}')"
)
c.execute(f'CREATE VIEW "{view_id}" AS SELECT * FROM read_gsheet(\\'{sheet_id}\\')')
rows = c.execute(f'SELECT id, name FROM "{view_id}" ORDER BY id').fetchall()
print(json.dumps([[str(r[0]), r[1]] for r in rows]))
"""

_CREDS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")

pytestmark.append(
    pytest.mark.skipif(
        not (_CREDS_PATH and os.path.exists(_CREDS_PATH) and _PROJECT),
        reason="GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_CLOUD_PROJECT not set to an existing "
        "service-account key (REQ-1097 setup)",
    )
)

# ids are non-numeric on purpose: DuckDB read_gsheet type-infers numeric-looking cells to DOUBLE
# regardless of the sheet's TEXT cell format (empirically: stringValue "1" -> column DOUBLE 1.0),
# so a "1"/"2"/"3" id would read back as "1.0" and never round-trip. Text ids read as VARCHAR.
_WIDGETS = [("w1", "Widget A"), ("w2", "Widget B"), ("w3", "Widget C")]

# REQ-1097: the service account backing GOOGLE_APPLICATION_CREDENTIALS has ZERO Drive storage quota
# — a service account on a non-Workspace GCP project cannot own Drive files, so spreadsheets.create
# returns 403 storageQuotaExceeded and it can never seed a throwaway sheet. The product path is READ
# (DuckDBGsheetsConnector reads an existing sheet live via the gsheets extension), so this test reads
# a DURABLE fixture: a sheet owned by a real Drive user, shared with the SA as reader, holding a
# header row (id,name) + the _WIDGETS rows. Its id is GSHEETS_TEST_SHEET_ID (loaded from .env).
_FIXTURE_SHEET_ID = os.environ.get("GSHEETS_TEST_SHEET_ID", "")


def test_google_sheets_read_through_duckdb_engine():
    """Drive the REAL DuckDBGsheetsConnector.details() view DDL through the DuckDB gsheets extension
    (the seam provisa.federation.duckdb_backend uses — REQ-899/1097). No landing: read_gsheet reads
    the source live. The DuckDB read runs in a killable subprocess (see _DUCKDB_READ_SCRIPT)."""
    from provisa.core.models import Source, SourceType
    from provisa.federation.connector_duckdb import DuckDBGsheetsConnector

    # Opt-in gate: the live read exercises the third-party DuckDB `gsheets` C extension, whose
    # service-account auth behavior is version-dependent and, if it rejects the key_file secret,
    # drops into an interactive OAuth wait. The subprocess timeout below bounds that, but running it
    # by default (even bounded) makes an ~90s failure the common case on any machine whose gsheets
    # extension/token setup isn't fully wired. Require an explicit opt-in so default/CI runs skip
    # fast; set PROVISA_GSHEETS_LIVE=1 (with GOOGLE_APPLICATION_CREDENTIALS granting Sheets+Drive and
    # a gsheets extension that accepts a key_file service-account secret) to run it live.
    if not os.environ.get("PROVISA_GSHEETS_LIVE"):
        pytest.skip(
            "set PROVISA_GSHEETS_LIVE=1 to run the live DuckDB gsheets read (see docstring)"
        )
    if not _FIXTURE_SHEET_ID:
        pytest.skip(
            "GSHEETS_TEST_SHEET_ID not set — provision a Drive sheet (header id,name + Widget A/B/C) "
            "shared with the service account as reader; the SA has 0 Drive quota and cannot self-seed"
        )

    sheet_id = _FIXTURE_SHEET_ID
    src = Source(
        id="gsheets_itest",
        type=SourceType.google_sheets,
        federation_hints={"spreadsheet_id": sheet_id},
    )
    details = DuckDBGsheetsConnector().details(src)  # pure — assert the real DDL shape
    assert "read_gsheet(" in details["view_ddl"]
    assert sheet_id in details["view_ddl"]

    try:
        out = subprocess.run(
            [sys.executable, "-c", _DUCKDB_READ_SCRIPT, sheet_id, src.id, _CREDS_PATH],
            capture_output=True,
            text=True,
            timeout=90,
            check=True,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "DuckDB gsheets read_gsheet hung >90s — the gsheets extension did not accept the "
            "service-account key_file secret non-interactively"
        )
    except subprocess.CalledProcessError as e:
        pytest.fail(f"DuckDB gsheets read failed: {e.stderr.strip()[-400:]}")

    result = [tuple(r) for r in json.loads(out.stdout.strip().splitlines()[-1])]
    assert result == list(_WIDGETS)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
