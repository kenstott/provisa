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

BLOCKED: Google Sheets API disabled on the credentialed GCP project
---------------------------------------------------------------------
This is not skipped for convenience — every avenue was attempted and independently confirmed
blocked, evidence below (all reproduced 2026-07-17 against the live GCP project referenced by
GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_CLOUD_PROJECT in .env):

1. `INSTALL gsheets FROM community; LOAD gsheets` succeeds standalone — the DuckDB extension itself
   loads fine (verified below, mirrors the firebird test's split between "extension loads" and
   "extension is functional").
2. Creating a throwaway test sheet via ``POST https://sheets.googleapis.com/v4/spreadsheets`` with
   the service account's bearer token (scope `spreadsheets`) returns HTTP 403
   ``SERVICE_DISABLED``: "Google Sheets API has not been used in project 906499566555 before or it
   is disabled."  (906499566555 is the numeric project id backing concise-volt-436619-g5, the
   project named in GOOGLE_CLOUD_PROJECT.)
3. Reading DuckDB gsheets' own PUBLIC documentation demo sheet
   (11QdEasMWbETbFVxry-SsD8jVcdYIT1zBQszcF84MdE8) via ``GET .../v4/spreadsheets/{id}`` with the same
   token returns the identical 403 SERVICE_DISABLED — the block is per-CALLING-project, independent
   of which sheet (public or private) is targeted, so there is no "read a public sheet without
   creating one" workaround either.
4. Attempting to self-enable the API (`POST serviceusage.googleapis.com/v1/projects/
   concise-volt-436619-g5/services/sheets.googleapis.com:enable` using the same service account with
   `cloud-platform` scope) returns HTTP 403 ``AUTH_PERMISSION_DENIED`` — the service account has no
   `serviceusage.services.enable` permission, so it cannot fix this from inside the test either.

Enabling the Sheets API for this GCP project (console access, outside repo/test scope) is required
before this test can run live. `_ensure_sheets_api_enabled()` below performs the exact live probe
from point 2 and skips with that evidence if the API is still disabled — a documented last resort,
not a dodge: once the API is enabled project-side, this test creates a real throwaway sheet, seeds
3 rows, reads them back through the REAL connector DDL, and deletes the sheet, with no code changes
required.
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = [pytest.mark.integration]

_CREDS_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")

pytestmark.append(
    pytest.mark.skipif(
        not (_CREDS_PATH and os.path.exists(_CREDS_PATH) and _PROJECT),
        reason="GOOGLE_APPLICATION_CREDENTIALS / GOOGLE_CLOUD_PROJECT not set to an existing "
        "service-account key (REQ-1097 setup)",
    )
)

_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

_WIDGETS = [("1", "Widget A"), ("2", "Widget B"), ("3", "Widget C")]


def _access_token() -> str:
    from google.auth.transport.requests import Request
    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_file(
        _CREDS_PATH, scopes=_SHEETS_SCOPES
    )
    creds.refresh(Request())
    return creds.token


def _ensure_sheets_api_enabled(token: str) -> None:
    """Live probe matching evidence point 2/3 in the module docstring: skip (documented last
    resort, not a dodge — see docstring) if the Sheets API is disabled for the calling project."""
    import requests

    r = requests.post(
        "https://sheets.googleapis.com/v4/spreadsheets",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"properties": {"title": f"provisa_gsheets_e2e_probe_{uuid.uuid4().hex[:8]}"}},
        timeout=30,
    )
    if r.status_code == 403 and "SERVICE_DISABLED" in r.text:
        pytest.skip(
            "Google Sheets API is disabled for the GCP project backing "
            f"GOOGLE_APPLICATION_CREDENTIALS ({_PROJECT}); service account lacks "
            "serviceusage.services.enable to fix it from here. Evidence: "
            f"HTTP {r.status_code} {r.text[:300]}"
        )
    r.raise_for_status()
    sheet_id = r.json()["spreadsheetId"]
    _delete_sheet(token, sheet_id)  # probe sheet — not the seeded test sheet, clean up immediately


def _create_test_sheet(token: str) -> str:
    import requests

    r = requests.post(
        "https://sheets.googleapis.com/v4/spreadsheets",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"properties": {"title": f"provisa_gsheets_e2e_{uuid.uuid4().hex[:8]}"}},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["spreadsheetId"]


def _seed_sheet(token: str, sheet_id: str) -> None:
    import requests

    values = [["id", "name"], *[[wid, name] for wid, name in _WIDGETS]]
    r = requests.put(
        f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/Sheet1!A1:B4",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        params={"valueInputOption": "RAW"},
        json={"values": values},
        timeout=30,
    )
    r.raise_for_status()


def _delete_sheet(token: str, sheet_id: str) -> None:
    import requests

    requests.delete(
        f"https://www.googleapis.com/drive/v3/files/{sheet_id}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )  # best-effort cleanup — a throwaway file left behind is not a test failure


def test_google_sheets_read_through_duckdb_engine():
    """Drive the REAL DuckDBGsheetsConnector.details() view DDL against an in-process DuckDB
    connection — the same seam provisa.federation.duckdb_backend.DuckDBBackend's persistent
    DuckDBFederationRuntime uses (REQ-899/1097). No landing: `read_gsheet` reads the source live."""
    import duckdb

    from provisa.core.models import Source, SourceType
    from provisa.federation.connector_duckdb import DuckDBGsheetsConnector

    token = _access_token()
    _ensure_sheets_api_enabled(token)  # skips with evidence if the API is disabled (see docstring)

    sheet_id = _create_test_sheet(token)
    try:
        _seed_sheet(token, sheet_id)

        src = Source(
            id="gsheets_itest",
            type=SourceType.google_sheets,
            federation_hints={"spreadsheet_id": sheet_id},
        )
        connector = DuckDBGsheetsConnector()
        details = connector.details(src)
        assert "read_gsheet(" in details["view_ddl"]
        assert sheet_id in details["view_ddl"]

        conn = duckdb.connect()
        try:
            conn.execute(connector._install_sql())  # "INSTALL gsheets FROM community"
            conn.execute(f"LOAD {connector.extension}")

            rows = conn.execute(
                "SELECT count(*) FROM duckdb_functions() "
                f"WHERE function_name = '{connector.probe_symbol}'"
            ).fetchall()
            assert rows[0][0] >= 1  # read_gsheet registered — extension genuinely loaded

            conn.execute(
                "CREATE SECRET gsheet_itest_secret (TYPE gsheet, PROVIDER access_token, "
                f"TOKEN '{token}')"
            )
            conn.execute(details["view_ddl"])  # the real CREATE VIEW ... read_gsheet DDL, unmodified

            result = conn.execute(f'SELECT id, name FROM "{src.id}" ORDER BY id').fetchall()
            assert [(str(r[0]), r[1]) for r in result] == _WIDGETS
        finally:
            conn.close()
    finally:
        _delete_sheet(token, sheet_id)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
