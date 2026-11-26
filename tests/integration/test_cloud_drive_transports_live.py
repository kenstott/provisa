# Copyright (c) 2026 Kenneth Stott
# Canary: b5e81c0a-3f2d-47a9-8e64-0d9c7a2f1b38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Live: onedrive:// and gdrive:// File Crawler transports against the real drive APIs (REQ-2280).

Each test creates its own folder for the run (named with the run's id, and stamped with the run
id and time in the folder's description), puts one CSV in it, registers a File Crawler source on
the transport, and checks the crawl finds the CSV as a table whose rows read the same as the same
file read through file://. Teardown deletes the folder and reads it back, failing when it is still
there (REQ-254; paid-cloud rule: create, test, destroy). No standing folder is relied on.

Credentials (CI secrets; absent, the test fails naming what is missing -- a skip is a defect):

- OneDrive: M365_TENANT_ID, M365_CLIENT_ID, M365_CLIENT_SECRET (an app registration with the Graph
  ``Files.ReadWrite.All`` application permission) and M365_TEST_MAILBOXES (its first entry is the
  user whose drive the run writes to).
- Google Drive: GOOGLE_APPLICATION_CREDENTIALS (a service account key file) and
  GDRIVE_TEST_PARENT_ID, a shared drive (or a folder in one) where the service account is a
  Content manager. A service account owns no storage, so it can create the run's folder only in a
  shared drive, whose files the drive owns.
"""

# Requirements: REQ-2280
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pyarrow.csv as pac
import pytest

from provisa.api_source.oauth_grants import service_account_token
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.file_source.cloud_drive import DRIVE_API, GRAPH, drive_storage_options
from provisa.file_source.crawler import crawl_directory
from tests.integration.live_teardown import none_remain, run_every, run_name

pytestmark = [pytest.mark.integration]

CSV = b"id,amount,region\n1,9.5,east\n2,3.25,west\n3,12.0,east\n"
DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"
RUN = run_name("provisa-req2280", "-")
STAMP = f"run {RUN} created {datetime.now(timezone.utc).isoformat(timespec='seconds')}"


def _require(*names: str) -> dict[str, str]:
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        pytest.fail(
            f"cloud drive live test cannot run; not set: {', '.join(missing)}", pytrace=False
        )
    return {n: os.environ[n] for n in names}


@pytest.fixture(autouse=True)
def _org():
    token = set_current_org("req2280-live")  # tokens are held per organization
    yield
    reset_current_org(token)


def _reads_as_the_file_does(tmp_path, found: list[dict], url: str, opts: dict) -> None:
    """The crawl found ``url`` as a table of the file's columns, and its rows read through the
    transport are the rows of the same file read from disk (file://)."""
    from provisa.file_source.files_glob import remote_fs

    (entry,) = found
    assert entry["path"] == url
    assert [c["name"] for c in entry["tables"][0]["columns"]] == ["id", "amount", "region"]
    local = tmp_path / "orders.csv"
    local.write_bytes(CSV)
    drive_fs, path = remote_fs(url, opts)
    with drive_fs.open(path) as f:
        assert pac.read_csv(f).equals(pac.read_csv(str(local)))


# --- OneDrive ----------------------------------------------------------------------------------


@pytest.mark.requires_microsoft_365
def test_onedrive_folder_csv_reads_as_the_file_does(tmp_path):
    env = _require("M365_TENANT_ID", "M365_CLIENT_ID", "M365_CLIENT_SECRET", "M365_TEST_MAILBOXES")
    user = env["M365_TEST_MAILBOXES"].split(",")[0].strip()
    source = SimpleNamespace(
        id="od-live",
        path=f"onedrive://{user}/{RUN}/*.csv",
        database=env["M365_TENANT_ID"],
        username=env["M365_CLIENT_ID"],
        password=env["M365_CLIENT_SECRET"],
        mapping={},
    )
    opts = drive_storage_options(source)
    token, client = opts["token"], opts["client"]
    drive = f"{GRAPH}/users/{user}/drive"

    def call(method: str, url: str, **kw) -> httpx.Response:
        resp = client.request(method, url, headers={"Authorization": f"Bearer {token()}"}, **kw)
        resp.raise_for_status()
        return resp

    folder = call(
        "POST",
        f"{drive}/root/children",
        json={"name": RUN, "folder": {}, "description": STAMP},
    ).json()
    try:
        call("PUT", f"{drive}/items/{folder['id']}:/orders.csv:/content", content=CSV)
        found = crawl_directory(source.path, storage_options=opts)
        _reads_as_the_file_does(tmp_path, found, f"onedrive://{user}/{RUN}/orders.csv", opts)
    finally:

        def delete() -> None:
            client.delete(
                f"{drive}/items/{folder['id']}", headers={"Authorization": f"Bearer {token()}"}
            ).raise_for_status()

        def read_back() -> None:
            resp = client.get(
                f"{drive}/items/{folder['id']}", headers={"Authorization": f"Bearer {token()}"}
            )
            none_remain(
                "the run's OneDrive folder", [folder["id"]] if resp.status_code != 404 else []
            )

        run_every([("delete the run's folder", delete), ("read the folder back", read_back)])


# --- Google Drive ------------------------------------------------------------------------------


@pytest.mark.requires_warehouse
def test_gdrive_folder_csv_reads_as_the_file_does(tmp_path):
    env = _require("GOOGLE_APPLICATION_CREDENTIALS", "GDRIVE_TEST_PARENT_ID")
    with open(env["GOOGLE_APPLICATION_CREDENTIALS"]) as fh:
        key = fh.read()
    json.loads(key)  # a key file that is no JSON fails here, not as an empty listing
    parent = env["GDRIVE_TEST_PARENT_ID"]
    probe = SimpleNamespace(
        id="gd-live",
        path=f"gdrive://{parent}",
        password=key,
        username="",
        database="",
        mapping={"gdrive_auth": "service_account"},
    )
    opts = drive_storage_options(probe)  # the transport reads with drive.readonly
    client = opts["client"]

    def token() -> str:  # the run's own folder is written with the full drive scope
        return service_account_token(key, [DRIVE_SCOPE])

    all_drives = {"supportsAllDrives": "true"}

    def call(method: str, url: str, **kw) -> httpx.Response:
        resp = client.request(method, url, headers={"Authorization": f"Bearer {token()}"}, **kw)
        resp.raise_for_status()
        return resp

    folder = call(
        "POST",
        f"{DRIVE_API}/files",
        params=all_drives,
        json={
            "name": RUN,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent],
            "description": STAMP,
        },
    ).json()
    try:
        metadata = json.dumps({"name": "orders.csv", "parents": [folder["id"]]})
        call(
            "POST",
            "https://www.googleapis.com/upload/drive/v3/files",
            params={"uploadType": "multipart", **all_drives},
            files={
                "metadata": ("metadata", metadata, "application/json; charset=UTF-8"),
                "file": ("orders.csv", CSV, "text/csv"),
            },
        )
        root = f"gdrive://{folder['id']}/*.csv"
        found = crawl_directory(root, storage_options=opts)
        _reads_as_the_file_does(tmp_path, found, f"gdrive://{folder['id']}/orders.csv", opts)
    finally:

        def delete() -> None:
            client.delete(
                f"{DRIVE_API}/files/{folder['id']}",
                params=all_drives,
                headers={"Authorization": f"Bearer {token()}"},
            ).raise_for_status()

        def read_back() -> None:
            resp = client.get(
                f"{DRIVE_API}/files/{folder['id']}",
                params=all_drives,
                headers={"Authorization": f"Bearer {token()}"},
            )
            none_remain("the run's Drive folder", [folder["id"]] if resp.status_code != 404 else [])

        run_every([("delete the run's folder", delete), ("read the folder back", read_back)])
