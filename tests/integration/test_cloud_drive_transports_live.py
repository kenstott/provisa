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
id and time in the folder's description), puts one CSV in a folder nested in it, registers a File
Crawler source on the transport, and checks the crawl finds the CSV as a table whose rows read the
same as the same file read through file://. Then the engines' read (REQ-2280, amended
2026-11-26): the source's Calcite pgwire server (the pinned pgwire-file bundle, whose file adapter
has the onedrive and gdrive storage providers) reads the table live from the drive, and its rows
are the file's. Teardown deletes the folder and reads it back, failing when it is still
there (REQ-254; paid-cloud rule: create, test, destroy). No standing folder is relied on.

Credentials (CI secrets; absent, the test fails naming what is missing -- a skip is a defect):

- OneDrive: M365_TENANT_ID, M365_CLIENT_ID, M365_CLIENT_SECRET (an app registration with the Graph
  ``Files.ReadWrite.All`` application permission) and M365_TEST_MAILBOXES (its first entry is the
  user whose drive the run writes to).
- Google Drive (the google_workspace lane; maintainer 2026-11-26): GWS_SERVICE_ACCOUNT_KEY_FILE
  (the path of a service account key file) and GWS_TEST_MAILBOXES (its first entry is the
  Workspace user the service account acts as). The domain has delegated the Drive scopes to the
  service account (domain-wide delegation), so it acts as that user: the run's folder is made in
  the user's My Drive with the full drive scope, and the source reads it with drive.readonly,
  naming the user as its ``gdrive_subject``. There is never a shared drive.
"""

# Requirements: REQ-2280
from __future__ import annotations

import asyncio
import base64
import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace

import httpx
import pyarrow.csv as pac
import pytest

from provisa.api_source.oauth_grants import access_token
from provisa.core.auth_models import ApiAuthGoogleServiceAccount
from provisa.core import org_files
from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.file_source.cloud_drive import DRIVE_API, GRAPH, drive_storage_options
from provisa.file_source.crawler import crawl_directory
from tests.integration.live_teardown import none_remain, run_every, run_name

pytestmark = [pytest.mark.integration]

CSV = b"id,amount,region\n1,9.5,east\n2,3.25,west\n3,12.0,east\n"
ROWS = [
    {"id": 1, "amount": 9.5, "region": "east"},
    {"id": 2, "amount": 3.25, "region": "west"},
    {"id": 3, "amount": 12.0, "region": "east"},
]
#: The folder the CSV is in, below the run's folder: a nested path, which the adapter's drive
#: providers address as more than one segment.
INNER = "inner"
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


def _ok(resp: httpx.Response) -> None:
    """Fails with the API's own error body (a Graph 404 names whether the user or the user's
    drive is missing), which raise_for_status leaves out."""
    if resp.is_error:
        pytest.fail(
            f"{resp.request.method} {resp.request.url}: {resp.status_code} {resp.text}",
            pytrace=False,
        )


#: Any one of these application roles lets an app-only token reach a user's OneDrive.
DRIVE_ROLES = ("Files.ReadWrite.All", "Files.Read.All", "Sites.ReadWrite.All")


def _preflight_app_only_drive(user: str, token: str, call_get) -> None:
    """Prints the token's `roles` claim (never the token) and GET /users/<user>/drive, UPN masked;
    fails naming the missing application permission or with the drive call's own body."""
    claims = json.loads(base64.urlsafe_b64decode(token.split(".")[1] + "=="))
    roles = claims.get("roles") or []
    print(f"onedrive preflight: token roles = {roles}", flush=True)
    resp = call_get(f"{GRAPH}/users/{user}/drive")
    body = resp.text.replace(user, "<upn>")
    print(f"onedrive preflight: GET /users/<upn>/drive -> {resp.status_code} {body}", flush=True)
    if not any(r in roles for r in DRIVE_ROLES):
        pytest.fail(
            f"the app-only token lacks an application permission for OneDrive "
            f"(need one of {', '.join(DRIVE_ROLES)}; token roles: {roles}). "
            f"GET drive: {resp.status_code} {body}",
            pytrace=False,
        )
    if resp.is_error:
        pytest.fail(f"GET /users/<upn>/drive: {resp.status_code} {body}", pytrace=False)


@pytest.fixture(autouse=True)
def _org():
    # The org's folder is made ready as its runtime's build does (REQ-1992); the source's pgwire
    # server keeps its state in it.
    org_files.folder("req2280_live").mkdir(parents=True, exist_ok=True)
    token = set_current_org("req2280_live")  # tokens are held per organization
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


def _print_server_log(source_id: str) -> None:
    """The end of the source's connector server's own log, so a failed read shows what the
    server's file adapter saw (the log is in the org's folder on this node)."""
    for log in org_files.folder("req2280_live").glob(
        f"nodes/*/pgwire/*/{source_id}/{pr.SERVER_LOG_NAME}"
    ):
        print(f"--- {log} (last 150 lines)")
        print("\n".join(log.read_text(errors="replace").splitlines()[-150:]))
        model = json.loads((log.parent / "model" / "model.json").read_text())
        for schema in model.get("schemas", []):
            operand = schema.get("operand", {})
            config = {
                k: ("<redacted>" if "ecret" in k or "assword" in k or k.endswith("Key") else v)
                for k, v in operand.get("storageConfig", {}).items()
            }
            print(f"--- model schema {schema.get('name')}: {dict(operand, storageConfig=config)}")


def _engine_reads_as_the_file_does(source: Source) -> None:
    """The source's Calcite pgwire server reads ``orders`` live from the drive (the file adapter's
    drive storage provider, signed in with the source's fields) and answers the file's rows."""
    replica = pr.ConnectorReplica(source)
    try:
        rows = asyncio.run(replica.load("orders"))
    except Exception:
        _print_server_log(source.id)
        raise
    finally:
        replica.close()
    assert sorted(rows, key=lambda r: r["id"]) == ROWS


# --- OneDrive ----------------------------------------------------------------------------------


# requires_warehouse keeps it out of the stack lanes (core leaves out only requires_warehouse);
# requires_microsoft_365 puts it in the microsoft_365 lane, which the warehouse lane leaves out.
@pytest.mark.requires_warehouse
@pytest.mark.requires_microsoft_365
def test_onedrive_folder_csv_reads_as_the_file_does(tmp_path):
    env = _require("M365_TENANT_ID", "M365_CLIENT_ID", "M365_CLIENT_SECRET", "M365_TEST_MAILBOXES")
    user = env["M365_TEST_MAILBOXES"].split(",")[0].strip()
    source = SimpleNamespace(
        id="od-live",
        path=f"onedrive://{user}/{RUN}/{INNER}/*.csv",
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
        _ok(resp)
        return resp

    _preflight_app_only_drive(
        user,
        token(),
        lambda url: client.request("GET", url, headers={"Authorization": f"Bearer {token()}"}),
    )
    folder = call(
        "POST",
        f"{drive}/root/children",
        json={"name": RUN, "folder": {}, "description": STAMP},
    ).json()
    try:
        inner = call(
            "POST", f"{drive}/items/{folder['id']}/children", json={"name": INNER, "folder": {}}
        ).json()
        call("PUT", f"{drive}/items/{inner['id']}:/orders.csv:/content", content=CSV)
        found = crawl_directory(source.path, storage_options=opts)
        _reads_as_the_file_does(
            tmp_path, found, f"onedrive://{user}/{RUN}/{INNER}/orders.csv", opts
        )
        try:
            _engine_reads_as_the_file_does(
                Source(
                    id="od-live",
                    type=SourceType.files,
                    path=source.path,
                    database=source.database,
                    username=source.username,
                    password=source.password,
                )
            )
        except Exception:
            # What Graph answers to the nested folder named by its path, as one %2F-encoded
            # segment and as slashes (a diagnostic; the failure is re-raised).
            for sub in (f"{RUN}/{INNER}", f"{RUN}%2F{INNER}"):
                for tail in ("", "/children"):
                    resp = client.get(
                        f"{drive}/root:/{sub}:{tail}",
                        headers={"Authorization": f"Bearer {token()}"},
                    )
                    print(f"graph probe root:/{sub}:{tail} -> {resp.status_code}")
            raise
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


# requires_warehouse keeps it out of the stack lanes; requires_google_workspace puts it in the
# google_workspace lane, which the warehouse lane leaves out.
@pytest.mark.requires_warehouse
@pytest.mark.requires_google_workspace
def test_gdrive_folder_csv_reads_as_the_file_does(tmp_path):
    env = _require("GWS_SERVICE_ACCOUNT_KEY_FILE", "GWS_TEST_MAILBOXES")
    with open(env["GWS_SERVICE_ACCOUNT_KEY_FILE"]) as fh:
        key = fh.read()
    json.loads(key)  # a key file that is no JSON fails here, not as an empty listing
    user = env["GWS_TEST_MAILBOXES"].split(",")[0].strip()
    mapping = {"gdrive_auth": "service_account", "gdrive_subject": user}
    probe = SimpleNamespace(
        id="gd-live", path="gdrive://root", password=key, username="", database="", mapping=mapping
    )
    opts = drive_storage_options(probe)  # the transport reads as the user, with drive.readonly
    client = opts["client"]
    writer = ApiAuthGoogleServiceAccount(key=key, subject=user, scopes=[DRIVE_SCOPE])

    def token() -> str:  # the run's own folder is written as the user, with the full drive scope
        return access_token(writer)

    def call(method: str, url: str, **kw) -> httpx.Response:
        resp = client.request(method, url, headers={"Authorization": f"Bearer {token()}"}, **kw)
        _ok(resp)
        return resp

    folder = call(
        "POST",
        f"{DRIVE_API}/files",
        json={
            "name": RUN,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": ["root"],
            "description": STAMP,
        },
    ).json()
    try:
        inner = call(
            "POST",
            f"{DRIVE_API}/files",
            json={
                "name": INNER,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [folder["id"]],
            },
        ).json()
        metadata = json.dumps({"name": "orders.csv", "parents": [inner["id"]]})
        call(
            "POST",
            "https://www.googleapis.com/upload/drive/v3/files",
            params={"uploadType": "multipart"},
            files={
                "metadata": ("metadata", metadata, "application/json; charset=UTF-8"),
                "file": ("orders.csv", CSV, "text/csv"),
            },
        )
        root = f"gdrive://{folder['id']}/{INNER}/*.csv"
        found = crawl_directory(root, storage_options=opts)
        _reads_as_the_file_does(
            tmp_path, found, f"gdrive://{folder['id']}/{INNER}/orders.csv", opts
        )
        _engine_reads_as_the_file_does(
            Source(id="gd-live", type=SourceType.files, path=root, password=key, mapping=mapping)
        )
    finally:

        def delete() -> None:
            client.delete(
                f"{DRIVE_API}/files/{folder['id']}",
                headers={"Authorization": f"Bearer {token()}"},
            ).raise_for_status()

        def read_back() -> None:
            resp = client.get(
                f"{DRIVE_API}/files/{folder['id']}",
                headers={"Authorization": f"Bearer {token()}"},
            )
            none_remain("the run's Drive folder", [folder["id"]] if resp.status_code != 404 else [])

        run_every([("delete the run's folder", delete), ("read the folder back", read_back)])
