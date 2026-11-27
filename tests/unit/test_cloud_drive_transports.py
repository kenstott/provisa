# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2f9a14-6b3e-4d85-a0f7-1e8d5c3b9a26
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2280: onedrive:// and gdrive:// are File Crawler transports. Each lists a folder and reads
a file by path through its drive's API (Microsoft Graph, the Google Drive API), so a path or glob
names the folder and files as for every other transport, and only the files a glob names are
read. Each signs in with the source's own credentials, as the SharePoint and Google Sheets
sources do. The APIs are answered by recorded-shape mock handlers (httpx.MockTransport)."""

# Requirements: REQ-2280
from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import parse_qs, unquote, urlparse

import httpx
import pyarrow.csv as pac
import pytest

from provisa.file_source import cloud_drive
from provisa.file_source.cloud_drive import (
    DriveFileNotReadable,
    DriveNameAmbiguous,
    GoogleDriveFileSystem,
    OneDriveFileSystem,
    drive_storage_options,
)
from provisa.file_source.crawler import crawl_directory
from provisa.file_source.drive_operand import DriveCredentialsMissing
from provisa.file_source.drive_schemes import scheme_of
from provisa.file_source.files_glob import (
    columns_of_file,
    matched_files,
    remote_fs,
    stat_matched_files,
)

ORDERS = b"id,amount\n1,9.5\n2,3.25\n"
STAMP = "2026-11-20T10:00:00Z"
OWNER = "ana@contoso.com"


# --- Microsoft Graph -------------------------------------------------------------------------


class _Graph:
    """A user's drive as the Graph drive API answers it: path -> bytes (a file) or None (a
    folder). Children are paged two at a time with @odata.nextLink, as Graph pages."""

    def __init__(self, items: dict[str, bytes | None]) -> None:
        self.items = items
        self.requests: list[httpx.Request] = []
        self.content_reads: list[str] = []

    def _item(self, path: str) -> dict:
        data = self.items[path]
        item: dict = {"name": path.rsplit("/", 1)[-1] or "root", "lastModifiedDateTime": STAMP}
        if data is None:
            item["folder"] = {"childCount": 0}
        else:
            item["file"] = {"mimeType": "text/csv"}
            item["size"] = len(data)
        return item

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.headers["Authorization"] == "Bearer graph-token"
        raw = unquote(request.url.raw_path.decode().split("?", 1)[0])
        prefix = f"/v1.0/users/{OWNER}/drive/root"
        assert raw.startswith(prefix), raw
        rest = raw[len(prefix) :]
        suffix = ""
        if rest.startswith(":/"):
            path, _, suffix = rest[2:].partition(":")
        else:
            path, suffix = "", rest
        if path not in self.items:
            return httpx.Response(404, json={"error": {"code": "itemNotFound"}})
        if suffix == "/content":
            self.content_reads.append(path)
            return httpx.Response(200, content=self.items[path])
        if suffix == "/children":
            prefix_path = f"{path}/" if path else ""
            kids = sorted(
                p
                for p in self.items
                if p.startswith(prefix_path) and p and "/" not in p[len(prefix_path) :]
            )
            page = int(request.url.params.get("page", "0"))
            body: dict = {"value": [self._item(p) for p in kids[page * 2 : page * 2 + 2]]}
            if len(kids) > page * 2 + 2:
                body["@odata.nextLink"] = f"{request.url.copy_with(params={'page': page + 1})}"
            return httpx.Response(200, json=body)
        assert suffix == "", suffix
        return httpx.Response(200, json=self._item(path))


def _onedrive(graph: _Graph) -> OneDriveFileSystem:
    return OneDriveFileSystem(
        token=lambda: "graph-token", client=httpx.Client(transport=httpx.MockTransport(graph))
    )


def _graph() -> _Graph:
    return _Graph(
        {
            "": None,
            "Reports": None,
            "Reports/orders.csv": ORDERS,
            "Reports/returns.csv": b"id,reason\n1,late\n",
            "Reports/notes.txt": b"hello",
            "Reports/2025": None,
            "Reports/2025/orders.csv": ORDERS,
        }
    )


def test_onedrive_lists_a_folder_across_pages_and_reads_a_file():
    graph = _graph()
    fs = _onedrive(graph)
    listed = fs.ls(f"onedrive://{OWNER}/Reports", detail=True)
    names = sorted(e["name"] for e in listed)
    assert names == [
        f"{OWNER}/Reports/2025",
        f"{OWNER}/Reports/notes.txt",
        f"{OWNER}/Reports/orders.csv",
        f"{OWNER}/Reports/returns.csv",
    ]
    kinds = {e["name"].rsplit("/", 1)[-1]: e["type"] for e in listed}
    assert kinds["2025"] == "directory" and kinds["orders.csv"] == "file"
    assert fs.cat_file(f"{OWNER}/Reports/orders.csv") == ORDERS
    info = fs.info(f"{OWNER}/Reports/orders.csv")
    assert info["size"] == len(ORDERS) and info["mtime"] > 0


def test_onedrive_a_missing_path_is_file_not_found():
    with pytest.raises(FileNotFoundError):
        _onedrive(_graph()).info(f"{OWNER}/Nope/x.csv")


def test_onedrive_is_read_only():
    with pytest.raises(PermissionError, match="read-only"):
        _onedrive(_graph()).open(f"{OWNER}/Reports/orders.csv", "wb")


def test_onedrive_a_glob_reads_only_the_files_it_names():
    graph = _graph()
    opts = {
        "token": lambda: "graph-token",
        "client": httpx.Client(transport=httpx.MockTransport(graph)),
    }
    files = matched_files(f"onedrive://{OWNER}/Reports/*.csv", "*.csv", opts)
    assert files == [
        f"onedrive://{OWNER}/Reports/orders.csv",
        f"onedrive://{OWNER}/Reports/returns.csv",
    ]
    assert graph.content_reads == []  # listing reads no file
    assert columns_of_file(files[0], storage_options=opts) == ["id", "amount"]
    assert graph.content_reads == ["Reports/orders.csv"]
    stats = stat_matched_files(f"onedrive://{OWNER}/Reports/*.csv", "orders.csv", opts)
    assert [(p, s) for p, _, s in stats] == [(files[0], len(ORDERS))]


def test_onedrive_a_crawl_finds_the_tables_under_a_folder():
    graph = _graph()
    opts = {
        "token": lambda: "graph-token",
        "client": httpx.Client(transport=httpx.MockTransport(graph)),
    }
    found = crawl_directory(f"onedrive://{OWNER}/Reports", storage_options=opts)
    assert sorted(e["path"] for e in found) == [
        f"onedrive://{OWNER}/Reports/2025/orders.csv",
        f"onedrive://{OWNER}/Reports/orders.csv",
        f"onedrive://{OWNER}/Reports/returns.csv",
    ]
    orders = next(e for e in found if e["path"].endswith("Reports/orders.csv"))
    assert [c["name"] for c in orders["tables"][0]["columns"]] == ["id", "amount"]


def test_a_drive_file_reads_the_same_as_the_file_on_disk(tmp_path):
    local = tmp_path / "orders.csv"
    local.write_bytes(ORDERS)
    fs = _onedrive(_graph())
    with fs.open(f"{OWNER}/Reports/orders.csv") as f:
        remote = pac.read_csv(f)
    assert remote.equals(pac.read_csv(str(local)))


# --- Google Drive ----------------------------------------------------------------------------

FOLDER = "1RootFolder"
FOLDER_MIME = "application/vnd.google-apps.folder"


class _Drive:
    """Files under a shared folder as the Drive v3 API answers: id -> (name, parent, mime, data).
    files.list is paged one file at a time with nextPageToken."""

    def __init__(self, files: dict[str, tuple[str, str | None, str, bytes | None]]) -> None:
        self.files = files
        self.media_reads: list[str] = []
        self.queries: list[str] = []

    def _meta(self, file_id: str) -> dict:
        name, _, mime, data = self.files[file_id]
        meta = {"id": file_id, "name": name, "mimeType": mime, "modifiedTime": STAMP}
        if data is not None:
            meta["size"] = str(len(data))
        return meta

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer drive-token"
        url = urlparse(str(request.url))
        params = {k: v[0] for k, v in parse_qs(url.query).items()}
        assert params.get("supportsAllDrives") == "true"
        path = unquote(url.path)
        if path == "/drive/v3/files":
            q = params["q"]
            self.queries.append(q)
            parent = q.split("'", 2)[1]
            wanted = None
            if " and name = '" in q:
                wanted = q.split(" and name = '", 1)[1][:-1].replace("\\'", "'")
            kids = sorted(
                fid
                for fid, (name, par, _, _) in self.files.items()
                if par == parent and (wanted is None or name == wanted)
            )
            start = int(params.get("pageToken", "0"))
            body: dict = {"files": [self._meta(f) for f in kids[start : start + 1]]}
            if start + 1 < len(kids):
                body["nextPageToken"] = str(start + 1)
            return httpx.Response(200, json=body)
        file_id = path.removeprefix("/drive/v3/files/")
        if file_id not in self.files:
            return httpx.Response(404, json={"error": {"code": 404}})
        if params.get("alt") == "media":
            self.media_reads.append(file_id)
            return httpx.Response(200, content=self.files[file_id][3])
        return httpx.Response(200, json=self._meta(file_id))


def _drive_files() -> dict:
    return {
        FOLDER: ("Shared", None, FOLDER_MIME, None),
        "f-reports": ("Reports", FOLDER, FOLDER_MIME, None),
        "f-orders": ("orders.csv", "f-reports", "text/csv", ORDERS),
        "f-returns": ("returns.csv", "f-reports", "text/csv", b"id,reason\n1,late\n"),
        "f-sheet": ("Budget", "f-reports", "application/vnd.google-apps.spreadsheet", None),
        "f-dup1": ("dup.csv", "f-reports", "text/csv", ORDERS),
        "f-dup2": ("dup.csv", "f-reports", "text/csv", ORDERS),
    }


def _opts(drive: _Drive) -> dict:
    return {
        "token": lambda: "drive-token",
        "client": httpx.Client(transport=httpx.MockTransport(drive)),
    }


def test_gdrive_lists_a_folder_across_pages_by_name():
    drive = _Drive(_drive_files())
    fs = GoogleDriveFileSystem(**_opts(drive))
    names = sorted(fs.ls(f"gdrive://{FOLDER}/Reports", detail=False))
    assert names == [
        f"{FOLDER}/Reports/Budget",
        f"{FOLDER}/Reports/dup.csv",
        f"{FOLDER}/Reports/dup.csv",
        f"{FOLDER}/Reports/orders.csv",
        f"{FOLDER}/Reports/returns.csv",
    ]
    assert fs.info(f"{FOLDER}/Reports")["type"] == "directory"
    assert fs.cat_file(f"{FOLDER}/Reports/orders.csv") == ORDERS


def test_gdrive_a_glob_reads_only_the_files_it_names():
    drive = _Drive(_drive_files())
    files = matched_files(f"gdrive://{FOLDER}/Reports/o*.csv", "o*.csv", _opts(drive))
    assert files == [f"gdrive://{FOLDER}/Reports/orders.csv"]
    assert drive.media_reads == []
    assert columns_of_file(files[0], storage_options=_opts(drive)) == ["id", "amount"]
    assert drive.media_reads == ["f-orders"]


def test_gdrive_a_name_with_a_quote_is_escaped_in_the_query():
    files = _drive_files()
    files["f-q"] = ("o'brien.csv", "f-reports", "text/csv", ORDERS)
    drive = _Drive(files)
    fs = GoogleDriveFileSystem(**_opts(drive))
    assert fs.cat_file(f"{FOLDER}/Reports/o'brien.csv") == ORDERS
    assert any("name = 'o\\'brien.csv'" in q for q in drive.queries)


def test_gdrive_two_items_of_one_name_are_refused_by_name():
    fs = GoogleDriveFileSystem(**_opts(_Drive(_drive_files())))
    with pytest.raises(DriveNameAmbiguous, match="dup.csv"):
        fs.cat_file(f"{FOLDER}/Reports/dup.csv")


def test_gdrive_a_google_sheet_is_refused_by_name_not_read_as_empty():
    fs = GoogleDriveFileSystem(**_opts(_Drive(_drive_files())))
    with pytest.raises(DriveFileNotReadable, match="spreadsheet"):
        fs.cat_file(f"{FOLDER}/Reports/Budget")


def test_gdrive_a_missing_path_is_file_not_found():
    fs = GoogleDriveFileSystem(**_opts(_Drive(_drive_files())))
    with pytest.raises(FileNotFoundError):
        fs.info(f"{FOLDER}/Reports/none.csv")


# --- Registration and sign-in ----------------------------------------------------------------


def test_both_schemes_are_registered_with_fsspec():
    fs, path = remote_fs(f"onedrive://{OWNER}/Reports", {"token": str, "client": httpx.Client()})
    assert isinstance(fs, OneDriveFileSystem) and path == f"{OWNER}/Reports"
    fs, path = remote_fs(f"gdrive://{FOLDER}/Reports", {"token": str, "client": httpx.Client()})
    assert isinstance(fs, GoogleDriveFileSystem) and path == f"{FOLDER}/Reports"
    assert scheme_of("s3://b/k") is None and scheme_of("/data/x.csv") is None


def _src(path: str, **kw) -> SimpleNamespace:
    base = dict(id="drv", path=path, database="", username="", password="", mapping={})
    return SimpleNamespace(**{**base, **kw})


def test_a_path_in_no_drive_needs_no_options():
    assert drive_storage_options(_src("s3://bucket/x/*.csv")) == {}


def test_onedrive_signs_in_with_the_tenants_client_credentials(monkeypatch):
    seen = {}

    def fetch(auth):
        seen["auth"] = auth
        return "graph-token"

    import provisa.api_source.caller as caller

    monkeypatch.setattr(caller, "_fetch_oauth2_token", fetch)
    opts = drive_storage_options(
        _src(f"onedrive://{OWNER}/R", database="tid", username="cid", password="sec")
    )
    assert opts["token"]() == "graph-token"
    auth = seen["auth"]
    assert auth.token_url == "https://login.microsoftonline.com/tid/oauth2/v2.0/token"
    assert (auth.client_id, auth.client_secret) == ("cid", "sec")
    assert auth.scope == "https://graph.microsoft.com/.default"


def test_onedrive_without_its_tenant_is_refused_by_name():
    with pytest.raises(DriveCredentialsMissing, match="database"):
        drive_storage_options(_src(f"onedrive://{OWNER}/R", username="cid", password="sec"))


def test_gdrive_signs_in_as_its_service_account(monkeypatch):
    import provisa.api_source.oauth_grants as grants

    monkeypatch.setattr(grants, "service_account_token", lambda key, scopes: f"{key}|{scopes[0]}")
    opts = drive_storage_options(
        _src(f"gdrive://{FOLDER}/R", password='{"k":1}', mapping={"gdrive_auth": "service_account"})
    )
    assert opts["token"]() == '{"k":1}|https://www.googleapis.com/auth/drive.readonly'


def test_gdrive_signs_in_as_the_workspace_user_its_service_account_acts_as(monkeypatch):
    # REQ-2280 (amended 2026-11-26): domain-wide delegation. The assertion names the source's
    # gdrive_subject, so the token reads that user's Drive under drive.readonly.
    import provisa.api_source.oauth_grants as grants
    from provisa.core.auth_models import ApiAuthGoogleServiceAccount

    seen = {}

    def token(auth):
        seen["auth"] = auth
        return "dwd-token"

    monkeypatch.setattr(grants, "access_token", token)
    opts = drive_storage_options(
        _src(
            f"gdrive://{FOLDER}/R",
            password='{"k":1}',
            mapping={"gdrive_auth": "service_account", "gdrive_subject": "ana@example.com"},
        )
    )
    assert opts["token"]() == "dwd-token"
    assert seen["auth"] == ApiAuthGoogleServiceAccount(
        key='{"k":1}',
        subject="ana@example.com",
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )


def test_gdrive_signs_in_with_an_oauth_refresh_token(monkeypatch):
    import provisa.api_source.oauth_grants as grants

    monkeypatch.setattr(
        grants, "access_token", lambda auth: f"{auth.client_id}|{auth.refresh_token}"
    )
    opts = drive_storage_options(
        _src(
            f"gdrive://{FOLDER}/R",
            username="cid",
            password="sec",
            mapping={"gdrive_auth": "oauth", "refresh_token": "rt"},
        )
    )
    assert opts["token"]() == "cid|rt"


@pytest.mark.parametrize(
    ("mapping", "match"),
    [({}, "gdrive_auth"), ({"gdrive_auth": "oauth"}, "refresh_token")],
)
def test_gdrive_without_its_sign_in_is_refused_by_name(mapping, match):
    with pytest.raises(DriveCredentialsMissing, match=match):
        drive_storage_options(
            _src(f"gdrive://{FOLDER}/R", username="c", password="s", mapping=mapping)
        )


def test_a_sources_row_is_read_as_a_source_is(monkeypatch):
    import provisa.api_source.oauth_grants as grants

    monkeypatch.setattr(grants, "service_account_token", lambda key, scopes: "t")
    row = {
        "id": "drv",
        "path": f"gdrive://{FOLDER}/R",
        "password": "{}",
        "mapping": {"gdrive_auth": "service_account"},
    }
    assert drive_storage_options(row)["token"]() == "t"


def test_the_module_registers_on_import():
    import fsspec

    cloud_drive.register()
    assert fsspec.get_filesystem_class("onedrive") is OneDriveFileSystem
    assert fsspec.get_filesystem_class("gdrive") is GoogleDriveFileSystem
