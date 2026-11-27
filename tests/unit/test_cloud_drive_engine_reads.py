# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8a1c27-9d3b-4f60-b5e2-7a0c9d1f3b84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2280 (amended 2026-11-26): every engine reads onedrive:// and gdrive:// files live through
the Calcite file adapter's OneDrive and Google Drive storage providers. The source's pgwire
server is given the adapter's storageType and storageConfig from the source's own fields,
secrets resolved, and a directory within the drive (never a path resolved on this host's disk).
Every place that lists the source's files before a read (the combine-files preview, the crawl
preview, the save-time glob check, the glob freshness probe) signs in with the same fields."""

# Requirements: REQ-2280
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import httpx
import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.file_source.drive_operand import DriveCredentialsMissing, drive_adapter_operand

pytestmark = pytest.mark.unit

OWNER = "ana@contoso.com"
FOLDER = "1AbCdEfGhIjK"
KEY = '{"type": "service_account", "client_email": "sa@p.iam.gserviceaccount.com"}'


def _files(path: str, **kw) -> Source:
    return Source(**{"id": "drv", "type": SourceType.files, "path": path, **kw})


def _operand(source: Source) -> dict:
    return pr.build_model_json(source)["schemas"][0]["operand"]


# --- the pgwire bundle's files operand ----------------------------------------------------------


def test_onedrive_is_read_by_the_adapter_with_the_tenants_client_credentials():
    operand = _operand(
        _files(
            f"onedrive://{OWNER}/Reports/2026/*.csv",
            database="tid",
            username="cid",
            password="sec",
        )
    )
    assert operand["storageType"] == "onedrive"
    assert operand["storageConfig"] == {
        "userPrincipalName": OWNER,
        "tenantId": "tid",
        "clientId": "cid",
        "clientSecret": "sec",
    }
    assert operand["directory"] == "Reports/2026"  # within the drive, not a path on this host
    assert operand["glob"] == "*.csv"


def test_gdrive_is_read_by_the_adapter_as_its_service_account():
    operand = _operand(
        _files(
            f"gdrive://{FOLDER}/Sales/*.xlsx",
            password=KEY,
            mapping={"gdrive_auth": "service_account"},
        )
    )
    assert operand["storageType"] == "gdrive"
    assert operand["storageConfig"] == {"folderId": FOLDER, "serviceAccountKey": KEY}
    assert operand["directory"] == "Sales"
    assert operand["glob"] == "*.xlsx"


USER = "ana@example.com"


def test_gdrive_is_read_by_the_adapter_as_the_workspace_user_its_service_account_acts_as():
    # REQ-2280 (amended 2026-11-26): a service account with domain-wide delegation acts as the
    # Workspace user the source names; calcite engine-v0.131.0's gdrive storageConfig takes it as
    # ``subject`` (the JWT ``sub`` claim) and reads with its default drive.readonly scope.
    operand = _operand(
        _files(
            f"gdrive://{FOLDER}/Sales/*.xlsx",
            password=KEY,
            mapping={"gdrive_auth": "service_account", "gdrive_subject": USER},
        )
    )
    assert operand["storageConfig"] == {
        "folderId": FOLDER,
        "serviceAccountKey": KEY,
        "subject": USER,
    }


def test_a_gdrive_subject_is_refused_by_name_with_an_oauth_sign_in():
    # The adapter refuses subject with refresh-token sign-in; the source says so first, by name.
    with pytest.raises(DriveCredentialsMissing, match="gdrive_subject"):
        _operand(
            _files(
                f"gdrive://{FOLDER}/*.csv",
                username="cid",
                password="sec",
                mapping={"gdrive_auth": "oauth", "refresh_token": "rt", "gdrive_subject": USER},
            )
        )


def test_gdrive_is_read_by_the_adapter_with_an_oauth_refresh_token():
    operand = _operand(
        _files(
            f"gdrive://{FOLDER}/*.csv",
            username="cid",
            password="sec",
            mapping={"gdrive_auth": "oauth", "refresh_token": "rt"},
        )
    )
    assert operand["storageConfig"] == {
        "folderId": FOLDER,
        "clientId": "cid",
        "clientSecret": "sec",
        "refreshToken": "rt",
    }
    assert operand["directory"] == ""  # the folder itself is the drive's root here
    assert operand["glob"] == "*.csv"


def test_the_sign_in_reaches_the_adapter_with_its_secrets_resolved(monkeypatch):
    monkeypatch.setenv("DRV_TENANT", "tid")
    monkeypatch.setenv("DRV_SECRET", "sec")
    monkeypatch.setenv("DRV_REFRESH", "rt")
    onedrive = _operand(
        _files(
            f"onedrive://{OWNER}/R/*.csv",
            database="${env:DRV_TENANT}",
            username="cid",
            password="${env:DRV_SECRET}",
        )
    )
    assert (onedrive["storageConfig"]["tenantId"], onedrive["storageConfig"]["clientSecret"]) == (
        "tid",
        "sec",
    )
    gdrive = _operand(
        _files(
            f"gdrive://{FOLDER}/*.csv",
            username="cid",
            password="${env:DRV_SECRET}",
            mapping={"gdrive_auth": "oauth", "refresh_token": "${env:DRV_REFRESH}"},
        )
    )
    assert (gdrive["storageConfig"]["clientSecret"], gdrive["storageConfig"]["refreshToken"]) == (
        "sec",
        "rt",
    )


@pytest.mark.parametrize(
    ("path", "fields", "match"),
    [
        (f"onedrive://{OWNER}/R/*.csv", {"username": "cid", "password": "sec"}, "database"),
        (f"onedrive://{OWNER}/R/*.csv", {"database": "tid", "password": "sec"}, "username"),
        (f"gdrive://{FOLDER}/*.csv", {"password": KEY}, "gdrive_auth"),
        (
            f"gdrive://{FOLDER}/*.csv",
            {"password": "s", "username": "c", "mapping": {"gdrive_auth": "oauth"}},
            "refresh_token",
        ),
    ],
)
def test_a_drive_source_missing_its_sign_in_is_refused_by_name(path, fields, match):
    with pytest.raises(DriveCredentialsMissing, match=match):
        _operand(_files(path, **fields))


def test_a_drive_path_naming_no_owner_or_folder_is_refused_by_name():
    with pytest.raises(ValueError, match="names no drive owner"):
        drive_adapter_operand(SimpleNamespace(id="drv"), "onedrive://")
    with pytest.raises(ValueError, match="names no folder"):
        drive_adapter_operand(SimpleNamespace(id="drv"), "gdrive://")


def test_a_local_directory_is_still_resolved_on_this_host(tmp_path):
    operand = _operand(_files(str(tmp_path / "orders" / "*.csv")))
    assert operand["directory"] == str((tmp_path / "orders").resolve())
    assert "storageType" not in operand and "storageConfig" not in operand


def test_a_drive_glob_table_is_a_url_within_the_drive_matched_with_the_sign_in(monkeypatch):
    seen: dict = {}

    def matched(source_path, file_glob, storage_options=None):
        seen["call"] = (source_path, file_glob, sorted(storage_options or {}))
        return [f"{source_path.rsplit('/', 1)[0]}/orders_01.csv"]

    import provisa.file_source.files_glob as files_glob

    monkeypatch.setattr(files_glob, "matched_files", matched)
    source = SimpleNamespace(
        id="drv",
        type=SimpleNamespace(value="files"),
        path=f"onedrive://{OWNER}/Reports/*.csv",
        database="tid",
        username="cid",
        password="sec",
        mapping={},
        file_glob_tables=[{"name": "orders", "file_glob": "orders_*.csv"}],
    )
    operand = pr._files_operand(source)
    assert operand["tables"] == [{"name": "orders", "url": "Reports/orders_*.csv"}]
    assert seen["call"] == (source.path, "orders_*.csv", ["client", "token"])


# --- Trino --------------------------------------------------------------------------------------


def _trino(source: Source) -> dict:
    from provisa.federation.trino_connectors import TrinoFilesConnector

    return TrinoFilesConnector().details(source)


def test_trino_reads_onedrive_with_the_same_storage_config_as_the_pgwire_bundle():
    import json

    source = _files(
        f"onedrive://{OWNER}/Reports/2026/*.csv",
        database="tid",
        username="cid",
        password="sec",
    )
    details = _trino(source)
    # The trino-file plugin (calcite engine-v0.130.0+, storage-config) strips the scheme and reads
    # the rest within the drive, so the owner moves from the path into the storage config.
    assert details["glob"] == "onedrive://Reports/2026/*.csv"
    assert json.loads(details["storage-config"]) == _operand(source)["storageConfig"]
    assert json.loads(details["storage-config"]) == {
        "userPrincipalName": OWNER,
        "tenantId": "tid",
        "clientId": "cid",
        "clientSecret": "sec",
    }


def test_trino_reads_gdrive_rooted_at_its_folder_id():
    import json

    source = _files(
        f"gdrive://{FOLDER}/Sales/*.xlsx",
        password=KEY,
        mapping={"gdrive_auth": "service_account"},
    )
    details = _trino(source)
    assert details["glob"] == "gdrive://Sales/*.xlsx"
    assert json.loads(details["storage-config"]) == {"folderId": FOLDER, "serviceAccountKey": KEY}
    assert json.loads(details["storage-config"]) == _operand(source)["storageConfig"]


def test_trino_reads_gdrive_as_the_workspace_user_with_the_same_storage_config():
    import json

    source = _files(
        f"gdrive://{FOLDER}/Sales/*.xlsx",
        password=KEY,
        mapping={"gdrive_auth": "service_account", "gdrive_subject": USER},
    )
    details = _trino(source)
    assert json.loads(details["storage-config"]) == {
        "folderId": FOLDER,
        "serviceAccountKey": KEY,
        "subject": USER,
    }
    assert json.loads(details["storage-config"]) == _operand(source)["storageConfig"]


def test_trino_drive_source_without_its_sign_in_is_refused_by_name():
    with pytest.raises(DriveCredentialsMissing, match="needs password"):
        _trino(_files(f"onedrive://{OWNER}/R/*.csv", database="tid", username="cid"))


def test_trino_local_glob_takes_no_storage_config():
    details = _trino(_files("/data/files/*.csv"))
    assert details["glob"] == "/data/files/*.csv"
    assert "storage-config" not in details


# --- the callers that list a drive source's files ------------------------------------------------


class _Graph:
    """One OneDrive folder, ``Reports``, holding two CSVs, as the Graph drive API answers it:
    path -> bytes (a file) or None (a folder)."""

    ITEMS: dict[str, bytes | None] = {
        "": None,
        "Reports": None,
        "Reports/orders_01.csv": b"id,amount\n1,9.5\n",
        "Reports/orders_02.csv": b"id,amount\n2,3.25\n",
    }

    @staticmethod
    def _item(path: str) -> dict:
        data = _Graph.ITEMS[path]
        item: dict = {
            "name": path.rsplit("/", 1)[-1],
            "lastModifiedDateTime": "2026-11-20T10:00:00Z",
        }
        if data is None:
            item["folder"] = {}
        else:
            item.update(file={}, size=len(data))
        return item

    def __call__(self, request: httpx.Request) -> httpx.Response:
        from urllib.parse import unquote

        rest = unquote(request.url.raw_path.decode().split("?", 1)[0]).split("/drive/root", 1)[1]
        path, suffix = rest[2:].partition(":")[::2] if rest.startswith(":/") else ("", rest)
        if path not in self.ITEMS:
            return httpx.Response(404, json={})
        if suffix == "/content":
            return httpx.Response(200, content=self.ITEMS[path])
        if suffix == "/children":
            prefix = f"{path}/" if path else ""
            kids = [
                p for p in self.ITEMS if p and p.startswith(prefix) and "/" not in p[len(prefix) :]
            ]
            return httpx.Response(200, json={"value": [self._item(p) for p in kids]})
        return httpx.Response(200, json=self._item(path))


def _graph_options() -> dict:
    return {
        "token": lambda: "graph-token",
        "client": httpx.Client(transport=httpx.MockTransport(_Graph())),
    }


def test_the_combine_files_preview_lists_and_reads_a_drive_with_its_sign_in():
    from provisa.api.admin._file_glob import file_glob_preview

    out = file_glob_preview(
        f"onedrive://{OWNER}/Reports/*.csv", "orders_*.csv", None, _graph_options()
    )
    assert [f["file"] for f in out["files"]] == ["orders_01.csv", "orders_02.csv"]
    assert out["shared_columns"] == ["id", "amount"]


def test_the_preview_is_given_the_sources_sign_in(monkeypatch):
    from provisa.api.admin._file_glob import source_files_path

    async def _get(_conn, _sid):
        return {
            "id": "drv",
            "type": "files",
            "path": f"onedrive://{OWNER}/Reports/*.csv",
            "database": "tid",
            "username": "cid",
            "password": "sec",
            "mapping": {},
        }

    monkeypatch.setattr("provisa.core.repositories.source.get", _get)
    path, options = asyncio.run(source_files_path(None, "drv"))
    assert path == f"onedrive://{OWNER}/Reports/*.csv"
    assert sorted(options or {}) == ["client", "token"]


def test_the_glob_freshness_probe_stats_a_drive_with_its_sign_in():
    from provisa.events.probes import build_probe

    probe = build_probe(
        "hash",
        source_path=f"onedrive://{OWNER}/Reports/*.csv",
        file_glob="orders_*.csv",
        storage_options=_graph_options(),
    )
    token = asyncio.run(probe())
    assert token is not None and token.startswith("2:")


def test_the_save_time_glob_check_lists_a_drive_with_its_sign_in(monkeypatch):
    from provisa.api.admin import schema_common
    from provisa.api.admin.types import SourceInput
    from provisa.file_source import cloud_drive

    seen: list[dict] = []

    def options(source):
        seen.append(dict(source))
        return _graph_options()

    monkeypatch.setattr(cloud_drive, "drive_storage_options", options)

    def _input(glob: str) -> SourceInput:
        return SourceInput(
            id="drv",
            type="files",
            path=f"onedrive://{OWNER}/Reports/{glob}",
            database="tid",
            username="cid",
            password="sec",
        )

    assert schema_common.files_source_glob_refusal(_input("orders_*.csv")) is None
    refused = schema_common.files_source_glob_refusal(_input("returns_*.csv"))
    assert refused is not None and refused.success is False
    assert (seen[0]["database"], seen[0]["username"], seen[0]["password"]) == ("tid", "cid", "sec")


def test_the_crawl_preview_signs_in_with_the_fields_given(monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from provisa.api.admin import crawl_router
    from provisa.file_source import cloud_drive

    seen: dict = {}

    def options(source):
        seen["source"] = dict(source)
        return _graph_options()

    monkeypatch.setattr(cloud_drive, "drive_storage_options", options)
    monkeypatch.setattr(crawl_router, "require_capability_request", lambda request, cap: None)
    app = FastAPI()
    app.include_router(crawl_router.router)
    resp = TestClient(app).post(
        "/admin/sources/crawl",
        json={
            "path": f"onedrive://{OWNER}/Reports",
            "database": "tid",
            "username": "cid",
            "password": "sec",
        },
    )
    assert resp.status_code == 200, resp.text
    assert sorted(e["name"] for e in resp.json()["discovered"]) == ["orders_01", "orders_02"]
    assert (seen["source"]["database"], seen["source"]["password"]) == ("tid", "sec")


# --- the pins the drive reads need ---------------------------------------------------------------


def test_trino_file_plugin_is_the_first_release_that_takes_storage_config():
    import tests.trino_plugins as pins

    # calcite engine-v0.130.0 (PR #1173) added the trino-file storage-config property;
    # engine-v0.131.0 its gdrive ``subject`` and ``scope`` keys (domain-wide delegation).
    assert pins.plugin_version("trino-file") == "0.131.0"


def test_pgwire_bundles_and_govdata_record_are_at_engine_v0_131_0():
    import json
    from pathlib import Path

    import provisa.govdata as govdata
    from provisa.runtime_deps import pgwire_bundles

    assert pgwire_bundles.RELEASE_TAG == "engine-v0.131.0"
    record = json.loads((Path(govdata.__file__).parent / "bundle_schemas.json").read_text())
    assert record["release"] == pgwire_bundles.RELEASE_TAG
