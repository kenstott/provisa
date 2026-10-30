# Copyright (c) 2026 Kenneth Stott
# Canary: 8d3f2a65-1b70-4e9c-a5d4-7c06b9e21f38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""scripts/databricks_location_cleanup.py against a stubbed Unity Catalog REST API (REQ-254)."""

# Requirements: REQ-254

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import httpx
import pytest

_SPEC = importlib.util.spec_from_file_location(
    "databricks_location_cleanup",
    Path(__file__).resolve().parents[2] / "scripts" / "databricks_location_cleanup.py",
)
assert _SPEC and _SPEC.loader
cleanup = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = cleanup
_SPEC.loader.exec_module(cleanup)

_P = "/api/2.1/unity-catalog"
_STALE = "r2://pubs@acct.r2.cloudflarestorage.com/provisa_ext_link_e2e"


def _loc(name: str, url: str) -> dict:
    return {"name": name, "url": url, "credential_name": "cred", "owner": "me", "created_at": 1}


class FakeUC:
    def __init__(self) -> None:
        self.locations = {
            "stale": _loc("stale", _STALE),
            "other": _loc("other", "s3://bucket/keep"),
            "used": _loc("used", "s3://bucket/used"),
        }
        self.tables = {("cat", "sch"): [{"name": "t", "storage_location": "s3://bucket/used/t1"}]}
        self.volumes = {
            ("cat", "sch"): [{"name": "v", "storage_location": "s3://bucket/usedother/v"}]
        }
        self.requests: list[tuple[str, str]] = []
        self.keep_after_delete = False

    def handler(self, req: httpx.Request) -> httpx.Response:
        self.requests.append((req.method, req.url.path))
        path = req.url.path.removeprefix(_P)
        q = req.url.params
        if path == "/catalogs":
            return httpx.Response(200, json={"catalogs": [{"name": "cat"}, {"name": "system"}]})
        if path == "/schemas":
            assert q["catalog_name"] == "cat"
            return httpx.Response(200, json={"schemas": [{"name": "sch"}]})
        if path in ("/tables", "/volumes"):
            key = (q["catalog_name"], q["schema_name"])
            src = self.tables if path == "/tables" else self.volumes
            return httpx.Response(200, json={path[1:]: src[key]})
        if path == "/external-locations":
            names = sorted(self.locations)
            if q.get("page_token") == "p2":
                return httpx.Response(
                    200, json={"external_locations": [self.locations[n] for n in names[2:]]}
                )
            return httpx.Response(
                200,
                json={
                    "external_locations": [self.locations[n] for n in names[:2]],
                    "next_page_token": "p2",
                },
            )
        name = path.removeprefix("/external-locations/")
        if req.method == "DELETE":
            if not self.keep_after_delete:
                del self.locations[name]
            return httpx.Response(200, json={})
        if name in self.locations:
            return httpx.Response(200, json=self.locations[name])
        return httpx.Response(404, json={})


@pytest.fixture
def uc() -> tuple[FakeUC, httpx.Client]:
    fake = FakeUC()
    client = httpx.Client(base_url="https://h", transport=httpx.MockTransport(fake.handler))
    return fake, client


def test_inventory_reads_every_page_and_maps_users(uc) -> None:
    _, client = uc
    inv = {loc["name"]: loc for loc in cleanup.inventory(client)}
    assert set(inv) == {"stale", "other", "used"}
    assert inv["used"]["used_by"] == ["table cat.sch.t"]
    assert inv["stale"]["used_by"] == []
    assert inv["stale"]["url"] == _STALE


def test_prefix_is_path_boundary(uc) -> None:
    _, client = uc
    inv = {loc["name"]: loc for loc in cleanup.inventory(client)}
    assert inv["used"]["used_by"] == ["table cat.sch.t"]


def test_list_makes_no_writes(uc) -> None:
    fake, client = uc
    cleanup.inventory(client)
    assert all(m == "GET" for m, _ in fake.requests)


def test_delete_removes_only_named_and_reads_back(uc) -> None:
    fake, client = uc
    out = cleanup.delete(client, ["stale"])
    assert "deleted stale; read back: gone" in out
    assert set(fake.locations) == {"other", "used"}
    assert fake.requests[-1] == ("GET", f"{_P}/external-locations/stale")


def test_delete_refuses_location_in_use(uc) -> None:
    fake, client = uc
    with pytest.raises(SystemExit) as exc:
        cleanup.delete(client, ["used"])
    assert "table cat.sch.t" in str(exc.value)
    assert "used" in fake.locations
    assert not any(m == "DELETE" for m, _ in fake.requests)


def test_delete_unknown_name_fails(uc) -> None:
    _, client = uc
    with pytest.raises(SystemExit, match="no such external location"):
        cleanup.delete(client, ["nope"])


def test_delete_fails_when_read_back_finds_it(uc) -> None:
    fake, client = uc
    fake.keep_after_delete = True
    with pytest.raises(SystemExit, match="still exists"):
        cleanup.delete(client, ["stale"])


def test_report_lists_fields(uc) -> None:
    _, client = uc
    text = cleanup.report(cleanup.inventory(client))
    assert "3 external location(s)" in text
    assert f"url={_STALE}" in text
    assert "credential=cred" in text and "owner=me" in text and "created=1" in text


def test_names_must_be_json_list(monkeypatch) -> None:
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "h")
    monkeypatch.setenv("DATABRICKS_TOKEN", "t")
    with pytest.raises(SystemExit, match="non-empty JSON list"):
        cleanup.main(["delete", "--names", json.dumps("x")])
