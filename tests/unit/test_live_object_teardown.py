# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3e9a15-b2d4-4f86-a1c7-0e5d8b2f6a93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-254: a test leaves nothing in a live external account.

The live tests' object teardown helpers name what a run creates for the run, delete exactly the
run's objects (never another run's, never one that existed before the run), attempt every delete,
read each one back, and raise every failure. No live account is reached here: each helper runs
against a fake of the account's API."""

# Requirements: REQ-254

from __future__ import annotations

import asyncio
import re
from http import HTTPStatus

import pytest

from tests.integration import databricks_uc, fabric_external_link, r2_objects, synapse_provision
from tests.integration import snowflake_warehouse
from tests.integration.live_teardown import none_remain, run_every_async, run_name

# --- naming, read-back, async run_every ------------------------------------------------------


def test_every_name_a_run_creates_carries_a_suffix_of_its_own():
    first, second = run_name("provisa_e2e"), run_name("provisa_e2e")
    assert re.fullmatch(r"provisa_e2e_[0-9a-f]{8}", first)
    assert first != second
    assert re.fullmatch(r"fab-r2-[0-9a-f]{8}", run_name("fab-r2", "-"))


def test_a_read_back_that_still_finds_an_object_raises():
    none_remain("schemas", [])
    with pytest.raises(AssertionError, match="provisa_e2e_1"):
        none_remain("schemas", ["provisa_e2e_1"])


def test_every_async_teardown_step_runs_and_every_failure_is_raised():
    ran = []

    def step(what, fails):
        async def run():
            ran.append(what)
            if fails:
                raise RuntimeError(what)

        return run

    with pytest.raises(ExceptionGroup) as raised:
        asyncio.run(
            run_every_async(
                [("a", step("a", True)), ("b", step("b", False)), ("c", step("c", True))]
            )
        )
    assert ran == ["a", "b", "c"]
    assert [str(e) for e in raised.value.exceptions] == ["a", "c"]
    assert raised.value.exceptions[1].__notes__ == ["teardown step: c"]


# --- R2 objects under the run's prefix --------------------------------------------------------


class _S3:
    def __init__(self, keys, *, deletes=True):
        self.keys, self.deletes, self.deleted = set(keys), deletes, []

    def get_paginator(self, _op):
        s3 = self

        class _Pages:
            def paginate(self, Bucket, Prefix):  # noqa: N803 -- boto3's own keywords
                del Bucket
                return [{"Contents": [{"Key": k} for k in sorted(s3.keys) if k.startswith(Prefix)]}]

        return _Pages()

    def delete_object(self, Bucket, Key):  # noqa: N803 -- boto3's own keywords
        del Bucket
        self.deleted.append(Key)
        if self.deletes:
            self.keys.discard(Key)


def _r2(monkeypatch, s3):
    monkeypatch.setattr(r2_objects, "client", lambda: s3)
    monkeypatch.setattr(r2_objects, "bucket", lambda: "pubs")


def test_only_the_runs_objects_are_deleted(monkeypatch):
    s3 = _S3(["run_a/orders.parquet", "run_a/x/y.parquet", "run_b/orders.parquet", "fixture.csv"])
    _r2(monkeypatch, s3)
    r2_objects.remove_prefix("run_a/")
    assert sorted(s3.deleted) == ["run_a/orders.parquet", "run_a/x/y.parquet"]
    assert s3.keys == {"run_b/orders.parquet", "fixture.csv"}


def test_an_r2_object_still_listed_after_its_delete_raises(monkeypatch):
    _r2(monkeypatch, _S3(["run_a/orders.parquet"], deletes=False))
    with pytest.raises(RuntimeError, match="run_a/orders.parquet"):
        r2_objects.remove_prefix("run_a/")


def test_an_r2_prefix_that_is_not_a_directory_is_refused(monkeypatch):
    # "run_a" would also match "run_ab/...", another run's objects.
    _r2(monkeypatch, _S3([]))
    with pytest.raises(ValueError):
        r2_objects.remove_prefix("run_a")


# --- Fabric shortcut, lakehouse, connection ---------------------------------------------------


class _Fabric:
    """A workspace holding a lakehouse, a connection and shortcuts; DELETE removes them."""

    def __init__(self, *, lakehouse=True, connection=True, shortcuts=(), delete_status=200):
        self.lakehouse, self.connection = lakehouse, connection
        self.shortcuts = set(shortcuts)
        self.delete_status = delete_status
        self.calls = []

    def _listed(self, path):
        if path == "/connections":
            name = fabric_external_link.connection_name("ws", "https://r2")
            return [{"id": "c1", "displayName": name}] if self.connection else []
        lakehouse = {"id": "lh1", "type": "Lakehouse", "displayName": "provisa_shortcuts"}
        return [lakehouse] if self.lakehouse else []

    def _req(self, method, path, body=None):
        del body
        self.calls.append((method, path))
        if method == "GET":
            name = path.rsplit("/", 1)[-1]
            return (HTTPStatus.OK, {}) if name in self.shortcuts else (HTTPStatus.NOT_FOUND, {})
        if self.delete_status != 200:
            return self.delete_status, {"error": "refused"}
        if "/shortcuts/" in path:
            self.shortcuts.discard(path.rsplit("/", 1)[-1])
        elif path.startswith("/connections/"):
            self.connection = False
        else:
            self.lakehouse = False
        return HTTPStatus.OK, {}


def _fabric(monkeypatch, fab):
    monkeypatch.setattr(fabric_external_link, "_Fabric", lambda: fab)


def test_the_runs_shortcut_is_deleted_and_read_back(monkeypatch):
    fab = _Fabric(shortcuts={"fab_r2_1_orders", "fab_r2_2_orders"})
    _fabric(monkeypatch, fab)
    fabric_external_link.remove_shortcut("ws", "fab_r2_1_orders")
    assert fab.shortcuts == {"fab_r2_2_orders"}


def test_a_shortcut_the_attach_never_made_needs_no_delete(monkeypatch):
    fab = _Fabric()
    _fabric(monkeypatch, fab)
    fabric_external_link.remove_shortcut("ws", "fab_r2_1_orders")
    assert [m for m, _ in fab.calls] == ["GET"]


def test_a_refused_fabric_delete_raises(monkeypatch):
    _fabric(monkeypatch, _Fabric(shortcuts={"s"}, delete_status=403))
    with pytest.raises(RuntimeError, match="403"):
        fabric_external_link.remove_shortcut("ws", "s")


def test_the_lakehouse_and_connection_found_before_the_run_stay(monkeypatch):
    fab = _Fabric()
    _fabric(monkeypatch, fab)
    before = fabric_external_link.Found(lakehouse=True, connection=True)
    fabric_external_link.remove_lakehouse("ws", before)
    fabric_external_link.remove_connection("ws", "https://r2", before)
    assert fab.lakehouse and fab.connection
    assert not [c for c in fab.calls if c[0] == "DELETE"]


def test_the_lakehouse_and_connection_the_run_created_are_deleted(monkeypatch):
    fab = _Fabric()
    _fabric(monkeypatch, fab)
    before = fabric_external_link.Found(lakehouse=False, connection=False)
    fabric_external_link.remove_lakehouse("ws", before)
    fabric_external_link.remove_connection("ws", "https://r2", before)
    assert not fab.lakehouse and not fab.connection


# --- Unity Catalog ----------------------------------------------------------------------------


class _Response:
    def __init__(self, body=None, status_code=HTTPStatus.OK, headers=None):
        self.body = body or {}
        self.status_code, self.headers = status_code, headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")
        return self

    def json(self):
        return self.body


class _UC:
    """Unity Catalog's REST API. ``throttled`` listings answer 429 first (the rate limit runs
    40861827715 and 40869045831 met on the read-back listing right after a delete)."""

    def __init__(self, locations, credentials, *, deletes=True, throttled=0, retry_after=None):
        self.locations, self.credentials = dict(locations), set(credentials)
        self.deletes, self.deleted = deletes, []
        self.throttled, self.retry_after = throttled, retry_after
        self.params: list = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, url, params=None):
        self.params.append(params)
        if self.throttled:
            self.throttled -= 1
            headers = {"Retry-After": self.retry_after} if self.retry_after else {}
            return _Response(status_code=HTTPStatus.TOO_MANY_REQUESTS, headers=headers)
        if url.endswith("/external-locations"):
            listed = [{"name": n, "url": u} for n, u in self.locations.items()]
            return _Response({"external_locations": listed})
        return _Response({"storage_credentials": [{"name": n} for n in self.credentials]})

    def delete(self, url):
        name = url.rsplit("/", 1)[-1]
        self.deleted.append(name)
        if self.deletes:
            self.locations.pop(name, None)
            self.credentials.discard(name)
        return _Response()


def _uc(monkeypatch, uc, slept=None):
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "dbc.example")
    monkeypatch.setattr(databricks_uc, "_client", lambda: uc)
    monkeypatch.setattr(databricks_uc.time, "sleep", (slept if slept is not None else []).append)


def test_a_location_stored_without_its_trailing_slash_is_the_runs(monkeypatch):
    """Unity Catalog lists a location at ``.../run_a`` for one created at ``.../run_a/``; it is the
    run's all the same, and a location at ``.../run_ab`` is not."""
    uc = _UC({"loc_run": "r2://pubs@a/run_a", "loc_stem": "r2://pubs@a/run_ab"}, [])
    _uc(monkeypatch, uc)
    databricks_uc.remove_locations_under("r2://pubs@a/run_a/")
    assert uc.deleted == ["loc_run"]
    assert set(uc.locations) == {"loc_stem"}


def test_a_throttled_listing_waits_and_is_read_again(monkeypatch):
    slept: list[float] = []
    uc = _UC({"loc_run": "r2://pubs@a/run_a/"}, [], throttled=2)
    _uc(monkeypatch, uc, slept)
    databricks_uc.remove_locations_under("r2://pubs@a/run_a/")
    assert uc.deleted == ["loc_run"] and not uc.locations
    assert slept == [1.0, 2.0]


def test_a_throttled_listing_waits_as_long_as_unity_catalog_asks(monkeypatch):
    slept: list[float] = []
    uc = _UC({}, [], throttled=1, retry_after="7")
    _uc(monkeypatch, uc, slept)
    databricks_uc.remove_locations_under("r2://pubs@a/run_a/")
    assert slept == [7.0]


def test_a_listing_still_throttled_after_every_wait_raises(monkeypatch):
    uc = _UC({}, [], throttled=100)
    _uc(monkeypatch, uc)
    with pytest.raises(RuntimeError, match="429"):
        databricks_uc.remove_locations_under("r2://pubs@a/run_a/")


def test_a_listing_asks_unity_catalog_to_page_it(monkeypatch):
    uc = _UC({}, [])
    _uc(monkeypatch, uc)
    databricks_uc.remove_locations_under("r2://pubs@a/run_a/")
    assert uc.params and all(p["max_results"] == 0 for p in uc.params)


def test_only_the_external_locations_under_the_runs_prefix_are_deleted(monkeypatch):
    uc = _UC(
        {
            "loc_run": "r2://pubs@a/run_a/",
            "loc_run_sub": "r2://pubs@a/run_a/x/",
            "loc_other": "r2://pubs@a/run_b/",
            "loc_bucket": "r2://pubs@a/",
        },
        [],
    )
    _uc(monkeypatch, uc)
    databricks_uc.remove_locations_under("r2://pubs@a/run_a/")
    assert sorted(uc.deleted) == ["loc_run", "loc_run_sub"]
    assert set(uc.locations) == {"loc_other", "loc_bucket"}


def test_an_external_location_still_listed_after_its_delete_raises(monkeypatch):
    _uc(monkeypatch, _UC({"loc_run": "r2://pubs@a/run_a/"}, [], deletes=False))
    with pytest.raises(RuntimeError, match="loc_run"):
        databricks_uc.remove_locations_under("r2://pubs@a/run_a/")


def test_a_storage_credential_found_before_the_run_stays(monkeypatch):
    uc = _UC({}, [databricks_uc.credential_name("pubs")])
    _uc(monkeypatch, uc)
    databricks_uc.remove_credential("pubs", existed=True)
    assert uc.deleted == []


def test_a_storage_credential_the_run_created_is_deleted(monkeypatch):
    uc = _UC({}, [databricks_uc.credential_name("pubs"), "someone_elses"])
    _uc(monkeypatch, uc)
    databricks_uc.remove_credential("pubs", existed=False)
    assert uc.credentials == {"someone_elses"}


class _Cursor:
    def __init__(self, listed):
        self.listed, self.executed = listed, []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchall(self):
        return self.listed

    def close(self):
        pass


class _Connection:
    def __init__(self, listed):
        self.cur = _Cursor(listed)

    def cursor(self):
        return self.cur


def test_a_catalog_is_dropped_with_its_contents_and_read_back():
    conn = _Connection([])
    databricks_uc.drop_catalog(conn, "e2e_dbx_1")
    assert conn.cur.executed[0] == "DROP CATALOG IF EXISTS `e2e_dbx_1` CASCADE"


def test_a_catalog_still_listed_after_its_drop_raises():
    with pytest.raises(RuntimeError, match="e2e_dbx_1"):
        databricks_uc.drop_catalog(_Connection([("e2e_dbx_1",)]), "e2e_dbx_1")


def test_a_schema_still_listed_after_its_drop_raises():
    with pytest.raises(RuntimeError, match="provisa_src_e2e_1"):
        databricks_uc.drop_schema(
            _Connection([("provisa_src_e2e_1",)]), "workspace", "provisa_src_e2e_1"
        )


# --- Snowflake database -----------------------------------------------------------------------


def test_a_snowflake_database_still_listed_after_its_drop_raises():
    with pytest.raises(RuntimeError, match="e2e_sf_1"):
        snowflake_warehouse.drop_database(_Connection([("t", "e2e_sf_1")]), "e2e_sf_1")


def test_a_snowflake_database_of_another_case_is_not_the_runs():
    # SHOW ... LIKE matches without case; the quoted name the run dropped is exact.
    conn = _Connection([("t", "E2E_SF_1")])
    snowflake_warehouse.drop_database(conn, "e2e_sf_1")
    assert conn.cur.executed[0] == 'DROP DATABASE IF EXISTS "e2e_sf_1"'


# --- Synapse lane teardown --------------------------------------------------------------------


def test_every_synapse_teardown_step_runs_and_a_failed_one_fails_the_lane(monkeypatch):
    ran = []

    def az(*args, **_kw):
        ran.append(args[:3])
        if args[:3] == ("synapse", "workspace", "delete"):
            raise RuntimeError("workspace delete refused")
        if args[:3] == ("storage", "account", "show"):
            raise RuntimeError("ResourceNotFound")
        return ""

    monkeypatch.setattr(synapse_provision, "_az", az)
    lane = {
        "resource_group": "rg",
        "workspace": "provisa-syn-e2e-1",
        "storage": "provisasyn1",
        "storage_scope": "/subscriptions/s/rg/storage",
    }
    with pytest.raises(ExceptionGroup) as raised:
        synapse_provision._teardown(lane)
    assert [str(e) for e in raised.value.exceptions] == ["workspace delete refused"]
    assert ran == [
        ("synapse", "workspace", "delete"),
        ("role", "assignment", "delete"),
        ("storage", "account", "delete"),
        ("storage", "account", "show"),
    ]
