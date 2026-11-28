# Copyright (c) 2026 Kenneth Stott
# Canary: 0c7d2e91-6a4b-4f35-9e18-b3d5a7f04c62
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Python code source opens a Spark source's session (REQ-2225, amended 2026-11-25).

``provisa_services.spark.session(<source>)`` asks Provisa's services for ``spark/<source>``: the
Spark Connect address of the organisation's Spark source of that name, its own cluster else the
organisation's default, its token resolved. The answer is recorded in the org's trail and the token
is matched by value from then on. A name that is not a Spark source, or a cluster that cannot be
resolved, is refused by name. ``provisa_services.spark.dataframe`` hands a governed table's rows,
read as the calling role, to that session."""

# Requirements: REQ-2225

from __future__ import annotations

import json
import sys
import types
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest

from provisa.codesource import services
from provisa.core import spark_cluster

pytestmark = pytest.mark.unit


# -- the services' answer ---------------------------------------------------------------------------


@pytest.fixture
def token():
    issued = services.issue(services.ServiceGrant(org_id="acme", source_id="lakehouse"))
    yield issued
    services.revoke(issued)


def _call(token: str, path: str, method: str = "GET") -> tuple[int, Any]:
    status, content_type, answered = services.answer(method, f"/{token}/{path}", b"", {})
    assert content_type == services.JSON
    return status, json.loads(answered)


@pytest.fixture
def org(monkeypatch) -> dict:
    """The org's sources, its default Spark cluster and its trail, with no database behind them."""
    held: dict = {
        "sources": {
            "spark_jobs": {"type": "spark", "mapping": {"settings": {}}},
            "spark_own": {
                "type": "spark",
                "mapping": {"settings": {"cluster": "sc://own.acme.test:15002"}},
            },
            "ledger": {"type": "python", "mapping": {}},
        },
        "default": spark_cluster.Resolved(
            "sc://default.acme.test:15002/;token=s3cr3t-t0ken", spark_cluster.ORIGIN_ORG
        ),
        "audited": [],
        "noted": [],
    }

    class _Conn:
        pass

    class _Db:
        @asynccontextmanager
        async def acquire(self):
            yield _Conn()

    @asynccontextmanager
    async def bound(grant):
        yield _Db(), {}

    async def get(conn, source_id):
        return held["sources"].get(source_id)

    async def default(admin_db, org_id):
        found = held["default"]
        if isinstance(found, Exception):
            raise found
        return found

    async def audit(db, **kw):
        held["audited"].append(kw)

    monkeypatch.setattr("provisa.codesource.llm_service.org_bound", bound)
    monkeypatch.setattr("provisa.core.repositories.source.get", get)
    monkeypatch.setattr(spark_cluster, "_resolved_default", default)
    monkeypatch.setattr("provisa.core.org_membership.record_admin_action", audit)
    monkeypatch.setattr(
        "provisa.security.redaction.note_secret_value",
        lambda name, value: held["noted"].append((name, value)),
    )
    return held


def test_a_spark_source_naming_no_cluster_answers_the_organisations_default(token, org):
    status, answered = _call(token, "spark/spark_jobs")
    assert status == 200, answered
    assert answered == {"address": "sc://default.acme.test:15002/;token=s3cr3t-t0ken"}
    assert org["noted"] == [("spark:spark_jobs", "s3cr3t-t0ken")]
    (audited,) = org["audited"]
    assert audited["action"] == "code_source.spark.session"
    assert audited["actor_id"] == "source:lakehouse" and audited["subject_id"] == "spark_jobs"
    assert audited["detail"] == {"source": "lakehouse", "origin": spark_cluster.ORIGIN_ORG}


def test_a_spark_sources_own_cluster_is_answered_over_the_default(token, org):
    org["default"] = AssertionError("the default is not read when the source names its own")
    status, answered = _call(token, "spark/spark_own")
    assert status == 200, answered
    assert answered == {"address": "sc://own.acme.test:15002"}
    assert org["noted"] == []


@pytest.mark.parametrize("name", ["ledger", "nowhere"])
def test_a_name_that_is_not_a_spark_source_is_refused_by_name(token, org, name):
    status, answered = _call(token, f"spark/{name}")
    assert status == 404
    assert answered["error"]["message"] == f"the organization has no Spark source {name!r}"
    assert org["audited"] == []


def test_a_cluster_that_cannot_be_resolved_is_refused_naming_the_source(token, org):
    org["default"] = spark_cluster.SparkClusterRefused(
        "not_configured", "No Spark cluster is configured", status=409
    )
    status, answered = _call(token, "spark/spark_jobs")
    assert status == 409
    assert answered["error"]["message"] == (
        "the Spark source 'spark_jobs': No Spark cluster is configured"
    )


def test_a_session_is_read_with_get_and_named(token, org):
    status, answered = _call(token, "spark/spark_jobs", method="POST")
    assert status == 405
    status, answered = _call(token, "spark/")
    assert status == 400 and "spark/<source>" in answered["error"]["message"]


# -- the code's side ------------------------------------------------------------------------------


@pytest.fixture
def ps(monkeypatch):
    """Provisa's services module, on the path as the host puts it (provisa/python/host.py)."""
    import importlib

    from provisa.python.source import HOST

    monkeypatch.syspath_prepend(str(HOST.parent / "services"))
    module = importlib.import_module("provisa_services")
    yield module
    sys.modules.pop("provisa_services", None)


@pytest.fixture
def pyspark(monkeypatch) -> dict:
    """pyspark.sql's SparkSession, stood in for: the address a session was opened on."""
    opened: dict = {}

    class _Session:
        def __init__(self, address: str) -> None:
            self.address = address

        def createDataFrame(self, data):  # noqa: N802 -- pyspark's name
            return ("frame", self.address, data)

    class _Builder:
        def remote(self, address: str):
            opened["address"] = address
            return self

        def getOrCreate(self):  # noqa: N802 -- pyspark's name
            return _Session(opened["address"])

    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = SimpleNamespace(builder=_Builder())  # type: ignore[attr-defined]
    package = types.ModuleType("pyspark")
    package.sql = sql  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "pyspark", package)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    return opened


def test_session_opens_the_address_the_services_answer(ps, pyspark):
    asked: list = []

    def transport(method, path, data, headers):
        asked.append((method, path))
        return 200, json.dumps({"address": "sc://spark.acme.test:15002"}).encode()

    reset = ps._transport.set(transport)  # pyright: ignore[reportPrivateUsage]
    try:
        session = ps.spark.session("spark jobs/eu")
    finally:
        ps._transport.reset(reset)  # pyright: ignore[reportPrivateUsage]
    assert asked == [("GET", "spark/spark%20jobs%2Feu")]
    assert pyspark["address"] == "sc://spark.acme.test:15002"
    assert session.address == "sc://spark.acme.test:15002"


def test_a_refused_session_raises_naming_the_service(ps, pyspark):
    def transport(method, path, data, headers):
        body = {"error": {"message": "the organization has no Spark source 'x'", "code": 404}}
        return 404, json.dumps(body).encode()

    reset = ps._transport.set(transport)  # pyright: ignore[reportPrivateUsage]
    try:
        with pytest.raises(ps.ServiceError) as refused:
            ps.spark.session("x")
    finally:
        ps._transport.reset(reset)  # pyright: ignore[reportPrivateUsage]
    assert refused.value.service == "spark" and refused.value.status == 404
    assert "no Spark source 'x'" in str(refused.value)
    assert "address" not in pyspark


def test_dataframe_hands_the_tables_rows_read_as_the_caller_to_the_session(ps):
    rows = object()
    table = SimpleNamespace(read=lambda: rows)
    session = SimpleNamespace(createDataFrame=lambda data: ("frame", data))
    assert ps.spark.dataframe(session, table) == ("frame", rows)
