# Copyright (c) 2026 Kenneth Stott
# Canary: 2b8e5f31-c6a4-4d79-8e10-a47d3c9f6b52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The organisation's default Spark cluster (REQ-2225): a Spark Connect address and a credential
kept as a secret reference, set by its administrator in that organisation alone, with a connection
test. A source's own cluster wins; the default applies where a source names none; a demo install
falls to its local test cluster; with none of these the cluster is reported as not configured.
The control plane is a real PostgreSQL database; the Spark Connect server the probe calls is a
gRPC server of this test answering as one does; every credential is made up."""

# Requirements: REQ-2225
from __future__ import annotations

from concurrent import futures
from typing import Any

import grpc
import pytest
import sqlalchemy as sa

from provisa.api.admin import spark_cluster_router as router
from provisa.api.errors import ApiError
from provisa.core import schema_admin, secrets_store, spark_cluster
from provisa.core.spark_cluster import SparkClusterRefused

pytestmark = pytest.mark.asyncio

TOKEN = "made-up-spark-token"
#: The guard is stood in for, so no request is read.
_NO_REQUEST: Any = None


class _Sealer:
    def encrypt(self, value: bytes) -> bytes:
        return b"sealed:" + value[::-1]

    def decrypt(self, blob: bytes) -> bytes:
        return blob[len(b"sealed:") :][::-1]


@pytest.fixture
def plane(control_plane, monkeypatch):
    with control_plane.engine().begin() as conn:
        for org_id in ("acme", "globex"):  # org_spark_clusters' foreign key
            conn.execute(sa.insert(schema_admin.orgs).values(id=org_id, name=org_id))
    monkeypatch.setattr(secrets_store, "_cipher", lambda *, mint: _Sealer())

    async def held(conn, *, mint):
        return None

    monkeypatch.setattr(secrets_store, "_hold_the_deployments_key", held)
    monkeypatch.delenv("PROVISA_DEMO", raising=False)
    monkeypatch.delenv(spark_cluster.LOCAL_TEST_CLUSTER_ENV, raising=False)
    return control_plane.platform


def _stored(plane) -> list[dict]:
    with plane.engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(sa.select(schema_admin.org_spark_clusters))]


async def _put(plane, org="acme", **changed):
    given = {"address": "sc://spark.acme.test:15002", "credential": None, "actor": "uid-ada"}
    given.update(changed)
    return await spark_cluster.put(plane, org, **given)


class TestTheAddress:
    async def test_an_address_is_split_into_host_port_and_parameters(self):
        found = spark_cluster.parse("sc://spark.acme.test:443/;use_ssl=true;user_id=ada")
        assert (found.host, found.port, found.params, found.tls) == (
            "spark.acme.test",
            443,
            {"use_ssl": "true", "user_id": "ada"},
            True,
        )

    async def test_an_address_with_no_port_is_spark_connects_own(self):
        assert spark_cluster.parse("sc://spark.acme.test").port == 15002

    @pytest.mark.parametrize(
        "address", ["http://spark:15002", "sc://", "sc://u:p@spark:1", "sc://spark/;broken"]
    )
    async def test_anything_else_is_refused_by_name(self, address):
        with pytest.raises(SparkClusterRefused) as refused:
            spark_cluster.parse(address)
        assert refused.value.code == "spark_cluster.address"

    async def test_a_token_is_carried_as_a_parameter_and_turns_tls_on(self):
        assert spark_cluster.with_token("sc://s:1", "t") == "sc://s:1/;token=t"
        assert spark_cluster.with_token("sc://s:1/;user_id=a", "t") == "sc://s:1/;user_id=a;token=t"
        assert spark_cluster.parse("sc://s:1/;token=t").tls


class TestTheStore:
    async def test_the_default_is_kept_as_entered(self, plane):
        kept = await _put(plane, credential="${secret:spark_token}")
        assert (kept.address, kept.credential) == (
            "sc://spark.acme.test:15002",
            "${secret:spark_token}",
        )
        (row,) = _stored(plane)
        assert row["org_id"] == "acme" and row["updated_by"] == "uid-ada"

    async def test_a_second_save_replaces_the_first(self, plane):
        await _put(plane, credential="${secret:spark_token}")
        kept = await _put(plane, address="sc://other.acme.test", credential="")
        assert (kept.address, kept.credential) == ("sc://other.acme.test", None)
        assert len(_stored(plane)) == 1

    async def test_one_organisations_default_is_not_anothers(self, plane):
        await _put(plane)
        assert await spark_cluster.read(plane, "globex") is None

    @pytest.mark.parametrize(
        ("changed", "code"),
        [
            ({"address": "spark.acme.test:15002"}, "spark_cluster.address"),
            ({"address": "sc://s:1/;token=abc"}, "spark_cluster.token_in_address"),
            ({"credential": TOKEN}, "spark_cluster.credential_not_reference"),
        ],
    )
    async def test_a_default_that_cannot_be_kept_is_refused_by_name(self, plane, changed, code):
        with pytest.raises(SparkClusterRefused) as refused:
            await _put(plane, **changed)
        assert refused.value.code == code
        assert _stored(plane) == []

    async def test_forgetting_removes_it(self, plane):
        await _put(plane)
        assert await spark_cluster.forget(plane, "acme") is True
        assert await spark_cluster.forget(plane, "acme") is False


class TestWhichClusterRuns:
    async def test_a_sources_own_cluster_wins(self, plane):
        await _put(plane)
        found = await spark_cluster.resolved(plane, "acme", "sc://own.acme.test:15002")
        assert (found.address, found.origin) == ("sc://own.acme.test:15002", "source")

    async def test_a_source_naming_none_runs_on_the_default_its_token_resolved(self, plane):
        await secrets_store.put(
            plane, "acme", "spark_token", TOKEN, owner_id=secrets_store.ORG_OWNER
        )
        await _put(plane, credential="${secret:spark_token}")
        for own in (None, "", "  "):
            found = await spark_cluster.resolved(plane, "acme", own)
            assert (found.address, found.origin) == (
                f"sc://spark.acme.test:15002/;token={TOKEN}",
                "organisation",
            )
        assert _stored(plane)[0]["credential"] == "${secret:spark_token}"

    async def test_a_credential_is_resolved_in_its_own_organisations_vault_only(self, plane):
        await secrets_store.put(
            plane, "globex", "spark_token", TOKEN, owner_id=secrets_store.ORG_OWNER
        )
        await _put(plane, credential="${secret:spark_token}")
        with pytest.raises(SparkClusterRefused) as refused:
            await spark_cluster.resolved(plane, "acme", None)
        assert refused.value.code == "spark_cluster.unresolved"

    async def test_with_neither_a_production_install_reports_it_not_configured(self, plane):
        with pytest.raises(SparkClusterRefused) as refused:
            await spark_cluster.resolved(plane, "acme", None)
        assert refused.value.code == "spark_cluster.not_configured"

    async def test_a_demo_install_runs_on_its_local_test_cluster(self, plane, monkeypatch):
        monkeypatch.setenv("PROVISA_DEMO", "true")
        monkeypatch.setenv(spark_cluster.LOCAL_TEST_CLUSTER_ENV, "sc://127.0.0.1:15999")
        found = await spark_cluster.resolved(plane, "acme", None)
        assert (found.address, found.origin) == ("sc://127.0.0.1:15999", "local_test")
        await _put(plane)
        assert (await spark_cluster.resolved(plane, "acme", None)).origin == "organisation"


# -- the connection test ----------------------------------------------------------------------------


@pytest.fixture
def spark_connect():
    """A gRPC server answering AnalyzePlan as Spark Connect does, and what it was asked."""
    seen: dict = {"requests": [], "metadata": []}

    def analyze(request: bytes, context) -> bytes:
        seen["requests"].append(request)
        seen["metadata"].append(dict(context.invocation_metadata()))
        version = spark_cluster._field(1, b"4.3.0")
        return spark_cluster._field(1, b"session") + spark_cluster._field(8, version)

    handler = grpc.method_handlers_generic_handler(
        "spark.connect.SparkConnectService",
        {
            "AnalyzePlan": grpc.unary_unary_rpc_method_handler(
                analyze, request_deserializer=None, response_serializer=None
            )
        },
    )
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers((handler,))
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    seen["address"] = f"sc://127.0.0.1:{port}"
    yield seen
    server.stop(None)


class TestTheConnectionTest:
    async def test_the_probe_asks_the_spark_version_and_answers_it(self, spark_connect):
        assert spark_cluster.probe(spark_connect["address"]) == "4.3.0"
        (request,) = spark_connect["requests"]
        fields = {n: v for n, _w, v in spark_cluster._fields(request)}
        assert set(fields) == {1, 2, 10} and fields[10] == b""
        session_id = fields[1]
        assert isinstance(session_id, bytes) and len(session_id) == 36  # a UUID

    async def test_a_cluster_that_does_not_answer_is_named_with_the_reason(self):
        from tests.port_lease import lease_ports

        (port,) = lease_ports(1)
        with pytest.raises(SparkClusterRefused) as refused:
            spark_cluster.probe(f"sc://127.0.0.1:{port}")
        assert refused.value.code == "spark_cluster.unreachable"
        assert "UNAVAILABLE" in str(refused.value)

    async def test_an_answer_without_a_version_is_not_spark_connect(self):
        with pytest.raises(ValueError, match="no Spark version"):
            spark_cluster.version_of(spark_cluster._field(1, b"session"))


class TestTheAdminSurface:
    @pytest.fixture
    def wired(self, plane, monkeypatch):
        seen: dict = {"guarded": [], "probed": []}

        async def guard(request, org_id):
            seen["guarded"].append(org_id)
            if seen.get("refuse"):
                raise ApiError(403, "secrets.org_settings_in_org_required", "no")
            return "uid-ada"

        def probe(address):
            seen["probed"].append(address)
            return "4.3.0"

        monkeypatch.setattr(router, "_org_guard", guard)
        monkeypatch.setattr(router, "_admin_pool", lambda: plane)
        monkeypatch.setattr(spark_cluster, "probe", probe)
        return seen

    async def test_it_is_opened_by_the_organisations_own_right_in_that_organisation(self, wired):
        empty = await router.get_cluster(_NO_REQUEST, "acme")
        assert empty["cluster"] is None and empty["in_use"]["origin"] is None
        body = router.ClusterBody(address="sc://spark.acme.test:15002")
        saved = await router.put_cluster(_NO_REQUEST, "acme", body)
        assert saved["in_use"] == {
            "origin": "organisation",
            "label": None,
            "target": "spark.acme.test:15002",
        }
        assert (await router.get_cluster(_NO_REQUEST, "acme"))["cluster"]["address"] == body.address
        await router.delete_cluster(_NO_REQUEST, "acme")
        assert wired["guarded"] == ["acme"] * 4

    async def test_the_test_calls_the_cluster_as_entered_its_token_resolved(self, wired, plane):
        await secrets_store.put(
            plane, "acme", "spark_token", TOKEN, owner_id=secrets_store.ORG_OWNER
        )
        answered = await router.test_cluster(
            _NO_REQUEST,
            "acme",
            router.ClusterBody(
                address="sc://unsaved.acme.test", credential="${secret:spark_token}"
            ),
        )
        assert answered == {
            "ok": True,
            "spark_version": "4.3.0",
            "target": "unsaved.acme.test:15002",
        }
        assert wired["probed"] == [f"sc://unsaved.acme.test/;token={TOKEN}"]

    async def test_an_empty_test_calls_the_cluster_a_source_naming_none_uses(
        self, wired, monkeypatch
    ):
        monkeypatch.setenv("PROVISA_DEMO", "true")
        monkeypatch.setenv(spark_cluster.LOCAL_TEST_CLUSTER_ENV, "sc://127.0.0.1:15999")
        state = await router.get_cluster(_NO_REQUEST, "acme")
        assert state["in_use"]["label"] == "local test cluster"
        await router.test_cluster(_NO_REQUEST, "acme", router.ClusterBody(address=""))
        assert wired["probed"] == ["sc://127.0.0.1:15999"]

    async def test_without_the_right_nothing_is_read_kept_removed_or_called(self, wired):
        wired["refuse"] = True
        body = router.ClusterBody(address="sc://spark.acme.test")
        for call in (
            lambda: router.get_cluster(_NO_REQUEST, "acme"),
            lambda: router.put_cluster(_NO_REQUEST, "acme", body),
            lambda: router.delete_cluster(_NO_REQUEST, "acme"),
            lambda: router.test_cluster(_NO_REQUEST, "acme", body),
        ):
            with pytest.raises(ApiError) as raised:
                await call()
            assert raised.value.status_code == 403
        assert wired["probed"] == []

    async def test_a_refusal_is_answered_by_name(self, wired):
        with pytest.raises(ApiError) as raised:
            await router.put_cluster(
                _NO_REQUEST, "acme", router.ClusterBody(address="sc://s", credential=TOKEN)
            )
        assert raised.value.status_code == 400
