# Copyright (c) 2026 Kenneth Stott
# Canary: 6a5ae79e-1340-47e8-b815-f4201ca1da69
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Kafka source with SASL credentials on a hosted engine shard (REQ-2073).

A shard's coordinator shares no directory with the control plane, so the client properties Trino's
Kafka connector reads from a file (a SASL password among them) reach it through the shard's own
Secret: a volume the node holds in memory, readable by Trino's group alone. The catalog is created
once the kubelet has synced the file in. A source whose topics' columns only the model declares (no
schema registry) needs a table-description directory per catalog, which a running shard cannot be
given, so it is refused by name when it is saved, and one stored before that rule gets no catalog
at boot, named at ERROR, without stopping the boot. A coordinator that comes back without its catalogs gets the Kafka
ones back with the rest.
"""

# Requirements: REQ-2073

from __future__ import annotations

import base64
import json
from types import SimpleNamespace

import httpx
import pytest
from trino.exceptions import TrinoQueryError

from provisa.core import catalog as catalog_module
from provisa.federation import k8s_provisioner as prov
from provisa.federation import trino_lifecycle
from tests.unit.test_k8s_engine_provisioner import (  # noqa: F401 -- `configured` is a fixture, used by name
    _cluster_get,
    _mock_api,
    _no_sleep,
    configured,
)

_SASL_SOURCE = {
    "id": "events",
    "bootstrap_servers": "broker:9092",
    "schema_registry_url": "http://registry:8081",
    "auth": {"type": "sasl_plain", "username": "svc", "password": "s3cret-kafka"},
    "topics": [{"id": "orders", "topic": "orders"}],
}


# ── the shard's pod ─────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("configured")
def test_the_shard_reads_kafka_client_files_from_its_secret_in_memory():
    spec = prov._deployment_manifest("shared_1", "shared")["spec"]["template"]["spec"]
    (volume,) = [v for v in spec["volumes"] if v["name"] == "kafka-client"]
    assert volume["secret"] == {
        "secretName": "trino-shared-1-kafka-client",
        "defaultMode": 0o440,
    }
    (mount,) = [m for m in spec["containers"][0]["volumeMounts"] if m["name"] == "kafka-client"]
    assert mount == {"name": "kafka-client", "mountPath": prov.KAFKA_CLIENT_DIR, "readOnly": True}
    # Group-owned by Trino's group, so 0440 is Trino's to read and no one else's.
    assert spec["securityContext"]["fsGroup"] == prov.TRINO_UID
    assert prov.KAFKA_CLIENT_DIR == trino_catalog_client_dir()


def trino_catalog_client_dir() -> str:
    from provisa.core.trino_catalog_files import _KAFKA_CLIENT

    return _KAFKA_CLIENT


@pytest.mark.usefixtures("configured")
async def test_bringing_a_shard_up_creates_its_secret_and_keeps_one_that_exists(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/secrets"):
            return httpx.Response(409, json={"reason": "AlreadyExists"})
        if "/deployments/" in request.url.path and request.method == "GET":
            return httpx.Response(200, json={"status": {"readyReplicas": 1}})
        if request.method == "PATCH":
            return httpx.Response(200, json={})
        return _cluster_get()

    calls = _mock_api(monkeypatch, handler)
    monkeypatch.setattr(prov.asyncio, "sleep", _no_sleep)
    await prov.ensure_shard_running("shared_1")

    (create,) = [c for c in calls if c.method == "POST" and c.url.path.endswith("/secrets")]
    body = json.loads(create.content)
    assert body["metadata"]["name"] == "trino-shared-1-kafka-client"
    assert "data" not in body
    # Never applied: an apply of the empty Secret would drop the keys already written.
    assert not [
        c
        for c in calls
        if c.method == "PATCH" and c.url.path.endswith("/trino-shared-1-kafka-client")
    ]


@pytest.mark.usefixtures("configured")
async def test_a_catalogs_client_properties_are_written_into_the_shards_secret(monkeypatch):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PATCH":
            return httpx.Response(200, json={})
        return _cluster_get()

    calls = _mock_api(monkeypatch, handler)
    await prov.put_kafka_client_properties("shared_1", "org_default__events", "sasl=1\n")

    (patch,) = [c for c in calls if c.method == "PATCH"]
    assert patch.url.path.endswith("/secrets/trino-shared-1-kafka-client")
    assert patch.headers["content-type"] == "application/merge-patch+json"
    data = json.loads(patch.content)["data"]
    assert base64.b64decode(data["org_default__events.properties"]).decode() == "sasl=1\n"


# ── registration ────────────────────────────────────────────────────────────────


@pytest.fixture
def on_a_shard(monkeypatch, tmp_path):
    """A provisioning deployment whose active org is served by shard ``shared_1``."""
    from provisa.core import settings_registry
    from provisa.federation import engine_wake

    monkeypatch.setattr(prov, "provisioning_available", lambda: True)
    monkeypatch.setattr(engine_wake, "active_shard", lambda state: "shared_1")
    monkeypatch.setattr(settings_registry, "value", lambda key: 30)
    monkeypatch.setenv("PROVISA_TRINO_ETC_DIR", str(tmp_path))
    written: list[tuple[str, str, str]] = []

    async def _put(shard, catalog, text):
        written.append((shard, catalog, text))

    monkeypatch.setattr(prov, "put_kafka_client_properties", _put)
    created: list[tuple[str, float]] = []
    monkeypatch.setattr(
        catalog_module,
        "create_kafka_catalog",
        lambda conn, ks, catalog, client_file_wait=0: created.append((catalog, client_file_wait)),
    )
    return SimpleNamespace(written=written, created=created, etc=tmp_path)


def test_a_sasl_source_reaches_the_shard_through_its_secret(on_a_shard):
    state = SimpleNamespace(engine_conn=object())
    trino_lifecycle.register_kafka_catalog(state, _SASL_SOURCE, "org_default__events")

    ((shard, catalog, text),) = on_a_shard.written
    assert (shard, catalog) == ("shared_1", "org_default__events")
    assert 'password="s3cret-kafka"' in text
    # Created once the file is there, waiting up to the engine's ready timeout for the kubelet.
    assert on_a_shard.created == [("org_default__events", 30.0)]
    # Nothing is written to the control plane's disk.
    assert not any(on_a_shard.etc.rglob("*.properties"))


def test_a_source_without_credentials_writes_no_secret(on_a_shard):
    source = {key: value for key, value in _SASL_SOURCE.items() if key != "auth"}
    trino_lifecycle.register_kafka_catalog(SimpleNamespace(engine_conn=object()), source, "c")
    assert on_a_shard.written == []
    assert on_a_shard.created == [("c", 0.0)]


def test_a_stored_source_without_a_schema_registry_gets_no_catalog_and_does_not_stop_the_boot(
    on_a_shard, caplog
):
    source = {key: value for key, value in _SASL_SOURCE.items() if key != "schema_registry_url"}
    trino_lifecycle.register_kafka_catalog(SimpleNamespace(engine_conn=object()), source, "c")
    assert on_a_shard.written == [] and on_a_shard.created == []
    assert "'events'" in caplog.text and "shared_1" in caplog.text


# ── refused when saved ──────────────────────────────────────────────────────────


@pytest.fixture
def engine_shard(monkeypatch):
    from provisa.core.repositories import kafka_source as repo

    holder = SimpleNamespace(shard="shared_1")
    monkeypatch.setattr(repo, "_engine_shard_resolver", lambda: holder.shard)
    return holder


async def test_saving_a_source_without_a_schema_registry_on_a_shard_is_refused(engine_shard):
    from provisa.core.repositories import kafka_source as repo

    source = {key: value for key, value in _SASL_SOURCE.items() if key != "schema_registry_url"}
    with pytest.raises(
        repo.KafkaSourceRefused, match="'events'.*shard shared_1.*schema_registry_url"
    ):
        await repo.upsert(None, source)


def test_a_source_with_a_schema_registry_is_saved_on_a_shard(engine_shard):
    from provisa.core.repositories import kafka_source as repo

    repo.refuse_on_engine_shard(_SASL_SOURCE)


def test_off_a_shard_a_source_without_a_schema_registry_is_saved(engine_shard):
    from provisa.core.repositories import kafka_source as repo

    engine_shard.shard = None
    repo.refuse_on_engine_shard({"id": "events", "bootstrap_servers": "broker:9092"})


def test_the_app_installs_the_resolver_from_the_orgs_engine_lane(monkeypatch):
    from provisa.api import app
    from provisa.core.repositories import kafka_source as repo
    from provisa.federation import engine_wake

    assert repo._engine_shard_resolver is app._org_engine_shard
    monkeypatch.setattr(prov, "provisioning_available", lambda: False)
    assert app._org_engine_shard() is None
    monkeypatch.setattr(prov, "provisioning_available", lambda: True)
    monkeypatch.setattr(engine_wake, "active_shard", lambda state: "org_acme")
    assert app._org_engine_shard() == "org_acme"


# ── waiting for the kubelet ─────────────────────────────────────────────────────


class _Coordinator:
    """CREATE CATALOG answered with ``refusals`` errors before it succeeds."""

    def __init__(self, refusals: list[str]):
        self.refusals = refusals
        self.statements: list[str] = []

    def cursor(self):
        return self

    def execute(self, sql: str) -> None:
        self.statements.append(sql)
        if sql.startswith("SHOW CATALOGS"):
            return
        if self.refusals:
            raise TrinoQueryError({"message": self.refusals.pop(0), "errorName": "GENERIC"})

    def fetchall(self):
        return []


@pytest.fixture
def kafka_create(monkeypatch):
    monkeypatch.setattr(catalog_module, "wait_until_ready", lambda conn: None)
    monkeypatch.setattr(catalog_module, "catalog_exists", lambda conn, name: False)
    monkeypatch.setattr(catalog_module.time, "sleep", lambda s: None)


_MISSING = "Invalid configuration property kafka.config.resources: file does not exist"


def test_the_catalog_is_created_once_the_client_file_has_synced(kafka_create):
    coordinator = _Coordinator([_MISSING, _MISSING])
    catalog_module.create_kafka_catalog(coordinator, _SASL_SOURCE, "c", client_file_wait=60)
    assert len([s for s in coordinator.statements if s.startswith("CREATE CATALOG")]) == 3


def test_any_other_refusal_is_raised_at_once(kafka_create):
    coordinator = _Coordinator(["Access Denied: Cannot create catalog c"])
    with pytest.raises(TrinoQueryError, match="Access Denied"):
        catalog_module.create_kafka_catalog(coordinator, _SASL_SOURCE, "c", client_file_wait=60)
    assert len(coordinator.statements) == 1


def test_a_client_file_that_never_arrives_is_raised(kafka_create):
    coordinator = _Coordinator([_MISSING] * 3)
    with pytest.raises(TrinoQueryError, match="kafka.config.resources"):
        catalog_module.create_kafka_catalog(coordinator, _SASL_SOURCE, "c", client_file_wait=0)


# ── a coordinator that came back without them ───────────────────────────────────


async def test_a_restored_terminal_gets_its_kafka_catalogs_back_as_the_deployment_org(
    monkeypatch,
):
    import contextlib

    from provisa.api import app_loaders
    from provisa.core import config_loader
    from provisa.core.request_context import current_org, reset_current_org, set_current_org
    from provisa.federation import engine_wake

    issued: list[tuple[str, str, str | None]] = []

    class _Engine:
        def register_kafka_catalog(self, ks, catalog):
            issued.append((ks["id"], catalog, current_org.get()))

    @contextlib.asynccontextmanager
    async def _acquire():
        yield object()

    async def _store_raw(raw, conn):
        return {"kafka_sources": [_SASL_SOURCE]}

    monkeypatch.setattr(config_loader, "store_raw", _store_raw)
    from provisa.core import secrets_store

    @contextlib.asynccontextmanager
    async def _bound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _bound)
    monkeypatch.setattr(
        app_loaders,
        "catalog_name_for_source",
        lambda state, kind, sid: f"org_{current_org.get()}__{sid}",
    )
    state = SimpleNamespace(
        org_id="default",
        model_db=SimpleNamespace(acquire=_acquire),
        federation_engine=_Engine(),
    )
    token = set_current_org("acme")  # the tenant whose query found the restart
    try:
        await engine_wake._reissue_kafka_catalogs(state, {})
    finally:
        reset_current_org(token)

    assert issued == [("events", "org_default__events", "default")]


def test_the_restore_reissues_the_kafka_catalogs():
    import inspect

    from provisa.federation import engine_wake

    source = inspect.getsource(engine_wake.restore_shared_terminal)
    assert "await _reissue_kafka_catalogs(state, raw)" in source


async def test_an_apply_that_saves_one_answers_422_naming_it(engine_shard):
    from provisa.api.app import create_app
    from provisa.core.repositories import kafka_source as repo

    with pytest.raises(repo.KafkaSourceRefused) as refused:
        await repo.upsert(None, {"id": "events", "bootstrap_servers": "broker:9092"})
    handler = create_app().exception_handlers[repo.KafkaSourceRefused]
    response = await handler(None, refused.value)  # type: ignore[arg-type]
    assert response.status_code == 422
    body = json.loads(response.body)
    assert body["code"] == "sources.kafka_needs_schema_registry"
    assert body["params"] == {"source": "events", "shard": "shared_1"}
    assert "'events'" in body["detail"]
