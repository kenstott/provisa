# Copyright (c) 2026 Kenneth Stott
# Canary: 206868e8-e4fa-43f7-9b9a-95aed3ae79a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-147: a Kafka source's TLS and SASL settings reach every path that opens Kafka, a plain cluster
is opened as it always was, and no secret is written where it should not be."""

from __future__ import annotations

import datetime
import io
import json
import logging
import re
import ssl
import subprocess
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from provisa.core.models import Source, SourceType
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.kafka import landed_offsets
from provisa.kafka.fetch import KafkaConnection, fetch_rows, iter_row_batches
from provisa.kafka.security import (
    HINT_CA,
    HINT_MECHANISM,
    HINT_PROTOCOL,
    PLAINTEXT,
    KafkaSecurity,
    declared_settings,
    of_declared,
)

_PASSWORD = "s3cret-pw"


def _ca_pem() -> str:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, "test-ca")])
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode()


CA = _ca_pem()


def _secured(mechanism: str = "SCRAM-SHA-256") -> KafkaSecurity:
    return KafkaSecurity("SASL_SSL", mechanism, "api-key", _PASSWORD, CA)


def _source(**kw) -> Source:
    fields = {
        "id": "k",
        "type": SourceType.kafka,
        "host": "broker",
        "port": 9093,
        "username": "api-key",
        "password": _PASSWORD,
        "federation_hints": {
            HINT_PROTOCOL: "SASL_SSL",
            HINT_MECHANISM: "SCRAM-SHA-256",
            HINT_CA: CA,
        },
    }
    return Source(**{**fields, **kw})


class _Consumer:
    """Records what each aiokafka consumer was opened with."""

    opened: list[dict] = []

    def __init__(self, *topics, **kwargs) -> None:
        type(self).opened.append(kwargs)
        self.commits: list[dict] = []
        self._client = SimpleNamespace(fetch_all_metadata=self._metadata)

    async def _metadata(self):
        return SimpleNamespace(partitions_for_topic=lambda _t: {0})

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def getmany(self, **_kw):
        return {}

    def assign(self, _partitions) -> None: ...

    def assignment(self):
        return set()

    def partitions_for_topic(self, _t):
        return set()

    async def end_offsets(self, _partitions):
        return {}

    async def committed(self, _tp):
        return None

    async def commit(self, offsets) -> None:
        self.commits.append(offsets)


@pytest.fixture
def consumers(monkeypatch) -> list[dict]:
    import aiokafka

    _Consumer.opened = []
    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", _Consumer)
    return _Consumer.opened


def _assert_secured(kwargs: dict, mechanism: str = "SCRAM-SHA-256") -> None:
    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == mechanism
    assert kwargs["sasl_plain_username"] == "api-key"
    assert kwargs["sasl_plain_password"] == _PASSWORD
    assert isinstance(kwargs["ssl_context"], ssl.SSLContext)


def _assert_plain(kwargs: dict) -> None:
    assert not {k for k in kwargs if k.startswith(("security", "sasl", "ssl"))}


# ------ the settings ------------------------------------------------------------------------------


@pytest.mark.parametrize("mechanism", ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
def test_each_mechanism_reaches_aiokafka(mechanism):
    _assert_secured(_secured(mechanism).client_kwargs(), mechanism)


def test_a_plain_cluster_gives_a_client_nothing():
    assert PLAINTEXT.client_kwargs() == {}
    assert PLAINTEXT.client_properties() is None
    assert PLAINTEXT.pipeline_config() is None


def test_a_source_naming_no_settings_is_a_plain_cluster():
    plain = Source(id="k", type=SourceType.kafka, host="broker", port=9092)
    assert KafkaSecurity.of_source(plain) == PLAINTEXT


def test_tls_alone_needs_no_credential_and_verifies_the_broker():
    kwargs = KafkaSecurity("SSL").client_kwargs()
    assert set(kwargs) == {"security_protocol", "ssl_context"}
    context = kwargs["ssl_context"]
    assert context.check_hostname and context.verify_mode == ssl.CERT_REQUIRED


def test_a_named_ca_is_loaded_into_the_context():
    context = KafkaSecurity("SSL", ca=CA).client_kwargs()["ssl_context"]
    assert context.cert_store_stats()["x509_ca"] == 1


@pytest.mark.parametrize(
    "settings, refused",
    [
        ({"protocol": "SASL_TLS"}, "Unknown Kafka security protocol"),
        ({"protocol": "SASL_SSL", "username": "u", "password": "p"}, "SASL mechanism"),
        (
            {"protocol": "SASL_SSL", "mechanism": "GSSAPI", "username": "u", "password": "p"},
            "SASL mechanism",
        ),
        ({"protocol": "SASL_SSL", "mechanism": "PLAIN", "password": "p"}, "username and a"),
        ({"protocol": "SASL_SSL", "mechanism": "PLAIN", "username": "u"}, "username and a"),
        ({"protocol": "SSL", "mechanism": "PLAIN"}, "carries no SASL"),
        ({"protocol": "PLAINTEXT", "ca": CA}, "has no TLS"),
        ({"protocol": "SASL_PLAINTEXT", "mechanism": "PLAIN"}, "username and a"),
    ],
)
def test_settings_that_disagree_are_refused_by_name(settings, refused):
    with pytest.raises(ValueError, match=refused):
        KafkaSecurity(**settings)


def test_a_source_that_disagrees_is_refused_when_saved():
    with pytest.raises(ValueError, match="username and a"):
        _source(password="")


def test_a_reference_is_held_as_entered_and_resolved_at_use(monkeypatch):
    monkeypatch.setenv("KAFKA_PW", _PASSWORD)
    source = _source(password="${env:KAFKA_PW}")
    assert source.password == "${env:KAFKA_PW}"
    assert KafkaSecurity.of_source(source).password == _PASSWORD


def test_an_unresolvable_reference_is_refused_and_never_used_as_written():
    source = _source(password="${env:KAFKA_PW_NOT_SET}")
    with pytest.raises(KeyError):
        KafkaSecurity.of_source(source)


def test_the_password_is_in_no_repr():
    conn = KafkaConnection("broker:9093", _secured())
    assert _PASSWORD not in repr(conn)
    assert _PASSWORD not in repr(conn.security)


# ------ the Java client's properties ---------------------------------------------------------------


def _java_properties(text: str) -> dict[str, str]:
    """The file as java.util.Properties reads it: a line's escapes are undone."""
    read: dict[str, str] = {}
    for line in text.splitlines():
        key, _, value = line.partition("=")
        read[key] = re.sub(r"\\(.)", lambda m: "\n" if m[1] == "n" else m[1], value)
    return read


@pytest.mark.parametrize(
    "mechanism, module",
    [
        ("PLAIN", "org.apache.kafka.common.security.plain.PlainLoginModule"),
        ("SCRAM-SHA-256", "org.apache.kafka.common.security.scram.ScramLoginModule"),
        ("SCRAM-SHA-512", "org.apache.kafka.common.security.scram.ScramLoginModule"),
    ],
)
def test_client_properties_carry_the_protocol_mechanism_and_jaas(mechanism, module):
    read = _java_properties(_secured(mechanism).client_properties() or "")
    assert read["security.protocol"] == "SASL_SSL"
    assert read["sasl.mechanism"] == mechanism
    assert read["sasl.jaas.config"] == (
        f'{module} required username="api-key" password="{_PASSWORD}";'
    )


def test_a_quote_or_backslash_in_a_password_stays_in_the_password():
    security = KafkaSecurity("SASL_SSL", "PLAIN", "u", 'p"w\\d')
    jaas = _java_properties(security.client_properties() or "")["sasl.jaas.config"]
    assert jaas.endswith('username="u" password="p\\"w\\\\d";')


def test_the_ca_travels_as_text_on_one_line_of_the_file():
    text = KafkaSecurity("SSL", ca=CA).client_properties() or ""
    assert len(text.splitlines()) == 3
    read = _java_properties(text)
    assert read["ssl.truststore.type"] == "PEM"
    assert read["ssl.truststore.certificates"] == CA.strip()


# ------ read and landing ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_read_opens_with_the_settings(consumers):
    await fetch_rows(KafkaConnection("broker:9093", _secured()), "t", ["id"])
    _assert_secured(consumers[0])


@pytest.mark.asyncio
async def test_a_read_of_a_plain_cluster_opens_as_it_did(consumers):
    await fetch_rows(KafkaConnection("broker:9092"), "t", ["id"])
    _assert_plain(consumers[0])


@pytest.mark.asyncio
async def test_a_landing_records_its_offsets_with_the_settings(consumers, monkeypatch):
    async def _drained(_consumer):
        return {"tp": 3}

    monkeypatch.setattr(landed_offsets, "drained_to", _drained)
    conn = KafkaConnection("broker:9093", _secured())
    assert [b async for b in iter_row_batches(conn, "t", ["id"], landed="group")] == []
    assert len(consumers) == 2
    for kwargs in consumers:
        _assert_secured(kwargs)


@pytest.mark.asyncio
async def test_the_loader_and_keyed_loader_read_with_the_sources_settings(consumers):
    from provisa.events.source_loader import make_kafka_keyed_loader, make_kafka_loader

    table = SimpleNamespace(table_name="t", columns=[SimpleNamespace(name="id")])
    await make_kafka_loader()(_source(), table)
    await make_kafka_keyed_loader()(_source(), table, ["id"], [(1,)])
    assert len(consumers) == 2
    for kwargs in consumers:
        _assert_secured(kwargs)


@pytest.mark.asyncio
async def test_a_replica_build_lands_with_the_sources_settings(consumers, bound_org):
    from provisa.events.source_loader import make_kafka_loader

    cursor = make_kafka_loader().replica_source(  # type: ignore[attr-defined]
        _source(), SimpleNamespace(table_name="t"), [("id", "bigint")]
    )
    assert [b async for b in cursor.batches(1000)] == []
    _assert_secured(consumers[0])


@pytest.mark.parametrize(
    "call",
    [
        lambda s: landed_offsets.end_offsets("b:1", "t", security=s),
        lambda s: landed_offsets.record("b:1", "g", {"tp": 1}, security=s),
        lambda s: landed_offsets.recorded("b:1", "g", "t", security=s),
        lambda s: landed_offsets.start_where_landed("b:1", "g", "t", "l", security=s),
    ],
    ids=["end_offsets", "record", "recorded", "start_where_landed"],
)
@pytest.mark.asyncio
async def test_every_landed_offset_call_opens_with_the_settings(consumers, call):
    await call(_secured())
    assert consumers
    for kwargs in consumers:
        _assert_secured(kwargs)


@pytest.mark.asyncio
async def test_discovery_samples_with_the_settings(consumers):
    from provisa.kafka.source import sample_topic_records

    await sample_topic_records("b:1", "t", security=_secured())
    _assert_secured(consumers[0])


# ------ the push listener and a subscription -------------------------------------------------------


@pytest.fixture
def bound_org() -> Iterator[None]:
    token = set_current_org("acme")
    yield
    reset_current_org(token)


def _listener(source: Source):
    from provisa.events.push_wiring import _build_provider

    built = _build_provider(source, {"live": {"kafka": {"topic": "t"}}}, node="k/s.t")
    assert built is not None
    return built


def test_the_push_listener_holds_the_sources_settings(bound_org):
    provider, topic = _listener(_source())
    assert topic == "t"
    assert provider._security == KafkaSecurity.of_source(_source())  # noqa: SLF001


def test_a_plain_sources_listener_holds_no_settings(bound_org):
    provider, _ = _listener(Source(id="k", type=SourceType.kafka, host="broker", port=9092))
    assert provider._security == PLAINTEXT  # noqa: SLF001


@pytest.mark.asyncio
async def test_the_listener_opens_and_resumes_with_the_settings(consumers, monkeypatch):
    from provisa.subscriptions.kafka_provider import KafkaNotificationProvider

    class _Iterating(_Consumer):
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise StopAsyncIteration

    import aiokafka

    monkeypatch.setattr(aiokafka, "AIOKafkaConsumer", _Iterating)
    provider = KafkaNotificationProvider("b:1", landed="landing-group", security=_secured())
    assert [e async for e in provider.watch("t")] == []
    assert len(consumers) >= 3, "the landed offsets and the listener's own consumer"
    for kwargs in consumers:
        _assert_secured(kwargs)


@pytest.mark.asyncio
async def test_a_declared_sources_subscription_opens_with_its_settings(monkeypatch):
    from provisa.api.data.subscribe import _kafka_security
    from contextlib import asynccontextmanager

    from provisa.core import secrets_store

    @asynccontextmanager
    async def _bound():
        yield

    monkeypatch.setattr(secrets_store, "bound_to_request_org", _bound)
    declared = {
        "id": "k",
        "security_protocol": "SASL_SSL",
        "auth": {"type": "sasl_scram_512", "username": "u", "password": "p"},
    }
    state = SimpleNamespace(kafka_declared={"k": declared})
    security = await _kafka_security(state, "k")
    assert (security.protocol, security.mechanism) == ("SASL_SSL", "SCRAM-SHA-512")


@pytest.mark.asyncio
async def test_a_plain_declared_source_asks_nothing_of_the_vault(monkeypatch):
    from provisa.api.data.subscribe import _kafka_security
    from provisa.core import secrets_store

    monkeypatch.setattr(secrets_store, "bound_to_request_org", None)
    state = SimpleNamespace(kafka_declared={"k": {"id": "k"}})
    assert await _kafka_security(state, "k") == PLAINTEXT


# ------ the Trino catalog --------------------------------------------------------------------------


class _Cursor:
    def __init__(self, sql: list[str]) -> None:
        self.sql = sql

    def execute(self, sql: str) -> None:
        self.sql.append(sql)

    def fetchall(self) -> list:
        return []


@pytest.fixture
def engine_files(monkeypatch, tmp_path) -> Path:
    """The catalog is created on a fake coordinator; the client file lands in tmp_path."""
    from provisa.core import engine_shard, trino_catalog_files

    monkeypatch.setattr(engine_shard, "bound_org_shard", lambda: None)
    monkeypatch.setattr(trino_catalog_files, "trino_etc_dir", lambda: tmp_path)
    monkeypatch.setattr(trino_catalog_files, "write_engine_secret_file", _plain_write)
    return tmp_path


def _plain_write(path: Path, data: bytes, *_a, **_k) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _create(source: Source) -> str:
    from provisa.core.catalog import create_catalog

    sql: list[str] = []
    conn = SimpleNamespace(cursor=lambda: _Cursor(sql))
    create_catalog(conn, source, "", catalog_name="org_k")  # type: ignore[arg-type]
    return next(s for s in sql if s.startswith("CREATE CATALOG"))


def test_a_secured_sources_catalog_names_its_client_file_and_the_file_holds_the_settings(
    engine_files,
):
    create = _create(_source(database="http://registry:8081"))
    assert "\"kafka.config.resources\" = '/etc/trino/kafka-client/org_k.properties'" in create
    assert _PASSWORD not in create, "a password never reaches the catalog's statement"
    read = _java_properties((engine_files / "kafka-client/org_k.properties").read_text())
    assert read["security.protocol"] == "SASL_SSL"
    assert read["sasl.mechanism"] == "SCRAM-SHA-256"
    assert _PASSWORD in read["sasl.jaas.config"]
    assert read["ssl.truststore.certificates"] == CA.strip()


def test_a_plain_sources_catalog_is_as_it_was(engine_files):
    create = _create(
        Source(id="k", type=SourceType.kafka, host="broker", port=9092, database="http://r:8081")
    )
    assert "kafka.config.resources" not in create
    assert not (engine_files / "kafka-client").exists()


def test_on_an_engine_shard_the_client_goes_to_the_shards_secret(engine_files, monkeypatch):
    from provisa.core import catalog, engine_shard
    from provisa.federation import k8s_provisioner

    put: list[tuple] = []

    def _put(shard, catalog_name, text):
        put.append((shard, catalog_name, text))
        return 0.0

    monkeypatch.setattr(engine_shard, "bound_org_shard", lambda: "shard-1")
    monkeypatch.setattr(k8s_provisioner, "put_kafka_client_on_shard", _put)
    props, wait = catalog._with_kafka_client(_source(), {"kafka.nodes": "b"}, "org_k")  # noqa: SLF001
    assert props["kafka.config.resources"].endswith("org_k.properties") and wait == 0.0
    assert put[0][:2] == ("shard-1", "org_k") and "SASL_SSL" in put[0][2]
    assert not (engine_files / "kafka-client").exists()


_DECLARED = {
    "id": "k",
    "bootstrap_servers": "broker:9093",
    "security_protocol": "SASL_SSL",
    "ssl_ca": CA,
    "auth": {"type": "sasl_scram_256", "username": "api-key", "password": _PASSWORD},
}


def test_a_declared_source_speaks_tls_when_it_says_so(engine_files):
    from provisa.core.trino_catalog_files import write_kafka_catalog_files

    write_kafka_catalog_files(_DECLARED, "org_k", etc_dir=engine_files)
    read = _java_properties((engine_files / "kafka-client/org_k.properties").read_text())
    assert read["security.protocol"] == "SASL_SSL"
    assert read["ssl.truststore.certificates"] == CA.strip()


def test_declared_auth_alone_speaks_sasl_over_no_tls_as_it_always_has():
    declared = {"auth": {"type": "sasl_plain", "username": "u", "password": "p"}}
    security = of_declared(declared)
    assert (security.protocol, security.mechanism) == ("SASL_PLAINTEXT", "PLAIN")


def test_a_declared_source_with_no_auth_is_plain():
    assert of_declared({}) == PLAINTEXT
    assert of_declared({"auth": {"type": "none"}}) == PLAINTEXT
    assert declared_settings({})["hints"] == {}


def test_a_declared_sources_row_carries_its_settings_to_the_landing_and_the_listener():
    from provisa.core.config_loader import kafka_topics_as_tables

    held = kafka_topics_as_tables({"kafka_sources": [_DECLARED]})["sources"][0]
    security = KafkaSecurity.of_source(Source(**held))
    assert security == of_declared(_DECLARED)


# ------ a SingleStore pipeline ---------------------------------------------------------------------


def _ddl(security: KafkaSecurity) -> str:
    from provisa.federation import singlestore_pipeline as sp

    return " ".join(
        sp.kafka_pipeline_ddl(
            schema="s",
            pipeline="p",
            procedure="q",
            bootstrap="broker:9093",
            origin=sp.LandOrigin("kafka", "t", "json"),
            table="orders",
            columns=[("id", "BIGINT")],
            pk_columns=["id"],
            security=security,
        )
    )


def test_a_pipeline_carries_the_settings_and_keeps_the_password_in_credentials():
    ddl = _ddl(KafkaSecurity("SASL_SSL", "SCRAM-SHA-512", "api-key", _PASSWORD))
    config = re.search(r"CONFIG '(.*?)' CREDENTIALS '(.*?)'", ddl)
    assert config is not None
    assert json.loads(config[1]) == {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "api-key",
    }
    assert json.loads(config[2]) == {"sasl.password": _PASSWORD}


def test_a_plain_pipeline_is_as_it_was():
    assert "CONFIG" not in _ddl(PLAINTEXT) and "CREDENTIALS" not in _ddl(PLAINTEXT)


def test_a_source_with_its_own_ca_keeps_the_relay():
    from provisa.federation import singlestore_pipeline as sp

    table = {"live": {"kafka": {"topic": "t"}}}
    assert not sp.lands_by_pipeline(sp.origin_of(_source(), table))
    public = {HINT_PROTOCOL: "SASL_SSL", HINT_MECHANISM: "PLAIN"}
    assert sp.lands_by_pipeline(sp.origin_of(_source(federation_hints=public), table))


def test_a_plain_tables_pipeline_keeps_its_name_and_a_secured_ones_digest_has_no_password():
    from provisa.federation import singlestore_pipeline as sp

    origin = sp.LandOrigin("kafka", "t", "json")
    args = (origin, [("id", "BIGINT")], ["id"], None, None, "b:1")
    assert sp.kafka_definition(*args, PLAINTEXT) == sp.kafka_definition(*args)
    assert sp.kafka_definition(*args, _secured()) != sp.kafka_definition(*args)
    rotated = KafkaSecurity("SASL_SSL", "SCRAM-SHA-256", "api-key", "other", CA)
    assert sp.kafka_definition(*args, _secured()) == sp.kafka_definition(*args, rotated)


def test_a_refused_pipeline_raises_without_the_password(monkeypatch):
    from provisa.federation import singlestore_kafka as sk
    from provisa.federation import singlestore_pipeline as sp

    class _Refused(Exception):
        pass

    def _fail(_conn, statements, **_k):
        if any("CREATE PIPELINE" in s for s in statements):
            raise _Refused(f"syntax error near {statements[-2]}")

    monkeypatch.setattr(sk, "_execute", _fail)
    monkeypatch.setattr(sk, "_running", lambda _c, _s: {})
    monkeypatch.setattr("provisa.federation.sqlalchemy_runtime._ensure_schema", lambda *_a: None)
    monkeypatch.setattr("provisa.federation.sqlalchemy_runtime._ensure_table", lambda *_a: None)
    engine = MagicMock()
    from sqlalchemy.dialects import mysql

    engine.dialect = mysql.dialect()
    with pytest.raises(RuntimeError) as refused:
        sk.ensure(
            engine,
            schema="s",
            table="orders",
            columns=[("id", "bigint")],
            pk_columns=["id"],
            origin=sp.LandOrigin("kafka", "t", "json"),
            bootstrap="b:1",
            landed={},
            security=_secured(),
        )
    assert _PASSWORD not in str(refused.value) and "***" in str(refused.value)
    assert refused.value.__cause__ is None and refused.value.__suppress_context__


# ------ no secret reaches a log --------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_path_logs_the_password(consumers, caplog, engine_files, bound_org):
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logging.getLogger().addHandler(handler)
    caplog.set_level(logging.DEBUG)
    try:
        await fetch_rows(KafkaConnection("b:1", _secured()), "t", ["id"])
        _listener(_source())
        _create(_source(database="http://registry:8081"))
    finally:
        logging.getLogger().removeHandler(handler)
    assert _PASSWORD not in caplog.text and _PASSWORD not in stream.getvalue()


def test_every_kafka_client_in_the_product_is_opened_through_the_settings():
    """A new consumer of a source's cluster that forgot the settings would reach a secured cluster
    plain: each place that opens one is named here, with why it needs none where it needs none."""
    found = subprocess.run(
        ["git", "grep", "-l", "-E", r"AIOKafka(Consumer|Producer|AdminClient)\(", "--", "provisa"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    assert sorted(found) == [
        "provisa/kafka/fetch.py",
        "provisa/kafka/landed_offsets.py",
        "provisa/kafka/producer.py",  # a topic output: names its own brokers, no source
        "provisa/kafka/source.py",
        "provisa/subscriptions/debezium_provider.py",  # a CDC transport: no source's settings
        "provisa/subscriptions/kafka_provider.py",
    ]
