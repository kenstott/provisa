# Copyright (c) 2026 Kenneth Stott
# Canary: 0e983729-98f8-4404-a511-1f4f3a5b0257
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Avro Debezium topics are read through the source's schema registry (REQ-1951), against a real
broker and real schema-registry containers this module starts.

The Avro path once imported a Kafka client the product does not declare; without it the topic was
read as JSON and the subscription died on its first message with a codec error. Here the product's
provider reads Avro-encoded Debezium inserts, updates and deletes from a registry that demands
basic auth, and from one that demands a client certificate; and each way of not being able to read
is refused by name.

Bearer-token registries are covered by unit tests only (tests/unit/test_avro_registry.py): the
registry image authenticates a bearer token only against an external identity provider."""

from __future__ import annotations

import asyncio
import datetime
import io
import json
import os
import struct
import subprocess
import time
import uuid

import fastavro
import httpx
import pytest

from provisa.kafka.avro_registry import RegistrySettings, SchemaRegistryRefusal
from provisa.subscriptions.debezium_provider import DebeziumNotificationProvider
from tests.itest_stack import ITEST_PROJECT
from tests.port_lease import lease_port

pytestmark = [
    pytest.mark.integration,
    pytest.mark.requires_kafka,
    pytest.mark.asyncio(loop_scope="session"),
]

_IMAGE = "confluentinc/cp-schema-registry:7.6.0"  # the stack's own registry image
_USER, _PASSWORD = "reader", "s3cret-registry"
_STORE_PASSWORD = "changeit"

_ENVELOPE = {
    "type": "record",
    "name": "Envelope",
    "namespace": "shop.app.orders",
    "fields": [
        {
            "name": "before",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "Value",
                    "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "region", "type": "string"},
                        {"name": "amount", "type": "double"},
                    ],
                },
            ],
            "default": None,
        },
        {"name": "after", "type": ["null", "Value"], "default": None},
        {"name": "op", "type": "string"},
        {"name": "ts_ms", "type": ["null", "long"], "default": None},
    ],
}


class _Registry:
    """A schema-registry container of this module's own, on the session stack's network (its
    schemas live in the session's broker, in a topic of its own)."""

    def __init__(self, tag: str, env: dict[str, str], files: dict[str, bytes], scheme: str):
        self.name = f"provisa-it-registry-{tag}-{uuid.uuid4().hex[:8]}"
        self.port = lease_port()
        self.url = f"{scheme}://localhost:{self.port}"
        self._env = env
        self._files = files
        self.certs: dict = {}  # the PEM files a client is pointed at (mutual TLS)
        self.client: dict = {}  # what an httpx client needs to reach this registry

    def start(self, tmp_dir, **client) -> None:
        env = {
            "SCHEMA_REGISTRY_HOST_NAME": self.name,
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "kafka:29092",
            "SCHEMA_REGISTRY_KAFKASTORE_TOPIC": f"_schemas_{self.name}",
            "SCHEMA_REGISTRY_SCHEMA_REGISTRY_GROUP_ID": self.name,
            "SCHEMA_REGISTRY_HEAP_OPTS": "-Xms128m -Xmx256m",
            **self._env,
        }
        args = ["docker", "create", "--memory", "768m", "--name", self.name]
        args += ["--network", f"{ITEST_PROJECT}_default", "-p", f"127.0.0.1:{self.port}:8081"]
        for key, value in env.items():
            args += ["-e", f"{key}={value}"]
        subprocess.run([*args, _IMAGE], check=True, capture_output=True)
        # The files go in before the container starts, so nothing depends on a host mount.
        conf = tmp_dir / "sr"
        conf.mkdir()
        conf.chmod(0o755)
        for name, content in self._files.items():
            (conf / name).write_bytes(content)
            (conf / name).chmod(0o644)
        subprocess.run(
            ["docker", "cp", str(conf), f"{self.name}:/etc/sr"], check=True, capture_output=True
        )
        subprocess.run(["docker", "start", self.name], check=True, capture_output=True)
        deadline = time.monotonic() + 180
        while True:
            try:
                httpx.get(f"{self.url}/subjects", timeout=5, **client).raise_for_status()
                return
            except httpx.HTTPError as exc:
                if time.monotonic() > deadline:
                    logs = subprocess.run(
                        ["docker", "logs", "--tail", "60", self.name],
                        capture_output=True,
                        text=True,
                    )
                    raise AssertionError(
                        f"registry {self.name} never answered: {exc}\n{logs.stdout}{logs.stderr}"
                    ) from exc
                time.sleep(2)

    def stop(self) -> None:
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


def _register(registry_url: str, topic: str, **client) -> int:
    """Register the envelope schema as the topic's value schema; the id the registry gave it."""
    response = httpx.post(
        f"{registry_url}/subjects/{topic}-value/versions",
        json={"schema": json.dumps(_ENVELOPE)},
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        timeout=30,
        **client,
    )
    response.raise_for_status()
    return response.json()["id"]


def _framed(schema_id: int, envelope: dict) -> bytes:
    out = io.BytesIO()
    out.write(struct.pack(">bI", 0, schema_id))
    fastavro.schemaless_writer(out, fastavro.parse_schema(_ENVELOPE), envelope)
    return out.getvalue()


async def _create_topic(topic: str) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic

    admin = AIOKafkaAdminClient(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"])
    await admin.start()
    try:
        await admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    finally:
        await admin.close()


async def _await_consumer_ready(provider: DebeziumNotificationProvider) -> None:
    """Until the provider's consumer holds its partition: it reads from the latest offset, so a
    message published before that is not its to read."""
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        consumer = getattr(provider, "_consumer", None)
        if consumer is not None and consumer.assignment():
            return
        await asyncio.sleep(0.25)
    raise AssertionError("the provider's consumer was assigned no partition within 60s")


async def _watch(provider: DebeziumNotificationProvider, messages: list[bytes], count: int):
    """What the provider reports for the messages published once its consumer is assigned."""
    from aiokafka import AIOKafkaProducer

    topic = provider._build_topic("orders")  # noqa: SLF001
    await _create_topic(topic)

    async def _collect() -> list:
        events: list = []
        async with asyncio.timeout(90):
            async for event in provider.watch("orders"):
                events.append(event)
                if len(events) == count:
                    break
        return events

    collecting = asyncio.create_task(_collect())
    try:
        ready = asyncio.create_task(_await_consumer_ready(provider))
        await asyncio.wait({collecting, ready}, return_when=asyncio.FIRST_COMPLETED)
        if collecting.done():  # refused before a message was read
            ready.cancel()
            return collecting.result()
        await ready
        producer = AIOKafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"])
        await producer.start()
        try:
            for message in messages:
                await producer.send_and_wait(topic, message)
        finally:
            await producer.stop()
        return await collecting
    finally:
        collecting.cancel()
        await provider.close()


def _provider(registry: RegistrySettings | None) -> DebeziumNotificationProvider:
    return DebeziumNotificationProvider(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP"],
        topic_prefix=f"avro{uuid.uuid4().hex[:8]}",
        database="app",
        consumer_group_id=f"provisa-test-{uuid.uuid4().hex[:8]}",
        registry=registry,
        source_type="mysql",
    )


def _changes(schema_id: int) -> tuple[list[bytes], list[tuple[str, dict]]]:
    """An insert, an update and a delete of one order, as Debezium writes them in Avro, and the
    rows a subscriber is owed."""
    new = {"id": 7, "region": "north", "amount": 12.5}
    moved = {"id": 7, "region": "south", "amount": 12.5}
    messages = [
        _framed(schema_id, {"before": None, "after": new, "op": "c", "ts_ms": 1790000000000}),
        _framed(schema_id, {"before": new, "after": moved, "op": "u", "ts_ms": 1790000001000}),
        _framed(schema_id, {"before": moved, "after": None, "op": "d", "ts_ms": 1790000002000}),
    ]
    return messages, [("insert", new), ("update", moved), ("delete", moved)]


# --- a registry that demands basic auth ---------------------------------------------------------


@pytest.fixture(scope="module")
def basic_registry(tmp_path_factory):
    jaas = (
        "SchemaRegistry {\n"
        "  org.eclipse.jetty.jaas.spi.PropertyFileLoginModule required\n"
        '  file="/etc/sr/passwords" debug="false";\n'
        "};\n"
    )
    registry = _Registry(
        "basic",
        {
            "SCHEMA_REGISTRY_AUTHENTICATION_METHOD": "BASIC",
            "SCHEMA_REGISTRY_AUTHENTICATION_ROLES": "reader",
            "SCHEMA_REGISTRY_AUTHENTICATION_REALM": "SchemaRegistry",
            "SCHEMA_REGISTRY_OPTS": "-Djava.security.auth.login.config=/etc/sr/jaas.conf",
        },
        {"jaas.conf": jaas.encode(), "passwords": f"{_USER}: {_PASSWORD},reader\n".encode()},
        "http",
    )
    try:
        registry.start(tmp_path_factory.mktemp("basic"), auth=(_USER, _PASSWORD))
        yield registry
    finally:
        registry.stop()


def _basic(registry: _Registry, monkeypatch, password: str = _PASSWORD) -> RegistrySettings:
    # The password is a reference, as a source holds it; resolved when the registry is called.
    monkeypatch.setenv("IT_AVRO_REGISTRY_PASSWORD", password)
    return RegistrySettings(
        url=registry.url,
        auth="basic",
        username=_USER,
        password="${env:IT_AVRO_REGISTRY_PASSWORD}",
    )


async def test_avro_changes_arrive_as_rows_through_a_basic_auth_registry(
    basic_registry, monkeypatch
):
    provider = _provider(_basic(basic_registry, monkeypatch))
    schema_id = _register(
        basic_registry.url,
        provider._build_topic("orders"),
        auth=(_USER, _PASSWORD),  # noqa: SLF001
    )
    messages, owed = _changes(schema_id)

    events = await _watch(provider, messages, count=3)

    assert [(e.operation, e.row) for e in events] == owed
    assert [e.timestamp for e in events] == [
        datetime.datetime.fromtimestamp(1790000000 + n, tz=datetime.timezone.utc) for n in range(3)
    ]


async def test_a_wrong_password_is_refused_by_name_before_any_message(basic_registry, monkeypatch):
    provider = _provider(_basic(basic_registry, monkeypatch, password="not-the-password"))
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await _watch(provider, [], count=1)
    assert refused.value.code == "subscribe.schema_registry_refused_credentials"
    assert refused.value.params == {
        "registry": basic_registry.url,
        "http_status": 401,
        "method": "basic",
    }


async def test_no_credentials_for_a_registry_that_demands_them_is_refused_by_name(basic_registry):
    provider = _provider(RegistrySettings(url=basic_registry.url))
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await _watch(provider, [], count=1)
    assert refused.value.code == "subscribe.schema_registry_refused_credentials"
    assert refused.value.params["method"] == "none"


async def test_an_id_the_registry_does_not_hold_is_refused_with_the_id(basic_registry, monkeypatch):
    provider = _provider(_basic(basic_registry, monkeypatch))
    new = {"id": 1, "region": "east", "amount": 1.0}
    foreign = _framed(987654, {"before": None, "after": new, "op": "c", "ts_ms": None})
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await _watch(provider, [foreign], count=1)
    assert refused.value.code == "subscribe.schema_id_unknown"
    assert refused.value.params == {"registry": basic_registry.url, "schema_id": 987654}


async def test_an_avro_topic_on_a_source_with_no_registry_is_refused_by_name(basic_registry):
    """Never read as JSON: the topic is named, and what to set."""
    provider = _provider(None)
    topic = provider._build_topic("orders")  # noqa: SLF001
    schema_id = _register(basic_registry.url, topic, auth=(_USER, _PASSWORD))
    messages, _owed = _changes(schema_id)
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await _watch(provider, messages, count=1)
    assert refused.value.code == "subscribe.avro_topic_without_registry"
    assert refused.value.params == {"topic": topic}


async def test_a_registry_that_is_not_there_is_refused_by_name_within_its_bounds():
    from provisa.kafka import avro_registry

    gone = f"http://localhost:{lease_port()}"
    provider = _provider(RegistrySettings(url=gone))
    began = time.monotonic()
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await _watch(provider, [], count=1)
    assert refused.value.code == "subscribe.schema_registry_unreachable"
    assert refused.value.params["registry"] == gone
    assert time.monotonic() - began < avro_registry.CONNECT_SECONDS + 5


# --- a registry that demands a client certificate -----------------------------------------------


def _certificates(tmp_dir) -> dict:
    """An authority, a server certificate for localhost and a client certificate, with the
    stores the registry reads and the PEM files the product is pointed at."""
    import ipaddress

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives.serialization import pkcs12
    from cryptography.x509.oid import NameOID

    now = datetime.datetime.now(datetime.timezone.utc)

    def _name(common: str) -> x509.Name:
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common)])

    def _issue(common: str, issuer_name, issuer_key, key, *, ca: bool, sans=()):
        builder = (
            x509.CertificateBuilder()
            .subject_name(_name(common))
            .issuer_name(issuer_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - datetime.timedelta(minutes=5))
            .not_valid_after(now + datetime.timedelta(days=2))
            .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        )
        if sans:
            builder = builder.add_extension(x509.SubjectAlternativeName(list(sans)), critical=False)
        return builder.sign(issuer_key, hashes.SHA256())

    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_cert = _issue("provisa-it-ca", _name("provisa-it-ca"), ca_key, ca_key, ca=True)
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_cert = _issue(
        "localhost",
        ca_cert.subject,
        ca_key,
        server_key,
        ca=False,
        sans=(x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))),
    )
    client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client_cert = _issue("provisa", ca_cert.subject, ca_key, client_key, ca=False)

    password = serialization.BestAvailableEncryption(_STORE_PASSWORD.encode())
    pem = serialization.Encoding.PEM
    paths = {}
    for name, content in {
        "ca.pem": ca_cert.public_bytes(pem),
        "client.crt": client_cert.public_bytes(pem),
        "client.key": client_key.private_bytes(
            pem, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ),
    }.items():
        (tmp_dir / name).write_bytes(content)
        paths[name] = str(tmp_dir / name)
    return {
        **paths,
        "server.p12": pkcs12.serialize_key_and_certificates(
            b"server", server_key, server_cert, [ca_cert], password
        ),
        "trust.p12": pkcs12.serialize_java_truststore(
            [pkcs12.PKCS12Certificate(ca_cert, b"ca")], password
        ),
    }


@pytest.fixture(scope="module")
def mtls_registry(tmp_path_factory):
    made = _certificates(tmp_path_factory.mktemp("certs"))
    registry = _Registry(
        "mtls",
        {
            "SCHEMA_REGISTRY_LISTENERS": "https://0.0.0.0:8081",
            "SCHEMA_REGISTRY_INTER_INSTANCE_PROTOCOL": "https",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_LOCATION": "/etc/sr/server.p12",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_TYPE": "PKCS12",
            "SCHEMA_REGISTRY_SSL_KEYSTORE_PASSWORD": _STORE_PASSWORD,
            "SCHEMA_REGISTRY_SSL_KEY_PASSWORD": _STORE_PASSWORD,
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_LOCATION": "/etc/sr/trust.p12",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_TYPE": "PKCS12",
            "SCHEMA_REGISTRY_SSL_TRUSTSTORE_PASSWORD": _STORE_PASSWORD,
            "SCHEMA_REGISTRY_SSL_CLIENT_AUTHENTICATION": "REQUIRED",
        },
        {"server.p12": made["server.p12"], "trust.p12": made["trust.p12"]},
        "https",
    )
    registry.certs = made
    registry.client = {
        "verify": made["ca.pem"],
        "cert": (made["client.crt"], made["client.key"]),
    }
    try:
        registry.start(tmp_path_factory.mktemp("mtls"), **registry.client)
        yield registry
    finally:
        registry.stop()


async def test_avro_changes_arrive_as_rows_through_a_mutual_tls_registry(mtls_registry):
    certs = mtls_registry.certs
    provider = _provider(
        RegistrySettings(
            url=mtls_registry.url,
            auth="mtls",
            client_cert=certs["client.crt"],
            client_key=certs["client.key"],
            ca=certs["ca.pem"],
        )
    )
    schema_id = _register(
        mtls_registry.url,
        provider._build_topic("orders"),
        **mtls_registry.client,  # noqa: SLF001
    )
    messages, owed = _changes(schema_id)

    events = await _watch(provider, messages, count=3)

    assert [(e.operation, e.row) for e in events] == owed


async def test_a_registry_that_demands_a_certificate_refuses_a_source_that_presents_none(
    mtls_registry,
):
    provider = _provider(RegistrySettings(url=mtls_registry.url, ca=mtls_registry.certs["ca.pem"]))
    with pytest.raises(SchemaRegistryRefusal) as refused:
        await _watch(provider, [], count=1)
    assert refused.value.code == "subscribe.schema_registry_unreachable"
    assert refused.value.params["registry"] == mtls_registry.url
