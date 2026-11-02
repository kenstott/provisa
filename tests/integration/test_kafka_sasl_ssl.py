# Copyright (c) 2026 Kenneth Stott
# Canary: 0456b04b-31e4-4240-b73e-1d7558af30d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-147: every client the product opens reaches a SASL_SSL broker, by each mechanism, and a client
with the wrong password or the wrong CA does not.

Everything here is the ``test`` instance: one broker container this module starts and removes, with
a private CA and server certificate generated here. Its Java client reads the file Trino would, so
the client properties are proved against the client they are written for."""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from provisa.kafka import landed_offsets
from provisa.kafka.fetch import KafkaConnection, fetch_rows
from provisa.kafka.security import KafkaSecurity
from provisa.kafka.source import sample_topic_records
from provisa.subscriptions.kafka_provider import KafkaNotificationProvider

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_USER, _PASSWORD = "provisa-app", "pa55-w0rd"
_MECHANISMS = ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
_PLAIN_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule"
_SCRAM_MODULE = "org.apache.kafka.common.security.scram.ScramLoginModule"


@dataclass(frozen=True)
class _Broker:
    name: str
    port: int
    ca: str
    certs: Path

    def security(self, mechanism: str, *, password: str = _PASSWORD) -> KafkaSecurity:
        return KafkaSecurity("SASL_SSL", mechanism, _USER, password, self.ca)

    def connection(self, security: KafkaSecurity) -> KafkaConnection:
        return KafkaConnection.build("localhost", self.port, security)


def _pem(key: ec.EllipticCurvePrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _name(common_name: str) -> x509.Name:
    return x509.Name([x509.NameAttribute(x509.NameOID.COMMON_NAME, common_name)])


def _cert(
    subject: str, key, issuer: str, signer, *, ca: bool, san: list[str] | None = None
) -> x509.Certificate:
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(_name(subject))
        .issuer_name(_name(issuer))
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
    )
    if san:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName(d) for d in san]), critical=False
        )
    return builder.sign(signer, hashes.SHA256())


def _private_ca() -> tuple[ec.EllipticCurvePrivateKey, x509.Certificate]:
    key = ec.generate_private_key(ec.SECP256R1())
    return key, _cert("provisa-test-ca", key, "provisa-test-ca", key, ca=True)


def _write_certs(certs: Path) -> str:
    """The broker's key and chain, one PEM file as its keystore; returns the CA's certificate."""
    ca_key, ca = _private_ca()
    key = ec.generate_private_key(ec.SECP256R1())
    server = _cert("localhost", key, "provisa-test-ca", ca_key, ca=False, san=["localhost"])
    ca_pem = ca.public_bytes(serialization.Encoding.PEM)
    keystore = certs / "broker.pem"
    keystore.write_bytes(_pem(key) + server.public_bytes(serialization.Encoding.PEM) + ca_pem)
    keystore.chmod(0o644)
    certs.chmod(0o755)
    return ca_pem.decode()


def _broker_env(port: int) -> dict[str, str]:
    """A one-node KRaft broker: plaintext for its own tools, SASL_SSL for everything under test.
    In the image's environment names ``___`` stands for a dash and ``_`` for a dot."""
    users = f'user_{_USER}="{_PASSWORD}"'
    scram = f'{_SCRAM_MODULE} required username="admin" password="unused";'
    return {
        "KAFKA_NODE_ID": "1",
        "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9091",
        "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_LISTENERS": f"CONTROLLER://:9091,INTERNAL://:9092,SECURED://:{port}",
        "KAFKA_ADVERTISED_LISTENERS": f"INTERNAL://localhost:9092,SECURED://localhost:{port}",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
            "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,SECURED:SASL_SSL"
        ),
        "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL",
        "KAFKA_SASL_ENABLED_MECHANISMS": ",".join(_MECHANISMS),
        "KAFKA_LISTENER_NAME_SECURED_PLAIN_SASL_JAAS_CONFIG": (
            f"{_PLAIN_MODULE} required {users};"
        ),
        "KAFKA_LISTENER_NAME_SECURED_SCRAM___SHA___256_SASL_JAAS_CONFIG": scram,
        "KAFKA_LISTENER_NAME_SECURED_SCRAM___SHA___512_SASL_JAAS_CONFIG": scram,
        "KAFKA_SSL_KEYSTORE_TYPE": "PEM",
        "KAFKA_SSL_KEYSTORE_LOCATION": "/certs/broker.pem",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
        "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_HEAP_OPTS": "-Xmx256m -Xms256m",
    }


def _tool(name: str, *args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "exec", name, *args], capture_output=True, text=True, timeout=timeout
    )


@pytest.fixture(scope="module")
def broker(tmp_path_factory) -> Iterator[_Broker]:
    from tests.port_lease import lease_ports

    (port,) = lease_ports(1)
    name = f"provisa-itest-kafka-sasl-{os.getpid()}"
    certs = tmp_path_factory.mktemp("kafka-certs")
    ca = _write_certs(certs)
    env = [item for k, v in _broker_env(port).items() for item in ("-e", f"{k}={v}")]
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--memory", "512m", "--name", name]
        + ["-p", f"127.0.0.1:{port}:{port}", "-v", f"{certs}:/certs"]
        + env
        + ["apache/kafka:3.9.0"],
        check=True,
        capture_output=True,
    )
    try:
        deadline = time.monotonic() + 120
        while True:
            added = _tool(
                name,
                "/opt/kafka/bin/kafka-configs.sh",
                "--bootstrap-server",
                "localhost:9092",
                "--alter",
                "--entity-type",
                "users",
                "--entity-name",
                _USER,
                "--add-config",
                f"SCRAM-SHA-256=[password={_PASSWORD}],SCRAM-SHA-512=[password={_PASSWORD}]",
            )
            if added.returncode == 0:
                break
            if time.monotonic() > deadline:  # the broker is still starting until then
                logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
                raise RuntimeError(f"{added.stderr}\n{logs.stdout[-2000:]}")
            time.sleep(2)
        yield _Broker(name, port, ca, certs)
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


async def _publish(broker: _Broker, security: KafkaSecurity, topic: str, *ids: int) -> None:
    from aiokafka import AIOKafkaProducer

    producer = AIOKafkaProducer(
        bootstrap_servers=f"localhost:{broker.port}", **security.client_kwargs()
    )
    await producer.start()
    try:
        for row_id in ids:
            await producer.send_and_wait(topic, json.dumps({"id": row_id}).encode())
    finally:
        await producer.stop()


def _topic() -> str:
    return f"secured-{uuid.uuid4().hex[:8]}"


@pytest.mark.parametrize("mechanism", _MECHANISMS)
async def test_a_read_reaches_the_broker_by_each_mechanism(broker, mechanism):
    security = broker.security(mechanism)
    topic = _topic()
    await _publish(broker, security, topic, 1, 2)
    assert await fetch_rows(broker.connection(security), topic, ["id"]) == [{"id": 1}, {"id": 2}]


async def test_discovery_samples_the_secured_topic(broker):
    security = broker.security("SCRAM-SHA-256")
    topic = _topic()
    await _publish(broker, security, topic, 7)
    records = await sample_topic_records(
        f"localhost:{broker.port}", topic, max_records=1, security=security
    )
    assert records == [{"id": 7}]


async def test_a_landing_records_its_offsets_and_the_listener_starts_there(broker):
    security = broker.security("SCRAM-SHA-512")
    bootstrap = f"localhost:{broker.port}"
    topic = _topic()
    landed, group = f"landed-{topic}", f"listener-{topic}"
    await _publish(broker, security, topic, 1, 2)
    stopped = await landed_offsets.end_offsets(bootstrap, topic, security=security)
    await landed_offsets.record(bootstrap, landed, stopped, security=security)
    assert await landed_offsets.recorded(bootstrap, landed, topic, security=security) == {0: 2}

    await _publish(broker, security, topic, 3)
    provider = KafkaNotificationProvider(bootstrap, group, landed=landed, security=security)
    stream = provider.watch(topic)
    first = await asyncio.wait_for(anext(stream), 30)
    await stream.aclose()
    assert first.row == {"id": 3}, "the listener starts where the landing stopped"


async def test_the_wrong_password_is_refused(broker):
    wrong = broker.security("SCRAM-SHA-256", password="not-the-password")
    with pytest.raises(Exception, match="(?i)auth"):
        await fetch_rows(broker.connection(wrong), _topic(), ["id"])


async def test_a_broker_the_named_ca_did_not_sign_is_refused(broker):
    _, other = _private_ca()
    pem = other.public_bytes(serialization.Encoding.PEM).decode()
    stranger = KafkaSecurity("SASL_SSL", "PLAIN", _USER, _PASSWORD, pem)
    with pytest.raises(Exception):
        await fetch_rows(broker.connection(stranger), _topic(), ["id"])


async def test_a_plain_client_does_not_reach_the_secured_listener(broker):
    with pytest.raises(Exception):
        await fetch_rows(KafkaConnection.build("localhost", broker.port), _topic(), ["id"])


@pytest.mark.parametrize("mechanism", _MECHANISMS)
async def test_the_file_trino_reads_opens_the_java_client(broker, mechanism):
    """Trino's connector is the Java client given this file (``kafka.config.resources``); the
    broker's own console consumer is that client."""
    security = broker.security(mechanism)
    topic = _topic()
    await _publish(broker, security, topic, 9)
    client_file = broker.certs / f"{mechanism}.properties"
    client_file.write_text(security.client_properties() or "", encoding="utf-8")
    client_file.chmod(0o644)
    consumed = await asyncio.to_thread(
        _tool,
        broker.name,
        "/opt/kafka/bin/kafka-console-consumer.sh",
        "--bootstrap-server",
        f"localhost:{broker.port}",
        "--consumer.config",
        f"/certs/{client_file.name}",
        "--topic",
        topic,
        "--from-beginning",
        "--max-messages",
        "1",
        "--timeout-ms",
        "30000",
    )
    assert '{"id": 9}' in consumed.stdout, consumed.stderr
