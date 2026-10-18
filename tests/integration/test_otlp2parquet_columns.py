# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Parquet the deployed otlp2parquet writes has the columns the telemetry compaction reads
(REQ-302, REQ-303).

Compaction (scheduler.jobs._compact_signal) copies each object's columns into the Iceberg
otel.signals tables by name, lowercased, and filters and converts traces by name. A column the
writer names otherwise is never copied: the Iceberg column stays empty with no error. This runs
the image docker-compose.observability.yml runs, with the config file it mounts, against a MinIO
it starts, sends it one span, one metric point and one log record over OTLP/HTTP, and reads the
schema of each object it writes.

Everything here is the ``test`` instance: one network and two containers this module starts and
removes."""

# Requirements: REQ-302, REQ-303, REQ-1349

from __future__ import annotations

import json
import logging
import os
import pathlib
import subprocess
import time

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration]

_REPO = pathlib.Path(__file__).resolve().parents[2]
_KEY, _SECRET, _BUCKET = "provisa-test", "provisa-test-secret", "provisa-otel"

#: The columns compaction reads by name, per signal (lowercased, as it compares them). traces:
#: the columns it filters on (_drop_foreign_rows), converts (_EPOCH_INT_INSTANTS) and extracts
#: the org and the request facts from; metrics and logs: the instants it converts, and the
#: attributes their org is read from.
_RELIED_ON = {
    "traces": {
        "trace_id",
        "span_id",
        "parent_span_id",
        "span_name",
        "service_name",
        "scope_name",
        "timestamp",
        "end_timestamp",
        "span_attributes",
    },
    "metrics": {"timestamp", "start_timestamp", "metric_name", "value", "metric_attributes"},
    "logs": {"timestamp", "observed_timestamp", "severity_text", "body", "log_attributes"},
}


def _service() -> dict:
    compose = yaml.safe_load((_REPO / "docker-compose.observability.yml").read_text())
    return compose["services"]["otlp2parquet"]


def _docker(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True)


@pytest.fixture(scope="module")
def writer():
    """otlp2parquet as the observability overlay runs it, writing to a MinIO of its own; yields
    its OTLP/HTTP base URL and an S3 client of the bucket."""
    import boto3
    from botocore.config import Config as BotoConfig

    from tests.port_lease import lease_ports

    service = _service()
    minio_port, otlp_port = lease_ports(2)
    tag = f"provisa-itest-o2p-{os.getpid()}"
    network, minio, o2p = f"{tag}-net", f"{tag}-minio", f"{tag}-writer"
    _docker("network", "create", network)
    try:
        _docker(
            "run", "-d", "--rm", "--memory", "512m", "--name", minio, "--network", network,
            # The writer's mounted config names its store minio:9000, as the compose stack does.
            "--network-alias", "minio",
            "-e", f"MINIO_ROOT_USER={_KEY}", "-e", f"MINIO_ROOT_PASSWORD={_SECRET}",
            # The image the core stack runs (docker-compose.core.yml).
            "-p", f"127.0.0.1:{minio_port}:9000", "bitnamilegacy/minio:latest",
            "server", "/bitnami/minio/data",
        )  # fmt: skip
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://127.0.0.1:{minio_port}",
            aws_access_key_id=_KEY,
            aws_secret_access_key=_SECRET,
            region_name="us-east-1",
            config=BotoConfig(signature_version="s3v4"),
        )
        deadline = time.monotonic() + 60
        while True:
            try:
                s3.create_bucket(Bucket=_BUCKET)
                break
            except Exception:  # MinIO is still starting
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        env = {
            "OTLP2PARQUET_STORAGE_BACKEND": "s3",
            "OTLP2PARQUET_S3_BUCKET": _BUCKET,
            "OTLP2PARQUET_S3_ENDPOINT": "http://minio:9000",
            "OTLP2PARQUET_S3_REGION": "us-east-1",
            "OTLP2PARQUET_HTTP_PORT": "4318",
            "AWS_ACCESS_KEY_ID": _KEY,
            "AWS_SECRET_ACCESS_KEY": _SECRET,
        }
        config = _REPO / "observability" / "otlp2parquet.toml"
        _docker(
            "run", "-d", "--rm", "--memory", "256m", "--name", o2p, "--network", network,
            "-v", f"{config}:/etc/otlp2parquet.toml:ro",
            *[arg for name, value in env.items() for arg in ("-e", f"{name}={value}")],
            "-p", f"127.0.0.1:{otlp_port}:4318",
            service["image"], *service["command"],
        )  # fmt: skip
        base = f"http://127.0.0.1:{otlp_port}"
        deadline = time.monotonic() + 60
        while True:
            try:
                if httpx.get(f"{base}/health", timeout=2).status_code == 200:
                    break
            except httpx.HTTPError:  # the writer is still starting
                pass
            if time.monotonic() > deadline:
                logs = subprocess.run(["docker", "logs", o2p], capture_output=True, text=True)
                raise AssertionError(f"otlp2parquet never answered /health:\n{logs.stderr}")
            time.sleep(1)
        yield base, s3
    finally:
        subprocess.run(["docker", "rm", "-f", o2p, minio], capture_output=True)
        subprocess.run(["docker", "network", "rm", network], capture_output=True)


def _send(base: str) -> None:
    """One span, one metric point and one log record of the org ``acme``, as Provisa sends them."""
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
    from opentelemetry.sdk._logs.export import SimpleLogRecordProcessor
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    resource = Resource.create({"service.name": "provisa"})
    org = {"provisa.tenant": "acme"}

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        SimpleSpanProcessor(OTLPSpanExporter(endpoint=f"{base}/v1/traces"))
    )
    with tracer_provider.get_tracer("provisa").start_as_current_span("work", attributes=org):
        pass
    tracer_provider.shutdown()

    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=f"{base}/v1/metrics"), export_interval_millis=3_600_000
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[reader])
    meter_provider.get_meter("provisa").create_counter("provisa.query.executed").add(1, org)
    meter_provider.shutdown()  # collects and exports once

    log_provider = LoggerProvider(resource=resource)
    log_provider.add_log_record_processor(
        SimpleLogRecordProcessor(OTLPLogExporter(endpoint=f"{base}/v1/logs"))
    )
    logger = logging.getLogger("provisa.itest.o2p")
    handler = LoggingHandler(level=logging.WARNING, logger_provider=log_provider)
    logger.addHandler(handler)
    try:
        logger.warning("refresh failed", extra=org)
    finally:
        logger.removeHandler(handler)
        log_provider.shutdown()


@pytest.fixture(scope="module")
def schemas(writer) -> dict[str, list]:
    """Each signal's written objects, read as Arrow tables, once the writer has flushed them."""
    import io

    import pyarrow.parquet as pq

    base, s3 = writer
    _send(base)
    found: dict[str, list] = {}
    deadline = time.monotonic() + 120  # the writer flushes a batch at its max_age_secs
    while set(found) != set(_RELIED_ON):
        for signal in _RELIED_ON:
            keys = [
                o["Key"]
                for o in s3.list_objects_v2(Bucket=_BUCKET, Prefix=f"{signal}/").get("Contents", [])
                if o["Key"].endswith(".parquet")
            ]
            if keys:
                found[signal] = [
                    pq.read_table(io.BytesIO(s3.get_object(Bucket=_BUCKET, Key=k)["Body"].read()))
                    for k in keys
                ]
        if time.monotonic() > deadline:
            raise AssertionError(f"objects written only for {sorted(found)}")
        time.sleep(2)
    return found


@pytest.mark.parametrize("signal", sorted(_RELIED_ON))
def test_the_writer_names_the_columns_compaction_reads(schemas, signal):
    for table in schemas[signal]:
        written = {name.lower() for name in table.schema.names}
        missing = _RELIED_ON[signal] - written
        assert not missing, f"{signal}: no {sorted(missing)} among {table.schema.names}"


@pytest.mark.parametrize(
    ("signal", "column"),
    [("traces", "span_attributes"), ("metrics", "metric_attributes"), ("logs", "log_attributes")],
)
def test_a_records_org_is_in_its_attributes(schemas, signal, column):
    for table in schemas[signal]:
        names = {name.lower(): name for name in table.schema.names}
        assert column in names, f"{signal}: no {column} among {table.schema.names}"
        values = [json.loads(v) for v in table.column(names[column]).to_pylist() if v]
        assert any(v.get("provisa.tenant") == "acme" for v in values), values
