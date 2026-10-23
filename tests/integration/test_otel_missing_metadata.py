# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3e9a14-2b6d-4f81-a5c0-e8d4b1f6a293
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A telemetry table whose metadata cannot be read is said loudly and left as it is.

The ``otel`` Iceberg catalog keeps its tables' registrations in PostgreSQL and their metadata
and data in the telemetry bucket. A bucket emptied or replaced under a catalog that kept its rows
(a store recreated, a volume reset) leaves each table registered with no metadata to read: Trino
answers ICEBERG_MISSING_METADATA to every statement, ``CREATE TABLE IF NOT EXISTS`` finds the table
there and does nothing, and every compaction run failed ("could not evolve partition spec",
"failed inserting traces into Iceberg"). Most often the metadata is intact and only served from the
wrong place for a while (a disk not mounted yet), so nothing is remade: compaction names the table
and its unreadable metadata in an ERROR, skips that signal, keeps its files, and leaves the
registration for the operator's recovery."""

from __future__ import annotations

import contextlib
import os
import re
import uuid
from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow as pa
import pytest
import trino

from tests.integration.isolated_server import IsolatedServer

pytestmark = [pytest.mark.integration, pytest.mark.requires_trino]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
_ORG = f"otelmeta{uuid.uuid4().hex[:6]}"


class _Engine:
    """The engine terminal compaction writes through, as a client of the test's own."""

    dialect = "trino"

    def execute_engine_sync(self, sql, params=None, *, authorization=None, session_hints=None):
        cur = trino.dbapi.connect(host=_TRINO_HOST, port=_TRINO_PORT, user="itest").cursor()
        cur.execute(sql, params) if params else cur.execute(sql)
        return SimpleNamespace(rows=cur.fetchall())


@pytest.fixture(scope="module")
def booted():
    """A Trino-engine server's boot issues the ``otel`` catalog and creates its tables."""
    srv = IsolatedServer(_ORG, engine="trino")
    try:
        srv.start(timeout=600)
        yield srv
    finally:
        srv.stop_process()


def _s3():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["PROVISA_OTEL_S3_ENDPOINT"],
        aws_access_key_id=os.environ["PROVISA_OTEL_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["PROVISA_OTEL_S3_SECRET_KEY"],
        region_name=os.environ.get("PROVISA_OTEL_S3_REGION", "us-east-1"),
    )


def _location(signal: str) -> tuple[str, str]:
    """``signal``'s table location in the bucket, as its bucket and key prefix."""
    [[ddl]] = _Engine().execute_engine_sync(f"SHOW CREATE TABLE otel.signals.{signal}").rows
    location = re.search(r"location = '(s3://[^']+)'", ddl)
    assert location, ddl
    bucket, prefix = location.group(1).removeprefix("s3://").split("/", 1)
    return bucket, prefix


def _keys(bucket: str, prefix: str) -> list[str]:
    return [
        obj["Key"]
        for page in _s3().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]


@contextlib.contextmanager
def _metadata_gone(signal: str):
    """Remove everything under ``signal``'s table location, as a bucket served empty would, and
    put it back after: the table is the server's own, which the module's other tests (and the
    next module's) read.

    The table is given a snapshot first, a row of its own trace: the catalog's metadata cache
    answers a read of a table with none without the bucket (ops_trino.metadata_files_gone), and
    whether an earlier test wrote one would decide what a read of it says."""
    trace_id = f"metadata-gone-{uuid.uuid4().hex[:8]}"
    _Engine().execute_engine_sync(
        f"INSERT INTO otel.signals.{signal} (trace_id, _date) VALUES ('{trace_id}', CURRENT_DATE)"
    )
    bucket, prefix = _location(signal)
    keys = _keys(bucket, prefix)
    assert keys, f"nothing under s3://{bucket}/{prefix}"
    s3 = _s3()
    kept = {k: s3.get_object(Bucket=bucket, Key=k)["Body"].read() for k in keys}
    s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]})
    try:
        yield
    finally:
        # The metadata served again, as from the right place once it is; the row taken back out.
        for k, body in kept.items():
            s3.put_object(Bucket=bucket, Key=k, Body=body)
        _Engine().execute_engine_sync(
            f"DELETE FROM otel.signals.{signal} WHERE trace_id = '{trace_id}'"
        )


def test_compaction_says_a_table_whose_metadata_is_unreadable_and_changes_nothing(booted, caplog):
    import io
    import logging

    import pyarrow.parquet as pq

    from provisa.core.trino_system_catalogs import otel_object_store
    from provisa.scheduler.jobs import _compact_signal

    with _metadata_gone("traces"):
        store = otel_object_store()
        today = datetime.now(UTC)
        key = (
            f"traces/provisa/year={today:%Y}/month={today:%m}/day={today:%d}/"
            f"waiting-{uuid.uuid4().hex[:8]}.parquet"
        )
        body = io.BytesIO()
        pq.write_table(
            pa.table(
                {"trace_id": ["t"], "span_name": ["provisa.query"], "service_name": ["provisa"]}
            ),
            body,
        )
        s3 = _s3()
        s3.put_object(Bucket=store["bucket"], Key=key, Body=body.getvalue())

        with caplog.at_level(logging.ERROR):
            _compact_signal(
                "traces",
                None,
                os.environ["PROVISA_OTEL_S3_ENDPOINT"],
                store["access_key"],
                store["secret_key"],
                store["bucket"],
                100,
                100,
                _Engine(),
            )

        said = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any(
            "otel.signals.traces is registered but its metadata cannot be read" in m for m in said
        ), said
        s3.head_object(Bucket=store["bucket"], Key=key)  # its file is kept for a later run
        # The registration is untouched: the table is not made again -- no metadata file is written
        # at its location -- so it still cannot be read.
        bucket, prefix = _location("traces")
        assert not [k for k in _keys(bucket, prefix) if k.endswith(".metadata.json")]
        with pytest.raises(trino.exceptions.TrinoQueryError, match="ICEBERG_MISSING_METADATA"):
            _Engine().execute_engine_sync("SELECT count(*) FROM otel.signals.traces")
        s3.delete_object(Bucket=store["bucket"], Key=key)
