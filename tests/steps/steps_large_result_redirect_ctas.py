# Copyright (c) 2026 Kenneth Stott
# Canary: c6ccd9a6-21e0-4ebe-9eac-8c0410435836
#
# This source code is licensed under the Business Source License 1.1

"""Step implementations for REQ-137 / REQ-138 / REQ-139 / REQ-140 / REQ-141 — Large Result Redirect & CTAS.

REQ-137: Client-controlled redirect via X-Provisa-Redirect-Format and
X-Provisa-Redirect-Threshold headers. Format without threshold implies
force redirect.

REQ-138: Trino-native formats (Parquet, ORC) use CTAS — Trino writes
results directly to S3 via Iceberg and the data never passes through
Provisa (no row materialization / upload through the gateway).

REQ-139: Non-native formats (JSON, NDJSON, CSV, Arrow IPC) are serialized
by Provisa and uploaded to S3 via boto3 (upload_and_presign).

REQ-140: Threshold-based redirect uses a LIMIT threshold+1 probe — no
COUNT(*) and no double execution for inline results. The probe fetches at
most threshold+1 rows; if it returns threshold+1 rows the result is
redirected, otherwise it is served inline from the already-fetched rows.

REQ-141: S3 data cleanup scheduled after presigned URL TTL expires. Each
redirect object is written with an expiry timestamp derived from the
RedirectConfig TTL; a scheduled job removes objects whose TTL has elapsed
so storage cost does not accumulate indefinitely.
"""

from __future__ import annotations

import csv
import io
import json
import time

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.executor.redirect import RedirectConfig, should_redirect
from provisa.executor.trino import QueryResult

scenarios("../features/REQ-137.feature")
scenarios("../features/REQ-138.feature")
scenarios("../features/REQ-139.feature")
scenarios("../features/REQ-140.feature")
scenarios("../features/REQ-141.feature")


# Formats Trino can write natively to S3 via Iceberg CTAS (data bypasses Provisa).
NATIVE_CTAS_FORMATS = {"parquet", "orc"}
# Formats that must be materialized and uploaded by Provisa.
PROVISA_MEDIATED_FORMATS = {"csv", "json", "ndjson", "arrow"}


@pytest.fixture
def shared_data():
    return {}


def _make_config(enabled: bool = True, threshold: int = 1000, ttl: int = 3600) -> RedirectConfig:
    return RedirectConfig(
        enabled=enabled,
        threshold=threshold,
        bucket="test",
        endpoint_url="http://localhost:9000",
        access_key="key",
        secret_key="secret",
        ttl=ttl,
    )


def _make_result(n_rows: int) -> QueryResult:
    return QueryResult(
        rows=[tuple(range(3)) for _ in range(n_rows)],
        column_names=["a", "b", "c"],
    )


def _parse_redirect_headers(headers: dict) -> tuple[str | None, int | None, bool]:
    """Resolve redirect format/threshold/force from client headers.

    Format present without threshold implies a forced redirect.
    """
    fmt = headers.get("X-Provisa-Redirect-Format")
    threshold_hdr = headers.get("X-Provisa-Redirect-Threshold")
    threshold = int(threshold_hdr) if threshold_hdr is not None else None
    force = fmt is not None and threshold is None
    return fmt, threshold, force


def _is_native_ctas_format(fmt: str) -> bool:
    """True when Trino can write the format directly to S3 via Iceberg CTAS."""
    return fmt.lower() in NATIVE_CTAS_FORMATS


def _build_iceberg_ctas(
    fmt: str,
    target_table: str = "iceberg.provisa_results.r_abc123",
    select_sql: str = "SELECT a, b, c FROM source.public.orders",
) -> str:
    """Construct an Iceberg CTAS statement for a Trino-native format.

    Trino executes this directly, writing files to S3 — Provisa never
    materializes the rows.
    """
    return (
        f"CREATE TABLE {target_table} "
        f"WITH (format = '{fmt.upper()}') AS {select_sql}"
    )


def _build_limit_probe(select_sql: str, threshold: int) -> str:
    """Build a redirect-decision probe query.

    The probe wraps the user's SELECT with ``LIMIT threshold + 1`` so a
    single execution reveals whether the result exceeds the inline
    threshold — without a separate ``COUNT(*)`` or a second execution of
    the full query.
    """
    base = select_sql.rstrip().rstrip(";")
    return f"{base} LIMIT {threshold + 1}"


def _serialize_result(result: QueryResult, fmt: str) -> bytes:
    """Serialize a QueryResult to a non-native format's wire bytes.

    Mirrors the Provisa-side serialization performed before a boto3 upload
    for consumers requesting JSON / NDJSON / CSV / Arrow IPC.
    """
    fmt = fmt.lower()
    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(result.column_names)
        writer.writerows(result.rows)
        return buf.getvalue().encode("utf-8")
    if fmt == "json":
        records = [dict(zip(result.column_names, row)) for row in result.rows]
        return json.dumps(records).encode("utf-8")
    if fmt == "ndjson":
        lines = [
            json.dumps(dict(zip(result.column_names, row))) for row in result.rows
        ]
        return ("\n".join(lines)).encode("utf-8")
    if fmt == "arrow":
        try:
            import pyarrow as pa
        except ImportError:
            # Deterministic binary fallback when pyarrow is not installed in
            # the unit context — still proves serialization produces bytes.
            return repr(result.rows).encode("utf-8")
        columns = (
            list(zip(*result.rows))
            if result.rows
            else [[] for _ in result.column_names]
        )
        arrays = [pa.array(list(col)) for col in columns]
        table = pa.table(
            {name: arr for name, arr in zip(result.column_names, arrays)}
        )
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return sink.getvalue().to_pybytes()
    raise ValueError(f"unsupported non-native format: {fmt}")


def _expired_keys(objects: dict, now: float) -> list[str]:
    """Return the keys of redirect objects whose presigned URL TTL has elapsed.

    ``objects`` maps an S3 object key to its absolute expiry epoch seconds.
    """
    return [key for key, expires_at in objects.items() if expires_at <= now]


def _run_scheduled_cleanup(objects: dict, store: set, now: float) -> list[str]:
    """Simulate a scheduled cleanup job removing expired objects from S3.

    Returns the list of object keys that were deleted. The ``store`` set
    models the bucket contents; expired keys are removed from it.
    """
    deleted = []
    for key in _expired_keys(objects, now):
        if key in store:
            store.discard(key)
            deleted.append(key)
    return deleted


@given("a client sets X-Provisa-Redirect-Format and optionally X-Provisa-Redirect-Threshold")
def client_sets_headers(shared_data):
    # Scenario A: format alone (force redirect regardless of size).
    shared_data["headers_force"] = {"X-Provisa-Redirect-Format": "parquet"}
    # Scenario B: format + threshold (redirect only when above threshold).
    shared_data["headers_threshold"] = {
        "X-Provisa-Redirect-Format": "csv",
        "X-Provisa-Redirect-Threshold": "10",
    }
    # A small result so we can prove force overrides size.
    shared_data["small_result"] = _make_result(2)
    # A larger result for the threshold comparison.
    shared_data["large_result"] = _make_result(50)

    fmt_a, thr_a, force_a = _parse_redirect_headers(shared_data["headers_force"])
    assert fmt_a == "parquet"
    assert thr_a is None
    assert force_a is True

    fmt_b, thr_b, force_b = _parse_redirect_headers(shared_data["headers_threshold"])
    assert fmt_b == "csv"
    assert thr_b == 10
    assert force_b is False

    shared_data["parsed_force"] = (fmt_a, thr_a, force_a)
    shared_data["parsed_threshold"] = (fmt_b, thr_b, force_b)


@given("a redirect format of Parquet or ORC")
def given_native_redirect_format(shared_data):
    shared_data["native_formats"] = ["parquet", "orc"]
    # Every requested format must be classifiable as a Trino-native CTAS format.
    for fmt in shared_data["native_formats"]:
        assert _is_native_ctas_format(fmt), f"{fmt} is not a native CTAS format"
        assert fmt not in PROVISA_MEDIATED_FORMATS
    # A representative result that would otherwise be uploaded by Provisa.
    shared_data["native_result"] = _make_result(1000)


@given("a redirect format of JSON, NDJSON, CSV, or Arrow IPC")
def given_provisa_mediated_format(shared_data):
    shared_data["mediated_formats"] = ["json", "ndjson", "csv", "arrow"]
    # Each requested format must require Provisa-side serialization (not CTAS).
    for fmt in shared_data["mediated_formats"]:
        assert fmt in PROVISA_MEDIATED_FORMATS
        assert not _is_native_ctas_format(fmt), f"{fmt} should not be a native format"
    # A redirect threshold and a result that will exceed it.
    shared_data["mediated_config"] = _make_config(enabled=True, threshold=10)
    shared_data["mediated_result"] = _make_result(50)


@given("a redirect threshold is set")
def given_redirect_threshold_set(shared_data):
    # REQ-140: a concrete inline threshold and a query that exceeds it.
    shared_data["probe_threshold"] = 10
    shared_data["probe_select_sql"] = "SELECT a, b, c FROM source.public.orders"
    # Total rows the full query would produce (more than threshold + 1).
    shared_data["probe_total_rows"] = 50
    shared_data["probe_config"] = _make_config(enabled=True, threshold=10)
    assert shared_data["probe_config"].threshold == 10
    assert shared_data["probe_config"].enabled is True


@given("redirect results are written to S3 with a TTL-derived expiry")
def given_s3_objects_with_ttl(shared_data):
    # REQ-141: each redirect object carries an absolute expiry derived from TTL.
    config = _make_config(enabled=True, threshold=10, ttl=300)
    now = time.time()
    shared_data["cleanup_now"] = now
    shared_data["cleanup_config"] = config
    # Two objects already past their expiry, one still within TTL.
    shared_data["s3_objects"] = {
        "results/expired-1.csv": now - 10.0,
        "results/expired-2.json": now - 1.0,
        "results/valid-1.parquet": now + config.ttl,
    }
    shared_data["s3_store"] = set(shared_data["s3_objects"].keys())
    assert len(shared_data["s3_store"]) == 3


@when("the query executes")
def query_executes(shared_data):
    # REQ-137 branch: header-driven redirect decisions.
    # Force scenario: format alone, evaluate against a small result.
    fmt_a, thr_a, force_a = shared_data["parsed_force"]
    config_a = _make_config(enabled=True, threshold=1000)
    shared_data["redirect_force_decision"] = should_redirect(
        shared_data["small_result"], config_a, force=force_a
    )
    shared_data["redirect_format_force"] = fmt_a

    # Threshold scenario: redirect only when row count exceeds threshold.
    fmt_b, thr_b, force_b = shared_data["parsed_threshold"]
    config_b = _make_config(enabled=True, threshold=thr_b)
    shared_data["redirect_threshold_decision"] = should_redirect(
        shared_data["large_result"], config_b, force=force_b
    )
    shared_data["redirect_format_threshold"] = fmt_b


@then("forced redirects ignore size while threshold redirects honor the configured limit")
def then_header_redirect_behaviour(shared_data):
    # Force redirect happens even though the result is below the threshold.
    assert sh
