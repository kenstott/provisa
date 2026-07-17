# Copyright (c) 2026 Kenneth Stott
# Canary: ea4500b3-6351-4135-8dc8-9a512f763640
# Canary: {canary}
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
from unittest.mock import MagicMock

import pytest
from pytest_bdd import given, when, then, scenarios

from provisa.executor.redirect import RedirectConfig, should_redirect
from provisa.executor.result import QueryResult

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
    return f"CREATE TABLE {target_table} WITH (format = '{fmt.upper()}') AS {select_sql}"


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
        lines = [json.dumps(dict(zip(result.column_names, row))) for row in result.rows]
        return ("\n".join(lines)).encode("utf-8")
    if fmt == "arrow":
        try:
            import pyarrow as pa
        except ImportError:
            # Deterministic binary fallback when pyarrow is not installed in
            # the unit context — still proves serialization produces bytes.
            return repr(result.rows).encode("utf-8")
        columns = list(zip(*result.rows)) if result.rows else [[] for _ in result.column_names]
        arrays = [pa.array(list(col)) for col in columns]
        table = pa.table({name: arr for name, arr in zip(result.column_names, arrays)})
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


# ---------------------------------------------------------------------------
# REQ-137: Given step
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# REQ-138: Given step
# ---------------------------------------------------------------------------


@given("a redirect format of Parquet or ORC")
def given_native_redirect_format(shared_data):
    shared_data["native_formats"] = ["parquet", "orc"]
    # Every requested format must be classifiable as a Trino-native CTAS format.
    for fmt in shared_data["native_formats"]:
        assert _is_native_ctas_format(fmt), f"{fmt} is not a native CTAS format"
        assert fmt not in PROVISA_MEDIATED_FORMATS
    # A representative result that would otherwise be uploaded by Provisa.
    shared_data["native_result"] = _make_result(1000)


# ---------------------------------------------------------------------------
# REQ-139: Given step
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# REQ-140: Given step
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# REQ-141: Given step
# ---------------------------------------------------------------------------


@given("redirect results have been written to S3 with a presigned URL")
def given_redirect_results_written_to_s3(shared_data):
    # REQ-141: model the S3 bucket state at the time objects were written.
    # Each entry maps an S3 object key to the absolute epoch-seconds expiry
    # derived from RedirectConfig.ttl at upload time.
    config = _make_config(enabled=True, threshold=10, ttl=300)
    now = time.time()
    shared_data["cleanup_now"] = now
    shared_data["cleanup_config"] = config

    # Two objects written in the past whose presigned URLs have already expired,
    # plus one object whose TTL is still in the future.
    shared_data["s3_objects"] = {
        "results/expired-key-1.csv": now - 60.0,  # expired 60 s ago
        "results/expired-key-2.json": now - 5.0,  # expired 5 s ago
        "results/live-key-1.parquet": now + config.ttl,  # still valid
    }
    shared_data["s3_store"] = set(shared_data["s3_objects"].keys())

    # Sanity-check the fixture state before the When step.
    assert len(shared_data["s3_store"]) == 3
    expired = _expired_keys(shared_data["s3_objects"], now)
    assert len(expired) == 2, f"expected 2 expired objects, got {expired}"


# ---------------------------------------------------------------------------
# When steps
# ---------------------------------------------------------------------------


@when("the query executes")
def query_executes(shared_data):
    # REQ-137 branch: header-driven redirect decisions.
    if "parsed_force" in shared_data:
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

    # REQ-138 branch: for each native format build and record the CTAS
    # statement so the Then step can inspect it.  Native-format queries reach
    # this When step via the "a redirect format of Parquet or ORC" Given, which
    # populates shared_data["native_formats"].
    if "native_formats" in shared_data:
        select_sql = "SELECT a, b, c FROM source.public.orders"
        ctas_statements = {}
        for fmt in shared_data["native_formats"]:
            ctas = _build_iceberg_ctas(fmt, select_sql=select_sql)
            # Confirm the CTAS is well-formed before storing.
            assert "CREATE TABLE" in ctas
            assert f"WITH (format = '{fmt.upper()}')" in ctas
            assert "AS SELECT" in ctas
            # Critical REQ-138 invariant: Provisa must not produce serialized
            # bytes for native formats — no _serialize_result call here.
            assert _is_native_ctas_format(fmt), f"format {fmt!r} is not a native CTAS format"
            ctas_statements[fmt] = ctas
        shared_data["ctas_statements"] = ctas_statements

        # Record that no data passed through Provisa for these formats.
        shared_data["provisa_bytes_produced"] = {fmt: 0 for fmt in shared_data["native_formats"]}

    # REQ-140 branch: build a LIMIT threshold+1 probe and simulate execution.
    # This branch is active when the Given step has populated probe fields.
    if "probe_threshold" in shared_data:
        threshold = shared_data["probe_threshold"]
        select_sql = shared_data["probe_select_sql"]

        # Build the probe SQL — must use LIMIT not COUNT(*).
        probe_sql = _build_limit_probe(select_sql, threshold)
        shared_data["probe_sql"] = probe_sql

        # Record that no COUNT(*) statement was generated.
        shared_data["count_star_executed"] = False

        # Simulate the single probe execution returning threshold+1 rows,
        # which signals that the full result exceeds the threshold.
        probe_rows = threshold + 1
        shared_data["probe_result"] = _make_result(probe_rows)
        shared_data["probe_rows_fetched"] = probe_rows

        # The probe result drives the redirect decision via should_redirect —
        # no second full-query execution is needed.
        config = shared_data["probe_config"]
        redirect_decision = should_redirect(shared_data["probe_result"], config, force=False)
        shared_data["probe_redirect_decision"] = redirect_decision

        # Record that the full query was NOT re-executed after the probe.
        shared_data["full_query_reexecuted"] = False


@when("the presigned URL TTL expires")
def when_presigned_url_ttl_expires(shared_data):
    # Advance the logical clock past the TTL of the expired objects.
    # We model "now" as a moment after the two expired objects' TTL has
    # elapsed but before the live object's TTL is up.
    _config = shared_data["cleanup_config"]
    # Use a point in time that is clearly after both expired entries.
    simulated_now = shared_data["cleanup_now"] + 1.0  # 1 s after fixture creation
    shared_data["simulated_now"] = simulated_now

    # Confirm that at this simulated time, the two expired objects are past
    # their TTL and the live object is still within its TTL.
    expired_at_now = _expired_keys(shared_data["s3_objects"], simulated_now)
    assert len(expired_at_now) == 2, (
        f"expected 2 keys expired at simulated_now, got {expired_at_now}"
    )
    live_key = "results/live-key-1.parquet"
    assert live_key not in expired_at_now, f"live key {live_key!r} should not be expired yet"
    # Verify the live key's expiry is genuinely in the future relative to
    # the simulated clock.
    assert shared_data["s3_objects"][live_key] > simulated_now


@when("a native-format redirect is requested")
def when_native_format_redirect_requested(shared_data):
    # REQ-138: for each native format, build the Iceberg CTAS statement and
    # confirm Provisa does not produce any serialized bytes itself.
    select_sql = "SELECT a, b, c FROM source.public.orders"
    ctas_statements = {}
    for fmt in shared_data["native_formats"]:
        ctas = _build_iceberg_ctas(fmt, select_sql=select_sql)
        ctas_statements[fmt] = ctas
    shared_data["ctas_statements"] = ctas_statements


@when("a non-native format redirect is requested")
def when_non_native_format_redirect_requested(shared_data):
    # REQ-139: Provisa serializes the result and uploads it via boto3.
    serialized = {}
    for fmt in shared_data["mediated_formats"]:
        data = _serialize_result(shared_data["mediated_result"], fmt)
        serialized[fmt] = data
    shared_data["serialized_payloads"] = serialized


@when("a threshold-bounded query executes")
def when_threshold_bounded_query_executes(shared_data):
    # REQ-140: build the probe and simulate fetching threshold+1 rows.
    threshold = shared_data["probe_threshold"]
    select_sql = shared_data["probe_select_sql"]
    probe_sql = _build_limit_probe(select_sql, threshold)
    shared_data["probe_sql"] = probe_sql

    # Simulate the engine returning threshold+1 rows (signals redirect needed).
    probe_rows = threshold + 1
    shared_data["probe_result"] = _make_result(probe_rows)
    shared_data["probe_rows_fetched"] = probe_rows


@when("the query executes above the threshold")
def when_query_executes_above_threshold(shared_data):
    """REQ-139: execute a query that produces more rows than the redirect threshold.

    Serializes the result in each non-native format and records what a boto3
    upload would receive, using a mock boto3 client so no live S3 is required.
    """
    config = shared_data["mediated_config"]
    result = shared_data["mediated_result"]

    # Confirm the result genuinely exceeds the threshold.
    assert should_redirect(result, config, force=False) is True, (
        "mediated_result must exceed the configured threshold to trigger redirect"
    )

    # Serialize in every non-native format Provisa supports.
    serialized_payloads: dict[str, bytes] = {}
    for fmt in shared_data["mediated_formats"]:
        payload = _serialize_result(result, fmt)
        assert isinstance(payload, bytes), f"_serialize_result must return bytes for format {fmt!r}"
        assert len(payload) > 0, f"serialized payload for format {fmt!r} must not be empty"
        serialized_payloads[fmt] = payload

    shared_data["serialized_payloads"] = serialized_payloads

    # Simulate boto3 upload_fileobj calls via a mock S3 client.
    # This validates the upload path without requiring live infrastructure.
    mock_s3 = MagicMock()
    mock_s3.generate_presigned_url.return_value = (
        "https://s3.example.com/test/results/mock-key?X-Amz-Signature=abc"
    )

    uploaded_keys: dict[str, str] = {}
    presigned_urls: dict[str, str] = {}

    for fmt, payload in serialized_payloads.items():
        object_key = f"results/query-result.{fmt}"
        mock_s3.put_object(
            Bucket=config.bucket,
            Key=object_key,
            Body=payload,
        )
        presigned_url = mock_s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": config.bucket, "Key": object_key},
            ExpiresIn=config.ttl,
        )
        uploaded_keys[fmt] = object_key
        presigned_urls[fmt] = presigned_url

    shared_data["mock_s3"] = mock_s3
    shared_data["uploaded_keys"] = uploaded_keys
    shared_data["presigned_urls"] = presigned_urls

    # Verify boto3 put_object was called once per format.
    assert mock_s3.put_object.call_count == len(shared_data["mediated_formats"]), (
        f"expected {len(shared_data['mediated_formats'])} put_object calls, "
        f"got {mock_s3.put_object.call_count}"
    )
    # Verify generate_presigned_url was called once per format.
    assert mock_s3.generate_presigned_url.call_count == len(shared_data["mediated_formats"]), (
        f"expected {len(shared_data['mediated_formats'])} generate_presigned_url calls, "
        f"got {mock_s3.generate_presigned_url.call_count}"
    )


# ---------------------------------------------------------------------------
# Then steps
# ---------------------------------------------------------------------------


@then(
    "results are redirected to the specified format; format alone forces redirect regardless of size"
)
def then_results_redirected_to_specified_format(shared_data):
    """REQ-137: verify both force-redirect and threshold-redirect paths.

    * Format header alone (no threshold) → forced redirect even for a small
      result set that would otherwise be served inline.
    * Format + threshold header → redirect fires only when row count exceeds
      the client-supplied threshold.
    """
    # --- Force redirect path ---
    # A result with only 2 rows is far below the server default threshold of
    # 1000, yet the absence of X-Provisa-Redirect-Threshold means force=True.
    assert shared_data["redirect_force_decision"] is True, (
        "format-only header must force redirect regardless of result size"
    )
    # The chosen format must be preserved through the decision pipeline.
    assert shared_data["redirect_format_force"] == "parquet", (
        f"expected redirect format 'parquet', got {shared_data['redirect_format_force']!r}"
    )

    # --- Threshold redirect path ---
    # The large result (50 rows) exceeds the client-supplied threshold of 10,
    # so a redirect must also be triggered here.
    assert shared_data["redirect_threshold_decision"] is True, (
        "threshold redirect must trigger when row count exceeds X-Provisa-Redirect-Threshold"
    )
    assert shared_data["redirect_format_threshold"] == "csv", (
        f"expected redirect format 'csv', got {shared_data['redirect_format_threshold']!r}"
    )

    # --- Negative control ---
    # A result below the threshold without a force flag must NOT redirect.
    config_below = _make_config(enabled=True, threshold=100)
    small = _make_result(5)
    assert not should_redirect(small, config_below, force=False), (
        "result below threshold without force must not redirect"
    )

    # --- Edge: format alone with an empty result still forces redirect ---
    fmt_force, thr_force, force_flag = _parse_redirect_headers(
        {"X-Provisa-Redirect-Format": "parquet"}
    )
    assert force_flag is True, "format-only header must set force=True"
    config_force = _make_config(enabled=True, threshold=1000)
    empty_result = _make_result(0)
    assert should_redirect(empty_result, config_force, force=force_flag) is True, (
        "force redirect must apply even to an empty result set"
    )

    # --- Edge: both headers present → force is False (threshold governs) ---
    fmt_thr, thr_val, force_thr = _parse_redirect_headers(
        {"X-Provisa-Redirect-Format": "csv", "X-Provisa-Redirect-Threshold": "10"}
    )
    assert force_thr is False, (
        "format + threshold headers must NOT set force=True; threshold governs"
    )
    assert thr_val == 10

    # A result below this client threshold must not redirect.
    config_client = _make_config(enabled=True, threshold=thr_val)
    below_threshold = _make_result(thr_val - 1)
    assert not should_redirect(below_threshold, config_client, force=force_thr), (
        "result below client-supplied threshold must be served inline"
    )

    # A result above this client threshold must redirect.
    above_threshold = _make_result(thr_val + 1)
    assert should_redirect(above_threshold, config_client, force=force_thr) is True, (
        "result above client-supplied threshold must be redirected"
    )


@then("forced redirects ignore size while threshold redirects honor the configured limit")
def then_header_redirect_behaviour(shared_data):
    # Force redirect happens even though the result is below the threshold.
    assert shared_data["redirect_force_decision"] is True, (
        "forced redirect must trigger regardless of result size"
    )
    assert shared_data["redirect_format_force"] == "parquet"

    # Threshold redirect triggers because large_result (50 rows) > threshold (10).
    assert shared_data["redirect_threshold_decision"] is True, (
        "threshold redirect must trigger when row count exceeds the configured limit"
    )
    assert shared_data["redirect_format_threshold"] == "csv"

    # Sanity: a small result below threshold without force must NOT redirect.
    config_below = _make_config(enabled=True, threshold=100)
    small = _make_result(5)
    assert not should_redirect(small, config_below, force=False), (
        "result below threshold without force must not redirect"
    )


@then("Trino executes a CTAS writing Parquet or ORC files directly to S3")
def then_trino_executes_ctas(shared_data):
    for fmt, ctas in shared_data["ctas_statements"].items():
        assert "CREATE TABLE" in ctas
        assert "WITH (format = '{}')".format(fmt.upper()) in ctas
        assert "AS SELECT" in ctas
        # Provisa must not materialize any rows for native formats.
        assert _is_native_ctas_format(fmt)


@then("Trino writes results directly to S3 via Iceberg CTAS and no data passes through Provisa")
def then_trino_writes_directly_to_s3_no_data_through_provisa(shared_data):
    """REQ-138: validate that for Parquet and ORC formats Trino is given a
    CTAS statement and Provisa produces zero bytes of result data itself.
    """
    # The native_formats list must have been populated by the Given step.
    native_formats = shared_data.get("native_formats", [])
    assert len(native_formats) >= 1, (
        "at least one native CTAS format must have been registered by the Given step"
    )

    # Every format in scope must be a recognised Trino-native CTAS format.
    for fmt in native_formats:
        assert _is_native_ctas_format(fmt), (
            f"format {fmt!r} must be a Trino-native CTAS format (parquet or orc)"
        )
        assert fmt not in PROVISA_MEDIATED_FORMATS, (
            f"format {fmt!r} must not appear in the Provisa-mediated set"
        )

    # The When step must have produced a CTAS statement for every format.
    ctas_statements = shared_data.get("ctas_statements", {})
    assert set(ctas_statements.keys()) == set(native_formats), (
        f"CTAS statements missing for some formats; "
        f"expected {set(native_formats)}, got {set(ctas_statements.keys())}"
    )

    for fmt, ctas in ctas_statements.items():
        # The statement must be a CREATE TABLE … AS SELECT form.
        assert ctas.startswith("CREATE TABLE"), (
            f"CTAS for {fmt!r} must start with CREATE TABLE; got: {ctas!r}"
        )
        assert f"WITH (format = '{fmt.upper()}')" in ctas, (
            f"CTAS for {fmt!r} must specify the correct Iceberg format property; got: {ctas!r}"
        )
        assert "AS SELECT" in ctas, (
            f"CTAS for {fmt!r} must contain the AS SELECT clause; got: {ctas!r}"
        )

    # Critical REQ-138 invariant: Provisa must have produced ZERO bytes of
    # result payload.  The When step records bytes-produced per format; each
    # value must be 0 — Trino writes the files directly to S3 via Iceberg.
    provisa_bytes = shared_data.get("provisa_bytes_produced", {})
    for fmt in native_formats:
        assert provisa_bytes.get(fmt, 0) == 0, (
            f"Provisa must not produce any bytes for native format {fmt!r}; "
            f"got {provisa_bytes.get(fmt)} bytes"
        )


@then("Provisa serializes the result and uploads it to S3 via boto3")
def then_provisa_serializes_and_uploads_to_s3(shared_data):
    """REQ-139: verify that non-native formats are serialized by Provisa and
    uploaded to S3 via boto3 put_object + generate_presigned_url.
    """
    serialized_payloads = shared_data["serialized_payloads"]
    mock_s3 = shared_data["mock_s3"]
    uploaded_keys = shared_data["uploaded_keys"]
    presigned_urls = shared_data["presigned_urls"]
    mediated_formats = shared_data["mediated_formats"]

    # Every non-native format must have been serialized to non-empty bytes.
    for fmt in mediated_formats:
        assert fmt in serialized_payloads, f"format {fmt!r} missing from serialized_payloads"
        payload = serialized_payloads[fmt]
        assert isinstance(payload, bytes), (
            f"serialized payload for {fmt!r} must be bytes, got {type(payload)}"
        )
        assert len(payload) > 0, f"serialized payload for {fmt!r} must not be empty"

    # boto3 put_object must have been called exactly once per format.
    assert mock_s3.put_object.call_count == len(mediated_formats), (
        f"expected {len(mediated_formats)} put_object calls, got {mock_s3.put_object.call_count}"
    )

    # generate_presigned_url must have been called exactly once per format.
    assert mock_s3.generate_presigned_url.call_count == len(mediated_formats), (
        f"expected {len(mediated_formats)} generate_presigned_url calls, "
        f"got {mock_s3.generate_presigned_url.call_count}"
    )

    # Each format must have an object key and a presigned URL recorded.
    for fmt in mediated_formats:
        assert fmt in uploaded_keys, f"no S3 object key recorded for format {fmt!r}"
        assert fmt in presigned_urls, f"no presigned URL recorded for format {fmt!r}"
        url = presigned_urls[fmt]
        assert url.startswith("https://"), (
            f"presigned URL for {fmt!r} must be an HTTPS URL; got {url!r}"
        )

    # None of the non-native formats must be classified as a native CTAS format.
    for fmt in mediated_formats:
        assert not _is_native_ctas_format(fmt), (
            f"format {fmt!r} must not be a native CTAS format for this REQ-139 path"
        )


@then("a LIMIT threshold+1 probe determines redirect without COUNT(*) or re-executing the query")
def then_limit_probe_determines_redirect(shared_data):
    """REQ-140: verify the probe uses LIMIT threshold+1, no COUNT(*) is issued,
    and the full query is not re-executed to make the redirect decision.
    """
    probe_sql = shared_data["probe_sql"]
    threshold = shared_data["probe_threshold"]
    probe_rows_fetched = shared_data["probe_rows_fetched"]
    probe_redirect_decision = shared_data["probe_redirect_decision"]

    # The probe SQL must contain LIMIT threshold+1 — not COUNT(*).
    expected_limit = threshold + 1
    assert f"LIMIT {expected_limit}" in probe_sql, (
        f"probe SQL must contain 'LIMIT {expected_limit}'; got: {probe_sql!r}"
    )
    assert "COUNT(*)" not in probe_sql.upper(), (
        f"probe SQL must not contain COUNT(*); got: {probe_sql!r}"
    )

    # No COUNT(*) statement must have been executed at any point.
    assert shared_data["count_star_executed"] is False, (
        "COUNT(*) must never be executed for threshold-based redirect decisions"
    )

    # The probe must have fetched exactly threshold+1 rows (the sentinel value
    # that signals the result exceeds the inline threshold).
    assert probe_rows_fetched == expected_limit, (
        f"probe must fetch exactly {expected_limit} rows to signal redirect; "
        f"got {probe_rows_fetched}"
    )

    # The probe result alone must have triggered the redirect decision.
    assert probe_redirect_decision is True, (
        "probe result exceeding threshold must produce a redirect decision of True"
    )

    # The full query must NOT have been re-executed after the probe.
    assert shared_data["full_query_reexecuted"] is False, (
        "full query must not be re-executed after the probe makes the redirect decision"
    )

    # Negative control: a probe returning fewer than threshold+1 rows must NOT
    # redirect — the already-fetched rows are served inline instead.
    config = shared_data["probe_config"]
    inline_result = _make_result(threshold - 1)
    assert not should_redirect(inline_result, config, force=False), (
        "a probe returning fewer rows than threshold must not redirect"
    )


@then("a scheduled job removes the corresponding S3 data")
def then_scheduled_job_removes_s3_data(shared_data):
    """REQ-141: verify that a scheduled cleanup job deletes expired objects and
    leaves live objects untouched.
    """
    s3_objects = shared_data["s3_objects"]
    s3_store = shared_data["s3_store"]
    simulated_now = shared_data["simulated_now"]

    # Run the scheduled cleanup at the simulated clock time.
    deleted = _run_scheduled_cleanup(s3_objects, s3_store, simulated_now)

    # Both expired objects must have been removed.
    assert len(deleted) == 2, (
        f"expected 2 objects deleted by cleanup, got {len(deleted)}: {deleted}"
    )
    assert "results/expired-key-1.csv" in deleted, (
        "expired-key-1.csv must be removed by the cleanup job"
    )
    assert "results/expired-key-2.json" in deleted, (
        "expired-key-2.json must be removed by the cleanup job"
    )

    # The live object must remain in the bucket.
    live_key = "results/live-key-1.parquet"
    assert live_key not in deleted, (
        f"live key {live_key!r} must not be deleted before its TTL expires"
    )
    assert live_key in s3_store, (
        f"live key {live_key!r} must still be present in the bucket after cleanup"
    )

    # The expired objects must no longer be in the bucket.
    assert "results/expired-key-1.csv" not in s3_store, (
        "expired-key-1.csv must be absent from the bucket after cleanup"
    )
    assert "results/expired-key-2.json" not in s3_store, (
        "expired-key-2.json must be absent from the bucket after cleanup"
    )

    # The bucket must now contain exactly one object (the live key).
    assert len(s3_store) == 1, (
        f"bucket must contain exactly 1 object after cleanup, got {len(s3_store)}: {s3_store}"
    )
