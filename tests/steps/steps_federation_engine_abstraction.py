# Copyright (c) 2026 Kenneth Stott
# Canary: 86d5cc08-5bd6-4104-822f-4a2423b190b2
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-883: binary COPY wire format."""

from __future__ import annotations

import struct

import pytest
from pytest_bdd import given, scenario, scenarios, then, when

from provisa.core.models import Source, SourceType
from provisa.executor.result import QueryResult
from provisa.federation.connector import Mechanism
from provisa.federation.engine import (
    UnreachableSource,
    build_duckdb_engine,
    build_trino_engine,
)
from provisa.pgwire.copy_binary import (
    _COPY_BINARY_SIGNATURE,
    _duckdb_binary_tag,
    _encode_binary_field,
)
from provisa.pgwire.copy_handler import _queryresult_to_copy_bytes

# ---------------------------------------------------------------------------
# Scenario registration
# ---------------------------------------------------------------------------

FEATURE = "../features/REQ-883.feature"


@scenario(FEATURE, "REQ-883 default behaviour")
def test_req_883_default_behaviour():
    pass


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


# ---------------------------------------------------------------------------
# Given
# ---------------------------------------------------------------------------


@given(
    "a client issues COPY (SELECT id, name FROM t) TO STDOUT (FORMAT binary) over pgwire",
    target_fixture="shared_data",
)
def given_copy_binary_issued():
    """Construct a QueryResult that represents the server-side result of
    ``SELECT id, name FROM t`` and store it plus its binary COPY serialisation
    in *shared_data* for downstream steps.
    """
    qr = QueryResult(
        rows=[(1, "alice"), (2, None), (3, "bob")],
        column_names=["id", "name"],
        column_types=["INTEGER", "VARCHAR"],
    )
    binary_bytes = _queryresult_to_copy_bytes(qr, "binary")
    text_bytes = _queryresult_to_copy_bytes(qr, "text")
    return {
        "query_result": qr,
        "binary_bytes": binary_bytes,
        "text_bytes": text_bytes,
        "format": "binary",
    }


# ---------------------------------------------------------------------------
# When
# ---------------------------------------------------------------------------


@when("the result has an INTEGER id and a VARCHAR name column")
def when_result_has_integer_id_and_varchar_name(shared_data):
    """Assert that the QueryResult actually carries an INTEGER id and VARCHAR name."""
    qr: QueryResult = shared_data["query_result"]
    assert qr.column_names[0] == "id"
    assert qr.column_names[1] == "name"
    assert qr.column_types[0] == "INTEGER"
    assert qr.column_types[1] == "VARCHAR"


# ---------------------------------------------------------------------------
# Then
# ---------------------------------------------------------------------------


@then(
    "the server replies with a binary-mode CopyOutResponse and a byte stream beginning with the "
    "PGCOPY signature, encoding id as a 4-byte int and name as UTF-8 text per row, a NULL as "
    "field length -1, and terminating with the int16 -1 trailer"
)
def then_binary_copy_stream_is_correct(shared_data):
    """Verify every structural invariant of the binary COPY stream."""
    out: bytes = shared_data["binary_bytes"]

    # 1. Starts with the 11-byte PGCOPY file signature.
    assert out.startswith(_COPY_BINARY_SIGNATURE), (
        f"Stream does not begin with PGCOPY signature: {out[:11]!r}"
    )

    # 2. Ends with the int16 -1 trailer.
    assert out.endswith(struct.pack("!h", -1)), "Stream missing int16 -1 trailer"

    # 3. Flags word (4 bytes) and header-extension-area length (4 bytes) immediately
    #    after signature are both 0.
    flags_offset = len(_COPY_BINARY_SIGNATURE)
    flags = struct.unpack_from("!i", out, flags_offset)[0]
    ext_len = struct.unpack_from("!i", out, flags_offset + 4)[0]
    assert flags == 0, f"Flags should be 0, got {flags}"
    assert ext_len == 0, f"Header extension length should be 0, got {ext_len}"

    # 4. First data row: field_count=2, id=1 encoded as int4, name="alice" as UTF-8 text.
    row_start = flags_offset + 4 + 4  # after sig + flags + ext_len
    field_count = struct.unpack_from("!h", out, row_start)[0]
    assert field_count == 2, f"Expected 2 fields per row, got {field_count}"

    id_len_offset = row_start + 2
    id_len = struct.unpack_from("!i", out, id_len_offset)[0]
    assert id_len == 4, f"id field length should be 4 (int4), got {id_len}"
    id_val = struct.unpack_from("!i", out, id_len_offset + 4)[0]
    assert id_val == 1, f"First row id should be 1, got {id_val}"

    name_len_offset = id_len_offset + 4 + 4
    name_len = struct.unpack_from("!i", out, name_len_offset)[0]
    alice_bytes = "alice".encode("utf-8")
    assert name_len == len(alice_bytes), (
        f"name field length should be {len(alice_bytes)}, got {name_len}"
    )
    name_val = out[name_len_offset + 4 : name_len_offset + 4 + name_len]
    assert name_val == alice_bytes, f"name field value should be b'alice', got {name_val!r}"

    # 5. NULL field: row with id=2, name=None — name must be encoded as length -1.
    null_marker = struct.pack("!i", -1)
    # The NULL for name=None in row 2 must appear in the stream.
    assert null_marker in out, "NULL field encoding (int32 -1) not found in stream"

    # 6. Verify via _encode_binary_field directly.
    assert _encode_binary_field(None, "int4") == null_marker
    assert _encode_binary_field(1, "int4") == struct.pack("!i", 4) + struct.pack("!i", 1)

    # 7. Type-tag mapping for the columns.
    assert _duckdb_binary_tag("INTEGER") == "int4"
    assert _duckdb_binary_tag("VARCHAR") == "text"


@then("the same COPY in text/csv format is byte-for-byte unchanged from before")
def then_text_copy_is_stable(shared_data):
    """Running text COPY serialisation twice must produce identical bytes (determinism).

    'Unchanged from before' means the text output is deterministic and does not
    accidentally switch to binary encoding just because binary was produced first.
    """
    qr: QueryResult = shared_data["query_result"]

    text_bytes_first: bytes = shared_data["text_bytes"]

    # Produce a second independent serialisation.
    text_bytes_second = _queryresult_to_copy_bytes(qr, "text")

    assert text_bytes_first == text_bytes_second, (
        "Text COPY output is not deterministic: first and second serialisations differ"
    )

    # Text output must NOT start with the binary PGCOPY signature.
    assert not text_bytes_first.startswith(_COPY_BINARY_SIGNATURE), (
        "Text COPY output incorrectly starts with binary PGCOPY signature"
    )

    # Text output for integer 1 should contain b"1" as a tab-separated field, not binary.
    assert b"1\t" in text_bytes_first or text_bytes_first.startswith(b"1\t"), (
        f"Text COPY output does not look like tab-delimited text: {text_bytes_first[:40]!r}"
    )

    # The NULL sentinel in text format is \N, not the 4-byte int32 -1.
    null_binary = struct.pack("!i", -1)
    assert b"\\N" in text_bytes_first, "Text COPY output should encode NULL as \\N"
    assert null_binary not in text_bytes_first, (
        "Text COPY output must not contain binary NULL encoding (int32 -1)"
    )


scenarios("../features/REQ-841.feature")


@pytest.fixture
def shared_data_841():
    return {}


@given(
    "a source reference and the configured federation engine",
    target_fixture="shared_data",
)
def given_source_and_engine():
    """Set up a reachable source (postgresql via Trino) and an unreachable one (parquet — no Trino
    connector and not materialize-only)."""
    reachable_source = Source(
        id="orders_pg",
        type=SourceType.postgresql,
        host="db.example.com",
        port=5432,
        database="orders",
        username="reader",
    )
    unreachable_source = Source(
        id="legacy_parquet",
        type=SourceType.parquet,
        path="/data/legacy.parquet",
    )
    engine = build_trino_engine()
    return {
        "engine": engine,
        "reachable_source": reachable_source,
        "unreachable_source": unreachable_source,
    }


@when("the planner resolves it")
def when_planner_resolves(shared_data):
    """Attempt resolution for both the reachable and unreachable source."""
    engine = shared_data["engine"]

    # Resolve the reachable source — must succeed.
    try:
        entry = engine.resolve(shared_data["reachable_source"])
        shared_data["resolved_entry"] = entry
        shared_data["resolve_error"] = None
    except UnreachableSource as exc:
        shared_data["resolved_entry"] = None
        shared_data["resolve_error"] = exc

    # Resolve the unreachable source — must raise.
    try:
        engine.resolve(shared_data["unreachable_source"])
        shared_data["unreachable_error"] = None
    except UnreachableSource as exc:
        shared_data["unreachable_error"] = exc


@then(
    "if a connector exists it is exposed by that connector's mechanism "
    "(attach in place, or land into materialization_store), "
    "otherwise it is rejected as unreachable."
)
def then_connector_mechanism_or_rejected(shared_data):
    engine = shared_data["engine"]

    # --- Reachable source: connector found, exposed by its fixed mechanism ---
    reachable_source = shared_data["reachable_source"]
    assert shared_data["resolve_error"] is None, (
        f"Reachable source unexpectedly raised: {shared_data['resolve_error']}"
    )
    entry = shared_data["resolved_entry"]
    assert entry is not None, "resolve() returned None for a reachable source"

    # The mechanism is fixed by the connector (REQ-841): postgresql on Trino is ATTACH.
    connector = engine.connector_for(reachable_source.type.value)
    assert entry.mechanism is connector.mechanism, (
        f"Entry mechanism {entry.mechanism!r} does not match connector's fixed "
        f"mechanism {connector.mechanism!r}"
    )
    assert entry.mechanism in (Mechanism.ATTACH_RW, Mechanism.ATTACH_R), (
        f"Mechanism must be an ATTACH_* live-read mode, got {entry.mechanism!r}"
    )

    # For ATTACH: details must describe the in-place reference (no data movement).
    if entry.mechanism is Mechanism.ATTACH_RW:
        assert entry.details, (
            "ATTACH mechanism must provide details describing the in-place reference"
        )

    # Engine and source_type are stamped correctly on the entry.
    assert entry.engine == engine.name
    assert entry.source_type == reachable_source.type.value

    # --- Unreachable source: no connector → rejected as UnreachableSource ---
    unreachable_err = shared_data["unreachable_error"]
    assert unreachable_err is not None, (
        "Expected UnreachableSource to be raised for a source with no connector, but it was not"
    )
    assert isinstance(unreachable_err, UnreachableSource), (
        f"Expected UnreachableSource, got {type(unreachable_err)}"
    )
    assert unreachable_err.engine == engine.name
    assert unreachable_err.source_type == shared_data["unreachable_source"].type.value

    # Cross-check with engine.reachable() — must be consistent.
    assert engine.reachable(reachable_source.type.value) is True
    assert engine.reachable(shared_data["unreachable_source"].type.value) is False

    # Verify mechanism is NOT chosen per query: calling resolve() again returns the
    # same mechanism — it is fixed by the connector.
    entry_again = engine.resolve(reachable_source)
    assert entry_again.mechanism is entry.mechanism, (
        "Mechanism changed between two resolve() calls — it must be fixed by the connector"
    )

    # Verify partial federator (DuckDB) also correctly rejects unreachable sources.
    duckdb = build_duckdb_engine()
    mysql_source = Source(
        id="inv_mysql",
        type=SourceType.mysql,
        host="mysql.example.com",
        port=3306,
        database="inventory",
        username="reader",
    )
    assert duckdb.reachable("mysql") is False
    with pytest.raises(UnreachableSource) as exc_info:
        duckdb.resolve(mysql_source)
    assert exc_info.value.engine == "duckdb"
    assert exc_info.value.source_type == "mysql"
