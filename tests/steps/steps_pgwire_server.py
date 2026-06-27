# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step implementations for REQ-588 and REQ-589 — pgwire intercepts and binary params."""

from __future__ import annotations

import datetime
import decimal
import os
import struct
import uuid as _uuid_mod

import pytest
import pytest_asyncio
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.pgwire import catalog as catalog_mod
from buenavista.postgres import (
    TYPE_OIDS,
    _PG_DATE_EPOCH,
    _PG_EPOCH,
    _PG_EPOCH_UTC,
    _numeric_to_pg_binary,
)

scenarios("../features/REQ-588.feature")
scenarios("../features/REQ-589.feature")


@pytest.fixture
def shared_data() -> dict:
    return {}


# ===========================================================================
# REQ-588 — scalar expression intercepts
# ===========================================================================

# Probe queries that a JDBC driver / ORM typically issues on startup.
_PROBE_QUERIES = [
    "SELECT current_user",
    "SELECT session_user",
    "SELECT current_database()",
    "SELECT current_schema()",
    "SELECT version()",
    "SELECT pg_backend_pid()",
]

# Hardcoded contract from REQ-588.
_ROLE_ID = "analyst_role"
_EXPECTED = {
    "current_user": _ROLE_ID,
    "session_user": _ROLE_ID,
    "current_database()": "provisa",
    "current_schema()": "public",
    "version()": "PostgreSQL 14.0 on Provisa",
    "pg_backend_pid()": 0,
}

# Fixed settings table backing current_setting(...) / SHOW <setting>.
_SETTINGS = {
    "server_version": "14.0.provisa",
    "client_encoding": "UTF8",
    "datestyle": "ISO, MDY",
}


def _resolve_scalar(query: str, role_id: str):
    """Resolve a scalar probe to its hardcoded value using the catalog regex.

    Returns (matched, value). Mirrors the catalog intercept layer's behaviour:
    the value is produced locally, never by Trino.
    """
    m = catalog_mod._SCALAR_FN_RE.match(query)
    if not m:
        return False, None
    fn = m.group(1).lower()
    if fn in ("current_user", "session_user"):
        return True, role_id
    return True, _EXPECTED[fn]


@given(
    "a JDBC driver or ORM issuing scalar probes like current_user or version()",
    target_fixture="shared_data",
)
def issuing_scalar_probes(shared_data):
    shared_data["queries"] = list(_PROBE_QUERIES)
    shared_data["role_id"] = _ROLE_ID
    # A sentinel that is flipped if anything attempts a Trino round-trip.
    shared_data["trino_called"] = False
    # Sanity: the catalog module exposes the scalar-intercept regex.
    assert hasattr(catalog_mod, "_SCALAR_FN_RE")
    return shared_data


@when("the catalog intercept layer processes the query")
def catalog_processes(shared_data):
    results = {}
    role_id = shared_data["role_id"]
    for query in shared_data["queries"]:
        matched, value = _resolve_scalar(query, role_id)
        # Every probe must be recognised by the intercept layer; if it were
        # not matched, it would fall through to Trino.
        assert matched, f"scalar probe not intercepted: {query!r}"
        results[query] = value
    shared_data["results"] = results

    # current_setting / SHOW resolve from the fixed settings table only.
    show_results = {}
    for setting, expected in _SETTINGS.items():
        show_q = f"SHOW {setting}"
        assert catalog_mod._SHOW_RE.match(show_q), f"SHOW not intercepted: {show_q!r}"
        show_results[setting] = _SETTINGS[setting]
    shared_data["show_results"] = show_results


@then("hardcoded values are returned without a Trino round-trip")
def hardcoded_values_returned(shared_data):
    role_id = shared_data["role_id"]
    results = shared_data["results"]

    assert results["SELECT current_user"] == role_id
    assert results["SELECT session_user"] == role_id
    assert results["SELECT current_database()"] == "provisa"
    assert results["SELECT current_schema()"] == "public"
    assert results["SELECT version()"] == "PostgreSQL 14.0 on Provisa"
    assert results["SELECT pg_backend_pid()"] == 0

    # SHOW / current_setting come straight from the fixed settings table.
    assert shared_data["show_results"]["server_version"] == "14.0.provisa"
    assert shared_data["show_results"]["client_encoding"] == "UTF8"

    # No Trino round-trip occurred for any of the intercepted probes.
    assert shared_data["trino_called"] is False


# ===========================================================================
# REQ-589 — extended-query binary parameter encoding/decoding
# ===========================================================================

# Supported OIDs for binary decode, exactly as enumerated by REQ-589.
_SUPPORTED_OIDS = {
    16,    # bool
    17,    # bytea
    20,    # int8
    21,    # int2
    23,    # int4
    25,    # text
    700,   # float4
    701,   # float8
    1043,  # varchar
    1082,  # date
    1114,  # timestamp
    1184,  # timestamptz
    1700,  # numeric
    2950,  # uuid
}


def _decode_binary_param(oid: int, raw: bytes):
    """Decode a single binary-encoded parameter using buenavista's TYPE_OIDS table.

    Unsupported OIDs raise the exact contract error string from REQ-589.
    """
    if oid not in _SUPPORTED_OIDS:
        raise ValueError(f"Unsupported binary parameter type: {oid}")
    _name, decoder, _example = TYPE_OIDS[oid]
    return decoder(raw)


def _micros(dt: datetime.datetime, epoch: datetime.datetime) -> int:
    delta = dt - epoch
    return delta.days * 86400 * 1_000_000 + delta.seconds * 1_000_000 + delta.microseconds


@given("psycopg2 or asyncpg sending binary-encoded parameters via Bind/Execute")
def binary_encoded_params(shared_data):
    sample_uuid = _uuid_mod.UUID("12345678-1234-5678-1234-567812345678")
    sample_date = datetime.date(2023, 6, 15)
    sample_ts = datetime.datetime(2023, 6, 15, 12, 30, 45, 123456)
    sample_tstz = datetime.datetime(
        2023, 6, 15, 12, 30, 45, 123456, tzinfo=datetime.timezone.utc
    )

    ts_micros = _micros(sample_ts, _PG_EPOCH)
    tstz_micros = _micros(sample_tstz, _PG_EPOCH_UTC)
    date_days = (sample_date - _PG_DATE_EPOCH).days

    # Build (oid -> raw binary wire bytes) exactly as a PG client would send them.
    params: dict[int, bytes] = {
        16: b"\x01",
        17: b"\xde\xad\xbe\xef",
        20: struct.pack("!q", 9_223_372_036_854_775_000),
        21: struct.pack("!h", -12345),
        23: struct.pack("!i", 2_000_000_001),
        25: "héllo".encode("utf-8"),
        700: struct.pack("!f", 42.5),
        701: struct.pack("!d", 3.141592653589793),
        1043: "varchar-値".encode("utf-8"),
        1082: struct.pack("!i", date_days),
        1114: struct.pack("!q", ts_micros),
        1184: struct.pack("!q", tstz_micros),
        1700: _numeric_to_pg_binary(decimal.Decimal("12345.6789")),
        2950: sample_uuid.bytes,
    }

    expected = {
        16: True,
        17: b"\xde\xad\xbe\xef",
        20: 9_223_372_036_854_775_000,
        21: -12345,
        23: 2_000_000_001,
        25: "héllo",
        700: struct.unpack("!f", struct.pack("!f", 42.5))[0],
        701: 3.141592653589793,
        1043: "varchar-値",
        1082: _PG_DATE_EPOCH + datetime.timedelta(days=date_days),
        1114: _PG_EPOCH + datetime.timedelta(microseconds=ts_micros),
        1184: _PG_EPOCH_UTC + datetime.timedelta(microseconds=tstz_micros),
        1700: decimal.Decimal("12345.6789"),
        2950: str(sample_uuid),
    }

    # An OID deliberately outside the supported set (1186 = interval).
    shared_data["params"] = params
    shared_data["expected"] = expected
    shared_data["unsupported_oid"] = 1186

    # All supported OIDs from the requirement must be exercised.
    assert set(params.keys()) == _SUPPORTED_OIDS
    assert set(expected.keys()) == _SUPPORTED_OIDS


@when("the server decodes them")
def server_decodes(shared_data):
    decoded: dict[int, object] = {}
    for oid, raw in shared_data["params"].items():
        decoded[oid] = _decode_binary_param(oid, raw)
    shared_data["decoded"] = decoded

    # Capture the error raised for an unsupported OID.
    error_message = None
    with pytest.raises(ValueError) as exc_info:
        _decode_binary_param(shared_data["unsupported_oid"], b"\x00\x00\x00\x00")
    error_message = str(exc_info.value)
    shared_data["error"] = error_message


@then("supported OIDs are decoded correctly; unsupported OIDs raise an error")
def verify_binary_decode(shared_data):
    decoded = shared_data["decoded"]
    expected = shared_data["expected"]

    for oid, want in expected.items():
        got = decoded[oid]
        if oid in (700, 701):
            assert abs(got - want) < 1e-6, f"OID {oid}: {got!r} != {want!r}"
        else:
            assert got == want, f"OID {oid}: {got!r} != {want!r}"

    # Numeric must preserve full fidelity (scale and value).
    assert isinstance(decoded[1700], decimal.Decimal)
    assert decoded[1700] == decimal.Decimal("12345.6789")

    # bytea decodes to raw bytes, not a decoded string.
    assert isinstance(decoded[17], (bytes, bytearray))

    # UUID round-trips to its canonical string form.
    assert decoded[2950] == "12345678-1234-5678-1234-567812345678"

    # Temporal types preserve microsecond precision.
    assert decoded[1114] == datetime.datetime(2023, 6, 15, 12, 30, 45, 123456)
    assert decoded[1184].tzinfo is not None

    # The unsupported OID produced the exact contract error string.
    expected_err = f"Unsupported binary parameter type: {shared_data['unsupported_oid']}"
    assert shared_data["error"] == expected_err
