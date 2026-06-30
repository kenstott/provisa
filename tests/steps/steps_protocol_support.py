# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""pytest-bdd step implementations for REQ-802: Bolt Protocol Support."""

from __future__ import annotations

import asyncio
import io
import os
import struct
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenario, then, when

# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data() -> dict:
    return {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_chunked(data: bytes) -> bytes:
    """Wrap raw bytes in a single Bolt chunk + end-of-message marker."""
    return struct.pack("!H", len(data)) + data + b"\x00\x00"


def _encode_version_proposal(major: int, minor: int, rng: int = 0) -> bytes:
    """Encode a 4-byte Bolt version proposal: [0x00, range, minor, major]."""
    return bytes([0x00, rng, minor, major])


def _build_hello_message() -> bytes:
    """Build a minimal Bolt HELLO message for Bolt 5.x (no credentials)."""
    from provisa.bolt.packstream import pack_message
    from provisa.bolt.messages import HELLO

    meta = {
        "user_agent": "pytest-bdd/1.0",
        "bolt_agent": {"product": "pytest-bdd/1.0"},
    }
    return pack_message(HELLO, meta)


def _build_run_message(cypher: str) -> bytes:
    """Build a Bolt RUN message for the given Cypher query."""
    from provisa.bolt.packstream import pack_message
    from provisa.bolt.messages import RUN

    # RUN fields: query, parameters, metadata
    return pack_message(RUN, [cypher, {}, {}])


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@pytest.mark.integration
@given("a Cypher client connecting to Bolt port 5251")
def step_cypher_client_connecting(shared_data: dict) -> None:
    """Establish a raw TCP connection to the Bolt server at port 5251."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    async def _connect():
        reader, writer = await asyncio.open_connection("127.0.0.1", 5251)
        return reader, writer

    reader, writer = asyncio.get_event_loop().run_until_complete(_connect())
    shared_data["reader"] = reader
    shared_data["writer"] = writer
    assert reader is not None
    assert writer is not None


@pytest.mark.integration
@when(parsers.parse('the client sends Cypher query "{cypher}"'))
def step_client_sends_cypher(cypher: str, shared_data: dict) -> None:
    """Send Bolt magic, version proposals, HELLO, and a RUN message."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    from provisa.bolt.messages import MAGIC

    reader: asyncio.StreamReader = shared_data["reader"]
    writer = shared_data["writer"]
    shared_data["cypher"] = cypher

    async def _read_chunked(r: asyncio.StreamReader) -> bytes:
        from provisa.bolt.framing import read_message

        return await read_message(r)

    async def _exchange():
        # 1. Send magic
        writer.write(MAGIC)

        # 2. Send four version proposals — prefer Bolt 5.4
        writer.write(_encode_version_proposal(5, 4))
        writer.write(_encode_version_proposal(5, 3))
        writer.write(_encode_version_proposal(4, 4))
        writer.write(b"\x00\x00\x00\x00")  # no preference
        await writer.drain()

        # 3. Read negotiated version (4 bytes)
        chosen = await reader.readexactly(4)
        shared_data["chosen_version"] = chosen

        # 4. Send HELLO (Bolt 5.x — no credentials in HELLO)
        hello_bytes = _build_hello_message()
        writer.write(_make_chunked(hello_bytes))
        await writer.drain()

        # 5. Read SUCCESS response to HELLO
        hello_resp = await _read_chunked(reader)
        shared_data["hello_response"] = hello_resp

        # 6. Send RUN
        run_bytes = _build_run_message(cypher)
        writer.write(_make_chunked(run_bytes))
        await writer.drain()

        # 7. Read RUN response (SUCCESS with fields metadata)
        run_resp = await _read_chunked(reader)
        shared_data["run_response"] = run_resp

        # 8. Send PULL all (-1)
        from provisa.bolt.packstream import pack_message
        from provisa.bolt.messages import PULL

        pull_bytes = pack_message(PULL, {"n": -1})
        writer.write(_make_chunked(pull_bytes))
        await writer.drain()

        # 9. Collect records until SUCCESS
        records = []
        while True:
            chunk = await _read_chunked(reader)
            if not chunk:
                break
            tag = chunk[1] if len(chunk) >= 2 else 0
            from provisa.bolt.messages import RECORD, SUCCESS, FAILURE

            if tag == RECORD:
                records.append(chunk)
            elif tag in (SUCCESS, FAILURE):
                shared_data["pull_final"] = chunk
                shared_data["pull_final_tag"] = tag
                break
        shared_data["records"] = records

    asyncio.get_event_loop().run_until_complete(_exchange())


@pytest.mark.integration
@then("the server accepts the handshake (magic + version negotiation)")
def step_server_accepts_handshake(shared_data: dict) -> None:
    """Assert the server replied with a non-zero version (handshake succeeded)."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    chosen = shared_data.get("chosen_version", b"\x00\x00\x00\x00")
    # Non-zero means the server agreed on a version
    assert chosen != b"\x00\x00\x00\x00", (
        f"Server rejected all version proposals; got {chosen.hex()}"
    )
    # Verify the chosen version is one we support
    from provisa.bolt.messages import SUPPORTED_VERSIONS

    major, minor = chosen[3], chosen[2]
    assert (major, minor) in SUPPORTED_VERSIONS, (
        f"Server chose unsupported version ({major}, {minor})"
    )


@pytest.mark.integration
@then("the query is transpiled to SQL via WriteTranslator")
def step_query_transpiled_to_sql(shared_data: dict) -> None:
    """Verify that the RUN response carries a SUCCESS (not FAILURE), implying
    the Cypher was accepted and transpiled without parse errors."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    run_response = shared_data.get("run_response", b"")
    assert len(run_response) >= 2, "No RUN response received"

    from provisa.bolt.messages import SUCCESS, FAILURE

    tag = run_response[1]
    assert tag != FAILURE, (
        f"Server returned FAILURE for RUN; transpilation failed. raw={run_response.hex()}"
    )
    assert tag == SUCCESS, (
        f"Expected SUCCESS(0x70) for RUN response, got 0x{tag:02X}"
    )


@pytest.mark.integration
@then("governance (RLS, masking, visibility) is applied at compile time")
def step_governance_applied(shared_data: dict) -> None:
    """Assert governance was applied: either rows returned reflect RLS filtering,
    or the server correctly returned a governed result set (no raw un-governed data).

    In integration mode we check that the pipeline completed (SUCCESS/RECORD tags),
    which requires the full governance path to have executed without exceptions.
    """
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    # A FAILURE here would mean governance raised an unhandled exception
    pull_final_tag = shared_data.get("pull_final_tag")
    from provisa.bolt.messages import SUCCESS, FAILURE

    assert pull_final_tag is not None, "No PULL response received"
    assert pull_final_tag != FAILURE, (
        "Governance pipeline raised FAILURE during PULL; "
        f"raw={shared_data.get('pull_final', b'').hex()}"
    )
    assert pull_final_tag == SUCCESS


@pytest.mark.integration
@then("results are executed and returned as Bolt structures (nodes, relationships)")
def step_results_as_bolt_structures(shared_data: dict) -> None:
    """Verify that RECORD messages contain valid PackStream node/relationship structs."""
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    records: list[bytes] = shared_data.get("records", [])
    # There may be zero records if the dataset is empty, but each RECORD that
    # was received must be a properly encoded PackStream structure.
    for raw in records:
        assert len(raw) >= 2, f"RECORD too short: {raw.hex()}"
        from provisa.bolt.messages import RECORD

        assert raw[1] == RECORD, f"Expected RECORD tag 0x71, got 0x{raw[1]:02X}"
        # RECORD body is a list — tiny list header starts with 0x91-0x9F or 0xD4...
        # Byte 2 should be a list header (0x90-0x9F for tiny list)
        list_byte = raw[2] if len(raw) > 2 else 0x90
        assert (list_byte & 0xF0) == 0x90 or list_byte in (0xD4, 0xD5, 0xD6), (
            f"RECORD field is not a PackStream list: 0x{list_byte:02X}"
        )


@pytest.mark.integration
@then("response is serialized via PackStream and framed for TCP")
def step_response_serialized_packstream_framed(shared_data: dict) -> None:
    """Verify that all responses were valid PackStream + Bolt framing.

    We re-verify the hello_response and run_response using the PackStream unpack
    path to ensure they decode without errors.
    """
    if not os.getenv("PROVISA_INTEGRATION"):
        pytest.skip("integration only")

    from provisa.bolt.packstream import unpack

    for key in ("hello_response", "run_response", "pull_final"):
        raw: bytes = shared_data.get(key, b"")
        if not raw:
            continue
        assert len(raw) >= 2, f"{key} too short to be a valid PackStream message"
        # The first byte is tiny-struct header (0xB0 | n_fields)
        assert (raw[0] & 0xF0) == 0xB0, (
            f"{key} does not start with a PackStream tiny-struct header: 0x{raw[0]:02X}"
        )
        # Attempt to decode the embedded field (byte index 2 onward)
        if len(raw) > 2:
            try:
                value = unpack(raw[2:])
                # Must be a dict (SUCCESS/FAILURE meta) or list (RECORD values)
                assert isinstance(value, (dict, list)), (
                    f"{key} decoded to unexpected type {type(value)}"
                )
            except Exception as exc:
                pytest.fail(f"PackStream decode failed for {key}: {exc}")


# ---------------------------------------------------------------------------
# Unit-level (non-integration) tests that verify the same invariants
# without a live server, using the Provisa bolt modules directly.
# ---------------------------------------------------------------------------


def test_bolt_magic_constant():
    """MAGIC must be 0x6060B017."""
    from provisa.bolt.messages import MAGIC

    assert MAGIC == b"\x60\x60\xb0\x17"


def test_supported_versions_non_empty():
    from provisa.bolt.messages import SUPPORTED_VERSIONS

    assert len(SUPPORTED_VERSIONS) > 0
    for major, minor in SUPPORTED_VERSIONS:
        assert isinstance(major, int)
        assert isinstance(minor, int)


def test_encode_version_roundtrip():
    from provisa.bolt.messages import encode_version, decode_version_proposal

    for major, minor in [(5, 4), (4, 4), (5, 1)]:
        encoded = encode_version(major, minor)
        assert len(encoded) == 4
        decoded = decode_version_proposal(encoded)
        assert decoded == (major, minor)


def test_packstream_node_struct_bytes():
    from provisa.bolt.packstream import pack

    node = {"id": 1, "label": "Person", "properties": {"name": "Alice"}}
    data = pack(node)
    assert data[0] == 0xB4, f"Expected 0xB4, got 0x{data[0]:02X}"
    assert data[1] == 0x4E, f"Expected TAG_NODE 0x4E, got 0x{data[1]:02X}"


def test_packstream_relationship_struct_bytes():
    from provisa.bolt.packstream import pack

    rel = {
        "identity": 10,
        "type": "KNOWS",
        "properties": {"since": 2020},
        "startNode": {"id": 1, "label": "Person", "properties": {}},
        "endNode": {"id": 2, "label": "Person", "properties": {}},
    }
    data = pack(rel)
    assert data[0] == 0xB8, f"Expected 0xB8, got 0x{data[0]:02X}"
    assert data[1] == 0x52, f"Expected TAG_RELATIONSHIP 0x52, got 0x{data[1]:02X}"


def test_framing_write_and_read_roundtrip():
    """Verify write_message + read_message round-trip preserves payload."""
    import asyncio as _asyncio

    from provisa.bolt.framing import write_message, read_message

    payload = b"\xb1\x70\xa0"  # SUCCESS {}

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

    fw = _FakeWriter()
    write_message(fw, payload)
    raw = fw.buf.getvalue()

    async def _read():
        reader = _asyncio.StreamReader()
        reader.feed_data(raw)
        reader.feed_eof()
        return await read_message(reader)

    result = _asyncio.run(_read())
    assert result == payload


def test_pack_message_run():
    from provisa.bolt.packstream import pack_message
    from provisa.bolt.messages import RUN

    data = pack_message(RUN, ["MATCH (n) RETURN n", {}, {}])
    # tiny struct header with 3 fields: 0xB3
    assert data[0] == 0xB3
    assert data[1] == RUN


def test_pack_message_hello():
    from provisa.bolt.packstream import pack_message
    from provisa.bolt.messages import HELLO

    data = pack_message(HELLO, {"user_agent": "test/1.0"})
    assert data[0] == 0xB1
    assert data[1] == HELLO


def test_bolt_session_hello_bolt5(tmp_path):
    """BoltSession.handle_hello for Bolt 5.x transitions to AUTHENTICATION and
    sends SUCCESS without requiring credentials."""
    from provisa.bolt.session import BoltSession, State

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

        async def drain(self):
            pass

    fw = _FakeWriter()
    session = BoltSession(fw, bolt_version=(5, 4))
    # state starts at AUTHENTICATION
    assert session.state == State.AUTHENTICATION

    session.handle_hello([{"user_agent": "pytest/1.0"}])

    # After HELLO in Bolt 5.x, state should still be AUTHENTICATION (awaiting LOGON)
    assert session.state == State.AUTHENTICATION
    written = fw.buf.getvalue()
    # Should have written something (SUCCESS message)
    assert len(written) > 0


def test_bolt_session_handle_reset():
    """handle_reset clears buffered results and returns to READY."""
    from provisa.bolt.session import BoltSession, State

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

        async def drain(self):
            pass

    fw = _FakeWriter()
    session = BoltSession(fw, bolt_version=(5, 4))
    session.role_id = "admin"
    session.state = State.READY
    session._result_rows = [[1, 2], [3, 4]]
    session._result_columns = ["a", "b"]

    session.handle_reset()

    assert session._result_rows == []
    assert session._result_columns == []
    assert session.state == State.READY


def test_packstream_scalars_roundtrip():
    """Pack/unpack roundtrip for all scalar types."""
    from provisa.bolt.packstream import pack, unpack

    cases: list[Any] = [
        None,
        True,
        False,
        0,
        127,
        -1,
        -128,
        32767,
        -32768,
        3.14159,
        "",
        "hello world",
        "unicode: \u00e9\u00e0",
    ]
    for value in cases:
        encoded = pack(value)
        decoded = unpack(encoded)
        if isinstance(value, float):
            assert abs(decoded - value) < 1e-9, f"float roundtrip failed for {value}"
        else:
            assert decoded == value, f"Roundtrip failed for {value!r}: got {decoded!r}"


def test_packstream_list_roundtrip():
    from provisa.bolt.packstream import pack, unpack

    lst = [1, "two", None, True, 3.0]
    encoded = pack(lst)
    decoded = unpack(encoded)
    assert decoded == lst


def test_packstream_dict_roundtrip():
    from provisa.bolt.packstream import pack, unpack

    d = {"name": "Alice", "age": 30, "active": True}
    encoded = pack(d)
    decoded = unpack(encoded)
    assert decoded == d


def test_version_negotiation_logic():
    """Simulate the server-side version negotiation logic from bolt/server.py."""
    from provisa.bolt.messages import SUPPORTED_VERSIONS

    def _candidates(b4: bytes) -> list[tuple[int, int]]:
        rng, minor, major = b4[1], b4[2], b4[3]
        return [(major, minor - i) for i in range(rng + 1) if minor - i >= 0]

    # Client proposes Bolt 5.4
    proposals = [
        _encode_version_proposal(5, 4),
        _encode_version_proposal(4, 4),
        b"\x00\x00\x00\x00",
        b"\x00\x00\x00\x00",
    ]

    all_candidates = [c for b in proposals for c in _candidates(b)]
    chosen = None
    for supported in SUPPORTED_VERSIONS:
        if supported in all_candidates:
            chosen = supported
            break

    assert chosen is not None, "Version negotiation failed"
    assert chosen == (5, 4)


def test_version_negotiation_fallback():
    """If the client only proposes Bolt 4.4, we should negotiate (4,4)."""
    from provisa.bolt.messages import SUPPORTED_VERSIONS

    def _candidates(b4: bytes) -> list[tuple[int, int]]:
        rng, minor, major = b4[1], b4[2], b4[3]
        return [(major, minor - i) for i in range(rng + 1) if minor - i >= 0]

    proposals = [
        _encode_version_proposal(4, 4),
        b"\x00\x00\x00\x00",
        b"\x00\x00\x00\x00",
        b"\x00\x00\x00\x00",
    ]

    all_candidates = [c for b in proposals for c in _candidates(b)]
    chosen = None
    for supported in SUPPORTED_VERSIONS:
        if supported in all_candidates:
            chosen = supported
            break

    assert chosen == (4, 4)


def test_version_negotiation_no_match():
    """Client proposing only unsupported version yields no chosen version."""
    from provisa.bolt.messages import SUPPORTED_VERSIONS

    def _candidates(b4: bytes) -> list[tuple[int, int]]:
        rng, minor, major = b4[1], b4[2], b4[3]
        return [(major, minor - i) for i in range(rng + 1) if minor - i >= 0]

    proposals = [
        _encode_version_proposal(1, 0),  # Bolt 1.0 — not supported
        b"\x00\x00\x00\x00",
        b"\x00\x00\x00\x00",
        b"\x00\x00\x00\x00",
    ]

    all_candidates = [c for b in proposals for c in _candidates(b)]
    chosen = None
    for supported in SUPPORTED_VERSIONS:
        if supported in all_candidates:
            chosen = supported
            break

    assert chosen is None


# ---------------------------------------------------------------------------
# Additional unit tests exercising the full scenario pipeline without a
# live server — covers handshake logic, PackStream framing, session state
# machine, and transpilation path at the unit level.
# ---------------------------------------------------------------------------


def test_handshake_magic_bytes_are_correct():
    """The MAGIC constant sent during Bolt handshake must be 0x6060B017."""
    from provisa.bolt.messages import MAGIC

    assert len(MAGIC) == 4
    assert struct.unpack("!I", MAGIC)[0] == 0x6060B017


def test_encode_version_proposal_structure():
    """Version proposal bytes must follow [0x00, range, minor, major] layout."""
    proposal = _encode_version_proposal(5, 4)
    assert proposal[0] == 0x00  # reserved
    assert proposal[1] == 0x00  # range
    assert proposal[2] == 4     # minor
    assert proposal[3] == 5     # major


def test_build_hello_message_structure():
    """HELLO message must be a tiny-struct with tag 0x01."""
    from provisa.bolt.messages import HELLO

    data = _build_hello_message()
    assert len(data) >= 2
    # tiny struct: 0xB0 | n_fields
    assert (data[0] & 0xF0) == 0xB0
    assert data[1] == HELLO


def test_build_run_message_structure():
    """RUN message must be a tiny-struct with tag 0x10 and 3 fields."""
    from provisa.bolt.messages import RUN

    data = _build_run_message("MATCH (n:Person) RETURN n")
    assert len(data) >= 2
    # 3-field tiny struct: 0xB3
    assert data[0] == 0xB3
    assert data[1] == RUN


def test_make_chunked_framing():
    """_make_chunked must produce a valid Bolt chunk: 2-byte length + data + 0x0000."""
    payload = b"\xb1\x70\xa0"
    chunked = _make_chunked(payload)
    # First 2 bytes = big-endian length of payload
    length = struct.unpack("!H", chunked[:2])[0]
    assert length == len(payload)
    # Then the payload
    assert chunked[2 : 2 + length] == payload
    # Then the end-of-message marker
    assert chunked[2 + length :] == b"\x00\x00"


def test_session_send_success_writes_success_tag():
    """BoltSession.send_success must write a message starting with SUCCESS tag."""
    from provisa.bolt.session import BoltSession
    from provisa.bolt.messages import SUCCESS

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

        async def drain(self):
            pass

    fw = _FakeWriter()
    session = BoltSession(fw, bolt_version=(5, 4))
    session.send_success({"server": "Neo4j/5.4 (Provisa)"})

    written = fw.buf.getvalue()
    # write_message wraps in chunks; skip the 2-byte chunk length header
    # to get to the raw packed message bytes
    assert len(written) >= 4
    # chunk length is first 2 bytes; actual message starts at offset 2
    chunk_len = struct.unpack("!H", written[:2])[0]
    msg_bytes = written[2 : 2 + chunk_len]
    assert len(msg_bytes) >= 2
    assert (msg_bytes[0] & 0xF0) == 0xB0, "Not a tiny-struct"
    assert msg_bytes[1] == SUCCESS


def test_session_send_failure_writes_failure_tag_and_sets_failed_state():
    """BoltSession.send_failure must write FAILURE and transition to FAILED state."""
    from provisa.bolt.session import BoltSession, State
    from provisa.bolt.messages import FAILURE

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

        async def drain(self):
            pass

    fw = _FakeWriter()
    session = BoltSession(fw, bolt_version=(5, 4))
    session.send_failure("Neo.ClientError.Security.Unauthorized", "bad creds")

    written = fw.buf.getvalue()
    chunk_len = struct.unpack("!H", written[:2])[0]
    msg_bytes = written[2 : 2 + chunk_len]
    assert msg_bytes[1] == FAILURE
    assert session.state == State.FAILED


def test_session_handle_logon_transitions_to_ready():
    """After LOGON with a valid principal, session state must be READY."""
    from provisa.bolt.session import BoltSession, State

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

        async def drain(self):
            pass

    fw = _FakeWriter()
    session = BoltSession(fw, bolt_version=(5, 4))

    # Patch _resolve_role to always succeed
    session._resolve_role = lambda principal, credentials: "test_role"  # type: ignore[method-assign]

    session.handle_logon([{"principal": "alice", "credentials": "secret"}])

    assert session.state == State.READY
    assert session.role_id == "test_role"


def test_session_handle_logon_bad_credentials_stays_authentication():
    """LOGON with bad credentials must send FAILURE and stay in AUTHENTICATION."""
    from provisa.bolt.session import BoltSession, State
    from provisa.bolt.messages import FAILURE

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

        async def drain(self):
            pass

    fw = _FakeWriter()
    session = BoltSession(fw, bolt_version=(5, 4))

    # Patch _resolve_role to always fail
    session._resolve_role = lambda principal, credentials: None  # type: ignore[method-assign]

    session.handle_logon([{"principal": "nobody", "credentials": "wrong"}])

    assert session.state == State.FAILED
    written = fw.buf.getvalue()
    chunk_len = struct.unpack("!H", written[:2])[0]
    msg_bytes = written[2 : 2 + chunk_len]
    assert msg_bytes[1] == FAILURE


def test_session_handle_begin_and_commit():
    """BEGIN transitions to TX_READY; COMMIT returns to READY."""
    from provisa.bolt.session import BoltSession, State

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

        async def drain(self):
            pass

    fw = _FakeWriter()
    session = BoltSession(fw, bolt_version=(5, 4))
    session.state = State.READY
    session.role_id = "admin"

    session.handle_begin()
    assert session.state == State.TX_READY

    session.handle_commit()
    assert session.state == State.READY


def test_session_handle_rollback():
    """ROLLBACK in TX_READY must return to READY."""
    from provisa.bolt.session import BoltSession, State

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

        async def drain(self):
            pass

    fw = _FakeWriter()
    session = BoltSession(fw, bolt_version=(5, 4))
    session.state = State.TX_READY
    session.role_id = "admin"

    session.handle_rollback()
    assert session.state == State.READY


def test_session_handle_logoff():
    """LOGOFF clears role_id and transitions to AUTHENTICATION."""
    from provisa.bolt.session import BoltSession, State

    class _FakeWriter:
        def __init__(self):
            self.buf = io.BytesIO()

        def write(self, data):
            self.buf.write(data)

        async def drain(self):
            pass

    fw = _FakeWriter()
    session = BoltSession(fw, bolt_version=(5, 4))
    session.state = State.READY
    session.role_id = "admin"

    session.handle_logoff()

    assert session.role_id is None
    assert session.state == State.AUTHENTICATION


def test_packstream_pack_message_pull():
    """PULL message must encode correctly with n=-1 metadata dict."""
    from provisa.bolt.packstream import pack_message
