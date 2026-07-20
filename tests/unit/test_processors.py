# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7c5d3a-8102-4f9c-8063-2c8e7f5a03b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-940: external processor transport toolkit.

Covers the one streaming contract (schema validation both ends, fail-loud), NDJSON framing, and the
shell/HTTP/gRPC transport adapters — each conforming to the identical rows-in→rows-out contract.
"""

from __future__ import annotations

import sys

import pytest

from provisa.processors import (
    GrpcAdapter,
    HttpAdapter,
    ProcessorError,
    Schema,
    SchemaViolation,
    ShellAdapter,
    ndjson_decode,
    ndjson_encode,
    validate_rows,
)

SCHEMA = Schema.of(("id", "Int"), ("name", "String"))


# ------------------------------------------------------- framing


def test_ndjson_round_trip():
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    encoded = b"".join(ndjson_encode(rows))
    assert encoded.count(b"\n") == 2
    assert list(ndjson_decode(encoded.splitlines())) == rows


def test_ndjson_decode_skips_blank_lines():
    assert list(ndjson_decode([b'{"id":1,"name":"a"}', b"", b"  "])) == [{"id": 1, "name": "a"}]


def test_ndjson_decode_rejects_non_object():
    with pytest.raises(ValueError, match="not a JSON object"):
        list(ndjson_decode([b"[1,2,3]"]))


# ------------------------------------------------------- contract / schema validation


def test_validate_rows_passes_conforming():
    rows = [{"id": 1, "name": "a"}]
    assert list(validate_rows(rows, SCHEMA, where="t")) == rows


def test_validate_rejects_unknown_field():
    with pytest.raises(SchemaViolation, match="unexpected field"):
        list(validate_rows([{"id": 1, "name": "a", "extra": 9}], SCHEMA, where="t"))


def test_validate_rejects_missing_field():
    with pytest.raises(SchemaViolation, match="missing field"):
        list(validate_rows([{"id": 1}], SCHEMA, where="t"))


def test_validate_rejects_wrong_type():
    with pytest.raises(SchemaViolation, match="expected Int"):
        list(validate_rows([{"id": "notint", "name": "a"}], SCHEMA, where="t"))


def test_validate_rejects_bool_for_int():
    # bool is an int subclass — must not pass an Int column
    with pytest.raises(SchemaViolation, match="expected Int"):
        list(validate_rows([{"id": True, "name": "a"}], SCHEMA, where="t"))


# ------------------------------------------------------- shell adapter


def test_shell_adapter_transforms_rows():
    # a pure transform: read NDJSON rows, uppercase name, write NDJSON rows
    script = (
        "import sys, json\n"
        "for line in sys.stdin:\n"
        "    line = line.strip()\n"
        "    if not line: continue\n"
        "    r = json.loads(line)\n"
        "    r['name'] = r['name'].upper()\n"
        "    sys.stdout.write(json.dumps(r) + '\\n')\n"
    )
    adapter = ShellAdapter([sys.executable, "-c", script])
    out = list(adapter.process([{"id": 1, "name": "ada"}], schema_in=SCHEMA, schema_out=SCHEMA))
    assert out == [{"id": 1, "name": "ADA"}]


def test_shell_adapter_nonzero_exit_raises():
    adapter = ShellAdapter([sys.executable, "-c", "import sys; sys.exit(3)"])
    with pytest.raises(ProcessorError, match="exited 3"):
        list(adapter.process([{"id": 1, "name": "a"}], schema_in=SCHEMA, schema_out=SCHEMA))


def test_shell_adapter_output_schema_enforced():
    # processor emits an off-schema field → output validation fails loud
    script = "import sys, json\nfor l in sys.stdin:\n    sys.stdout.write('{\"bad\":1}\\n')\n"
    adapter = ShellAdapter([sys.executable, "-c", script])
    with pytest.raises(SchemaViolation):
        list(adapter.process([{"id": 1, "name": "a"}], schema_in=SCHEMA, schema_out=SCHEMA))


def test_shell_adapter_empty_argv_rejected():
    with pytest.raises(ValueError, match="non-empty argv"):
        ShellAdapter([])


# ------------------------------------------------------- http adapter


def test_http_adapter_with_injected_poster():
    captured = {}

    def poster(url, body):
        captured["url"] = url
        # echo back the rows with name lowercased
        rows = list(ndjson_decode(body.splitlines()))
        for r in rows:
            r["name"] = r["name"].lower()
        return b"".join(ndjson_encode(rows))

    adapter = HttpAdapter("https://proc/transform", poster=poster)
    out = list(adapter.process([{"id": 1, "name": "ADA"}], schema_in=SCHEMA, schema_out=SCHEMA))
    assert out == [{"id": 1, "name": "ada"}]
    assert captured["url"] == "https://proc/transform"


def test_http_adapter_input_validation_fails_before_send():
    sent = False

    def poster(url, body):
        nonlocal sent
        sent = True
        return b""

    adapter = HttpAdapter("https://proc", poster=poster)
    with pytest.raises(SchemaViolation):
        list(adapter.process([{"id": "bad"}], schema_in=SCHEMA, schema_out=SCHEMA))
    assert not sent  # invalid input never reaches the transport


# ------------------------------------------------------- grpc adapter


def test_grpc_adapter_with_fake_bidi_stream():
    def bidi(request_chunks):
        # a conforming echo processor: decode, passthrough, re-encode
        rows = list(ndjson_decode(request_chunks))
        return ndjson_encode(rows)

    adapter = GrpcAdapter(bidi)
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert list(adapter.process(rows, schema_in=SCHEMA, schema_out=SCHEMA)) == rows


def test_all_adapters_share_one_contract():
    # the three transports are interchangeable behind TransportAdapter.process
    from provisa.processors.contract import TransportAdapter

    def bidi(chunks):
        return ndjson_encode(ndjson_decode(chunks))

    adapters = [
        ShellAdapter([sys.executable, "-c", "import sys;sys.stdout.write(sys.stdin.read())"]),
        HttpAdapter("https://x", poster=lambda u, b: b),
        GrpcAdapter(bidi),
    ]
    rows = [{"id": 7, "name": "z"}]
    for a in adapters:
        assert isinstance(a, TransportAdapter)
        assert list(a.process(rows, schema_in=SCHEMA, schema_out=SCHEMA)) == rows
