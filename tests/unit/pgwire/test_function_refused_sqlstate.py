# Copyright (c) 2026 Kenneth Stott
# Canary: 3f8b2d61-9c47-4e05-a1d3-7e6c0b5f2a94
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A translator refusal reaches the pgwire client as PostgreSQL reports a feature it lacks:
SQLSTATE 0A000 (feature_not_supported) with the refusal's message, and a grid row's own SQLSTATE
where the row states one. FunctionRefused read as XX000 before (polyfill triage 2026-11-22, G3)."""

# Requirements: REQ-2236

from __future__ import annotations

import struct
from typing import Any, cast

import pytest

from provisa.pgwire.server import ProvisaHandler
from provisa.transpiler.function_macros import FunctionRefused, RowRefused
from provisa.transpiler.pg_xml import XmlFunctionRefused
from provisa.transpiler.polyfill.notation import Refusal
from provisa.transpiler.transpile import transpile


def _fields(exc: BaseException) -> dict[str, str]:
    written: list[bytes] = []

    class _W:
        def write(self, data):
            written.append(bytes(data))

        def flush(self):
            pass

    handler = ProvisaHandler.__new__(ProvisaHandler)
    handler.wfile = cast(Any, _W())
    handler.send_error(exc)
    blob = b"".join(written)
    assert blob[:1] == b"E"
    (length,) = struct.unpack("!i", blob[1:5])
    body = blob[5 : 1 + length]
    fields: dict[str, str] = {}
    for part in body.rstrip(b"\x00").split(b"\x00"):
        fields[part[:1].decode()] = part[1:].decode()
    return fields


@pytest.mark.parametrize("dialect", ["trino", "clickhouse"])
def test_a_refused_function_reaches_the_client_as_0a000(dialect: str) -> None:
    with pytest.raises(FunctionRefused) as caught:
        transpile("SELECT convert(b, 'UTF8', 'LATIN1') FROM t", dialect)
    fields = _fields(caught.value)
    assert fields["C"] == "0A000"
    assert fields["M"] == f"convert() has no form on {dialect}"


def test_a_refusal_the_pipeline_re_raises_keeps_0a000() -> None:
    try:
        try:
            raise FunctionRefused("area", "tsql")
        except FunctionRefused as cause:
            raise RuntimeError("area() has no form on tsql") from cause
    except RuntimeError as wrapped:
        fields = _fields(wrapped)
    assert (fields["C"], fields["M"]) == ("0A000", "area() has no form on tsql")


def test_the_row_stated_sqlstate_wins() -> None:
    refused = RowRefused("like_escape", "clickhouse", Refusal("22025", "invalid escape string"))
    fields = _fields(refused)
    assert (fields["C"], fields["M"]) == ("22025", "invalid escape string")
    # the class's own code is untouched by a row's
    assert FunctionRefused("area", "tsql").sqlstate == "0A000"


@pytest.mark.parametrize("dialect", ["trino", "mysql"])
def test_a_refused_xml_function_reaches_the_client_as_0a000(dialect: str) -> None:
    with pytest.raises(XmlFunctionRefused) as caught:
        transpile("SELECT xpath('/a', x) FROM t", dialect)
    fields = _fields(caught.value)
    assert (fields["C"], fields["M"]) == (
        "0A000",
        f"xpath needs XML, which {dialect} has no SQL for",
    )
