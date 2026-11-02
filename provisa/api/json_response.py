# Copyright (c) 2026 Kenneth Stott
# Canary: 5a1515aa-92fd-4aa1-ae7a-20eea9738688
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The app's JSON response class (REQ-1867): orjson encodes the body.

A data route returns this response directly, built from its rows. FastAPI otherwise runs
``jsonable_encoder`` over a returned dict before any response class sees it: a full Python walk of
every row that costs about 25x the orjson encode itself (7.9 ms against 0.3 ms for 1,000 rows).
orjson encodes the types it knows (str, numbers, bool, None, lists, dicts, datetimes, UUIDs,
enums, dataclasses) natively, and hands only the rest -- a Decimal, a timedelta, a Pydantic model,
a set -- to ``jsonable_encoder``, so the body is what jsonable_encoder would have produced.

Values are exact (REQ-1867, amended 2026-11-02): a Decimal is the string of its exact digits,
PostgreSQL's infinite date or timestamp is ``"infinity"`` / ``"-infinity"``, and an integer past
64 bits is written with all its digits. What JSON cannot carry -- nesting past orjson's depth, a
lone surrogate -- is refused by name (``JsonResponseRefused``), never a 500.
"""

# Requirements: REQ-1867

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from typing import Any

import orjson
from dateutil.relativedelta import relativedelta
from fastapi.responses import JSONResponse

from provisa.core.ir_types import infinity_of, iso8601_duration
from provisa.core.json_refusal import JsonResponseRefused

_OPTIONS = orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY
#: the integers orjson writes itself; any other is written by its digits (_exact_ints)
_INT_MIN, _INT_MAX = -(2**63), 2**64 - 1


def _encode_unknown(value: Any) -> Any:  # noqa: ANN401 — whatever orjson cannot encode itself
    """A value orjson has no encoding for, as jsonable_encoder would give it."""
    from fastapi.encoders import jsonable_encoder

    # orjson encodes float itself and no subclass of it, and jsonable_encoder hands a float
    # subclass back as it is: JPype's boxed java.lang.Double, in a govdata row.
    if isinstance(value, float):
        return float(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        # Binary as PostgreSQL writes it (\x and hex), the spelling a JSON read of a bytea column
        # gives (transpiler.native_json): jsonable_encoder decodes bytes as UTF-8, and refused a
        # BLOB/RAW value that is not text.
        return "\\x" + bytes(value).hex()
    if isinstance(value, (timedelta, relativedelta)):
        # An interval as its ISO 8601 duration, as on every surface (ir_types.iso8601_duration):
        # jsonable_encoder writes a timedelta as its total seconds, and a relativedelta not at all.
        return iso8601_duration(value)
    if isinstance(value, Decimal):
        # Its exact digits, as the serializer writes a DECIMAL (serialize.decimal_text):
        # jsonable_encoder makes a float of it.
        from provisa.executor.serialize import decimal_text

        return decimal_text(value)
    sign = infinity_of(value)
    if sign:
        # PostgreSQL's infinite date or timestamp (ir_types.infinite_date), as pgwire writes it:
        # jsonable_encoder writes the finite edge date the IR holds it as.
        return "infinity" if sign > 0 else "-infinity"
    return jsonable_encoder(value)


def _exact_ints(content: Any) -> Any:  # noqa: ANN401 — any body a route returns
    """``content`` with each integer orjson cannot write as a Fragment of its own digits."""
    if isinstance(content, dict):
        return {k: _exact_ints(v) for k, v in content.items()}
    if isinstance(content, (list, tuple)):
        return [_exact_ints(v) for v in content]
    if isinstance(content, int) and not isinstance(content, bool):
        if not _INT_MIN <= content <= _INT_MAX:
            return orjson.Fragment(str(content))
    return content


def dumps(content: Any) -> bytes:  # noqa: ANN401 — any body a route returns
    """``content`` as JSON bytes: orjson for what it knows, jsonable_encoder for the rest. An
    integer past 64 bits is written with all its digits; a body JSON cannot carry is refused by
    name (JsonResponseRefused)."""
    try:
        return orjson.dumps(content, default=_encode_unknown, option=_OPTIONS)
    except orjson.JSONEncodeError as exc:
        reason = str(exc)
        if reason == "Integer exceeds 64-bit range":
            # orjson writes no integer past 64 bits and has no hook for one: the body is encoded
            # again with each such integer as a Fragment of its digits -- the same JSON number.
            return orjson.dumps(_exact_ints(content), default=_encode_unknown, option=_OPTIONS)
        if reason == "Recursion limit reached":
            raise JsonResponseRefused(
                "nests deeper than the 255 levels a JSON response is written to"
            ) from exc
        if "surrogates not allowed" in reason:
            raise JsonResponseRefused(
                "holds a lone UTF-16 surrogate, which is no character UTF-8 can carry"
            ) from exc
        raise


class OrjsonResponse(JSONResponse):
    """A JSON response whose body orjson encodes (see :func:`dumps`)."""

    def render(self, content: Any) -> bytes:  # noqa: ANN401 — any body a route returns
        return dumps(content)
