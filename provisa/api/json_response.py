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
"""

# Requirements: REQ-1867

from __future__ import annotations

from typing import Any

import orjson
from fastapi.responses import JSONResponse


def _encode_unknown(value: Any) -> Any:  # noqa: ANN401 — whatever orjson cannot encode itself
    """A value orjson has no encoding for, as jsonable_encoder would give it."""
    from fastapi.encoders import jsonable_encoder

    # orjson encodes float itself and no subclass of it, and jsonable_encoder hands a float
    # subclass back as it is: JPype's boxed java.lang.Double, in a govdata row.
    if isinstance(value, float):
        return float(value)
    return jsonable_encoder(value)


def dumps(content: Any) -> bytes:  # noqa: ANN401 — any body a route returns
    """``content`` as JSON bytes: orjson for what it knows, jsonable_encoder for the rest."""
    return orjson.dumps(
        content,
        default=_encode_unknown,
        option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY,
    )


class OrjsonResponse(JSONResponse):
    """A JSON response whose body orjson encodes (see :func:`dumps`)."""

    def render(self, content: Any) -> bytes:  # noqa: ANN401 — any body a route returns
        return dumps(content)
