# Copyright (c) 2026 Kenneth Stott
# Canary: 97636628-2d1e-4f91-a550-2201561ee369
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A value's cell as pgwire sends it (REQ-2194): the value through a row engine's fetch boundary
(``ir_arrow.row_batch``) and the Rust encoder, the cell read back from the DataRow."""

from __future__ import annotations

import struct
from typing import Any

from buenavista.core import wire_type_name

from provisa._encoders import pgwire as encoder
from provisa.core.ir_arrow import row_batch


def cell(bvtype: Any, value: Any, fmt: int = 0, declared: str | None = None) -> bytes | None:
    """The cell ``value`` is sent as, a column of wire type ``bvtype`` in format ``fmt``."""
    batch = row_batch([(value,)], ["value"], [declared])
    message = encoder.encode_batch(batch, [wire_type_name(bvtype)], [fmt])
    (size,) = struct.unpack_from("!i", message, 7)
    return None if size < 0 else message[11 : 11 + size]


def text(bvtype: Any, value: Any, declared: str | None = None) -> str:
    sent = cell(bvtype, value, 0, declared)
    assert sent is not None
    return sent.decode("utf-8")


def binary(bvtype: Any, value: Any, declared: str | None = None) -> bytes:
    sent = cell(bvtype, value, 1, declared)
    assert sent is not None
    return sent


def entry(bvtype: Any) -> tuple[int, Any, Any]:
    """A wire type's (OID, text writer, binary writer), each writer pgwire's own send of a value."""
    from buenavista.postgres import BVTYPE_TO_OID

    return (
        BVTYPE_TO_OID[bvtype],
        lambda value: text(bvtype, value),
        lambda value: binary(bvtype, value),
    )
