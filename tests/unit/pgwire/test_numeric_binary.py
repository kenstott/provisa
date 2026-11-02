# Copyright (c) 2026 Kenneth Stott
# Canary: 764fac7a-2ed1-464c-8dc6-fdb7d2c34aa0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""pgwire's binary numeric (OID 1700) keeps every digit of a value, as PostgreSQL 17 sends it
(REQ-2167): no decimal context rounds a value past 28 significant digits."""

from __future__ import annotations

import struct
from decimal import Decimal, localcontext

import pytest

from buenavista.core import BVType
from buenavista.postgres import BVTYPE_TO_PGTYPE


def _decode(data: bytes) -> Decimal:
    ndigits, weight, sign, dscale = struct.unpack("!HhHH", data[:8])
    digits = struct.unpack(f"!{ndigits}H", data[8:])
    with localcontext(prec=1000):
        value = sum(Decimal(d) * Decimal(10000) ** (weight - i) for i, d in enumerate(digits))
        value = -value if sign == 0x4000 else value
        return value.quantize(Decimal(1).scaleb(-dscale))


@pytest.mark.parametrize(
    "value",
    [
        Decimal("1234567890123456789012345678.9999999999"),
        Decimal("-1234567890123456789012345678.9999999999"),
        Decimal("9999999999999999999999999999.9999999999"),
        Decimal("0.0000000001"),
    ],
)
def test_a_binary_decimal_38_10_decodes_back_exactly(value):
    _, to_text, to_binary = BVTYPE_TO_PGTYPE[BVType.DECIMAL]
    data = to_binary(value)
    assert struct.unpack("!h", data[6:8]) == (10,)
    decoded = _decode(data)
    assert decoded == value
    assert str(decoded) == to_text(value)
