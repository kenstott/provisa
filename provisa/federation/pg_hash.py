# Copyright (c) 2026 Kenneth Stott
# Canary: a4d7e0b2-19c6-4f3e-8e72-5bc3f9016d4a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL's hash of bytes and of a numeric, as src/common/hashfn.c and numeric.c compute them
on a little-endian server (REQ-2171): a hash a client compares has to be PostgreSQL's value."""

# Requirements: REQ-2171

from __future__ import annotations

from decimal import Decimal

_MASK = 0xFFFFFFFF
_START = 0x9E3779B9 + 3923095


def _rot(value: int, bits: int) -> int:
    return ((value << bits) | (value >> (32 - bits))) & _MASK


def _mix(a: int, b: int, c: int) -> tuple[int, int, int]:
    steps = ((0, 2, 1, 4), (1, 0, 2, 6), (2, 1, 0, 8), (0, 2, 1, 16), (1, 0, 2, 19), (2, 1, 0, 4))
    for x, y, z, bits in steps:
        state = [a, b, c]
        state[x] = ((state[x] - state[y]) & _MASK) ^ _rot(state[y], bits)
        state[y] = (state[y] + state[z]) & _MASK
        a, b, c = state
    return a, b, c


def _final(a: int, b: int, c: int) -> tuple[int, int, int]:
    c = ((c ^ b) - _rot(b, 14)) & _MASK
    a = ((a ^ c) - _rot(c, 11)) & _MASK
    b = ((b ^ a) - _rot(a, 25)) & _MASK
    c = ((c ^ b) - _rot(b, 16)) & _MASK
    a = ((a ^ c) - _rot(c, 4)) & _MASK
    b = ((b ^ a) - _rot(a, 14)) & _MASK
    c = ((c ^ b) - _rot(b, 24)) & _MASK
    return a, b, c


def _seeded(length: int, seed: int) -> tuple[int, int, int]:
    a = b = c = (_START + length) & _MASK
    if seed:
        a = (a + (seed >> 32)) & _MASK
        b = (b + (seed & _MASK)) & _MASK
        a, b, c = _mix(a, b, c)
    return a, b, c


def _absorbed(data: bytes, a: int, b: int, c: int) -> tuple[int, int, int]:
    position = 0
    while len(data) - position >= 12:
        a = (a + int.from_bytes(data[position : position + 4], "little")) & _MASK
        b = (b + int.from_bytes(data[position + 4 : position + 8], "little")) & _MASK
        c = (c + int.from_bytes(data[position + 8 : position + 12], "little")) & _MASK
        a, b, c = _mix(a, b, c)
        position += 12
    tail = data[position:]
    a = (a + int.from_bytes(tail[:4], "little")) & _MASK
    b = (b + int.from_bytes(tail[4:8], "little")) & _MASK
    # the lowest byte of c is reserved for the length
    c = (c + (int.from_bytes(tail[8:11], "little") << 8)) & _MASK
    return _final(a, b, c)


def hash_bytes(data: bytes) -> int:
    """hash_any."""
    a = b = c = (_START + len(data)) & _MASK
    return _absorbed(data, a, b, c)[2]


def hash_bytes_extended(data: bytes, seed: int) -> int:
    """hash_any_extended."""
    _, b, c = _absorbed(data, *_seeded(len(data), seed))
    return (b << 32) | c


def hash_uint32_extended(value: int, seed: int) -> int:
    a, b, c = _seeded(4, seed)
    _, b, c = _final((a + value) & _MASK, b, c)
    return (b << 32) | c


def _base_10000(value: Decimal) -> tuple[list[int], int]:
    """A numeric's digits in base 10000, no zeros at either end, and its first digit's weight."""
    _, digits, exponent = value.as_tuple()
    assert isinstance(exponent, int)  # a jsonb number is finite
    number = int("".join(map(str, digits)) or "0")
    pad = exponent % 4  # a base-10000 digit starts at a multiple of four decimal places
    number *= 10**pad
    low = (exponent - pad) // 4
    groups: list[int] = []
    while number:
        number, group = divmod(number, 10000)
        groups.append(group)
    position = 0
    while position < len(groups) and groups[position] == 0:
        position += 1
    kept = groups[position:]
    return kept[::-1], low + position + len(kept) - 1


def _digit_bytes(digits: list[int]) -> bytes:
    return b"".join(digit.to_bytes(2, "little") for digit in digits)


def hash_numeric(value: Decimal) -> int:
    """hash_numeric: of a value's digits and weight, whatever its sign or display scale."""
    digits, weight = _base_10000(value)
    if not digits:
        return _MASK
    return hash_bytes(_digit_bytes(digits)) ^ (weight & _MASK)


def hash_numeric_extended(value: Decimal, seed: int) -> int:
    digits, weight = _base_10000(value)
    if not digits:
        return (seed - 1) & (2**64 - 1)
    return hash_bytes_extended(_digit_bytes(digits), seed) ^ (weight & (2**64 - 1))
