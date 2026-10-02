# Copyright (c) 2026 Kenneth Stott
# Canary: 56c2c899-7ac5-496e-86c7-8c62476b9a01
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Canonical content hash for the event-loop output gate (REQ-981).

A node hashes its landed replace-shaped content to a stable digest; the base loop compares it to the
prior land's digest and suppresses the downstream re-post when they match. The hash MUST be
deterministic across fetches — same rows, same digest — or the gate silently defeats itself (a
non-canonical hash always differs → always ripples). Determinism requires: an order-independent row
set (sort by the primary key, else by the row's own canonical form), stable key order within a row,
and a canonical encoding of values whose default repr is not stable (datetime, Decimal, bytes, float).
"""

from __future__ import annotations

import datetime as _dt
import decimal
import hashlib
import json
from typing import Any


def _canonical(value: Any) -> Any:
    """Coerce a value to a JSON-encodable, stable form. Types whose ``repr``/JSON encoding is not
    deterministic across fetches are pinned here; everything else passes through."""
    if isinstance(value, _dt.datetime):
        # Normalise to UTC when tz-aware so an aware/naive-equal instant hashes identically.
        if value.tzinfo is not None:
            value = value.astimezone(_dt.timezone.utc)
        return value.isoformat()
    if isinstance(value, (_dt.date, _dt.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        # Normalised so 1.0 and 1.00 collapse to one canonical string.
        return format(value.normalize(), "f")
    if isinstance(value, float):
        # repr(float) is round-trippable and stable across platforms for the same value.
        return repr(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, dict):
        return {str(k): _canonical(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(v) for v in value]
    return value


def _row_key(row: dict, pk_columns: list[str] | None) -> Any:
    """The sort key for a row: its primary-key tuple when available (the identity), else its full
    canonical serialization (a stable total order over arbitrary rows without a declared key)."""
    if pk_columns:
        return tuple(_canonical(row.get(c)) for c in pk_columns)
    return json.dumps(_canonical(row), sort_keys=True, separators=(",", ":"))


def content_hash(rows: list[dict], pk_columns: list[str] | None = None) -> str:
    """A stable sha256 over ``rows``, independent of fetch order (REQ-981).

    Rows are sorted by ``pk_columns`` (their identity) when given, else by their canonical form, then
    each is canonically encoded and length-prefixed into the digest. Returns the hex digest; an empty
    set hashes to the digest of the empty stream (a real value, distinct from "no hash yet")."""
    ordered = sorted(rows, key=lambda r: _row_key(r, pk_columns))
    h = hashlib.sha256()
    for row in ordered:
        encoded = json.dumps(_canonical(row), sort_keys=True, separators=(",", ":")).encode("utf-8")
        # Length-prefix so row boundaries can't be forged by concatenation ambiguity.
        h.update(str(len(encoded)).encode("ascii"))
        h.update(b":")
        h.update(encoded)
    return h.hexdigest()


class RowSetHash:
    """The content hash of a row set that arrives in batches (REQ-981, REQ-1915).

    :func:`content_hash` sorts the whole set, so it needs every row at once. This is the same
    kind of digest for a stream: each row's own sha256, combined by addition, together with the
    row count. Addition does not depend on order, so the same rows hash alike however the
    source returns them, and only the running sum is held. The two digests are not comparable
    with each other."""

    _MODULUS = 1 << 256

    def __init__(self) -> None:
        self._sum = 0
        self._count = 0

    def update(self, rows: list[dict]) -> None:
        """Add a batch of rows."""
        for row in rows:
            encoded = json.dumps(_canonical(row), sort_keys=True, separators=(",", ":")).encode()
            digest = int.from_bytes(hashlib.sha256(encoded).digest(), "big")
            self._sum = (self._sum + digest) % self._MODULUS
        self._count += len(rows)

    def hexdigest(self) -> str:
        """The digest so far: the row count and the combined row hashes."""
        return f"{self._count:x}:{self._sum:064x}"
