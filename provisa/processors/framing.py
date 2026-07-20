# Copyright (c) 2026 Kenneth Stott
# Canary: 073e1f9c-4d6a-4b58-8c2f-8e4a3b1d69c5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""NDJSON framing for the processor stream (REQ-940).

NDJSON (one JSON object per line) is the default wire framing: it streams row-by-row with no
whole-batch buffering, is transport-agnostic (shell pipe, HTTP body, gRPC bytes), and is trivially
inspectable. Arrow-stream framing plugs in later for throughput; the contract (contract.py) is
framing-independent.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator


def ndjson_encode(rows: Iterable[dict]) -> Iterator[bytes]:
    """Encode rows as NDJSON lines (one ``{...}\\n`` per row), lazily (REQ-940)."""
    for row in rows:
        yield (json.dumps(row, separators=(",", ":"), default=str) + "\n").encode("utf-8")


def ndjson_decode(lines: Iterable[bytes | str]) -> Iterator[dict]:
    """Decode NDJSON lines back into row dicts, skipping blank lines (REQ-940).

    Fail-loud on a non-object or malformed line — a processor that emits a bare scalar or broken JSON
    is a contract violation, not something to silently drop."""
    for raw in lines:
        text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        text = text.strip()
        if not text:
            continue
        obj = json.loads(text)  # raises json.JSONDecodeError on malformed input — fail loud
        if not isinstance(obj, dict):
            raise ValueError(f"NDJSON line is not a JSON object: {text!r}")
        yield obj
