# Copyright (c) 2026 Kenneth Stott
# Canary: dc4e6e43-4d84-4e32-bcfd-4f66f18d80c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Batched join keys: a code table whose function takes a ``loader`` (REQ-2100).

A join that feeds a code table's parameters gives it one argument set per distinct set of the
joining rows' values (REQ-2099). A function that takes a parameter named ``loader`` is called with
them as keys instead, the pattern GraphQL's DataLoader made familiar:

- each set is a SPARSE key: only the parameters it gives a value, so an absent one takes the
  function's own default, as on a single call;
- a key is named by its hash, the SHA-256 of its canonical JSON (keys sorted, compact separators,
  each value as the code-source wire carries it), so the same values always give the same key and
  de-duplication is the hash itself;
- ``loader`` is a hash of the keys, ``{hash: key}``, the function's other parameters left absent;
  with a declared batch size (``loader N``) the keys go in chunks of it, one call each;
- the function answers with the same shape, ``{hash: [rows]}``, its host sending each row with its
  key (``$key``); a key the batch did not send is refused by name;
- each joining argument set is given the rows of its own key, carrying its own values for the
  key's parameters, so the engine's join meets them on the parent's values and not on a default
  the function filled in.
"""

# Requirements: REQ-2100
from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any

#: The field a loader call's row carries its key in (provisa/python/host.py, host-core.mjs).
KEY = "$key"


def sparse_key(values: dict[str, Any]) -> dict[str, Any]:
    """A set's key: the parameters it gives a value, by name; one left out takes its default."""
    return {name: value for name, value in values.items() if value is not None}


def key_hash(key: dict[str, Any]) -> str:
    """The key's name: the SHA-256 hex of its canonical JSON, each value as the wire carries it."""
    from provisa.codesource.worker import encode

    text = json.dumps(encode(key), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


def chunks(keys: dict[str, dict], size: int | None) -> list[dict[str, dict]]:
    """The keys in batches of ``size`` (every key in one when no size is declared)."""
    items = list(keys.items())
    if size is None or not items:
        return [dict(items)] if items else []
    return [dict(items[at : at + size]) for at in range(0, len(items), size)]


def assign(
    table: str,
    sets: Sequence[dict[str, Any]],
    hashes: Sequence[str],
    answered: Sequence[dict],
    sent: set[str],
) -> list[dict]:
    """Each joining set's rows: the rows answered under its key, carrying the set's own values for
    the key's parameters. A row under a key the batch did not send is refused by name."""
    from provisa.codesource.table import CodeTableRefused

    by_key: dict[str, list[dict]] = {}
    for row in answered:
        key = row.get(KEY)
        if key not in sent:
            raise CodeTableRefused(
                table, f"its loader answered with key {key!r}, which the batch did not send"
            )
        by_key.setdefault(key, []).append({k: v for k, v in row.items() if k != KEY})
    out: list[dict] = []
    for values, key in zip(sets, hashes, strict=True):
        for row in by_key.get(key, []):
            out.append({**row, **{k: v for k, v in values.items() if k in row}})
    return out
