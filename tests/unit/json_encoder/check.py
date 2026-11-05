# Copyright (c) 2026 Kenneth Stott
# Canary: 4f6a1c2e-7b3d-4e9a-a5c8-0d2e1f3b6a97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The differential check shared by the tests: both bodies parsed and compared structurally (key
order, JSON types, every value exactly; floats within a relative tolerance of 1e-14), byte
identity tallied; when the reference raises, the encoder path raises the same: a refusal by its
class and what it names, any other error as the encoder's EncoderFailed (a 500)."""

from __future__ import annotations

import json
import math
from collections import Counter

from provisa.api.odata.errors import QueryOptionRefused
from provisa.core.json_refusal import JsonResponseRefused, JsonValueRefused
from provisa.executor.json_encoders import EncoderFailed

REL_TOL = 1e-14
TALLY: Counter = Counter()


class Obj(list):
    """A parsed JSON object: its (key, value) pairs in order."""


def parse(b: bytes):
    return json.loads(b, object_pairs_hook=Obj)


def same(a, b, path="$"):
    if isinstance(a, float) or isinstance(b, float):
        assert type(a) is type(b), f"{path}: {a!r} vs {b!r}"
        assert math.isclose(a, b, rel_tol=REL_TOL, abs_tol=0.0), f"{path}: {a!r} vs {b!r}"
        return
    assert type(a) is type(b), f"{path}: {type(a).__name__} {a!r} vs {type(b).__name__} {b!r}"
    if isinstance(a, Obj):
        assert [k for k, _ in a] == [k for k, _ in b], f"{path}: keys {a!r} vs {b!r}"
        for (k, x), (_, y) in zip(a, b):
            same(x, y, f"{path}.{k}")
    elif isinstance(a, list):
        assert len(a) == len(b), f"{path}: length {len(a)} vs {len(b)}"
        for i, (x, y) in enumerate(zip(a, b)):
            same(x, y, f"{path}[{i}]")
    else:
        assert a == b, f"{path}: {a!r} vs {b!r}"


NOT_JSON = "is typed JSON but its text is not JSON ("


def same_error(p: BaseException, r: BaseException) -> None:
    if isinstance(p, JsonValueRefused):
        assert isinstance(r, JsonValueRefused), f"python {p!r}; rust {r!r}"
        assert r.params["field"] == p.params["field"], f"python {p!r}; rust {r!r}"
        if p.params["reason"].startswith(NOT_JSON):
            assert r.params["reason"].startswith(NOT_JSON), f"python {p!r}; rust {r!r}"
        else:
            assert r.params["reason"] == p.params["reason"], f"python {p!r}; rust {r!r}"
    elif isinstance(p, JsonResponseRefused):
        assert isinstance(r, JsonResponseRefused), f"python {p!r}; rust {r!r}"
        assert r.params["reason"] == p.params["reason"], f"python {p!r}; rust {r!r}"
    elif isinstance(p, QueryOptionRefused):
        assert isinstance(r, QueryOptionRefused), f"python {p!r}; rust {r!r}"
        assert (r.status, r.code, str(r)) == (p.status, p.code, str(p)), f"python {p!r}; rust {r!r}"
    else:
        assert type(r) is EncoderFailed, f"python {p!r}; rust {r!r}"


def compare(surface: str, python, rust) -> str:
    """Run both; assert they agree. Returns 'ok' or 'raise'."""
    p_err = r_err = None
    try:
        p = python()
    except Exception as e:  # noqa: BLE001 — any failure of the product's path
        p_err = e
    try:
        r = rust()
    except Exception as e:  # noqa: BLE001
        r_err = e
    TALLY[f"{surface}.cases"] += 1
    if p_err is not None or r_err is not None:
        assert p_err is not None and r_err is not None, f"python: {p_err!r}; rust: {r_err!r}"
        same_error(p_err, r_err)
        TALLY[f"{surface}.both_raise"] += 1
        TALLY[f"{surface}.raise.{type(p_err).__name__}"] += 1
        return "raise"
    if surface.endswith("csv"):
        assert p == r, f"python {p[:300]!r}; rust {r[:300]!r}"
    else:
        same(parse(p), parse(r))
    TALLY[f"{surface}.byte_identical"] += p == r
    return "ok"
