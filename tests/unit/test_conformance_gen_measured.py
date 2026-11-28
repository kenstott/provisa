"""measured.py's tables written from a Conformance (functions) run (REQ-2236): a row that refuses
only a NULL argument is not said to refuse every call, though the run records no sample of it."""

# Requirements: REQ-2236

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from provisa.transpiler import polyfill

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "conformance"))

import gen_measured  # noqa: E402

_NULL_CALL = 'SELECT pg_catalog."encode"(NULL::bytea, NULL::text) AS v'
# A run that records only encode's NULL call on trino: every engine answered its samples.
_ENCODE = {
    "function": "encode",
    "overload": "bytea, text",
    "family": "String",
    "kind": "f",
    "volatility": "i",
    "outcomes": {
        "trino": {
            "outcome": "refused",
            "detail": {
                "statement": _NULL_CALL,
                "error": "ERROR:  encode() has no form on trino",
                "sqlstate": "0A000",
            },
        },
        "clickhouse": {"outcome": "pass"},
    },
}


@pytest.fixture
def families_grid(monkeypatch: pytest.MonkeyPatch) -> polyfill.Grid:
    grid = gen_measured.families_grid()
    monkeypatch.setattr(polyfill, "GRID", grid)
    return grid


def test_the_samples_are_read_though_no_outcome_records_them():
    samples = gen_measured._samples(_ENCODE)
    assert samples
    assert all(s.startswith('SELECT pg_catalog."encode"(') and "NULL::" not in s for s in samples)


def test_a_row_refusing_only_a_null_format_keeps_its_form(families_grid: polyfill.Grid):
    refused, limited = gen_measured.tables(
        gen_measured.evidence({"functions": [_ENCODE]}, families_grid, frozenset())
    )
    rule = ("encode(bytea, text)", "encode($a, $f)")
    assert all("trino" not in dialects.split() for _, dialects, _ in refused.get(rule, ()))
    trino = [e for e in limited[rule] if "trino" in e[2].split()]
    assert trino and "not a NULL" in trino[0][3]


def test_a_variadic_any_is_one_argument():
    entry = {**_ENCODE, "function": "json_build_array", "overload": 'VARIADIC "any"'}
    assert all("VARIADIC" not in s for s in gen_measured._samples(entry))
