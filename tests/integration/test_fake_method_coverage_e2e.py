# Copyright (c) 2026 Kenneth Stott
# Canary: 7486b2bf-37dc-4d6e-a10d-22417a53b560
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every fake method a column may name is computed by the Trino engine (REQ-1494): each method of
the Python side is either computed there for a range of values, or on the named list of methods
no column can declare -- refused when declared, on every engine."""

from __future__ import annotations

import pytest

from provisa.fakes.kinds import FakeRefused, Method
from provisa.fakes.methods import UNSUPPORTED, check_method, method_names

pytestmark = [pytest.mark.integration]


def test_every_method_is_computed_by_trino_or_named_unsupported(trino_conn):
    declarable = [m for m in method_names() if m not in UNSUPPORTED]
    assert UNSUPPORTED <= set(method_names())
    values = ", ".join(f"('{m}')" for m in declarable)
    cur = trino_conn.cursor()
    cur.execute(
        "SELECT m, d, provisa_fake_method(m, '{}', d, 5) FROM (VALUES "
        + values
        + ") AS t(m) CROSS JOIN UNNEST(sequence(1, 5)) AS s(d)"
    )
    rows = cur.fetchall()
    computed = {m for m, _, v in rows if v is not None}
    # null_boolean makes NULL a third of the time; every other method always makes a value.
    assert set(declarable) - computed == set(), sorted(set(declarable) - computed)
    assert len(rows) == 5 * len(declarable)


def test_every_method_is_a_function_of_the_digest_alone_on_trino(trino_conn):
    """REQ-1494 (determinism): the same digest gives the same value at two reads a second apart."""
    import time

    declarable = [m for m in method_names() if m not in UNSUPPORTED]
    values = ", ".join(f"('{m}')" for m in declarable)
    sql = (
        "SELECT m, d, provisa_fake_method(m, '{}', d, 5) FROM (VALUES "
        + values
        + ") AS t(m) CROSS JOIN UNNEST(ARRAY[1, -7, 4611686018427387904]) AS s(d)"
    )
    cur = trino_conn.cursor()
    cur.execute(sql)
    first = {(m, d): v for m, d, v in cur.fetchall()}
    time.sleep(1.1)  # the clock moves past a second: a method reading it would differ
    cur.execute(sql)
    second = {(m, d): v for m, d, v in cur.fetchall()}
    assert len(first) == 3 * len(declarable)
    assert {k for k in first if first[k] != second[k]} == set()


def test_trino_computes_every_stable_fake_as_the_portable_definition_does(trino_conn):
    """REQ-1494, A STABLE FAKE: byte-identical to provisa.fakes.portable for 1000 digests."""
    from provisa.fakes.digest import definition_hash
    from provisa.fakes.methods import STABLE_METHODS
    from provisa.fakes.portable import stable_fake

    seeds = [d * 7919 * 104729 for d in range(-500, 500)] + [-(2**63), 2**63 - 1]
    hashes = {m: definition_hash(m, {}, 1, "varchar") for m in STABLE_METHODS}
    methods = ", ".join(f"('{m}', BIGINT '{h}')" for m, h in sorted(hashes.items()))
    cur = trino_conn.cursor()
    cur.execute(
        "SELECT m, d, provisa_stable_fake(m, 1, d, h) FROM (VALUES "
        + methods
        + ") AS t(m, h) CROSS JOIN UNNEST(ARRAY["
        + ", ".join(f"BIGINT '{d}'" for d in seeds)
        + "]) AS s(d)"
    )
    got = {(m, d): v for m, d, v in cur.fetchall()}
    assert len(got) == len(STABLE_METHODS) * len(seeds)
    differ = [k for k, v in got.items() if v != stable_fake(k[0], 1, k[1], hashes[k[0]])]
    assert differ == [], differ[:5]


@pytest.mark.parametrize("name", sorted(UNSUPPORTED))
def test_a_method_no_column_holds_is_refused_by_name(name):
    with pytest.raises(FakeRefused, match="makes values no column can hold"):
        check_method(Method(name), "text")


_ARGUMENTS = [
    ("date_between", {"start_date": "2024-01-01", "end_date": "2024-01-31"}),
    ("date_time_between", {"start_date": "-1y", "end_date": "now"}),
    ("date_of_birth", {"minimum_age": 18, "maximum_age": 20}),
    ("pyint", {"min_value": 5, "max_value": 7}),
    ("pyfloat", {"left_digits": 2, "right_digits": 3, "positive": True}),
    ("random_number", {"digits": 4, "fix_len": True}),
    ("pystr", {"min_chars": 3, "max_chars": 5}),
    ("numerify", {"text": "AB-###"}),
    ("date", {"pattern": "%d/%m/%Y"}),
    ("boolean", {"chance_of_getting_true": 100}),
    ("words", {"nb": 2}),
    ("currency", {}),
]


def test_trino_honours_the_range_format_and_length_arguments(trino_conn):
    import datetime as dt
    import json
    import re

    cur = trino_conn.cursor()
    rows = {}
    for method, args in _ARGUMENTS:
        cur.execute(f"SELECT provisa_fake_method('{method}', '{json.dumps(args)}', 7, 5)")
        rows[method] = cur.fetchall()[0][0]
    assert "2024-01-01" <= rows["date_between"] <= "2024-01-31"
    born = dt.date.fromisoformat(rows["date_of_birth"])
    # Ages count from the reference instant (provisa.fakes.methods.REFERENCE_INSTANT).
    assert 18 <= (dt.date(2026, 7, 15) - born).days // 365.25 <= 21
    assert 5 <= int(rows["pyint"]) <= 7
    assert re.fullmatch(r"\d{1,2}\.\d{3}", rows["pyfloat"])
    assert re.fullmatch(r"\d{4}", rows["random_number"])
    assert 3 <= len(rows["pystr"]) <= 5
    assert re.fullmatch(r"AB-\d{3}", rows["numerify"])
    assert re.fullmatch(r"\d{2}/\d{2}/\d{4}", rows["date"])
    assert rows["boolean"] == "true"
    assert len(rows["words"].split(" ")) == 2
    assert re.fullmatch(r"[A-Z]{3}", rows["currency"])
