# Copyright (c) 2026 Kenneth Stott
# Canary: cebe2dec-4a0f-4e7b-800b-97c42019e0f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2147: the classification kinds, their tests, and the suggestions a column's sampled values
reach.

Each sensitive kind's validator passes the values its rules admit and refuses the ones they do
not; each reference kind tests membership of its shipped list. A column is suggested a kind when
the share of its sampled non-null values passing reaches the kind's threshold, over at least
MIN_TESTED values. The catalogue holds every kind to a validator or list that exists, a mask preset
that exists for every sensitive kind, and none for any other.
"""

# Requirements: REQ-2147

from __future__ import annotations

import pytest

from provisa.profiler import classify
from provisa.profiler.classify import MIN_TESTED, catalogue, kind_of

_VALID = {
    "us_ssn": ["123-45-6789", "078051120", "1234567"],
    "payment_card": ["4111 1111 1111 1111", "5500-0000-0000-0004", "378282246310005"],
    "iban": ["GB82 WEST 1234 5698 7654 32", "DE89370400440532013000"],
    "uk_national_insurance_number": ["AB123456C", "AB 12 34 56 C"],
    "ca_social_insurance_number": ["130 692 544", "130-692-544"],
    "es_dni": ["12345678Z", "X1234567L"],
    "nl_bsn": ["111222333", "123456782"],
    "in_aadhaar": ["234123412346"],
    "us_passport": ["A12345678"],
}
_INVALID = {
    "us_ssn": [
        "000-12-3456",  # area 000
        "666-12-3456",  # area 666
        "900-12-3456",  # area 900-999
        "123-00-4567",  # group 00
        "123-45-0000",  # serial 0000
        "12-345-6789",
        "abc",
    ],
    "payment_card": ["4111111111111112", "1234", "4111-1111-1111-111x"],
    "iban": ["GB82WEST12345698765433", "XX00", "GB82 WEST"],
    "uk_national_insurance_number": [
        "QQ123456A",  # Q is no first letter
        "BG123456A",  # a reserved pair
        "AB123456E",  # suffix past D
        "DA123456A",  # D is no first letter
    ],
    "ca_social_insurance_number": ["130692545", "046454286", "846454286"],
    "es_dni": ["12345678A", "1234567Z"],
    "nl_bsn": ["111222334", "000000000"],
    "in_aadhaar": ["234123412345", "123412341234"],
    "us_passport": ["123456789", "AB1234567"],
}


@pytest.mark.parametrize("kind_id", sorted(_VALID))
def test_a_validator_passes_what_its_rules_admit_and_refuses_the_rest(kind_id):
    validate = classify.VALIDATORS[kind_of(kind_id).test]
    assert [v for v in _VALID[kind_id] if not validate(v)] == []
    assert [v for v in _INVALID[kind_id] if validate(v)] == []


def test_every_sensitive_kind_has_a_validator_case():
    assert {k.id for k in catalogue() if k.sensitive} == set(_VALID)


@pytest.mark.parametrize(
    ("kind_id", "known", "unknown"),
    [
        ("country", ["France", "united states"], ["Atlantis"]),
        ("country_code", ["us", "FRA"], ["QQ"]),
        ("currency_code", ["USD", "eur"], ["ZZZ"]),
        ("language_code", ["en", "fra", "fre", "ger", "deu"], ["xx"]),
        ("city", ["Paris", "São Paulo", "  new   york city "], ["Notacity Upon Nowhere"]),
    ],
)
def test_a_reference_kind_tests_membership_of_its_shipped_list(kind_id, known, unknown):
    kind = kind_of(kind_id)
    assert kind.reference is not None
    listed = classify.reference_list(kind.reference)
    assert all(classify.normalise(v) in listed for v in known)
    assert not any(classify.normalise(v) in listed for v in unknown)


def test_the_catalogue_names_only_validators_lists_and_presets_that_exist():
    from provisa.security.mask_presets import PRESETS

    presets = {p["id"] for p in PRESETS}
    for kind in catalogue():
        if kind.reference is None:
            assert kind.test in classify.VALIDATORS, kind.id
        else:
            assert (classify.REFERENCE_DIR / f"{kind.reference}.txt.gz").is_file(), kind.id
        assert kind.sensitive == (kind.mask_preset is not None), kind.id
        if kind.mask_preset is not None:
            assert kind.mask_preset in presets, kind.id
        assert kind.term == kind.term.strip().lower(), kind.id  # term names are lowercase


def test_sensitive_kinds_come_first_in_the_catalogue():
    flags = [k.sensitive for k in catalogue()]
    assert flags == sorted(flags, reverse=True)


def _ssns(n: int) -> list[str | None]:
    return [f"123-45-{6789 - i:04d}" for i in range(n)]


def test_a_column_is_suggested_a_kind_when_the_passing_share_reaches_its_threshold():
    kind = kind_of("us_ssn")
    values = _ssns(19) + ["not an ssn"]  # 95% pass, over the 90% threshold
    (match,) = classify.classify("text", values, (kind,))
    assert (match.tested, match.passed) == (20, 19)
    assert match.evidence("customers", "tax_id") == (
        "customers.tax_id looks like a US social security number: 95% of sampled values pass the "
        "SSN rules"
    )
    assert classify.classify("text", _ssns(17) + ["x"] * 3, (kind,)) == []  # 85%: under it


def test_nulls_are_not_tested_and_too_few_values_suggest_nothing():
    kind = kind_of("us_ssn")
    assert classify.classify("text", _ssns(MIN_TESTED - 1) + [None] * 50, (kind,)) == []
    (match,) = classify.classify("text", _ssns(MIN_TESTED) + [None] * 50, (kind,))
    assert match.tested == MIN_TESTED


def test_a_number_column_is_tested_by_its_whole_numbers_and_a_fraction_fails():
    kind = kind_of("us_ssn")
    whole = ["123456789", "78051120.0"] + [str(123450000 + i) for i in range(1, 9)]
    (match,) = classify.classify("numeric", whole, (kind,))
    assert match.passed == match.tested == 10
    (match,) = classify.classify("numeric", whole[:9] + ["1.5"], (kind,))
    assert (match.tested, match.passed) == (10, 9)  # the fraction is tested and fails


def test_a_kind_tests_only_the_families_it_names():
    assert classify.classify("numeric", ["France"] * 20, (kind_of("country"),)) == []
    assert classify.classify("temporal", _ssns(20), catalogue()) == []
