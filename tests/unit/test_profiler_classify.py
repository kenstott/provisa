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

import re

import pytest

from provisa.profiler import classify
from provisa.profiler.classify import MIN_TESTED, catalogue, kind_of

# header {"alg":"HS256","typ":"JWT"}, payload {"sub":"1234567890","name":"John"}
_JWT = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
    "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4ifQ."
    "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)

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
    "us_ein": ["12-3456789", "95-0000001", "99-1234567"],
    "us_itin": ["912-70-1234", "970881234", "950-50-1234", "999-94-0000"],
    "us_bank_routing_number": ["021000021", "122000030", "011401533"],
    "de_steuer_id": ["65929970489", "86123456784", "86 123 456 784", "48123456791"],
    "fr_nir": ["184127645108946", "2 69 05 11 402 065 85", "185072A01001219"],
    "au_tfn": ["123456782", "123 456 782", "876543210"],
    "br_cpf": ["123.456.789-09", "11144477735", "111.444.777-35"],
    "uk_nhs_number": ["943 476 5919", "9434765919", "401-023-2137", "9434765900"],
    "aws_access_key_id": ["AKIAIOSFODNN7EXAMPLE", "ASIAY34FZKBOKMUTVV7A"],
    "github_token": [
        "ghp_" + "aB3" * 12,
        "gho_" + "x" * 36,
        "ghs_" + "7" * 36,
        "github_pat_" + "A" * 22 + "_" + "b" * 59,
    ],
    "jwt": [_JWT, _JWT.rsplit(".", 1)[0] + "."],  # an unsecured token has an empty signature
    "email_address": ["ada@example.com", "first.last+tag@mail.example.co.uk", "o'brien@x-y.org"],
    "e164_phone": ["+14155552671", "+44 20 7946 0958", "+1 (415) 555-2671", "+49-30-901820"],
    "ipv4_address": ["192.168.0.1", "0.0.0.0", "255.255.255.255", "10.0.0.254"],
    "ipv6_address": [
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
        "2001:db8::1",
        "::1",
        "::",
        "fe80::1ff:fe23:4567:890a",
        "2001:DB8::8a2e:370:7334",
    ],
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
    "us_ein": [
        "07-1234567",  # 07 is no campus prefix
        "12-345678",  # a digit short
        "123456789",  # no hyphen: an SSN-shaped number, not an EIN
        "12-3456789x",
    ],
    "us_itin": [
        "812-70-1234",  # an SSN-shaped number: an ITIN starts with 9
        "912-93-1234",  # group 93 is not an ITIN group
        "912-69-1234",  # nor is 69
        "912-70-123",
    ],
    "us_bank_routing_number": [
        "021000022",  # check digit off by one
        "000000000",  # sums to zero
        "12345678",  # a digit short
        "02100002A",
    ],
    "de_steuer_id": [
        "65929970488",  # check digit off by one
        "12345678903",  # right check digit, but no repeated digit among the first ten
        "11234566772",  # right check digit, but two digits repeat
        "11123456786",  # right check digit, but a digit three in a row
        "01234567890",  # starts with 0
        "6592997048",  # a digit short
    ],
    "fr_nir": [
        "184127645108947",  # key off by one
        "384127645108946",  # sex digit 3
        "184137645108946",  # month 13
        "18412764510894",  # a digit short
    ],
    "au_tfn": ["123456783", "12345678", "000000000", "123 456 78x"],
    "br_cpf": [
        "12345678908",  # check digit off
        "111.111.111-11",  # all alike passes the arithmetic and is refused
        "123.456.789-0",  # a digit short
        "123.456.789-09x",
    ],
    "uk_nhs_number": [
        "9434765918",  # check digit off by one
        "1234567890",  # the check works out to 10, which is never issued
        "943476591",  # a digit short
        "943 476 591x",
    ],
    "aws_access_key_id": [
        "AKIAIOSFODNN7EXAMPL",  # a character short
        "AKIAIOSFODNN7EXAMPLE1",  # a character over
        "akiaiosfodnn7example",  # lower case
        "AKIA IOSFODNN7EXAMP",
        "AIZAIOSFODNN7EXAMPLE",  # no such prefix
    ],
    "github_token": [
        "ghp_" + "a" * 35,  # a character short
        "ghx_" + "a" * 36,  # no such kind letter
        "ghp-" + "a" * 36,
        "ghp_" + "a" * 35 + "!",
        "a" * 40,  # a bare 40-character secret has no prefix
    ],
    "jwt": [
        "eyJ0eXAiOiJKV1QifQ.eyJzdWIiOiIxIn0.abc",  # a header that names no alg
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0",  # two segments
        "eyJ!bGc.eyJzdWIi.abc",  # not base64url
        "eyJhbGciOiJIUzI1NiJ9.e30.a.b",  # four segments
        "notajwt.eyJzdWIiOiIxIn0.abc",
    ],
    "email_address": [
        "ada@example",  # no dot in the domain
        "ada.@example.com",  # a trailing dot in the local part
        "a..da@example.com",
        "ada@@example.com",
        "x" * 65 + "@example.com",  # a local part over 64
        "ada@-example.com",
    ],
    "e164_phone": [
        "14155552671",  # no plus sign
        "+0 4155552671",  # country code cannot start with 0
        "+123456",  # too few digits
        "+1234567890123456",  # over 15 digits
        "+1415555abcd",
    ],
    "ipv4_address": [
        "256.1.1.1",
        "1.2.3",
        "1.2.3.4.5",
        "01.02.03.04",  # leading zeros
        "1.2.3.a",
    ],
    "ipv6_address": [
        "2001:db8::1::1",  # two ::
        "12345::1",  # a group of five
        "2001:db8:85a3:0:0:8a2e:370",  # seven groups, no ::
        "g001:db8::1",
        "1:2:3:4:5:6:7:8:9",
    ],
}


@pytest.mark.parametrize("kind_id", sorted(_VALID))
def test_a_validator_passes_what_its_rules_admit_and_refuses_the_rest(kind_id):
    validate = classify.VALIDATORS[kind_of(kind_id).test]
    assert [v for v in _VALID[kind_id] if not validate(v)] == []
    assert [v for v in _INVALID[kind_id] if validate(v)] == []


@pytest.mark.parametrize("kind_id", sorted(_VALID))
def test_a_pattern_is_unanchored_and_the_whole_value_test_anchors_it(kind_id):
    kind = kind_of(kind_id)
    assert kind.pattern is not None
    assert not kind.pattern.startswith("^") and not kind.pattern.endswith("$"), kind_id
    flags = re.IGNORECASE if kind.ignore_case else 0
    sample = _VALID[kind_id][0]
    assert re.search(kind.pattern, f"id: {sample}.", flags) is not None  # found inside a text
    assert not classify.VALIDATORS[kind.test](f"id: {sample} extra")  # but not as a whole value


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
            assert kind.pattern, kind.id
            assert kind.check is None or kind.check in classify.CHECKS, kind.id
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
