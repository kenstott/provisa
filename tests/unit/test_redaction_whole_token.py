# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1c9d2e-7b4a-4e60-9a1d-5c8e2f0b6a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A secret is redacted only where its value is a whole token (REQ-2087, amended 2026-11-22):
bounded by characters that cannot be part of an identifier, or as a whole quoted value. An
occurrence inside a longer identifier is left as written."""

# Requirements: REQ-2087

from __future__ import annotations

import pytest

from provisa.security import redaction

ORG = "org-a"


@pytest.fixture(autouse=True)
def _fresh_rules():
    redaction.reset()
    yield
    redaction.reset()


def _secret(value: str) -> None:
    redaction.set_org(ORG, [], {"ci_pw": value}, lambda ref: ref)


@pytest.mark.parametrize(
    "text",
    [
        "relation provisa_cache.mv_orders_summary does not exist",
        "no such field provisa_fake_method",
        "column myprovisa not found",
        "table cache_provisa2 missing",
    ],
)
def test_a_secret_inside_a_longer_identifier_is_left_as_written(text: str):
    _secret("provisa")
    assert redaction.redact(text, ORG) == text
    assert redaction.client_redact(text, ORG) == text


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("user provisa connected", "user [REDACTED] connected"),
        ("password=provisa", "password=[REDACTED]"),
        ("'provisa'", "'[REDACTED]'"),
        ('"provisa"', '"[REDACTED]"'),
        ("provisa", "[REDACTED]"),
        ("auth failed: provisa.", "auth failed: [REDACTED]."),
        ("schema provisa.orders", "schema [REDACTED].orders"),
    ],
)
def test_a_secret_standing_as_a_whole_token_is_redacted(text: str, expected: str):
    _secret("provisa")
    assert redaction.client_redact(text, ORG) == expected
    assert redaction.redact(text, ORG) == expected.replace("[REDACTED]", "[REDACTED:auto:ci_pw]")


def test_a_secret_with_regex_metacharacters_is_matched_literally():
    _secret("p.ss+(w)*rd")
    assert redaction.client_redact("password=p.ss+(w)*rd;", ORG) == "password=[REDACTED];"
    # Read as a pattern, "." and "+" would match this; matched literally, it does not.
    assert redaction.client_redact("password=pXssswwrd;", ORG) == "password=pXssswwrd;"


def test_a_secret_edged_by_punctuation_is_redacted_wherever_it_stands():
    # The secret's own first and last characters cannot extend an identifier, so no neighbour
    # can make it part of one.
    _secret("!hunter2!")
    assert redaction.client_redact("pw=x!hunter2!y", ORG) == "pw=x[REDACTED]y"
