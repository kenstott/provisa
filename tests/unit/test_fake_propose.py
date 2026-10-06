# Copyright (c) 2026 Kenneth Stott
# Canary: 49e93877-bed7-4bf2-96d3-2df85fd8b7ac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Fill from profile (REQ-1494): fakes for identifying columns only, synthetic rules for the rest,
nothing for a pii column with no confident match, and nothing for a column already declared."""

from __future__ import annotations

from provisa.fakes.kinds import parse
from provisa.fakes.propose import ProfiledColumnFacts, propose


def _col(name, family="text", plausible="unknown", confidence=0.9, freqs=(), length=None):
    return ProfiledColumnFacts(name, family, length, plausible, confidence, tuple(freqs))


_COLUMNS = [
    _col("email", plausible="email"),
    _col("full_name", plausible="person_name_full"),
    _col("phone", plausible="phone", confidence=0.4),  # not confident
    _col("ssn"),  # a government identifier, by name
    _col("account_no", family="numeric", plausible="identifier"),  # an account number, by name
    _col("amount", family="numeric", plausible="numeric_distribution"),
    _col("created", family="temporal", plausible="temporal"),
    _col("active", family="boolean", plausible="boolean"),
    _col("tier", plausible="category", freqs=(40, 30, 30)),
    _col("rare", plausible="category", freqs=(40, 2)),  # thin evidence
    _col("notes", plausible="free_text", length=180),
    _col("secret", plausible="unknown"),  # tagged pii, no match
    _col("city", plausible="address_city"),  # declared already
]


def test_identifying_columns_get_fakes_and_the_rest_synthetic_rules():
    out = propose("r1", _COLUMNS, pii={"secret", "notes"}, declared={"city"})
    assert out.columns == {
        "email": {"fake": "email()"},
        "full_name": {"fake": "name()"},
        "ssn": {"fake": "ssn()"},
        "account_no": {"fake": "bban()"},
        "amount": {"syntheticRule": "profile(run='r1')"},
        "created": {"syntheticRule": "profile(run='r1')"},
        "active": {"syntheticRule": "bool()"},
        "tier": {"syntheticRule": "categories()"},
    }
    assert out.unmatched_pii == ["notes", "secret"]
    assert "phone" not in out.columns and "rare" not in out.columns and "city" not in out.columns


def test_a_free_text_column_not_tagged_pii_is_proposed_text_to_its_length():
    out = propose("r1", [_col("notes", plausible="free_text", length=180)], set(), set())
    assert out.columns == {"notes": {"syntheticRule": "text(max_nb_chars=180)"}}


def test_every_proposal_is_a_declaration_the_save_reads():
    out = propose("r1", _COLUMNS, pii=set(), declared=set())
    for proposal in out.columns.values():
        if "fake" in proposal:
            parse(proposal["fake"])
        else:
            parse(proposal["syntheticRule"], rule=True)
