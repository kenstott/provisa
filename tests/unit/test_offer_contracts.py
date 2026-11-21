# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7e1a58-6b29-4d84-9f0e-5a2d8c4b7e16
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2180 (amended 2026-11-11, CONTRACTS ARE DERIVED FROM THE OFFERS): each offer's contract on
a dataset is the offer's terms applied to it, and changing the offer changes the contract's
version, a column removed or masked being a breaking change."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from provisa.core import offer_contracts as oc
from provisa.core.models import ContractCheck, DataProductOffer

_AT = datetime(2026, 11, 11, tzinfo=UTC)
_LATER = datetime(2026, 11, 12, tzinfo=UTC)

_COLUMNS = [
    oc.SeenColumn("id", "integer"),
    oc.SeenColumn("email", "text", mask="regex"),
    oc.SeenColumn("salary", "numeric", read=False),
]
_CHECK = ContractCheck(name="not_null", column="id")


def _terms(offer: DataProductOffer, columns=_COLUMNS, **more):
    given = {
        "row_rules": [],
        "sample_views": [],
        "checks": [_CHECK],
        "profiles": [],
        **more,
    }
    return oc.terms(offer, columns, **given)


def test_the_schema_is_the_columns_the_role_reads_masked_ones_stated():
    t = _terms(DataProductOffer(name="o", role_id="viewer"))
    assert [(f.name, f.masked) for f in t.fields] == [("id", None), ("email", "regex")]


def test_row_rules_in_words_and_dq_promises_are_always_stated():
    offer = DataProductOffer(name="o", role_id="viewer", dq_results=False)
    t = _terms(offer, row_rules=["only the EMEA region", "only the EMEA region"])
    assert t.row_rules == ["only the EMEA region"]
    assert t.checks == [_CHECK]
    assert t.dq_results is False


def test_profiles_appear_only_where_the_offer_includes_them():
    without = _terms(DataProductOffer(name="o", role_id="viewer"), profiles=["orders_1_profile_x"])
    assert without.profiles == []
    included = DataProductOffer(name="o", role_id="viewer", profiles=True)
    assert _terms(included, profiles=["orders_1_profile_x"]).profiles == ["orders_1_profile_x"]


def test_a_sample_states_its_views_over_the_dataset():
    offer = DataProductOffer(name="s", role_id="viewer", data_level="sample", sample_views=[9])
    t = _terms(offer, sample_views=["orders_sample"])
    assert (t.data_level, t.sample_views) == ("sample", ["orders_sample"])


def test_the_first_version_is_one_and_saving_the_same_terms_keeps_it():
    t = _terms(DataProductOffer(name="o", role_id="viewer"))
    first = oc.recorded(None, t, at=_AT)
    assert first.version == oc.FIRST_OFFER_CONTRACT_VERSION
    assert oc.recorded(first, t, at=_LATER) is first


def test_masking_a_column_is_breaking_and_takes_the_next_major_version():
    offer = DataProductOffer(name="o", role_id="viewer")
    first = oc.recorded(None, _terms(offer), at=_AT)
    masked = [oc.SeenColumn("id", "integer", mask="constant"), *_COLUMNS[1:]]
    second = oc.recorded(first, _terms(offer, masked), at=_LATER)
    assert second.version == "2.0.0"
    assert oc.breaking(second)
    (change,) = second.versions[-1].changes
    assert (change.kind, change.subject, change.breaking) == ("field_masked", "id", True)


def test_removing_a_column_is_breaking():
    offer = DataProductOffer(name="o", role_id="viewer")
    first = oc.recorded(None, _terms(offer), at=_AT)
    second = oc.recorded(first, _terms(offer, _COLUMNS[:1]), at=_LATER)
    assert second.version == "2.0.0"
    assert [c.kind for c in second.versions[-1].changes] == ["field_removed"]


def test_unmasking_or_adding_a_column_takes_the_next_minor_version():
    offer = DataProductOffer(name="o", role_id="viewer")
    first = oc.recorded(None, _terms(offer), at=_AT)
    wider = [
        oc.SeenColumn("id", "integer"),
        oc.SeenColumn("email", "text"),
        oc.SeenColumn("salary", "numeric"),
    ]
    second = oc.recorded(first, _terms(offer, wider), at=_LATER)
    assert second.version == "1.1.0"
    assert not oc.breaking(second)
    assert sorted(c.kind for c in second.versions[-1].changes) == ["field_added", "field_unmasked"]
    assert [v.version for v in second.versions] == ["1.0.0", "1.1.0"]


@pytest.mark.parametrize(
    ("before", "after", "breaking"),
    [
        ({"dq_results": True}, {"dq_results": False}, True),
        ({"dq_results": False}, {"dq_results": True}, False),
        ({"profiles": True}, {"profiles": False}, True),
        ({}, {"duration_days": 30}, True),
        ({"duration_days": 30}, {}, False),
        ({"duration_days": 30}, {"duration_days": 10}, True),
    ],
)
def test_an_offer_giving_less_is_breaking(before, after, breaking):
    def offer(**kw):
        return DataProductOffer(name="o", role_id="viewer", **kw)

    first = oc.recorded(None, _terms(offer(**before), profiles=["p"]), at=_AT)
    second = oc.recorded(first, _terms(offer(**after), profiles=["p"]), at=_LATER)
    assert oc.breaking(second) is breaking
    assert second.version == ("2.0.0" if breaking else "1.1.0")


def test_a_row_rule_added_is_breaking_and_one_removed_is_not():
    offer = DataProductOffer(name="o", role_id="viewer")
    first = oc.recorded(None, _terms(offer), at=_AT)
    ruled = oc.recorded(first, _terms(offer, row_rules=["only open orders"]), at=_LATER)
    assert ruled.version == "2.0.0"
    freed = oc.recorded(ruled, _terms(offer), at=_LATER)
    assert freed.version == "2.1.0"


def test_full_to_sample_is_breaking():
    full = DataProductOffer(name="o", role_id="viewer")
    sample = DataProductOffer(name="o", role_id="viewer", data_level="sample", sample_views=[9])
    first = oc.recorded(None, _terms(full), at=_AT)
    second = oc.recorded(first, _terms(sample, sample_views=["v"]), at=_LATER)
    (change,) = second.versions[-1].changes
    assert (change.kind, change.before, change.after) == (
        "data_level_changed",
        "full",
        "sample (v)",
    )
    assert change.breaking


@pytest.mark.parametrize(
    "kw",
    [
        {"data_level": "sample"},
        {"sample_views": [1]},
        {"data_level": "synthetic", "profiles": True},
        {"duration_days": 0},
    ],
)
def test_an_offer_that_does_not_fit_is_refused(kw):
    with pytest.raises(ValueError):
        DataProductOffer(name="o", role_id="viewer", **kw)


def test_a_contract_states_the_offers_terms_of_use():
    offer = DataProductOffer(
        name="o",
        role_id="viewer",
        purpose="Churn analysis",
        usage="Monthly models",
        limitations="No re-identification",
    )
    t = _terms(offer)
    assert (t.purpose, t.usage, t.limitations) == (
        "Churn analysis",
        "Monthly models",
        "No re-identification",
    )


def test_changing_the_terms_of_use_records_each_change_and_takes_the_next_minor_version():
    before = oc.recorded(None, _terms(DataProductOffer(name="o", role_id="viewer")), at=_AT)
    offer = DataProductOffer(name="o", role_id="viewer", usage="Reporting only")
    after = oc.recorded(before, _terms(offer), at=_LATER)
    (change,) = after.versions[-1].changes
    assert (change.kind, change.subject, change.before, change.after, change.breaking) == (
        "terms_of_use_changed",
        "usage",
        None,
        "Reporting only",
        False,
    )
    assert after.version == "1.1.0"
