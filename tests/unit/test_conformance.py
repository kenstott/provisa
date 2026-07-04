# Copyright (c) 2026 Kenneth Stott
# Canary: 2c9d8a71-4b08-4e75-8f12-3c7a0d4f9c20
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-827: governance-parity conformance comparator + certification gate."""

from __future__ import annotations

import pytest

from provisa.federation.conformance import (
    ConformanceRegistry,
    UncertifiedEngineError,
    compare_governed_results,
)


# ---- governed-result comparison ---------------------------------------------


def test_identical_results_are_certified():
    rows = [(1, "ada"), (2, "bo")]
    assert compare_governed_results(rows, list(rows)).certified is True


def test_order_differences_are_allowed():
    # NULL/ordering is an allowed semantic edge — same set, different order → certified.
    ref = [(1, "ada"), (2, "bo"), (3, None)]
    cand = [(3, None), (1, "ada"), (2, "bo")]
    assert compare_governed_results(ref, cand).certified is True


def test_row_visible_only_in_candidate_is_divergence():
    # RLS leak: candidate shows a row the reference filtered out.
    res = compare_governed_results([(1, "ada")], [(1, "ada"), (2, "secret")])
    assert res.certified is False
    assert any(d.kind == "only_in_candidate" and d.row == (2, "secret") for d in res.divergences)


def test_row_over_filtered_in_candidate_is_divergence():
    res = compare_governed_results([(1, "ada"), (2, "bo")], [(1, "ada")])
    assert res.certified is False
    assert any(d.kind == "only_in_reference" and d.row == (2, "bo") for d in res.divergences)


def test_differently_masked_cell_is_divergence():
    # Reference masks the ssn; candidate leaks it → the row tuples differ → divergence.
    res = compare_governed_results([(1, "***")], [(1, "123-45-6789")])
    assert res.certified is False


def test_duplicate_row_counts_matter():
    # Aggregate/row-count parity: a differing multiplicity is a divergence.
    res = compare_governed_results([(1,), (1,)], [(1,)])
    assert res.certified is False


# ---- certification gate -----------------------------------------------------


def test_reference_is_always_certified():
    reg = ConformanceRegistry(reference="trino")
    assert reg.is_certified("trino") is True
    reg.require_certified("trino")  # no raise


def test_uncertified_engine_is_rejected():
    reg = ConformanceRegistry(reference="trino")
    assert reg.is_certified("duckdb") is False
    with pytest.raises(UncertifiedEngineError):
        reg.require_certified("duckdb")


def test_certify_then_selectable():
    reg = ConformanceRegistry(reference="trino")
    reg.certify("duckdb")
    reg.require_certified("duckdb")  # no raise
    assert reg.is_certified("duckdb") is True


def test_revoke_uncertifies():
    reg = ConformanceRegistry(reference="trino")
    reg.certify("duckdb")
    reg.revoke("duckdb")
    with pytest.raises(UncertifiedEngineError):
        reg.require_certified("duckdb")


def test_cannot_revoke_reference():
    reg = ConformanceRegistry(reference="trino")
    with pytest.raises(ValueError):
        reg.revoke("trino")
