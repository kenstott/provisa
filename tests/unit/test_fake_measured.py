# Copyright (c) 2026 Kenneth Stott
# Canary: 7530720d-47c8-4bc1-921d-f2d2340c7781
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Measured values and shares (REQ-1494, MEASURED FROM THE PROFILE, ELSE FROM THE TABLE): what a
faked read computes from, taken from a profile run's column or from the table's counted values,
and the refusals the REQ names."""

from __future__ import annotations

import pytest

from provisa.fakes.kinds import parse
from provisa.fakes.measured import (
    DISTANCE_POINTS,
    from_counts,
    from_distance,
    from_profile,
    needs,
)
from provisa.synthetic.plan import ProfiledColumn


def _profiled(**over) -> ProfiledColumn:
    base = dict(
        physical="c",
        family="numeric",
        null_count=0,
        distinct_count=3,
        distinct_ratio=0.03,
        integer_only=True,
        min_value="1",
        sketch=tuple(float(i) for i in range(101)),
        top=(),
        frequencies=(),
        shapes=(),
    )
    return ProfiledColumn(**{**base, **over})


@pytest.mark.parametrize(
    "decl, measured",
    [
        ("bool()", True),
        ("bool(.8)", False),
        ("categories()", True),
        ("categories((a, b))", False),
        ("profile()", True),
        ("pattern()", True),
        ("after(other)", True),
        ("after(other, 3 days)", False),
        ("uniform(min=0, max=1)", False),
        ("email()", False),
    ],
)
def test_the_kinds_that_compute_from_measurement(decl, measured):
    assert needs(parse(decl)) is measured


def test_bool_takes_the_measured_share_of_true_and_is_even_on_an_empty_table():
    assert (
        from_counts("c", parse("bool()"), [("true", 3), ("false", 1), (None, 9)]).true_share == 0.75
    )
    assert from_counts("c", parse("bool()"), [(None, 4)]).true_share == 0.5


def test_categories_take_the_non_null_values_at_their_shares_and_refuse_an_empty_table():
    m = from_counts("tier", parse("categories()"), [("gold", 1), ("silver", 3), (None, 5)])
    assert m.values == (("silver", 0.75), ("gold", 0.25))
    refused = from_counts("tier", parse("categories()"), [(None, 2)])
    assert refused.refused is not None and "declare them" in refused.refused


def test_a_profile_reads_the_frequencies_of_a_few_valued_column_else_its_sketch():
    few = from_profile("c", parse("profile()"), _profiled(frequencies=(("1", 2), ("2", 2))), "r1")
    assert few.values == (("1", 0.5), ("2", 0.5)) and few.run_id == "r1"
    many = from_profile("c", parse("profile()"), _profiled(), "r1")
    assert (
        many.points is not None and many.points[0] == (0.0, 0.0) and many.points[-1] == (1.0, 100.0)
    )
    none = from_profile("c", parse("profile()"), _profiled(sketch=None), "r1")
    assert none.refused == "c: profile run r1 recorded no distribution of the column"


def test_categories_and_bool_read_a_profile_s_full_frequency_table():
    m = from_profile("c", parse("bool()"), _profiled(frequencies=(("true", 1), ("false", 3))), "r1")
    assert m.true_share == 0.25 and m.run_id == "r1"


def test_a_pattern_reads_the_run_s_shapes_and_refuses_a_run_with_none():
    m = from_profile("c", parse("pattern()"), _profiled(shapes=(("AA-99", 3), ("a9", 1))), "r1")
    assert m.shapes == (("AA-99", 0.75), ("a9", 0.25))
    assert from_profile("c", parse("pattern()"), _profiled(), "r1").refused == (
        "c: profile run r1 recorded no value shapes"
    )


def test_a_measured_difference_is_its_quantiles_and_refused_with_no_row_holding_both():
    kind = parse("after(created)")
    m = from_distance("shipped", kind, [float(i) for i in range(len(DISTANCE_POINTS))])
    assert m.points is not None and m.points[0] == (0.0, 0.0) and m.points[-1] == (1.0, 20.0)
    refused = from_distance("shipped", kind, None)
    assert refused.refused is not None and "declare a distance" in refused.refused
