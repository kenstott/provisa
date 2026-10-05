# Copyright (c) 2026 Kenneth Stott
# Canary: ea8b32a7-57a8-49ed-9ef6-8de66a7c2b9d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1915 (maintainer ruling 2026-10-05): the row-level copy serves only reads the ENGINE
federates. A single-source read through the source's own driver (DIRECT) reads the source live:
no bound is taken, so nothing is fetched into the copy and an unfiltered read is not refused. The
one decision is ``_row_level_bounds``, keyed on the plan's route; the bounds themselves are taken
once, before the cache (``_RowLevelBounds``)."""

from __future__ import annotations

import pytest

from provisa.api.errors import RowLevelFilterRequired
from provisa.compiler.pk_bounds import FilterBound
from provisa.pgwire._pipeline import _row_level_bounds, _RowLevelBounds
from provisa.transpiler.router import Route

_FILTER = FilterBound(
    source_id="rl-mariadb",
    schema_name="provisa",
    table_name="orders",
    pk_columns=("order_id",),
    predicate='"amount" > 100',
)


_UNBOUND = _RowLevelBounds(unbound="orders")


def test_a_direct_read_takes_no_bound_and_is_not_refused():
    assert _row_level_bounds(Route.DIRECT, _UNBOUND) == ()


def test_a_direct_filtered_read_fetches_nothing_into_the_copy():
    assert _row_level_bounds(Route.DIRECT, _RowLevelBounds(bounds=(_FILTER,))) == ()


def test_a_federated_read_takes_its_filter_bound():
    assert _row_level_bounds(Route.ENGINE, _RowLevelBounds(bounds=(_FILTER,))) == (_FILTER,)


def test_a_federated_unfiltered_read_is_refused_naming_the_table():
    with pytest.raises(RowLevelFilterRequired) as refused:
        _row_level_bounds(Route.ENGINE, _UNBOUND)
    assert refused.value.code == "data.row_level_filter_required"
    assert refused.value.params == {"table": "orders"}


def test_the_bounds_are_taken_once_and_an_unbound_read_is_kept_not_raised():
    def unfiltered() -> tuple:
        raise RowLevelFilterRequired("orders")

    assert _RowLevelBounds.of(unfiltered) == _UNBOUND
    assert _RowLevelBounds.of(lambda: (_FILTER,)) == _RowLevelBounds(bounds=(_FILTER,))
