# Copyright (c) 2026 Kenneth Stott
# Canary: 5a1e8c37-d92b-4f60-b3a4-7e0c6d15f928
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The values sent with a statement are the values its placeholders number (REQ-1937).

``/data/sql`` takes a statement's values apart from its text. They are bound by the pipeline's
own parameter path; before that, a list that does not fit the statement's ``$N`` placeholders is
refused by name, and a list sent with a batch is refused: values belong to one statement.
"""

# Requirements: REQ-1937, REQ-589

from __future__ import annotations

import pytest

from provisa.compiler.params import (
    ParametersDoNotFit,
    require_parameters_fit,
    statement_placeholders,
)


@pytest.mark.parametrize(
    "sql, numbers",
    [
        ("SELECT * FROM t WHERE a = $1 AND b > $2 AND d = $1", (1, 2)),
        ("SELECT $1,$2", (1, 2)),  # no space: not a dollar-quoted tag
        ("SELECT $10, $2, $1", (1, 2, 10)),
        ("SELECT 1", ()),
        # Text, not placeholders:
        ("SELECT * FROM t WHERE c = 'costs $5' AND a = $1", (1,)),
        ("SELECT 'it''s $6', $1", (1,)),
        ('SELECT "col$4", $1', (1,)),
        ("SELECT $1 -- and $7\n+ $2", (1, 2)),
        ("SELECT $1 /* not $8 */ + $2", (1, 2)),
        ("SELECT $$body $3$$, $2", (2,)),
        ("SELECT $tag$ body $9 $tag$, $1", (1,)),
    ],
)
def test_a_statements_placeholders_are_read_outside_its_text(sql, numbers):
    assert statement_placeholders(sql) == numbers


@pytest.mark.parametrize(
    "sql, params",
    [
        ("SELECT * FROM t WHERE a = $1", [7]),
        ("SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1", ["x", None]),
        ("SELECT 1", []),
    ],
)
def test_values_that_number_the_placeholders_exactly_fit(sql, params):
    require_parameters_fit(sql, params)


@pytest.mark.parametrize(
    "sql, params, named",
    [
        ("SELECT $1, $2", [1], "placeholder $2 has no value"),
        ("SELECT $1, $3", [1, 2], "placeholder $3 has no value"),
        ("SELECT $1", [1, 2], "no placeholder takes value $2"),
        ("SELECT $2", [1, 2], "no placeholder takes value $1"),
        ("SELECT 1", [1], "no placeholder takes value $1"),
        ("SELECT $1", [], "placeholder $1 has no value"),
    ],
)
def test_values_that_do_not_fit_are_refused_naming_what_does_not(sql, params, named):
    with pytest.raises(ParametersDoNotFit, match=named.replace("$", r"\$")):
        require_parameters_fit(sql, params)


async def test_a_batch_takes_no_values():
    """Values are bound to one statement; which statement of a batch would they be for?"""
    from provisa.pgwire._pipeline import execute_sql_batch

    with pytest.raises(ParametersDoNotFit, match="one statement; this request holds 2"):
        await execute_sql_batch("SELECT $1; SELECT 2", "analyst", object(), params=[1])


def test_the_values_travel_by_the_pipelines_own_parameter_path():
    """One binding path: the batch entry point hands the values to ``_govern_and_route``'s
    ``params``, which pgwire's extended protocol uses (REQ-589). Nothing substitutes them."""
    import inspect

    from provisa.pgwire import _pipeline

    source = inspect.getsource(_pipeline.execute_sql_batch)
    assert source.count("params=params,") == 2  # the temp-session re-entry, and the governed call
    assert "substitute" not in source and ".format(" not in source
