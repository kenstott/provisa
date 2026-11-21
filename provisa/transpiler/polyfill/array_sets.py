# Copyright (c) 2026 Kenneth Stott
# Canary: 2b7f4c9e-1d63-4a85-b0f2-8e5c3a9d7164
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Array containment and array_remove on ClickHouse (REQ-2236; the function support measure,
REQ-2171): rows of ClickHouse's alone, every other dialect keeping the old table's form
(provisa.transpiler.function_macros).

arraycontains, arraycontained and arrayoverlap read an array's elements flattened, as PostgreSQL
does whatever its dimensions; a NULL element is held by no array (array_contain_compare). array_remove
drops each element equal to the value, a NULL equal to a NULL, and of an array of more dimensions is
PostgreSQL's 0A000."""

# Requirements: REQ-2236, REQ-2171

from __future__ import annotations

import sqlglot.expressions as exp

from provisa.transpiler.polyfill import trino
from provisa.transpiler.polyfill.grid import DIALECTS, NATIVE, Feature, Rule, Side
from provisa.transpiler.polyfill.matching import graft
from provisa.transpiler.polyfill.notation import Form, Refusal, Refused, Rewrite, replacement

FAMILY = "arrays on ClickHouse"


def _ch_only(form: str) -> dict[str, Form]:
    """ClickHouse's ``form``, every other dialect keeping what it had (the old table's form)."""
    return {**{d: trino.KEPT for d in DIALECTS if d not in NATIVE}, "clickhouse": form}


def _removed(node: exp.Expr, dialect: str) -> exp.Expr | None:
    """array_remove of a one-dimensional array: each element not equal to the value, a NULL equal
    to a NULL. Of a literal of more dimensions, PostgreSQL's 0A000."""
    call = trino.called(node)
    if call is None or len(call[1]) != 2:
        return None
    array, value = call[1]
    nested = any(isinstance(inner.parent, exp.Array) for inner in array.find_all(exp.Array))
    held = array.unnest()
    if isinstance(held, exp.Cast):
        to = held.to
        nested = nested or any(
            d.is_type(exp.DataType.Type.ARRAY) for d in to.find_all(exp.DataType) if d is not to
        )
    if nested:
        raise Refused(
            Refusal("0A000", "removing elements from multidimensional arrays is not supported"),
            ruling=True,
        )
    form = replacement(f"arrayFilter(_x -> NOT {_SAME}, $a)", dialect, "array_remove($a, $v)")
    return graft(form, {"a": array, "v": value})


#: Whether an element is the value, a NULL equal to a NULL (array_remove's test).
_SAME = "((_x IS NULL AND $v IS NULL) OR ifNull(_x = $v, 0))"
CONTAINMENT: tuple[Feature, ...] = (
    Feature(
        "arraycontains",
        FAMILY,
        "Whether an array holds every element of another; a NULL element of the other is held by "
        "none.",
        read=Side(
            rules=(
                Rule(
                    "arraycontains($a::anyarray, $b::anyarray)",
                    _ch_only(
                        "CAST(hasAll(arrayFlatten($a), arrayFlatten($b)) AND "
                        "NOT has(arrayFlatten($b), NULL) AS Nullable(Bool))"
                    ),
                ),
            )
        ),
    ),
    Feature(
        "arraycontained",
        FAMILY,
        "Whether every element of an array is held by another; a NULL element is held by none.",
        read=Side(
            rules=(
                Rule(
                    "arraycontained($a::anyarray, $b::anyarray)",
                    _ch_only(
                        "CAST(hasAll(arrayFlatten($b), arrayFlatten($a)) AND "
                        "NOT has(arrayFlatten($a), NULL) AS Nullable(Bool))"
                    ),
                ),
            )
        ),
    ),
    Feature(
        "arrayoverlap",
        FAMILY,
        "Whether two arrays hold an element in common; a NULL element is in common with none.",
        read=Side(
            rules=(
                Rule(
                    "arrayoverlap($a::anyarray, $b::anyarray)",
                    _ch_only(
                        "CAST(hasAny(arrayFilter(_x -> _x IS NOT NULL, arrayFlatten($a)), "
                        "arrayFilter(_x -> _x IS NOT NULL, arrayFlatten($b))) AS Nullable(Bool))"
                    ),
                ),
            )
        ),
    ),
    Feature(
        "array_remove",
        FAMILY,
        "An array without each element equal to a value, a NULL equal to a NULL.",
        read=Side(
            rules=(
                Rule(
                    "array_remove($a::anycompatiblearray, $v::anycompatible)",
                    {**_ch_only(""), "clickhouse": Rewrite(_removed)},
                ),
            )
        ),
    ),
)

FEATURES: tuple[Feature, ...] = CONTAINMENT
