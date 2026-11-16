# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0b7c1e-9a64-4f2b-8e37-2c6f1a9d4b58
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The refusals of binding a table's parameters (REQ-1986).

A statement gives an opaque parameter its value by equality: with one value, with a list of
values, or with another table's column in a join. Each value is one call to the source. Every
other condition on it is refused here, by name, before anything is sent to the source.
"""

# Requirements: REQ-1986

from __future__ import annotations

from typing import Any

#: The operator setting that caps the calls one statement makes to a parameterized table.
CALLS_SETTING = "parameters.max_calls_per_statement"

REAL_COLUMN_KINDS = frozenset({"partition_key", "column_value"})


def is_opaque(column: Any) -> bool:
    """Whether a parameter column is an input of the source's call, whose effect on the rows is
    not known: every kind but a real column of the table (a partition key, a column value)."""
    kind = (
        column.get("native_filter_type")
        if isinstance(column, dict)
        else getattr(column, "native_filter_type", None)
    )
    return kind is not None and kind not in REAL_COLUMN_KINDS


class ParameterBindingRefused(ValueError):
    """A statement whose condition on a parameter cannot bind it. ``code`` is the stable
    identifier of the refusal and ``params`` what it is about."""

    code: str = "data.parameter_binding_refused"
    params: dict[str, Any]


class ParameterByEqualityOnly(ParameterBindingRefused):
    """A condition on an opaque parameter that assigns it no value: a range, a pattern, an
    inequality, a null test, a negation."""

    code = "data.parameter_by_equality_only"

    def __init__(self, parameter: str, form: str) -> None:
        super().__init__(
            f"The parameter {parameter} takes a value by equality: {parameter} = a value, "
            f"{parameter} IN (values), or a join on {parameter}. {form} gives it no value."
        )
        self.params = {"parameter": parameter, "form": form}


class ParameterUnbound(ParameterBindingRefused):
    """A parameter given a value on one branch of an OR and none on another."""

    code = "data.parameter_unbound_on_branch"

    def __init__(self, parameter: str) -> None:
        super().__init__(
            f"The parameter {parameter} is given a value on one branch of an OR and none on "
            f"another. Give {parameter} a value on every branch."
        )
        self.params = {"parameter": parameter}


class ParameterConflict(ParameterBindingRefused):
    """Two different values assigned to one parameter on one branch."""

    code = "data.parameter_conflict"

    def __init__(self, parameter: str, first: Any, second: Any) -> None:
        super().__init__(
            f"The parameter {parameter} is given two different values at once ({first!r} and "
            f"{second!r}). Give it one value, or list the values with IN or OR."
        )
        self.params = {"parameter": parameter, "first": str(first), "second": str(second)}


class ParameterCallsOverCap(ParameterBindingRefused):
    """A statement whose values for a table's parameters need more calls than the cap."""

    code = "data.parameter_calls_over_cap"

    def __init__(self, table: str, calls: int, cap: int) -> None:
        super().__init__(
            f"The statement needs {calls} calls to {table}, one for each value given to its "
            f"parameters, and at most {cap} are made for one statement (the setting "
            f"{CALLS_SETTING}). Give fewer values."
        )
        self.params = {
            "table": table,
            "calls": str(calls),
            "cap": str(cap),
            "setting": CALLS_SETTING,
        }
