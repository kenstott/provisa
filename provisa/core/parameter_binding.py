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

import re
from typing import Any

#: The operator setting that caps the calls one statement makes to a parameterized table.
CALLS_SETTING = "parameters.max_calls_per_statement"

# One definition of an opaque parameter, for the binder and the metadata alike (REQ-1986).
from provisa.core.parameters import REAL_COLUMN_KINDS, is_opaque  # noqa: E402,F401


class ParameterBindingRefused(ValueError):
    """A statement whose condition on a parameter cannot bind it. ``code`` is the stable
    identifier of the refusal and ``params`` what it is about."""

    code: str = "data.parameter_binding_refused"
    params: dict[str, Any]
    #: What pgwire reports it as: invalid_parameter_value, the statement's own mistake.
    sqlstate: str = "22023"


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


class ParameterNotWritten(ParameterBindingRefused):
    """A write of a relationship that joins on a parameter of one of its ends: a parameter is
    bound to a value for the source's call, never written."""

    code = "data.parameter_not_written"

    def __init__(self, parameter: str, relationship: str) -> None:
        super().__init__(
            f"The relationship {relationship} joins on the parameter {parameter}, which a "
            f"statement binds to a value and never writes. A write does not make {relationship}."
        )
        self.params = {"parameter": parameter, "relationship": relationship}


class ParameterRequired(ParameterBindingRefused):
    """A read of a table that leaves a required parameter without a value: refused before anything
    is sent to the source, naming the table and the parameters, under one code on every front."""

    code = "data.parameter_required"

    def __init__(self, table: str, missing: list[str]) -> None:
        from provisa.core.parameters import missing_reason

        self.table = table
        self.missing = list(missing)
        super().__init__(f"Table {table!r} {missing_reason(self.missing)}")
        self.params = {"table": table, "parameters": ", ".join(self.missing)}


# The IR types a parameter's value is checked against before the call, by the base type's
# spelling (core.ir_types.base_type): each with the value kinds it takes. A type not listed takes
# any value, which the source alone can judge.
_INTEGER_TYPES = frozenset(
    {"smallint", "integer", "int", "bigint", "int2", "int4", "int8", "tinyint", "hugeint"}
)
_NUMBER_TYPES = frozenset(
    {"numeric", "decimal", "real", "double precision", "double", "float", "float4", "float8"}
)
_BOOLEAN_TYPES = frozenset({"boolean", "bool"})
_INTEGER_TEXT = re.compile(r"\s*[+-]?\d+\s*\Z")
_BOOLEAN_TEXT = frozenset({"true", "false"})


def _fits(base: str, value: Any) -> bool:
    if base in _INTEGER_TYPES:
        if isinstance(value, bool):
            return False
        if isinstance(value, int):
            return True
        if isinstance(value, float):
            return value.is_integer()
        return isinstance(value, str) and _INTEGER_TEXT.match(value) is not None
    if base in _NUMBER_TYPES:
        if isinstance(value, bool):
            return False
        if isinstance(value, (int, float)):
            return True
        if not isinstance(value, str):
            return False
        try:
            float(value)
        except ValueError:
            return False
        return True
    if base in _BOOLEAN_TYPES:
        return isinstance(value, bool) or (
            isinstance(value, str) and value.strip().lower() in _BOOLEAN_TEXT
        )
    return True


class ParameterWrongType(ParameterBindingRefused):
    """A value given to a parameter that its registered type does not take: refused naming the
    parameter and the type, before the source is called."""

    code = "data.parameter_wrong_type"

    def __init__(self, parameter: str, data_type: str, value: Any) -> None:
        super().__init__(
            f"The parameter {parameter} takes a value of type {data_type}; {value!r} is not one."
        )
        self.params = {"parameter": parameter, "type": data_type, "value": str(value)}


def check_value(parameter: str, data_type: str | None, value: Any) -> None:
    """Refuse ``value`` for ``parameter`` when its registered ``data_type`` does not take it
    (:class:`ParameterWrongType`). A value not known yet (None: a ``$N`` bound when the statement
    runs) and an array parameter's list are the source's to judge."""
    from provisa.core.ir_types import base_type, type_name

    if data_type is None or value is None or isinstance(value, (list, tuple)):
        return
    if not _fits(type_name(base_type(type_name(data_type))), value):
        raise ParameterWrongType(parameter, data_type, value)
