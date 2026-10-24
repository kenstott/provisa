# Copyright (c) 2026 Kenneth Stott
# Canary: 10dfb26e-d7fe-470c-8ea0-09b02c88fe47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A table's parameters and the one rule for a required one (REQ-1986).

A parameter is a value a caller passes to the source to read the table, held as a parameter
column (``_nf_<name>``, ``native_filter_type`` naming how it reaches the source). It is required
when the source requires it (``native_filter_required``, recorded at registration) or when a
steward made it required for the table (``required_by_steward``). A read that leaves a required
parameter without a value is refused before anything is sent to the source, naming the table and
the parameter -- the same for every source kind.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

# How a parameter reaches the source (core/models.py Column.native_filter_type).
# partition_key (REQ-1982): a Cosmos DB container's partition key, which narrows a read to one
# partition.
PARAMETER_KINDS = frozenset(
    {"path_param", "query_param", "graphql_arg", "grpc_input", "query_arg", "partition_key"}
)

NF_PREFIX = "_nf_"
# The key under which a native GraphQL argument records the parameter it fills (its argument name
# is ``_<name>`` when ``<name>`` is also one of the field's own).
PARAMETER_EXTENSION = "provisa_parameter"


def _get(column: Any, key: str) -> Any:
    return column.get(key) if isinstance(column, Mapping) else getattr(column, key, None)


def column_name(column: Any) -> str:
    name = _get(column, "column_name")
    return name if name is not None else _get(column, "name")


def is_parameter(column: Any) -> bool:
    return _get(column, "native_filter_type") is not None


def is_required(column: Any) -> bool:
    """Whether a parameter must be given: the source requires it, or a steward made it
    required."""
    return bool(_get(column, "native_filter_required")) or bool(_get(column, "required_by_steward"))


def parameter_name(column: Any) -> str:
    """The parameter as a caller names it: the column's name without its ``_nf_`` prefix."""
    name = column_name(column)
    return name[len(NF_PREFIX) :] if name.startswith(NF_PREFIX) else name


def _canonical(name: str) -> str:
    from provisa.compiler.naming import apply_sql_name

    # A statement names a parameter by its column, which the naming authority may have
    # re-cased and GraphQL may have prefixed with ``_`` (a collision with a field): both sides
    # reduce to one sql name.
    return apply_sql_name(name.lstrip("_"))


def missing_parameters(
    columns: Iterable[Any], given: Mapping[str, Any] | None, fed: Callable[[str], bool]
) -> list[str]:
    """The required parameters of a table's ``columns`` that a read gives no value: neither in
    ``given`` (the statement's arguments, by parameter name) nor through a join (``fed`` answers
    for a parameter column's name)."""
    have = {_canonical(k) for k, v in (given or {}).items() if v is not None}
    # Each parameter once: a remote field that is also an argument is held both as its column and
    # as its ``_nf_`` column, one parameter ("requires the parameter(s) ['id', 'id']").
    return list(
        dict.fromkeys(
            parameter_name(c)
            for c in columns
            if is_parameter(c)
            and is_required(c)
            and _canonical(parameter_name(c)) not in have
            and not fed(column_name(c))
        )
    )


class ParameterRequired(ValueError):
    """A read of a table that leaves a required parameter without a value: refused before anything
    is sent to the source, naming the table and the parameters (REQ-1986)."""

    def __init__(self, table: str, reason: str) -> None:
        self.table = table
        super().__init__(f"Table {table!r} {reason}")


def missing_reason(missing: list[str]) -> str:
    """Why a read of a table is refused, after the table's name (``apply_dropped_tables``)."""
    return (
        f"requires the parameter(s) {missing}: give each a value (a WHERE condition on its "
        "column in SQL, an argument in GraphQL)"
    )
