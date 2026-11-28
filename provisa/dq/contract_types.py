# Copyright (c) 2026 Kenneth Stott
# Canary: 8c3f1d26-5a9e-4b70-b2d4-6e0a7f19c853
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every contract's built-in promise check: the declared logical type (REQ-2158, amended 2026-10-28,
THE DECLARED LOGICAL TYPE IS PROMISED BY DEFAULT; kept by the 2026-11-27 amendment).

A contract's current version declares each field's ODCS logicalType: the logical type of the type
the version states for it (:func:`logical_type`, the one map the ODCS export publishes too). The
check fails when a column's live type no longer fits that logical type -- a date become a string, a
number become text, a column gone -- and names each such column with its declared logicalType and
live type. A change of width, length or precision within the logical type passes: that stays an
accepted risk unless one of the product's own checks covers it. A failing type check is handled as
any failing promise check; nothing is re-versioned by it.
"""

# Requirements: REQ-2158

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from sqlalchemy import select

if TYPE_CHECKING:
    from provisa.core.models import OfferContract

#: Reads a source table's columns as the source lists them now, each with its ``name`` and
#: ``data_type``: provisa.api.admin.schema_query.resolve_available_columns_metadata.
ColumnReader = Callable[[str, str, str], Awaitable[Sequence[Any]]]

#: The built-in check's name, as a contract's promise results list it.
CONTRACT_TYPE_CHECK = "contract_logical_type"

# A column's declared type as ODCS's logical types name it. A type outside these is published by
# its physical name alone: guessing a logical type would misstate it.
_LOGICAL = (
    (re.compile(r"\[\]$|^array"), "array"),
    (re.compile(r"^(small|big)?int(eger)?\d*$|^(tiny|medium)int$|^serial|^long$"), "integer"),
    (re.compile(r"^(numeric|decimal|number|double|float|real)"), "number"),
    (re.compile(r"^bool"), "boolean"),
    (re.compile(r"^timestamp|^datetime"), "timestamp"),
    (re.compile(r"^date$"), "date"),
    (re.compile(r"^time(\b|$)"), "time"),
    (re.compile(r"^(json|jsonb|object|struct|map|variant)"), "object"),
    (re.compile(r"^(text|string|varchar|char|character|uuid|citext|nvarchar|nchar)"), "string"),
)

#: The logical types whose values another one's always are: every integer is a number.
_HELD_BY = {"number": {"integer"}}


def logical_type(data_type: str) -> str | None:
    """The ODCS logical type of a declared column type, or None when it has none to name."""
    lowered = data_type.strip().lower()
    return next((logical for pattern, logical in _LOGICAL if pattern.search(lowered)), None)


def fits(declared: str, live_type: str | None) -> bool:
    """Whether a column of ``live_type`` (None: the column is gone) holds what the ``declared``
    logical type promises."""
    if live_type is None:
        return False
    live = logical_type(live_type)
    return live == declared or live in _HELD_BY.get(declared, set())


@dataclass(frozen=True)
class TypeMismatch:
    """A column whose live type no longer fits its declared logicalType; ``live`` None: gone."""

    column: str
    declared: str
    live: str | None

    @property
    def message(self) -> str:
        live = f"live type {self.live}" if self.live is not None else "live absent"
        return f"column {self.column}: declared logicalType {self.declared}, {live}"


@dataclass(frozen=True)
class TypeCheck:
    """One run of the built-in check: its outcome and every column that failed it."""

    outcome: Literal["pass", "fail"]
    mismatches: list[TypeMismatch] = field(default_factory=list)
    name: str = CONTRACT_TYPE_CHECK


def check_types(declared: Iterable[tuple[str, str]], live: Mapping[str, str]) -> TypeCheck:
    """The check over ``declared`` (each field's name and the type its version states) against
    ``live`` (each column's live type, by the same name). A field whose type names no logical type
    declares none, so promises none."""
    mismatches = [
        TypeMismatch(column=name, declared=logical, live=live.get(name))
        for name, data_type in declared
        if (logical := logical_type(data_type)) is not None and not fits(logical, live.get(name))
    ]
    return TypeCheck(outcome="fail" if mismatches else "pass", mismatches=mismatches)


def check_contract(contract: "OfferContract", live: Mapping[str, str]) -> TypeCheck:
    """The check of ``contract``'s current version against its dataset's ``live`` column types,
    keyed by the names its fields carry (a column's SQL alias where one is set)."""
    return check_types(((f.name, f.type) for f in contract.terms.fields), live)


async def check_dataset(
    conn: Any, dataset: int, read_columns: ColumnReader
) -> dict[int, TypeCheck]:
    """The check of every offer's contract on ``dataset``, by offer id, against the columns its
    source lists now (``read_columns``). A field is matched to its live column by the name it is
    served by: the column's SQL alias where one is set, otherwise its column name."""
    from provisa.core.models import OfferContract
    from provisa.core.schema_org import (
        data_product_offer_contracts,
        registered_tables,
        table_columns,
    )

    contracts = (
        await conn.execute_core(
            select(data_product_offer_contracts.c.offer_id, data_product_offer_contracts.c.contract)
            .where(data_product_offer_contracts.c.table_id == dataset)
            .order_by(data_product_offer_contracts.c.offer_id)
        )
    ).fetchall()
    if not contracts:
        return {}
    table = (
        await conn.execute_core(
            select(
                registered_tables.c.source_id,
                registered_tables.c.schema_name,
                registered_tables.c.table_name,
            ).where(registered_tables.c.id == dataset)
        )
    ).one()
    served_as = {
        row.column_name: row.alias or row.column_name
        for row in (
            await conn.execute_core(
                select(table_columns.c.column_name, table_columns.c.alias).where(
                    table_columns.c.table_id == dataset
                )
            )
        ).fetchall()
    }
    listed = await read_columns(table.source_id, table.schema_name, table.table_name)
    live = {served_as[c.name]: c.data_type for c in listed if c.name in served_as}
    return {
        int(row.offer_id): check_contract(OfferContract.model_validate(row.contract), live)
        for row in contracts
    }
