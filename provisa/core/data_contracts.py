# Copyright (c) 2026 Kenneth Stott
# Canary: 4a9aad62-6628-4f7f-bd31-7e4a596d60b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product member's contract, its output port (REQ-2158): which one a save stores, and the
two rules that keep it honest.

* A contract that stops promising a column, or promises it as another type, is a new version of
  it: the same version is refused, naming the contract.
* A member provides every column its contract promises, of the promised type: a save that drops
  or retypes one is refused, naming the contract. Its owner releases a new version without the
  column first, or, in a configuration, in the same save.

A member with no contract is given a draft at FIRST_CONTRACT_VERSION promising its columns as
they are. A table in no product, and a checker table (whose product is its owner, not a product
it is a member of, REQ-2157), has none.
"""

# Requirements: REQ-2158

from __future__ import annotations

from typing import Any

from provisa.core.models import FIRST_CONTRACT_VERSION, ContractColumn, DataContract
from provisa.core.numeric_type import ReportedNumericMalformed, stored_type_of


class ContractRefused(ValueError):
    """A save that would break a member's contract, named."""


def contract_id(product_id: str, schema_name: str, table_name: str) -> str:
    """A contract's id: its product and the member it describes."""
    return f"{product_id}/{schema_name}.{table_name}"


def draft(column_types: dict[str, str]) -> DataContract:
    """A new member's contract: a draft promising its columns as they are."""
    return DataContract(
        version=FIRST_CONTRACT_VERSION,
        columns=[ContractColumn(name=name, type=t) for name, t in column_types.items()],
    )


def as_stored(cid: str, contract: DataContract) -> DataContract:
    """``contract`` with each promised type spelled as a column's type is stored (REQ-1961), so a
    promise compares with the column it is of."""
    columns = []
    for c in contract.columns:
        try:
            columns.append(c.model_copy(update={"type": stored_type_of(c.type)}))
        except ReportedNumericMalformed as malformed:
            raise ContractRefused(
                f"contract {cid} promises {c.name} as {c.type}, which is no column type"
            ) from malformed
    return contract.model_copy(update={"columns": columns})


def _same_type(a: str | None, b: str) -> bool:
    """Whether two stored column types are one: SQL spells a type in any case."""
    return a is not None and a.lower() == b.lower()


def _breaks(old: DataContract, new: DataContract) -> list[str]:
    """What ``new`` stops promising of ``old``: each column it drops or retypes."""
    kept = {c.name: c.type for c in new.columns}
    return [
        f"{c.name} ({c.type})" if c.name not in kept else f"{c.name} ({c.type} -> {kept[c.name]})"
        for c in old.columns
        if not _same_type(kept.get(c.name), c.type)
    ]


def require_new_version(cid: str, old: DataContract, new: DataContract) -> None:
    """``new`` replacing ``old``: a contract that stops promising a column is a new version."""
    broken = _breaks(old, new)
    if broken and new.version == old.version:
        raise ContractRefused(
            f"contract {cid} version {old.version} stops promising {', '.join(broken)}: "
            "a change that breaks a contract is a new version of it"
        )


def require_provided(cid: str, contract: DataContract, column_types: dict[str, str]) -> None:
    """The member provides every column ``contract`` promises, of the promised type."""
    missing = [
        f"{c.name} ({c.type})"
        if c.name not in column_types
        else f"{c.name} ({c.type}, now {column_types[c.name]})"
        for c in contract.columns
        if not _same_type(column_types.get(c.name), c.type)
    ]
    if missing:
        raise ContractRefused(
            f"contract {cid} version {contract.version} promises {', '.join(missing)}: release "
            "a new version of the contract without it first, or in the same save"
        )


def for_save(
    *,
    product_id: str | None,
    is_checker: bool,
    schema_name: str,
    table_name: str,
    declared: DataContract | None,
    stored: dict[str, Any] | None,
    stored_product_id: str | None,
    column_types: dict[str, str],
) -> DataContract | None:
    """The contract a table's save stores: none outside a product (or for a checker); the one
    declared, else the one stored for the same product, else a draft of its columns. Refused
    (:class:`ContractRefused`) when it would break the stored one or the columns break it."""
    if product_id is None or is_checker:
        return None
    cid = contract_id(product_id, schema_name, table_name)
    previous = (
        DataContract.model_validate(stored)
        if stored is not None and stored_product_id == product_id
        else None
    )
    if declared is not None:
        declared = as_stored(cid, declared)
    contract = declared or previous or draft(column_types)
    if previous is not None and declared is not None:
        require_new_version(cid, previous, declared)
    require_provided(cid, contract, column_types)
    return contract
