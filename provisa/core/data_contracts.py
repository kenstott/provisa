# Copyright (c) 2026 Kenneth Stott
# Canary: 4a9aad62-6628-4f7f-bd31-7e4a596d60b6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A data product member's contract, its output port (REQ-2158): which one a save stores.

A contract's schema is its member table's columns: a contract holds no column selection of its
own (no promised columns), and a product offering a subset of a table makes a view of that subset
its member. Its promises are the product's checks on the member (REQ-2157), so a member's columns
changing refuses nothing here.

A member with no contract is given a draft at FIRST_CONTRACT_VERSION. A table in no product, and a
checker table (whose product is its owner, not a product it is a member of, REQ-2157), has none.
"""

# Requirements: REQ-2158

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from provisa.core.models import FIRST_CONTRACT_VERSION, DataContract


class ContractRefused(ValueError):
    """A save that would break a member's contract, named."""


def contract_id(product_id: str, schema_name: str, table_name: str) -> str:
    """A contract's id: its product and the member it describes."""
    return f"{product_id}/{schema_name}.{table_name}"


def draft() -> DataContract:
    """A new member's contract: a draft, its schema its member's columns."""
    return DataContract(version=FIRST_CONTRACT_VERSION, created_at=datetime.now(timezone.utc))


def created_as(previous: DataContract, contract: DataContract) -> DataContract:
    """``contract`` replacing ``previous``, created when ``previous`` was (REQ-2118): a new version
    is the same contract, so its contractCreatedTs is never reset by a save."""
    return contract.model_copy(update={"created_at": previous.created_at})


def for_save(
    *,
    product_id: str | None,
    is_checker: bool,
    declared: DataContract | None,
    stored: dict[str, Any] | None,
    stored_product_id: str | None,
) -> DataContract | None:
    """The contract a table's save stores: none outside a product (or for a checker); the one
    declared, else the one stored for the same product, else a draft."""
    if product_id is None or is_checker:
        return None
    previous = (
        DataContract.model_validate(stored)
        if stored is not None and stored_product_id == product_id
        else None
    )
    if previous is not None and declared is not None:
        declared = created_as(previous, declared)
    elif declared is not None and declared.created_at is None:
        # A contract declared for a new member is created by this save.
        declared = declared.model_copy(update={"created_at": datetime.now(timezone.utc)})
    return declared or previous or draft()
