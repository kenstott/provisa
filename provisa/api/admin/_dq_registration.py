# Copyright (c) 2026 Kenneth Stott
# Canary: 8c41d0b7-52ea-4f39-9a6d-71b2c4e05f8a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Registering a checker table through the admin API (REQ-1443).

The derivation itself is :func:`provisa.dq.registration.derive_checker_table`, shared with the YAML
loader. What lives here is the control-plane half of its inputs: the source's type and the governed
tables the contract's dataset may resolve against, both read from ``registered_tables``/``sources``
rather than from a ProvisaConfig, because a registerTable mutation has no config in hand.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import select

from provisa.core.schema_org import sources

if TYPE_CHECKING:
    from provisa.core.database import Connection


async def apply_dq_registration(conn: "Connection", model) -> None:
    """Derive ``model``'s registration from its contract when its source is a checker.

    A no-op for every other source type EXCEPT that a contract on one is rejected — the same
    whole-config rule ``_validate_dq_contracts`` applies, so the two surfaces cannot disagree about
    which tables may carry a contract. Raises :class:`ValueError`; the callers turn that into a
    failed ``MutationResult`` rather than a 500.
    """
    from provisa.api.app import state
    from provisa.dq.contract import CHECKERS
    from provisa.dq.registration import (
        check_contract_target,
        derive_checker_table,
        is_checker_source_type,
    )

    result = await conn.execute_core(select(sources.c.type).where(sources.c.id == model.source_id))
    fetched = result.fetchone()
    source_type = fetched._mapping["type"] if fetched is not None else None
    if not is_checker_source_type(source_type):
        if model.dq_contract:
            raise ValueError(
                f"Table {model.table_name!r}: dq_contract is only valid on a data-quality checker "
                f"source ({sorted(CHECKERS)}), not on source type {str(source_type)!r}"
            )
        return
    dataset = derive_checker_table(model, source_type)
    check_contract_target(model, dataset, state.contexts)
