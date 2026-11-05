# Copyright (c) 2026 Kenneth Stott
# Canary: 9b4e2d71-6c38-4f05-a1e9-2d7f5c8b0e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The keys a table's source declares, read for the table's registration (REQ-2170)."""

from __future__ import annotations

from typing import Any

from provisa.discovery.declared_keys import DeclaredKeys, declared_keys


async def registration_keys(model: Any) -> DeclaredKeys | None:
    """What ``model``'s source declares of its keys, over the source's pooled driver; None when
    the source is not read here (no driver for it in this process, a kind with no reader, or a
    built-in source such as the derived views')."""
    from provisa.api.app import state

    source_type = state.source_types.get(model.source_id)
    if source_type is None:
        return None
    return await declared_keys(
        state.source_pools, source_type, model.source_id, model.schema_name, model.table_name
    )
