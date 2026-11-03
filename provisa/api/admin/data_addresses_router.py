# Copyright (c) 2026 Kenneth Stott
# Canary: 2f7a9d14-c6e3-4b81-9d05-7a3e1b8c6f29
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The data addresses a coordinator-served UI picks from (REQ-1916, amended 2026-11-02).

``choose`` says whether the UI picks a data node at all (a coordinator's UI does; an every node's
sends its data calls to its own origin and shows no setting), and ``addresses`` is the list for
this node's region (:mod:`provisa.core.data_addresses`). Part of the UI's backend: a query node
refuses it, like every other admin route."""

# Requirements: REQ-1916

from __future__ import annotations

from fastapi import APIRouter

from provisa.core import data_addresses

router = APIRouter()


@router.get("/admin/data-addresses")
async def get_data_addresses() -> dict:
    from provisa.api.app import state

    assert state.config is not None  # the UI's backend is served once the config is loaded
    assert state.platform_state_db is not None  # brought up with the control planes at boot
    return {
        "choose": data_addresses.ui_chooses(),
        "addresses": await data_addresses.for_ui(state.config.platform, state.platform_state_db),
    }
