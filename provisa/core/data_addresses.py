# Copyright (c) 2026 Kenneth Stott
# Canary: 8c41e7d2-5b93-4f0a-a6d8-1e2f9c7b3a50
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a coordinator-served UI sends its data calls (REQ-1916, amended 2026-11-02).

A coordinator serves the UI but no data requests, so the UI's data calls go to a data node the
user picks in a browser setting. The coordinator offers the list for its region, like an OpenAPI
document's servers list: the addresses the platform declares for the region
(``platform.regions[].data_addresses``), else the region's live query and every nodes, each at the
address it registered (``server.node_url``). An every node serves its own UI's data calls, so its
UI is offered nothing and uses its own origin."""

# Requirements: REQ-1916

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from provisa.core import process_mode, process_region
from provisa.core.regions import DEFAULT_REGION

if TYPE_CHECKING:
    from provisa.core.database import Database
    from provisa.core.regions import PlatformConfig

# The modes whose nodes serve data requests.
_DATA_MODES = frozenset({process_mode.QUERY, process_mode.EVERY})


def ui_chooses() -> bool:
    """Whether this node's UI picks a data node: a coordinator serves no data requests, an every
    node serves its own UI's."""
    return process_mode.mode() == process_mode.COORDINATOR


def declared(platform: "PlatformConfig", region: str) -> list[str]:
    """The data addresses the platform declares for ``region`` (none for the implicit region)."""
    for declared_region in platform.regions:
        if declared_region.id == region:
            return list(declared_region.data_addresses)
    return []


def registered(live: "list[dict[str, Any]]", region: str) -> list[str]:
    """The addresses of the live query and every nodes of ``region`` in the node list ``live``,
    oldest node first, each once; a node that registered no address is not offered."""
    in_region = None if region == DEFAULT_REGION else region
    out: list[str] = []
    for node in live:
        address = node.get("address")
        if (
            node["mode"] in _DATA_MODES
            and node.get("region") == in_region
            and address
            and address not in out
        ):
            out.append(address)
    return out


async def for_ui(platform: "PlatformConfig", db: "Database") -> list[str]:
    """The data addresses this node's UI is offered: none on an every node; on a coordinator the
    region's declared list, else its registered nodes."""
    if not ui_chooses():
        return []
    region = process_region.region()
    addresses = declared(platform, region)
    if addresses:
        return addresses
    # REQ-1916 (amended 2026-11-02, "registered nodes when none are declared"): with no declared
    # addresses, the region's registered live query and every nodes are the list.
    from provisa.core.platform_state import nodes

    return registered(await nodes.live(db), region)
