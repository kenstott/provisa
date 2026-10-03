# Copyright (c) 2026 Kenneth Stott
# Canary: 4f6b0c92-e1d7-4a38-b5c0-93a2d8e61b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The cache tiers as operator settings (REQ-1913): hot tables, Hot replication (when a busy
table is replicated, REQ-826) and the materialized-view default TTL.

Part of the settings declarations (``provisa/core/settings_catalog.py`` lists them all). The
defaults are the config models' own. All of them are read when the tiers are started, so a change
takes effect at the next start.
"""

# Requirements: REQ-1913, REQ-230, REQ-231, REQ-543, REQ-544, REQ-826

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from provisa.core.models import HotTablesConfig, MaterializedViewsConfig, ReplicationConfig
from provisa.core.settings_registry import Setting


def _tier(block: str, model: type[BaseModel], field: str, type_: str, **more: Any) -> Setting:
    """``<block>.<field>``, stated in the config file under the same names."""
    if not more.get("nullable"):
        more.setdefault("default", model.model_fields[field].default)
    return Setting(
        key=f"{block}.{field}",
        card="cache",
        type=type_,
        effect="restart",
        req="REQ-544",
        config_path=(block, field),
        **more,
    )


def _hot(field: str, type_: str, **more: Any) -> Setting:
    return _tier("hot_tables", HotTablesConfig, field, type_, **more)


def _replication_hot(field: str, unit: str) -> Setting:
    """``replication.<field>`` (REQ-826): when a busy table is replicated. Live — the count is
    taken and judged with the value in force at each evaluation."""
    return Setting(
        key=f"replication.{field}",
        card="cache",
        type="int",
        effect="live",
        req="REQ-826",
        env=f"PROVISA_REPLICATION_{field.upper()}",
        config_path=("replication", field),
        default=ReplicationConfig.model_fields[field].default,
        min=1,
        unit=unit,
    )


DECLARED: list[Setting] = [
    _hot("auto_threshold", "int", min=0, unit="rows"),
    # REQ-230: unset, the hot tier's row ceiling is its auto threshold.
    _hot("max_rows", "int", min=1, nullable=True, unit="rows"),
    _hot("max_bytes", "int", min=1, unit="bytes"),
    # REQ-231: unset, the hot tier refreshes on the materialized-view default TTL.
    _hot("refresh_interval", "int", min=1, nullable=True, unit="seconds"),
    _replication_hot("hot_threshold", "statements"),
    _replication_hot("hot_interval", "seconds"),
    _replication_hot("hot_max_rows", "rows"),
    _tier(
        "materialized_views", MaterializedViewsConfig, "default_ttl", "int", min=1, unit="seconds"
    ),
]
