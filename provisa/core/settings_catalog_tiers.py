# Copyright (c) 2026 Kenneth Stott
# Canary: 4f6b0c92-e1d7-4a38-b5c0-93a2d8e61b47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The cache tiers as operator settings (REQ-1913): hot tables, warm tables and the
materialized-view default TTL.

Part of the settings declarations (``provisa/core/settings_catalog.py`` lists them all). The
defaults are the config models' own. All of them are read when the tiers are started, so a change
takes effect at the next start.
"""

# Requirements: REQ-1913, REQ-230, REQ-231, REQ-240, REQ-543, REQ-544

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from provisa.core.models import HotTablesConfig, MaterializedViewsConfig, WarmTablesConfig
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


def _warm(field: str, type_: str, **more: Any) -> Setting:
    return _tier("warm_tables", WarmTablesConfig, field, type_, **more)


DECLARED: list[Setting] = [
    _hot("auto_threshold", "int", min=0, unit="rows"),
    # REQ-230: unset, the hot tier's row ceiling is its auto threshold.
    _hot("max_rows", "int", min=1, nullable=True, unit="rows"),
    _hot("max_bytes", "int", min=1, unit="bytes"),
    # REQ-231: unset, the hot tier refreshes on the materialized-view default TTL.
    _hot("refresh_interval", "int", min=1, nullable=True, unit="seconds"),
    _warm("query_threshold", "int", min=1, unit="queries"),
    _warm("max_rows", "int", min=1, unit="rows"),
    _warm("refresh_interval", "int", min=1, unit="seconds"),
    _warm("fs_cache_enabled", "bool"),
    _warm("fs_cache_directories", "str"),
    _warm("fs_cache_max_sizes", "str"),
    _tier(
        "materialized_views", MaterializedViewsConfig, "default_ttl", "int", min=1, unit="seconds"
    ),
]
