# Copyright (c) 2026 Kenneth Stott
# Canary: 84f0b2d6-3a91-4c5e-8d27-6b9e1c40a7f3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The federation engine's sizing as operator settings (REQ-1913, REQ-055).

Part of the settings declarations (``provisa/core/settings_catalog.py`` lists them all). Each is a
top-level config key the engine's own config files are rendered from
(``provisa/api/trino_setup.write_trino_config``); the defaults are ``ProvisaConfig``'s.

They take effect when the ENGINE restarts (``restart_scope="engine"``): a save regenerates the
engine's config files at once from the stored value, and the running engine keeps its own until
it is restarted.
"""

# Requirements: REQ-1913, REQ-055, REQ-250

from __future__ import annotations

from typing import Any

from provisa.core.models import ProvisaConfig
from provisa.core.settings_registry import Setting

# The sizing keys, by the type each is stated in.
SIZING: dict[str, str] = {
    "jvm_heap_gb": "int",
    "query_max_memory": "str",
    "query_max_memory_per_node": "str",
    "query_max_total_memory": "str",
    "fault_tolerant_execution": "bool",
    "fault_tolerant_task_memory": "str",
    "exchange_spool_dir": "str",
}


def _sizing(field: str, type_: str) -> Setting:
    more: dict[str, Any] = {"min": 1, "unit": "GB"} if field == "jvm_heap_gb" else {}
    return Setting(
        key=f"engine.{field}",
        card="engine",
        type=type_,
        effect="restart",
        restart_scope="engine",
        req="REQ-055",
        config_path=(field,),
        default=ProvisaConfig.model_fields[field].default,
        **more,
    )


DECLARED: list[Setting] = [_sizing(field, type_) for field, type_ in SIZING.items()]
