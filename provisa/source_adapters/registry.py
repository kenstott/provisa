# Copyright (c) 2026 Kenneth Stott
# Canary: a3b4c5d6-e7f8-9012-3456-789012a0123b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Source adapter registry — maps source_type string to adapter module (REQ-252).

Each adapter module must expose:
- generate_catalog_properties(config) -> dict[str, str]
- generate_table_definitions(config) -> list[dict]
- discover_schema(...) -> list[dict]  (optional)
"""

from __future__ import annotations

import importlib
import logging
from types import ModuleType

log = logging.getLogger(__name__)

_ADAPTER_MAP: dict[str, str] = {
    "redis": "provisa.redis.source",
    "mongodb": "provisa.mongodb.source",
    "elasticsearch": "provisa.elasticsearch.source",
    "cassandra": "provisa.cassandra.source",
    "prometheus": "provisa.prometheus.source",
    "accumulo": "provisa.accumulo.source",
}

_loaded: dict[str, ModuleType] = {}


def get_adapter(source_type: str) -> ModuleType:
    """Look up and return the adapter module for a source type.

    Raises KeyError if the source type is not registered.
    """
    if source_type in _loaded:
        return _loaded[source_type]

    module_path = _ADAPTER_MAP.get(source_type)
    if module_path is None:
        raise KeyError(f"Unknown source type: {source_type!r}")

    module = importlib.import_module(module_path)
    _loaded[source_type] = module
    return module


def registered_types() -> list[str]:
    """Return sorted list of registered source type names."""
    return sorted(_ADAPTER_MAP.keys())


def register_adapter(source_type: str, module_path: str) -> None:
    """Register a custom adapter module for a source type."""
    _ADAPTER_MAP[source_type] = module_path
    _loaded.pop(source_type, None)
