# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4b7c21-6e83-4a5f-b0d9-1c8f6a2e35d0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Pinot source adapter entry point for the Register-Table discovery flow (REQ-1730/REQ-252).

Only ``discover_schema`` is implemented — ``generate_catalog_properties``/
``generate_table_definitions`` are the CONFIG-YAML declarative-source path's own hooks
(core/trino_catalog_files.py), explicitly dispatched only for redis/elasticsearch/prometheus
today; pinot has never been a config-declarable source type and this adapter is registered here
solely so ``discovery_schema.py``'s ``get_adapter("pinot")`` finds a module with
``discover_schema``."""

from __future__ import annotations


def discover_schema(columns: list[dict]) -> list[dict]:  # REQ-252
    """Pass through the columns `provisa.pinot.fetch.table_columns` already resolved live — the
    adapter-registry seam (discovery_schema.py's ``_call_discover``) expects a module-level
    ``discover_schema(...)`` it can call generically; the real work already happened by the time
    this runs."""
    return columns
