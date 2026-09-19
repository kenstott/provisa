# Copyright (c) 2026 Kenneth Stott
# Canary: 1e6b8d43-7c92-45a1-9f0e-3b5d7c8a2e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Druid source adapter entry point for the Register-Table discovery flow (REQ-1730/REQ-252).

Only ``discover_schema`` is implemented — see provisa.pinot.source's own module doc for why
``generate_catalog_properties``/``generate_table_definitions`` are out of scope here."""

from __future__ import annotations


def discover_schema(columns: list[dict]) -> list[dict]:  # REQ-252
    """Pass through the columns `provisa.druid.fetch.table_columns` already resolved live."""
    return columns
