# Copyright (c) 2026 Kenneth Stott
# Canary: 5aa63c1f-1b26-4f3a-9d6e-0b5c8f7a1e42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Locator for the DuckDB extension blobs of the warehouse part of provisa-duckdb-ext (delta, bigquery, snowflake, motherduck, mssql).

The blobs live under ``_ext/<raw_duckdb_version>/<platform>/<name>.duckdb_extension`` (the exact layout
DuckDB's ``extension_directory`` expects). ``provisa.federation.duckdb_extensions`` reads ``ext_root()``
to stage the running platform's binaries offline — no ``extensions.duckdb.org`` round trip.
"""

from __future__ import annotations

from pathlib import Path

__version__ = "0.1.2"


def ext_root() -> Path:
    """Absolute path to the embedded extension tree (``<version>/<platform>/*.duckdb_extension``)."""
    return Path(__file__).resolve().parent / "_ext"
