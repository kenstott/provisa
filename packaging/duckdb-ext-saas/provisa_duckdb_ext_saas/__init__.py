# Copyright (c) 2026 Kenneth Stott
# Canary: 9c21e0b7-44df-4a8b-8e3a-3f6d2b1c7d95
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Locator for the DuckDB extension blobs of the saas part of provisa-duckdb-ext (airport, mongo, firebird, gsheets).

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
