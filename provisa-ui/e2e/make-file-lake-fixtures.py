#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 4d0c4cfd-af29-40db-9006-a33cfbcdcacb
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1743: writes a real local Delta Lake table and a real local Iceberg table for
source-to-query-file-lake.spec.ts to register through the actual Sources form -> Register Table
form -> SQL page UI flow (DuckDB engine, DuckDBDeltaConnector/DuckDBIcebergConnector, both
SCAN-mechanism/view_ddl, REQ-899). Mirrors tests/integration/test_duckdb_delta_source_e2e.py's
_make_delta_table and tests/integration/test_embedded_pg_duckdb_iceberg_e2e.py's
_make_iceberg_table exactly, so the fixtures are proven-readable shapes, not new guesses.

Usage: python make-file-lake-fixtures.py <output-dir>
Prints two lines to stdout: the delta table path, then the iceberg table path.
"""

from __future__ import annotations

import glob
import shutil
import sys
from pathlib import Path


def make_delta_table(root: Path) -> str:
    import pyarrow as pa
    from deltalake import write_deltalake

    path = str(root / "delta_pets")
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int32()),
            "name": pa.array(["Fido", "Whiskers", "Rex", "Tweety"]),
            "species": pa.array(["dog", "cat", "dog", "bird"]),
        }
    )
    write_deltalake(path, data)
    return path


def make_iceberg_table(root: Path) -> str:
    from pyiceberg.catalog.sql import SqlCatalog
    import pyarrow as pa

    wh = root / "iceberg_wh"
    wh.mkdir(parents=True, exist_ok=True)
    cat = SqlCatalog("d", uri=f"sqlite:///{wh}/cat.db", warehouse=f"file://{wh}")
    cat.create_namespace("db")
    data = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int32()),
            "name": pa.array(["Fido", "Whiskers", "Rex", "Tweety"]),
            "species": pa.array(["dog", "cat", "dog", "bird"]),
        }
    )
    t = cat.create_table("db.pets", schema=data.schema)
    t.append(data)
    table_root = wh / "db" / "pets"
    latest = sorted(glob.glob(str(table_root / "metadata" / "00001-*.metadata.json")))[-1]
    shutil.copy(latest, table_root / "metadata" / "v1.metadata.json")
    (table_root / "metadata" / "version-hint.text").write_text("1")
    return str(table_root)


def main() -> None:
    out = Path(sys.argv[1])
    out.mkdir(parents=True, exist_ok=True)
    print(make_delta_table(out))
    print(make_iceberg_table(out))


if __name__ == "__main__":
    main()
