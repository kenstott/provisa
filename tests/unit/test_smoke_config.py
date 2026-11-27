# Copyright (c) 2026 Kenneth Stott
# Canary: 197c15db-c88a-4458-a395-97c0b729cba1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The smoke check's own configuration (tests/smoke/smoke.yaml, REQ-1989) registers each file source's
table where every engine reads it. The installed host runs the smoke on Trino, whose ``file`` catalog
exposes a single-file csv/parquet source under the connector's fixed schema with the file's basename
as the table (TrinoParquetConnector, REQ-1732/REQ-1730); a table registered under any other schema
reads on DuckDB and fails on Trino with SCHEMA_NOT_FOUND."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from provisa.core.models import Source
from provisa.federation.trino_connectors import TrinoCsvConnector, TrinoParquetConnector

SMOKE = Path(__file__).resolve().parents[2] / "tests" / "smoke"
CONNECTORS = {"parquet": TrinoParquetConnector(), "csv": TrinoCsvConnector()}


def _config() -> dict:
    return yaml.safe_load((SMOKE / "smoke.yaml").read_text())


def test_every_file_table_is_registered_where_trino_exposes_it():
    config = _config()
    sources = {s["id"]: s for s in config["sources"] if s["type"] in CONNECTORS}
    assert sources, "the smoke config reads a file source"
    for table in config["tables"]:
        declared = sources.get(table["source_id"])
        if declared is None:
            continue
        source = Source(id=declared["id"], type=declared["type"], path="/files/x/products.parquet")
        schema = CONNECTORS[declared["type"]].details(source)["schema-name"]
        stem = os.path.splitext(os.path.basename(declared["path"]))[0]
        assert (table["schema"], table["table"]) == (schema, stem), table


def test_the_parquet_check_queries_the_registered_schema():
    config = _config()
    table = next(t for t in config["tables"] if t["source_id"] == "smoke-parquet")
    module = (SMOKE / "test_main_is_startable.py").read_text()
    assert f"FROM {table['schema']}.{table['table']} WHERE id = 1" in module
