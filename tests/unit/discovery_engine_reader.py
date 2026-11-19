# Copyright (c) 2026 Kenneth Stott
# Canary: 9b2e4c71-5d38-4a06-b1f7-e3c8a2d5f640
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A measure reader whose engine is an in-memory DuckDB: each column of the data is a table of its
own, and the measure's query runs on it as the governed pipeline would run it (REQ-413)."""

# Requirements: REQ-413

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import duckdb

from provisa.discovery import measure
from provisa.discovery.columns import TableFacts
from provisa.discovery.measure import Column


class EngineReader:
    """Columns ``data[(table, column)]`` in an engine; each query kept with the tables it read."""

    def __init__(self, data: dict[tuple[str, str], list[Any]], sql_type: str = "VARCHAR"):
        self.data = data
        self.queries: list[tuple[tuple[str, ...], str]] = []
        self._db = duckdb.connect()
        self._db.execute("CREATE SCHEMA city")
        for (table, column), values in data.items():
            relation = f'city."{table}__{column}"'
            self._db.execute(f'CREATE TABLE {relation} ("{column}" {sql_type})')
            if values:
                self._db.executemany(f"INSERT INTO {relation} VALUES (?)", [(v,) for v in values])

    def column(self, table: TableFacts, column: str) -> Column | measure.Unmeasured:
        if table.reduced:
            return measure.NOT_READ
        return Column(f'city."{table.table_name}__{column}"', f'"{column}"')

    async def rows(self, tables: Sequence[TableFacts], sql: str) -> list[tuple]:
        self.queries.append((tuple(t.table_name for t in tables), sql))
        return self._db.execute(sql).fetchall()
