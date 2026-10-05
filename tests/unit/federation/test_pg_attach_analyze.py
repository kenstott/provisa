# Copyright (c) 2026 Kenneth Stott
# Canary: 830e6085-1a6d-4fc8-9b5c-e7e0ff722618
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1902: attach_source ANALYZEs each foreign table once per source, so the planner never costs
it against an FDW's placeholder row estimate. Fake cursor, no live Postgres."""

from __future__ import annotations

import types

from provisa.federation.pg_runtime import PgFederationRuntime


class _Cur:
    def __init__(self, log: list[str]) -> None:
        self._log = log

    def execute(self, sql: str, params=None) -> None:
        self._log.append(sql)

    def fetchone(self):
        return None


class _Con:
    def __init__(self, log: list[str]) -> None:
        self._log = log

    def cursor(self):
        return _Cur(self._log)


def _runtime(details: dict, log: list[str]) -> PgFederationRuntime:
    rt = PgFederationRuntime.__new__(PgFederationRuntime)
    rt._con = _Con(log)
    rt._raw_attached = set()
    rt._engine = types.SimpleNamespace(
        resolve=lambda source: types.SimpleNamespace(details=details)
    )
    return rt


def _source(table: str = "orders", source_id: str = "bench-pg"):
    return types.SimpleNamespace(
        id=source_id,
        # REQ-1266/1529: the catalog name the engine names its attach objects (and the live
        # view's schema) after — the attach view of a source carries it.
        catalog=source_id.replace("-", "_"),
        table_name=table,
        schema_name="public",
        type=types.SimpleNamespace(value="postgresql"),
        columns=[("id", "bigint")],
    )


def _analyzes(log: list[str]) -> list[str]:
    return [s for s in log if s.startswith("ANALYZE")]


def test_an_imported_foreign_table_is_analyzed_right_after_attach() -> None:
    log: list[str] = []
    rt = _runtime(
        {
            "attach_ddl": ["IMPORT FOREIGN SCHEMA public FROM SERVER s INTO fdw_pg"],
            "local_schema": "fdw_pg",
        },
        log,
    )
    rt.attach_source(_source())
    assert _analyzes(log) == ['ANALYZE "fdw_pg"."orders"']
    assert log.index("IMPORT FOREIGN SCHEMA public FROM SERVER s INTO fdw_pg") < log.index(
        'ANALYZE "fdw_pg"."orders"'
    )


def test_a_second_attach_of_the_same_source_does_not_re_analyze() -> None:
    log: list[str] = []
    rt = _runtime(
        {
            "attach_ddl": ["IMPORT FOREIGN SCHEMA public FROM SERVER s INTO fdw_pg"],
            "local_schema": "fdw_pg",
        },
        log,
    )
    rt.attach_source(_source())
    rt.attach_source(_source())
    assert len(_analyzes(log)) == 1


def test_a_file_fdw_foreign_table_is_analyzed_on_first_attach_only() -> None:
    log: list[str] = []
    rt = _runtime(
        {
            "server_ddl": ["CREATE SERVER csvs FOREIGN DATA WRAPPER file_fdw"],
            "server": "csvs",
            "table_options": "OPTIONS (filename '/x.csv')",
        },
        log,
    )
    rt.attach_source(_source("people", "bench-csv"))
    rt.attach_source(_source("people", "bench-csv"))
    assert _analyzes(log) == ['ANALYZE "csvs__people"']
