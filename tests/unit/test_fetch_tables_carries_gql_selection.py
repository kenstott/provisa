# Copyright (c) 2026 Kenneth Stott
# Canary: bf909f79-6cb2-4fee-901f-118aa691ca8a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tables a server reads its statements with carry each column's ``gql_selection``.

``fetch_tables`` is what a running server holds as ``state.tables``: the registration a remote
GraphQL read builds its query from (``materialization._mat_gql_remote_table``). It names the
column fields it reads one by one, and did not name ``gql_selection`` -- so the demo's registered
``schedules.employee_id`` (``employee_id: employee { id }``) reached the remote read with no
selection and was asked for as ``employeeId``, a field ``Schedule`` does not have.
"""

# Requirements: REQ-308, REQ-413

from __future__ import annotations

from collections import defaultdict

import pytest

from provisa.api.admin.db_queries import fetch_tables

pytestmark = pytest.mark.asyncio


class _Conn:
    """Answers fetch_tables' statements from one stored remote GraphQL table."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def fetch(self, sql: str, *args):
        self.statements.append(sql)
        if "FROM table_write_grants" in sql:
            return []
        if "FROM table_columns" not in sql and "FROM registered_tables" in sql:
            row = defaultdict(lambda: None)
            row.update(
                id=1,
                source_id="graphql-demo",
                domain_id="shelter",
                schema_name="graphql",
                table_name="schedules",
            )
            return [row]
        if "gql_selection" not in sql:
            return [
                defaultdict(lambda: None, table_id=1, column_name="id", scope="domain"),
                defaultdict(lambda: None, table_id=1, column_name="employee_id", scope="domain"),
            ]
        return [
            defaultdict(lambda: None, table_id=1, column_name="id", scope="domain"),
            defaultdict(
                lambda: None,
                table_id=1,
                column_name="employee_id",
                scope="domain",
                gql_selection="employee_id: employee { id }",
            ),
        ]


async def test_each_column_is_fetched_with_its_stored_selection():
    (table,) = await fetch_tables(_Conn())  # type: ignore[arg-type]
    assert {c["column_name"]: c["gql_selection"] for c in table["columns"]} == {
        "id": None,
        "employee_id": "employee_id: employee { id }",
    }
