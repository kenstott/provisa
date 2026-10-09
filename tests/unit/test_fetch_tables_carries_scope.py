# Copyright (c) 2026 Kenneth Stott
# Canary: 4f9a2c71-0e5d-4b38-a6c9-d8e1b7f30a52
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tables a server builds its schemas from carry each column's scope (REQ-1959).

``fetch_tables`` is what a running server reads its registered tables with, for every role's
schema build and governance. It names the column fields it reads one by one, and did not name
``scope`` — so a column stored as ``public`` reached the served-set rule
(``security.rights.column_served``) with no scope, was judged ``domain``, and was served to no
role outside its table's domain on any surface. The rule was right; it was never shown the field.
"""

# Requirements: REQ-1959

from __future__ import annotations

from collections import defaultdict

import pytest

from provisa.api.admin.db_queries import fetch_tables
from provisa.security.rights import column_served, served_across_domains

pytestmark = pytest.mark.asyncio


class _Conn:
    """Answers fetch_tables' two statements from one stored table with a public column."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def fetch(self, sql: str, *args):
        self.statements.append(sql)
        if "FROM registered_tables" in sql:
            row = defaultdict(lambda: None)
            row.update(
                id=1, source_id="pg", domain_id="hr", schema_name="public", table_name="staff"
            )
            return [row]
        stored = {
            "id": ("public", ["seller"]),
            "name": ("public", ["seller"]),
            "salary": ("domain", ["seller"]),
            "ssn": ("restricted", []),
        }
        return [
            defaultdict(lambda: None, column_name=name, scope=scope, visible_to=granted)
            for name, (scope, granted) in stored.items()
        ]


async def test_each_column_is_fetched_with_its_stored_scope():
    (table,) = await fetch_tables(_Conn())  # type: ignore[arg-type]
    assert {c["column_name"]: c["scope"] for c in table["columns"]} == {
        "id": "public",
        "name": "public",
        "salary": "domain",
        "ssn": "restricted",
    }


async def test_a_published_column_is_served_across_domains_from_what_the_server_fetches():
    """The failure as a server met it: a role that reaches only ``sales`` is served the public
    columns of ``hr.staff`` from the fetched table, and nothing else of it."""
    (table,) = await fetch_tables(_Conn())  # type: ignore[arg-type]
    seller = {"id": "seller", "domain_access": ["sales"]}
    assert served_across_domains(seller, table)
    served = [
        c["column_name"]
        for c in table["columns"]
        if column_served(seller, table["domain_id"], c, reaches=False)
    ]
    assert served == ["id", "name"]


async def test_the_fetch_reads_every_column_field_the_served_set_rule_reads():
    """The rule reads a column's grant, scope and parameter mark; a field it reads and the fetch
    does not select is a column judged on a default."""
    conn = _Conn()
    (table,) = await fetch_tables(conn)  # type: ignore[arg-type]
    (columns_sql,) = [s for s in conn.statements if "FROM table_columns" in s]
    for field in ("visible_to", "scope", "native_filter_type"):
        assert field in columns_sql, field
        assert all(field in column for column in table["columns"]), field
