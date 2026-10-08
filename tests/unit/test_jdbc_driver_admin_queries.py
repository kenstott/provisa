# Copyright (c) 2026 Kenneth Stott
# Canary: 5f8d2b60-9c47-4e13-a72f-6b0e3d9c1a84
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The JDBC driver's catalog queries are valid against the admin GraphQL schema (REQ-126).

The driver answers ``getTables``/``getColumns`` and key metadata from two queries it sends to
``/admin/graphql``. Their text lives in Java and the schema in Python, so nothing noticed when a
field the driver asked for (``governance``) left the schema: every metadata call of the driver
failed validation on the server. This reads the query text out of the driver's source and
validates it against the schema the server serves.
"""

# Requirements: REQ-126, REQ-128

from __future__ import annotations

import re
from pathlib import Path

from graphql import parse, validate

from provisa.api.admin.schema import admin_schema

_SOURCE = (
    Path(__file__).resolve().parents[2]
    / "jdbc-driver/src/main/java/io/provisa/jdbc/ProvisaConnection.java"
)


def _queries() -> list[str]:
    """Every ``String gql = "..." + "...";`` in the driver, with its Java concatenation joined."""
    java = _SOURCE.read_text(encoding="utf-8")
    found = []
    for literal in re.findall(r"String gql = ((?:\s*\"[^\"]*\"\s*\+?)+);", java):
        found.append("".join(re.findall(r"\"([^\"]*)\"", literal)))
    return found


def test_the_driver_sends_its_two_catalog_queries():
    queries = _queries()
    assert len(queries) == 2, queries
    assert queries[0].startswith("{ tables {") and queries[1].startswith("{ relationships {")


def test_every_field_the_driver_asks_for_exists_in_the_admin_schema():
    for query in _queries():
        errors = validate(admin_schema._schema, parse(query))
        assert errors == [], (query, [e.message for e in errors])
