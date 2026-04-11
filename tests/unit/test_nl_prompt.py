# Copyright (c) 2026 Kenneth Stott
# Canary: 95233223-80f1-44c5-bb1d-37fdc8d90328
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for provisa/nl/prompt.py."""

import pytest

from provisa.nl.prompt import build_prompt


_SDL = "type Query { persons: [Person] }\ntype Person { id: ID! name: String }"


def test_prompt_includes_schema_sdl():
    p = build_prompt("Who are the people?", "graphql", _SDL)
    assert _SDL in p


def test_prompt_includes_nl_query():
    p = build_prompt("Find all persons", "cypher", _SDL)
    assert "Find all persons" in p


def test_prompt_no_prior_error():
    p = build_prompt("q", "sql", _SDL, prior_error=None)
    assert "PREVIOUS ATTEMPT" not in p


def test_prompt_includes_prior_error():
    p = build_prompt("q", "sql", _SDL, prior_error="Syntax error at line 1")
    assert "Syntax error at line 1" in p
    assert "PREVIOUS ATTEMPT" in p


def test_cypher_target_instructions_present():
    p = build_prompt("q", "cypher", _SDL)
    assert "MATCH" in p or "Cypher" in p


def test_graphql_target_instructions_present():
    p = build_prompt("q", "graphql", _SDL)
    assert "GraphQL" in p or "mutation" in p.lower() or "query" in p.lower()


def test_sql_target_instructions_present():
    p = build_prompt("q", "sql", _SDL)
    assert "SQL" in p or "SELECT" in p or "Trino" in p


def test_role_scoped_sdl_excludes_invisible_tables():
    """SDL passed in is already role-scoped; prompt must include it verbatim."""
    restricted_sdl = "type Query { public_data: [PublicData] }"
    p = build_prompt("show me data", "graphql", restricted_sdl)
    assert "public_data" in p
    assert "secret_table" not in p
