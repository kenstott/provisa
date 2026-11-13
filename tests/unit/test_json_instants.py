# Copyright (c) 2026 Kenneth Stott
# Canary: 32f6d7c2-3cf0-449a-aabf-e1437a33e38b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A registered timestamptz column that to_json reads is said to be one (REQ-2171): the physical
rewrite casts it to the type it has, which is how Trino's to_json of it is written as PostgreSQL's
(tests/unit/test_zoned_instants.py)."""

# Requirements: REQ-2171

from __future__ import annotations

import pytest

from provisa.compiler.json_instants import state_json_instants

_INSTANTS = {("public", "events"): {"at": "timestamptz"}}


@pytest.mark.parametrize(
    "sql, said",
    [
        ("SELECT to_json(e.at) FROM public.events AS e", "TO_JSON(CAST(e.at AS TIMESTAMPTZ))"),
        ("SELECT to_jsonb(at) FROM public.events", "CAST(at AS TIMESTAMPTZ)"),
        ("SELECT to_json(ARRAY[at]) FROM public.events", "ARRAY[CAST(at AS TIMESTAMPTZ)]"),
    ],
)
def test_a_registered_instant_to_json_reads_is_said_to_be_one(sql, said):
    assert said in state_json_instants(sql, _INSTANTS)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT at, at::text FROM public.events",  # no to_json: nothing to say
        "SELECT to_json(name) FROM public.events",  # not an instant
        "SELECT to_json(CAST(at AS timestamptz)) FROM public.events",  # said already
        "SELECT to_json(at) FROM public.other",  # a table with no instant
    ],
)
def test_what_needs_no_saying_is_returned_as_given(sql):
    assert state_json_instants(sql, _INSTANTS) == sql


def test_saying_it_twice_is_saying_it_once():
    once = state_json_instants("SELECT to_json(at) FROM public.events", _INSTANTS)
    assert state_json_instants(once, _INSTANTS) == once


def _events_input(role_id: str = "admin"):
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput

    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "pg",
                "domain_id": "sales",
                "schema_name": "public",
                "table_name": "events",
                "columns": [
                    {"column_name": "id", "visible_to": [role_id]},
                    {"column_name": "at", "visible_to": [role_id]},
                    {"column_name": "day", "visible_to": [role_id]},
                    {"column_name": "odd", "visible_to": [role_id]},
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        ],
        relationships=[],
        column_types={
            1: [
                ColumnMetadata(column_name="id", data_type="integer", is_nullable=False),
                ColumnMetadata("at", "timestamp with time zone", True),
                ColumnMetadata("day", "timestamp", True),
                ColumnMetadata("odd", "not a ( type", True),  # a type sqlglot cannot read
            ]
        },
        naming_rules=[],
        role={"id": role_id, "capabilities": ["query_development"], "domain_access": ["*"]},
        domains=[{"id": "sales", "description": "Sales"}],
    )


def test_the_physical_rewrite_says_a_registered_timestamptz_column_is_one():
    from provisa.compiler.context import build_context
    from provisa.compiler.sql_rewrite import rewrite_semantic_to_catalog_physical

    ctx = build_context(_events_input())
    assert ctx.instant_columns == {("public", "events"): {"at": "timestamptz"}}
    physical = rewrite_semantic_to_catalog_physical(
        'SELECT to_json("e"."at"), to_json("e"."day") FROM "public"."events" AS "e"', ctx
    )
    assert physical.count("AS TIMESTAMPTZ)") == 1


def test_a_role_built_over_types_already_read_tokenizes_no_sql(monkeypatch):
    """A role's context is built in the background after every rebuild, one role after another
    (app_loaders.warm_role_surfaces), while requests are served: telling its timestamptz columns
    apart reads each column's type, and a type string read once is not tokenized again -- a
    rebuild's contexts derive no SQL for types the model has already shown, and a request served
    meanwhile is not charged with thousands of tokenizations (REQ-1877, REQ-1897)."""
    from sqlglot.tokens import Tokenizer

    from provisa.compiler.context import build_context

    build_context(_events_input("admin"))  # the model's types, read once
    real = Tokenizer.tokenize
    calls: list[str] = []

    def counted(self, sql):  # type: ignore[no-untyped-def]
        calls.append(sql)
        return real(self, sql)

    monkeypatch.setattr(Tokenizer, "tokenize", counted)
    for role_id in ("admin", "analyst"):
        ctx = build_context(_events_input(role_id))
        assert ctx.instant_columns == {("public", "events"): {"at": "timestamptz"}}, role_id
    assert calls == []
