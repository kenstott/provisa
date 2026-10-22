# Copyright (c) 2026 Kenneth Stott
# Canary: de687caf-c619-40de-a63b-556095eebb6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A statement command's contract, derived from its statement against the model (REQ-2001).

The statement is the declaration: what it reads (views expanded), what it writes and whether it
upserts, its output, its arguments' types and its column lineage. A statement that no longer
resolves is broken, with the reason. Its write enters the lineage graph, and impact analysis names
the command a rename would break.
"""

# Requirements: REQ-2001

from __future__ import annotations

import pytest

from provisa.compiler.statement_contract import ROW_COUNT, derive

_TABLES = [
    {
        "id": 1,
        "domain_id": "kb",
        "schema_name": "public",
        "table_name": "docs",
        "columns": [
            {"column_name": "id", "data_type": "integer"},
            {"column_name": "title", "data_type": "varchar"},
        ],
    },
    {
        "id": 2,
        "domain_id": "support",
        "schema_name": "public",
        "table_name": "tickets",
        "columns": [
            {"column_name": "ticket_id", "data_type": "integer"},
            {"column_name": "subject", "data_type": "varchar"},
            {"column_name": "updated_at", "data_type": "timestamp"},
        ],
    },
    {
        "id": 3,
        "domain_id": "support",
        "schema_name": "derived",
        "table_name": "open_tickets",
        "view_sql": "SELECT ticket_id, subject FROM support.tickets",
        "columns": [
            {"column_name": "ticket_id", "data_type": "integer"},
            {"column_name": "subject", "data_type": "varchar"},
        ],
    },
]
_ARGS = [{"name": "since", "type": "String"}]
_UPSERT = (
    "INSERT INTO kb.docs (id, title) SELECT ticket_id, upper(subject) FROM support.tickets "
    "WHERE updated_at > {{since}} ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title"
)


def _derive(sql: str, args=_ARGS, tables=_TABLES):
    return derive("embed_tickets", {"sql": sql}, list(args), tables)


def test_a_write_declares_its_reads_its_target_its_upsert_and_its_count():
    c = _derive(_UPSERT)
    assert c.broken is None
    assert (c.kind, c.writes, c.upsert, c.reads) == (
        "mutation",
        "kb.docs",
        True,
        ("support.tickets",),
    )
    assert c.outputs == (ROW_COUNT,)
    assert c.lineage == {"id": ["support.tickets.ticket_id"], "title": ["support.tickets.subject"]}
    assert c.write_select == (
        "SELECT ticket_id AS id, UPPER(subject) AS title FROM support.tickets WHERE updated_at > $1"
    )


def test_an_argument_compared_with_a_column_takes_its_type():
    assert _derive(_UPSERT).arguments == ({"name": "since", "type": "timestamp"},)
    uncompared = _derive("SELECT ticket_id, {{since}} AS s FROM support.tickets")
    assert uncompared.arguments == ({"name": "since", "type": "String"},)


def test_a_view_read_is_expanded_to_the_tables_it_reads():
    c = _derive("SELECT ticket_id, subject FROM support.open_tickets WHERE ticket_id = {{since}}")
    assert (c.kind, c.writes, c.reads) == ("query", None, ("support.tickets",))
    assert [o["name"] for o in c.outputs] == ["ticket_id", "subject"]
    assert not c.upsert and c.write_select is None


@pytest.mark.parametrize(
    ("sql", "reason"),
    [
        ("SELECT nope FROM support.tickets", "Column 'nope' could not be resolved"),
        ("SELECT * FROM support.gone", "no registered table is named 'support.gone'"),
        (
            "INSERT INTO kb.docs (idx) SELECT ticket_id FROM support.tickets",
            "kb.docs has no column 'idx'",
        ),
    ],
)
def test_a_statement_that_no_longer_resolves_is_broken_with_the_reason(sql, reason):
    c = _derive(sql, args=[])
    assert c.broken == reason


def test_a_dropped_column_breaks_the_command_at_the_next_derivation():
    renamed = [
        {**t, "columns": [{"column_name": "headline", "data_type": "varchar"}]}
        if t["table_name"] == "tickets"
        else t
        for t in _TABLES
    ]
    assert _derive(_UPSERT, tables=renamed).broken is not None


def test_the_contract_version_is_what_it_says():
    assert _derive(_UPSERT).version == _derive(_UPSERT).version
    other = _UPSERT.replace("upper(subject)", "subject")
    assert _derive(other).version != _derive(_UPSERT).version
    assert _derive(_UPSERT).as_dict()["version"] == _derive(_UPSERT).version


# -- lineage -----------------------------------------------------------------------------------


def _served(sql: str = _UPSERT, name: str = "embed_tickets") -> dict:
    contract = derive(name, {"sql": sql}, list(_ARGS), _TABLES).as_dict()
    return {name: {"name": name, "impl_kind": "statement", "contract": contract}}


def test_a_commands_write_enters_the_lineage_graph_naming_it():
    from provisa.lineage.commands import COMMAND_WRITE, command_write_graphs

    ((name, graph),) = command_write_graphs(_served())
    assert name == "embed_tickets"
    edges = {
        (e.source, e.target): [o for o in e.ops if o.kind == COMMAND_WRITE] for e in graph.edges
    }
    assert set(edges) == {
        ("support.tickets.ticket_id", "kb.docs.id"),
        ("support.tickets.subject", "kb.docs.title"),
    }
    assert all(ops and ops[0].name == "embed_tickets" for ops in edges.values())


def test_a_broken_or_reading_command_adds_no_write():
    from provisa.lineage.commands import command_write_graphs

    broken = _served("SELECT nope FROM support.tickets WHERE ticket_id = {{since}}")
    reading = _served("SELECT ticket_id FROM support.tickets WHERE ticket_id = {{since}}")
    assert command_write_graphs({**broken}) == []
    assert command_write_graphs({**reading}) == []


def test_impact_analysis_names_the_command_a_rename_would_break():
    from provisa.lineage.commands import with_command_writes
    from provisa.lineage.dependents import graph_dependents
    from provisa.lineage.merge import build_federation_graph

    views = [("kb.doc_titles", "SELECT title FROM kb.docs")]
    merged = with_command_writes(build_federation_graph(views), _served())
    read = graph_dependents(merged.graph, {"support.tickets", "tickets"}, "subject")
    assert [(d.kind, d.name, d.detail) for d in read] == [
        ("command", "embed_tickets", "reads subject")
    ]
    written = graph_dependents(merged.graph, {"kb.docs", "docs"}, "title")
    assert ("command", "embed_tickets", "writes title") in [
        (d.kind, d.name, d.detail) for d in written
    ]
    assert ("view", "kb.doc_titles", "selects title") in [
        (d.kind, d.name, d.detail) for d in written
    ]
