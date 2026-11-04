# Copyright (c) 2026 Kenneth Stott
# Canary: 7b19e4c2-0d5a-4f63-8c2e-a9d1f6b3e085
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The BI tools' catalog replay (REQ-2204): the recording proxy's reading of the wire, the capture's
tokens and deduplication, the stored statements, and the replay's comparison and counts."""

# Requirements: REQ-2204

from __future__ import annotations

import io
import json
import re
import struct
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts" / "conformance"))  # the scripts' sibling imports

import by_design  # noqa: E402
from bi_tools import capture, fixtures, proxy, replay  # noqa: E402

# --- the proxy ---------------------------------------------------------------------------------


def _parse(name: str, query: str, oids: list[int]) -> bytes:
    return (
        name.encode()
        + b"\0"
        + query.encode()
        + b"\0"
        + struct.pack(f">h{len(oids)}I", len(oids), *oids)
    )


def _bind(statement: str, formats: list[int], values: list[bytes | None]) -> bytes:
    body = b"\0" + statement.encode() + b"\0"
    body += struct.pack(f">h{len(formats)}h", len(formats), *formats)
    body += struct.pack(">h", len(values))
    for v in values:
        body += struct.pack(">i", -1) if v is None else struct.pack(">i", len(v)) + v
    return body + struct.pack(">h", 0)


def test_parse_message_reads_name_query_and_parameter_types():
    assert proxy.parse_message(_parse("S_1", "SELECT $1", [23])) == ("S_1", "SELECT $1", [23])
    assert proxy.parse_message(_parse("", "SELECT 1", [])) == ("", "SELECT 1", [])


def test_bind_message_reads_formats_and_values_with_null():
    body = _bind("S_1", [0, 1], [b"abc", None])
    assert proxy.bind_message(body) == ("S_1", [0, 1], [b"abc", None])


@pytest.mark.parametrize(
    ("formats", "count", "expected"),
    [([], 3, [0, 0, 0]), ([1], 2, [1, 1]), ([0, 1], 2, [0, 1])],
)
def test_param_formats_follow_the_protocol(formats, count, expected):
    assert proxy.param_formats(formats, count) == expected


def test_binary_integers_and_booleans_decode_to_text_other_binary_stays_hex():
    assert proxy.param_entry(23, 1, struct.pack(">i", 16390)) == {"type": 23, "value": "16390"}
    assert proxy.param_entry(26, 1, struct.pack(">I", 16390)) == {"type": 26, "value": "16390"}
    assert proxy.param_entry(20, 1, struct.pack(">q", -5)) == {"type": 20, "value": "-5"}
    assert proxy.param_entry(16, 1, b"\x01") == {"type": 16, "value": "t"}
    assert proxy.param_entry(17, 1, b"\xde\xad") == {"type": 17, "value": "dead", "format": 1}
    assert proxy.param_entry(1043, 0, b"x") == {"type": 1043, "value": "x"}
    assert proxy.param_entry(25, 0, None) == {"type": 25, "value": None}


def test_startup_params_read_user_and_application_name():
    body = b"user\0bi_psql\0application_name\0psql\0\0"
    assert proxy.startup_params(body) == {"user": "bi_psql", "application_name": "psql"}


def test_session_records_a_simple_query_and_a_bind_of_its_parsed_statement():
    out = io.StringIO()
    session = proxy.Session(out)
    session.params = {"user": "bi_metabase", "application_name": "Metabase"}
    session.message(b"Q", b"SELECT 1\0")
    session.message(b"P", _parse("S_2", "SELECT * FROM t WHERE n = $1", [1043]))
    session.message(b"B", _bind("S_2", [], [b"conformance"]))
    lines = [json.loads(line) for line in out.getvalue().splitlines()]
    assert lines == [
        {
            "user": "bi_metabase",
            "application_name": "Metabase",
            "protocol": "simple",
            "sql": "SELECT 1",
        },
        {
            "user": "bi_metabase",
            "application_name": "Metabase",
            "protocol": "extended",
            "sql": "SELECT * FROM t WHERE n = $1",
            "params": [{"type": 1043, "value": "conformance"}],
        },
    ]


# --- the capture -------------------------------------------------------------------------------


def test_tokenise_replaces_only_a_standalone_oid_and_name():
    oids = {16390: "{{oid:pg_class:conformance.bi_order}}"}
    names = {"bitools": "{{database}}", "bi_psql": "{{user}}"}
    text = "WHERE c.oid = '16390' AND x = 163901 AND d = 'bitools' AND u = 'bi_psql_2'"
    assert capture.tokenise(text, oids, names) == (
        "WHERE c.oid = '{{oid:pg_class:conformance.bi_order}}' AND x = 163901 "
        "AND d = '{{database}}' AND u = 'bi_psql_2'"
    )


def test_stored_tokenises_then_deduplicates_keeping_first_order():
    entries = [
        {"protocol": "simple", "sql": "SELECT 1"},
        {"protocol": "extended", "sql": "SELECT $1", "params": [{"type": 26, "value": "16390"}]},
        {"protocol": "extended", "sql": "SELECT $1", "params": [{"type": 26, "value": "16390"}]},
        {"protocol": "simple", "sql": "SELECT 1"},
        {
            "protocol": "extended",
            "sql": "SELECT $1",
            "params": [{"type": 17, "value": "16390", "format": 1}],
        },
    ]
    got = capture.stored(entries, {16390: "{{oid:pg_class:conformance.t}}"}, {})
    assert got == [
        {"id": 1, "protocol": "simple", "sql": "SELECT 1"},
        {
            "id": 2,
            "protocol": "extended",
            "sql": "SELECT $1",
            "params": [{"type": 26, "value": "{{oid:pg_class:conformance.t}}"}],
        },
        {
            "id": 3,
            "protocol": "extended",
            "sql": "SELECT $1",
            # a binary value is not text: it is never tokenised
            "params": [{"type": 17, "value": "16390", "format": 1}],
        },
    ]


def test_grafana_get_schema_quotes_the_table_as_its_editor_does():
    sql = capture.grafana_get_schema("conformance.o'x")
    assert "parse_ident('conformance.o''x')" in sql
    assert "information_schema.columns" in sql


def test_block_dumper_writes_multiline_sql_as_a_block():
    text = yaml.dump({"sql": "SELECT 1\nFROM t"}, Dumper=capture.block_dumper())
    assert text == "sql: |-\n  SELECT 1\n  FROM t\n"


# --- the stored statements -----------------------------------------------------------------------


STORED = sorted((REPO / "scripts" / "conformance" / "bi_tools").glob("*.yaml"))


def test_every_captured_tool_is_one_the_replay_reports():
    assert {p.stem for p in STORED} <= set(replay.TOOLS)
    assert {"psql", "metabase", "grafana"} <= {p.stem for p in STORED}


@pytest.mark.parametrize("path", STORED, ids=lambda p: p.stem)
def test_stored_statements_are_well_formed(path):
    doc = yaml.safe_load(path.read_text())
    assert doc["tool"] == path.stem
    assert doc["version"] and doc["captured"] and doc["method"] and doc["browse"]
    statements = doc["statements"]
    assert statements
    assert [s["id"] for s in statements] == list(range(1, len(statements) + 1))
    keys = [capture.statement_key(s) for s in statements]
    assert len(keys) == len(set(keys)), "a statement is stored twice"
    for s in statements:
        assert s["protocol"] in ("simple", "extended")
        for token in replay.tokens_of(s):
            replay.resolver_sql(token)  # every token is one the replay resolves


# --- the replay ----------------------------------------------------------------------------------


def test_resolver_sql_names_each_object_by_schema_and_name():
    assert replay.resolver_sql("oid:pg_class:conformance.bi_order") == (
        "SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
        "ON n.oid = c.relnamespace WHERE n.nspname = 'conformance' AND c.relname = 'bi_order'"
    )
    assert "nspname = 'conformance'" in replay.resolver_sql("oid:pg_namespace:conformance")
    assert "conname = 'bi_order_pkey'" in replay.resolver_sql(
        "oid:pg_constraint:conformance.bi_order_pkey"
    )
    assert replay.resolver_sql("database") == "SELECT current_database()"
    assert replay.resolver_sql("user") == "SELECT current_user"
    assert "current_database()" in replay.resolver_sql("oid:pg_database:")


def test_bound_fills_tokens_in_sql_and_text_params_and_decodes_hex():
    statement = {
        "sql": "SELECT '{{oid:pg_class:conformance.t}}', $1, $2, $3",
        "params": [
            {"type": 1043, "value": "{{user}}"},
            {"type": 17, "value": "dead", "format": 1},
            {"type": 25, "value": None},
        ],
    }
    values = {"oid:pg_class:conformance.t": "42", "user": "conformance"}
    assert replay.tokens_of(statement) == set(values)
    assert replay.bound(statement, values) == (
        "SELECT '42', $1, $2, $3",
        [(1043, b"conformance", 0), (17, b"\xde\xad", 1), (25, None, 0)],
    )


def _ok(*columns: tuple[str, int, int]) -> dict:
    fields = [{"name": n, "type_oid": t, "typmod": m, "format": 0} for n, t, m in columns]
    return {"ok": True, "fields": fields, "row": None}


def _refused(sqlstate: str, primary: str) -> dict:
    return {"ok": False, "error": primary, "sqlstate": sqlstate, "primary": primary}


NAMES = {19: "name", 25: "text", 23: "int4"}


def test_compare_matches_described_columns_and_names_each_difference():
    assert replay.compare(_ok(("a", 25, -1)), _ok(("a", 25, -1)), NAMES) == []
    assert replay.compare(_ok(("a", 19, -1)), _ok(("a", 25, -1)), NAMES) == [
        "a: type text (oid 25), PostgreSQL name (oid 19)"
    ]
    assert replay.compare(_ok(("a", 25, -1), ("b", 23, -1)), _ok(("a", 25, -1)), NAMES) == [
        "missing columns: b"
    ]


def test_compare_refusals_pass_only_when_the_same():
    want = _refused("42P01", 'ERROR:  relation "x" does not exist')
    assert replay.compare(want, dict(want), NAMES) == []
    assert replay.compare(want, _ok(("a", 25, -1)), NAMES) == [
        'PostgreSQL refuses: ERROR:  relation "x" does not exist; Provisa answers'
    ]
    assert replay.compare(_ok(("a", 25, -1)), _refused("XX000", "ERROR:  boom"), NAMES) == [
        "Provisa error: ERROR:  boom"
    ]


class _FakeServer:
    """Answers by statement text; records what it ran."""

    def __init__(self, answers: dict[str, dict]) -> None:
        self.answers = answers
        self.ran: list[tuple[str, list]] = []
        self.conn = SimpleNamespace(pgconn=SimpleNamespace(transaction_status=0))

    def run(self, sql: str, result_format: int = 0, params=None) -> dict:
        self.ran.append((sql, params or []))
        return self.answers.get(sql, _refused("42601", f"ERROR:  no answer for {sql}"))


def _doc(*statements: dict) -> dict:
    return {
        "tool": "psql",
        "version": "17.11",
        "captured": "2026-11-04",
        "method": "Automated",
        "browse": ["\\d"],
        "statements": list(statements),
    }


def test_replay_tool_counts_passed_failed_and_not_tested(monkeypatch):
    monkeypatch.setattr(by_design, "groups", lambda surface: [])
    oid_sql = replay.resolver_sql("oid:pg_class:conformance.t")
    gone_sql = replay.resolver_sql("oid:pg_constraint:conformance.t_pkey")
    lost_sql = replay.resolver_sql("oid:pg_class:conformance.lost")
    pg = _FakeServer(
        {
            oid_sql: {"ok": True, "fields": [], "row": [b"16400"]},
            gone_sql: {"ok": True, "fields": [], "row": [b"16401"]},
            lost_sql: {"ok": True, "fields": [], "row": None},
            "SELECT 1": _ok(("?column?", 23, -1)),
            "SELECT relname FROM pg_class WHERE oid = 16400": _ok(("relname", 19, -1)),
            "SELECT 16401": _ok(("?column?", 23, -1)),
        }
    )
    provisa = _FakeServer(
        {
            oid_sql: {"ok": True, "fields": [], "row": [b"9"]},
            gone_sql: {"ok": True, "fields": [], "row": None},
            "SELECT 1": _ok(("?column?", 23, -1)),
            "SELECT relname FROM pg_class WHERE oid = 9": _ok(("relname", 25, -1)),
        }
    )
    doc = _doc(
        {"id": 1, "protocol": "simple", "sql": "SELECT 1"},
        {
            "id": 2,
            "protocol": "simple",
            "sql": "SELECT relname FROM pg_class WHERE oid = {{oid:pg_class:conformance.t}}",
        },
        {"id": 3, "protocol": "simple", "sql": "SELECT {{oid:pg_constraint:conformance.t_pkey}}"},
        {"id": 4, "protocol": "simple", "sql": "SELECT {{oid:pg_class:conformance.lost}}"},
    )
    result = replay.replay_tool(doc, pg, provisa, NAMES)
    outcomes = {c["id"]: (c["outcome"], c["reason"]) for c in result["cases"]}
    assert outcomes == {
        1: ("passed", ""),
        2: ("failed", "relname: type text (oid 25), PostgreSQL name (oid 19)"),
        3: (
            "failed",
            "names what Provisa's catalog does not have: oid:pg_constraint:conformance.t_pkey",
        ),
        4: ("not_tested", "PostgreSQL has no oid:pg_class:conformance.lost"),
    }
    assert (result["passed"], result["by_design"], result["failed"], result["not_tested"]) == (
        1,
        0,
        2,
        1,
    )
    assert result["statements"] == 4
    # Provisa ran its own OID, never PostgreSQL's.
    assert ("SELECT relname FROM pg_class WHERE oid = 9", []) in provisa.ran


def test_a_by_design_group_skips_a_failed_statement(monkeypatch):
    group = by_design.Group(
        group="The server's version",
        requirement="REQ-579",
        decision="Provisa reports 14.",
        reason=re.compile("^a: type"),
        case=re.compile("^psql catalog replay::#1 "),
    )
    monkeypatch.setattr(by_design, "groups", lambda surface: [group])
    cases = [
        {
            "id": 1,
            "feature": "psql catalog replay",
            "scenario": "#1 SELECT 1",
            "outcome": "failed",
            "reason": "a: type x",
        },
        {
            "id": 2,
            "feature": "psql catalog replay",
            "scenario": "#2 SELECT 2",
            "outcome": "failed",
            "reason": "a: type x",
        },
    ]
    result = replay.tool_outcomes(_doc(), cases)
    assert [c["outcome"] for c in result["cases"]] == ["by_design", "failed"]
    assert (result["by_design"], result["failed"]) == (1, 1)
    assert result["by_design_groups"][0]["requirement"] == "REQ-579"


def test_recover_rolls_back_an_aborted_transaction():
    from psycopg import pq

    server = _FakeServer({"ROLLBACK": {"ok": True, "fields": [], "row": None}})
    server.conn.pgconn.transaction_status = pq.TransactionStatus.INERROR
    replay._recover(server)
    assert server.ran == [("ROLLBACK", [])]


def _report() -> dict:
    tools = {t: replay.not_captured(t) for t in replay.TOOLS}
    tools["psql"] = replay.tool_outcomes(
        _doc(),
        [
            {
                "id": 1,
                "feature": "psql catalog replay",
                "scenario": "#1 SELECT 1",
                "outcome": "passed",
                "reason": "",
            },
            {
                "id": 2,
                "feature": "psql catalog replay",
                "scenario": "#2 SELECT a|b",
                "outcome": "failed",
                "reason": "x",
            },
        ],
    )
    return {"surface": "pgwire-bi-tools", "postgres_version": "17.11", "failed": 1, "tools": tools}


def test_markdown_gives_each_tool_its_counts_and_failures(monkeypatch):
    monkeypatch.setattr(by_design, "groups", lambda surface: [])
    text = replay.markdown(_report())
    assert "| psql | 17.11 | 2026-11-04 | Automated | 2 | 1 | 0 | 1 | 0 |" in text
    assert "| DBeaver | — | — | not captured | 0 | 0 | 0 | 0 | not tested |" in text
    assert "<summary>psql: failed (1)</summary>" in text
    assert "| `#2 SELECT a\\|b` | x |" in text


def test_fill_replaces_only_the_marked_block():
    page = f"intro\n{replay.BEGIN}\nold\n{replay.END}\nrest\n"
    assert replay.fill(page, "new") == f"intro\n{replay.BEGIN}\nnew\n{replay.END}\nrest\n"
    with pytest.raises(ValueError, match="no bi-tools-results markers"):
        replay.fill("no markers", "new")


def test_the_pgwire_page_has_the_markers():
    assert replay.BEGIN in replay.DOCS.read_text() and replay.END in replay.DOCS.read_text()


# --- the fixture and the workflow ----------------------------------------------------------------


def test_fixture_declares_each_table_keyed_and_each_foreign_key():
    ddl = "\n".join(fixtures.ddl())
    assert "CREATE TABLE conformance.bi_customer (id integer PRIMARY KEY" in ddl
    assert "FOREIGN KEY (customer_id) REFERENCES conformance.bi_customer(id)" in ddl
    assert "CREATE INDEX bi_order_customer_id_idx" in ddl
    tables = {t["table"]: t for t in fixtures.tables()}
    assert set(tables) == set(fixtures.TABLES)
    order = [c["name"] for c in tables["bi_order"]["columns"]]
    assert order == ["id", "customer_id", "total", "placed"]
    assert tables["bi_order"]["columns"][0]["is_primary_key"] is True
    assert fixtures.relationships() == [
        {
            "id": "bi_order__customer_id",
            "alias": "BI_ORDER_BI_CUSTOMER",
            "cardinality": "many-to-one",
            "source_table_id": "bi_order",
            "source_column": "customer_id",
            "target_table_id": "bi_customer",
            "target_column": "id",
        }
    ]


def test_the_pgwire_workflow_seeds_the_tables_and_replays_after_the_catalog():
    workflow = yaml.safe_load((REPO / ".github/workflows/conformance-pgwire.yml").read_text())
    steps = workflow["jobs"]["pgwire"]["steps"]
    names = [s.get("name") for s in steps]
    seed = names.index("Seed the BI tools' tables")
    start = names.index("Start the Provisa instance")
    replayed = names.index("Replay the BI tools' catalog statements")
    assert seed < start < names.index("Compare the catalog and types with PostgreSQL") < replayed
    assert steps[start]["with"]["extra-tables"].endswith("bi-tools-tables.yaml")
    assert steps[start]["with"]["extra-relationships"].endswith("bi-tools-relationships.yaml")
    assert "bi_tools/replay.py replay" in steps[replayed]["run"]
    summarise = steps[names.index("Summarise")]["run"]
    assert "bi_tools/replay.py docs" in summarise and "bi-tools.md" in summarise
