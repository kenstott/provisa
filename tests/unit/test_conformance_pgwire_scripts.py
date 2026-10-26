# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0f8a3e-7c2b-4e61-9a4d-2f8b1c6e0a97
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The pgwire conformance job's report scripts (REQ-2167): pgspec's results summarised by section,
the catalog comparison, and the model that presents the fixture under PostgreSQL's names."""

# Requirements: REQ-2167

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _load(name: str):
    spec = importlib.util.spec_from_file_location(
        name, REPO / "scripts" / "conformance" / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


_SOURCE = """
// Section 1: Startup (55.2.1)
func TestStartup(t *testing.T) {
\tt.Run("Version30", func(t *testing.T) {
\t})
\tt.Run("WrongVersion", func(t *testing.T) {
\t})
}

// Section 2: Simple Query (55.2.3)
func TestSimpleQuery(t *testing.T) {
\tt.Run("Insert", func(t *testing.T) {
\t})
\tt.Run("Cancel", func(t *testing.T) {
\t})
}

func helper() {
\tt.Run("NotATest", nil)
}

// Section 3: Edge Cases
func TestLone(t *testing.T) {
}
"""


def _ev(action: str, test: str, output: str = "") -> dict:
    ev = {"Action": action, "Package": "pgspec", "Test": test}
    if output:
        ev["Output"] = output
    return ev


def test_pgspec_results_are_counted_per_leaf_and_failures_grouped_by_section():
    m = _load("pgspec_summary")
    events = [
        _ev("run", "TestStartup"),
        _ev("run", "TestStartup/Version30"),
        _ev("pass", "TestStartup/Version30"),
        _ev("run", "TestStartup/WrongVersion"),
        _ev("output", "TestStartup/WrongVersion", "=== RUN   TestStartup/WrongVersion\n"),
        _ev(
            "output", "TestStartup/WrongVersion", "    pgspec_test.go:60: expected ErrorResponse\n"
        ),
        _ev("fail", "TestStartup/WrongVersion"),
        _ev("fail", "TestStartup"),
        _ev("run", "TestSimpleQuery"),
        _ev("run", "TestSimpleQuery/Insert"),
        _ev("skip", "TestSimpleQuery/Insert"),
        # Cancel never ran: the binary exited.
        _ev("run", "TestLone"),
        _ev("pass", "TestLone"),
    ]
    s = m.summarise(_SOURCE, events, "v1.5.11")
    assert (s["total"], s["passed"], s["failed"], s["skipped"]) == (5, 2, 2, 1)
    assert s["not_reached"] == ["TestSimpleQuery/Cancel"]
    assert s["pass_rate"] == 0.4
    assert list(s["failed_by_section"]) == [
        "Section 1: Startup (55.2.1)",
        "Section 2: Simple Query (55.2.3)",
    ]
    startup = s["failed_by_section"]["Section 1: Startup (55.2.1)"]
    assert startup == [
        {"test": "TestStartup/WrongVersion", "reason": "pgspec_test.go:60: expected ErrorResponse"}
    ]
    md = m.markdown(s)
    assert "| 5 | 2 | 2 | 1 | 40.0% |" in md
    assert "`TestSimpleQuery/Cancel`: not reached" in md


def test_a_top_level_test_that_failed_before_its_subtests_counts_them_not_itself():
    m = _load("pgspec_summary")
    events = [_ev("run", "TestStartup"), _ev("fail", "TestStartup")]
    s = m.summarise(_SOURCE, events)
    assert "TestStartup" not in {f["test"] for fs in s["failed_by_section"].values() for f in fs}
    assert {"TestStartup/Version30", "TestStartup/WrongVersion"} <= set(s["not_reached"])


def test_the_summary_fails_only_when_the_suite_did_not_run(tmp_path):
    m = _load("pgspec_summary")
    src = tmp_path / "pgspec_test.go"
    src.write_text(_SOURCE)
    skipped = tmp_path / "skipped.jsonl"
    skipped.write_text(
        "\n".join(
            json.dumps(_ev("skip", t)) for t in ("TestStartup", "TestSimpleQuery", "TestLone")
        )
    )
    args = [str(tmp_path / "o.json"), str(tmp_path / "o.md")]
    assert m.main(["x", str(src), str(skipped), *args]) == 1
    failing = tmp_path / "failing.jsonl"
    failing.write_text(json.dumps(_ev("fail", "TestLone")))
    assert m.main(["x", str(src), str(failing), *args]) == 0
    assert json.loads((tmp_path / "o.json").read_text())["failed"] == 5


def test_catalog_columns_are_compared_by_name_order_type_and_typmod():
    m = _load("pgwire_catalog_types")
    names = {19: "name", 25: "text", 23: "int4"}
    expected = [
        {"name": "a", "type_oid": 19, "typmod": -1},
        {"name": "b", "type_oid": 23, "typmod": -1},
        {"name": "c", "type_oid": 25, "typmod": -1},
    ]
    assert m._compare_columns(expected, [dict(c) for c in expected], names) == []
    got = [
        {"name": "b", "type_oid": 23, "typmod": 4},
        {"name": "a", "type_oid": 25, "typmod": -1},
        {"name": "z", "type_oid": 25, "typmod": -1},
    ]
    assert m._compare_columns(expected, got, names) == [
        "missing columns: c",
        "extra columns: z",
        "column order differs",
        "a: type text (oid 25), PostgreSQL name (oid 19)",
        "b: typmod 4, PostgreSQL -1",
    ]


def test_each_part_of_the_catalog_report_is_its_own_section():
    m = _load("pgwire_catalog_types")
    report = {
        "postgres_version": "16.4",
        "catalog_relations": {
            "postgres_relations": 3,
            "served": 2,
            "matching": ["pg_catalog.pg_class"],
            "differing": {"pg_catalog.pg_type": ["typname: type text (oid 25), PostgreSQL name"]},
            "not_served": {"pg_catalog.pg_stat_database": "relation does not exist"},
            "served_not_in_postgres": [],
        },
        "catalog_functions": {
            "postgres_functions": 2,
            "excluded": "pseudo-types",
            "served": 1,
            "matching": 1,
            "differing": {},
            "not_served": {"pg_catalog.foo(integer)": "function foo does not exist"},
        },
        "type_conversions": {
            "fixture_columns": [],
            "casts": [
                {
                    "statement": "SELECT 'x'::name AS v",
                    "postgres_type": "name",
                    "provisa_type": "text",
                    "differences": ["type oid 25 (text), PostgreSQL 19 (name)"],
                }
            ],
            "compared": 1,
            "matching": 0,
            "differing": 1,
        },
    }
    md = m.markdown(report)
    for heading in (
        "## pgwire catalog shape (PostgreSQL 16.4)",
        "## pgwire catalog coverage (PostgreSQL 16.4)",
        "## pgwire type conversions (PostgreSQL 16.4)",
    ):
        assert heading in md
    assert "`pg_catalog.pg_stat_database`" in md
    assert "`pg_catalog.foo(integer)`" in md
    assert "| `SELECT 'x'::name AS v` | name | text |" in md


def test_the_model_presents_the_fixture_under_the_names_postgresql_gives_it(tmp_path, monkeypatch):
    m = _load("pgwire_catalog_types")
    from provisa.compiler.naming import domain_to_sql_name
    from provisa.core.config_loader import parse_config

    for key, value in {
        "CONFORMANCE_PG_HOST": "127.0.0.1",
        "CONFORMANCE_PG_DATABASE": "conformance",
        "CONFORMANCE_PG_USER": "postgres",
        "CONFORMANCE_PG_PASSWORD": "conformance",
    }.items():
        monkeypatch.setenv(key, value)
    path = tmp_path / "provisa.yaml"
    m.write_config(path, 55432)
    cfg = parse_config(path)
    assert cfg.auth.provider == "none"
    assert cfg.naming.domain_prefix is False
    (table,) = cfg.tables
    assert domain_to_sql_name(table.domain_id) == m.FIXTURE_SCHEMA == table.schema_name
    assert table.table_name == m.FIXTURE_TABLE == table.alias
    assert [c.name for c in table.columns] == [name for name, _, _ in m.FIXTURE]
    assert all(c.visible_to == ["org_admin"] for c in table.columns)
    assert cfg.sources[0].port == 55432
