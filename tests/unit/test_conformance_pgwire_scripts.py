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
sys.path.insert(0, str(REPO / "scripts" / "conformance"))  # the scripts' sibling imports


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
    # REQ-2167 (amended 2026-10-28): a skipped or unreached leaf no group names is failed.
    assert (s["total"], s["passed"], s["by_design"], s["failed"]) == (5, 2, 0, 3)
    assert "skipped" not in s
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
    assert [f["test"] for f in s["failed_by_section"]["Section 2: Simple Query (55.2.3)"]] == [
        "TestSimpleQuery/Cancel",
        "TestSimpleQuery/Insert",
    ]
    md = m.markdown(s)
    assert "5 tests: 2 passed, 0 skipped by design, 3 failed. Pass rate 40.0%" in md
    assert "`TestSimpleQuery/Cancel`: not reached" in md
    assert "`TestSimpleQuery/Insert`: skipped by pgspec" in md


def test_a_top_level_test_that_failed_before_its_subtests_counts_them_not_itself():
    m = _load("pgspec_summary")
    events = [_ev("run", "TestStartup"), _ev("fail", "TestStartup")]
    s = m.summarise(_SOURCE, events)
    assert "TestStartup" not in {f["test"] for fs in s["failed_by_section"].values() for f in fs}
    assert {"TestStartup/Version30", "TestStartup/WrongVersion"} <= set(s["not_reached"])


def test_the_summary_fails_when_the_suite_did_not_run_or_a_test_failed(tmp_path):
    # pgwire's baseline is every test passing: a failure no by-design group excludes is a
    # regression, and fails the job.
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
    assert m.main(["x", str(src), str(failing), *args]) == 1
    assert json.loads((tmp_path / "o.json").read_text())["failed"] == 5
    passing = tmp_path / "passing.jsonl"
    passing.write_text(
        "\n".join(
            json.dumps(_ev("pass", t))
            for t in (
                "TestStartup/Version30",
                "TestStartup/WrongVersion",
                "TestSimpleQuery/Insert",
                "TestSimpleQuery/Cancel",
                "TestLone",
            )
        )
    )
    assert m.main(["x", str(src), str(passing), *args]) == 0


def test_a_failure_a_by_design_group_names_is_skipped_by_design_and_fails_nothing(
    tmp_path, monkeypatch
):
    m = _load("pgspec_summary")
    rules = tmp_path / "by_design"
    rules.mkdir()
    (rules / "pgwire.yaml").write_text(
        "- group: Cancel\n"
        "  requirement: REQ-2167\n"
        "  decision: a test decision.\n"
        '  case: "TestSimpleQuery::Cancel"\n'
    )
    monkeypatch.setattr(m.by_design, "RULES", rules)
    events = [
        _ev("pass", t)
        for t in ("TestStartup/Version30", "TestStartup/WrongVersion", "TestSimpleQuery/Insert")
    ] + [_ev("fail", "TestSimpleQuery/Cancel"), _ev("pass", "TestLone")]
    s = m.summarise(_SOURCE, events)
    assert (s["total"], s["passed"], s["by_design"], s["failed"]) == (5, 4, 1, 0)
    assert s["pass_rate"] == 1.0
    (group,) = s["by_design_groups"]
    assert (group["group"], group["count"], group["cases"]) == (
        "Cancel",
        1,
        ["TestSimpleQuery::Cancel"],
    )
    assert "| Cancel | 1 | REQ-2167 |" in m.markdown(s)
    src, results = tmp_path / "pgspec_test.go", tmp_path / "r.jsonl"
    src.write_text(_SOURCE)
    results.write_text("\n".join(json.dumps(e) for e in events))
    assert (
        m.main(["x", str(src), str(results), str(tmp_path / "o.json"), str(tmp_path / "o.md")]) == 0
    )


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
    import by_design

    m = _load("pgwire_catalog_types")
    geometric = {
        "group": "Geometric functions",
        "requirement": "REQ-2167",
        "decision": "d",
        "count": 2,
        "cases": ["catalog function::point_above()", "catalog function::box()"],
    }
    report = {
        "postgres_version": "16.4",
        "catalog_relations": {
            **by_design.outcomes(1, [], [{}, {}]),
            "postgres_relations": 3,
            "served": 2,
            "matching": ["pg_catalog.pg_class"],
            "differing": {"pg_catalog.pg_type": ["typname: type text (oid 25), PostgreSQL name"]},
            "not_served": {"pg_catalog.pg_stat_database": "relation does not exist"},
            "served_not_in_postgres": [],
        },
        "catalog_functions": {
            **by_design.outcomes(1, [geometric], [{}]),
            "postgres_functions": 2,
            "excluded": "pseudo-types",
            "served": 1,
            "matching": 1,
            "differing": {},
            "not_served": {"pg_catalog.foo(integer)": "function foo does not exist"},
        },
        "type_conversions": {
            **by_design.outcomes(0, [], [{}]),
            "fixture_columns": [],
            "casts": [
                {
                    "statement": "SELECT 'x'::name AS v",
                    "postgres_type": "name",
                    "provisa_type": "text",
                    "differences": ["type oid 25 (text), PostgreSQL 19 (name)"],
                    "outcome": "failed",
                }
            ],
            "compared": 1,
            "matching": 0,
            "differing": 1,
        },
    }
    md = m.markdown(report)
    # REQ-2167 (amended 2026-10-28): each part's three counts, side by side.
    assert "| catalog relations | 3 | 1 | 0 | 2 | 33.3% |" in md
    assert "| catalog functions | 4 | 1 | 2 | 1 | 50.0% |" in md
    assert "| type conversions | 1 | 0 | 0 | 1 | 0.0% |" in md
    for heading in (
        "## pgwire catalog shape (PostgreSQL 16.4)",
        "## pgwire catalog coverage (PostgreSQL 16.4)",
        "## pgwire type conversions (PostgreSQL 16.4)",
    ):
        assert heading in md
    assert "`pg_catalog.pg_stat_database`" in md
    assert "`pg_catalog.foo(integer)`" in md
    assert "Functions skipped by design (2)" in md
    assert "| Geometric functions | 2 | REQ-2167 |" in md
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
        "CONFORMANCE_JWT_SECRET": "conformance-jwt-signing-key-not-a-secret-0000",
        "PROVISA_SUPERUSER_USERNAME": "conformance-su",
        "PROVISA_SUPERUSER_PASSWORD": "conformance-su-password",
    }.items():
        monkeypatch.setenv(key, value)
    # An unsized numeric's scale is read from the job's PostgreSQL by registration's own sampling
    # (introspect._sized_numeric, tested with it); here it stands in.
    sampled: list[str] = []

    def _sized(dsn: str, column: str) -> dict:
        sampled.append(column)
        return {"data_type": "numeric(38,4)"}

    monkeypatch.setattr(m, "_sized_numeric", _sized)
    path = tmp_path / "provisa.yaml"
    m.write_config(path, 55432, "host=127.0.0.1 port=55432")
    cfg = parse_config(path)
    # Password auth with SCRAM: pgspec's auth tests meet a real exchange, not trust mode.
    assert cfg.auth.provider == "basic"
    assert cfg.auth.scram is True
    assert cfg.naming.domain_prefix is False
    (table,) = cfg.tables
    assert domain_to_sql_name(table.domain_id) == m.FIXTURE_SCHEMA == table.schema_name
    assert table.table_name == m.FIXTURE_TABLE == table.alias
    assert all(c.visible_to == ["org_admin"] for c in table.columns)
    # REQ-1426: an untyped column cannot be persisted (run 40358436057 failed its boot on it).
    # Each column is typed as registration types the PostgreSQL column it reads.
    from provisa.compiler.type_map import column_type_to_graphql

    registered = m.registrable_columns("host=127.0.0.1 port=55432")
    typed = {n: typing["data_type"] for n, _, typing in registered}
    assert [(c.name, c.data_type) for c in table.columns] == list(typed.items())
    for column in table.columns:
        column_type_to_graphql(column.data_type)  # every registered type maps
    assert set(typed) == {n for n, _, _ in m.FIXTURE}  # REQ-2171: every column registers
    assert typed["c_numeric_10_2"] == "numeric(10,2)"
    assert typed["c_numeric"] == "numeric(38,4)" and "c_numeric" in sampled
    # REQ-1965: an array of one primitive type registers as JSON with its element type.
    arrays = {c.name: c for c in table.columns}
    assert arrays["c_int4_array"].data_type == "json"
    assert (arrays["c_int4_array"].array_depth, arrays["c_int4_array"].element_type) == (
        1,
        "integer",
    )
    # REQ-2171: a subtype registers as itself; money is no unsized numeric to sample.
    assert {n: typed[n] for n in ("c_inet", "c_cidr", "c_macaddr", "c_oid", "c_money")} == {
        "c_inet": "inet",
        "c_cidr": "cidr",
        "c_macaddr": "macaddr",
        "c_oid": "oid",
        "c_money": "money",
    }
    assert set(sampled) == {"c_numeric"}
    assert cfg.sources[0].port == 55432


class _Probe:
    """A server answering the catalog function probe: each function either answers or refuses."""

    def __init__(self, functions: list[tuple[str, str]], refused: set[str]) -> None:
        self._functions = functions
        self._refused = refused

    def rows(self, sql: str) -> list[tuple]:
        return [(name, args, "f", False, "integer", []) for name, args in self._functions]

    def run(self, sql: str, result_format: int = 0) -> dict:
        name = sql.split('pg_catalog."', 1)[1].split('"', 1)[0]
        if name in self._refused:
            return {"ok": False, "error": f"Scalar Function with name {name} does not exist!"}
        return {"ok": True, "fields": [{"type_oid": 23}]}


def test_a_catalog_function_of_a_by_design_family_is_left_out_of_not_served():
    # PGWIRE'S CATALOG FUNCTIONS (REQ-2167): a function of a family no client calls is not
    # served by design; one a client calls stays not served, to be answered.
    m = _load("pgwire_catalog_types")
    functions = [("point_above", ""), ("pg_cancel_backend", ""), ("abs", "")]
    pg = _Probe(functions, refused=set())
    provisa = _Probe(functions, refused={"point_above", "pg_cancel_backend"})
    report = m.catalog_functions(pg, provisa, {23: "int4"})
    assert list(report["not_served"]) == ["pg_cancel_backend()"]
    assert [(g["group"], g["count"]) for g in report["by_design_groups"]] == [
        ("Geometric functions", 1)
    ]
    assert report["matching"] == 1
    # Each function probed is a case: passed, skipped by design, or failed (not served).
    assert (report["total"], report["passed"], report["by_design"], report["failed"]) == (
        3,
        1,
        1,
        1,
    )
    assert report["failures"][0]["reason"].startswith("not served: ")


def test_no_by_design_function_group_names_a_function_a_client_calls():
    import by_design

    called = [
        "pg_cancel_backend",
        "pg_terminate_backend",
        "pg_get_indexdef",
        "pg_get_functiondef",
        "aclexplode",
        "quote_ident",
        "quote_literal",
        "pg_database_size",
        "pg_relation_size",
        "pg_options_to_table",
        "unnest",
        "btrim",
        "json_to_record",
        "pg_encoding_to_char",
    ]
    groups = [g for g in by_design.groups("pgwire") if g.kind == "design"]
    for name in called:
        failure = {"feature": "catalog function", "scenario": f"{name}()", "reason": ""}
        assert not [g.group for g in groups if g.matches(failure)], name


def _conversion(statement: str, *differences: str) -> dict:
    return {"statement": statement, "differences": list(differences)}


def test_a_conversion_is_passed_by_design_or_failed():
    """REQ-2167 (amended 2026-10-28): each compared statement is in one outcome; a difference a
    by-design group names (by_design/pgwire.yaml) is by design only when every difference the
    statement has is one the group allows."""
    import by_design

    m = _load("pgwire_catalog_types")
    fixture = [
        _conversion("SELECT c_int4 FROM conformance.types_fixture"),
        _conversion(
            "SELECT c_int4_array FROM conformance.types_fixture",
            "type oid 3802 (jsonb), PostgreSQL 1007 (_int4)",
            "text '[1, 2, 3]', PostgreSQL '{1,2,3}'",
            "binary 015b312c20322c20335d, PostgreSQL 00000001",
        ),
    ]
    casts = [
        _conversion(
            "SELECT current_user AS v",
            "text 'org_admin', PostgreSQL 'postgres'",
            "binary 6f72675f61646d696e, PostgreSQL 706f737467726573",
        ),
        # The same statement with a difference no decision makes stays a failure.
        _conversion(
            "SELECT session_user AS v",
            "type oid 25 (text), PostgreSQL 19 (name)",
            "text 'duckdb', PostgreSQL 'postgres'",
            "binary 6475636b6462, PostgreSQL 706f737467726573",
        ),
        _conversion(
            "SELECT pg_catalog.current_setting('server_version_num') AS v",
            "text '140000', PostgreSQL '170011'",
            "binary 313430303030, PostgreSQL 313730303131",
        ),
        _conversion("SELECT 'x'::varchar AS v", "type oid 25 (text), PostgreSQL 1043 (varchar)"),
    ]
    out = m.conversion_outcomes(fixture, casts)
    assert (out["compared"], out["matching"], out["differing"]) == (6, 1, 2)
    assert (out["total"], out["passed"], out["by_design"], out["failed"]) == (6, 1, 3, 2)
    assert {g["group"]: g["count"] for g in out["by_design_groups"]} == {
        "A source's array column is JSON": 1,
        "The session's role and database": 1,
        "The server's version": 1,
    }
    assert [r["outcome"] for r in fixture + casts] == [
        "passed",
        "by_design",
        "by_design",
        "failed",
        "by_design",
        "failed",
    ]
    md = m.markdown(
        {
            "postgres_version": "17.11",
            "catalog_relations": {
                **by_design.outcomes(0, [], []),
                "postgres_relations": 0,
                "served": 0,
                "matching": [],
                "differing": {},
                "not_served": {},
                "served_not_in_postgres": [],
            },
            "catalog_functions": {
                **by_design.outcomes(0, [], []),
                "postgres_functions": 0,
                "excluded": "pseudo-types",
                "served": 0,
                "matching": 0,
                "differing": {},
                "not_served": {},
            },
            "type_conversions": out,
        }
    )
    assert (
        "1 passed (identical in type OID, typmod, text and binary encoding); 3 skipped by "
        "design; 2 failed." in md
    )
    assert "| `SELECT session_user AS v` |" in md
    assert "| `SELECT current_user AS v` |" not in md


def test_the_pgwire_artifact_sums_each_part_s_three_counts_at_the_top():
    """REQ-2167 (amended 2026-10-28): the pgwire run's artifact has the shape every conformance
    report has -- passed, by_design and failed at the top -- summed over pgspec and the catalog
    and type parts, each part keeping its own; a comparison that did not complete counts nowhere."""
    import by_design

    m = _load("pgwire_catalog_types")
    group = {"group": "g", "requirement": "REQ-1", "decision": "d", "count": 1, "cases": ["x"]}
    pgspec = {"suite": "pgspec", **by_design.outcomes(50, [group], [])}
    catalog = {
        "postgres_version": "17.11",
        "catalog_relations": by_design.outcomes(196, [], [{}] * 15),
        "catalog_functions": by_design.outcomes(477, [{**group, "count": 10}], [{}] * 5),
        "type_conversions": by_design.outcomes(81, [{**group, "count": 3}], [{}] * 5),
    }
    doc = m.combined(pgspec, catalog)
    assert doc["surface"] == "pgwire"
    assert (doc["total"], doc["passed"], doc["by_design"], doc["failed"]) == (843, 804, 14, 25)
    assert doc["pass_rate"] == round(804 / 829, 4)
    assert doc["pgspec"] is pgspec and doc["catalog_relations"]["failed"] == 15
    alone = m.combined(pgspec, None)
    assert (alone["total"], alone["passed"], alone["by_design"], alone["failed"]) == (51, 50, 1, 0)
    assert alone["catalog_functions"] is None
