# Copyright (c) 2026 Kenneth Stott
# Canary: d3ccb789-b1ad-4d73-9087-37b91354c239
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Flight SQL conformance job's scripts (REQ-2167): the suite's fixture tables adapted into
the fixture model, and the cases a page marks adapted or counts as not sent by the client."""

# Requirements: REQ-2167

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
CONFORMANCE = REPO / "scripts" / "conformance"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


by_design = _load("by_design", CONFORMANCE / "by_design.py")  # the scripts' sibling import
junit = _load("junit_summary", CONFORMANCE / "junit_summary.py")
fill_docs = _load("fill_docs", CONFORMANCE / "fill_docs.py")
fixtures = _load("flightsql_fixtures", CONFORMANCE / "flightsql" / "fixtures.py")

_SELECT = "adbc_drivers_validation/queries/type/select"


_SCHEMA = '{"format": "+s", "children": [{"name": "res", "format": "i", "flags": ["nullable"]}]}'


def _case(directory: Path, name: str, setup: str, query: str) -> None:
    (directory / f"{name}.txtcase").write_text(
        "// part: metadata\n\n[tags]\n\n"
        f"// part: setup_query\n\n{setup}\n\n// part: query\n\n{query}\n\n"
        f"// part: expected_schema\n\n{_SCHEMA}\n\n"
        '// part: expected\n\n{"res": 1}\n'
    )


@pytest.fixture
def validation(tmp_path):
    """A suite checkout with three type/select cases: one whose table is its own, one sharing its
    table's name with another precision, and one written in another dialect."""
    select = tmp_path / _SELECT
    select.mkdir(parents=True)
    _case(
        select,
        "int32",
        "CREATE TABLE test_int32 (idx INTEGER, res INTEGER);",
        "SELECT res FROM test_int32",
    )
    _case(
        select,
        "timestamp3tz",
        "CREATE TABLE test_timestamptz (idx INTEGER, res TIMESTAMP(3) WITH TIME ZONE);\n"
        "INSERT INTO test_timestamptz (idx, res) VALUES (6, NULL);",
        "SELECT res FROM test_timestamptz ORDER BY idx",
    )
    _case(
        select,
        "binary",
        "CREATE TABLE test_binary (idx INTEGER, res BLOB);\n"
        "INSERT INTO test_binary (idx, res) VALUES (3, X'deadbeef'), (4, X'');",
        "SELECT res FROM test_binary ORDER BY idx",
    )
    (select / "literal.txtcase").write_text("// part: query\n\nSELECT 1\n")  # no setup: not adapted
    return tmp_path


def test_each_case_that_creates_its_table_reads_its_own_in_postgresqls_spelling(validation):
    found = {c.name: c for c in fixtures.cases(validation)}
    assert sorted(found) == ["binary", "int32", "timestamp3tz"]
    assert not found["int32"].renamed
    shared = found["timestamp3tz"]
    assert (shared.table, shared.adapted_table, shared.renamed) == (
        "test_timestamptz",
        "test_timestamp3tz",
        True,
    )
    assert "CREATE TABLE test_timestamp3tz (" in shared.adapted_setup
    assert "INSERT INTO test_timestamp3tz " in shared.adapted_setup
    assert shared.adapted_query == "SELECT res FROM test_timestamp3tz ORDER BY idx"
    binary = found["binary"].adapted_setup
    assert "res BYTEA)" in binary
    assert "(3, '\\xdeadbeef'::bytea), (4, '\\x'::bytea)" in binary


def test_each_cases_override_lists_its_table_with_the_link_columns(validation):
    """A case's table is listed with its own columns, then the link columns every model table
    carries; a renamed case's query and metadata name its own table, which the suite reads the
    case's rows and table schema from."""
    found = {c.name: c for c in fixtures.cases(validation)}
    own = fixtures._parts(fixtures.override(found["int32"]))
    assert list(own) == ["catalog_schema"]
    listed = json.loads(own["catalog_schema"])
    assert [c["name"] for c in listed["children"]] == ["res", "_name_", "_domain_"]
    assert listed["children"][1:] == [
        {"name": n, "format": "u", "flags": ["nullable"]} for n in ("_name_", "_domain_")
    ]
    renamed = fixtures._parts(fixtures.override(found["timestamp3tz"]))
    assert list(renamed) == ["metadata", "query", "catalog_schema"]
    assert renamed["metadata"].strip() == '[setup]\ndrop = "test_timestamp3tz"'
    assert renamed["query"].strip() == "SELECT res FROM test_timestamp3tz ORDER BY idx"


def test_an_override_not_made_from_the_suites_case_is_refused(validation, tmp_path, monkeypatch):
    """Before the instance starts: a stale, a missing or a stray override."""
    monkeypatch.setattr(fixtures, "OVERRIDES", tmp_path / "overrides")
    found = fixtures.cases(validation)
    fixtures.write_overrides(found)
    fixtures.check_overrides(found)
    (fixtures.OVERRIDES / "int32.txtcase").write_text("// part: query\n\nSELECT 2\n")
    with pytest.raises(ValueError, match="int32.txtcase: not what fixtures.py overrides makes"):
        fixtures.check_overrides(found)
    (fixtures.OVERRIDES / "int32.txtcase").unlink()
    with pytest.raises(FileNotFoundError, match="int32.txtcase: run fixtures.py overrides"):
        fixtures.check_overrides(found)
    fixtures.write_overrides(found)
    (fixtures.OVERRIDES / "int64.sql").write_text("SELECT 1")
    with pytest.raises(ValueError, match=r"overrides for no adapted case: \['int64.sql'\]"):
        fixtures.check_overrides(found)


def test_the_checked_in_overrides_are_the_suites_select_cases():
    """One generated override per type/select case; the timestamp precisions, which share two
    table names, each name their own."""
    paths = sorted(fixtures.OVERRIDES.iterdir())
    assert {p.suffix for p in paths} == {".txtcase"}
    assert len(paths) == 31
    for path in paths:
        parts = fixtures._parts(path.read_text())
        children = json.loads(parts["catalog_schema"])["children"]
        assert [c["name"] for c in children[-2:]] == ["_name_", "_domain_"], path.name
        if path.stem.startswith("timestamp"):
            assert f'drop = "test_{path.stem}"' in parts["metadata"], path.name
            assert f"FROM test_{path.stem} " in parts["query"], path.name
        else:
            assert list(parts) == ["catalog_schema"], path.name


_JUNIT = (
    '<testsuites><testsuite name="pytest">'
    '<testcase classname="tests.test_query.TestQuery" '
    'name="test_query[provisa:flightsql:type/select/int32]"/>'
    '<testcase classname="tests.test_query.TestQuery" '
    'name="test_query[provisa:flightsql:type/select/timestamp3tz]">'
    '<failure message="AssertionError: res">t</failure></testcase>'
    '<testcase classname="tests.test_query.TestQuery" '
    'name="test_query[provisa:flightsql:type/bind/int64]">'
    '<failure message="adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL '
    'Statement] must set IngestTargetTable before bulk ingestion">t</failure></testcase>'
    '<testcase classname="tests.test_connection.TestConnection" '
    'name="test_get_objects_table_present[provisa:flightsql]"/>'
    '<testcase classname="tests.test_connection.TestConnection" '
    'name="test_current_catalog[provisa:flightsql]">'
    '<failure message="NOT_FOUND: GetSessionOptions">t</failure></testcase>'
    '<testcase classname="tests.test_statement.TestStatement" '
    'name="test_parameter_execute[provisa:flightsql]">'
    '<failure message="OperationalError: Invalid Input Error: Values were not provided for the '
    'following prepared statement parameters: 1">t</failure></testcase>'
    '<testcase classname="tests.test_query.TestQuery" '
    'name="test_query[provisa:flightsql:type/select/timestamp9tz]">'
    '<failure message="AssertionError: Field types do not match: expected res (timestamp[ns, '
    'tz=UTC]) != actual res (timestamp[us, tz=UTC])">t</failure></testcase>'
    "</testsuite></testsuites>"
)


def test_the_flight_sql_page_marks_adapted_cases_and_counts_unsent_ones_apart(tmp_path):
    """REQ-2167 (amended 2026-10-27): an adapted case runs and counts, marked with what its setup
    became; a case whose client did not send what it needs -- refusing before any request, or
    dropping the values it was given -- is left out of the pass rate in its own group, with the
    client, what it does, the evidence and the workaround; a case a design decision excludes is
    counted under its requirement; any other failure stays a failure."""
    xml = tmp_path / "junit.xml"
    xml.write_text(_JUNIT)
    doc = junit.summarise(str(xml), "flight-sql", "ADBC driver validation suite", "9f9f441", "t")
    assert (doc["ran"], doc["passed"], doc["failed"]) == (4, 2, 2)
    assert (doc["not_runnable"], doc["client_not_sent"]) == (1, 2)
    assert [(g["group"], g["requirement"], g["count"]) for g in doc["by_design"]] == [
        ("Timestamps beyond microseconds", "REQ-1961", 1)
    ]
    refused, dropped = doc["not_sent_by_client"]
    assert (refused["group"], refused["count"]) == ("Bind with no Prepare", 1)
    assert "sends no RPC" in refused["evidence"]
    assert "Prepare the statement before binding" in refused["workaround"]
    assert (dropped["group"], dropped["count"]) == ("Bind before Prepare", 1)
    assert "send no DoPut" in dropped["evidence"]
    assert dropped["workaround"] == "Bind after Prepare, as the suite's other bind tests do."
    marks = {m["group"]: m["cases"] for m in doc["adapted"]}
    query = "tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/"
    assert marks == {
        "Type cases' tables": [{"case": f"{query}int32]", "outcome": "passed"}],
        "Timestamp cases' tables": [
            {"case": f"{query}timestamp3tz]", "outcome": "failed"},
            {"case": f"{query}timestamp9tz]", "outcome": "failed"},
        ],
        "get_objects table": [
            {
                "case": "tests.test_connection.TestConnection::"
                "test_get_objects_table_present[provisa:flightsql]",
                "outcome": "passed",
            }
        ],
    }
    assert [f["scenario"] for f in doc["failures"]] == [
        "test_query[provisa:flightsql:type/select/timestamp3tz]",
        "test_current_catalog[provisa:flightsql]",
    ]
    _, _, rate, cases = fill_docs.read(doc)
    assert rate == ("50.0% (2 of 4); 1 cases not runnable by design; 2 not sent by the client")
    assert "## Not sent by the client" in cases
    assert "| Group | Count | Client | What it does | Evidence | Workaround |" in cases
    assert "| Bind before Prepare | 1 | adbc-driver-flightsql 1.12.0 (the Go driver) |" in cases
    assert "| Timestamps beyond microseconds | 1 |" in cases
    assert "## Adapted cases" in cases
    assert "<summary>Timestamp cases' tables (0 of 2 passed)</summary>" in cases
    assert "Setup became: The ten precisions" in cases
    assert f"| {query}int32] | passed |" in cases


def test_a_surface_with_no_such_groups_has_no_such_sections(tmp_path):
    """The other surfaces' pages keep their sections as they were."""
    xml = tmp_path / "junit.xml"
    xml.write_text(_JUNIT)
    doc = junit.summarise(str(xml), "bolt", "driver tests", "6.2.0", "t")
    assert (doc["adapted"], doc["not_sent_by_client"], doc["client_not_sent"]) == ([], [], 0)
    assert doc["failed"] == 5
    artifact = {k: v for k, v in doc.items() if k not in ("adapted", "not_sent_by_client")}
    _, _, _, cases = fill_docs.read(artifact)
    assert "Not sent by the client" not in cases
    assert "Adapted cases" not in cases


@pytest.mark.parametrize(
    ("entry", "match"),
    [
        ("- kind: client\n  group: g\n  client: c\n  behavior: b\n  reason: x\n", "has no"),
        ("- kind: adapted\n  group: g\n  case: x\n", r"has no \['became'\]"),
        ("- kind: other\n  group: g\n  reason: x\n", "is of no kind"),
        ("- kind: adapted\n  group: g\n  became: b\n", "matches nothing"),
        (
            "- group: g\n  requirement: REQ-1\n  decision: as REQ-1 says\n  reason: x\n",
            r"names a requirement in \['decision'\]",
        ),
    ],
)
def test_a_group_missing_its_kinds_fields_is_refused(tmp_path, monkeypatch, entry, match):
    (tmp_path / "x.yaml").write_text(entry)
    monkeypatch.setattr(by_design, "RULES", tmp_path)
    with pytest.raises(ValueError, match=match):
        by_design.groups("x")


class _Columns:
    """A PostgreSQL connection that answers every catalog read with ``columns``."""

    def __init__(self, columns: list[tuple[str, str]]):
        self.columns = columns

    def execute(self, *_args):
        return self

    def fetchall(self) -> list[tuple[str, str]]:
        return self.columns


def test_a_statement_tests_table_is_declared_writable_by_the_job_user():
    """The statement tests write their tables: each is seeded empty and granted org_admin's
    insert, update and delete of every column; a type case's table is only read."""
    assert sorted(fixtures.WRITTEN_TABLES) == ["sample_table", "test_rows_affected"]
    conn = _Columns([("id", "integer"), ("value", "text")])
    written = fixtures._model_table(conn, "sample_table", written=True)
    assert written["write_grants"] == [
        {"role": "org_admin", "operation": "insert", "columns": ["id", "value"]},
        {"role": "org_admin", "operation": "update", "columns": ["id", "value"]},
        {"role": "org_admin", "operation": "delete"},
    ]
    assert [c["name"] for c in written["columns"]] == ["id", "value"]
    assert "write_grants" not in fixtures._model_table(conn, "test_int32")


def test_each_case_the_suite_skips_is_named_with_its_reason(tmp_path):
    """A skipped case is counted apart and listed on the page by name, with the reason the suite
    gave, so what it did not run can be decided case by case."""
    xml = tmp_path / "junit.xml"
    xml.write_text(
        '<testsuites><testsuite name="pytest">'
        '<testcase classname="tests.test_connection.TestConnection" name="test_ok[p]"/>'
        '<testcase classname="tests.test_connection.TestConnection" '
        'name="test_an_unclaimed_feature[p]">'
        '<skipped type="pytest.skip" message="not implemented">skip</skipped></testcase>'
        '<testcase classname="tests.test_statement.TestStatement" name="test_transaction_toggle[p]">'
        '<skipped type="pytest.skip" message="Driver does not support transactions">s</skipped>'
        "</testcase></testsuite></testsuites>"
    )
    doc = junit.summarise(str(xml), "flight-sql", "ADBC driver validation suite", "9f9f441", "t")
    assert (doc["ran"], doc["skipped"]) == (1, 2)
    assert doc["skipped_cases"] == [
        {
            "feature": "tests.test_connection.TestConnection",
            "scenario": "test_an_unclaimed_feature[p]",
            "reason": "not implemented",
        },
        {
            "feature": "tests.test_statement.TestStatement",
            "scenario": "test_transaction_toggle[p]",
            "reason": "Driver does not support transactions",
        },
    ]
    _, _, _, cases = fill_docs.read(doc)
    assert "## Skipped by the suite" in cases
    assert (
        "| tests.test_statement.TestStatement | test_transaction_toggle[p] | Driver does not "
        "support transactions |" in cases
    )


def test_a_skip_a_group_names_is_counted_in_that_group(tmp_path, monkeypatch):
    """A case the suite skips because a feature is not claimed is counted under the design
    decision or the client behaviour behind it, not as skipped by the suite; a skip no group
    names stays a skip."""
    (tmp_path / "s.yaml").write_text(
        "- group: Transactions\n  requirement: REQ-883\n  decision: a no-op\n"
        '  case: "test_transaction_toggle"\n'
        "- kind: client\n  group: No statistics\n  client: c\n  behavior: b\n  evidence: e\n"
        '  workaround: w\n  case: "test_get_statistics"\n'
    )
    monkeypatch.setattr(by_design, "RULES", tmp_path)
    xml = tmp_path / "junit.xml"
    xml.write_text(
        '<testsuites><testsuite name="pytest">'
        '<testcase classname="T" name="test_ok"/>'
        '<testcase classname="T" name="test_transaction_toggle">'
        '<skipped message="Driver does not support transactions">s</skipped></testcase>'
        '<testcase classname="T" name="test_get_statistics">'
        '<skipped message="connection_get_statistics not supported">s</skipped></testcase>'
        '<testcase classname="T" name="test_set_current_catalog">'
        '<skipped message="not implemented">s</skipped></testcase>'
        "</testsuite></testsuites>"
    )
    doc = junit.summarise(str(xml), "s", "suite", "1", "t")
    assert (doc["ran"], doc["passed"], doc["skipped"]) == (1, 1, 1)
    assert [c["scenario"] for c in doc["skipped_cases"]] == ["test_set_current_catalog"]
    assert [(g["group"], g["count"]) for g in doc["by_design"]] == [("Transactions", 1)]
    assert [(g["group"], g["count"]) for g in doc["not_sent_by_client"]] == [("No statistics", 1)]
    assert (doc["not_runnable"], doc["client_not_sent"], doc["total"]) == (1, 1, 2)


@pytest.mark.parametrize(
    ("arrow_format", "declared"),
    [
        ("l", "bigint"),
        ("vu", "text"),
        ("w:15", "bytea"),
        ("d:10,2", "numeric(10,2)"),
        ("ttm", "time(3)"),
        ("tss:", "timestamp(0)"),
        ("tsn:", "timestamp(6)"),  # PostgreSQL holds six digits at most
        ("tsm:Asia/Tokyo", "timestamptz(3)"),
    ],
)
def test_an_ingested_column_is_declared_as_its_postgresql_type(arrow_format, declared):
    assert fixtures.ingest_type(arrow_format) == declared


def test_each_ingest_test_has_the_table_it_would_create(tmp_path):
    """From the suite's ingest cases: test_create's table for every case, the other tests' for
    the cases they run on, and none for a type the model cannot declare."""
    cases = tmp_path / "adbc_drivers_validation/queries/ingest"
    cases.mkdir(parents=True)

    def case(name: str, value: str) -> None:
        schema = {
            "format": "+s",
            "children": [{"name": "idx", "format": "l"}, {"name": "value", "format": value}],
        }
        (cases / f"{name}.txtcase").write_text(
            f"// part: input_schema\n\n{json.dumps(schema)}\n\n// part: input\n\n{{}}\n"
        )

    for name, value in (
        ("string", "u"),
        ("large_string", "U"),
        ("binary", "z"),
        ("decimal_scale_negative", "d:2,-2"),
    ):
        case(name, value)
    tables = fixtures.ingest_tables(tmp_path)
    assert tables["test_ingest_create_large_string"] == [("idx", "bigint"), ("value", "text")]
    assert tables["test_ingest_long_values_binary"] == [("idx", "bigint"), ("value", "bytea")]
    assert "test_ingest_create_decimal_scale_negative" not in tables
    assert {"test_ingest_append_string", "test_ingest_temporary_string"} <= set(tables)
    assert len(tables["test_ingest_many_columns"]) == 100
