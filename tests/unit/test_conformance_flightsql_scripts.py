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
the fixture model, and the cases a page marks adapted or counts as refused by the client before
any request."""

# Requirements: REQ-2167

from __future__ import annotations

import importlib.util
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


def _case(directory: Path, name: str, setup: str, query: str) -> None:
    (directory / f"{name}.txtcase").write_text(
        "// part: metadata\n\n[tags]\n\n"
        f"// part: setup_query\n\n{setup}\n\n// part: query\n\n{query}\n\n"
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


def test_a_renamed_cases_query_is_overridden_to_read_its_table(validation):
    """The checked-in override for timestamp3tz reads the case's own table; an override that is
    not the suite's query with the table renamed, or a missing one, is refused before the
    instance starts."""
    found = fixtures.cases(validation)
    fixtures.check_overrides(found)
    (renamed,) = [c for c in found if c.renamed]
    wrong = fixtures.Case(renamed.name, renamed.table, renamed.setup, "SELECT 1 FROM elsewhere")
    with pytest.raises(ValueError, match="timestamp3tz.sql: expected 'SELECT 1 FROM elsewhere'"):
        fixtures.check_overrides([wrong])
    missing = fixtures.Case("timestamp11", "test_timestamp", "", "SELECT res FROM test_timestamp")
    with pytest.raises(FileNotFoundError, match="timestamp11 reads test_timestamp11"):
        fixtures.check_overrides([missing])


def test_every_checked_in_override_is_for_a_renamed_timestamp_case():
    """Each renamed case has its query, and its metadata naming its table (which the suite reads
    the case's table schema from); nothing else is overridden."""
    renamed = sorted(f"timestamp{p}{tz}" for p in range(10) for tz in ("", "tz"))
    for suffix in ("sql", "toml"):
        assert sorted(p.stem for p in fixtures.OVERRIDES.glob(f"*.{suffix}")) == renamed
    assert {p.suffix for p in fixtures.OVERRIDES.iterdir()} == {".sql", ".toml"}
    assert (fixtures.OVERRIDES / "timestamp3tz.toml").read_text() == (
        '[setup]\ndrop = "test_timestamp3tz"\n'
    )
    names = renamed
    for name in names:
        table = "test_timestamptz" if name.endswith("tz") else "test_timestamp"
        case = fixtures.Case(name, table, "", f"SELECT res FROM {table} ORDER BY idx")
        fixtures.check_overrides([case])


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
    "</testsuite></testsuites>"
)


def test_the_flight_sql_page_marks_adapted_cases_and_counts_client_refusals_apart(tmp_path):
    """REQ-2167 (amended 2026-10-27): an adapted case runs and counts, marked with what its setup
    became; a case the client refused before any request is left out of the pass rate in its own
    group, with the client, its refusal, the evidence and the workaround; any other failure
    stays a failure."""
    xml = tmp_path / "junit.xml"
    xml.write_text(_JUNIT)
    doc = junit.summarise(str(xml), "flight-sql", "ADBC driver validation suite", "9f9f441", "t")
    assert (doc["ran"], doc["passed"], doc["failed"]) == (4, 2, 2)
    assert (doc["not_runnable"], doc["client_refused"]) == (0, 1)
    (refusal,) = doc["refused_by_client"]
    assert refusal["group"] == "Bind before Prepare"
    assert refusal["count"] == 1
    assert "sends no RPC" in refusal["evidence"]
    assert "Prepare the statement before binding" in refusal["workaround"]
    marks = {m["group"]: m["cases"] for m in doc["adapted"]}
    assert marks == {
        "Type cases' tables": [
            {
                "case": "tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/int32]",
                "outcome": "passed",
            }
        ],
        "Timestamp cases' tables": [
            {
                "case": "tests.test_query.TestQuery::"
                "test_query[provisa:flightsql:type/select/timestamp3tz]",
                "outcome": "failed",
            }
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
    assert rate == "50.0% (2 of 4); 1 refused by the client before any request"
    assert "## Refused by the client before any request" in cases
    assert "| Bind before Prepare | 1 | adbc-driver-flightsql 1.12.0 (the Go driver) |" in cases
    assert "## Adapted cases" in cases
    assert "<summary>Timestamp cases' tables (0 of 1 passed)</summary>" in cases
    assert "Setup became: The ten precisions" in cases
    int32 = "tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/int32]"
    assert f"| {int32} | passed |" in cases


def test_a_surface_with_no_such_groups_has_no_such_sections(tmp_path):
    """The other surfaces' pages keep their sections as they were."""
    xml = tmp_path / "junit.xml"
    xml.write_text(_JUNIT)
    doc = junit.summarise(str(xml), "bolt", "driver tests", "6.2.0", "t")
    assert (doc["adapted"], doc["refused_by_client"], doc["client_refused"]) == ([], [], 0)
    assert doc["failed"] == 3
    artifact = {k: v for k, v in doc.items() if k not in ("adapted", "refused_by_client")}
    _, _, _, cases = fill_docs.read(artifact)
    assert "Refused by the client" not in cases
    assert "Adapted cases" not in cases


@pytest.mark.parametrize(
    ("entry", "match"),
    [
        ("- kind: client\n  group: g\n  client: c\n  refusal: r\n  reason: x\n", "has no"),
        ("- kind: adapted\n  group: g\n  case: x\n", r"has no \['became'\]"),
        ("- kind: other\n  group: g\n  reason: x\n", "is of no kind"),
        ("- kind: adapted\n  group: g\n  became: b\n", "matches nothing"),
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
