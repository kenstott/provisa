# Copyright (c) 2026 Kenneth Stott
# Canary: 53361ab8-2c47-42a3-9fc5-c970bc9cd7c8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The customer360 demo's entity-resolution rerun, registered as Provisa commands (REQ-2034,
amended 2026-11-14).

The code source (entity_resolution/er_source.py, shipped as code/entity_resolution.tar.gz) has two
@query functions, each given every registry table as a TableRef of the columns the matching reads
of it; config.yaml registers them as Provisa would derive them, and two statement commands, each
one MERGE replacing one bridge from one of them. What the queries answer is the job's run over the
rows the demo declares, the bridges it ships. The bridges are written as er_writer, and only the
entity steward may delete their rows, which a rerun does."""

# Requirements: REQ-2034
from __future__ import annotations

import importlib
import json
import subprocess
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import sqlglot
import yaml
from sqlglot import exp

pytestmark = pytest.mark.unit

_REPO = Path(__file__).resolve().parents[2]
_DEMO = _REPO / "demo" / "named" / "customer360"
_CODE = _DEMO / "entity_resolution"
for _path in (_CODE, _REPO / "provisa" / "python" / "services"):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

ER = importlib.import_module("entity_resolution")
JOB = importlib.import_module("er_job")
SOURCE = importlib.import_module("er_source")
PACK = importlib.import_module("pack")

_CONFIG = yaml.safe_load((_DEMO / "config.yaml").read_text())
_FUNCTIONS = {f["name"]: f for f in _CONFIG["functions"]}
_QUERIES = {"match_organizations": "org", "match_people": "person"}
_COMMANDS = {
    "rerun_org_bridge": ("entity_org_bridge", "match_organizations", JOB.ORG_COLUMNS),
    "rerun_person_bridge": ("entity_person_bridge", "match_people", JOB.PERSON_COLUMNS),
}
_KEY = ["source_schema", "source_table", "source_column", "source_key"]


def _source(source_id: str) -> dict:
    [source] = [s for s in _CONFIG["sources"] if s["id"] == source_id]
    return source


def _table(source_id: str, table: str) -> dict:
    [found] = [t for t in _CONFIG["tables"] if (t["source_id"], t["table"]) == (source_id, table)]
    return found


@pytest.fixture(scope="module")
def manifest():
    """The code source's functions, as Provisa's Python reader reads the package."""
    from provisa.codesource.manifest import PYTHON_RULES, read_manifest
    from provisa.python.source import READER

    entry = _source("entity_resolution")["mapping"]["entry"]
    done = subprocess.run(
        [sys.executable, "-I", str(READER), str(_CODE), entry], capture_output=True, text=True
    )
    assert done.returncode == 0, done.stderr
    return read_manifest(json.loads(done.stdout), PYTHON_RULES)


# --------------------------------------------------------------------------------------------- #
# The code source and its two queries.                                                          #
# --------------------------------------------------------------------------------------------- #


def test_the_code_source_offers_the_two_queries_and_nothing_else(manifest):
    assert {(e.name, e.kind) for e in manifest.exports} == {
        ("match_organizations", "query"),
        ("match_people", "query"),
    }


def test_each_query_is_given_every_registry_table_with_the_columns_the_matching_reads(manifest):
    read = ER.columns_read()
    assert sorted(SOURCE.TABLES.values()) == sorted(read)
    from provisa.codesource.commands import contract

    for name in _QUERIES:
        arguments, _ = contract(manifest, manifest.export(name))
        assert [a["name"] for a in arguments] == list(SOURCE.TABLES), name
        for argument in arguments:
            assert argument["arg_kind"] == "table_ref"
            columns = [c["name"] for c in argument["columns"]]
            assert columns == read[SOURCE.TABLES[argument["name"]]], argument["name"]


def test_each_table_argument_is_the_registry_tables_registered_name():
    for name, (source, table) in SOURCE.TABLES.items():
        registered = _table(source, table)
        assert registered["alias"] == name and registered["domain_id"] == "customer360", name


def test_config_registers_each_query_as_provisa_derives_it_from_the_code(manifest):
    from provisa.codesource.commands import CODE_COMMAND, SCHEMA, command_kind, contract

    for name in _QUERIES:
        export = manifest.export(name)
        arguments, outputs = contract(manifest, export)
        declared = _FUNCTIONS[name]
        assert declared["impl_kind"] == CODE_COMMAND and declared["schema"] == SCHEMA
        assert declared["source_id"] == "entity_resolution" and declared["function_name"] == name
        assert declared["kind"] == command_kind(export) == "query"
        assert declared["arguments"] == arguments, name
        assert declared["output_columns"] == outputs, name
        assert [c["name"] for c in outputs] == (
            JOB.ORG_COLUMNS if _QUERIES[name] == "org" else JOB.PERSON_COLUMNS
        )


def test_the_shipped_archive_is_the_package_packed_and_config_names_its_hash():
    archive = (_DEMO / "code" / PACK.ARCHIVE).read_bytes()
    assert archive == PACK.archive()
    code = _source("entity_resolution")["mapping"]["code"]
    assert code == {"kind": "upload", "upload": PACK.upload_hash(archive), "filename": PACK.ARCHIVE}
    with tarfile.open(_DEMO / "code" / PACK.ARCHIVE) as tar:
        names = sorted(m.name for m in tar.getmembers() if m.isfile())
    pyproject = (_CODE / "pyproject.toml").read_text()
    modules = sorted(PACK.FILES)
    assert names == [f"{_CODE.name}/{f}" for f in modules]
    for module in (f for f in modules if f.endswith(".py")):
        assert f'"{module}"' in pyproject, module  # the package's only-include lists it


def test_what_the_queries_answer_is_the_jobs_run_over_the_declared_rows(monkeypatch):
    rows, columns = JOB.read_replica_rows(_DEMO)
    shipped = JOB.run(rows, columns, JOB.SEED_RUN_ID)
    given = {
        name: SimpleNamespace(ref=f"customer360.{name}", where=where, columns=tuple(columns[where]))
        for name, where in SOURCE.TABLES.items()
    }
    monkeypatch.setattr(SOURCE, "read_table", lambda table: rows[table.where])

    def without_run(found: list[dict]) -> list[dict]:
        return [{k: v for k, v in r.items() if k != "match_run_id"} for r in found]

    org = [vars(r) for r in SOURCE.match_organizations(**given)]
    people = [vars(r) for r in SOURCE.match_people(**given)]
    assert without_run(org) == without_run(shipped.org)
    assert without_run(people) == without_run(shipped.person)
    runs = {r["match_run_id"] for r in org + people}
    assert len(runs) == 1 and runs != {JOB.SEED_RUN_ID}


# --------------------------------------------------------------------------------------------- #
# Reading a table over Arrow Flight (REQ-2226).                                                 #
# --------------------------------------------------------------------------------------------- #


class _Reference(SimpleNamespace):
    """A table reference whose ``read`` answers as ``TableRef.read`` would: ``answer`` or the
    refusal it raises."""

    def read(self):
        if isinstance(self.answer, BaseException):
            raise self.answer
        return self.answer


def test_a_table_is_read_over_flight_each_value_text_or_null():
    import datetime

    import pyarrow as pa

    answer = pa.table(
        {
            "customer": ["Redfern Hottles", "Quillon Systems"],
            "reporter_email": ["a@redfern.com", None],
            "opened": [datetime.date(2024, 1, 2), None],
        }
    )
    rows = SOURCE.read_table(_Reference(ref="customer360.bugs", answer=answer))
    assert rows == [
        {"customer": "Redfern Hottles", "reporter_email": "a@redfern.com", "opened": "2024-01-02"},
        {"customer": "Quillon Systems", "reporter_email": None, "opened": None},
    ]


def test_a_read_provisa_refuses_is_refused_naming_the_table_and_the_answer():
    import pyarrow.flight as flight

    refusal = flight.FlightUnauthorizedError("column reporter_email is not visible")
    reference = _Reference(ref="customer360.bugs", answer=refusal)
    with pytest.raises(SOURCE.TableReadFailed, match="customer360.bugs: column reporter_email"):
        SOURCE.read_table(reference)


def test_a_table_not_given_over_flight_is_refused_naming_it():
    refusal = ValueError("table 'customer360.bugs' was given over PostgreSQL")
    with pytest.raises(SOURCE.TableReadFailed, match="customer360.bugs: table"):
        SOURCE.read_table(_Reference(ref="customer360.bugs", answer=refusal))


# --------------------------------------------------------------------------------------------- #
# The two rerun commands.                                                                       #
# --------------------------------------------------------------------------------------------- #


@pytest.mark.parametrize("command", sorted(_COMMANDS))
def test_each_rerun_command_is_one_merge_replacing_its_bridge_from_its_query(command):
    from provisa.core.statement_declaration import declared

    table, query, columns = _COMMANDS[command]
    fn = _FUNCTIONS[command]
    assert fn["impl_kind"] == "statement" and fn.get("arguments", []) == []
    found = declared(command, fn["binding"], [])
    assert (found.kind, found.writes) == ("mutation", f"customer360.{table}")

    merge = sqlglot.parse_one(fn["binding"]["sql"], read="postgres")
    assert isinstance(merge, exp.Merge)
    using = merge.args["using"]
    [call] = [f for f in using.find_all(exp.Anonymous) if f.name == query]
    assert [a.name for a in call.expressions] == [f"customer360.{name}" for name in SOURCE.TABLES]
    on = {(c.left.sql(), c.right.sql()) for c in merge.args["on"].find_all(exp.EQ)}
    assert on == {(f"t.{k}", f"s.{k}") for k in _KEY}

    whens = merge.args["whens"].expressions
    matched, inserted, by_source = whens
    assert matched.args["matched"] and not matched.args.get("source")
    updated = [e.this.name for e in matched.args["then"].expressions]
    assert updated == [c for c in columns if c not in _KEY]
    assert not inserted.args["matched"] and not inserted.args.get("source")
    assert [c.name for c in inserted.args["then"].this.expressions] == columns
    assert by_source.args.get("source") and isinstance(by_source.args["then"], exp.Var)
    assert by_source.args["then"].name.upper() == "DELETE"


def test_the_queries_and_commands_are_assigned_to_data_stewards():
    for name in [*_QUERIES, *_COMMANDS]:
        assert _FUNCTIONS[name]["visible_to"] == ["data_steward"], name


# --------------------------------------------------------------------------------------------- #
# Who may write the bridges.                                                                    #
# --------------------------------------------------------------------------------------------- #


def test_the_bridges_are_read_and_written_as_er_writer_which_may_merge_into_them():
    er = _source("er")
    assert (er["username"], er["password"]) == ("er_writer", "er_writer")
    reader = (_DEMO / "db" / "reader.sql").read_text()
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA er TO er_writer;" in reader


# What the entity steward holds: running the two commands, reading every registry table whole (a
# rerun that read a capped table would delete the mentions past the cap), and writing.
_STEWARD_CAPABILITIES = {"command_execution", "full_results", "write"}


def test_only_the_entity_steward_may_delete_bridge_rows_and_it_writes_as_a_data_steward():
    [steward] = [r for r in _CONFIG["roles"] if r["id"] == "entity_steward"]
    # data_steward holds no write right; data_writer is the parent that holds it.
    assert steward["parents"] == ["data_steward", "data_writer"]
    assert set(steward["capabilities"]) == _STEWARD_CAPABILITIES
    for table, _, columns in _COMMANDS.values():
        grants = {(g["role"], g["operation"]): g for g in _table("er", table)["write_grants"]}
        assert set(grants) == {
            ("data_steward", "insert"),
            ("data_steward", "update"),
            ("entity_steward", "delete"),
        }, table
        assert grants[("data_steward", "insert")]["columns"] == columns
        assert grants[("data_steward", "update")]["columns"] == columns


def _server_config(monkeypatch):
    """The demo's config.yaml as the server parses it at start (api/app.py: read with its
    includes, then parse_config_dict), with the store namespace the bundle runs under."""
    from provisa.core.config_loader import parse_config_dict, read_config_with_includes
    from provisa.core.demo_bundles import store_namespaces

    for name, value in store_namespaces(_DEMO).items():
        monkeypatch.setenv(name, value)
    return parse_config_dict(read_config_with_includes(_DEMO / "config.yaml"))


def test_the_server_accepts_the_demo_config(monkeypatch):
    config = _server_config(monkeypatch)
    assert {"entity_steward", "customer_success"} <= {r.id for r in config.roles}
    assert {"rerun_org_bridge", "rerun_person_bridge"} <= {f.name for f in config.functions}


def test_the_entity_steward_holds_only_what_the_rerun_needs_and_no_steward_above_it_writes(
    monkeypatch,
):
    from provisa.core.db import standard_role_dicts
    from provisa.security.inheritance import flatten_role_dicts

    config = _server_config(monkeypatch)
    roles = [*standard_role_dicts(), *(r.model_dump() for r in config.roles)]
    held = {r["id"]: set(r["capabilities"]) for r in flatten_role_dicts(roles)}
    assert held["entity_steward"] == _STEWARD_CAPABILITIES
    assert "write" not in held["data_steward"]
