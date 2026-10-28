# Copyright (c) 2026 Kenneth Stott
# Canary: 4026ec2d-f62d-4f8d-b5ff-bc2a2ad0b67c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Bolt conformance job's fixture model: the driver examples' graph in a domain of its own,
and the role the driver's user acts as there (REQ-2167)."""

# Requirements: REQ-2167

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

from provisa.core.models import Domain, Relationship, Role, Table

CONFORMANCE = Path(__file__).resolve().parents[2] / "scripts" / "conformance"
sys.path.insert(0, str(CONFORMANCE))  # the scripts' sibling imports


def _load(name: str, path: Path):
    """A script as a module of its own name: the Flight SQL job has a fixtures.py too."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # a dataclass resolves its module by name
    spec.loader.exec_module(module)
    return module


fixtures = _load("bolt_fixtures", CONFORMANCE / "bolt" / "fixtures.py")
by_design = _load("by_design", CONFORMANCE / "by_design.py")
pgwire_catalog_types = _load("pgwire_catalog_types", CONFORMANCE / "pgwire_catalog_types.py")


def test_the_examples_graph_is_writable_tables_and_junction_relationships():
    tables = {t["table"]: Table(**t) for t in fixtures.tables()}
    assert set(tables) == {"person", "company", "greeting", "knows", "works_for"}
    assert {t.domain_id for t in tables.values()} == {"bolt_examples"}
    assert {(g.role, g.operation) for g in tables["person"].write_grants} == {
        ("bolt_examples", "insert"),
        ("bolt_examples", "update"),
        ("bolt_examples", "delete"),
    }
    assert {c.name: c.visible_to for c in tables["person"].columns} == {
        "id": ["bolt_examples"],
        "name": ["bolt_examples"],
    }
    rels = {r["alias"]: Relationship(**r) for r in fixtures.relationships()}
    assert {a: (r.source_table_id, r.target_table_id, r.kind) for a, r in rels.items()} == {
        "KNOWS": ("person", "person", "junction"),
        "WORKS_FOR": ("person", "company", "junction"),
    }


def test_a_persons_relationships_cascade_with_it_and_the_tables_start_empty():
    # Each example writes the rows it reads, after its own cleanup has emptied the graph.
    ddl = "\n".join(fixtures.ddl())
    assert ddl.count("ON DELETE CASCADE") == 4
    assert "INSERT" not in ddl


def test_the_model_carries_a_jobs_relationships(tmp_path, monkeypatch):
    monkeypatch.setattr(pgwire_catalog_types, "registrable_columns", lambda dsn: [])
    tables, rels = tmp_path / "t.yaml", tmp_path / "r.yaml"
    tables.write_text(yaml.safe_dump(fixtures.tables()))
    rels.write_text(yaml.safe_dump(fixtures.relationships()))
    pgwire_catalog_types.write_config(tmp_path / "provisa.yaml", 5432, "dsn", tables, rels)
    config = yaml.safe_load((tmp_path / "provisa.yaml").read_text())
    assert [r["alias"] for r in config["relationships"]] == ["KNOWS", "WORKS_FOR"]
    assert {t["table"] for t in config["tables"]} >= {"person", "knows"}


def test_the_drivers_user_acts_as_a_role_reaching_the_examples_domain_alone():
    access = fixtures.access("conformance")
    assert [Domain(**d).id for d in access["domains"]] == ["bolt_examples"]
    (role,) = (Role(**r) for r in access["roles"])
    assert (role.id, role.domain_access) == ("bolt_examples", ["bolt_examples"])
    assert "write" in role.capabilities
    assert access["role_mapping"] == [
        {"claim": "username", "type": "exact", "value": "conformance", "role": "bolt_examples"}
    ]


def _bolt_config(tmp_path, monkeypatch) -> dict:
    monkeypatch.setattr(pgwire_catalog_types, "registrable_columns", lambda dsn: [])
    tables, rels, access = tmp_path / "t.yaml", tmp_path / "r.yaml", tmp_path / "a.yaml"
    tables.write_text(yaml.safe_dump(fixtures.tables()))
    rels.write_text(yaml.safe_dump(fixtures.relationships()))
    access.write_text(yaml.safe_dump(fixtures.access("conformance")))
    pgwire_catalog_types.write_config(tmp_path / "provisa.yaml", 5432, "dsn", tables, rels, access)
    return yaml.safe_load((tmp_path / "provisa.yaml").read_text())


def test_the_model_maps_the_drivers_user_to_its_role_beside_the_shared_fixture(
    tmp_path, monkeypatch
):
    config = _bolt_config(tmp_path, monkeypatch)
    roles = {r["id"]: r for r in config["roles"]}
    # The shared fixture's domain is not the examples' role's: their DETACH DELETE never sees it.
    assert roles["bolt_examples"]["domain_access"] == ["bolt_examples"]
    assert {t["table"]: t["domain_id"] for t in config["tables"]}["types_fixture"] == "conformance"
    assert {d["id"] for d in config["domains"]} >= {"conformance", "bolt_examples"}
    mapped = {m["value"]: m["role"] for m in config["auth"]["role_mapping"]}
    assert mapped["conformance"] == "bolt_examples"
    assert mapped[pgwire_catalog_types.TCK_SESSION_USER] == "tck_empty"


def test_an_access_file_with_another_field_is_refused_by_name(tmp_path, monkeypatch):
    monkeypatch.setattr(pgwire_catalog_types, "registrable_columns", lambda dsn: [])
    access = tmp_path / "a.yaml"
    access.write_text(yaml.safe_dump({**fixtures.access("conformance"), "tables": []}))
    with pytest.raises(SystemExit, match="a.yaml: has .*'tables'"):
        pgwire_catalog_types.write_config(tmp_path / "p.yaml", 5432, "dsn", None, None, access)


def test_the_examples_run_as_written():
    """No step rewrites the driver's examples: their MERGE and DETACH DELETE run as written
    (REQ-2176), and no group of the page says their cleanup or MERGE became anything else."""
    workflow = (CONFORMANCE.parents[1] / ".github/workflows/conformance-bolt.yml").read_text()
    assert "adapt.py" not in workflow
    assert not (CONFORMANCE / "bolt" / "adapt.py").exists()
    groups = {g.group for g in by_design.groups("bolt")}
    assert not groups & {"DETACH DELETE cleanup", "MERGE in an empty graph"}


@pytest.mark.parametrize(
    ("feature", "scenario", "kinds"),
    [
        ("tests.integration.examples.test_session_example", "test_example", {"adapted"}),
        (
            "tests.integration.examples.test_hello_world_example",
            "test_hello_world_example",
            {"adapted"},
        ),
        (
            "tests.integration.mixed.test_async_cancellation",
            "test_async_cancellation_does_not_leak",
            {"adapted"},
        ),
        # The README's graph is the examples' (Person, KNOWS), its MERGE and cleanup as written.
        ("tests.integration.test_readme", "test_should_run_readme", {"adapted"}),
        ("tests.integration.test_readme", "test_readme_contains_example", set()),
        ("tests.integration.examples.test_service_unavailable_example", "test_example", set()),
    ],
)
def test_each_case_is_in_the_groups_that_name_it(feature, scenario, kinds):
    case = {"feature": feature, "scenario": scenario, "reason": ""}
    assert {g.kind for g in by_design.groups("bolt") if g.matches(case)} == kinds


junit_summary = _load("junit_summary", CONFORMANCE / "junit_summary.py")


def _junit(path: Path, cases: dict[str, bool]) -> str:
    body = "".join(
        f'<testcase classname="tests.integration.mixed.t" name="{name}">'
        + ("" if ok else '<failure message="Timeout">stack</failure>')
        + "</testcase>"
        for name, ok in cases.items()
    )
    path.write_text(f"<testsuites><testsuite>{body}</testsuite></testsuites>")
    return str(path)


def test_a_run_in_two_calls_is_summarised_as_one(tmp_path):
    """The suite, and its cancellation test in a call of its own, each write a junit file."""
    suite = _junit(tmp_path / "suite.xml", {"test_a": True, "test_b": True})
    alone = _junit(tmp_path / "alone.xml", {"test_does_not_leak": False})
    doc = junit_summary.summarise(f"{suite},{alone}", "bolt", "suite", "1", "tests")
    assert (doc["passed"], doc["ran"]) == (2, 3)
    assert [f["scenario"] for f in doc["failures"]] == ["test_does_not_leak"]
