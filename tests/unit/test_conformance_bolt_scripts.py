# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The Bolt conformance job's fixture model and the driver examples' adapted setup (REQ-2167)."""

# Requirements: REQ-2167

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

from provisa.core.models import Relationship, Table

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


adapt = _load("bolt_adapt", CONFORMANCE / "bolt" / "adapt.py")
fixtures = _load("bolt_fixtures", CONFORMANCE / "bolt" / "fixtures.py")
by_design = _load("by_design", CONFORMANCE / "by_design.py")
pgwire_catalog_types = _load("pgwire_catalog_types", CONFORMANCE / "pgwire_catalog_types.py")


def test_the_examples_graph_is_writable_tables_and_junction_relationships():
    tables = {t["table"]: Table(**t) for t in fixtures.tables()}
    assert set(tables) == {"person", "company", "greeting", "knows", "works_for"}
    assert {(g.role, g.operation) for g in tables["person"].write_grants} == {
        ("org_admin", "insert"),
        ("org_admin", "update"),
        ("org_admin", "delete"),
    }
    rels = {r["alias"]: Relationship(**r) for r in fixtures.relationships()}
    assert {a: (r.source_table_id, r.target_table_id, r.kind) for a, r in rels.items()} == {
        "KNOWS": ("person", "person", "junction"),
        "WORKS_FOR": ("person", "company", "junction"),
    }


def test_a_persons_relationships_cascade_with_it_and_the_named_companies_are_seeded():
    ddl = "\n".join(fixtures.ddl())
    assert ddl.count("ON DELETE CASCADE") == 4
    for company in ("Wayne Enterprises", "LexCorp", "Neo4j"):
        assert f"'{company}'" in ddl


def test_the_model_carries_a_jobs_relationships(tmp_path, monkeypatch):
    monkeypatch.setattr(pgwire_catalog_types, "registrable_columns", lambda dsn: ([], []))
    tables, rels = tmp_path / "t.yaml", tmp_path / "r.yaml"
    tables.write_text(yaml.safe_dump(fixtures.tables()))
    rels.write_text(yaml.safe_dump(fixtures.relationships()))
    pgwire_catalog_types.write_config(tmp_path / "provisa.yaml", 5432, "dsn", tables, rels)
    config = yaml.safe_load((tmp_path / "provisa.yaml").read_text())
    assert [r["alias"] for r in config["relationships"]] == ["KNOWS", "WORKS_FOR"]
    assert {t["table"] for t in config["tables"]} >= {"person", "knows"}


def _examples(root: Path) -> Path:
    tests = root / "integration"
    for name, text, _replacement, count in adapt.REWRITES:
        path = tests / name
        path.parent.mkdir(parents=True, exist_ok=True)
        before = path.read_text() if path.exists() else ""
        if text not in before:
            path.write_text(before + "\n".join(f"session.run({text})" for _ in range(count)) + "\n")
    return tests


def test_only_the_examples_setup_and_cleanup_are_rewritten(tmp_path):
    tests = _examples(tmp_path)
    adapt.adapt(tests)
    bookmarks = (tests / "examples/test_pass_bookmarks_example.py").read_text()
    assert "DETACH" not in bookmarks and "MERGE" not in bookmarks
    assert bookmarks.count(adapt.PERSONS_DELETED) == 2
    assert '"CREATE (a)-[:KNOWS]->(b)"' in bookmarks


def test_an_example_that_changed_fails_the_adaptation_by_name(tmp_path):
    tests = _examples(tmp_path)
    session = tests / "examples/test_session_example.py"
    session.write_text(session.read_text().replace(adapt.CLEANUP, '"MATCH (n) DETACH DELETE n"', 1))
    with pytest.raises(SystemExit, match="test_session_example.py"):
        adapt.adapt(tests)


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
        ("tests.integration.test_readme", "test_should_run_readme", {"design"}),
        ("tests.integration.examples.test_service_unavailable_example", "test_example", set()),
    ],
)
def test_each_case_is_in_the_groups_that_name_it(feature, scenario, kinds):
    case = {"feature": feature, "scenario": scenario, "reason": ""}
    assert {g.kind for g in by_design.groups("bolt") if g.matches(case)} == kinds
