# Copyright (c) 2026 Kenneth Stott
# Canary: 5e0b7d2a-9c41-4f38-a6de-1b72c9f4e803
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The W3C SPARQL fixture honours REQ-388: a relationship type names one relationship of its
source table, so the config the fixture writes loads, or the dataset is refused by name."""

# Requirements: REQ-388, REQ-2167

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import yaml
from rdflib import Graph, XSD

from provisa.core.config_loader import parse_config_dict

CONFORMANCE = Path(__file__).resolve().parents[2] / "scripts" / "conformance"
sys.path.insert(0, str(CONFORMANCE))  # the scripts' sibling imports


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, CONFORMANCE / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # a dataclass resolves its module by name
    spec.loader.exec_module(module)
    return module


catalog = _load("pgwire_catalog_types")
_load("sparql_w3c_manifest")
fixtures = _load("sparql_w3c_fixtures")

EX = "http://example.org/"
PG = "host=127.0.0.1 port=55432 dbname=conformance user=postgres password=conformance"
#: The variables the instance's model references, as .github/workflows/conformance-sparql.yml
#: sets them (ephemeral values on a throwaway instance, not secrets).
JOB_ENV = {
    "CONFORMANCE_JWT_SECRET": "conformance-jwt-signing-key-not-a-secret-0000",
    "CONFORMANCE_PG_DATABASE": "conformance",
    "CONFORMANCE_PG_HOST": "127.0.0.1",
    "CONFORMANCE_PG_PASSWORD": "conformance",
    "CONFORMANCE_PG_USER": "postgres",
    "PROVISA_SUPERUSER_PASSWORD": "conformance-su-password",
    "PROVISA_SUPERUSER_USERNAME": "conformance-su",
}


def _adapt(turtle: str):
    g = Graph()
    g.parse(data=f"@prefix : <{EX}> . @prefix xsd: <{XSD}> .\n{turtle}", format="turtle")
    return fixtures.adapt("d044", g)


def _config(tmp_path: Path, served: list, monkeypatch) -> dict:
    """The instance's model with ``served`` registered, as the job writes it. The types fixture's
    unsized numeric columns are sized by sampling the job's PostgreSQL, which a unit test has not:
    they take one fixed size here, which the relationships under test do not read."""
    monkeypatch.setattr(
        catalog,
        "registrable_columns",
        lambda _dsn: catalog.registered_columns(
            [(name, typ) for name, typ, _ in catalog.FIXTURE],
            lambda _column: {"data_type": "numeric(38, 4)"},
        ),
    )
    for name, value in JOB_ENV.items():
        monkeypatch.setenv(name, value)
    tables = tmp_path / "tables.yaml"
    rels = tmp_path / "relationships.yaml"
    access = tmp_path / "access.yaml"
    out = tmp_path / "provisa.yaml"
    tables.write_text(yaml.safe_dump([t for f in served for t in fixtures.table_entries(f)]))
    rels.write_text(yaml.safe_dump([r for f in served for r in fixtures.relationship_entries(f)]))
    access.write_text(yaml.safe_dump(fixtures.access(served, "conformance")))
    catalog.write_config(out, 55432, PG, tables, rels, access)
    return yaml.safe_load(out.read_text())


def test_a_predicate_from_one_table_into_two_tables_is_refused_by_name():
    # :b and :c are of different classes, so :knows from :a's table reaches two tables: two
    # relationships of one table under the one type that is :knows's IRI.
    f = _adapt(":a :knows :b, :c . :b a :B . :c a :C .")
    assert f.refusal is not None
    assert f"<{EX}knows>" in f.refusal
    assert "a relationship type names one relationship of its table" in f.refusal
    assert not f.tables and not f.edges


def test_a_predicate_from_two_tables_keeps_one_type_and_loads(tmp_path, monkeypatch):
    # Two source tables, one target table each: one type, one relationship per source table.
    f = _adapt(":a a :A ; :knows :x . :b a :B ; :knows :y .")
    assert f.refusal is None
    assert len(f.edges) == 2 and len({e.source for e in f.edges}) == 2
    assert len({e.rel_type for e in f.edges}) == 1
    config = parse_config_dict(_config(tmp_path, [f], monkeypatch))
    assert {r.alias for r in config.relationships} == {f.edges[0].rel_type}


def test_the_refused_dataset_is_left_out_and_the_rest_loads(tmp_path, monkeypatch):
    refused = _adapt(":a :knows :b, :c . :b a :B . :c a :C .")
    served = fixtures.adapt("d045", _graph(":a :knows :b . :b :knows :c . :a :name 'Ann' ."))
    assert refused.refusal is not None and served.refusal is None
    config = parse_config_dict(_config(tmp_path, [served], monkeypatch))
    assert [r.id for r in config.relationships] == [e.via for e in served.edges]


def _graph(turtle: str) -> Graph:
    g = Graph()
    g.parse(data=f"@prefix : <{EX}> .\n{turtle}", format="turtle")
    return g
