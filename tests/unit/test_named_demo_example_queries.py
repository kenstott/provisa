# Copyright (c) 2026 Kenneth Stott
# Canary: dd7c6c21-0d45-45e3-ab17-2a35e67438cc
#
# This source code is licensed under the Business Source License 1.1
"""REQ-2009: a named demo's example GraphQL queries reach the UI on /setup/status, and every one
shipped is valid against the demo's views."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from provisa.core.models import config_table_name
from graphql import parse, validate

from provisa.core import demo

_WEBLOGS = Path(__file__).resolve().parents[2] / "demo" / "named" / "weblogs"
_SONNETS = Path(__file__).resolve().parents[2] / "demo" / "named" / "sonnets"


def test_a_named_demo_s_queries_are_read_in_file_name_order(tmp_path, monkeypatch):
    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "2-b.graphql").write_text("query B { b }")
    (tmp_path / "queries" / "1-a.graphql").write_text("query A { a }")
    (tmp_path / "queries" / "notes.md").write_text("not a query")
    monkeypatch.setenv(demo.NAMED_DEMO_DIR_ENV, str(tmp_path))
    assert demo.example_queries(demo.started_demo_directory()) == [
        {"name": "1-a", "query": "query A { a }"},
        {"name": "2-b", "query": "query B { b }"},
    ]


def test_no_named_demo_ships_no_queries(tmp_path, monkeypatch):
    monkeypatch.delenv(demo.NAMED_DEMO_DIR_ENV, raising=False)
    assert demo.example_queries(demo.started_demo_directory()) == []
    monkeypatch.setenv(demo.NAMED_DEMO_DIR_ENV, str(tmp_path))  # a demo with no queries/
    assert demo.example_queries(demo.started_demo_directory()) == []


async def test_the_setup_status_carries_them(tmp_path, monkeypatch):
    from provisa.api import setup_router

    (tmp_path / "queries").mkdir()
    (tmp_path / "queries" / "1-a.graphql").write_text("query A { a }")
    monkeypatch.setenv(demo.NAMED_DEMO_DIR_ENV, str(tmp_path))

    async def state():
        return {"needs_setup": False, "demo_mode": True}

    monkeypatch.setattr(setup_router, "_setup_state", state)
    assert await setup_router.setup_status(_request()) == {
        "needs_setup": False,
        "demo_mode": True,
        "named_demo": tmp_path.name,
        "example_queries": [{"name": "1-a", "query": "query A { a }"}],
        "demo_tour": [],
        "demo_landing": None,
    }


def _weblogs_views_schema():
    """The GraphQL schema the analyst is served for the weblogs demo's views."""
    config = yaml.safe_load((_WEBLOGS / "config.yaml").read_text())
    (role,) = [r for r in config["roles"] if r["id"] == "analyst"]
    return _views_schema(_WEBLOGS, role)


_PARAMETER_KEYS = ("native_filter_type", "native_filter_required", "required_by_steward")


def _views_schema(home: Path, role: dict, *, include_tables: bool = False):
    """The GraphQL schema ``role`` is served for a demo's views (and, with ``include_tables``, its tables
    too), built from the demo's own config as the server builds it (the declared columns and
    types, the tables' parameters, and the relationships between what it holds)."""
    from provisa.compiler import naming
    from provisa.compiler.introspect import ColumnMetadata
    from provisa.compiler.schema_gen import SchemaInput, generate_schema
    from provisa.core.config_loader import views_as_tables

    from provisa.core.models import Table

    raw = yaml.safe_load((home / "config.yaml").read_text())
    # A view's GraphQL fields as the config model reads them: what it declares, else the
    # model's own default (Table.enable_aggregates / enable_group_by).
    fields = {
        name: Table.model_fields[name].default for name in ("enable_aggregates", "enable_group_by")
    }
    config = views_as_tables(raw)
    views = [t for t in config["tables"] if include_tables or "view_sql" in t]
    tables, column_types = [], {}
    for i, view in enumerate(views, start=1):
        tables.append(
            {
                "id": i,
                "source_id": view["source_id"],
                "domain_id": view["domain_id"],
                "schema_name": view.get("schema", "default"),
                "table_name": config_table_name(view),
                "alias": view.get("alias"),
                **({"view_sql": view["view_sql"]} if "view_sql" in view else {}),
                **{name: view.get(name, default) for name, default in fields.items()},
                # A parameter column keeps what makes it a field argument (REQ-1986).
                "columns": [
                    {
                        "column_name": c["name"],
                        "visible_to": c["visible_to"],
                        # A published name is the column's field, as the server serves it.
                        **({"alias": c["alias"]} if c.get("alias") else {}),
                        **{k: c[k] for k in _PARAMETER_KEYS if k in c},
                    }
                    for c in view["columns"]
                ],
                "write_ops": [],
                "write_returns_rows": False,
                "write_refused_forms": [],
            }
        )
        column_types[i] = [
            ColumnMetadata(column_name=c["name"], data_type=c["data_type"], is_nullable=True)
            for c in view["columns"]
        ]
    # The relationships between what the schema holds, by table id: a relationship names its
    # tables by their alias or table name.
    ids = {t.get("alias") or t["table_name"]: t["id"] for t in tables} | {
        t["table_name"]: t["id"] for t in tables
    }
    relationships = [
        {
            **rel,
            "source_table_id": ids[rel["source_table_id"]],
            "target_table_id": ids[rel["target_table_id"]],
            **({"via_table_id": ids[rel["via_table"]]} if rel.get("via_table") else {}),
        }
        for rel in raw.get("relationships", [])
        if rel["source_table_id"] in ids
        and rel["target_table_id"] in ids
        and rel.get("via_table", rel["source_table_id"]) in ids
    ]
    naming.configure(gql="apollo_graphql")
    return generate_schema(
        SchemaInput(
            tables=tables,
            relationships=relationships,
            column_types=column_types,
            naming_rules=[],
            role=role,
            domains=config["domains"],
            domain_prefix=config["naming"]["domain_prefix"],
        )
    )


QUERIES = sorted((_WEBLOGS / "queries").glob("*.graphql"))


def test_the_weblogs_demo_ships_one_example_a_view():
    assert len(QUERIES) == 6


@pytest.mark.parametrize("path", QUERIES, ids=[p.stem for p in QUERIES])
def test_each_weblogs_example_is_valid_against_the_demo_s_views(path):
    # That each runs and answers rows is tests/integration/test_weblogs_demo.py's, on the demo.
    errors = validate(_weblogs_views_schema(), parse(path.read_text()))
    assert errors == [], [e.message for e in errors]


EVERY_QUERY = sorted((_WEBLOGS.parent).glob("*/queries/*.graphql"))


@pytest.mark.parametrize(
    "path", EVERY_QUERY, ids=[f"{p.parents[1].name}/{p.stem}" for p in EVERY_QUERY]
)
def test_each_example_of_every_demo_is_valid_against_its_tables_and_views(path):
    # As org_admin, who reads every table and view of the demo (scripts/check_demo.py's check).
    admin = {"id": "org_admin", "capabilities": [], "domain_access": ["*"]}
    errors = validate(
        _views_schema(path.parents[1], admin, include_tables=True), parse(path.read_text())
    )
    assert errors == [], [e.message for e in errors]


SONNETS_QUERIES = sorted((_SONNETS / "queries").glob("*.graphql"))


@pytest.mark.parametrize("path", SONNETS_QUERIES, ids=[p.stem for p in SONNETS_QUERIES])
def test_each_sonnets_example_is_valid_against_the_demo_s_views(path):
    # REQ-2016. That each runs and answers rows is tests/integration/test_sonnets_demo.py's.
    admin = {"id": "org_admin", "capabilities": [], "domain_access": ["*"]}
    errors = validate(_views_schema(_SONNETS, admin), parse(path.read_text()))
    assert errors == [], [e.message for e in errors]


def _request(headers: dict[str, str] | None = None):
    """A request to /setup/status, with ``headers``."""
    from starlette.requests import Request

    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request({"type": "http", "method": "GET", "path": "/setup/status", "headers": raw})
