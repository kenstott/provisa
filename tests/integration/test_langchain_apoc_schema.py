# Copyright (c) 2026 Kenneth Stott
# Canary: 2c8f4e19-7a63-4b05-9d21-6e3b8a1c4f70
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""LangChain's Neo4j integration connects to Provisa over Bolt, unchanged, and learns the graph's
schema with apoc.meta.data (REQ-2168).

One real server over a PostgreSQL source holding employees, departments and payroll, each
related to the next. The role ``reader`` (inheriting the standard analyst, so viewer, to which
the APOC set is granted) reaches only the shelter domain and reads employees and departments but no
column of payroll. LangChain's Neo4jGraph, connected as ``reader``, loads the schema: it holds Employees,
Departments and the relationship between them, and nothing of Payroll. A Cypher query it then
writes with apoc.text.join runs and pushes the function down.
"""

# Requirements: REQ-2168

from __future__ import annotations

import os

import pytest
import sqlalchemy as sa

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))

_DDL = [
    "CREATE TABLE public.departments (id integer PRIMARY KEY, title text)",
    "CREATE TABLE public.employees (id integer PRIMARY KEY, name text, dept_id integer)",
    "CREATE TABLE public.payroll (id integer PRIMARY KEY, employee_id integer, salary numeric)",
    "INSERT INTO public.departments VALUES (1, 'Kennels'), (2, 'Clinic')",
    "INSERT INTO public.employees VALUES (1, 'Ada', 1), (2, 'Grace', 2)",
    "INSERT INTO public.payroll VALUES (1, 1, 1000), (2, 2, 2000)",
]


def _table(name: str, columns: list[tuple[str, str]], readers: list[str]) -> dict:
    return {
        "source_id": "sales-pg",
        "domain_id": "shelter",
        "schema": "public",
        "table": name,
        "columns": [
            {
                "name": column,
                "data_type": data_type,
                "visible_to": readers,
                **({"is_primary_key": True} if column == "id" else {}),
            }
            for column, data_type in columns
        ],
    }


@pytest.fixture(scope="module")
def server():
    everyone = ["org_admin", "reader"]
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "domains": [{"id": "shelter", "description": "Animal shelter"}],
            "tables": [
                _table("departments", [("id", "integer"), ("title", "varchar")], everyone),
                _table(
                    "employees",
                    [("id", "integer"), ("name", "varchar"), ("dept_id", "integer")],
                    everyone,
                ),
                _table(
                    "payroll",
                    [("id", "integer"), ("employee_id", "integer"), ("salary", "numeric")],
                    ["org_admin"],
                ),
            ],
            "relationships": [
                {
                    "id": "employee-department",
                    "source_table_id": "employees",
                    "target_table_id": "departments",
                    "source_column": "dept_id",
                    "target_column": "id",
                    "cardinality": "many-to-one",
                    "alias": "WORKS_IN",
                },
                {
                    "id": "payroll-employee",
                    "source_table_id": "payroll",
                    "target_table_id": "employees",
                    "source_column": "employee_id",
                    "target_column": "id",
                    "cardinality": "many-to-one",
                    "alias": "PAID_TO",
                },
            ],
            "roles": [
                {
                    "id": "reader",
                    "description": "Reads the shelter's staff, not its payroll.",
                    "capabilities": ["query_development", "full_results"],
                    "parents": ["analyst"],
                    "domain_access": ["shelter"],
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    engine = sa.create_engine(boot.url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        for ddl in _DDL:
            conn.execute(sa.text(ddl))
    engine.dispose()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


@pytest.fixture(scope="module")
def graph(server):
    from langchain_neo4j import Neo4jGraph

    connected = Neo4jGraph(
        url=f"bolt://127.0.0.1:{server.ports['bolt']}",
        # Passed, never read from the environment: Neo4jGraph takes an empty password as missing
        # and falls back to NEO4J_PASSWORD. The instance has no auth provider, so Bolt reads the
        # principal as the role and does not check the password.
        username="reader",
        password="unchecked",
        database="provisa_reader",
    )
    yield connected
    connected.close()


def test_langchain_loads_the_schema_the_role_reads(graph):
    schema = graph.get_structured_schema
    assert set(schema["node_props"]) == {"Employees", "Departments"}
    # A Cypher property key follows the configured GraphQL naming convention (REQ-471), as
    # keys(n) and every MATCH name it: dept_id is deptId.
    assert {p["property"] for p in schema["node_props"]["Employees"]} == {"id", "name", "deptId"}
    assert schema["relationships"] == [
        {"start": "Employees", "type": "WORKS_IN", "end": "Departments"}
    ]


def test_what_the_role_does_not_read_is_absent(graph):
    assert "Payroll" not in graph.get_schema
    assert "PAID_TO" not in graph.get_schema


def test_apoc_meta_data_without_its_config_takes_the_default(graph):
    # Older LangChain (langchain_community's Neo4jGraph) calls apoc.meta.data() with no argument.
    rows = graph.query(
        "CALL apoc.meta.data() YIELD label, other, elementType, type, property "
        "WHERE type = 'RELATIONSHIP' AND elementType = 'node' "
        "RETURN label, property AS type, other"
    )
    assert rows == [{"label": "Employees", "type": "WORKS_IN", "other": ["Departments"]}]


def test_a_query_written_with_an_apoc_function_runs_and_pushes_it_down(graph):
    rows = graph.query(
        "MATCH (e:Employees)-[:WORKS_IN]->(d:Departments) "
        "RETURN apoc.text.join([e.name, d.title], ' / ') AS label ORDER BY label"
    )
    assert rows == [{"label": "Ada / Kennels"}, {"label": "Grace / Clinic"}]
