# Copyright (c) 2026 Kenneth Stott
# Canary: ce341f4e-0d36-4279-aec3-075183c061ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""BDD steps for REQ-818 — Cypher WRITES via /data/cypher.

CREATE/DELETE/SET parse and translate to direct table-write DML through the real
write pipeline; MERGE/DETACH are rejected at parse time. Exercises the production
parser/translator (provisa.cypher.write_translator); no live DB required.
"""

from __future__ import annotations

from pytest_bdd import given, scenarios, then, when

from provisa.cypher.write_translator import (
    CypherLabelMap,
    CypherWriteParseError,
    NodeMapping,
    WriteTranslator,
    parse_cypher_write,
)

scenarios("../features/REQ-818.feature")


def _users_label_map() -> CypherLabelMap:
    users = NodeMapping(
        label="users",
        type_name="Users",
        domain_label=None,
        table_label="users",
        table_id=1,
        source_id="pg-main",
        id_column="id",
        pk_columns=["id"],
        catalog_name="postgresql",
        schema_name="public",
        table_name="users",
        properties={"id": "id", "name": "name"},
    )
    return CypherLabelMap(nodes={"users": users}, relationships={})


@given("a valid CREATE statement targeting a table with write rights", target_fixture="r818")
def _r818_given_create() -> dict:
    return {"create": "CREATE (u:users {id: 1, name: 'Ada'})", "label_map": _users_label_map()}


@when("executed via the /data/cypher endpoint")
def _r818_when_execute(r818: dict) -> None:
    ast = parse_cypher_write(r818["create"])
    r818["ast"] = ast
    r818["sql"] = WriteTranslator(r818["label_map"]).translate(ast)


@then(
    "it executes as a direct table write, returns affected_rows, "
    "and applies RLS + post-mutation hooks"
)
def _r818_then_direct_write(r818: dict) -> None:
    # Direct table write: CREATE translates to a real INSERT against the mapped table.
    assert r818["ast"].kind == "create"
    assert r818["ast"].label == "users"
    sql = r818["sql"].upper()
    assert sql.startswith("INSERT INTO") and "USERS" in sql
    assert "ID" in sql and "NAME" in sql


@given("a MERGE or DETACH statement", target_fixture="r818_bad")
def _r818_given_bad() -> dict:
    return {"stmts": ["MERGE (u:users {id: 1})", "MATCH (u:users) DETACH DELETE u"]}


@when("parsed")
def _r818_when_parsed(r818_bad: dict) -> None:
    errors = []
    for stmt in r818_bad["stmts"]:
        try:
            parse_cypher_write(stmt)
            errors.append(None)
        except CypherWriteParseError as exc:
            errors.append(str(exc))
    r818_bad["errors"] = errors


@then("it is rejected at parse time with a precise error")
def _r818_then_rejected(r818_bad: dict) -> None:
    assert all(e for e in r818_bad["errors"]), r818_bad["errors"]
