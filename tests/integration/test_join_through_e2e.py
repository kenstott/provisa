# Copyright (c) 2026 Kenneth Stott
# Canary: 8b4e1f7a-3c92-4d65-a0b8-5e2d9c7f1a36
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: two relationships sharing a middle table, joined through it (REQ-1586).

The ``test`` instance: the smoke check's isolated instance of the default demo. Two of its
relationship pairs share a middle table:

- a junction: inquiries name a pet and a user (inquiries-to-pets, inquiries-to-users), so pets and
  users are joined through inquiries, whose other columns are the joined relationship's attributes;
- a shared dimension: pets and the shelter's assignments both name a breed (pets-to-shelter-breed,
  shelter-assignments-to-breeds), so pets and assignments are joined through animal_breeds.

Each pair is joined through the admin API as the Relationships list's Join through does it. The two
hops and the joined relationship are then all served: listed in SQL's catalog, traversed in GraphQL
and in Cypher, and the joined relationship reaches exactly the rows its two hops reach together.
"""

# Requirements: REQ-1586

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

from tests.smoke import test_main_is_startable as smoke
from tests.smoke.test_main_is_startable import Instance

# isolated: the instance is the smoke check's, which writes the demo's files and owns its ports.
pytestmark = [pytest.mark.integration, pytest.mark.isolated, pytest.mark.local_platform]

_HEADERS = {"X-Provisa-Role": "org_admin"}
_TIMEOUT = 300


@pytest.fixture(scope="module")
def instance() -> Iterator[Instance]:
    yield from smoke.instance.__wrapped__()


def _ok(resp: httpx.Response) -> Any:
    assert resp.status_code == 200, resp.text
    return resp.json()


def _admin(inst: Instance, query: str, variables: dict | None = None) -> dict[str, Any]:
    body = _ok(
        httpx.post(
            f"{inst.url}/admin/graphql",
            json={"query": query, "variables": variables or {}},
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
    )
    assert "errors" not in body, body
    return body["data"]


def _graphql(inst: Instance, query: str) -> dict[str, Any]:
    body = _ok(
        httpx.post(
            f"{inst.url}/data/graphql", json={"query": query}, headers=_HEADERS, timeout=_TIMEOUT
        )
    )
    assert "errors" not in body, body
    return body["data"]


def _cypher(inst: Instance, query: str) -> list[dict[str, Any]]:
    return _ok(
        httpx.post(
            f"{inst.url}/data/cypher", json={"query": query}, headers=_HEADERS, timeout=_TIMEOUT
        )
    )["rows"]


_PLAN = """
query Plan($first: String!, $second: String!) {
  joinThrough(firstId: $first, secondId: $second) {
    shape name sourceTable sourceColumn targetTable targetColumn
    viaTable viaSourceColumn viaTargetColumn attributes
  }
}
"""

_JOIN = """
mutation Join($first: String!, $second: String!, $id: String!) {
  joinThrough(firstId: $first, secondId: $second, id: $id) { success message code }
}
"""


def _join(inst: Instance, first: str, second: str) -> dict[str, Any]:
    plan = _admin(inst, _PLAN, {"first": first, "second": second})["joinThrough"]
    assert plan is not None, (first, second)
    done = _admin(inst, _JOIN, {"first": first, "second": second, "id": plan["name"]})
    assert done["joinThrough"]["success"] is True, done
    # Joined, the pair is offered no more.
    assert _admin(inst, _PLAN, {"first": first, "second": second})["joinThrough"] is None
    return plan


def _served(inst: Instance, ids: list[str]) -> dict[str, dict[str, Any]]:
    """Each relationship as SQL's catalog lists it: its stored GraphQL field and Cypher type."""
    quoted = ", ".join(f"'{i}'" for i in ids)
    rows = smoke._sql_rows(
        inst,
        "SELECT id, source_table_id, target_table_id, via_table_id, graphql_field, cypher_type "
        f"FROM meta.relationships WHERE id IN ({quoted})",
    )
    return {r["id"]: r for r in rows}


def _roots(inst: Instance) -> dict[int, tuple[str, str]]:
    """Each registered table's root GraphQL field and Cypher label, by id."""
    tables = _admin(inst, "{ tables { id tableName alias graphqlFieldName } }")["tables"]
    return {
        int(t["id"]): (t["graphqlFieldName"], _label(t["alias"] or t["tableName"]))
        for t in tables
        if t["graphqlFieldName"]
    }


def _label(name: str) -> str:
    """A table's Cypher label: its name in PascalCase (pets: Pets, animal_breeds: AnimalBreeds)."""
    return "".join(w[:1].upper() + w[1:] for w in re.split(r"[^A-Za-z0-9]+", name) if w)


def _pairs(rows: list[dict[str, Any]], key: str, field: str, inner: str) -> set[tuple]:
    out: set[tuple] = set()
    for row in rows:
        reached = row[field]
        for each in reached if isinstance(reached, list) else [reached] if reached else []:
            out.add((row[key], each[inner]))
    return out


def test_a_junction_pair_is_joined_and_all_three_relationships_are_served(instance: Instance):
    plan = _join(instance, "inquiries-to-pets", "inquiries-to-users")
    assert plan["shape"] == "junction"
    assert (plan["sourceTable"], plan["viaTable"], plan["targetTable"]) == (
        "pets",
        "inquiries",
        "users",
    )
    assert (plan["viaSourceColumn"], plan["viaTargetColumn"]) == ("pet_id", "user_id")
    # The junction's other columns are the relationship's attributes.
    assert "message" in plan["attributes"] and "pet_id" not in plan["attributes"]

    served = _served(instance, ["inquiries-to-pets", "inquiries-to-users", plan["name"]])
    assert set(served) == {"inquiries-to-pets", "inquiries-to-users", plan["name"]}
    joined = served[plan["name"]]
    assert joined["via_table_id"] == served["inquiries-to-pets"]["source_table_id"]
    assert joined["graphql_field"] and joined["cypher_type"]

    # SQL: the pairs inquiries name, each pet and user registered.
    expected = {
        (r["pet_id"], r["user_id"])
        for r in smoke._sql_rows(
            instance,
            "SELECT DISTINCT i.pet_id, i.user_id FROM default.inquiries i "
            "JOIN pet_store.pets p ON p.id = i.pet_id JOIN default.users u ON u.id = i.user_id",
        )
    }
    assert expected, "the demo's inquiries name pets and users"

    roots = _roots(instance)
    pets_root, pets_label = roots[joined["source_table_id"]]
    _, users_label = roots[joined["target_table_id"]]
    inquiries_root, inquiries_label = roots[joined["via_table_id"]]
    to_pet = served["inquiries-to-pets"]
    to_user = served["inquiries-to-users"]

    # GraphQL: the hops from inquiries, and the joined relationship from pets.
    hops = _graphql(
        instance,
        f"{{ {inquiries_root}(limit: 500) {{ {to_pet['graphql_field']} {{ id }} "
        f"{to_user['graphql_field']} {{ id }} }} }}",
    )[inquiries_root]
    assert hops and all(r[to_pet["graphql_field"]] for r in hops)
    from_hops = {
        (r[to_pet["graphql_field"]]["id"], r[to_user["graphql_field"]]["id"])
        for r in hops
        if r[to_pet["graphql_field"]] and r[to_user["graphql_field"]]
    }
    assert from_hops == expected
    through = _graphql(
        instance, f"{{ {pets_root}(limit: 500) {{ id {joined['graphql_field']} {{ id }} }} }}"
    )[pets_root]
    assert _pairs(through, "id", joined["graphql_field"], "id") == expected

    # Cypher: each hop as an edge from inquiries, and the joined relationship from pets to users.
    for hop, label in ((to_pet, pets_label), (to_user, users_label)):
        rows = _cypher(
            instance,
            f"MATCH (:{inquiries_label})-[:{hop['cypher_type']}]->(n:{label}) "
            "RETURN count(n) AS reached",
        )
        assert rows[0]["reached"] > 0, hop
    rows = _cypher(
        instance,
        f"MATCH (p:{pets_label})-[:{joined['cypher_type']}]->(u:{users_label}) "
        "RETURN p.id AS pet, u.id AS user",
    )
    assert {(r["pet"], r["user"]) for r in rows} == expected


def test_a_shared_dimension_pair_is_joined_and_all_three_relationships_are_served(
    instance: Instance,
):
    plan = _join(instance, "pets-to-shelter-breed", "shelter-assignments-to-breeds")
    assert plan["shape"] == "shared_dimension"
    assert (plan["sourceColumn"], plan["targetColumn"]) == ("breed_name", "breed_name")
    assert (plan["viaSourceColumn"], plan["viaTargetColumn"]) == ("name", "name")

    ids = ["pets-to-shelter-breed", "shelter-assignments-to-breeds", plan["name"]]
    served = _served(instance, ids)
    assert set(served) == set(ids)
    joined = served[plan["name"]]
    to_breed = served["pets-to-shelter-breed"]
    assignment_breed = served["shelter-assignments-to-breeds"]
    assert joined["via_table_id"] == to_breed["target_table_id"]

    roots = _roots(instance)
    pets_root, pets_label = roots[joined["source_table_id"]]
    assignments_root, assignments_label = roots[joined["target_table_id"]]
    _, breeds_label = roots[joined["via_table_id"]]

    # GraphQL: each side's hop to the breed, and the pets each assignment's breed shares.
    pets = _graphql(
        instance, f"{{ {pets_root}(limit: 500) {{ id {to_breed['graphql_field']} {{ name }} }} }}"
    )[pets_root]
    assignments = _graphql(
        instance,
        f"{{ {assignments_root}(limit: 500) {{ id {assignment_breed['graphql_field']} "
        "{ name } } }",
    )[assignments_root]
    pet_breed = _pairs(pets, "id", to_breed["graphql_field"], "name")
    assignment_breeds = _pairs(assignments, "id", assignment_breed["graphql_field"], "name")
    assert pet_breed and assignment_breeds
    expected = {(p, a) for p, b in pet_breed for a, c in assignment_breeds if b == c}
    assert expected, "the demo's pets and assignments share breeds"
    through = _graphql(
        instance, f"{{ {pets_root}(limit: 500) {{ id {joined['graphql_field']} {{ id }} }} }}"
    )[pets_root]
    assert _pairs(through, "id", joined["graphql_field"], "id") == expected

    # Cypher: both hops end at the breed, and the joined relationship runs pets to assignments.
    for hop, label in ((to_breed, pets_label), (assignment_breed, assignments_label)):
        rows = _cypher(
            instance,
            f"MATCH (:{label})-[:{hop['cypher_type']}]->(b:{breeds_label}) "
            "RETURN count(b) AS reached",
        )
        assert rows[0]["reached"] > 0, hop
    rows = _cypher(
        instance,
        f"MATCH (p:{pets_label})-[:{joined['cypher_type']}]->(a:{assignments_label}) "
        "RETURN p.id AS pet, a.id AS assignment",
    )
    assert {(r["pet"], r["assignment"]) for r in rows} == expected
