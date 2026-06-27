# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""Step definitions for REQ-601 — OpenAPI Auto-Registration Connector.

OpenAPI virtual table names are derived from the operation's ``operationId``.
If no ``operationId`` is defined, Provisa slugifies ``{method}_{path}``. An alias
is derived by stripping the leading verb segment and singularizing the noun
(e.g. ``findPetsByStatus`` → ``pet_by_status``). The alias is used as the
consumer-facing name in GraphQL and other query interfaces.
"""
from __future__ import annotations

import pytest
from pytest_bdd import given, when, then, parsers, scenarios

from provisa.openapi.loader import parse_text
from provisa.openapi.mapper import OpenAPIQuery
from provisa.openapi.register import _operation_id_to_alias

scenarios("../features/REQ-601.feature")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def shared_data() -> dict:
    """Plain dict used to pass state between Given/When/Then steps."""
    return {}


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
@given(parsers.parse('an OpenAPI spec with operationId "{op_id}"'))
def given_spec_with_operation_id(shared_data, op_id):
    spec_text = f"""
openapi: "3.0.0"
info:
  title: Pet Store API
  version: "1.0.0"
components:
  schemas:
    Pet:
      type: object
      properties:
        id:
          type: integer
        name:
          type: string
        status:
          type: string
paths:
  /pets/findByStatus:
    get:
      operationId: {op_id}
      summary: Finds pets by status
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Pet"
"""
    spec = parse_text(spec_text)
    assert spec["openapi"] == "3.0.0"

    # Locate the operation we just declared and confirm its operationId.
    get_op = spec["paths"]["/pets/findByStatus"]["get"]
    assert get_op["operationId"] == op_id

    shared_data["spec"] = spec
    shared_data["operation_id"] = op_id
    shared_data["path"] = "/pets/findByStatus"


@when("the spec is registered")
def when_spec_registered(shared_data):
    spec = shared_data["spec"]
    registrations: dict = {}

    for path, methods in spec.get("paths", {}).items():
        for method, op in methods.items():
            op_id = op.get("operationId")
            if not op_id:
                op_id = f"{method}_{path}"
            if method.lower() != "get":
                continue
            query = OpenAPIQuery(
                operation_id=op_id,
                path=path,
                method=method.upper(),
                summary=op.get("summary"),
            )
            alias = _operation_id_to_alias(op_id)
            registrations[op_id] = {
                "descriptor": query,
                "alias": alias,
            }

    assert registrations, "no GET operations were registered as virtual tables"
    shared_data["registrations"] = registrations


@then(
    parsers.parse(
        'the virtual table alias is "{alias}" used as the consumer-facing GraphQL name'
    )
)
def then_alias_is(shared_data, alias):
    registrations = shared_data["registrations"]
    op_id = shared_data["operation_id"]

    assert op_id in registrations, f"operation {op_id} was not registered"
    derived_alias = registrations[op_id]["alias"]

    # The verb segment is stripped and the noun singularized.
    assert derived_alias == alias, (
        f"expected alias {alias!r} for operationId {op_id!r}, got {derived_alias!r}"
    )

    # The alias is a valid consumer-facing GraphQL field name (snake_case, no verb).
    assert derived_alias.replace("_", "").isalnum()
    assert not derived_alias.startswith("find_")
    assert "pets" not in derived_alias.split("_"), "noun was not singularized"

    # Direct confirmation against the registration helper used by Provisa.
    assert _operation_id_to_alias(op_id) == alias
