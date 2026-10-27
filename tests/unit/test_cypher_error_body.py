# Copyright (c) 2026 Kenneth Stott
# Canary: 2fd766e6-8c15-4e24-8ff7-ed5ce844d251
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-778 (amended 2026-10-24): every error of the graph routes is the app's ApiError body --
``detail``, ``code`` and ``params`` (REQ-1350) -- so every client reads the reason; an access
refusal also lists each violation, which ``detail`` joins."""

from __future__ import annotations

import json
from types import SimpleNamespace

from provisa.api.rest import cypher_router
from provisa.api.rest.cypher_router import CypherRequest, _build_sql_from_ast, access_violations
from provisa.compiler.sql_validator import ValidationViolation as Violation
from provisa.gql.translator_types import CypherTranslateError


def _body(response) -> dict:
    return json.loads(bytes(response.body))


def test_an_access_refusal_is_403_naming_each_violation_and_joining_them(monkeypatch):
    import provisa.compiler.sql_validator as validator
    import provisa.compiler.stage2 as stage2
    import provisa.pgwire._pipeline as pipeline
    import provisa.security.rights as rights

    found = [
        Violation(
            code="column_not_visible", message="Column pets.price is not visible to role all"
        ),
        Violation(code="table_not_visible", message="Table owners is not visible to role all"),
    ]
    monkeypatch.setattr(rights, "effective_domain_access_role", lambda role_id, roles: {})
    monkeypatch.setattr(stage2, "build_governance_context", lambda *a, **k: None)
    monkeypatch.setattr(pipeline, "relationship_guard_bypassed", lambda *a, **k: False)
    monkeypatch.setattr(validator, "validate_sql", lambda *a, **k: found)
    state = SimpleNamespace(roles={}, masking_rules={}, tables=[], relationships=[])
    surface = SimpleNamespace(context=None, rls=None)

    refused = access_violations("SELECT 1", surface, "all", state)

    assert refused is not None and refused.status_code == 403
    assert _body(refused) == {
        "detail": "Column pets.price is not visible to role all; "
        "Table owners is not visible to role all",
        "code": "cypher.access_violations",
        "params": {"count": 2},
        "violations": [
            {
                "code": "column_not_visible",
                "message": "Column pets.price is not visible to role all",
            },
            {"code": "table_not_visible", "message": "Table owners is not visible to role all"},
        ],
    }


def test_a_statement_the_translation_refuses_is_400_with_its_reason():
    def refuse(*_a):
        raise CypherTranslateError("MERGE is not supported")

    refused = _build_sql_from_ast(
        SimpleNamespace(comments=[]),
        None,
        CypherRequest(query="MERGE (n)"),
        refuse,
        None,
        "admin",
        SimpleNamespace(),
    )

    assert refused.status_code == 400
    assert _body(refused) == {
        "detail": "MERGE is not supported",
        "code": "cypher.refused",
        "params": {"reason": "MERGE is not supported"},
    }


def test_no_graph_route_answers_an_error_field():
    import inspect

    from provisa.api.rest import graph_tools_router

    for module in (cypher_router, graph_tools_router):
        assert 'content={"error"' not in inspect.getsource(module), module.__name__
