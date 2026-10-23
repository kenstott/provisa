# Copyright (c) 2026 Kenneth Stott
# Canary: becbfcf2-019d-477f-8cb5-8bcf222d65ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""/data/graph-counts (REQ-392): a label the engine could not count is said, with its reason, and
a total is a total only when every label in it was counted -- never a sum that left some out."""

from __future__ import annotations

import logging

from fastapi.responses import JSONResponse

from provisa.api.rest.cypher_router import _total, _uncounted


def test_an_uncounted_label_says_why_and_is_logged(caplog):
    refused = JSONResponse(
        status_code=400, content={"error": "Catalog petstore is not attached", "sql": "SELECT"}
    )
    with caplog.at_level(logging.WARNING, logger="provisa.api.rest.cypher_router"):
        entry = _uncounted("MATCH (n:PetStore:INVENTORY) RETURN count(n) AS cnt", refused)
    assert entry == {"count": None, "error": "Catalog petstore is not attached"}
    assert "PetStore:INVENTORY" in caplog.text
    assert "Catalog petstore is not attached" in caplog.text


def test_the_total_is_null_when_any_label_is_uncounted():
    assert _total({"A": {"count": 3}, "B": {"count": 4}}) == 7
    assert _total({"A": {"count": 3}, "B": {"count": None, "error": "not attached"}}) is None
    assert _total({}) == 0


async def test_bolt_totals_are_unknown_when_a_count_fails_and_say_why(monkeypatch, caplog):
    """The Neo4j Browser's node/relationship totals over Bolt: a count the engine could not make
    leaves the total unknown (null), logged and sent as a warning notification -- not 0."""
    from types import SimpleNamespace

    from provisa.bolt import session
    from provisa.core.statement_warnings import collecting

    nodes = {
        "Pet": SimpleNamespace(label="Pet", native_filter_columns=[]),
        "Inquiry": SimpleNamespace(label="Inquiry", native_filter_columns=[]),
    }
    monkeypatch.setattr(
        session,
        "_bolt_label_map",
        lambda *a: SimpleNamespace(nodes=nodes, relationships={}),
    )

    async def _execute(cypher, params, role_id, include_ops):
        if ":Inquiry" in cypher:
            raise RuntimeError("Catalog petstore is not attached")
        return ["cnt"], [[5]], None

    monkeypatch.setattr(session, "_execute_cypher", _execute)
    with caplog.at_level(logging.WARNING, logger="provisa.bolt.session"), collecting() as said:
        node_count, rel_count = await session._graph_counts(None, "analyst", False, None)
    assert node_count is None
    assert rel_count == 0
    assert [w.code for w in said] == ["cypher.count_unknown"]
    assert said[0].params["reason"] == "Catalog petstore is not attached"
    assert "Catalog petstore is not attached" in caplog.text
