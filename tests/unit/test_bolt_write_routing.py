# Copyright (c) 2026 Kenneth Stott
# Canary: 2c1408c2-1cf0-41c7-bd8a-00e9858867ae
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A model write over Bolt with nothing in the connection's session reaches the governed write
path as it did before the ephemeral layer (REQ-2176, REQ-670): the driver examples' cleanup
DELETE and their CREATE ... SET ... RETURN. Only the parse decides that a statement is a write;
an error the write itself raises is its answer, never sent on to the read path."""

# Requirements: REQ-2176, REQ-670

from __future__ import annotations

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.compiler import temp_tables
from provisa.cypher.write_translator import CypherWriteParseError
from provisa.gql.label_map import CypherLabelMap
from tests.unit.ephemeral_harness import model_node

pytestmark = [pytest.mark.asyncio(loop_scope="session")]

_MODEL = CypherLabelMap(
    nodes={
        "Person": model_node("Person", {"id": "integer", "name": "text"}),
        "Greeting": model_node("Greeting", {"id": "integer", "message": "text"}),
    },
    relationships={},
    nodes_by_table={"Person": ["Person"], "Greeting": ["Greeting"]},
)


class _Kept:
    def __init__(self, *_args, **_kwargs) -> None:
        pass

    def cached(self):
        return None

    def record(self, _translation) -> None:
        pass


async def _run(cypher: str, governed_write) -> object:
    from provisa.bolt.session import _execute_cypher

    held = temp_tables.TempSession()  # the connection's session, holding nothing
    token = temp_tables.bind(held)
    try:
        with ExitStack() as stack:
            for p in (
                patch("provisa.api.app.state", SimpleNamespace(contexts={"org_admin": object()})),
                patch("provisa.bolt.session._system_query", return_value=None),
                patch("provisa.bolt.session._maybe_invoke_metric_call", return_value=None),
                patch("provisa.bolt.session._maybe_invoke_command_call", return_value=None),
                patch("provisa.bolt.session._bolt_label_map", return_value=_MODEL),
                patch("provisa.bolt.session._bolt_domain_access", return_value=["*"]),
                patch("provisa.api.rest.cypher_plan.TranslationRequest", _Kept),
                patch("provisa.bolt.session._execute_write_cypher", governed_write),
            ):
                stack.enter_context(p)
            return await _execute_cypher(cypher, {"message": "hello"}, "org_admin")
    finally:
        temp_tables.unbind(token)


@pytest.mark.parametrize(
    "cypher",
    [
        "MATCH (_:Person) WHERE _.id IS NOT NULL DELETE _",
        "CREATE (a:Greeting) SET a.message = $message RETURN a.message + ', from node ' + id(a)",
    ],
)
async def test_a_model_write_with_an_empty_session_reaches_the_governed_write_path(cypher):
    written: list[str] = []

    async def governed_write(statement, *_args):
        written.append(statement)
        return ["n"], [[1]]

    assert await _run(cypher, governed_write) == (["n"], [[1]], None)
    assert written == [cypher]


async def test_an_error_the_governed_write_raises_is_its_answer():
    async def governed_write(statement, *_args):
        raise CypherWriteParseError("Label 'Person' is not registered in the label map")

    with pytest.raises(CypherWriteParseError, match="not registered"):
        await _run("MATCH (_:Person) WHERE _.id IS NOT NULL DELETE _", governed_write)
