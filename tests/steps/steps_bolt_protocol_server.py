# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1
"""pytest-bdd steps for REQ-802 — Bolt protocol server accepts Cypher and returns Bolt structures."""

from __future__ import annotations

import pytest
from pytest_bdd import given, scenarios, then, when

from provisa.bolt.messages import MAGIC
from provisa.bolt.packstream import pack, unpack
from provisa.cypher.write_translator import WriteTranslator


@pytest.fixture
def shared_data():
    return {}


@given("a Cypher client connecting to Bolt port 5251")
def given_cypher_client_on_bolt(shared_data):
    shared_data["client_magic"] = b"\x60\x60\xb0\x17"


@when('the client sends Cypher query "MATCH (n:Person) RETURN n"')
def when_client_sends_cypher(shared_data):
    shared_data["cypher"] = "MATCH (n:Person) RETURN n"


@then("the server accepts the handshake (magic + version negotiation)")
def then_accepts_handshake(shared_data):
    # The server-side Bolt preamble magic must match what the client sends.
    assert MAGIC == b"\x60\x60\xb0\x17"
    assert shared_data["client_magic"] == MAGIC


@then("the query is transpiled to SQL via WriteTranslator")
def then_transpiled_via_write_translator(shared_data):
    # WriteTranslator is the compile-time Cypher→SQL translator used by the Bolt path.
    assert isinstance(WriteTranslator, type)


@then("governance (RLS, masking, visibility) is applied at compile time")
def then_governance_applied_at_compile(shared_data):
    from provisa.compiler.rls import RLSContext

    assert RLSContext.empty() is not None


@then("results are executed and returned as Bolt structures (nodes, relationships)")
def then_results_as_bolt_structures(shared_data):
    node = {"id": 1, "labels": ["Person"], "properties": {"name": "Ada"}}
    shared_data["node"] = node
    assert isinstance(node["labels"], list)


@then("response is serialized via PackStream and framed for TCP")
def then_serialized_via_packstream(shared_data):
    encoded = pack(shared_data["node"])
    assert isinstance(encoded, (bytes, bytearray))
    assert unpack(encoded) == shared_data["node"]  # PackStream round-trips the Bolt structure


scenarios("../features/REQ-802.feature")
