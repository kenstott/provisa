# Copyright (c) 2026 Kenneth Stott
# Canary: 83da44a8-1f6a-4e87-a309-c65fe0e9cb1f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1832 / REQ-1834 / REQ-1846: the rules Polly's system prompt and navigate tool carry."""

from __future__ import annotations

from provisa.api.mcp import chat as chat_mod


def _flat(text: str) -> str:
    return " ".join(text.split())


def _navigate_tool() -> dict:
    return next(t for t in chat_mod._CLIENT_TOOLS if t["name"] == "navigate")


class TestSingleConfirmation:  # REQ-1832
    def test_a_single_candidate_is_proposed_with_no_prior_permission_question(self):
        s = _flat(chat_mod._SYSTEM)
        assert "call propose_source/propose_table right away" in s
        assert (
            "asking permission to merely propose something is asking permission to ask permission"
            in s
        )

    def test_a_multi_candidate_pick_is_the_go_ahead_to_propose(self):
        s = _flat(chat_mod._SYSTEM)
        assert "Their pick IS the go-ahead" in s
        assert "no separate 'would you like me to propose it?' question first" in s

    def test_one_real_confirmation_precedes_the_irreversible_create(self):
        s = _flat(chat_mod._SYSTEM)
        assert "'Create the <name> source now?'" in s
        assert (
            "Never call create_source_now or register_table_now without that explicit confirmation"
            in s
        )

    def test_an_outcome_message_and_a_next_step_follow_the_action(self):
        s = _flat(chat_mod._SYSTEM)
        assert "never end the turn silently right after a tool call" in s
        assert "proactively suggest the natural next step" in s


class TestNavigateBeforeAction:  # REQ-1834
    def test_the_prompt_requires_navigating_to_the_page_first_every_time(self):
        s = _flat(chat_mod._SYSTEM)
        assert "call navigate to that page FIRST" in s
        assert "Do this every single time that kind of action comes up" in s
        assert "even when the action's own tool doesn't require the user to be on that page" in s


class TestNavigateRouteCatalogAndState:  # REQ-1846
    def test_the_description_names_the_real_graphql_route_and_forbids_the_wrong_guesses(self):
        d = _flat(_navigate_tool()["description"])
        assert "/query (GraphQL)" in d
        assert "NOT /explore" in d
        assert "NOT /admin/explorer" in d

    def test_the_tool_accepts_an_optional_state_object(self):
        schema = _navigate_tool()["input_schema"]
        assert schema["properties"]["state"]["type"] == "object"
        assert schema["required"] == ["route"]
        assert "autoRun" in schema["properties"]["state"]["description"]

    def test_state_reaches_the_browser_unchanged_for_a_graphql_query(self):
        call = {
            "id": "t1",
            "name": "navigate",
            "input": {
                "route": "/query",
                "state": {"query": "query Q { iris { id } }", "autoRun": True},
            },
        }
        prepared, note = chat_mod._prepare_client_call(call)
        assert prepared == call
        assert note is None
