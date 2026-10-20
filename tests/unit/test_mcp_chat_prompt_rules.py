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


class TestClaimsAndFailures:
    """From a maintainer's transcript on the sonnets demo."""

    def test_nothing_is_claimed_done_beside_the_call_that_does_it(self):
        # "Done -- the Cypher explorer (/graph) is now running this...", written with the navigate.
        s = _flat(chat_mod._SYSTEM)
        assert (
            "Text you write in the same message as a tool call is written before that call's result exists"
            in s
        )
        assert "never say there that anything is done, open, running, shown or fixed" in s

    def test_a_failure_is_explained_only_from_its_error(self):
        # A binder error on _nf_query was blamed on "an active natural-language / vector search ...
        # stuck on the Graph page's state", and the user told to clear a search box there is none of.
        s = _flat(chat_mod._SYSTEM)
        assert "Explain a failure only from its error text and the tool results" in s
        assert "never invent a cause, a mechanism, or UI state" in s
        assert "never tell the user to change something you have not seen" in s

    def test_a_provisa_error_is_said_quoted_and_worked_around(self):
        s = _flat(chat_mod._SYSTEM)
        assert "'Binder Error', an internal column such as _nf_query) -- is a Provisa error" in s
        assert "say so, quote it, and offer a working way to the same answer" in s
        assert "never a step for the user to fix it" in s

    def test_a_picture_and_a_graph_page_view_are_both_done_not_offered(self):
        # "collapse by theme ... or render it as a picture here in the chat?"
        s = _flat(chat_mod._SYSTEM)
        assert "Never offer a choice between a picture in the chat and the Graph page" in s
        assert "do both in the same turn: render_graph, and navigate to /graph" in s

    def test_a_choice_carries_all_the_user_needs_in_its_question(self):
        # Text beside the present_choice call is not shown, so the widget's question is all there is.
        s = _flat(chat_mod._SYSTEM)
        assert "`question` is all the user sees beside the options" in s
        assert "the user never sees it" in s


def test_a_failed_catalog_search_is_followed_by_browsing_the_catalog():
    # On the bake-off's server (no embedding model), GPT-5.4 mini's search_catalog failed on every
    # question and it answered "I couldn't search the catalog" eleven times instead of listing
    # the tables, which the other models did.
    s = _flat(chat_mod._SYSTEM)
    assert "A search that fails (no embedding model is enabled, say) is no answer either" in s
    assert "find the data with list_schemas, list_tables and describe_table instead" in s


def test_a_page_is_opened_besides_the_answer_not_instead():
    # GPT-5.4 mini opened the SQL page and said the top city was "the one returned by the SQL query
    # at the top of the results" (bake-off run 38635124027).
    s = _flat(chat_mod._SYSTEM)
    assert (
        "Answer in the chat: opening a page is in addition to the answer, never instead of it" in s
    )


class TestOneFileOrMany:  # REQ-2113
    def test_one_file_is_a_csv_or_parquet_source_given_its_file_by_download(self):
        s = _flat(chat_mod._SYSTEM)
        assert "ONE file by a fully resolved path" in s
        assert "download_from set to that path" in s
        assert "register_table_draft true" in s
        assert "refuses a file mount path by name: offer an https:// or s3:// address" in s

    def test_a_directory_a_glob_or_several_files_is_a_files_source(self):
        s = _flat(chat_mod._SYSTEM)
        assert "A directory, a glob or several files is a files source instead (type='files'" in s

    def test_the_proposal_carries_the_download_fields(self):
        spec = next(t for t in chat_mod._TOOLS if t["name"] == "propose_source")
        described = _flat(spec["input_schema"]["properties"]["source"]["description"])
        assert "download_from" in described and "table_domain_id" in described
