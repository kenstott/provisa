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


class TestSingleConfirmation:  # REQ-1832, REQ-2114
    def test_a_single_candidate_is_proposed_with_no_prior_permission_question(self):
        s = _flat(chat_mod._SYSTEM)
        assert "If exactly one candidate was found, propose it with admin_mutation right away" in s
        assert "never ask a separate 'shall I?' question first" in s

    def test_a_multi_candidate_pick_comes_before_any_proposal(self):
        s = _flat(chat_mod._SYSTEM)
        assert "Then propose that ONE chosen candidate with admin_mutation" in s
        assert (
            "Never propose a candidate from a multi-candidate result the user has not picked" in s
        )

    def test_the_one_confirmation_is_the_change_shown_in_the_chat(self):
        """REQ-2114: admin_mutation itself shows the change and waits for the user's yes."""
        s = _flat(chat_mod._SYSTEM)
        assert (
            "admin_mutation itself shows the user the change and waits for them to confirm it" in s
        )
        assert "sends it from their own session only when they confirm there" in s

    def test_an_outcome_message_and_a_next_step_follow_the_action(self):
        s = _flat(chat_mod._SYSTEM)
        assert "never end the turn silently right after a tool call" in s
        assert "suggest the natural next step" in s


class TestTheAdminApi:  # REQ-2114
    def test_every_change_is_an_admin_mutation_as_the_user(self):
        s = _flat(chat_mod._SYSTEM)
        assert "you act as the user through the same admin API the UI calls" in s
        assert "Make every change with admin_mutation" in s
        assert "exactly as on the admin page" in s

    def test_rest_actions_and_relationship_discovery_are_reached(self):
        s = _flat(chat_mod._SYSTEM)
        assert "found with admin_rest_routes and reached with admin_request" in s
        assert "run discover_relationships for it" in s
        assert "/admin/discover/candidates/{id}/accept" in s

    def test_show_demo_or_teach_uses_the_screen_not_the_api(self):
        """REQ-2115's rule stands."""
        s = _flat(chat_mod._SYSTEM)
        assert "shown, demoed or taught how something is done, use the screen instead" in s

    def test_the_per_action_source_and_table_tools_are_gone(self):
        names = {t["name"] for t in chat_mod._TOOLS} | set(chat_mod._CLIENT_TOOL_NAMES)
        assert not names & {
            "propose_source",
            "propose_table",
            "create_source_now",
            "register_table_now",
        }
        assert {"admin_schema", "admin_query", "admin_rest_routes", "discover_relationships"} <= {
            t["name"] for t in chat_mod._TOOLS
        }
        assert {"admin_mutation", "admin_request"} <= set(chat_mod._CLIENT_TOOL_NAMES)


class TestReadScreen:  # REQ-2115
    def test_read_screen_is_a_client_tool_taking_no_input(self):
        tool = next(t for t in chat_mod._CLIENT_TOOLS if t["name"] == "read_screen")
        assert tool["input_schema"] == {"type": "object", "properties": {}}
        assert "read_screen" in chat_mod._CLIENT_TOOL_NAMES

    def test_the_prompt_reads_the_screen_when_the_user_points_at_it(self):
        s = _flat(chat_mod._SYSTEM)
        assert "call read_screen first and work from what it returns" in s
        assert "never say you cannot see the screen" in s


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

    def test_the_create_source_input_carries_the_download_fields(self):
        """REQ-2114: what the old proposal tool described now guides the createSource input."""
        s = _flat(chat_mod._SYSTEM)
        assert "sets download_from and leaves path out" in s
        assert "register_table_draft with table_domain_id" in s
