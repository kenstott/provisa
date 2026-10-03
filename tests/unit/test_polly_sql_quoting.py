# Copyright (c) 2026 Kenneth Stott
# Canary: b2cb6707-48ec-4386-903b-355fa464cc66
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly's SQL leaves the chat with every identifier quoted.

A model writes `SELECT order FROM sales` as readily as anything else, and a column named for a
reserved word then breaks the query. The quoting is done here, on the SQL itself, not left to the
prompt: the SQL handed to the SQL explorer (the navigate tool) and the SQL Polly runs (run_sql) is
rendered with every identifier quoted. SQL that does not parse is handed on unchanged, with the
parse error said, never silently altered.
"""

from __future__ import annotations

import pytest

from provisa.api.mcp.sql_quoting import quote_identifiers


class TestQuoteIdentifiers:
    def test_a_reserved_word_column_is_quoted(self):
        sql, error = quote_identifiers("select order, user from sales where order > 1")
        assert error is None
        assert sql == 'SELECT "order", "user" FROM "sales" WHERE "order" > 1'

    def test_unquoted_names_fold_to_lower_case_as_postgres_folds_them(self):
        sql, _ = quote_identifiers("SELECT Amount FROM Orders")
        assert sql == 'SELECT "amount" FROM "orders"'

    def test_an_already_quoted_name_keeps_its_case(self):
        sql, _ = quote_identifiers('SELECT "Amount" FROM orders')
        assert sql == 'SELECT "Amount" FROM "orders"'

    def test_every_statement_of_a_batch_is_quoted(self):
        sql, _ = quote_identifiers("select order from a; select user from b")
        assert sql == 'SELECT "order" FROM "a";\nSELECT "user" FROM "b"'

    def test_sql_that_does_not_parse_is_returned_unchanged_with_the_error(self):
        sql, error = quote_identifiers("select from where")
        assert sql == "select from where"
        assert error is not None and "table name" in error


class TestTheChatQuotesWhatItHandsOn:
    def test_the_sql_explorer_hand_off_is_quoted(self):
        from provisa.api.mcp.chat import _prepare_client_call

        call = {
            "id": "t1",
            "name": "navigate",
            "input": {
                "route": "/sql",
                "state": {"sql": "select order from sales", "autoRun": True},
            },
        }
        prepared, note = _prepare_client_call(call)
        assert prepared["input"]["state"]["sql"] == 'SELECT "order" FROM "sales"'
        assert prepared["input"]["state"]["autoRun"] is True
        assert note is None

    def test_an_unparsable_hand_off_is_unchanged_and_says_why(self):
        from provisa.api.mcp.chat import _prepare_client_call

        call = {
            "id": "t1",
            "name": "navigate",
            "input": {"route": "/sql", "state": {"sql": "select from where"}},
        }
        prepared, note = _prepare_client_call(call)
        assert prepared["input"]["state"]["sql"] == "select from where"
        assert note is not None and "could not be read" in note

    @pytest.mark.parametrize(
        "call",
        [
            {
                "id": "t",
                "name": "navigate",
                "input": {"route": "/graph", "state": {"query": "MATCH (n) RETURN n"}},
            },
            {"id": "t", "name": "navigate", "input": {"route": "/sources"}},
            {"id": "t", "name": "refresh_mv", "input": {"mv_id": "x"}},
        ],
    )
    def test_other_client_calls_are_untouched(self, call):
        from provisa.api.mcp.chat import _prepare_client_call

        assert _prepare_client_call(call) == (call, None)

    async def test_run_sql_runs_the_quoted_sql(self, monkeypatch):
        from provisa.api.mcp import chat

        seen = {}

        async def _run_sql(state, role, sql, limit=None, offset=0):
            seen["sql"] = sql
            return {"columns": [], "rows": [], "row_count": 0}

        monkeypatch.setattr(chat.mcp_tools, "run_sql", _run_sql)
        result = await chat._execute_tool(
            object(), "analyst", "run_sql", {"sql": "select order from sales"}
        )
        assert seen["sql"] == 'SELECT "order" FROM "sales"'
        assert "sql_note" not in result

    async def test_run_sql_runs_unparsable_sql_as_written_and_says_why(self, monkeypatch):
        from provisa.api.mcp import chat

        seen = {}

        async def _run_sql(state, role, sql, limit=None, offset=0):
            seen["sql"] = sql
            return {"columns": [], "rows": [], "row_count": 0}

        monkeypatch.setattr(chat.mcp_tools, "run_sql", _run_sql)
        result = await chat._execute_tool(
            object(), "analyst", "run_sql", {"sql": "select from where"}
        )
        assert seen["sql"] == "select from where"
        assert "could not be read" in result["sql_note"]
