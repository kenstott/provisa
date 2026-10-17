# Copyright (c) 2026 Kenneth Stott
# Canary: c990720c-5cf5-4ac0-90cd-fadfa23cdb9c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly is told about the Data Science area's Python: its route, its navigate state key, and that
the navigate result carries the run's output, value and error."""

# Requirements: REQ-2071

from __future__ import annotations

from provisa.api.mcp import chat as chat_mod


def _flat(text: str) -> str:
    return " ".join(text.split())


def _navigate() -> dict:
    [tool] = [t for t in chat_mod._CLIENT_TOOLS if t["name"] == "navigate"]
    return tool


def test_the_navigate_tool_names_both_tabs_and_says_their_results_come_back():
    tool = _navigate()
    described = _flat(tool["description"])
    assert "Explore's Python tab (/python) and Notebook tab (/notebook)" in described
    assert "provisa_client.query" in described
    assert "the Python tab (state.code: one script" in described
    assert "the Notebook tab (state.cells: a list of cells" in described
    assert (
        "Use the tab the user names; only when they name neither, use the Python tab." in described
    )
    assert "Never call the Python tab a notebook." in described
    assert "starts with `import provisa_client` and `provisa_client.sign_in()`" in described
    assert (
        "a cell using %%sql, %%graphql or %%cypher needs `%load_ext provisa_client` run first"
        in described
    )
    assert "read the result before you answer" in described
    state = _flat(tool["input_schema"]["properties"]["state"]["description"])
    assert '/python -> {"code": "<Python script>"} (the Python tab)' in state
    assert '/notebook -> {"cells": ["<cell>", ...]} (the Notebook tab)' in state


def test_the_system_prompt_names_both_tabs_and_how_to_choose():
    s = _flat(chat_mod._SYSTEM)
    assert "the Python tab /python (state.code) and the Notebook tab /notebook (state.cells)" in s
    assert "never call the Python tab a notebook" in s
