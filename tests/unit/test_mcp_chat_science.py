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


def test_the_navigate_tool_names_the_python_area_and_says_its_result_comes_back():
    tool = _navigate()
    described = _flat(tool["description"])
    assert "/science" in described
    assert "provisa_client.query" in described
    assert "read the result before you answer" in described
    state = _flat(tool["input_schema"]["properties"]["state"]["description"])
    assert '/science -> {"code": "<Python>"}' in state


def test_the_system_prompt_lists_the_python_area_with_its_state_key():
    s = _flat(chat_mod._SYSTEM)
    assert "Python /science (state.code; its run's output, value and error come back" in s
