# Copyright (c) 2026 Kenneth Stott
# Canary: c762a7b2-a056-4dd6-940b-a7dcd275d1c9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2115: Polly's navigate tool carries the steps she takes on the screen it opens (fill,
choose, press, show), each naming its element by test id or by role and name, run in the user's
browser; asked to show, demo or teach, she uses them, and the admin API otherwise."""

from provisa.api.mcp import chat


def _navigate() -> dict:
    [tool] = [t for t in chat._CLIENT_TOOLS if t["name"] == "navigate"]
    return tool


def test_navigate_carries_the_steps_taken_on_its_screen():
    schema = _navigate()["input_schema"]
    assert schema["required"] == ["route"]
    step = schema["properties"]["actions"]["items"]
    assert step["properties"]["do"]["enum"] == ["fill", "choose", "press", "show"]
    assert set(step["properties"]["target"]["properties"]) == {"testId", "role", "name"}
    assert step["required"] == ["do", "target"]


def test_the_steps_run_in_the_browser_never_on_the_server():
    assert "navigate" in chat._CLIENT_TOOL_NAMES


def test_she_shows_on_the_screen_only_when_asked_to_show_demo_or_teach():
    prompt = chat._SYSTEM
    assert "Asked to show, demo or teach something" in prompt
    assert "never by the admin API" in prompt
    assert "Any other request to make a change you make through the admin API" in prompt
    # Creating a source on screen, the picker's whole list is seen before one is chosen.
    assert "show source-type-picker-body" in prompt
