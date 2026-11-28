# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8b1c37-2a9d-4f60-b7e5-93d0c6a1f842
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly uploads code it drafted as a code source's code (REQ-1984, REQ-2114, REQ-1838).

A maintainer on /admin/reports had Polly draft a Python code source (orders with a region
parameter) and answered "yes. upload it."; the turn ended "(The model returned no answer — please
ask again.)" with every tool call green, and the server log held only the request line. Here the
turn is run with a scripted model: its rounds and tool calls are logged, a blank ending states the
model's stop reason, and the drafted code goes to the code-uploads route as an archive the user
confirms first.
"""

# Requirements: REQ-1984, REQ-2114, REQ-1838

from __future__ import annotations

import base64
import io
import logging
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from provisa.api.mcp import chat
from provisa.api.mcp import code_upload
from provisa.api.mcp import tools as mcp_tools
from provisa.codesource import bundle
from tests.unit.test_mcp_chat import _block, _install_fake_anthropic, _state

pytestmark = pytest.mark.asyncio

_SOURCE = "def orders(region: str):\n    return [{'id': 1, 'region': region}]\n"
_FILES = {"orders_source.py": _SOURCE}
_SUMMARY = "Upload the orders code source, which takes a region parameter."


@pytest.fixture(autouse=True)
def _a_configured_vendor_key(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")


def _request(org_id: str | None = "acme"):
    return SimpleNamespace(state=SimpleNamespace(active_org_id=org_id))


def _call(name: str, call_id: str, **input_):
    return SimpleNamespace(
        content=[_block(type="tool_use", name=name, input=input_, id=call_id)],
        stop_reason="tool_use",
    )


_BLANK = SimpleNamespace(content=[_block(type="text", text="")], stop_reason="end_turn")


async def _turn(question: str, request=None) -> list[dict]:
    return [
        ev
        async for ev in chat.run_chat(
            _state(), "analyst", [{"role": "user", "content": question}], request=request
        )
    ]


async def test_a_blank_ending_with_every_tool_green_states_the_stop_reason(monkeypatch, caplog):
    """The maintainer's turn, scripted: a tool call that succeeds, then a blank final round. The
    user is told the model's stop reason, and the log tells each round, each tool result, the
    stop reason and the vendor and model."""
    _install_fake_anthropic(monkeypatch, [_call("list_schemas", "t1"), _BLANK])

    async def _schemas(*_a, **_k):
        return []

    monkeypatch.setattr(mcp_tools, "list_schemas", _schemas)
    with caplog.at_level(logging.INFO, logger=chat.__name__):
        events = await _turn("yes. upload it.")
    [text] = [e["text"] for e in events if e["type"] == "text"]
    assert text == (
        "\n\n(The model returned no answer: it stopped with stop reason 'end_turn', and no tool "
        "call failed — please ask again.)"
    )
    assert not [e for e in events if e["type"] == "round"]  # logged, never sent to the browser
    logged = [r.getMessage() for r in caplog.records if r.name == chat.__name__]
    model = await chat._resolve_model(_state())
    assert logged == [
        f"Polly round 1: vendor=anthropic model={model} stop_reason=tool_use "
        "tool_calls=['list_schemas']",
        f"Polly round 2: vendor=anthropic model={model} stop_reason=end_turn tool_calls=[]",
        f"Polly turn: vendor=anthropic model={model} role=analyst rounds=2 stop_reason=end_turn "
        "tools=['list_schemas ok'] ending=text: " + repr(text.strip()[:160]),
    ]


async def test_a_failed_tool_is_logged_as_an_error(monkeypatch, caplog):
    _install_fake_anthropic(
        monkeypatch,
        [
            _call("list_tables", "t1", schema="ghost"),
            SimpleNamespace(content=[_block(type="text", text="None.")], stop_reason="end_turn"),
        ],
    )

    async def _tables(state, role, schema):
        raise ValueError(f"Unknown schema {schema!r}")

    monkeypatch.setattr(mcp_tools, "list_tables", _tables)
    with caplog.at_level(logging.INFO, logger=chat.__name__):
        await _turn("q")
    [summary] = [r.getMessage() for r in caplog.records if "Polly turn:" in r.getMessage()]
    assert "tools=['list_tables error']" in summary
    assert summary.endswith("ending=text: 'None.'")


async def test_upload_it_hands_the_browser_the_drafted_code_to_confirm(monkeypatch, caplog):
    """'yes. upload it.': the model uploads the code it drafted with upload_code. The browser is
    handed the code-uploads route's admin_request for the active organization, with the files for
    the user to read and confirm and their zip archive as the body; nothing is sent here."""
    _install_fake_anthropic(
        monkeypatch, [_call("upload_code", "u1", files=_FILES, summary=_SUMMARY)]
    )
    with caplog.at_level(logging.INFO, logger=chat.__name__):
        events = await _turn("yes. upload it.", request=_request("acme"))
    [pause] = [e for e in events if e["type"] == "awaiting_client_tools"]
    [handed] = pause["pending"]
    assert handed["id"] == "u1" and handed["name"] == "admin_request"
    body = handed["input"]
    assert body["method"] == "POST"
    assert body["path"] == "/admin/orgs/acme/code-uploads/archive?filename=code.zip"
    assert body["summary"] == _SUMMARY
    assert body["files"] == _FILES
    with zipfile.ZipFile(io.BytesIO(base64.b64decode(body["archive"]))) as zipped:
        assert {n: zipped.read(n).decode() for n in zipped.namelist()} == _FILES
    [summary] = [r.getMessage() for r in caplog.records if "Polly turn:" in r.getMessage()]
    assert summary.endswith("ending=awaiting the browser for ['admin_request']")


async def test_the_archive_is_one_the_upload_route_keeps(tmp_path: Path):
    """The route unpacks what it is sent as a code archive; the same files make the same
    archive, so the same content hash."""
    data = code_upload.archive(_FILES)
    root = bundle.unpack(code_upload.ARCHIVE_NAME, data, tmp_path / "a")
    assert (root / "orders_source.py").read_text() == _SOURCE
    assert code_upload.archive(dict(_FILES)) == data


@pytest.mark.parametrize(
    ("org_id", "files", "summary", "why"),
    [
        (None, _FILES, _SUMMARY, "Not proposed: no organization is active, so there is nowhere "
         "to keep the code"),
        ("acme", {}, _SUMMARY, "Not proposed: 'files' must map at least one relative path to its "
         "text"),
        ("acme", {"../x.py": "x"}, _SUMMARY, "Not proposed: '../x.py' is not a relative path "
         "inside the code"),
        ("acme", {"/x.py": "x"}, _SUMMARY, "Not proposed: '/x.py' is not a relative path inside "
         "the code"),
        ("acme", {"x.py": 1}, _SUMMARY, "Not proposed: 'x.py': a file's content must be its text"),
        ("acme", _FILES, " ", "Not proposed: 'summary' must say what this code is"),
    ],
)  # fmt: skip
async def test_an_upload_that_cannot_be_kept_is_refused_to_the_model(
    monkeypatch, org_id, files, summary, why
):
    """An upload with nowhere to go or no usable files is refused by name to the model, which
    corrects it; nothing is put to the user."""
    _install_fake_anthropic(
        monkeypatch,
        [
            _call("upload_code", "u1", files=files, summary=summary),
            SimpleNamespace(content=[_block(type="text", text="Fixed.")], stop_reason="end_turn"),
        ],
    )
    holder_events = await _turn("upload it", request=_request(org_id))
    assert not [e for e in holder_events if e["type"] == "awaiting_client_tools"]
    assert [(e["name"], e["is_error"]) for e in holder_events if e["type"] == "tool_result"] == [
        ("upload_code", True)
    ]
    assert code_upload.refusal({"files": files, "summary": summary}, org_id) == why


async def test_the_prompt_sends_drafted_code_through_upload_code():
    assert "Code you wrote for a code source is uploaded with upload_code" in chat._SYSTEM
    assert "upload_code" in chat._CLIENT_TOOL_NAMES


async def test_polly_is_told_a_read_only_function_is_a_query_not_a_command():
    """Maintainer, 2026-11-28: Polly wrote a sample command whose function only computed and
    returned rows, tagged @command (a mutation). The rule for each tag, with an example of each,
    reaches the model in the prompt and in upload_code's own description."""
    rule = code_upload.CODE_TAGS_RULE
    for tag, meaning in (
        ("@table", "a catalog table that returns rows"),
        ("@query", "a command of kind query"),
        ("@command", "a command of kind mutation"),
    ):
        assert f"{tag} -- {meaning}" in rule
        assert f"with {tag}." in rule  # its one-line example
    assert "only computes or reads and returns rows is @query, never @command" in rule
    assert "a code resolver is a @query with exactly two table parameters" in rule.lower()
    assert rule in chat._SYSTEM
    (upload,) = [t for t in chat._CLIENT_TOOLS if t["name"] == code_upload.UPLOAD_CODE]
    assert rule in upload["description"]
