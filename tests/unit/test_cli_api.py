# Copyright (c) 2026 Kenneth Stott
# Canary: 03871e8a-0878-4f15-86b2-029cf397e7e4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""`provisa api METHOD PATH` (REQ-2222): the generic call to a running instance's API.

The HTTP layer is stubbed; these pin the request sent, what reaches stdout and the exit code.
"""

from __future__ import annotations

import email.message
import io
import json
import urllib.error
import urllib.request

import pytest

from provisa import cli

_TOKEN = "unit-test-placeholder"  # nosec B105


class _Resp:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def _stub(monkeypatch, answer) -> dict:
    seen: dict = {}

    def fake_urlopen(req: urllib.request.Request, timeout: int) -> _Resp:
        seen["url"] = req.full_url
        seen["method"] = req.get_method()
        seen["auth"] = req.get_header("Authorization")
        seen["content_type"] = req.get_header("Content-type")
        seen["timeout"] = timeout
        seen["data"] = req.data
        if isinstance(answer, Exception):
            raise answer
        return _Resp(answer)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return seen


def _refusal(status: int, body: bytes) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "http://x", status, "refused", email.message.Message(), io.BytesIO(body)
    )


@pytest.fixture(autouse=True)
def _no_ambient_instance(monkeypatch):
    monkeypatch.delenv("PROVISA_API_URL", raising=False)
    monkeypatch.delenv("PROVISA_API_TOKEN", raising=False)


@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE"])
def test_each_method_is_sent_to_the_path(monkeypatch, capsys, method):
    seen = _stub(monkeypatch, b'{"ok": true}')
    assert cli.main(["api", method, "/admin/things?limit=1", "--api", "http://host:9/"]) == 0
    assert seen["method"] == method
    assert seen["url"] == "http://host:9/admin/things?limit=1"
    assert seen["data"] is None
    assert seen["content_type"] is None
    assert capsys.readouterr().out == '{"ok": true}'


def test_a_method_outside_the_set_is_a_usage_error(monkeypatch, capsys):
    seen = _stub(monkeypatch, b"")
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["api", "TRACE", "/x"])
    assert exit_info.value.code == 2
    assert seen == {}
    capsys.readouterr()


def test_a_path_without_its_slash_stays_on_the_instance(monkeypatch, capsys):
    seen = _stub(monkeypatch, b"{}")
    assert cli.main(["api", "GET", "@elsewhere.example/x", "--api", "http://host:9"]) == 0
    assert seen["url"] == "http://host:9/@elsewhere.example/x"
    capsys.readouterr()


def test_literal_body_is_sent_as_json(monkeypatch, capsys):
    seen = _stub(monkeypatch, b"{}")
    assert cli.main(["api", "POST", "/x", "--data", '{"a":  1}']) == 0
    assert seen["data"] == b'{"a":  1}'
    assert seen["content_type"] == "application/json"
    capsys.readouterr()


def test_body_from_a_file(monkeypatch, capsys, tmp_path):
    seen = _stub(monkeypatch, b"{}")
    payload = tmp_path / "body.json"
    payload.write_bytes(b'{"from": "file"}')
    assert cli.main(["api", "PUT", "/x", "--data", f"@{payload}"]) == 0
    assert seen["data"] == b'{"from": "file"}'
    capsys.readouterr()


def test_body_from_stdin(monkeypatch, capsys):
    seen = _stub(monkeypatch, b"{}")

    class _Stdin:
        buffer = io.BytesIO(b'{"from": "stdin"}')

    monkeypatch.setattr("sys.stdin", _Stdin)
    assert cli.main(["api", "POST", "/x", "--data", "@-"]) == 0
    assert seen["data"] == b'{"from": "stdin"}'
    capsys.readouterr()


def test_an_unreadable_body_file_is_a_usage_error(monkeypatch, capsys, tmp_path):
    seen = _stub(monkeypatch, b"{}")
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["api", "POST", "/x", "--data", f"@{tmp_path / 'absent.json'}"])
    assert exit_info.value.code == 2
    assert seen == {}
    assert "absent.json" in capsys.readouterr().err


def test_token_flag_is_sent_as_bearer_and_never_printed(monkeypatch, capsys):
    seen = _stub(monkeypatch, b"{}")
    assert cli.main(["api", "GET", "/x", "--token", _TOKEN]) == 0
    assert seen["auth"] == f"Bearer {_TOKEN}"
    captured = capsys.readouterr()
    assert _TOKEN not in captured.out + captured.err


def test_instance_and_token_come_from_the_environment(monkeypatch, capsys):
    monkeypatch.setenv("PROVISA_API_URL", "http://from-env:7")
    monkeypatch.setenv("PROVISA_API_TOKEN", _TOKEN)
    seen = _stub(monkeypatch, b"{}")
    assert cli.main(["api", "GET", "/x"]) == 0
    assert seen["url"] == "http://from-env:7/x"
    assert seen["auth"] == f"Bearer {_TOKEN}"
    capsys.readouterr()


def test_no_token_sends_no_auth_header(monkeypatch, capsys):
    seen = _stub(monkeypatch, b"{}")
    assert cli.main(["api", "GET", "/x"]) == 0
    assert seen["auth"] is None
    assert seen["url"] == "http://127.0.0.1:8000/x"
    capsys.readouterr()


def test_timeout_reaches_the_call(monkeypatch, capsys):
    seen = _stub(monkeypatch, b"{}")
    assert cli.main(["api", "GET", "/x", "--timeout", "5"]) == 0
    assert seen["timeout"] == 5
    capsys.readouterr()


@pytest.mark.parametrize("body", [b"", b"plain text\n", b'{"b":1,  "a":2}'])
def test_success_prints_the_body_unchanged(monkeypatch, capsys, body):
    _stub(monkeypatch, body)
    assert cli.main(["api", "GET", "/x"]) == 0
    assert capsys.readouterr().out == body.decode()


@pytest.mark.parametrize("status", [400, 403, 404, 500])
def test_a_refused_call_exits_one_with_its_body_on_stdout(monkeypatch, capsys, status):
    refusal = {"code": "auth.denied", "detail": "no", "params": {}}
    _stub(monkeypatch, _refusal(status, json.dumps(refusal).encode()))
    assert cli.main(["api", "DELETE", "/x"]) == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out)["code"] == "auth.denied"
    assert captured.err.strip() == f"HTTP {status}"


def test_an_unreachable_instance_exits_non_zero(monkeypatch, capsys):
    _stub(monkeypatch, urllib.error.URLError("connection refused"))
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["api", "GET", "/x", "--api", "http://host:9"])
    assert (
        exit_info.value.code == "cannot reach the Provisa API at http://host:9: connection refused"
    )
    assert capsys.readouterr().out == ""


def test_a_timeout_exits_non_zero(monkeypatch, capsys):
    _stub(monkeypatch, TimeoutError())
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["api", "GET", "/x", "--api", "http://host:9", "--timeout", "5"])
    assert exit_info.value.code == "no answer from the Provisa API at http://host:9 within 5s"
    assert capsys.readouterr().out == ""
