# Copyright (c) 2026 Kenneth Stott
# Canary: 9f4e2a61-7c8b-4d3f-9a05-1e6b8c247d90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""`provisa metadata export` CLI job (REQ-1072, REQ-1074).

The command is a thin client for POST /admin/metadata-export/publish — the server's single
publish path — so these tests pin the request it sends and how it renders/exits on each
outcome, with the HTTP layer stubbed.
"""

from __future__ import annotations

import io
import json
import urllib.error
import urllib.request

import pytest

from provisa import cli


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def _capture(monkeypatch, payload: dict) -> dict:
    seen: dict = {}

    def fake_urlopen(req: urllib.request.Request, timeout: int) -> _Resp:
        seen["url"] = req.full_url
        seen["method"] = req.get_method()
        seen["auth"] = req.get_header("Authorization")
        seen["timeout"] = timeout
        return _Resp(payload)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return seen


_OK = {"ok": True, "provider": "datahub", "total_published": 12, "errors": []}


def test_posts_to_the_single_publish_path(monkeypatch, capsys):
    seen = _capture(monkeypatch, _OK)
    rc = cli.main(["metadata", "export", "--api", "http://127.0.0.1:8000"])
    assert rc == 0
    assert seen["url"] == "http://127.0.0.1:8000/admin/metadata-export/publish"
    assert seen["method"] == "POST"
    assert seen["auth"] is None
    assert seen["timeout"] == 300
    assert "published 12 assets via datahub" in capsys.readouterr().out


def test_token_flag_becomes_bearer_header(monkeypatch):
    seen = _capture(monkeypatch, _OK)
    rc = cli.main(["metadata", "export", "--api", "http://x", "--token", "t0k", "--timeout", "9"])
    assert rc == 0
    assert seen["auth"] == "Bearer t0k"
    assert seen["timeout"] == 9


def test_env_supplies_api_and_token(monkeypatch):
    monkeypatch.setenv("PROVISA_API_URL", "http://envhost:8000/")
    monkeypatch.setenv("PROVISA_API_TOKEN", "envtok")
    seen = _capture(monkeypatch, _OK)
    assert cli.main(["metadata", "export"]) == 0
    assert seen["url"] == "http://envhost:8000/admin/metadata-export/publish"
    assert seen["auth"] == "Bearer envtok"


def test_partial_publish_exits_nonzero_and_names_assets(monkeypatch, capsys):
    _capture(
        monkeypatch,
        {
            "ok": False,
            "provider": "atlas",
            "total_published": 3,
            "errors": [{"asset": "sales.orders", "message": "rejected"}],
        },
    )
    rc = cli.main(["metadata", "export", "--api", "http://x"])
    captured = capsys.readouterr()
    assert rc == 1
    assert "PARTIAL" in captured.out
    assert "sales.orders: rejected" in captured.err


def test_http_error_reports_body_and_exits_nonzero(monkeypatch, capsys):
    def raise_403(req, timeout):
        raise urllib.error.HTTPError(
            req.full_url,
            403,
            "Forbidden",
            None,
            io.BytesIO(b"not entitled"),  # type: ignore[arg-type]
        )

    monkeypatch.setattr(urllib.request, "urlopen", raise_403)
    rc = cli.main(["metadata", "export", "--api", "http://x"])
    assert rc == 1
    assert "HTTP 403" in capsys.readouterr().err


def test_unreachable_api_exits_nonzero(monkeypatch, capsys):
    def raise_conn(req, timeout):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", raise_conn)
    rc = cli.main(["metadata", "export", "--api", "http://down:1"])
    assert rc == 1
    assert "cannot reach the Provisa API" in capsys.readouterr().err


def test_unauthenticated_deployment_sends_no_auth_header(monkeypatch):
    monkeypatch.delenv("PROVISA_API_TOKEN", raising=False)
    seen = _capture(monkeypatch, _OK)
    assert cli.main(["metadata", "export", "--api", "http://x"]) == 0
    assert seen["auth"] is None


@pytest.mark.parametrize("argv", [["metadata"], ["metadata", "unknown"]])
def test_metadata_requires_a_known_subcommand(argv):
    with pytest.raises(SystemExit):
        cli.main(argv)
