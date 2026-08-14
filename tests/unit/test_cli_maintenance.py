# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7d81b0-4e26-4a95-9f1d-62b0a7e35c48
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""`provisa maintenance on|off|status` (REQ-1466).

The command exists so a deploy script can raise the banner before work that replaces the engine
cluster (REQ-1465) and clear it after, without a browser. It is a thin client for
PUT/GET /admin/platform/maintenance — the server owns the wording, the ``started_at`` stamp and the
``platform_settings`` gate — so these tests pin the request it sends and what it prints, with the
HTTP layer stubbed.
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
        seen["body"] = json.loads(bytes(req.data)) if req.data else None  # type: ignore[arg-type]
        return _Resp(payload)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return seen


_ON = {
    "active": True,
    "message": "Provisa is undergoing scheduled maintenance.",
    "ends_at": None,
    "started_at": "2026-08-14T10:00:00+00:00",
}
_OFF = {"active": False, "message": None, "ends_at": None, "started_at": None}


def test_on_puts_an_active_notice(monkeypatch, capsys):
    seen = _capture(monkeypatch, _ON)
    assert cli.main(["maintenance", "on", "--api", "http://127.0.0.1:8000"]) == 0
    assert seen["url"] == "http://127.0.0.1:8000/admin/platform/maintenance"
    assert seen["method"] == "PUT"
    # No message means the server's standard wording — the CLI must not compose its own, or the
    # deployment says two different things depending on which surface raised the banner.
    assert seen["body"] == {"active": True, "message": None, "ends_at": None}
    out = capsys.readouterr().out
    assert "Maintenance notice: ON" in out
    assert "no estimate given" in out


def test_on_forwards_the_operators_wording_and_end_time(monkeypatch):
    seen = _capture(monkeypatch, _ON)
    rc = cli.main(
        [
            "maintenance",
            "on",
            "--api",
            "http://x",
            "--message",
            "Swapping the engine cluster.",
            "--ends-at",
            "2026-08-14T22:30:00Z",
        ]
    )
    assert rc == 0
    assert seen["body"] == {
        "active": True,
        "message": "Swapping the engine cluster.",
        "ends_at": "2026-08-14T22:30:00Z",
    }


def test_off_clears_rather_than_blanks(monkeypatch, capsys):
    seen = _capture(monkeypatch, _OFF)
    assert cli.main(["maintenance", "off", "--api", "http://x"]) == 0
    assert seen["body"] == {"active": False, "message": None, "ends_at": None}
    assert "Maintenance notice: OFF" in capsys.readouterr().out


def test_status_reads_without_writing(monkeypatch, capsys):
    seen = _capture(monkeypatch, _ON)
    assert cli.main(["maintenance", "status", "--api", "http://x"]) == 0
    assert seen["method"] == "GET"
    assert seen["body"] is None
    assert "Provisa is undergoing scheduled maintenance." in capsys.readouterr().out


def test_env_supplies_api_and_token(monkeypatch):
    monkeypatch.setenv("PROVISA_API_URL", "http://envhost:8000/")
    monkeypatch.setenv("PROVISA_API_TOKEN", "envtok")
    seen = _capture(monkeypatch, _ON)
    assert cli.main(["maintenance", "status"]) == 0
    assert seen["url"] == "http://envhost:8000/admin/platform/maintenance"
    assert seen["auth"] == "Bearer envtok"


def test_a_rejected_write_fails_loudly(monkeypatch):
    def raise_403(req, timeout):
        raise urllib.error.HTTPError(
            req.full_url,
            403,
            "Forbidden",
            None,
            io.BytesIO(b"platform_settings required"),  # type: ignore[arg-type]
        )

    monkeypatch.setattr(urllib.request, "urlopen", raise_403)
    # A silent failure here leaves an operator believing the banner is up while the cluster is
    # being replaced under users who were never told.
    with pytest.raises(SystemExit, match="HTTP 403"):
        cli.main(["maintenance", "on", "--api", "http://x"])


def test_unreachable_api_fails_loudly(monkeypatch):
    def raise_conn(req, timeout):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", raise_conn)
    with pytest.raises(SystemExit, match="cannot reach the Provisa API"):
        cli.main(["maintenance", "off", "--api", "http://down:1"])


@pytest.mark.parametrize("argv", [["maintenance"], ["maintenance", "unknown"]])
def test_maintenance_requires_a_known_subcommand(argv):
    with pytest.raises(SystemExit):
        cli.main(argv)
