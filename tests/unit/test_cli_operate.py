# Copyright (c) 2026 Kenneth Stott
# Canary: 3f1c8a52-7b0e-4d19-a6c4-90e2d7b1c3f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What the other CLIs taught (REQ-2222): a saved plan, the drift exit, can-i, contexts, one output
option and filter, wait, schema, completion and the documented exit codes.

The HTTP layer is stubbed; these pin the request sent, what is printed and the exit code.
"""

# Requirements: REQ-2222

from __future__ import annotations

import copy
import email.message
import io
import json
import stat
import subprocess  # nosec B404
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest
import yaml

from provisa import cli, cli_operate

_REFERENCE = "${env:J1056_UNSET_PASSWORD}"
_SECRET = "unit-test-placeholder"  # nosec B105

_CONFIG: dict[str, Any] = {
    "sources": [
        {
            "id": "pg",
            "type": "postgresql",
            "host": "db",
            "port": 5432,
            "database": "d",
            "username": "u",
            "password": _REFERENCE,
        }
    ],
    "domains": [{"id": "sales"}],
    "tables": [
        {
            "source_id": "pg",
            "domain_id": "sales",
            "schema": "public",
            "table": "orders",
            "columns": [{"name": "id", "visible_to": ["analyst"]}],
        }
    ],
    "roles": [
        {"id": "analyst", "description": "reads", "capabilities": [], "domain_access": ["sales"]}
    ],
}

_APPLIED = json.dumps({"summary": {"domains": 2}}).encode()


class _Resp:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self._body = body
        self.status = status
        self.headers = email.message.Message()

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def _http_error(status: int, body: bytes) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "http://x", status, "no", email.message.Message(), io.BytesIO(body)
    )


def _refusal(status: int, code: str) -> urllib.error.HTTPError:
    return _http_error(status, json.dumps({"code": code, "detail": "no", "params": {}}).encode())


class _Instance:
    """Answers each call from a queue, repeating the last; records every request."""

    def __init__(self, monkeypatch, *answers: Any) -> None:
        self.answers = list(answers)
        self.calls: list[urllib.request.Request] = []
        monkeypatch.setattr(urllib.request, "urlopen", self._open)

    def _open(self, req: urllib.request.Request, timeout: int) -> _Resp:
        self.calls.append(req)
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, Exception):
            raise answer
        return _Resp(answer)

    @property
    def methods(self) -> list[str]:
        return [c.get_method() for c in self.calls]


def _write(tmp_path: Path, config: dict[str, Any], name: str = "c.yaml") -> str:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    return str(path)


def _config(**changes: Any) -> dict[str, Any]:
    return {**copy.deepcopy(_CONFIG), **changes}


def _exported(config: dict[str, Any]) -> bytes:
    return yaml.safe_dump(config).encode()


@pytest.fixture(autouse=True)
def _isolated(monkeypatch, tmp_path):
    monkeypatch.delenv("PROVISA_API_URL", raising=False)
    monkeypatch.delenv("PROVISA_API_TOKEN", raising=False)
    monkeypatch.delenv("J1056_UNSET_PASSWORD", raising=False)
    monkeypatch.setenv("PROVISA_CONTEXTS", str(tmp_path / "contexts.yaml"))


# --- the drift exit -------------------------------------------------------------------------- #


def test_diff_exit_code_is_zero_when_the_instance_matches(monkeypatch, capsys, tmp_path):
    _Instance(monkeypatch, _exported(_config()))
    args = ["diff", _write(tmp_path, _config()), "--org", "acme", "--exit-code"]
    assert cli.main(args) == 0
    capsys.readouterr()


def test_diff_exit_code_reports_a_change_as_drift(monkeypatch, capsys, tmp_path):
    _Instance(monkeypatch, _exported(_config(domains=[{"id": "sales"}, {"id": "old"}])))
    ours = _config(domains=[{"id": "sales"}, {"id": "new"}])
    assert cli.main(["diff", _write(tmp_path, ours), "--org", "a", "--exit-code"]) == 3
    capsys.readouterr()


def test_diff_exit_code_counts_what_only_the_instance_has(monkeypatch, capsys, tmp_path):
    """An apply would leave it, but the instance has still moved from the config."""
    _Instance(monkeypatch, _exported(_config(domains=[{"id": "sales"}, {"id": "ops_local"}])))
    assert cli.main(["diff", _write(tmp_path, _config()), "--org", "a", "--exit-code"]) == 3
    capsys.readouterr()


def test_diff_without_the_flag_still_exits_zero_on_a_change(monkeypatch, capsys, tmp_path):
    _Instance(monkeypatch, _exported(_config()))
    ours = _config(domains=[{"id": "sales"}, {"id": "new"}])
    assert cli.main(["diff", _write(tmp_path, ours), "--org", "a"]) == 0
    capsys.readouterr()


# --- a saved plan ---------------------------------------------------------------------------- #


def _plan(monkeypatch, capsys, tmp_path, ours: dict, held: dict, org: str = "acme") -> str:
    _Instance(monkeypatch, _exported(held))
    plan = str(tmp_path / "plan.json")
    assert cli.main(["diff", _write(tmp_path, ours), "--org", org, "--out", plan]) == 0
    capsys.readouterr()
    return plan


def test_a_plan_keeps_the_secret_a_reference(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("J1056_UNSET_PASSWORD", _SECRET)
    plan = _plan(monkeypatch, capsys, tmp_path, _config(), _config())
    text = Path(plan).read_text()
    assert _REFERENCE in text
    assert _SECRET not in text


def test_apply_of_a_plan_sends_exactly_what_was_diffed(monkeypatch, capsys, tmp_path):
    ours = _config(domains=[{"id": "sales"}, {"id": "new"}])
    source = tmp_path / "c.yaml"
    plan = _plan(monkeypatch, capsys, tmp_path, ours, _config())
    source.write_text(yaml.safe_dump(_config(domains=[{"id": "edited-after-the-plan"}])))

    instance = _Instance(monkeypatch, _exported(_config()), _APPLIED)
    assert cli.main(["apply", "--plan", plan, "--org", "acme"]) == 0
    assert instance.methods == ["GET", "POST"]
    sent = yaml.safe_load(json.loads(instance.calls[1].data)["config_yaml"])
    assert sent == ours
    capsys.readouterr()


def test_apply_refuses_a_plan_when_the_instance_has_moved(monkeypatch, capsys, tmp_path):
    plan = _plan(monkeypatch, capsys, tmp_path, _config(), _config())
    moved = _config(domains=[{"id": "sales"}, {"id": "someone_else"}])
    instance = _Instance(monkeypatch, _exported(moved))
    assert cli.main(["apply", "--plan", plan, "--org", "acme"]) == 1
    assert capsys.readouterr().err.startswith("refused (plan.stale): ")
    assert instance.methods == ["GET"]


def test_apply_refuses_a_plan_made_for_another_org(monkeypatch, capsys, tmp_path):
    plan = _plan(monkeypatch, capsys, tmp_path, _config(), _config(), org="acme")
    instance = _Instance(monkeypatch, _exported(_config()))
    assert cli.main(["apply", "--plan", plan, "--org", "other"]) == 1
    assert capsys.readouterr().err.startswith("refused (plan.wrong_org): ")
    assert instance.calls == []


def test_apply_refuses_a_file_that_is_not_a_plan(monkeypatch, capsys, tmp_path):
    instance = _Instance(monkeypatch, b"")
    path = tmp_path / "plan.json"
    path.write_text("{}")
    assert cli.main(["apply", "--plan", str(path), "--org", "acme"]) == 1
    assert capsys.readouterr().err.startswith("refused (plan.invalid): ")
    assert instance.calls == []


def test_apply_refuses_an_unreadable_plan(monkeypatch, capsys, tmp_path):
    _Instance(monkeypatch, b"")
    assert cli.main(["apply", "--plan", str(tmp_path / "absent"), "--org", "acme"]) == 1
    assert capsys.readouterr().err.startswith("refused (plan.unreadable): ")


@pytest.mark.parametrize("extra", [[], ["c.yaml", "--plan", "p.json"]])
def test_apply_takes_a_file_or_a_plan_exactly(monkeypatch, capsys, extra):
    instance = _Instance(monkeypatch, b"")
    assert cli.main(["apply", *extra, "--org", "acme"]) == 2
    assert "either a file or --plan" in capsys.readouterr().err
    assert instance.calls == []


def test_diff_to_an_unwritable_plan_is_refused(monkeypatch, capsys, tmp_path):
    _Instance(monkeypatch, _exported(_config()))
    plan = str(tmp_path / "no-such-dir" / "plan.json")
    assert cli.main(["diff", _write(tmp_path, _config()), "--org", "a", "--out", plan]) == 1
    assert "refused (plan.unwritable)" in capsys.readouterr().err


# --- can-i ----------------------------------------------------------------------------------- #

_EFFECTIVE = {
    "role": "analyst",
    "capabilities": [{"right": "query_development", "from": ["analyst"]}],
    "data": [
        {
            "name": "sales.orders",
            "columns": [
                {"name": "id", "read": True, "write": [], "mask": None, "row_rule": None},
                {
                    "name": "email",
                    "read": True,
                    "write": ["update"],
                    "mask": {"type": "hash"},
                    "row_rule": "region = 'EU'",
                },
                {"name": "notes", "read": False, "write": ["insert"], "mask": None},
            ],
        }
    ],
}


def _can_i(monkeypatch, capsys, *words: str, answer: Any = None) -> tuple[int, Any, str]:
    _Instance(monkeypatch, json.dumps(_EFFECTIVE).encode() if answer is None else answer)
    code = cli.main(["can-i", *words, "--role", "analyst", "--org", "acme", "--json"])
    captured = capsys.readouterr()
    return code, json.loads(captured.out) if captured.out else None, captured.err


def test_can_i_asks_for_the_roles_effective_rights(monkeypatch, capsys):
    instance = _Instance(monkeypatch, json.dumps(_EFFECTIVE).encode())
    args = ["can-i", "read", "sales.orders", "--role", "a/b", "--org", "acme"]
    assert cli.main(args) == 0
    assert instance.calls[0].full_url.endswith("/admin/roles/a%2Fb/effective")
    assert instance.calls[0].get_header("X-org-provisa") == "acme"
    capsys.readouterr()


def test_can_i_holds_a_capability(monkeypatch, capsys):
    code, doc, _ = _can_i(monkeypatch, capsys, "capability", "query_development")
    assert code == 0
    assert doc["allowed"] is True
    assert doc["from"] == ["analyst"]


def test_can_i_says_why_a_capability_is_not_held(monkeypatch, capsys):
    code, doc, _ = _can_i(monkeypatch, capsys, "capability", "platform_settings")
    assert code == 1
    assert doc["allowed"] is False
    assert "does not hold platform_settings" in doc["reason"]


def test_can_i_read_a_column_reports_its_mask_and_row_rule(monkeypatch, capsys):
    code, doc, _ = _can_i(monkeypatch, capsys, "read", "sales.orders.email")
    assert code == 0
    assert doc["allowed"] is True
    assert doc["mask"] == {"type": "hash"}
    assert doc["row_rule"] == "region = 'EU'"


def test_can_i_read_a_table_is_any_readable_column(monkeypatch, capsys):
    code, doc, _ = _can_i(monkeypatch, capsys, "read", "sales.orders")
    assert code == 0
    assert doc["columns"] == ["id", "email"]


def test_can_i_says_why_a_column_cannot_be_read(monkeypatch, capsys):
    code, doc, _ = _can_i(monkeypatch, capsys, "read", "sales.orders.notes")
    assert code == 1
    assert "notes" in doc["reason"]


def test_can_i_says_why_a_table_is_out_of_reach(monkeypatch, capsys):
    code, doc, _ = _can_i(monkeypatch, capsys, "read", "hr.pay")
    assert code == 1
    assert "hr.pay" in doc["reason"]


def test_can_i_write_names_the_operations(monkeypatch, capsys):
    code, doc, _ = _can_i(monkeypatch, capsys, "write", "sales.orders.email")
    assert code == 0
    assert doc["operations"] == ["update"]


def test_can_i_says_why_a_column_cannot_be_written(monkeypatch, capsys):
    code, doc, _ = _can_i(monkeypatch, capsys, "write", "sales.orders.id")
    assert code == 1
    assert "id" in doc["reason"]


def test_can_i_as_text_says_yes_or_no_and_why(monkeypatch, capsys):
    _Instance(monkeypatch, json.dumps(_EFFECTIVE).encode())
    args = ["can-i", "capability", "platform_settings", "--role", "analyst", "--org", "acme"]
    assert cli.main(args) == 1
    out = capsys.readouterr().out
    assert out.startswith("no: ")
    assert "does not hold platform_settings" in out


def test_can_i_that_cannot_be_answered_is_not_a_no(monkeypatch, capsys):
    """A role the caller may not read is not a role that may not act."""
    code, _, err = _can_i(
        monkeypatch, capsys, "capability", "x", answer=_refusal(404, "roles.not_found")
    )
    assert code == 4
    assert "roles.not_found" in err


def test_can_i_as_a_person_is_refused_until_entitlements_apply(monkeypatch, capsys):
    instance = _Instance(monkeypatch, b"{}")
    args = ["can-i", "read", "sales.orders", "--person", "p", "--org", "acme"]
    assert cli.main(args) == 2
    assert "entitlements" in capsys.readouterr().err
    assert instance.calls == []


def test_can_i_needs_a_role_or_a_person(monkeypatch, capsys):
    instance = _Instance(monkeypatch, b"{}")
    assert cli.main(["can-i", "read", "sales.orders", "--org", "acme"]) == 2
    assert "--role" in capsys.readouterr().err
    assert instance.calls == []


# --- contexts -------------------------------------------------------------------------------- #


def _set_context(capsys, name: str, *flags: str) -> None:
    assert cli.main(["context", "set", name, "--api", f"http://{name}:1", *flags]) == 0
    capsys.readouterr()


def test_a_context_file_is_private_to_its_owner(capsys, tmp_path):
    _set_context(capsys, "dev", "--token", _SECRET)
    mode = stat.S_IMODE((tmp_path / "contexts.yaml").stat().st_mode)
    assert mode == 0o600


def test_context_list_never_prints_a_token(capsys):
    _set_context(capsys, "dev", "--token", _SECRET)
    assert cli.main(["context", "list", "--output", "json"]) == 0
    out = capsys.readouterr().out
    assert _SECRET not in out
    assert json.loads(out)["contexts"] == [
        {
            "name": "dev",
            "api": "http://dev:1",
            "has_token": True,
            "production": False,
            "current": True,
        }
    ]


def test_the_first_context_set_becomes_current(monkeypatch, capsys):
    _set_context(capsys, "dev", "--token", _SECRET)
    instance = _Instance(monkeypatch, json.dumps({"active": False}).encode())
    assert cli.main(["maintenance", "status"]) == 0
    assert instance.calls[0].full_url == "http://dev:1/admin/platform/maintenance"
    assert instance.calls[0].get_header("Authorization") == f"Bearer {_SECRET}"
    capsys.readouterr()


def test_use_switches_the_current_context(monkeypatch, capsys):
    _set_context(capsys, "dev")
    _set_context(capsys, "test")
    assert cli.main(["context", "use", "test"]) == 0
    instance = _Instance(monkeypatch, json.dumps({"active": False}).encode())
    assert cli.main(["maintenance", "status"]) == 0
    assert instance.calls[0].full_url.startswith("http://test:1/")
    capsys.readouterr()


def test_the_context_flag_names_one_for_this_call(monkeypatch, capsys):
    _set_context(capsys, "dev")
    _set_context(capsys, "test")
    instance = _Instance(monkeypatch, json.dumps({"active": False}).encode())
    assert cli.main(["maintenance", "status", "--context", "test"]) == 0
    assert instance.calls[0].full_url.startswith("http://test:1/")
    capsys.readouterr()


def test_the_api_flag_beats_a_context(monkeypatch, capsys):
    _set_context(capsys, "dev")
    instance = _Instance(monkeypatch, json.dumps({"active": False}).encode())
    assert cli.main(["maintenance", "status", "--api", "http://flag:2"]) == 0
    assert instance.calls[0].full_url.startswith("http://flag:2/")
    capsys.readouterr()


def test_the_environment_beats_the_current_context(monkeypatch, capsys):
    _set_context(capsys, "dev")
    monkeypatch.setenv("PROVISA_API_URL", "http://env:3")
    instance = _Instance(monkeypatch, json.dumps({"active": False}).encode())
    assert cli.main(["maintenance", "status"]) == 0
    assert instance.calls[0].full_url.startswith("http://env:3/")
    capsys.readouterr()


def test_a_named_context_beats_the_environment(monkeypatch, capsys):
    _set_context(capsys, "dev")
    monkeypatch.setenv("PROVISA_API_URL", "http://env:3")
    instance = _Instance(monkeypatch, json.dumps({"active": False}).encode())
    assert cli.main(["maintenance", "status", "--context", "dev"]) == 0
    assert instance.calls[0].full_url.startswith("http://dev:1/")
    capsys.readouterr()


def test_an_unknown_context_is_a_usage_error(monkeypatch, capsys):
    instance = _Instance(monkeypatch, b"{}")
    assert cli.main(["maintenance", "status", "--context", "nope"]) == 2
    assert "nope" in capsys.readouterr().err
    assert instance.calls == []


def test_use_of_an_unknown_context_is_a_usage_error(capsys):
    assert cli.main(["context", "use", "nope"]) == 2
    assert "nope" in capsys.readouterr().err


def test_delete_removes_a_context_and_clears_current(monkeypatch, capsys):
    _set_context(capsys, "dev")
    assert cli.main(["context", "delete", "dev"]) == 0
    capsys.readouterr()
    assert cli.main(["context", "list", "--output", "json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"contexts": []}


def test_setting_a_context_again_keeps_a_token_not_restated(monkeypatch, capsys):
    _set_context(capsys, "dev", "--token", _SECRET)
    _set_context(capsys, "dev", "--production")
    instance = _Instance(monkeypatch, json.dumps({"active": False}).encode())
    assert cli.main(["maintenance", "status"]) == 0
    assert instance.calls[0].get_header("Authorization") == f"Bearer {_SECRET}"
    capsys.readouterr()


# --- a production context asks --------------------------------------------------------------- #


def _not_a_terminal(monkeypatch) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO(""))


def test_production_refuses_a_change_with_nobody_to_ask(monkeypatch, capsys, tmp_path):
    _set_context(capsys, "prod", "--production")
    _not_a_terminal(monkeypatch)
    instance = _Instance(monkeypatch, _APPLIED)
    assert cli.main(["apply", _write(tmp_path, _config()), "--org", "acme"]) == 1
    assert "context.confirmation_required" in capsys.readouterr().err
    assert instance.calls == []


def test_production_proceeds_with_yes(monkeypatch, capsys, tmp_path):
    _set_context(capsys, "prod", "--production")
    _not_a_terminal(monkeypatch)
    instance = _Instance(monkeypatch, _APPLIED)
    assert cli.main(["apply", _write(tmp_path, _config()), "--org", "acme", "--yes"]) == 0
    assert instance.methods == ["POST"]
    capsys.readouterr()


def _typing(monkeypatch, answer: str) -> None:
    tty = io.StringIO(answer + "\n")
    tty.isatty = lambda: True  # type: ignore[method-assign]
    monkeypatch.setattr("sys.stdin", tty)


def test_production_goes_on_when_its_name_is_typed(monkeypatch, capsys, tmp_path):
    _set_context(capsys, "prod", "--production")
    _typing(monkeypatch, "prod")
    instance = _Instance(monkeypatch, _APPLIED)
    assert cli.main(["apply", _write(tmp_path, _config()), "--org", "acme"]) == 0
    assert instance.methods == ["POST"]
    capsys.readouterr()


def test_production_stops_on_any_other_answer(monkeypatch, capsys, tmp_path):
    _set_context(capsys, "prod", "--production")
    _typing(monkeypatch, "y")
    instance = _Instance(monkeypatch, _APPLIED)
    assert cli.main(["apply", _write(tmp_path, _config()), "--org", "acme"]) == 1
    assert "context.not_confirmed" in capsys.readouterr().err
    assert instance.calls == []


@pytest.mark.parametrize(
    "argv",
    [
        ["maintenance", "status"],
        ["api", "GET", "/x"],
        ["explain", "sql", "select 1", "--role", "r"],
        ["env", "list", "--org", "a"],
    ],
)
def test_production_does_not_ask_for_what_only_reads(monkeypatch, capsys, argv):
    _set_context(capsys, "prod", "--production")
    _not_a_terminal(monkeypatch)
    instance = _Instance(monkeypatch, b'{"active": false, "environments": []}')
    cli.main(argv)
    assert len(instance.calls) == 1
    capsys.readouterr()


@pytest.mark.parametrize(
    "argv",
    [
        ["maintenance", "on"],
        ["api", "POST", "/x"],
        ["api", "DELETE", "/x"],
        ["explain", "sql", "select 1", "--role", "r", "--analyze"],
        ["query", "sql", "delete from t", "--role", "r"],
        ["env", "delete", "--org", "a", "--env", "e"],
        ["promote", "--org", "a", "--from", "d", "--to", "t", "--message", "m"],
    ],
)
def test_production_asks_before_what_can_change_or_run(monkeypatch, capsys, argv):
    _set_context(capsys, "prod", "--production")
    _not_a_terminal(monkeypatch)
    instance = _Instance(monkeypatch, b"{}")
    assert cli.main(argv) == 1
    assert "context.confirmation_required" in capsys.readouterr().err
    assert instance.calls == []


def test_a_context_that_is_not_production_never_asks(monkeypatch, capsys, tmp_path):
    _set_context(capsys, "dev")
    _not_a_terminal(monkeypatch)
    instance = _Instance(monkeypatch, _APPLIED)
    assert cli.main(["apply", _write(tmp_path, _config()), "--org", "acme"]) == 0
    assert instance.methods == ["POST"]
    capsys.readouterr()


# --- one output option, one filter ----------------------------------------------------------- #

_LIST = json.dumps(
    {"items": [{"id": "a", "n": 1, "extra": {"k": [1]}}, {"id": "b", "n": 2}], "total": 2}
).encode()


def _shaped(monkeypatch, capsys, *flags: str, answer: bytes = _LIST) -> tuple[int, str]:
    _Instance(monkeypatch, answer)
    code = cli.main(["api", "GET", "/x", *flags])
    return code, capsys.readouterr().out


def test_output_json_is_stable(monkeypatch, capsys):
    code, out = _shaped(monkeypatch, capsys, "--output", "json", answer=b'{"b": 1, "a": 2}')
    assert code == 0
    assert out == '{"a": 2, "b": 1}\n'


def test_output_yaml(monkeypatch, capsys):
    _, out = _shaped(monkeypatch, capsys, "-o", "yaml")
    assert yaml.safe_load(out) == json.loads(_LIST)


def test_output_table_of_a_list_has_a_header_and_one_row_each(monkeypatch, capsys):
    _, out = _shaped(monkeypatch, capsys, "-o", "table", "--filter", "items")
    lines = out.splitlines()
    assert lines[0].split() == ["id", "n", "extra"]
    assert lines[1].split()[:2] == ["a", "1"]
    assert '{"k":[1]}' in lines[1]
    assert lines[2].split() == ["b", "2"]


def test_output_table_of_a_mapping_is_one_field_a_line(monkeypatch, capsys):
    _, out = _shaped(monkeypatch, capsys, "-o", "table", answer=b'{"status": "ok", "n": 1}')
    assert [line.split() for line in out.splitlines()] == [["status", "ok"], ["n", "1"]]


def test_output_table_of_an_empty_list_prints_nothing(monkeypatch, capsys):
    _, out = _shaped(monkeypatch, capsys, "-o", "table", answer=b"[]")
    assert out == ""


def test_filter_a_path(monkeypatch, capsys):
    _, out = _shaped(monkeypatch, capsys, "--filter", "items[1].id")
    assert json.loads(out) == "b"


def test_filter_each_item(monkeypatch, capsys):
    _, out = _shaped(monkeypatch, capsys, "--filter", "items[].id")
    assert json.loads(out) == ["a", "b"]


def test_filter_a_selection(monkeypatch, capsys):
    _, out = _shaped(monkeypatch, capsys, "--filter", "items[n=2].id")
    assert json.loads(out) == ["b"]


def test_filter_a_selection_on_text(monkeypatch, capsys):
    _, out = _shaped(monkeypatch, capsys, "--filter", "items[id=a].n")
    assert json.loads(out) == [1]


def test_filter_what_is_not_there_is_null(monkeypatch, capsys):
    code, out = _shaped(monkeypatch, capsys, "--filter", "nothing.here")
    assert code == 0
    assert out == "null\n"


def test_a_bad_filter_is_refused_before_anything_runs(monkeypatch, capsys, tmp_path):
    instance = _Instance(monkeypatch, _APPLIED)
    args = ["apply", _write(tmp_path, _config()), "--org", "a", "--filter", "a[["]
    assert cli.main(args) == 2
    assert "--filter" in capsys.readouterr().err
    assert instance.calls == []


def test_a_refusal_survives_the_filter(monkeypatch, capsys, tmp_path):
    """A filter chosen for the answer must not swallow the code of a refusal."""
    _Instance(monkeypatch, _refusal(403, "auth.denied"))
    args = ["apply", _write(tmp_path, _config()), "--org", "a", "-o", "json", "--filter", "summary"]
    assert cli.main(args) == 1
    assert json.loads(capsys.readouterr().out)["refused"]["code"] == "auth.denied"


def test_a_query_refusal_survives_the_filter(monkeypatch, capsys):
    _Instance(monkeypatch, _refusal(403, "auth.denied"))
    args = ["query", "sql", "select 1", "--role", "r", "--filter", "result.data"]
    assert cli.main(args) == 1
    assert json.loads(capsys.readouterr().out)["code"] == "auth.denied"


def test_the_exit_code_comes_through_the_output_option(monkeypatch, capsys):
    _Instance(monkeypatch, _http_error(500, b'{"detail": "boom"}'))
    assert cli.main(["api", "GET", "/x", "-o", "yaml"]) == 1
    assert yaml.safe_load(capsys.readouterr().out) == {"detail": "boom"}


def test_a_yaml_export_can_be_read_as_json(monkeypatch, capsys):
    _Instance(monkeypatch, b"domains:\n- id: sales\n")
    assert cli.main(["export", "--org", "a", "-o", "json", "--filter", "domains[].id"]) == 0
    assert json.loads(capsys.readouterr().out) == ["sales"]


def test_without_the_options_output_is_untouched(monkeypatch, capsysbinary):
    _Instance(monkeypatch, b"not json at all")
    assert cli.main(["api", "GET", "/x"]) == 0
    assert capsysbinary.readouterr().out == b"not json at all"


def test_an_answer_with_no_structure_cannot_be_shaped(monkeypatch, capsys):
    _Instance(monkeypatch, b"plain words")
    assert cli.main(["api", "GET", "/x", "-o", "json"]) == 4
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "no structured output" in captured.err
    assert "plain words" in captured.err


def _leaves(parser, path=()):
    subs = [a for a in parser._actions if hasattr(a, "choices") and isinstance(a.choices, dict)]
    if not subs:
        yield path, parser
        return
    for name, child in subs[0].choices.items():
        yield from _leaves(child, (*path, name))


def test_every_command_takes_the_one_output_and_the_one_filter():
    parser = cli.build_parser()
    for path, leaf in _leaves(parser):
        options = {s for a in leaf._actions for s in a.option_strings}
        if path[0] in cli_operate.NOT_SHAPED:
            continue
        assert {"--output", "-o", "--filter", "--context", "--yes"} <= options, path


@pytest.mark.parametrize(
    "argv, answer",
    [
        (["maintenance", "status"], b'{"active": false}'),
        (["maintenance", "off"], b'{"active": false}'),
        (["metadata", "export"], b'{"ok": true, "targets": []}'),
    ],
)
def test_the_commands_that_only_printed_text_give_a_document(monkeypatch, capsys, argv, answer):
    _Instance(monkeypatch, answer)
    assert cli.main([*argv, "--yes", "-o", "json"]) == 0
    assert json.loads(capsys.readouterr().out) == json.loads(answer)


def test_license_status_gives_a_document(monkeypatch, capsys):
    state = type(
        "S",
        (),
        {
            "machine_id": "m",
            "first_seen": 1,
            "elapsed_days": 2.5,
            "trial_expired": False,
            "licensed": False,
            "license_reason": "none",
        },
    )
    monkeypatch.setattr("provisa.licensing.state.evaluate", lambda **_: state)
    assert cli.main(["license", "status", "-o", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["machine_id"] == "m"


def test_license_apply_gives_a_document(monkeypatch, capsys, tmp_path):
    result = type("R", (), {"valid": False, "reason": "bad signature"})
    monkeypatch.setattr("provisa.licensing.apply_license", lambda _: result)
    assert cli.main(["license", "apply", str(tmp_path / "l.json"), "-o", "json"]) == 1
    assert json.loads(capsys.readouterr().out) == {"applied": False, "reason": "bad signature"}


# --- wait ------------------------------------------------------------------------------------ #


@pytest.fixture
def _no_sleep(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(cli_operate.time, "sleep", slept.append)
    return slept


def test_wait_ready_returns_when_the_instance_answers(monkeypatch, capsys, _no_sleep):
    instance = _Instance(monkeypatch, b'{"status": "ready"}')
    assert cli.main(["wait", "ready", "--api", "http://h:1"]) == 0
    assert instance.calls[0].full_url == "http://h:1/ready"
    assert _no_sleep == []
    capsys.readouterr()


def test_wait_ready_polls_through_warming_and_a_dead_port(monkeypatch, capsys, _no_sleep):
    instance = _Instance(
        monkeypatch,
        urllib.error.URLError("connection refused"),
        _http_error(503, b'{"status": "warming"}'),
        b'{"status": "ready"}',
    )
    assert cli.main(["wait", "ready", "--interval", "7"]) == 0
    assert len(instance.calls) == 3
    assert _no_sleep == [7, 7]
    capsys.readouterr()


def _clock(monkeypatch, step: float) -> None:
    now = [0.0]

    def tick() -> float:
        now[0] += step
        return now[0]

    monkeypatch.setattr(cli_operate.time, "monotonic", tick)


def test_wait_times_out_with_its_own_exit(monkeypatch, capsys, _no_sleep):
    _clock(monkeypatch, 10)
    _Instance(monkeypatch, _http_error(503, b'{"status": "warming"}'))
    assert cli.main(["wait", "ready", "--timeout", "25"]) == 3
    assert "wait.timeout" in capsys.readouterr().err


def test_wait_ready_does_not_wait_out_a_refusal(monkeypatch, capsys, _no_sleep):
    _Instance(monkeypatch, _refusal(403, "auth.denied"))
    assert cli.main(["wait", "ready"]) == 1
    assert "auth.denied" in capsys.readouterr().err
    assert _no_sleep == []


def _builds(state: str, **fields: Any) -> bytes:
    build = {
        "sourceId": "pg",
        "schemaName": "public",
        "tableName": "orders",
        "state": state,
        "partial": False,
        "completedAt": "2026-11-09T00:00:00Z",
        "lastError": None,
        "lastErrorCode": None,
        **fields,
    }
    return json.dumps({"data": {"replicaBuilds": {"builds": [build]}}}).encode()


_WAIT_REPLICA = ["wait", "replica", "pg/public/orders", "--org", "acme"]


def test_wait_replica_asks_the_admin_api_as_the_org(monkeypatch, capsys, _no_sleep):
    instance = _Instance(monkeypatch, _builds("idle"))
    assert cli.main(_WAIT_REPLICA) == 0
    call = instance.calls[0]
    assert call.full_url.endswith("/admin/graphql")
    assert call.get_header("X-org-provisa") == "acme"
    assert "replicaBuilds" in json.loads(call.data)["query"]
    capsys.readouterr()


def test_wait_replica_polls_until_the_build_lands(monkeypatch, capsys, _no_sleep):
    instance = _Instance(monkeypatch, _builds("building"), _builds("requested"), _builds("idle"))
    assert cli.main(_WAIT_REPLICA) == 0
    assert len(instance.calls) == 3
    capsys.readouterr()


@pytest.mark.parametrize(
    "answer",
    [
        _builds("idle", partial=True),
        _builds("idle", completedAt=None),
        _builds("retired"),
    ],
)
def test_wait_replica_a_replica_still_filling_is_not_current(
    monkeypatch, capsys, _no_sleep, answer
):
    _clock(monkeypatch, 100)
    _Instance(monkeypatch, answer)
    assert cli.main([*_WAIT_REPLICA, "--timeout", "150"]) == 3
    assert "wait.timeout" in capsys.readouterr().err


def test_wait_replica_timeout_names_the_last_failure(monkeypatch, capsys, _no_sleep):
    _clock(monkeypatch, 100)
    _Instance(monkeypatch, _builds("failed", lastError="disk full", lastErrorCode="x.y"))
    assert cli.main([*_WAIT_REPLICA, "--timeout", "150"]) == 3
    assert "disk full" in capsys.readouterr().err


def test_wait_replica_for_a_table_with_no_replica_times_out(monkeypatch, capsys, _no_sleep):
    _clock(monkeypatch, 100)
    _Instance(monkeypatch, json.dumps({"data": {"replicaBuilds": {"builds": []}}}).encode())
    assert cli.main([*_WAIT_REPLICA, "--timeout", "150"]) == 3
    assert "no replica" in capsys.readouterr().err


def test_wait_replica_reports_a_graphql_refusal(monkeypatch, capsys, _no_sleep):
    answer = {"errors": [{"message": "no", "extensions": {"code": "auth.denied"}}]}
    _Instance(monkeypatch, json.dumps(answer).encode())
    assert cli.main(_WAIT_REPLICA) == 1
    assert "auth.denied" in capsys.readouterr().err


def test_wait_replica_needs_all_three_names(monkeypatch, capsys, _no_sleep):
    instance = _Instance(monkeypatch, b"{}")
    assert cli.main(["wait", "replica", "orders", "--org", "acme"]) == 2
    assert "source/schema/table" in capsys.readouterr().err
    assert instance.calls == []


def test_wait_replica_needs_an_org(monkeypatch, capsys, _no_sleep):
    instance = _Instance(monkeypatch, b"{}")
    assert cli.main(["wait", "replica", "a/b/c"]) == 2
    assert "--org" in capsys.readouterr().err
    assert instance.calls == []


def test_wait_as_a_document(monkeypatch, capsys, _no_sleep):
    _Instance(monkeypatch, b'{"status": "ready"}')
    assert cli.main(["wait", "ready", "-o", "json"]) == 0
    assert json.loads(capsys.readouterr().out) == {"ready": True, "waited_for": "instance"}


# --- schema ---------------------------------------------------------------------------------- #


def test_schema_lists_the_kinds_with_no_argument(capsys):
    assert cli.main(["schema", "-o", "json"]) == 0
    kinds = json.loads(capsys.readouterr().out)["kinds"]
    assert {"sources", "tables", "roles"} <= set(kinds)


def test_schema_prints_the_fields_of_a_kind_from_the_model(capsys):
    from provisa.core.models import Role

    assert cli.main(["schema", "roles", "-o", "json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    fields = {f["name"]: f for f in doc["fields"]}
    assert set(fields) == set(Role.model_json_schema(by_alias=True)["properties"])
    assert fields["id"]["required"] is True


def test_schema_descends_into_a_nested_kind(capsys):
    assert cli.main(["schema", "tables.columns", "-o", "json"]) == 0
    fields = {f["name"] for f in json.loads(capsys.readouterr().out)["fields"]}
    assert {"name", "visible_to"} <= fields


def test_schema_uses_the_name_written_in_a_file(capsys):
    """A table's field is `schema` in the file, whatever the model calls it."""
    assert cli.main(["schema", "tables", "-o", "json"]) == 0
    fields = {f["name"] for f in json.loads(capsys.readouterr().out)["fields"]}
    assert {"schema", "table"} <= fields


def test_schema_as_text_is_a_field_a_line(capsys):
    assert cli.main(["schema", "roles"]) == 0
    assert any(line.split()[:1] == ["id"] for line in capsys.readouterr().out.splitlines())


def test_schema_of_an_unknown_kind_is_a_usage_error(capsys):
    assert cli.main(["schema", "nope"]) == 2
    assert "nope" in capsys.readouterr().err


def test_schema_makes_no_call(monkeypatch, capsys):
    instance = _Instance(monkeypatch, b"")
    assert cli.main(["schema", "domains"]) == 0
    assert instance.calls == []
    capsys.readouterr()


# --- help, completion, exit codes ------------------------------------------------------------ #


def test_the_exit_codes_are_documented(capsys):
    assert cli.main(["exit-codes", "-o", "json"]) == 0
    doc = json.loads(capsys.readouterr().out)
    for command in ("query", "diff", "apply", "can-i", "wait", "api", "env"):
        assert command in doc


def test_help_points_at_the_exit_codes(capsys):
    with pytest.raises(SystemExit) as info:
        cli.main(["--help"])
    assert info.value.code == 0
    assert "provisa exit-codes" in capsys.readouterr().out


@pytest.mark.parametrize("shell", ["bash", "zsh", "fish"])
def test_completion_names_commands_generated_from_the_parser(capsys, shell):
    assert cli.main(["completion", shell]) == 0
    out = capsys.readouterr().out
    # Fish writes a long option without its dashes (`-l filter`).
    for word in ("can-i", "promote", "filter"):
        assert word in out


def _bash_complete(script: str, *words: str) -> list[str]:
    line = " ".join(f"'{w}'" for w in words)
    probe = f'{script}\nCOMP_WORDS=(provisa {line}); COMP_CWORD={len(words)}; _provisa; echo "${{COMPREPLY[*]}}"\n'
    done = subprocess.run(  # nosec B603 B607
        ["bash", "-c", probe], capture_output=True, text=True, check=True
    )
    return done.stdout.split()


def test_bash_completion_completes_a_command_and_a_subcommand(capsys):
    assert cli.main(["completion", "bash"]) == 0
    script = capsys.readouterr().out
    assert "can-i" in _bash_complete(script, "ca")
    assert "list" in _bash_complete(script, "env", "")
    assert "--filter" in _bash_complete(script, "env", "list", "--fi")
