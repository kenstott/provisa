# Copyright (c) 2026 Kenneth Stott
# Canary: 9ac31930-d253-491e-8b93-6a3624534b39
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""`provisa validate|diff|apply|export|promote` (REQ-2222): the CLI moves the config.

The HTTP layer is stubbed; these pin the request sent, what is reported and the exit code.
"""

# Requirements: REQ-2222

from __future__ import annotations

import copy
import email.message
import io
import json
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest
import yaml

from provisa import cli

_TOKEN = "unit-test-placeholder"  # nosec B105
_REFERENCE = "${env:J1050_UNSET_PASSWORD}"
_LEAK = "must-never-be-printed"

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
        seen["org"] = req.get_header("X-org-provisa")
        seen["timeout"] = timeout
        seen["data"] = req.data
        if isinstance(answer, Exception):
            raise answer
        return _Resp(answer)

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    return seen


def _refusal(status: int, code: str) -> urllib.error.HTTPError:
    body = json.dumps({"code": code, "detail": "no", "params": {}}).encode()
    return urllib.error.HTTPError(
        "http://x", status, "refused", email.message.Message(), io.BytesIO(body)
    )


def _write(tmp_path: Path, config: dict[str, Any], name: str = "c.yaml") -> str:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(config, sort_keys=False))
    return str(path)


def _config(**changes: Any) -> dict[str, Any]:
    return {**copy.deepcopy(_CONFIG), **changes}


@pytest.fixture(autouse=True)
def _no_ambient_instance(monkeypatch):
    monkeypatch.delenv("PROVISA_API_URL", raising=False)
    monkeypatch.delenv("PROVISA_API_TOKEN", raising=False)
    monkeypatch.delenv("J1050_UNSET_PASSWORD", raising=False)


# --- validate -------------------------------------------------------------------------------- #


def test_validate_a_good_file_exits_zero_and_makes_no_call(monkeypatch, capsys, tmp_path):
    seen = _stub(monkeypatch, b"")
    assert cli.main(["validate", _write(tmp_path, _config()), "--json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["valid"] is True
    assert report["sections"]["tables"] == 1
    assert seen == {}


def test_validate_needs_no_secret_a_reference_names(capsys, tmp_path):
    """The variable is unset here; a resolving parse would raise on it."""
    assert cli.main(["validate", _write(tmp_path, _config())]) == 0
    assert capsys.readouterr().out.startswith("valid: ")


def test_validate_takes_a_reference_in_a_typed_field(capsys, tmp_path):
    config = _config()
    config["sources"][0]["port"] = "${env:J1050_UNSET_PORT:-5432}"
    assert cli.main(["validate", _write(tmp_path, config)]) == 0
    capsys.readouterr()


def test_validate_still_finds_what_a_reference_was_hiding(capsys, tmp_path):
    """A reference standing in a typed field does not stop the rest being checked."""
    config = _config()
    config["sources"][0]["port"] = "${env:J1050_UNSET_PORT}"
    config["sources"][0]["type"] = "not-a-source-type"
    assert cli.main(["validate", _write(tmp_path, config), "--json"]) == 1
    refused = json.loads(capsys.readouterr().out)["refused"]
    assert refused["code"] == "config.invalid"
    assert any("type" in e["loc"] for e in refused["params"]["errors"])


def test_validate_refuses_text_in_a_typed_field(capsys, tmp_path):
    config = _config()
    config["sources"][0]["port"] = "not-a-number"
    assert cli.main(["validate", _write(tmp_path, config), "--json"]) == 1
    refused = json.loads(capsys.readouterr().out)["refused"]
    assert refused["code"] == "config.invalid"
    assert refused["params"]["errors"][0]["loc"].startswith("sources.0.")


def test_validate_refusal_names_its_code_on_stderr(capsys, tmp_path):
    assert cli.main(["validate", _write(tmp_path, {"sources": "nope"})]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("refused (config.invalid): ")


def test_validate_refuses_a_file_that_cannot_be_read(capsys, tmp_path):
    assert cli.main(["validate", str(tmp_path / "absent.yaml")]) == 1
    assert "refused (config.unreadable)" in capsys.readouterr().err


def test_validate_refuses_bad_yaml_without_quoting_the_file(capsys, tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(f"sources:\n  - password: {_LEAK}\n    : : [\n")
    assert cli.main(["validate", str(path)]) == 1
    err = capsys.readouterr().err
    assert "refused (config.bad_yaml)" in err
    assert _LEAK not in err


def test_validate_never_prints_a_rejected_value(capsys, tmp_path):
    config = _config()
    config["sources"][0]["port"] = _LEAK
    assert cli.main(["validate", _write(tmp_path, config)]) == 1
    captured = capsys.readouterr()
    assert _LEAK not in captured.out + captured.err


def test_validate_reads_includes(capsys, tmp_path):
    fragment = _write(tmp_path, {"domains": [{"id": "sales"}]}, "fragment.yaml")
    config = _config()
    del config["domains"]
    config["includes"] = [Path(fragment).name]
    assert cli.main(["validate", _write(tmp_path, config), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sections"]["domains"] == 1


# --- diff ------------------------------------------------------------------------------------ #


def _diff(monkeypatch, capsys, tmp_path, ours: dict, held: dict) -> dict:
    _stub(monkeypatch, yaml.safe_dump(held).encode())
    assert cli.main(["diff", _write(tmp_path, ours), "--org", "acme", "--json"]) == 0
    return json.loads(capsys.readouterr().out)


def test_diff_reads_the_org_export(monkeypatch, capsys, tmp_path):
    seen = _stub(monkeypatch, yaml.safe_dump(_config()).encode())
    args = ["diff", _write(tmp_path, _config()), "--org", "a/b", "--api", "http://host:9"]
    assert cli.main(args) == 0
    assert seen["method"] == "GET"
    assert seen["url"] == "http://host:9/admin/orgs/a%2Fb/config-export"
    assert seen["org"] == "a/b"
    capsys.readouterr()


def test_diff_of_the_same_model_reports_no_change(monkeypatch, capsys, tmp_path):
    report = _diff(monkeypatch, capsys, tmp_path, _config(), _config())
    assert report["changes"] is False
    assert report["sections"] == {}
    assert report["unchanged"] == 4


def test_diff_reports_an_item_to_add(monkeypatch, capsys, tmp_path):
    held = _config(domains=[{"id": "sales"}])
    ours = _config(domains=[{"id": "sales"}, {"id": "finance"}])
    report = _diff(monkeypatch, capsys, tmp_path, ours, held)
    assert report["changes"] is True
    assert report["sections"]["domains"]["add"] == ["finance"]


def test_diff_reports_a_changed_field_by_name_and_never_its_value(monkeypatch, capsys, tmp_path):
    held = _config()
    held["sources"][0]["password"] = f"${{env:{_LEAK}}}"
    report = _diff(monkeypatch, capsys, tmp_path, _config(), held)
    assert report["sections"]["sources"]["change"] == [{"item": "pg", "fields": ["password"]}]
    assert _LEAK not in json.dumps(report)


def test_diff_keeps_what_only_the_instance_has_apart(monkeypatch, capsys, tmp_path):
    held = _config(domains=[{"id": "sales"}, {"id": "ops_local"}])
    report = _diff(monkeypatch, capsys, tmp_path, _config(), held)
    assert report["sections"]["domains"]["instance_only"] == ["ops_local"]
    assert report["changes"] is False


def test_diff_a_stated_default_is_no_change(monkeypatch, capsys, tmp_path):
    ours = _config()
    ours["sources"][0]["pool_min"] = 1
    report = _diff(monkeypatch, capsys, tmp_path, ours, _config())
    assert report["changes"] is False


def test_diff_a_reference_in_a_typed_field_is_no_change(monkeypatch, capsys, tmp_path):
    """Only the instance knows what the reference resolves to."""
    ours = _config()
    ours["sources"][0]["port"] = "${env:J1050_UNSET_PORT}"
    report = _diff(monkeypatch, capsys, tmp_path, ours, _config())
    assert report["changes"] is False


def test_diff_ignores_order(monkeypatch, capsys, tmp_path):
    ours = _config(domains=[{"id": "a"}, {"id": "b"}])
    ours["roles"][0]["domain_access"] = ["a", "b", "sales"]
    held = _config(domains=[{"id": "b"}, {"id": "a"}])
    held["roles"][0]["domain_access"] = ["sales", "b", "a"]
    report = _diff(monkeypatch, capsys, tmp_path, ours, held)
    assert report["changes"] is False


def test_diff_text_report(monkeypatch, capsys, tmp_path):
    held = _config(domains=[{"id": "sales"}, {"id": "ops_local"}])
    ours = _config(domains=[{"id": "sales"}, {"id": "finance"}])
    _stub(monkeypatch, yaml.safe_dump(held).encode())
    assert cli.main(["diff", _write(tmp_path, ours), "--org", "acme"]) == 0
    out = capsys.readouterr().out
    assert "  A domains/finance" in out
    assert "  I domains/ops_local" in out


def test_diff_refused_by_the_instance_exits_one_with_its_code(monkeypatch, capsys, tmp_path):
    _stub(monkeypatch, _refusal(403, "auth.denied"))
    assert cli.main(["diff", _write(tmp_path, _config()), "--org", "acme"]) == 1
    assert capsys.readouterr().err.startswith("refused (auth.denied): ")


def test_diff_of_a_refused_file_makes_no_call(monkeypatch, capsys, tmp_path):
    seen = _stub(monkeypatch, b"")
    assert cli.main(["diff", _write(tmp_path, {"sources": "nope"}), "--org", "acme"]) == 1
    assert "refused (config.invalid)" in capsys.readouterr().err
    assert seen == {}


# --- apply ----------------------------------------------------------------------------------- #

_APPLIED = json.dumps(
    {
        "summary": {
            "sources": 1,
            "domains": 1,
            "tables": 1,
            "columns": 1,
            "roles": 1,
            "relationships": 0,
            "rls_rules": 0,
            "source_ids": ["pg"],
            "domain_ids": ["sales"],
            "role_ids": ["analyst"],
        }
    }
).encode()


def test_apply_posts_the_file_to_the_org(monkeypatch, capsys, tmp_path):
    seen = _stub(monkeypatch, _APPLIED)
    args = ["apply", _write(tmp_path, _config()), "--org", "acme", "--token", _TOKEN]
    assert cli.main(args) == 0
    assert seen["method"] == "POST"
    assert seen["url"] == "http://127.0.0.1:8000/admin/import/hasura/apply"
    assert seen["org"] == "acme"
    assert seen["auth"] == f"Bearer {_TOKEN}"
    assert yaml.safe_load(json.loads(seen["data"])["config_yaml"]) == _config()
    captured = capsys.readouterr()
    assert "tables 1" in captured.out
    assert _TOKEN not in captured.out + captured.err


def test_apply_sends_a_secret_as_its_reference(monkeypatch, capsys, tmp_path):
    monkeypatch.setenv("J1050_UNSET_PASSWORD", _LEAK)
    seen = _stub(monkeypatch, _APPLIED)
    assert cli.main(["apply", _write(tmp_path, _config()), "--org", "acme"]) == 0
    assert _REFERENCE in seen["data"].decode()
    assert _LEAK not in seen["data"].decode()
    capsys.readouterr()


def test_apply_sends_its_includes(monkeypatch, capsys, tmp_path):
    fragment = _write(tmp_path, {"domains": [{"id": "sales"}]}, "fragment.yaml")
    config = _config()
    del config["domains"]
    config["includes"] = [Path(fragment).name]
    seen = _stub(monkeypatch, _APPLIED)
    assert cli.main(["apply", _write(tmp_path, config), "--org", "acme"]) == 0
    sent = yaml.safe_load(json.loads(seen["data"])["config_yaml"])
    assert sent["domains"] == [{"id": "sales"}]
    assert "includes" not in sent
    capsys.readouterr()


def test_apply_a_refused_config_exits_one_with_its_code(monkeypatch, capsys, tmp_path):
    _stub(monkeypatch, _refusal(400, "import.invalid_config"))
    assert cli.main(["apply", _write(tmp_path, _config()), "--org", "acme"]) == 1
    assert capsys.readouterr().err.startswith("refused (import.invalid_config): ")


def test_apply_a_refused_config_as_json(monkeypatch, capsys, tmp_path):
    _stub(monkeypatch, _refusal(403, "import.sensitive_data_required"))
    assert cli.main(["apply", _write(tmp_path, _config()), "--org", "acme", "--json"]) == 1
    refused = json.loads(capsys.readouterr().out)["refused"]
    assert refused["code"] == "import.sensitive_data_required"
    assert refused["status"] == 403


def test_apply_a_file_that_cannot_be_read_makes_no_call(monkeypatch, capsys, tmp_path):
    seen = _stub(monkeypatch, b"")
    assert cli.main(["apply", str(tmp_path / "absent.yaml"), "--org", "acme"]) == 1
    assert "refused (config.unreadable)" in capsys.readouterr().err
    assert seen == {}


def test_apply_needs_an_org(monkeypatch, capsys, tmp_path):
    seen = _stub(monkeypatch, b"")
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["apply", _write(tmp_path, _config())])
    assert exit_info.value.code == 2
    assert seen == {}
    capsys.readouterr()


def test_an_unreachable_instance_exits_non_zero(monkeypatch, capsys, tmp_path):
    _stub(monkeypatch, urllib.error.URLError("connection refused"))
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["apply", _write(tmp_path, _config()), "--org", "a", "--api", "http://host:9"])
    assert (
        exit_info.value.code == "cannot reach the Provisa API at http://host:9: connection refused"
    )
    capsys.readouterr()


def test_a_timeout_exits_non_zero(monkeypatch, capsys, tmp_path):
    _stub(monkeypatch, TimeoutError())
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["apply", _write(tmp_path, _config()), "--org", "a", "--timeout", "5"])
    assert "within 5s" in str(exit_info.value.code)
    capsys.readouterr()


# --- export ---------------------------------------------------------------------------------- #

_EXPORTED = b"sources: []\n# as the instance wrote it\n"


def test_export_prints_the_instance_answer_unchanged(monkeypatch, capsysbinary):
    seen = _stub(monkeypatch, _EXPORTED)
    assert cli.main(["export", "--org", "acme"]) == 0
    assert capsysbinary.readouterr().out == _EXPORTED
    assert seen["method"] == "GET"
    assert seen["url"] == "http://127.0.0.1:8000/admin/orgs/acme/config-export"
    assert seen["org"] == "acme"
    assert seen["data"] is None


def test_export_writes_a_file(monkeypatch, capsys, tmp_path):
    _stub(monkeypatch, _EXPORTED)
    target = tmp_path / "out.yaml"
    assert cli.main(["export", "--org", "acme", "--file", str(target)]) == 0
    assert target.read_bytes() == _EXPORTED
    assert capsys.readouterr().out == ""


def test_export_refused_leaves_the_file_alone(monkeypatch, capsys, tmp_path):
    _stub(monkeypatch, _refusal(403, "orgs.not_admin"))
    target = tmp_path / "out.yaml"
    target.write_text("kept")
    assert cli.main(["export", "--org", "acme", "--file", str(target)]) == 1
    assert target.read_text() == "kept"
    assert capsys.readouterr().err.startswith("refused (orgs.not_admin): ")


def test_export_to_an_unwritable_path_is_refused(monkeypatch, capsys, tmp_path):
    _stub(monkeypatch, _EXPORTED)
    target = tmp_path / "no-such-dir" / "out.yaml"
    assert cli.main(["export", "--org", "acme", "--file", str(target)]) == 1
    assert "refused (config.unwritable)" in capsys.readouterr().err


def test_an_export_diffs_clean_against_the_instance_it_came_from(monkeypatch, capsys, tmp_path):
    exported = yaml.safe_dump(_config()).encode()
    _stub(monkeypatch, exported)
    target = tmp_path / "out.yaml"
    assert cli.main(["export", "--org", "acme", "--file", str(target)]) == 0
    capsys.readouterr()
    assert cli.main(["diff", str(target), "--org", "acme", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["changes"] is False


# --- promote --------------------------------------------------------------------------------- #

_PROMOTE = ["promote", "--org", "acme", "--from", "dev", "--to", "test", "--message", "m"]

_MERGED = {
    "applied": True,
    "report": {
        "target_env": "test",
        "added": 1,
        "changed": 0,
        "removed": 0,
        "unchanged": 2,
        "paths": {"added": ["tables/x.yaml"], "changed": [], "removed": []},
        "settings": {"added": [], "changed": [], "removed": []},
        "pulled_in": [],
        "conflicts": [],
    },
}


def test_promote_merges_through_the_environments_api(monkeypatch, capsys):
    seen = _stub(monkeypatch, json.dumps(_MERGED).encode())
    assert cli.main(_PROMOTE) == 0
    assert seen["method"] == "POST"
    assert seen["url"].endswith("/admin/orgs/acme/environments/test/merge")
    assert json.loads(seen["data"]) == {"from_env": "dev", "message": "m"}
    out = capsys.readouterr().out
    assert "Promote APPLIED to test" in out
    assert "A tables/x.yaml" in out


def test_promote_to_a_protected_environment_is_proposed(monkeypatch, capsys):
    answer = {
        "applied": False,
        "requires_approval": True,
        "request": {"id": 7, "target_env": "prod", "report": _MERGED["report"]},
    }
    _stub(monkeypatch, json.dumps(answer).encode())
    assert cli.main(_PROMOTE) == 2
    assert "PROPOSED as request 7" in capsys.readouterr().out


def test_promote_refused_exits_one_with_its_code(monkeypatch, capsys):
    _stub(monkeypatch, _refusal(404, "environments.unknown"))
    assert cli.main(_PROMOTE) == 1
    assert capsys.readouterr().err.startswith("refused (environments.unknown): ")


def test_promote_needs_a_message(monkeypatch, capsys):
    seen = _stub(monkeypatch, b"{}")
    with pytest.raises(SystemExit) as exit_info:
        cli.main(_PROMOTE[:-2])
    assert exit_info.value.code == 2
    assert seen == {}
    capsys.readouterr()
