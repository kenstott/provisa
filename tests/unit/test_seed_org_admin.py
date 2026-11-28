# Copyright (c) 2026 Kenneth Stott
# Canary: a83e1f60-4c27-4d95-b0e8-7f2d61c9a4b5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A TEST INSTANCE'S ADMINISTRATOR IS SEATED THE WAY THE PRODUCT SEATS ITS FIRST ONE (REQ-1264).

The config superuser is retired. A conformance job, the regions demo and the pgwire timing script
each seed their instance's org_admin through first-run setup (scripts/seed_org_admin.py): the
instance boots on its model without an auth section, POST /setup/ creates the administrator, and
the full model is put as it. Nothing outside the superuser's own code signs in with it."""

# Requirements: REQ-1264, REQ-1913

from __future__ import annotations

import base64
import http.server
import json
import re
import sys
import threading
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))

import seed_org_admin as seeder  # noqa: E402

MODEL = {
    "federation_engine": "duckdb",
    "auth": {"provider": "basic", "scram": True, "jwt_secret": "${env:CONFORMANCE_JWT_SECRET}"},
    "domains": [{"id": "conformance"}],
    "sources": [],
    "tables": [],
}


def test_the_boot_config_is_the_model_without_its_auth_section():
    boot = seeder.boot_config(MODEL)
    assert "auth" not in boot
    assert boot == {key: value for key, value in MODEL.items() if key != "auth"}
    assert "auth" in MODEL  # the model itself is untouched


def test_writing_the_boot_config_from_a_model_file(tmp_path: Path):
    model = tmp_path / "model.yaml"
    model.write_text(yaml.safe_dump(MODEL, sort_keys=False))
    out = tmp_path / "home" / "provisa.yaml"
    seeder.write_boot_config(model, out)
    assert yaml.safe_load(out.read_text()) == seeder.boot_config(MODEL)


def test_a_model_without_auth_has_nothing_for_the_administrator_to_sign_in_by(tmp_path: Path):
    model = tmp_path / "model.yaml"
    model.write_text(yaml.safe_dump(seeder.boot_config(MODEL)))
    with pytest.raises(ValueError, match="no auth section"):
        seeder.write_boot_config(model, tmp_path / "provisa.yaml")


class _Recorder(http.server.BaseHTTPRequestHandler):
    """Stands in for the booting server: answers setup and the model put, recording each."""

    seen: list[dict] = []
    answers: dict[tuple[str, str], list[int]] = {}

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length)
        self.seen.append(
            {
                "method": self.command,
                "path": self.path,
                "authorization": self.headers.get("Authorization"),
                "content_type": self.headers.get("Content-Type"),
                "body": body,
            }
        )
        queue = self.answers.get((self.command, self.path)) or [200]
        status = queue.pop(0) if len(queue) > 1 else queue[0]
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"success": true}')

    do_POST = _handle
    do_PUT = _handle

    def log_message(self, *args) -> None:
        return None


@pytest.fixture
def server(monkeypatch):
    monkeypatch.setattr(seeder, "_PAUSE", 0.0)
    _Recorder.seen = []
    _Recorder.answers = {}
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Recorder)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()


def test_seed_runs_setup_then_puts_the_model_as_the_new_administrator(server, tmp_path: Path):
    model = tmp_path / "model.yaml"
    model.write_text(yaml.safe_dump(MODEL, sort_keys=False))
    seeder.seed(server, model, "conformance-admin", "generated-password-0123456789")
    setup, put = _Recorder.seen
    assert (setup["method"], setup["path"]) == ("POST", "/setup/")
    assert setup["authorization"] is None
    assert json.loads(setup["body"]) == {
        "provider": "basic",
        "mode": "single",
        "admin_username": "conformance-admin",
        "admin_password": "generated-password-0123456789",
    }
    assert (put["method"], put["path"]) == ("PUT", "/admin/config")
    assert put["content_type"] == "application/yaml"
    assert put["body"] == model.read_bytes()
    scheme, _, credential = put["authorization"].partition(" ")
    assert scheme == "Basic"
    assert (
        base64.b64decode(credential).decode() == "conformance-admin:generated-password-0123456789"
    )


def test_a_server_still_loading_is_asked_again(server, tmp_path: Path):
    model = tmp_path / "model.yaml"
    model.write_text(yaml.safe_dump(MODEL))
    _Recorder.answers[("PUT", "/admin/config")] = [503, 200]
    seeder.seed(server, model, "conformance-admin", "generated-password-0123456789")
    assert [(s["method"], s["path"]) for s in _Recorder.seen] == [
        ("POST", "/setup/"),
        ("PUT", "/admin/config"),
        ("PUT", "/admin/config"),
    ]


def test_a_refusal_is_raised_at_once_and_nothing_follows_it(server, tmp_path: Path):
    model = tmp_path / "model.yaml"
    model.write_text(yaml.safe_dump(MODEL))
    _Recorder.answers[("POST", "/setup/")] = [409]
    with pytest.raises(RuntimeError, match="answered 409"):
        seeder.seed(server, model, "conformance-admin", "generated-password-0123456789")
    assert [(s["method"], s["path"]) for s in _Recorder.seen] == [("POST", "/setup/")]


def test_the_password_comes_from_the_environment_never_the_command_line(monkeypatch, tmp_path):
    called = {}
    monkeypatch.setattr(seeder, "seed", lambda *args: called.setdefault("args", args))
    monkeypatch.setenv(seeder.PASSWORD_ENV, "from-the-environment-0123456789")
    model = tmp_path / "model.yaml"
    seeder.main(
        [
            "seed_org_admin.py",
            "seed",
            "--api",
            "http://127.0.0.1:1/",
            "--model",
            str(model),
            "--user",
            "conformance-admin",
        ]
    )
    assert called["args"] == (
        "http://127.0.0.1:1",
        model,
        "conformance-admin",
        "from-the-environment-0123456789",
    )


# --- nothing outside the superuser's own code signs in with it ---------------------------------

#: What a test instance, demo or deploy script would sign in with if it still used the config
#: superuser: its variables, its sign-in route, its config section.
_SUPERUSER_USE = re.compile(r"PROVISA_SUPERUSER_|/auth/superuser-login|^\s*superuser:\s*$", re.M)
_CONSUMERS = [
    *sorted((REPO / ".github" / "workflows").glob("conformance-*.yml")),
    REPO / ".github" / "actions" / "conformance-instance" / "action.yml",
    REPO / "config" / "provisa-regions-demo.yaml.tmpl",
    REPO / "scripts" / "launch-demo-regions.sh",
    REPO / "scripts" / "deploy-cloud.sh",
    REPO / "scripts" / "pgwire_catalog_timing.py",
    *sorted((REPO / "scripts" / "conformance").rglob("*.py")),
]


@pytest.mark.parametrize("path", _CONSUMERS, ids=lambda p: str(p.relative_to(REPO)))
def test_no_test_instance_demo_or_deploy_signs_in_with_the_config_superuser(path: Path):
    assert path.exists()
    found = _SUPERUSER_USE.findall(path.read_text())
    assert not found, f"{path.relative_to(REPO)} still uses the config superuser: {found}"


def test_the_conformance_instance_seeds_its_org_admin_by_first_run_setup():
    action = yaml.safe_load(
        (REPO / ".github" / "actions" / "conformance-instance" / "action.yml").read_text()
    )
    start = next(s for s in action["runs"]["steps"] if s["name"] == "Start the Provisa instance")
    script = start["run"]
    boot = script.index("seed_org_admin.py boot-config")
    serve = script.index("uvicorn main:app")
    seed = script.index("seed_org_admin.py seed")
    create_user = script.index("/admin/users/")
    assert boot < serve < seed < create_user
    assert 'PROVISA_CONFIG="$home/provisa.yaml"' in script
    assert '--model "$home/model.yaml" --out "$home/provisa.yaml"' in script
    # The generated password is masked before anything can print it, and handed to later steps.
    assert script.index("::add-mask::$admin_password") < script.index("CONFORMANCE_ADMIN_PASSWORD=")
