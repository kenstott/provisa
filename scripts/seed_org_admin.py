# Copyright (c) 2026 Kenneth Stott
# Canary: 5d3a9c71-2e84-4f0b-9a6d-c1e7b4f80293
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Seed a test instance's org_admin the way the product seats its first administrator (REQ-1264).

A test instance (a conformance job's, a demo launcher's) needs one account that administers its
model: puts it with PUT /admin/config, creates its users, stores its settings. That account is
created by the first-run setup every install goes through (POST /setup/, provider ``basic``, mode
``single``), which seats it as the deployment's bootstrap administrator: org_admin, and the
platform_admin a single-tenant install's operator holds (REQ-1913). No config backdoor.

The setup wizard runs only on a deployment that still needs it: one whose config has no auth
section. So the instance boots on its model without one (``boot-config``), and ``seed`` then runs
setup and puts the full model, auth section included, as the new administrator.

Usage:
  seed_org_admin.py boot-config --model <full.yaml> --out <boot.yaml>
  seed_org_admin.py seed --api http://127.0.0.1:<port> --model <full.yaml> --user <name>
      (the password is read from $SEED_ADMIN_PASSWORD, never the command line)
"""

# Requirements: REQ-1264, REQ-1913, REQ-124

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import yaml

#: The environment variable ``seed`` reads the administrator's password from.
PASSWORD_ENV = "SEED_ADMIN_PASSWORD"
#: Attempts, ``_PAUSE`` seconds apart, at a request the booting server cannot answer yet.
_ATTEMPTS = 6
_PAUSE = 3.0


def boot_config(model: dict) -> dict:
    """The model without its auth section: what the instance boots on so that first-run setup
    still has an administrator to create."""
    return {key: value for key, value in model.items() if key != "auth"}


def write_boot_config(model_path: Path, out: Path) -> None:
    model = yaml.safe_load(model_path.read_text())
    if "auth" not in model:
        raise ValueError(
            f"{model_path} has no auth section for the seeded administrator to sign in by"
        )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(yaml.safe_dump(boot_config(model), sort_keys=False))


def basic_authorization(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


def _send(request: urllib.request.Request) -> bytes:
    """One request, retried while the server is still coming up: no connection, or a 5xx from a
    model still loading. A 4xx is the server's answer and is raised at once."""
    for attempt in range(_ATTEMPTS):
        try:
            with urllib.request.urlopen(request, timeout=1800) as answer:
                return answer.read()
        except urllib.error.HTTPError as exc:
            if exc.code < 500 or attempt == _ATTEMPTS - 1:
                detail = exc.read().decode(errors="replace")
                raise RuntimeError(
                    f"{request.get_method()} {request.full_url} answered {exc.code}: {detail}"
                ) from exc
        except urllib.error.URLError:
            if attempt == _ATTEMPTS - 1:
                raise
        time.sleep(_PAUSE)
    raise AssertionError("unreachable: the last attempt returns or raises")


def setup_request(api: str, user: str, password: str) -> urllib.request.Request:
    """First-run setup: the basic provider, a single-tenant install, this administrator."""
    body = {
        "provider": "basic",
        "mode": "single",
        "admin_username": user,
        "admin_password": password,
    }
    return urllib.request.Request(
        f"{api}/setup/",
        data=json.dumps(body).encode(),
        method="POST",
        headers={"Content-Type": "application/json"},
    )


def config_request(api: str, model: bytes, user: str, password: str) -> urllib.request.Request:
    """The full model, put by the administrator setup created."""
    return urllib.request.Request(
        f"{api}/admin/config",
        data=model,
        method="PUT",
        headers={
            "Authorization": basic_authorization(user, password),
            "Content-Type": "application/yaml",
        },
    )


def seed(api: str, model_path: Path, user: str, password: str) -> None:
    _send(setup_request(api, user, password))
    _send(config_request(api, model_path.read_bytes(), user, password))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    boot = commands.add_parser("boot-config")
    boot.add_argument("--model", required=True, type=Path)
    boot.add_argument("--out", required=True, type=Path)
    run = commands.add_parser("seed")
    run.add_argument("--api", required=True)
    run.add_argument("--model", required=True, type=Path)
    run.add_argument("--user", required=True)
    args = parser.parse_args(argv[1:])
    if args.command == "boot-config":
        write_boot_config(args.model, args.out)
    else:
        seed(args.api.rstrip("/"), args.model, args.user, os.environ[PASSWORD_ENV])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
