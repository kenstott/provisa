#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 54021000-9413-4f46-8a92-1a98b4c2425f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""Run a named demo's setup.yaml once against a running server (REQ-2016).

A demo's setup.yaml lists, in order, what it needs run after the server is up: ``command: <name>``
calls a command of its config.yaml as org_admin with no arguments; ``refresh: <view>`` builds the
stored rows of a view of its config.yaml with ``materialize: true``; ``profile: <profiler>`` runs a
Data Profiler source of its config.yaml at once for every member table (REQ-2052): a sandbox
cloned from the demo's template carries those runs (env_copy.clone_model); ``environment: {name,
data_mode}`` creates an environment of the model (REQ-2053), as the Environments page does;
``client: <script>`` runs a client of the demo's own (a script in its directory) against the server
until it exits, told where the server is -- ``PROVISA_URL`` and ``PROVISA_FLIGHT`` -- beside this
run's environment: what the client writes to the demo's stores is then theirs (REQ-2067, the
provisioning demo's recorded pulls). start-ui-install.sh and the demo's template build both call
this, so the two set a demo up alike.

A bundled demo runs only its environment steps where it runs (``--environments-only``): the other
steps ran when its bundle was built and what they wrote is in its stores' dumps, but an
environment is a branch of the model in the control plane, which every start of the demo builds
anew. Its profile runs, kept in the control plane too, are the build's, carried in the bundle and
loaded in place of its profile steps (REQ-2052).

Once per control plane: the demo's name is added to the operator setting ``demo.setup_completed``
when every step has succeeded, and a demo already listed there is not set up again. The first step
that fails stops the run: the exit status is 1 and the step is named. Nothing is retried.

Usage: run_demo_setup.py <demo dir> --url <server base url> [--flight <grpc://host:port>]
       [--environments-only]
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import httpx
import yaml

SETTING = "demo.setup_completed"
#: Where a bundled demo's server loads the profile runs its bundle carries (REQ-2052).
CARRIED_RUNS = "/admin/profilers/carried-runs"
_ROLE = {"X-Provisa-Role": "org_admin"}
_TIMEOUT = 1800.0


class StepFailed(RuntimeError):
    def __init__(self, step: dict, why: str) -> None:
        super().__init__(f"setup step {step} failed: {why}")


def steps(demo_dir: Path) -> list[dict]:
    """The demo's steps, each one ``command``, ``refresh``, ``profile``, ``environment`` or
    ``client``; none when it has no setup.yaml."""
    path = demo_dir / "setup.yaml"
    if not path.exists():
        return []
    listed = (yaml.safe_load(path.read_text()) or {}).get("steps") or []
    for step in listed:
        if (
            not isinstance(step, dict)
            or len(step) != 1
            or next(iter(step))
            not in (
                "command",
                "refresh",
                "profile",
                "environment",
                "client",
            )
        ):
            raise SystemExit(
                f"{path}: a step is one 'command: <name>', 'refresh: <view>', "
                f"'profile: <profiler>', 'environment: {{name: <name>, data_mode: <mode>}}' or "
                f"'client: <script>': {step}"
            )
        if "client" in step and not (demo_dir / str(step["client"])).is_file():
            raise SystemExit(f"{path}: a client step names a script of the demo's: {step}")
        if "environment" in step and (
            not isinstance(step["environment"], dict)
            or set(step["environment"]) != {"name", "data_mode"}
        ):
            raise SystemExit(f"{path}: an environment step names its name and data_mode: {step}")
    return listed


def _ok(response: httpx.Response, step: dict) -> Any:
    if response.status_code != 200:
        raise StepFailed(step, f"HTTP {response.status_code}: {response.text}")
    body = response.json()
    if body.get("errors"):
        raise StepFailed(step, "; ".join(e.get("message", str(e)) for e in body["errors"]))
    return body.get("data")


def completed(client: httpx.Client) -> list[str]:
    response = client.get("/admin/settings/catalog")
    response.raise_for_status()
    for card in response.json()["cards"]:
        for setting in card["settings"]:
            if setting["key"] == SETTING:
                return list(setting["value"] or [])
    raise SystemExit(f"the server has no setting {SETTING}")


def _mutation_field(client: httpx.Client, command: str, step: dict) -> tuple[str, str | None]:
    """The GraphQL mutation field of ``command`` (its name in the active naming convention, under
    its domain's prefix when there is one) and a field of its result to select, if it has fields."""
    data = _ok(
        client.post(
            "/data/graphql",
            json={
                "query": "{ __schema { mutationType { fields { name type { kind name ofType "
                "{ kind name ofType { kind name ofType { kind name } } } } } } } }"
            },
        ),
        step,
    )
    wanted = command.replace("_", "").lower()
    found = [
        f
        for f in data["__schema"]["mutationType"]["fields"]
        if f["name"].rsplit("__", 1)[-1].replace("_", "").lower() == wanted
    ]
    if len(found) != 1:
        raise StepFailed(step, f"the server has {len(found)} mutations for command {command!r}")
    named = found[0]["type"]
    while named.get("ofType"):
        named = named["ofType"]
    selection = None
    if named["kind"] == "OBJECT":
        fields = _ok(
            client.post(
                "/data/graphql",
                json={"query": f'{{ __type(name: "{named["name"]}") {{ fields {{ name }} }} }}'},
            ),
            step,
        )["__type"]["fields"]
        selection = fields[0]["name"]
    return found[0]["name"], selection


def run_client(demo_dir: Path, step: dict, url: str, flight: str | None) -> str:
    """Run the demo's own client script against the server, until it exits."""
    if flight is None:
        raise StepFailed(step, "a client step needs the server's Arrow Flight address (--flight)")
    script = demo_dir / step["client"]
    done = subprocess.run(
        [sys.executable, str(script)],
        cwd=demo_dir,
        env={**os.environ, "PROVISA_URL": url, "PROVISA_FLIGHT": flight},
        timeout=_TIMEOUT,
    )
    if done.returncode != 0:
        raise StepFailed(step, f"{script.name} exited {done.returncode}")
    return f"client {script.name}: done"


def run_step(client: httpx.Client, step: dict) -> str:
    """Run one step; what it did, for the log."""
    if "command" in step:
        field, selection = _mutation_field(client, step["command"], step)
        query = f"mutation {{ {field}{f' {{ {selection} }}' if selection else ''} }}"
        data = _ok(client.post("/data/graphql", json={"query": query}), step)
        return f"command {step['command']}: {data[field]}"
    if "carried_runs" in step:
        response = client.post(CARRIED_RUNS)
        if response.status_code != 200:
            raise StepFailed(step, f"HTTP {response.status_code}: {response.text}")
        return f"profile runs carried in the bundle: {', '.join(response.json()['loaded'])}"
    if "profile" in step:
        profiler = step["profile"]
        response = client.post(f"/admin/profilers/{profiler}/run")
        if response.status_code != 200:
            raise StepFailed(step, f"HTTP {response.status_code}: {response.text}")
        outcomes = response.json()
        failed = [f"{o['table']}: {o['error']}" for o in outcomes if o["error"] is not None]
        if failed or not outcomes:
            raise StepFailed(
                step, "; ".join(failed) or f"profiler {profiler!r} has no member tables"
            )
        return f"profile {profiler}: {', '.join(o['table'] for o in outcomes)}"
    if "environment" in step:
        me = client.get("/auth/me")
        me.raise_for_status()
        org = me.json()["active_org_id"]
        created = client.post(f"/admin/orgs/{org}/environments", json=step["environment"])
        if created.status_code != 200:
            raise StepFailed(step, f"HTTP {created.status_code}: {created.text}")
        made = created.json()["environment"]
        if made["data_mode"] == "test_synthetic":
            return _generated(client, org, step, made["name"])
        return f"environment {made['name']}: created ({made['data_mode']})"
    view = step["refresh"]
    data = _ok(
        client.post(
            "/admin/graphql",
            json={
                "query": f'mutation {{ refreshMv(mvId: "view-view_{view}") {{ success message }} }}'
            },
        ),
        step,
    )
    if not data["refreshMv"]["success"]:
        raise StepFailed(step, data["refreshMv"]["message"])
    return f"refresh {view}: {data['refreshMv']['message']}"


def _generated(client: httpx.Client, org: str, step: dict, env: str) -> str:
    """Generate the Test (synthetic) environment ``env`` from prod's profile runs, as its
    Generate and Confirm on the Environments page do (REQ-1942): declaring it in setup.yaml is the
    demo's confirmation of the warning. Waits until it is generated."""
    base = f"/admin/orgs/{org}/environments/{env}/synthetic"
    shown = client.post(base, json={})
    if shown.status_code != 200:
        raise StepFailed(step, f"HTTP {shown.status_code}: {shown.text}")
    digest = shown.json()["warning"]["digest"]
    started = client.post(f"{base}/confirm", json={"digest": digest})
    if started.status_code != 200:
        raise StepFailed(step, f"HTTP {started.status_code}: {started.text}")
    deadline = time.monotonic() + _TIMEOUT
    while True:
        listed = client.get(f"/admin/orgs/{org}/environments")
        listed.raise_for_status()
        [row] = [e for e in listed.json()["environments"] if e["name"] == env]
        if row["data_status"] == "ready":
            return f"environment {env}: created and generated (test_synthetic)"
        if row["data_status"] == "failed":
            raise StepFailed(step, f"{env} was not generated: {row['data_error']}")
        if time.monotonic() > deadline:
            raise StepFailed(step, f"{env} was not generated in {_TIMEOUT:.0f}s")
        time.sleep(2)


def run(
    demo_dir: Path, url: str, *, environments_only: bool = False, flight: str | None = None
) -> int:
    listed = steps(demo_dir)
    if environments_only:
        # REQ-2052 (A FIRST RUN IS SEEDED): the profile runs are the bundle build's, carried in the
        # bundle and loaded once, before its environments, as its hosted template loads
        # them: the first run and the drift run setup measured, not two runs of the data as setup
        # left it. A Test (synthetic) environment is generated from them.
        profiles = any("profile" in step for step in listed)
        listed = [step for step in listed if "environment" in step]
        if profiles:
            listed.insert(0, {"carried_runs": True})
    if not listed:
        return 0
    name = demo_dir.resolve().name
    with httpx.Client(base_url=url, headers=_ROLE, timeout=_TIMEOUT) as client:
        done = completed(client)
        if name in done:
            print(f"demo {name}: already set up ({SETTING}); nothing to run")
            return 0
        for step in listed:
            try:
                did = (
                    run_client(demo_dir, step, url, flight)
                    if "client" in step
                    else run_step(client, step)
                )
                print(f"demo {name}: {did}", flush=True)
            except StepFailed as failed:
                print(f"demo {name}: {failed}", file=sys.stderr)
                return 1
        stored = client.put(
            "/admin/settings/catalog", json={"values": {SETTING: [*done, name]}, "confirm": []}
        )
        stored.raise_for_status()
    print(f"demo {name}: set up")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("demo_dir", type=Path)
    parser.add_argument("--url", required=True)
    parser.add_argument(
        "--flight", help="the server's Arrow Flight address, grpc://host:port (a client step's)"
    )
    parser.add_argument(
        "--environments-only",
        action="store_true",
        help="run only the environment steps (a bundled demo, where it runs)",
    )
    args = parser.parse_args()
    sys.exit(
        run(args.demo_dir, args.url, environments_only=args.environments_only, flight=args.flight)
    )
