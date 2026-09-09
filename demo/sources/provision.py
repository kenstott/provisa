# Copyright (c) 2026 Kenneth Stott
# Canary: b5c0adc9-2285-48b7-8c98-5a9edf869d0a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provision the live demo sources (demo/sources/<name>) — the ONE entry point both the demo
start (start-ui-install.sh --source=<name>) and the source-to-query e2e (provisa-ui/e2e/
demo-source-containers.ts) call, so a source is started, primed and removed the same way in both.

    provision.py up   [--prefix P] [--engine E] [--env K=V ...] NAME...
    provision.py down [--prefix P] NAME...
    provision.py list

Each NAME is a directory under demo/sources with a compose.yml, an optional prime.py (seed) and an
optional ``engine`` file naming the only engine that can read it. ``up`` starts the compose project
``<prefix>-<name>`` (default prefix provisa-demo; the e2e passes provisa-e2e so a running demo and a
running e2e never share a project), waits for health, then primes. ``--engine`` refuses a source
whose ``engine`` file names a different engine rather than registering one the engine cannot read.
``--env`` values (the port variables the compose files and prime scripts read) apply to both.
``--network NET`` joins the started containers to an existing Docker network under the source's
name as alias — how a coordinator that is itself a container (the Trino lane, the Docker demo
start) reaches a source at ``<name>:<container port>`` instead of a host-published port.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

SOURCES_DIR = Path(__file__).resolve().parent


def _source_dir(name: str) -> Path:
    d = SOURCES_DIR / name
    if not (d / "compose.yml").is_file():
        available = sorted(p.name for p in SOURCES_DIR.iterdir() if (p / "compose.yml").is_file())
        raise SystemExit(f"unknown source {name!r}; available: {' '.join(available)}")
    return d


def _compose(prefix: str, name: str, args: list[str], env: dict[str, str]) -> None:
    d = _source_dir(name)
    subprocess.run(
        ["docker", "compose", "-p", f"{prefix}-{name}", "-f", str(d / "compose.yml"), *args],
        check=True,
        env=env,
    )


def _join_network(prefix: str, name: str, network: str, env: dict[str, str]) -> None:
    d = _source_dir(name)
    ids = subprocess.run(
        ["docker", "compose", "-p", f"{prefix}-{name}", "-f", str(d / "compose.yml"), "ps", "-q"],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    ).stdout.split()
    for cid in ids:
        res = subprocess.run(
            ["docker", "network", "connect", "--alias", name, network, cid],
            capture_output=True,
            text=True,
            env=env,
        )
        # Already joined (a re-run against a live project) is not an error; anything else is.
        if res.returncode != 0 and "already exists" not in res.stderr:
            raise SystemExit(f"joining {name} to network {network} failed: {res.stderr.strip()}")


def up(
    names: list[str], prefix: str, engine: str | None, env: dict[str, str], network: str | None
) -> None:
    for name in names:
        d = _source_dir(name)
        engine_file = d / "engine"
        if engine is not None and engine_file.is_file():
            needed = engine_file.read_text().strip()
            if needed != engine:
                raise SystemExit(
                    f"--source={name} is served only by the {needed} engine; this start runs "
                    f"{engine}, which has no path to it."
                )
        print(f"Provisioning source '{name}' (compose project {prefix}-{name})...", flush=True)
        _compose(prefix, name, ["up", "-d", "--wait"], env)
        if network:
            _join_network(prefix, name, network, env)
        prime = d / "prime.py"
        if prime.is_file():
            subprocess.run([sys.executable, str(prime)], check=True, env=env)


def down(names: list[str], prefix: str, env: dict[str, str]) -> None:
    for name in names:
        # A project that was never started removes nothing; compose exits non-zero only on a real error.
        _compose(prefix, name, ["down", "-v"], env)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="provision.py")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_up = sub.add_parser("up")
    p_up.add_argument("names", nargs="+")
    p_up.add_argument("--prefix", default="provisa-demo")
    p_up.add_argument("--engine", default=None)
    p_up.add_argument("--env", action="append", default=[], metavar="K=V")
    p_up.add_argument("--network", default=None)
    p_down = sub.add_parser("down")
    p_down.add_argument("names", nargs="+")
    p_down.add_argument("--prefix", default="provisa-demo")
    p_down.add_argument("--env", action="append", default=[], metavar="K=V")
    sub.add_parser("list")
    a = parser.parse_args(argv)
    if a.cmd == "list":
        for p in sorted(SOURCES_DIR.iterdir()):
            if (p / "compose.yml").is_file():
                print(p.name)
        return 0
    env = dict(os.environ)
    for kv in a.env:
        k, _, v = kv.partition("=")
        env[k] = v
    if a.cmd == "up":
        up(a.names, a.prefix, a.engine, env, a.network)
    else:
        down(a.names, a.prefix, env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
