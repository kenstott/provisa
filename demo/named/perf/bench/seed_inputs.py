# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The seed hash (REQ-2092): the sha256 of what decides the backends device's seeded data, which
names the stored seed image (provisa-perf-seed-<first 12>) a run boots its backends device from.

The inputs, in a fixed order: the seeder's own files (generate_*.py, seed.py, Dockerfile.seeder)
and Dockerfile.postgres whole, each as its path and its content; then, read out of
docker-compose.yml, only the engines' pinned image tags (postgresql, mongodb, clickhouse, neo4j)
and the scale settings the seeder reads (its PROVISA_BENCH_* row counts), so an engine upgrade or
a new scale forces a re-seed and an unrelated compose edit does not. A missing file, service or
setting is an error naming it.

Usage: seed_inputs.py [--commit SHA] [--root REPO]
  --commit  hash the files as they are at that commit (what the devices check out), not the
            working tree
"""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
from pathlib import Path

import yaml

PERF = "demo/named/perf"
FILES = [
    f"{PERF}/generate_postgres.py",
    f"{PERF}/generate_clickhouse.py",
    f"{PERF}/generate_mongo.py",
    f"{PERF}/generate_neo4j.py",
    f"{PERF}/seed.py",
    f"{PERF}/Dockerfile.seeder",
    f"{PERF}/Dockerfile.postgres",
]
COMPOSE = f"{PERF}/docker-compose.yml"
ENGINE_SERVICES = ["postgresql", "mongodb", "clickhouse", "neo4j"]
SCALE_SETTINGS = [
    "PROVISA_BENCH_ORDERS",
    "PROVISA_BENCH_NEO4J_ORDERS",
    "PROVISA_BENCH_ITEMS_PER_ORDER",
]


class SeedInputError(ValueError):
    pass


def _reader(root: Path, commit: str | None):
    def read(path: str) -> bytes:
        if commit is None:
            target = root / path
            if not target.is_file():
                raise SeedInputError(f"seed input {path} is missing under {root}")
            return target.read_bytes()
        done = subprocess.run(
            ["git", "-C", str(root), "show", f"{commit}:{path}"], capture_output=True
        )
        if done.returncode != 0:
            raise SeedInputError(
                f"seed input {path} is not readable at {commit}: {done.stderr.decode().strip()}"
            )
        return done.stdout

    return read


def compose_inputs(compose_text: str) -> list[tuple[str, str]]:
    """The engines' image tags and the seeder's scale settings, by name, from the compose file."""
    services = yaml.safe_load(compose_text)["services"]
    out: list[tuple[str, str]] = []
    for name in ENGINE_SERVICES:
        if name not in services or "image" not in services[name]:
            raise SeedInputError(f"{COMPOSE}: service {name} names no image")
        out.append((f"services.{name}.image", str(services[name]["image"])))
    if "seeder" not in services or "environment" not in services["seeder"]:
        raise SeedInputError(f"{COMPOSE}: service seeder has no environment")
    seeder_env = services["seeder"]["environment"]
    for setting in SCALE_SETTINGS:
        if setting not in seeder_env:
            raise SeedInputError(f"{COMPOSE}: seeder environment has no {setting}")
        out.append((f"services.seeder.environment.{setting}", str(seeder_env[setting])))
    return out


def seed_hash(root: Path, commit: str | None = None) -> str:
    read = _reader(root, commit)
    h = hashlib.sha256()
    for path in FILES:
        content = read(path)
        h.update(f"{path}\0{len(content)}\0".encode())
        h.update(content)
    for key, value in compose_inputs(read(COMPOSE).decode()):
        h.update(f"{key}\0{value}\0".encode())
    return h.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="The seed hash of the perf benchmark's seed inputs."
    )
    parser.add_argument("--commit")
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[4])
    args = parser.parse_args()
    try:
        print(seed_hash(args.root, args.commit))
    except SeedInputError as e:
        print(f"seed_inputs.py: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
