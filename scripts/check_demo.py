# Copyright (c) 2026 Kenneth Stott
# Canary: 41658f5f-455a-4796-821a-d4165ac5f335
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Run every check a named demo has to pass, and report what is missing.

    python scripts/check_demo.py <name>

Each check is the one the repo already has, run for this demo alone:

  tests       tests/unit/test_named_demo_tour.py's cases for the demo: every step names what the
              demo has, has its words in every language, and the tour ends on a landing page
              and tests/unit/test_named_demo_columns.py's
  examples    each queries/*.graphql valid against the demo's tables and views, as
              tests/unit/test_named_demo_example_queries.py builds their schema
  card        demo.yaml as scripts/gen-site-demos.mjs reads it (published, title, description, a
              screenshot beside it), and the site page in step with it; and its video's tagline
  images      each image step's picture a PNG beside tour.yaml, as the tour serves it
              (provisa.core.demo.tour_image)
  bundle      requires_provisa a Provisa version (scripts/demo_bundle.py's rule), and the
              directory one this Provisa reads (provisa.core.demo_bundles.check_directory)
  run-compose docker-compose.run.yml, for a demo with stores: each service labelled with the
              demo and its role, each store with where config.yaml reads it, every port on the
              loopback, and each store of a type the sandbox tier's node shares
              (scripts/demo_stores.py SHARED_TYPES) with the suffix config.yaml names its data with
              (ns_env) and, for Postgres and MongoDB, its access script
  locales     the tour's words translated in every locale, not the English left in place
  regions     a published demo in regions (demo.yaml regions) one the regions sandbox tier serves:
              named in PROVISA_SANDBOX_REGIONS_DEMOS (packaging/linux/first-launch.sh), the list
              the UI server routes to that tier by (provisa/ui_server.py)

Exits 1 when any check found something missing.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

NAMED = ROOT / "demo" / "named"
LOCALES = ROOT / "provisa-ui" / "src" / "i18n" / "locales"
_TESTS = ("tests/unit/test_named_demo_tour.py", "tests/unit/test_named_demo_columns.py")
#: The store roles the sandbox tier's node finds by label (scripts/demo_stores_sync.sh).
STORE_ROLES = ("rdb", "vector", "document", "search", "graph")
_STORE_LABELS = ("com.provisa.demo.host_env", "com.provisa.demo.port_env", "com.provisa.demo.port")
#: The shared store types whose data the node loads with the demo's roles and grants: their access
#: script (com.provisa.demo.access) is required (scripts/demo_stores.py loads it).
_SHARED_ACCESS = ("postgres", "mongo")
INSTALL = ROOT / "packaging" / "linux" / "first-launch.sh"


def demo_cases(collected: list[str], name: str) -> list[str]:
    """The test ids of ``collected`` that are this demo's: the parametrized cases whose id is its
    name, and the check that the demo is among those there are."""
    return [
        node
        for node in collected
        if node.endswith(f"[{name}]") or node.endswith("::test_there_is_a_demo_tour_to_check")
    ]


def check_tests(name: str) -> list[str]:
    run = [sys.executable, "-m", "pytest", "-p", "no:cacheprovider"]
    listed = subprocess.run(
        [*run, "--collect-only", "-q", *_TESTS], cwd=ROOT, capture_output=True, text=True
    )
    if listed.returncode != 0:
        return [f"the demo tests did not collect:\n{listed.stdout}{listed.stderr}"]
    cases = demo_cases(listed.stdout.split(), name)
    if not any(case.endswith(f"[{name}]") for case in cases):
        return [f"no test case is {name}'s: it has no tour.yaml or no config.yaml"]
    result = subprocess.run([*run, "-q", *cases], cwd=ROOT, capture_output=True, text=True)
    if result.returncode != 0:
        return [f"the demo's test cases failed:\n{result.stdout[-6000:]}"]
    return []


def check_examples(home: Path) -> list[str]:
    from graphql import parse, validate

    from tests.unit.test_named_demo_example_queries import _views_schema

    queries = sorted((home / "queries").glob("*.graphql"))
    if not queries:
        return ["queries/ holds no example query"]
    admin = {"id": "org_admin", "capabilities": [], "domain_access": ["*"]}
    schema = _views_schema(home, admin, include_tables=True)
    problems = []
    for query in queries:
        errors = validate(schema, parse(query.read_text()))
        problems += [f"queries/{query.name}: {e.message}" for e in errors]
    return problems


def check_card(home: Path) -> list[str]:
    spec = yaml.safe_load((home / "demo.yaml").read_text()) or {}
    problems = []
    if spec.get("published") is not True:
        problems.append("demo.yaml: published is not true, so the demo has no card")
    for key in ("title", "description", "screenshot"):
        if not isinstance(spec.get(key), str) or not spec[key].strip():
            problems.append(f"demo.yaml: no {key}")
    shot = spec.get("screenshot")
    if isinstance(shot, str) and shot.strip() and not (home / shot).is_file():
        problems.append(f"demo.yaml: screenshot {shot} is not beside it")
    video = spec.get("video")
    tagline = video.get("tagline") if isinstance(video, dict) else None
    if not isinstance(tagline, str) or not tagline.strip():
        problems.append("demo.yaml: no video tagline")
    if not problems:
        site = subprocess.run(
            ["node", "scripts/gen-site-demos.mjs", "--check"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        if site.returncode != 0:
            problems.append(
                "site/demos.html is not in step (node scripts/gen-site-demos.mjs): "
                + (site.stdout + site.stderr).strip()
            )
    return problems


def check_images(home: Path) -> list[str]:
    from provisa.core.demo import tour_image

    tour = home / "tour.yaml"
    if not tour.is_file():
        return []
    problems = []
    for step in (yaml.safe_load(tour.read_text()) or {}).get("steps") or []:
        if "image" not in step:
            continue
        try:
            tour_image(home, step["image"])
        except ValueError as refused:
            problems.append(f"tour.yaml: step {step['key']}: {refused}")
            continue
        if (home / step["image"]).read_bytes()[:8] != b"\x89PNG\r\n\x1a\n":
            problems.append(f"tour.yaml: step {step['key']}: {step['image']} is not a PNG")
    return problems


def check_bundle(home: Path) -> list[str]:
    from provisa.core import demo_bundles as bundles

    required = (yaml.safe_load((home / "demo.yaml").read_text()) or {}).get("requires_provisa")
    if not isinstance(required, str):
        return ["demo.yaml: no requires_provisa (the oldest Provisa it runs on)"]
    try:
        bundles.provisa_version_key(required)
        bundles.check_directory(home)
    except bundles.BundleError as e:
        return [f"demo.yaml: {e}"]
    return []


def _shared_type(role: str | None, image: str) -> str | None:
    """The shared store type the sandbox tier's node runs a service of this role and image as."""
    from scripts.demo_stores import SHARED_TYPES

    return next(
        (t for (r, prefix), t in SHARED_TYPES.items() if r == role and image.startswith(prefix)),
        None,
    )


def check_run_compose(home: Path, name: str) -> list[str]:
    run = home / "docker-compose.run.yml"
    if not run.is_file():
        if (home / "docker-compose.yml").is_file():
            return ["no docker-compose.run.yml: the stores of docker-compose.yml run from it"]
        return []
    config = (home / "config.yaml").read_text()
    problems = []
    for service, spec in (yaml.safe_load(run.read_text()).get("services") or {}).items():
        labels = spec.get("labels") or {}
        if labels.get("com.provisa.demo") != name:
            problems.append(f"{service}: not labelled com.provisa.demo: {name}")
        role = labels.get("com.provisa.demo.role")
        if not role:
            problems.append(f"{service}: no com.provisa.demo.role label")
        if role in STORE_ROLES:
            for label in _STORE_LABELS:
                if not labels.get(label):
                    problems.append(f"{service}: a store with no {label} label")
            for label in _STORE_LABELS[:2]:
                variable = labels.get(label)
                if variable and f"${{env:{variable}" not in config:
                    problems.append(f"{service}: config.yaml does not read {variable}")
        shared = _shared_type(role, str(spec.get("image", "")))
        if shared is not None:
            ns = labels.get("com.provisa.demo.ns_env")
            if not ns:
                problems.append(
                    f"{service}: a shared {shared} store with no com.provisa.demo.ns_env label"
                )
            elif f"${{env:{ns}" not in config:
                problems.append(f"{service}: config.yaml does not name its data with {ns}")
            if shared in _SHARED_ACCESS:
                access = labels.get("com.provisa.demo.access")
                if not access:
                    problems.append(
                        f"{service}: a shared {shared} store with no com.provisa.demo.access label"
                    )
                elif not (home / access).is_file():
                    problems.append(f"{service}: its access script {access} is not in the demo")
        for port in spec.get("ports") or []:
            if not str(port).startswith("127.0.0.1:"):
                problems.append(f"{service}: port {port} is not on the loopback (127.0.0.1:)")
    return problems


def check_locales(name: str, home: Path, locales: Path = LOCALES) -> list[str]:
    if not (home / "tour.yaml").is_file():
        return []  # toured by the core tour (demo.yaml tour: core): no words of its own
    keys = [s["key"] for s in yaml.safe_load((home / "tour.yaml").read_text())["steps"]]

    def words(locale: str) -> dict:
        tour = json.loads((locales / locale / "tour.json").read_text())["tour"]
        return {
            **{f"steps.{k}": tour["steps"].get(k) for k in keys},
            f"demos.{name}": tour["demos"].get(name),
        }

    english = words("en")
    problems = []
    for locale in sorted(p.name for p in locales.iterdir() if p.is_dir() and p.name != "en"):
        same = [key for key, value in words(locale).items() if value and value == english[key]]
        if same:
            problems.append(f"{locale}: still the English words for {', '.join(same)}")
    return problems


def regions_demos(install: Path = INSTALL) -> frozenset[str]:
    """The demos the install's regions sandbox tier serves: its PROVISA_SANDBOX_REGIONS_DEMOS."""
    found = re.search(r'PROVISA_SANDBOX_REGIONS_DEMOS:\s*"([^"]*)"', install.read_text())
    if found is None:
        raise ValueError(f"{install.relative_to(ROOT)} sets no PROVISA_SANDBOX_REGIONS_DEMOS")
    return frozenset(n for n in found.group(1).split(",") if n)


def check_regions(name: str, home: Path, listed: frozenset[str] | None = None) -> list[str]:
    spec = yaml.safe_load((home / "demo.yaml").read_text()) or {}
    if not spec.get("published") or not spec.get("regions"):
        return []
    if name in (regions_demos() if listed is None else listed):
        return []
    return [
        "demo.yaml: in regions, but not one PROVISA_SANDBOX_REGIONS_DEMOS names "
        f"({INSTALL.relative_to(ROOT)})"
    ]


def checks(name: str, home: Path) -> dict[str, Callable[[], list[str]]]:
    return {
        "tests": lambda: check_tests(name),
        "examples": lambda: check_examples(home),
        "card": lambda: check_card(home),
        "images": lambda: check_images(home),
        "bundle": lambda: check_bundle(home),
        "run-compose": lambda: check_run_compose(home, name),
        "locales": lambda: check_locales(name, home),
        "regions": lambda: check_regions(name, home),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run every check a named demo has to pass.")
    parser.add_argument("name", help="the demo's name: its directory under demo/named/")
    args = parser.parse_args(argv)
    home = NAMED / args.name
    if not re.fullmatch(r"[a-z][a-z0-9]*", args.name) or not (home / "config.yaml").is_file():
        print(f"no named demo {args.name!r}: demo/named/{args.name}/config.yaml is not there")
        return 1
    missing = 0
    for check, run in checks(args.name, home).items():
        problems = run()
        print(f"{'ok  ' if not problems else 'FAIL'} {check}")
        for problem in problems:
            print(f"     {problem}")
        missing += len(problems)
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
