"""Temporary: capture the insider demo's answers for the card harness (not committed)."""

import json
from pathlib import Path

import httpx
import yaml

from tests.integration.test_insider_demo import _DEMO, server, set_up, stack  # noqa: F401

OUT = Path.home() / "provisa-ui-forms-render" / "harness" / "insider-capture"


def test_capture(set_up):
    srv = set_up
    OUT.mkdir(parents=True, exist_ok=True)
    h = {"X-Provisa-Role": "org_admin"}
    intro = httpx.get(f"{srv.base_url}/data/introspection", headers=h, timeout=120)
    assert intro.status_code == 200, intro.text
    (OUT / "introspection.json").write_text(json.dumps(intro.json()["data"]))
    role = "org_admin"
    for step in yaml.safe_load((_DEMO / "tour.yaml").read_text())["steps"]:
        if "role" in step:
            role = "org_admin" if step["role"] == "all" else step["role"]
        if "sql" in step:
            r = httpx.post(f"{srv.base_url}/data/sql", json={"sql": step["sql"]},
                           headers={"X-Provisa-Role": role}, timeout=300)
            assert r.status_code == 200, (step["key"], r.text)
            (OUT / f"sql-{step['key']}.json").write_text(json.dumps(r.json()))
    for q in sorted((_DEMO / "queries").glob("*.graphql")):
        r = httpx.post(f"{srv.base_url}/data/graphql", json={"query": q.read_text()}, headers=h, timeout=300)
        assert r.status_code == 200 and not r.json().get("errors"), r.text
        (OUT / f"gql-{q.stem}.json").write_text(json.dumps(r.json()))
