# Copyright (c) 2026 Kenneth Stott
# Canary: 59369440-062a-4b29-905f-3aaa2c4a77d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A custom role saved with no domains reads nothing, on every surface.

A role reaches the domains it lists; ``"*"`` is the only way to say all, and an empty
``domain_access`` is none. Two custom roles are loaded onto one real server, identical in every
other respect — same capabilities, both named in every column's ``visible_to``:

* ``nodomains``  — ``domain_access: []``
* ``alldomains`` — ``domain_access: ["*"]``

The first is refused data on GraphQL, on SQL over HTTP and on pgwire, and is shown an empty
catalog. The second reads the same table on all three. Column grants are held constant on purpose:
the only thing that differs between the two roles is the domain list.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest
import yaml

from tests.integration.isolated_server import IsolatedServer, drop_org_schema

pytestmark = [pytest.mark.integration]

_ORG = "role_no_domains"
_REPO_ROOT = Path(__file__).parents[2]
_CAPABILITIES = ["query_development", "full_results", "usage"]
_NO_DOMAINS = "nodomains"
_ALL_DOMAINS = "alldomains"


def _config_with_the_two_roles(tmp_dir: Path) -> str:
    """sample_config.yaml plus the two roles, each granted every column the fixture declares."""
    with open(_REPO_ROOT / "tests" / "fixtures" / "sample_config.yaml") as f:
        cfg = yaml.safe_load(f)
    cfg["roles"].append(
        {"id": _NO_DOMAINS, "capabilities": list(_CAPABILITIES), "domain_access": []}
    )
    cfg["roles"].append(
        {"id": _ALL_DOMAINS, "capabilities": list(_CAPABILITIES), "domain_access": ["*"]}
    )
    granted = 0
    for table in cfg["tables"]:
        for column in table["columns"]:
            column["visible_to"] = [*column["visible_to"], _NO_DOMAINS, _ALL_DOMAINS]
            granted += 1
    assert granted, "the fixture declares no columns: nothing would be granted to either role"
    path = tmp_dir / "role-no-domains.yaml"
    path.write_text(yaml.safe_dump(cfg, sort_keys=False))
    return str(path)


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    srv = IsolatedServer(
        _ORG,
        enable_pgwire=True,
        config=_config_with_the_two_roles(tmp_path_factory.mktemp("role-no-domains")),
    )
    srv.start()
    try:
        yield srv
    finally:
        srv.stop_process()
        asyncio.run(drop_org_schema(_ORG))


def _client(srv: IsolatedServer) -> httpx.Client:
    return httpx.Client(base_url=srv.base_url, timeout=srv.request_timeout + 15)


def _pgwire(srv: IsolatedServer, role: str):
    import psycopg2

    return psycopg2.connect(
        host="127.0.0.1", port=srv.pgwire_port, dbname="provisa", user=role, password="provisa"
    )


# --- both roles are loaded as they were saved ----------------------------------------------------


def test_both_roles_are_saved_with_the_lists_they_were_given(server):
    with _client(server) as c:
        resp = c.get("/admin/roles/")
    assert resp.status_code == 200, resp.text
    roles = {r["id"]: r for r in resp.json()}
    assert roles[_NO_DOMAINS]["domain_access"] == []
    assert roles[_ALL_DOMAINS]["domain_access"] == ["*"]
    assert set(roles[_NO_DOMAINS]["capabilities"]) == set(roles[_ALL_DOMAINS]["capabilities"])


# --- the role with "*" reads on every surface (the control) --------------------------------------


def test_the_wildcard_role_reads_on_graphql(server):
    with _client(server) as c:
        resp = c.post(
            "/data/graphql",
            json={"query": "{ __schema { queryType { fields { name } } } }"},
            headers={"x-provisa-role": _ALL_DOMAINS},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert not body.get("errors"), body
    names = {f["name"] for f in body["data"]["__schema"]["queryType"]["fields"]}
    assert any("orders" in n.lower() for n in names), sorted(names)


def test_the_wildcard_role_reads_on_sql_over_http(server):
    with _client(server) as c:
        resp = c.post(
            "/data/sql",
            json={"sql": "SELECT COUNT(*) AS n FROM orders"},
            headers={"x-provisa-role": _ALL_DOMAINS},
        )
    assert resp.status_code == 200, resp.text


def test_the_wildcard_role_reads_on_pgwire(server):
    conn = _pgwire(server, _ALL_DOMAINS)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM orders")
        row = cur.fetchone()
    finally:
        conn.close()
    assert row is not None and int(row[0]) > 0, "the fixture's orders table is seeded"


def test_the_wildcard_role_is_shown_its_domains(server):
    with _client(server) as c:
        resp = c.get("/data/domains", headers={"x-provisa-role": _ALL_DOMAINS})
    assert resp.status_code == 200, resp.text
    assert resp.json(), "a role reaching every domain is shown the org's domains"


# --- the role with no domains is refused data on every surface -----------------------------------


def test_the_empty_role_is_refused_on_graphql(server):
    with _client(server) as c:
        for query in ("{ __schema { queryType { fields { name } } } }", "{ __typename }"):
            resp = c.post(
                "/data/graphql", json={"query": query}, headers={"x-provisa-role": _NO_DOMAINS}
            )
            assert resp.status_code == 400, resp.text
            body = resp.json()
            assert body["code"] == "data.no_schema_available_for_role", body
            assert body["params"] == {"role_id": _NO_DOMAINS}


def test_the_empty_role_is_refused_on_sql_over_http(server):
    with _client(server) as c:
        resp = c.post(
            "/data/sql",
            json={"sql": "SELECT COUNT(*) AS n FROM orders"},
            headers={"x-provisa-role": _NO_DOMAINS},
        )
    assert resp.status_code in (400, 403), resp.text
    assert _NO_DOMAINS in resp.text
    assert "rows" not in resp.json()


def test_the_empty_role_is_refused_on_pgwire(server):
    import psycopg2

    conn = _pgwire(server, _NO_DOMAINS)
    try:
        cur = conn.cursor()
        with pytest.raises(psycopg2.Error) as err:
            cur.execute("SELECT COUNT(*) FROM orders")
            cur.fetchall()
    finally:
        conn.close()
    assert _NO_DOMAINS in str(err.value), str(err.value)


def test_the_empty_role_sees_an_empty_catalog(server):
    with _client(server) as c:
        # The domains it reaches: none.
        domains = c.get("/data/domains", headers={"x-provisa-role": _NO_DOMAINS})
        assert domains.status_code == 200, domains.text
        assert domains.json() == []
        # No schema to describe, as SDL or as a proto.
        sdl = c.get("/data/sdl", headers={"x-provisa-role": _NO_DOMAINS})
        assert sdl.status_code == 404, sdl.text
        assert sdl.json()["code"] == "data.no_schema_for_role"
        proto = c.get(f"/data/proto/{_NO_DOMAINS}")
        assert proto.status_code == 404, proto.text
        # ...and naming a domain in the request adds nothing to it.
        asked = c.get(
            "/data/sdl",
            params={"domain": "sales-analytics"},
            headers={"x-provisa-role": _NO_DOMAINS},
        )
        assert asked.status_code == 403, asked.text
        assert asked.json()["code"] == "data.domain_not_accessible"


def test_the_empty_role_sees_no_table_in_the_pgwire_catalog(server):
    import psycopg2

    conn = _pgwire(server, _NO_DOMAINS)
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
            )
            tables = [r[0] for r in cur.fetchall()]
        except psycopg2.Error:
            # Refusing the catalog read outright shows the role no table either.
            tables = []
    finally:
        conn.close()
    assert tables == [], f"a role that lists no domain was shown tables: {tables}"


def test_the_wildcard_role_sees_the_table_in_the_pgwire_catalog(server):
    conn = _pgwire(server, _ALL_DOMAINS)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema NOT IN ('pg_catalog', 'information_schema')"
        )
        tables = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
    assert any("orders" in t for t in tables), tables
