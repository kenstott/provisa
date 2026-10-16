# Copyright (c) 2026 Kenneth Stott
# Canary: 8198d1ec-d419-4ede-810d-94060b19e387
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: demo sandboxes on the sandbox tier (REQ-2006).

Everything here is the ``test`` instance on the local test platform (tests/local_platform.py):
an isolated server started as the sandbox tier (``PROVISA_SANDBOX_TIER=true``), on the embedded
Postgres control plane, with a data directory of its own and two published named demos of its own,
``alpha`` and ``beta``, each a SQLite file of a few rows. At startup the tier builds each demo's
template org and lands its replicas; a visitor then opens sandboxes of them through
``POST /public/demos/{name}``, with no sign-in, and is answered a token:

- each sandbox reads its own demo's rows, from its own copy, and nobody else's token reaches it;
- an unknown or unpublished demo, and a sandbox past ``sandbox.max_concurrent``, are refused by
  name;
- an upload past ``sandbox.disk_limit`` is refused by name;
- a sandbox carries its template's environments (alpha's setup.yaml declares two Test (fake)
  ones), and its visitor switches between them but creates and deletes none (REQ-2053);
- a sandbox no request has reached for ``sandbox.idle_ttl`` is deleted with its org, its folder and
  its token.
"""

# Requirements: REQ-2006, REQ-1035, REQ-1263, REQ-1992, REQ-2053

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ORG = "sbxdeploye2e"  # the deployment's own org; never a sandbox (no ``sbx`` + hex shape matters)
_JWT_SECRET = "demo-sandboxes-e2e-signing-secret-0123456789"
_ROWS = {"alpha": ["ant", "ape"], "beta": ["bee", "bat", "boar"]}
_CAP = 3
#: The Test (fake) environments a demo's setup.yaml creates (REQ-2053).
_ENVIRONMENTS = {"alpha": ["dev", "test"]}
_DISK = 16 * 1024 * 1024


#: The embedding models a demo declares (its config's vector_models), as the sonnets demo does.
_EMBEDDING_MODELS = {
    "alpha": [
        {
            "id": "sentence-transformers/all-MiniLM-L6-v2",
            "provider": "huggingface",
            "dimensions": 384,
            "enabled": True,
        }
    ]
}


def _demo(root: Path, name: str, *, published: bool) -> None:
    folder = root / name
    folder.mkdir(parents=True)
    db = sqlite3.connect(folder / "animals.sqlite")
    db.execute("CREATE TABLE animals (id INTEGER PRIMARY KEY, name TEXT)")
    db.executemany(
        "INSERT INTO animals VALUES (?, ?)", list(enumerate(_ROWS.get(name, ["x"]), start=1))
    )
    db.commit()
    db.close()
    (folder / "demo.yaml").write_text(
        yaml.safe_dump({"title": f"The {name} demo", "description": name, "published": published})
    )
    config = {
        "federation_engine": "duckdb",
        "auth": {"provider": "none", "assignments_source": "provisa"},
        "naming": {"domain_prefix": False, "rules": []},
        "domains": [{"id": "zoo", "description": "animals"}],
        "roles": [],
        "sources": [
            {
                "id": "zoo-db",
                "type": "sqlite",
                "path": "${env:PROVISA_NAMED_DEMO_DIR}/animals.sqlite",
            }
        ],
        "tables": [
            {
                "source_id": "zoo-db",
                "domain_id": "zoo",
                "schema": "main",
                "table": "animals",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": ["sandbox", "org_admin"]},
                    {
                        "name": "name",
                        "data_type": "varchar",
                        "visible_to": ["sandbox", "org_admin"],
                    },
                ],
            }
        ],
        "relationships": [],
    }
    if name in _EMBEDDING_MODELS:
        config["vector_models"] = _EMBEDDING_MODELS[name]
    if name in _ENVIRONMENTS:
        # REQ-2053: a demo with Test (fake) environments, as the testdata demo has: every column
        # declares its fake, and its setup.yaml creates the environments.
        for column in config["tables"][0]["columns"]:
            column["fake"] = "hash()" if column["name"] == "id" else "name()"
        (folder / "setup.yaml").write_text(
            yaml.safe_dump(
                {
                    "steps": [
                        {"environment": {"name": env, "data_mode": "test_fake"}}
                        for env in _ENVIRONMENTS[name]
                    ]
                }
            )
        )
    (folder / "config.yaml").write_text(yaml.safe_dump(config))


def _deployment_config(path: Path) -> None:
    path.write_text(
        yaml.safe_dump(
            {
                "federation_engine": "duckdb",
                # The sandbox tier serves many orgs: a multitenant deployment, as the cloud's is.
                "multitenancy": True,
                "naming": {"domain_prefix": False, "rules": []},
                "cache": {"enabled": False},
                "domains": [{"id": "ops", "description": "the deployment's own"}],
                "roles": [],
                "sources": [],
                "tables": [],
            }
        )
    )


@pytest.fixture(scope="module")
def tier(tmp_path_factory):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("demo_sandboxes")
    demos = work / "named"
    _demo(demos, "alpha", published=True)
    _demo(demos, "beta", published=True)
    _demo(demos, "gamma", published=False)
    config = work / "config.yaml"
    _deployment_config(config)
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={
            "PROVISA_SANDBOX_TIER": "true",
            "PROVISA_NAMED_DEMOS_DIR": str(demos),
            "PROVISA_DATA_DIR": str(work / "data"),
            "PROVISA_SANDBOX_MAX_CONCURRENT": str(_CAP),
            "PROVISA_SANDBOX_DISK_LIMIT": str(_DISK),
        },
    )
    srv.data_dir = work / "data"
    srv.demos = demos
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _open(tier, name: str) -> httpx.Response:
    return httpx.post(f"{tier.base_url}/public/demos/{name}", timeout=240)


def _opened(tier, name: str) -> dict:
    """A sandbox of ``name``, once its template is ready (its replicas land after startup)."""
    deadline = time.monotonic() + 180
    while True:
        resp = _open(tier, name)
        if resp.status_code != 503 or resp.json().get("code") != "sandbox.demo_not_ready":
            assert resp.status_code == 200, (resp.text, _last_traceback(tier))
            return resp.json()
        assert time.monotonic() < deadline, tier.dump_stderr_debug()[-6000:]
        time.sleep(2)


def _last_traceback(tier) -> str:
    log = tier.dump_stderr_debug()
    return log[log.rfind("Traceback (most recent call last)") :][:12000]


def _sql(tier, sandbox: dict, sql: str, *, token: str | None = None) -> httpx.Response:
    return httpx.post(
        f"{tier.base_url}/data/sql",
        json={"sql": sql},
        headers={
            "Authorization": f"Bearer {token or sandbox['token']}",
            "x-org-provisa": sandbox["org_id"],
        },
        timeout=120,
    )


@pytest.fixture(scope="module")
def alpha(tier):
    return _opened(tier, "alpha")


@pytest.fixture(scope="module")
def beta(tier):
    return _opened(tier, "beta")


def _names(resp: httpx.Response) -> list[str]:
    assert resp.status_code == 200, resp.text
    return sorted(r["name"] for r in resp.json()["data"]["sql"])


def test_a_sandbox_has_the_embedding_models_its_demo_declares(tier, alpha, beta):
    """A demo's own embedding model reaches each of its sandboxes (a query that embeds text is
    refused by the model allowlist without it); a demo that declares none gives its sandboxes
    none."""

    def models(sandbox: dict) -> list[str]:
        resp = httpx.get(
            f"{tier.base_url}/admin/ai-models",
            headers={
                "Authorization": f"Bearer {sandbox['token']}",
                "x-org-provisa": sandbox["org_id"],
            },
            timeout=60,
        )
        assert resp.status_code == 200, resp.text
        return [m["id"] for m in resp.json()["vector_models"]]

    assert models(alpha) == ["sentence-transformers/all-MiniLM-L6-v2"]
    assert models(beta) == []


def test_each_sandbox_reads_its_own_demos_rows(tier, alpha, beta):
    assert alpha["org_id"] != beta["org_id"]
    assert alpha["org_id"].startswith("sbx") and alpha["token"].startswith("provisa_pat_")
    assert _names(_sql(tier, alpha, "SELECT name FROM animals")) == sorted(_ROWS["alpha"])
    assert _names(_sql(tier, beta, "SELECT name FROM animals")) == sorted(_ROWS["beta"])


def test_a_sandboxs_token_reaches_no_other_sandbox(tier, alpha, beta):
    crossed = _sql(tier, beta, "SELECT name FROM animals", token=alpha["token"])
    assert crossed.status_code in (401, 403), crossed.text


def test_a_sandbox_reads_its_demos_own_source_live(tier, alpha):
    """REQ-2049 (the maintainer's ruling): a demo's own sources are read live, as the demo
    defines them -- a row the demo's source holds now is a row its sandbox reads now."""
    source = sqlite3.connect(tier.demos / "alpha" / "animals.sqlite")
    try:
        source.execute("INSERT INTO animals VALUES (99, 'auk')")
        source.commit()
        assert _names(_sql(tier, alpha, "SELECT name FROM animals")) == sorted(
            [*_ROWS["alpha"], "auk"]
        )
    finally:
        source.execute("DELETE FROM animals WHERE id = 99")
        source.commit()
        source.close()


def _envs(tier, sandbox: dict) -> httpx.Response:
    return httpx.get(
        f"{tier.base_url}/admin/orgs/{sandbox['org_id']}/environments",
        headers={
            "Authorization": f"Bearer {sandbox['token']}",
            "x-org-provisa": sandbox["org_id"],
        },
        timeout=60,
    )


def _sql_in(tier, sandbox: dict, env: str, sql: str) -> httpx.Response:
    return httpx.post(
        f"{tier.base_url}/data/sql",
        json={"sql": sql},
        headers={
            "Authorization": f"Bearer {sandbox['token']}",
            "x-org-provisa": sandbox["org_id"],
            "x-provisa-env": env,
        },
        timeout=120,
    )


def test_a_sandbox_carries_its_templates_environments(tier, alpha, beta):
    """REQ-2053: a sandbox has the environments its demo's template has (its setup.yaml's), each
    with the template's data mode; a demo that declares none gives its sandboxes prod alone."""
    listed = _envs(tier, alpha)
    assert listed.status_code == 200, listed.text
    modes = {e["name"]: e["data_mode"] for e in listed.json()["environments"]}
    assert modes.keys() == {"prod", "dev", "test"}
    assert modes["dev"] == modes["test"] == "test_fake"
    listed = _envs(tier, beta)
    assert listed.status_code == 200, listed.text
    assert [e["name"] for e in listed.json()["environments"]] == ["prod"]


def test_a_visitor_switches_to_a_test_environment_and_reads_fakes(tier, alpha):
    """REQ-2053: the visitor is served by each existing environment: prod answers the real rows,
    dev and test the same fakes, none of them a real value."""
    real = _names(_sql_in(tier, alpha, "prod", "SELECT name FROM animals"))
    assert real == sorted(_ROWS["alpha"])
    dev = _names(_sql_in(tier, alpha, "dev", "SELECT name FROM animals"))
    test = _names(_sql_in(tier, alpha, "test", "SELECT name FROM animals"))
    assert dev == test and len(dev) == len(real)
    assert not set(dev) & set(real)


def test_a_visitor_neither_creates_nor_deletes_an_environment(tier, alpha):
    """REQ-2053: switching is the visitor's; making or dropping an environment is not."""
    headers = {"Authorization": f"Bearer {alpha['token']}", "x-org-provisa": alpha["org_id"]}
    base = f"{tier.base_url}/admin/orgs/{alpha['org_id']}/environments"
    made = httpx.post(base, json={"name": "mine", "data_mode": "test_fake"}, headers=headers)
    assert made.status_code == 403, made.text
    dropped = httpx.delete(f"{base}/dev", headers=headers)
    assert dropped.status_code == 403, dropped.text
    assert {e["name"] for e in _envs(tier, alpha).json()["environments"]} == {
        "prod",
        "dev",
        "test",
    }


def test_an_unknown_or_unpublished_demo_is_refused_by_name(tier):
    for name in ("nosuchdemo", "gamma"):
        resp = _open(tier, name)
        assert resp.status_code == 404, resp.text
        assert resp.json()["code"] == "sandbox.unknown_demo"


def test_past_the_cap_a_visitor_is_asked_to_try_again(tier, alpha, beta):
    opened = [alpha, beta]
    while len(opened) < _CAP:
        opened.append(_opened(tier, "alpha"))
    refused = _open(tier, "beta")
    assert refused.status_code == 503, refused.text
    assert refused.json()["code"] == "sandbox.at_capacity"


def test_an_upload_past_the_disk_cap_is_refused_by_name(tier, alpha):
    resp = httpx.put(
        f"{tier.base_url}/admin/sources/bigfile/file",
        params={"name": "big.csv", "type": "csv"},
        content=b"x" * (_DISK + 1),
        headers={"Authorization": f"Bearer {alpha['token']}", "x-org-provisa": alpha["org_id"]},
        timeout=120,
    )
    assert resp.status_code == 507, resp.text
    assert resp.json()["code"] == "sandbox.disk_full"


def test_an_idle_sandbox_is_deleted_whole(tier, beta):
    """Its last request is moved past the idle limit; within the reaper's minute its org, its
    folder and its token are gone."""
    _age(beta["org_id"])
    folder = tier.data_dir / "orgs" / beta["org_id"]
    deadline = time.monotonic() + 120
    while folder.exists():
        assert time.monotonic() < deadline, tier.dump_stderr_debug()[-6000:]
        time.sleep(2)
    gone = _sql(tier, beta, "SELECT name FROM animals")
    assert gone.status_code == 401, gone.text


def _age(org_id: str) -> None:
    import os

    from sqlalchemy import create_engine, update

    from provisa.core.schema_admin import demo_sandboxes

    engine = create_engine(os.environ["PLATFORM_DATABASE_URL"])
    try:
        with engine.begin() as conn:
            conn.execute(
                update(demo_sandboxes)
                .where(demo_sandboxes.c.org_id == org_id)
                .values(last_active_at="2000-01-01T00:00:00+00:00")
            )
    finally:
        engine.dispose()


class _Listener:
    """A TCP port on this host that counts the connections made to it."""

    def __init__(self) -> None:
        import socket
        import threading

        self._sock = socket.socket()
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(16)
        self.port = self._sock.getsockname()[1]
        self.accepted = 0
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            self.accepted += 1
            conn.close()

    def close(self) -> None:
        self._sock.close()


def _admin_graphql(tier, sandbox: dict, query: str) -> httpx.Response:
    return httpx.post(
        f"{tier.base_url}/admin/graphql",
        json={"query": query},
        headers={"Authorization": f"Bearer {sandbox['token']}", "x-org-provisa": sandbox["org_id"]},
        timeout=120,
    )


def test_a_sandbox_source_on_a_private_address_is_refused_and_never_reached(tier, alpha):
    """A visitor's source pointed at this host's loopback is refused by name when it is
    registered, before the connection that would validate it is opened: nothing reaches the port.
    (Before the check, the registration's own validation opened four connections to it.)"""
    listener = _Listener()
    try:
        created = _admin_graphql(
            tier,
            alpha,
            'mutation { createSource(input: {id: "probe", type: "postgresql", host: "127.0.0.1", '
            f'port: {listener.port}, database: "x", username: "u", password: "p"}}) '
            "{ success message code } }",
        )
        assert created.status_code == 200, created.text
        result = created.json()["data"]["createSource"]
        assert result["success"] is False and result["code"] == "sources.private_destination"
        time.sleep(2)
        assert listener.accepted == 0
    finally:
        listener.close()


_NAMED_USER, _NAMED_PASSWORD = "trier", "trier-password-0123"


def _seat_named_user() -> None:
    import os

    import bcrypt
    from sqlalchemy import create_engine, insert, select

    from provisa.core.schema_admin import local_users

    engine = create_engine(os.environ["PLATFORM_DATABASE_URL"])
    try:
        with engine.begin() as conn:
            if conn.execute(
                select(local_users.c.id).where(local_users.c.id == _NAMED_USER)
            ).fetchone():
                return
            conn.execute(
                insert(local_users).values(
                    id=_NAMED_USER,
                    username=_NAMED_USER,
                    password_hash=bcrypt.hashpw(
                        _NAMED_PASSWORD.encode(), bcrypt.gensalt()
                    ).decode(),
                    email="trier@example.com",
                    display_name="Trier",
                    is_active=True,
                )
            )
    finally:
        engine.dispose()


def _open_named(tier, name: str) -> dict:
    """The named sandbox of ``name``, once its template is ready."""
    deadline = time.monotonic() + 180
    while True:
        resp = httpx.post(
            f"{tier.base_url}/sandboxes/{name}", auth=(_NAMED_USER, _NAMED_PASSWORD), timeout=240
        )
        if resp.status_code != 503 or resp.json().get("code") != "sandbox.demo_not_ready":
            assert resp.status_code == 200, (resp.text, _last_traceback(tier))
            return resp.json()
        assert time.monotonic() < deadline, tier.dump_stderr_debug()[-6000:]
        time.sleep(2)


def test_a_named_sandbox_is_the_persons_own_from_every_device(tier):
    """A signed-in person opens a sandbox of a demo; asked again -- from another device -- they
    get the same one."""
    _seat_named_user()
    auth = (_NAMED_USER, _NAMED_PASSWORD)
    first = _open_named(tier, "alpha")
    again = _open_named(tier, "alpha")
    assert again["org_id"] == first["org_id"]
    named = {"org_id": first["org_id"]}
    rows = httpx.post(
        f"{tier.base_url}/data/sql",
        json={"sql": "SELECT name FROM animals"},
        auth=auth,
        headers={"x-org-provisa": named["org_id"]},
        timeout=120,
    )
    assert _names(rows) == sorted(_ROWS["alpha"])


def test_two_devices_writing_at_once_one_succeeds_and_the_other_is_refused_by_name(tier):
    """Two devices of one named sandbox create the same source at the same moment: one is
    created, the other is refused by name, and the model holds that one source, whole. (A
    write-write conflict DuckDB itself raises is refused by name too,
    tests/unit/test_duckdb_write_conflict.py.)"""
    import concurrent.futures

    _seat_named_user()
    auth = (_NAMED_USER, _NAMED_PASSWORD)
    org = _open_named(tier, "alpha")["org_id"]
    uploaded = httpx.put(
        f"{tier.base_url}/admin/sources/twice/file",
        params={"name": "t.csv", "type": "csv"},
        content=b"id,name\n1,owl\n",
        auth=auth,
        headers={"x-org-provisa": org},
        timeout=120,
    )
    assert uploaded.status_code == 200, uploaded.text
    mutation = (
        'mutation { createSource(input: {id: "twice", type: "csv", host: "", port: 0, '
        f'database: "", path: "{uploaded.json()["path"]}"}}) {{ success message code }} }}'
    )

    def register() -> dict:
        resp = httpx.post(
            f"{tier.base_url}/admin/graphql",
            json={"query": mutation},
            auth=auth,
            headers={"x-org-provisa": org},
            timeout=120,
        )
        assert resp.status_code == 200, resp.text
        return resp.json()["data"]["createSource"]

    with concurrent.futures.ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda _: register(), range(2)))
    assert sorted(r["success"] for r in results) == [False, True], results
    refused = next(r for r in results if not r["success"])
    assert refused["code"] == "schema.source_id_taken", refused
    listed = httpx.post(
        f"{tier.base_url}/admin/graphql",
        json={"query": "{ sources { id type } }"},
        auth=auth,
        headers={"x-org-provisa": org},
        timeout=120,
    )
    assert listed.status_code == 200, listed.text
    twice = [s for s in listed.json()["data"]["sources"] if s["id"] == "twice"]
    assert twice == [{"id": "twice", "type": "csv"}]


def test_the_deployments_own_api_refuses_a_sandbox_by_name(tier, alpha, tmp_path_factory):
    """A request for a sandbox that reaches the deployment's ordinary API -- a process that is not
    the sandbox tier -- is refused by name: that process never opens the sandbox's file."""
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp("ordinary_tier")
    config = work / "config.yaml"
    _deployment_config(config)
    ordinary = IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(config),
        auth={"provider": "basic", "jwt_secret": _JWT_SECRET, "assignments_source": "provisa"},
        env={"PROVISA_DATA_DIR": str(work / "data")},
    )
    ordinary.start(timeout=240)
    try:
        refused = _sql(ordinary, alpha, "SELECT name FROM animals")
        assert refused.status_code == 421, (refused.text, _last_traceback(ordinary))
        assert refused.json()["code"] == "sandbox.wrong_tier"
        opened = httpx.post(f"{ordinary.base_url}/public/demos/alpha", timeout=60)
        assert opened.status_code == 421, opened.text
    finally:
        ordinary.stop_process()


def test_a_visitor_lands_a_source_of_their_own_within_the_caps(tier, alpha):
    """A visitor uploads a file, registers it as a source and its table replicated always, and
    reads it: the replica is landed into the sandbox's own store, within its disk cap."""
    headers = {"Authorization": f"Bearer {alpha['token']}", "x-org-provisa": alpha["org_id"]}
    import tempfile

    with tempfile.TemporaryDirectory() as scratch:
        db = sqlite3.connect(Path(scratch) / "birds.sqlite")
        db.execute("CREATE TABLE birds (id INTEGER PRIMARY KEY, name TEXT)")
        db.executemany("INSERT INTO birds VALUES (?, ?)", [(1, "owl"), (2, "wren")])
        db.commit()
        db.close()
        content = (Path(scratch) / "birds.sqlite").read_bytes()
    uploaded = httpx.put(
        f"{tier.base_url}/admin/sources/birds/file",
        params={"name": "birds.sqlite", "type": "sqlite"},
        content=content,
        headers=headers,
        timeout=120,
    )
    assert uploaded.status_code == 200, uploaded.text
    created = _admin_graphql(
        tier,
        alpha,
        'mutation { createSource(input: {id: "birds", type: "sqlite", host: "", port: 0, '
        f'database: "", path: "{uploaded.json()["path"]}", cacheTtl: 3600}}) '
        "{ success message code } }",
    )
    assert created.json()["data"]["createSource"]["success"], created.text
    columns = ", ".join(
        f'{{name: "{n}", visibleTo: ["sandbox"], dataType: "{t}"}}'
        for n, t in (("id", "integer"), ("name", "varchar"))
    )
    registered = _admin_graphql(
        tier,
        alpha,
        'mutation { registerTable(input: {sourceId: "birds", domainId: "zoo", '
        f'schemaName: "main", tableName: "birds", columns: [{columns}]}}) '
        "{ success message } }",
    )
    assert (registered.json().get("data") or {}).get("registerTable", {}).get("success"), (
        registered.text
    )
    from tests.helpers import registered_id, release_field

    _admin_graphql(
        tier,
        alpha,
        "mutation { "
        + release_field(registered_id(registered.json()["data"]["registerTable"]["message"]))
        + " { success message } }",
    )
    # Registered, the source is read from its replica from now on: always (REQ-826).
    replicated = _admin_graphql(
        tier,
        alpha,
        'mutation { updateSource(input: {id: "birds", type: "sqlite", host: "", port: 0, '
        f'database: "", path: "{uploaded.json()["path"]}", replicate: 0, cacheTtl: 3600}}) '
        "{ success message code } }",
    )
    assert (replicated.json().get("data") or {}).get("updateSource", {}).get("success"), (
        replicated.text
    )
    deadline = time.monotonic() + 120
    while True:
        read = _sql(tier, alpha, "SELECT name FROM birds")
        if read.status_code == 200:
            break
        assert time.monotonic() < deadline, (read.text, _last_traceback(tier))
        time.sleep(2)
    assert _names(read) == ["owl", "wren"]
    store = tier.data_dir / "orgs" / alpha["org_id"] / "materialize.duckdb"
    assert store.stat().st_size < _DISK
    import duckdb

    con = duckdb.connect(str(store), read_only=True)
    try:
        landed = con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE schema_name = ?",
            [f"org_{alpha['org_id']}_replicas"],
        ).fetchall()
    finally:
        con.close()
    assert any("birds" in name for (name,) in landed), landed
