# Copyright (c) 2026 Kenneth Stott
# Canary: 7e5be274-f099-4f3b-80d3-d158269a20b3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The sandbox tier node's shared demo stores (scripts/demo_stores.py, REQ-2049): each demo
version's data is loaded into one instance of its store type under names of its own, and dropped
by them, leaving every other demo's and version's untouched."""

# Requirements: REQ-2049

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[2]


def _module():
    spec = importlib.util.spec_from_file_location(
        "demo_stores", _REPO / "scripts" / "demo_stores.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # its dataclasses look their module up
    spec.loader.exec_module(module)
    return module


ds = _module()
SUFFIX = ds.namespace("weblogs", "202610200000-abc1234")


class FakeRunner:
    """Records each command (and what it was given on standard input); answers by the first
    ``answers`` entry whose text is in the command line."""

    def __init__(self, answers: dict[str, str] | None = None, fail: set[str] | None = None) -> None:
        self.calls: list[tuple[str, str | None]] = []
        self.answers = answers or {}
        self.fail = dict.fromkeys(fail or (), 1)

    def run(self, cmd, *, stdin=None):
        line = " ".join(cmd)
        given = stdin.read_text() if isinstance(stdin, Path) else stdin
        self.calls.append((line, given))
        for text in list(self.fail):
            if text in line and self.fail[text] > 0:
                self.fail[text] -= 1
                raise ds.StoreError(f"failed: {line}")
        for text, answer in self.answers.items():
            if text in line:
                return answer
        return ""

    def lines(self) -> list[str]:
        return [line for line, _ in self.calls]


def _shared(runner: FakeRunner):
    return ds.Shared(
        runner,
        "provisa-demo-stores-t",
        "/shared.yml",
        {
            "DEMO_BUNDLES_DIR": "/b",
            "DEMO_STORES_QDRANT_KEY": "k",
            "DEMO_STORES_KAFKA_PORT": "29092",
        },
    )


def _pg_store(**labels):
    return ds.Store(
        "postgres",
        "postgres",
        {
            "com.provisa.demo.role": "rdb",
            "com.provisa.demo.host_env": "WEBLOGS_PG_HOST",
            "com.provisa.demo.port_env": "WEBLOGS_PG_PORT",
            "com.provisa.demo.ns_env": "WEBLOGS_PG_NS",
            **labels,
        },
        {"POSTGRES_DB": "shop"},
    )


# -- names ------------------------------------------------------------------------------------------


def test_a_demo_version_has_one_suffix_its_other_versions_do_not_share():
    assert SUFFIX == ds.namespace("weblogs", "202610200000-abc1234")
    assert SUFFIX.startswith("_weblogs_") and len(SUFFIX) == len("_weblogs_") + 8
    assert SUFFIX != ds.namespace("weblogs", "202610210000-def5678")
    assert SUFFIX != ds.namespace("retail", "202610200000-abc1234")


def test_a_demo_name_that_cannot_be_part_of_a_store_name_is_refused():
    with pytest.raises(ds.StoreError, match="lower-case"):
        ds.namespace("Web-Logs", "1")


def test_postgres_mongo_elasticsearch_qdrant_and_kafka_stores_are_shared_and_the_rest_are_the_demos_own():
    config = {
        "services": {
            "postgres": {"image": "postgres:16", "labels": _pg_store().labels},
            "mongodb": {
                "image": "mongo:7",
                "labels": {"com.provisa.demo.role": "document", "com.provisa.demo.ns_env": "R_NS"},
            },
            "elasticsearch": {
                "image": "docker.elastic.co/elasticsearch/elasticsearch:8.13.0",
                "labels": {"com.provisa.demo.role": "search", "com.provisa.demo.ns_env": "E_NS"},
            },
            "neo4j": {
                "image": "neo4j:5.19-community",
                "labels": {"com.provisa.demo.role": "graph"},
            },
            "qdrant": {
                "image": "qdrant/qdrant:v1.9.0",
                "labels": {"com.provisa.demo.role": "vector", "com.provisa.demo.ns_env": "Q_NS"},
            },
            "kafka": {
                "image": "confluentinc/cp-kafka:7.6.0",
                "labels": {"com.provisa.demo.role": "stream", "com.provisa.demo.ns_env": "K_NS"},
            },
        }
    }
    kinds = {s.service: s.kind for s in ds.stores_of(config)}
    assert kinds == {
        "elasticsearch": "elasticsearch",
        "kafka": "kafka",
        "mongodb": "mongo",
        "neo4j": None,
        "postgres": "postgres",
        "qdrant": "qdrant",
    }


def test_a_shared_store_whose_data_names_no_variable_is_refused():
    config = {
        "services": {
            "postgres": {"image": "postgres:16", "labels": {"com.provisa.demo.role": "rdb"}}
        }
    }
    with pytest.raises(ds.StoreError, match="ns_env"):
        ds.stores_of(config)


# -- Postgres ---------------------------------------------------------------------------------------


def test_a_demos_access_script_creates_its_roles_once_and_grants_in_its_own_databases():
    sql = (
        "CREATE ROLE provisa_reader LOGIN PASSWORD 'provisa_reader';\n"
        "\\connect eu_shop\n"
        "GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;\n"
        "\\connect us_shop\n"
        "GRANT USAGE ON SCHEMA public TO provisa_reader;\n"
    )
    roles, grants = ds.pg_access(sql, "_retail_00000000", ["eu_shop", "us_shop"])
    assert "IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'provisa_reader')" in roles
    assert "CREATE ROLE provisa_reader LOGIN PASSWORD 'provisa_reader'" in roles
    assert "CREATE ROLE" not in grants
    assert "\\connect eu_shop_retail_00000000\n" in grants
    assert "\\connect us_shop_retail_00000000\n" in grants
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA public TO provisa_reader;" in grants


def test_an_access_script_connecting_to_a_database_the_demo_does_not_dump_is_refused():
    with pytest.raises(ds.StoreError, match="other"):
        ds.pg_access("\\connect other\n", SUFFIX, ["shop"])


def _bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "weblogs" / "202610200000-abc1234"
    (bundle / "dump" / "postgres").mkdir(parents=True)
    (bundle / "dump" / "postgres" / "shop.sql").write_text("CREATE TABLE shop.orders (id int);\n")
    (bundle / "db").mkdir()
    (bundle / "db" / "reader.sql").write_text(
        "CREATE ROLE provisa_reader LOGIN PASSWORD 'provisa_reader';\n"
        "GRANT USAGE ON SCHEMA shop TO provisa_reader;\n"
    )
    return bundle


def test_each_dumped_database_loads_as_its_namespaced_copy_and_is_marked_loaded_last(tmp_path):
    runner = FakeRunner({"shobj_description": "0"})
    bundle = _bundle(tmp_path)
    ds.pg_load(
        _shared(runner), bundle, _pg_store(**{"com.provisa.demo.access": "db/reader.sql"}), SUFFIX
    )
    target = f"shop{SUFFIX}"
    lines = runner.lines()
    drop = next(i for i, line in enumerate(lines) if f'DROP DATABASE IF EXISTS "{target}"' in line)
    create = next(i for i, line in enumerate(lines) if f'CREATE DATABASE "{target}"' in line)
    load = next(
        i for i, (line, given) in enumerate(runner.calls) if given and "shop.orders" in given
    )
    assert f"-d {target}" in lines[load]
    roles = next(i for i, (line, given) in enumerate(runner.calls) if given and "pg_roles" in given)
    assert "-d postgres" in lines[roles]
    grants = next(
        i for i, (line, given) in enumerate(runner.calls) if given and "GRANT USAGE" in given
    )
    assert f"-d {target}" in lines[grants]
    mark = next(i for i, line in enumerate(lines) if f'COMMENT ON DATABASE "{target}"' in line)
    assert drop < create < load < roles < grants < mark == len(lines) - 1
    assert all("exec -T postgres" in line for line in lines)


def test_a_loaded_database_is_not_loaded_again(tmp_path):
    runner = FakeRunner({"shobj_description": "1"})
    ds.pg_load(_shared(runner), _bundle(tmp_path), _pg_store(), SUFFIX)
    assert not any("CREATE DATABASE" in line for line in runner.lines())


def test_a_postgres_store_with_no_dump_a_database_is_refused(tmp_path):
    bundle = tmp_path / "b"
    bundle.mkdir()
    with pytest.raises(ds.StoreError, match="dump/postgres/<database>.sql"):
        ds.pg_load(_shared(FakeRunner({"shobj_description": "0"})), bundle, _pg_store(), SUFFIX)


def test_dropping_a_version_drops_its_databases_and_no_other():
    other = ds.namespace("weblogs", "202610210000-def5678")
    listed = f"postgres\nshop{SUFFIX}\nshop{other}\nsales_quality_12345678\n"
    runner = FakeRunner({"NOT datistemplate": listed})
    assert ds.pg_drop(_shared(runner), SUFFIX) == [f"shop{SUFFIX}"]
    drops = [line for line in runner.lines() if "DROP DATABASE" in line]
    assert len(drops) == 1 and f"shop{SUFFIX}" in drops[0]


# -- MongoDB ----------------------------------------------------------------------------------------


def test_a_demos_mongo_reader_is_created_or_granted_its_versions_databases():
    script = (
        'db.getSiblingDB("admin").createUser({\n'
        '  user: "storefront_reader",\n'
        '  pwd: "storefront_reader",\n'
        '  roles: [{ role: "read", db: "catalog" }, { role: "read", db: "admin" }],\n'
        "});\n"
    )
    out = ds.mongo_access(script, "_storefront_00000000", ["catalog"])
    assert "function provisaUser(spec)" in out and "grantRolesToUser" in out
    assert "provisaUser({" in out and "createUser({" not in out.split("}\n", 4)[-1]
    assert 'db: "catalog_storefront_00000000"' in out
    assert 'db: "admin"' in out


def test_a_mongo_reader_script_that_creates_no_user_is_refused():
    with pytest.raises(ds.StoreError, match="createUser"):
        ds.mongo_access("print(1)", SUFFIX, [])


def test_a_mongo_archive_restores_each_database_under_its_namespaced_name(tmp_path):
    bundle = tmp_path / "reviews" / "v1"
    (bundle / "dump").mkdir(parents=True)
    (bundle / "dump" / "mongodb.archive.gz").write_bytes(b"archive")
    (bundle / "db").mkdir()
    (bundle / "db" / "mongo-reader.js").write_text(
        'db.getSiblingDB("admin").createUser({user: "reviews_reader", pwd: "p", roles: []});\n'
    )
    suffix = ds.namespace("reviews", "v1")
    runner = FakeRunner({"countDocuments": "0", "listDatabases": f"admin\ncatalog{suffix}\n"})
    store = ds.Store(
        "mongodb",
        "mongo",
        {
            "com.provisa.demo.ns_env": "REVIEWS_MONGO_NS",
            "com.provisa.demo.access": "db/mongo-reader.js",
        },
        {},
    )
    ds.mongo_load(_shared(runner), bundle, store, suffix)
    restore = next(line for line in runner.lines() if "mongorestore" in line)
    assert "--nsFrom=$db$.$col$" in restore and f"--nsTo=$db${suffix}.$col$" in restore
    assert "--nsExclude=admin.*" in restore
    assert any(given == "archive" for _, given in runner.calls)
    assert any("provisaUser" in line for line in runner.lines())
    assert "replaceOne" in runner.lines()[-1]


def test_a_loaded_mongo_namespace_is_not_restored_again(tmp_path):
    runner = FakeRunner({"countDocuments": "1"})
    store = ds.Store("mongodb", "mongo", {"com.provisa.demo.ns_env": "R_NS"}, {})
    ds.mongo_load(_shared(runner), tmp_path, store, SUFFIX)
    assert not any("mongorestore" in line for line in runner.lines())


def test_dropping_a_version_drops_its_mongo_databases_and_no_other():
    runner = FakeRunner({"listDatabases": f"admin\ncatalog{SUFFIX}\ncatalog_storefront_1234abcd\n"})
    assert ds.mongo_drop(_shared(runner), SUFFIX) == [f"catalog{SUFFIX}"]
    dropped = [line for line in runner.lines() if "dropDatabase" in line]
    assert len(dropped) == 1 and f"catalog{SUFFIX}" in dropped[0]


# -- Elasticsearch ----------------------------------------------------------------------------------


def _bodies(runner: FakeRunner) -> list[tuple[str, dict]]:
    found = []
    for line, _ in runner.calls:
        if " -d {" in line:
            body = line.split(" -d ", 1)[1].rsplit(" http", 1)[0]
            found.append((line, json.loads(body)))
    return found


def test_an_index_snapshot_restores_each_index_under_its_namespaced_name():
    suffix = ds.namespace("reviews", "v1")
    runner = FakeRunner({"-w %{http_code}": "404"})
    store = ds.Store(
        "elasticsearch", "elasticsearch", {"com.provisa.demo.ns_env": "REVIEWS_ES_NS"}, {}
    )
    ds.es_load(_shared(runner), "reviews", "v1", store, suffix)
    bodies = _bodies(runner)
    repository = next(b for line, b in bodies if "/_snapshot/" in line and b.get("type") == "fs")
    assert repository["settings"] == {
        "location": "/bundles/reviews/v1/dump/elasticsearch",
        "readonly": True,
    }
    restore = next(b for line, b in bodies if "_restore" in line)
    assert restore["rename_pattern"] == "(.+)" and restore["rename_replacement"] == f"$1{suffix}"
    assert restore["include_global_state"] is False
    assert any(f"/*{suffix}/_settings" in line for line, _ in bodies)
    assert "provisa_demo_marks/_doc" in runner.lines()[-1]


def test_a_loaded_index_namespace_is_not_restored_again():
    runner = FakeRunner({"-w %{http_code}": "200"})
    store = ds.Store("elasticsearch", "elasticsearch", {"com.provisa.demo.ns_env": "E_NS"}, {})
    ds.es_load(_shared(runner), "reviews", "v1", store, SUFFIX)
    assert not any("_restore" in line for line in runner.lines())


def test_dropping_a_version_drops_its_indices_by_their_suffix():
    runner = FakeRunner()
    ds.es_drop(_shared(runner), SUFFIX)
    assert any("DELETE" in line and f"/*{SUFFIX}?" in line for line in runner.lines())


# -- Qdrant ----------------------------------------------------------------------------------------


def test_a_demo_versions_qdrant_token_reads_its_own_collections_alone_signed_with_the_nodes_key():
    import base64
    import hashlib
    import hmac

    token = ds.qdrant_token("node-key", ["sonnet_lines_sonnets_00000000"])
    header, body, signature = token.split(".")

    def _decoded(part: str) -> dict:
        return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))

    assert _decoded(header) == {"alg": "HS256", "typ": "JWT"}
    assert _decoded(body) == {
        "access": [{"collection": "sonnet_lines_sonnets_00000000", "access": "r"}]
    }
    expected = hmac.new(b"node-key", f"{header}.{body}".encode(), hashlib.sha256).digest()
    assert base64.urlsafe_b64decode(signature + "=" * (-len(signature) % 4)) == expected


def _qdrant_bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "sonnets" / "v1"
    (bundle / "dump" / "qdrant").mkdir(parents=True)
    (bundle / "dump" / "qdrant" / "sonnet_lines.snapshot").write_bytes(b"snapshot")
    return bundle


_QDRANT = ds.Store("qdrant", "qdrant", {"com.provisa.demo.ns_env": "SONNETS_QDRANT_NS"}, {})


def test_each_snapshot_recovers_as_its_namespaced_collection(tmp_path):
    bundle = _qdrant_bundle(tmp_path)
    suffix = ds.namespace("sonnets", "v1")
    runner = FakeRunner({"port qdrant 6333": "127.0.0.1:46333", "%{http_code}": "404"})
    assert ds.qdrant_load(_shared(runner), bundle, _QDRANT, suffix, "k") == [
        f"sonnet_lines{suffix}"
    ]
    upload = next(line for line in runner.lines() if "snapshots/upload" in line)
    assert "-H api-key: k" in upload
    assert f"snapshot=@{bundle / 'dump' / 'qdrant' / 'sonnet_lines.snapshot'}" in upload
    assert f"http://127.0.0.1:46333/collections/sonnet_lines{suffix}/snapshots/upload" in upload


def test_a_recovered_collection_is_not_recovered_again(tmp_path):
    runner = FakeRunner({"port qdrant 6333": "127.0.0.1:46333", "%{http_code}": "200"})
    ds.qdrant_load(_shared(runner), _qdrant_bundle(tmp_path), _QDRANT, SUFFIX, "k")
    assert not any("upload" in line for line in runner.lines())


def test_dropping_a_version_drops_its_collections_and_no_other():
    other = "sonnet_lines_sonnets_ffffffff"
    listed = json.dumps(
        {"result": {"collections": [{"name": f"sonnet_lines{SUFFIX}"}, {"name": other}]}}
    )
    runner = FakeRunner(
        {"port qdrant 6333": "127.0.0.1:46333", "/collections/": "", "/collections": listed}
    )
    assert ds.qdrant_drop(_shared(runner), SUFFIX, "k") == [f"sonnet_lines{SUFFIX}"]
    deletes = [line for line in runner.lines() if "-X DELETE" in line]
    assert len(deletes) == 1 and deletes[0].endswith(f"/collections/sonnet_lines{SUFFIX}")


def test_the_nodes_qdrant_key_is_made_once_and_kept_private(tmp_path):
    key = ds.qdrant_key(tmp_path)
    assert len(key) == 64 and ds.qdrant_key(tmp_path) == key
    assert (tmp_path / ".demo-stores" / "qdrant.key").stat().st_mode & 0o077 == 0


# -- Kafka -----------------------------------------------------------------------------------------


def test_dropping_a_version_deletes_its_topics_by_their_suffix():
    runner = FakeRunner()
    ds.kafka_drop(_shared(runner), SUFFIX)
    (line,) = runner.lines()
    assert (
        "exec -T kafka kafka-topics --bootstrap-server localhost:9092 --delete --if-exists" in line
    )
    assert line.endswith(f"--topic .*{SUFFIX}")


def test_the_shared_kafka_is_reached_at_its_one_port_by_name_or_on_the_loopback():
    runner = FakeRunner({"ps -q kafka": "kcid", "{{.Name}}": "/provisa-demo-stores-t-kafka-1"})
    shared = _shared(runner)
    assert shared.address("kafka", "") == ("127.0.0.1", "29092")
    assert shared.address("kafka", "compose_default") == ("provisa-demo-stores-t-kafka-1", "29092")


# -- a demo version ---------------------------------------------------------------------------------


def _demo(monkeypatch, tmp_path, runner, config):
    monkeypatch.setenv("DEMO_STORES_KAFKA_PORT", "29092")
    monkeypatch.setenv("DEMO_STORES_PROJECT", "provisa-demo-stores-t")
    monkeypatch.setenv("DEMO_STORES_SHARED", "/shared.yml")
    monkeypatch.setenv("DEMO_BUNDLES_DIR", str(tmp_path))
    monkeypatch.setenv("DEMO_STORES_NETWORK", "")
    runner.answers.setdefault("config --format json", json.dumps(config))
    return ds.Demo(runner, "reviews", "v1", _bundle(tmp_path))


_REVIEWS = {
    "services": {
        "postgres": {
            "image": "postgres:16",
            "labels": _pg_store().labels,
            "environment": {"POSTGRES_DB": "shop"},
        },
        "neo4j": {
            "image": "neo4j:5.19-community",
            "labels": {
                "com.provisa.demo.role": "graph",
                "com.provisa.demo.host_env": "REVIEWS_NEO4J_HOST",
                "com.provisa.demo.port_env": "REVIEWS_NEO4J_HTTP_PORT",
                "com.provisa.demo.port": "7474",
            },
        },
    }
}


def test_a_version_brought_up_says_where_each_store_is_and_the_names_of_its_shared_data(
    monkeypatch, tmp_path
):
    runner = FakeRunner(
        {
            "shobj_description": "1",
            "ps -q postgres": "pgcid",
            "port postgres 5432": "127.0.0.1:41000",
            "ps -q neo4j": "neocid",
            "port neo4j 7474": "127.0.0.1:42000",
            " ps -q": "neocid",
        }
    )
    found = _demo(monkeypatch, tmp_path, runner, _REVIEWS).up()
    suffix = ds.namespace("reviews", "v1")
    assert found == {
        "WEBLOGS_PG_HOST": "127.0.0.1",
        "WEBLOGS_PG_PORT": "41000",
        "WEBLOGS_PG_NS": suffix,
        "REVIEWS_NEO4J_HOST": "127.0.0.1",
        "REVIEWS_NEO4J_HTTP_PORT": "42000",
    }
    lines = runner.lines()
    assert any(
        "-p provisa-demo-stores-t -f /shared.yml up -d --wait postgres" in line for line in lines
    )
    own = next(
        line for line in lines if "provisa-demo-reviews-v1" in line and " up -d --wait" in line
    )
    # Its own services alone, on ports docker picks; the shared Postgres is not started for it.
    assert own.endswith("--no-deps neo4j") and "REVIEWS_NEO4J_HTTP_PORT= " in own
    assert any(line.startswith("docker update --restart no") for line in lines)


def test_an_own_store_that_never_turns_healthy_has_its_volumes_recreated_once(
    monkeypatch, tmp_path
):
    runner = FakeRunner(
        {"shobj_description": "1", " ps -q": "cid", " port ": "127.0.0.1:42000"},
        fail={"--no-deps neo4j"},
    )
    _demo(monkeypatch, tmp_path, runner, _REVIEWS).up()
    own = [
        line
        for line in runner.lines()
        if "provisa-demo-reviews-v1" in line and (" up " in line or " down " in line)
    ]
    assert [line.split(" -f ")[1].split(" ", 1)[1].split(" --no-deps")[0] for line in own] == [
        "up -d --wait",
        "down -v --remove-orphans",
        "up -d --wait",
    ]


def test_dropping_a_version_drops_its_data_from_the_shared_stores_running_and_its_own_services(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("DEMO_STORES_KAFKA_PORT", "29092")
    monkeypatch.setenv("DEMO_STORES_PROJECT", "provisa-demo-stores-t")
    monkeypatch.setenv("DEMO_STORES_SHARED", "/shared.yml")
    monkeypatch.setenv("DEMO_BUNDLES_DIR", str(tmp_path))
    suffix = ds.namespace("reviews", "v1")
    runner = FakeRunner({"NOT datistemplate": f"shop{suffix}\n"})
    ds.Demo(runner, "reviews", "v1", None).drop({"postgres"})
    lines = runner.lines()
    assert any(f'DROP DATABASE IF EXISTS "shop{suffix}"' in line for line in lines)
    assert not any("mongosh" in line or "elasticsearch" in line for line in lines)
    assert lines[-1] == "docker compose -p provisa-demo-reviews-v1 down -v --remove-orphans"


def test_dropping_a_version_removes_the_images_its_services_ran_that_no_container_still_uses(
    monkeypatch, tmp_path
):
    """Every version pulled leaves its images on the node's disk (cloud-dev's sandbox VM at 97%,
    its shared Elasticsearch red): those no other container uses go, and only those."""
    monkeypatch.setenv("DEMO_STORES_KAFKA_PORT", "29092")
    monkeypatch.setenv("DEMO_STORES_PROJECT", "provisa-demo-stores-t")
    monkeypatch.setenv("DEMO_STORES_SHARED", "/shared.yml")
    monkeypatch.setenv("DEMO_BUNDLES_DIR", str(tmp_path))
    runner = FakeRunner(
        {
            "{{.Image}}": "neo4j:5.19-community\nreviews-api:1\nreviews-api:1\n",
            "ancestor=neo4j:5.19-community": "newercid\n",
        }
    )
    ds.Demo(runner, "reviews", "v1", None).drop(set())
    lines = runner.lines()
    down = lines.index("docker compose -p provisa-demo-reviews-v1 down -v --remove-orphans")
    listed = next(
        i
        for i, x in enumerate(lines)
        if "label=com.docker.compose.project=provisa-demo-reviews-v1" in x
    )
    assert listed < down
    assert lines[down + 1 :] == [
        "docker ps -a -q --filter ancestor=neo4j:5.19-community",
        "docker ps -a -q --filter ancestor=reviews-api:1",
        "docker image rm reviews-api:1",
    ]
    assert not any(" prune" in x for x in lines)


def test_stopping_a_lingered_out_version_keeps_the_images_its_services_ran(monkeypatch, tmp_path):
    """A demo whose stores lingered out is brought back at its next open: its data and its own
    services go, its images stay, so it comes back without a pull (REQ-2049)."""
    monkeypatch.setenv("DEMO_STORES_KAFKA_PORT", "29092")
    monkeypatch.setenv("DEMO_STORES_PROJECT", "provisa-demo-stores-t")
    monkeypatch.setenv("DEMO_STORES_SHARED", "/shared.yml")
    monkeypatch.setenv("DEMO_BUNDLES_DIR", str(tmp_path))
    suffix = ds.namespace("reviews", "v1")
    runner = FakeRunner({"NOT datistemplate": f"shop{suffix}\n", "{{.Image}}": "reviews-api:1\n"})
    ds.Demo(runner, "reviews", "v1", None).drop({"postgres"}, keep_images=True)
    lines = runner.lines()
    assert any(f'DROP DATABASE IF EXISTS "shop{suffix}"' in line for line in lines)
    assert lines[-1] == "docker compose -p provisa-demo-reviews-v1 down -v --remove-orphans"
    assert not any("image rm" in line or "ancestor=" in line for line in lines)


_STREAMING = {
    "services": {
        "kafka": {
            "image": "confluentinc/cp-kafka:7.6.0",
            "labels": {
                "com.provisa.demo.role": "stream",
                "com.provisa.demo.host_env": "STREAMING_KAFKA_HOST",
                "com.provisa.demo.port_env": "STREAMING_KAFKA_PORT",
                "com.provisa.demo.ns_env": "STREAMING_KAFKA_NS",
            },
        },
        "producer": {
            "image": "provisa-demo-streaming-producer:2",
            "labels": {"com.provisa.demo.role": "producer"},
        },
    }
}


def test_a_demos_producer_writes_its_versions_topics_to_the_shared_kafka(monkeypatch, tmp_path):
    """Created, joined to the shared stores' network, then started: a producer started before it
    joined could not resolve "kafka", exited and was restarted, and connecting a container
    between restarts failed ("network sandbox ... not found", the streaming and provisioning
    walks)."""
    runner = FakeRunner(
        {
            "ps -q kafka": "kcid",
            "ps -a -q producer": "pcid",
            "ps -q producer": "pcid",
            " ps -q": "pcid",
        }
    )
    demo = _demo(monkeypatch, tmp_path, runner, _STREAMING)
    found = demo.up()
    suffix = ds.namespace("reviews", "v1")
    assert found == {
        "STREAMING_KAFKA_HOST": "127.0.0.1",
        "STREAMING_KAFKA_PORT": "29092",
        "STREAMING_KAFKA_NS": suffix,
    }
    lines = runner.lines()
    assert not any("up -d --no-deps producer" in line for line in lines)  # never started unjoined
    started = next(i for i, line in enumerate(lines) if "up --no-start --no-deps producer" in line)
    joined = next(
        i
        for i, line in enumerate(lines)
        if "network connect provisa-demo-stores-t_default pcid" in line
    )
    waited = next(i for i, line in enumerate(lines) if "up -d --wait --no-deps producer" in line)
    assert started < joined < waited
    # The producer's topics are named with the suffix (its compose file's TOPIC_SUFFIX).
    assert f"STREAMING_KAFKA_NS={suffix} " in lines[waited]


_SONNETS = {
    "services": {
        "qdrant": {
            "image": "qdrant/qdrant:v1.19.1",
            "labels": {
                "com.provisa.demo.role": "vector",
                "com.provisa.demo.host_env": "SONNETS_QDRANT_HOST",
                "com.provisa.demo.port_env": "SONNETS_QDRANT_PORT",
                "com.provisa.demo.ns_env": "SONNETS_QDRANT_NS",
                "com.provisa.demo.key_env": "SONNETS_QDRANT_KEY",
            },
        }
    }
}


def test_a_demo_version_reads_the_shared_qdrant_with_a_token_of_its_own(monkeypatch, tmp_path):
    runner = FakeRunner(
        {"port qdrant 6333": "127.0.0.1:46333", "%{http_code}": "200", "ps -q qdrant": "qcid"}
    )
    demo = _demo(monkeypatch, tmp_path, runner, _SONNETS)
    demo.bundle = _qdrant_bundle(tmp_path)
    found = demo.up()
    suffix = ds.namespace("reviews", "v1")
    key = ds.qdrant_key(tmp_path)
    assert found["SONNETS_QDRANT_NS"] == suffix
    assert found["SONNETS_QDRANT_KEY"] == ds.qdrant_token(key, [f"sonnet_lines{suffix}"])
    assert (found["SONNETS_QDRANT_HOST"], found["SONNETS_QDRANT_PORT"]) == ("127.0.0.1", "46333")


_PGVECTOR = {
    "services": {
        "postgres": {
            "image": "pgvector/pgvector:pg16",
            "labels": {
                "com.provisa.demo.role": "rdb",
                "com.provisa.demo.host_env": "CLOUDPOLICY_PG_HOST",
                "com.provisa.demo.port_env": "CLOUDPOLICY_PG_PORT",
                "com.provisa.demo.port": "5432",
                "com.provisa.demo.ns_env": "CLOUDPOLICY_PG_NS",
                "com.provisa.demo.access": "db/reader.sql",
            },
            "environment": {"POSTGRES_DB": "cloudpolicy"},
        },
    }
}


def test_a_postgres_with_pgvector_is_the_demos_own_and_names_its_data_unsuffixed(
    monkeypatch, tmp_path
):
    """No shared instance has pgvector: such a store runs as the demo's own, its database under
    its own name, so the suffix its config names its data with is empty (cloudpolicy)."""
    runner = FakeRunner(
        {"ps -q postgres": "pgcid", "port postgres 5432": "127.0.0.1:43000", " ps -q": "pgcid"}
    )
    found = _demo(monkeypatch, tmp_path, runner, _PGVECTOR).up()
    assert found == {
        "CLOUDPOLICY_PG_HOST": "127.0.0.1",
        "CLOUDPOLICY_PG_PORT": "43000",
        "CLOUDPOLICY_PG_NS": "",
    }
    assert not any("/shared.yml up" in line for line in runner.lines())


# -- every <STEM>_NS is set before any compose command reads it -------------------------------------


def _published_run_files() -> list[Path]:
    import yaml

    return sorted(
        run
        for run in (_REPO / "demo" / "named").glob("*/docker-compose.run.yml")
        if (yaml.safe_load((run.parent / "demo.yaml").read_text()) or {}).get("published") is True
    )


def test_a_version_reads_its_stores_without_interpolating_the_file(monkeypatch, tmp_path):
    """The run file names its data by the <STEM>_NS this reading sets; reading it must not need
    them (visitor walk 39480892669: STREAMING_KAFKA_NS required and unset at `compose config`)."""
    runner = FakeRunner({"ps -q kafka": "kcid", "ps -q producer": "pcid", " ps -q": "pcid"})
    _demo(monkeypatch, tmp_path, runner, _STREAMING).up()
    [config] = [line for line in runner.lines() if " config " in line]
    assert "--no-interpolate" in config


def test_a_store_the_demo_runs_itself_has_its_names_set_before_its_first_start(
    monkeypatch, tmp_path
):
    runner = FakeRunner(
        {"ps -q postgres": "pgcid", "port postgres 5432": "127.0.0.1:43000", " ps -q": "pgcid"}
    )
    _demo(monkeypatch, tmp_path, runner, _PGVECTOR).up()
    first_up = next(line for line in runner.lines() if " up -d" in line)
    assert "CLOUDPOLICY_PG_NS= " in first_up


@pytest.mark.parametrize("run", _published_run_files(), ids=lambda p: p.parent.name)
def test_every_variable_a_published_demo_requires_is_a_name_its_stores_set(run):
    """Every ${VAR?...} a published demo's run file requires is the ns_env of one of its stores:
    set by the sandbox tier's node (Demo.up, shared or own) and by the bundle build and a local
    start (demo_bundles.store_namespaces)."""
    import re

    import yaml

    from provisa.core.demo_bundles import store_namespaces

    stores = ds.stores_of(yaml.safe_load(run.read_text()))
    named = {
        s.labels["com.provisa.demo.ns_env"] for s in stores if "com.provisa.demo.ns_env" in s.labels
    }
    required = set(re.findall(r"\$\{([A-Za-z0-9_]+)\?", run.read_text()))
    assert required <= named, f"{run.parent.name} requires {sorted(required - named)} unset"
    assert set(store_namespaces(run.parent)) == named


def _compose_files_with_kafka() -> list[Path]:
    shared = _REPO / "packaging" / "linux" / "demo-stores" / "docker-compose.shared.yml"
    demos = sorted((_REPO / "demo" / "named").glob("*/docker-compose*.yml"))
    return [f for f in [shared, *demos] if "CLUSTER_ID" in f.read_text()]


@pytest.mark.parametrize(
    "compose", _compose_files_with_kafka(), ids=lambda p: str(p.relative_to(_REPO))
)
def test_every_kafka_cluster_id_is_a_uuid_kraft_can_format(compose):
    """KRaft formats its storage with CLUSTER_ID, which must be a UUID in base64: 16 bytes. The
    shared Kafka's was 21 ("provisa-demo-stores-1"), and the broker exited (1) at its first start
    on the sandbox tier (visitor walk 39485183923); the demos' own brokers' were 16."""
    import base64

    import yaml

    services = yaml.safe_load(compose.read_text())["services"]
    ids = {
        name: str(env["CLUSTER_ID"])
        for name, svc in services.items()
        if isinstance(env := svc.get("environment"), dict) and "CLUSTER_ID" in env
    }
    assert ids, f"{compose} names CLUSTER_ID outside a service's environment mapping"
    for name, cluster_id in ids.items():
        raw = base64.urlsafe_b64decode(cluster_id + "=" * (-len(cluster_id) % 4))
        assert len(raw) == 16, f"{name}: CLUSTER_ID {cluster_id!r} is {len(raw)} bytes, not 16"


def test_a_shared_store_that_will_not_start_says_why_from_its_own_log():
    """A store that exits before it is healthy says why only in its container's log: the error
    the sync prints carries its end (the walk's Kafka exited (1) with no reason shown)."""
    runner = FakeRunner(
        {
            "ps -a -q kafka": "kcid",
            "docker logs --tail": "ERROR the cluster id is not a valid UUID",
        },
        fail={"up -d --wait kafka"},
    )
    with pytest.raises(ds.StoreError) as failed:
        _shared(runner).start("kafka")
    assert "--- docker logs kcid ---" in str(failed.value)
    assert "the cluster id is not a valid UUID" in str(failed.value)


def test_a_superseded_versions_own_kafka_on_the_shared_port_is_taken_down_before_it_starts():
    """A version an earlier release ran with a Kafka of its own keeps the shared Kafka's port
    after a newer version took over: the shared Kafka could not bind it, and no demo naming Kafka
    was ready again (cloud-dev's streaming 202610182058)."""
    old = "provisa-demo-streaming-202610182058-a481c50b2"
    runner = FakeRunner(
        {
            "publish=29092": f"{old} {old}-kafka-1\nprovisa-demo-stores-t provisa-demo-stores-t-kafka-1"
        }
    )
    _shared(runner).start("kafka")
    lines = runner.lines()
    down = f"docker compose -p {old} down -v --remove-orphans"
    assert down in lines
    assert lines.index(down) < next(i for i, x in enumerate(lines) if "up -d --wait kafka" in x)
    assert not any("-p provisa-demo-stores-t down" in x for x in lines)


def test_the_shared_kafkas_port_held_by_no_demo_version_is_refused_by_name():
    runner = FakeRunner({"publish=29092": " some-broker"})
    with pytest.raises(ds.StoreError, match="held by some-broker"):
        _shared(runner).start("kafka")
    assert not any("down" in x or "up -d" in x for x in runner.lines())


@pytest.mark.parametrize("kind", ["postgres", "mongo"])
def test_a_shared_store_is_healthy_only_once_its_real_server_answers_on_its_address(kind):
    """Postgres and MongoDB images first initialize with a server on the socket or loopback
    alone, then restart it. A check there passed before the real server was up: the wait
    returned, and storefront's first mongosh load failed on the sandbox tier. On the sandbox
    node the store sits on two networks, so ``hostname -i`` prints two addresses; the check
    takes the first, or every probe failed ("172.18.0.45 192.168.96.2:5432 - no response")."""
    import yaml

    shared = _REPO / "packaging" / "linux" / "demo-stores" / "docker-compose.shared.yml"
    test = yaml.safe_load(shared.read_text())["services"][kind]["healthcheck"]["test"]
    assert test[0] == "CMD-SHELL", test
    address = "\"$$(hostname -i | cut -d' ' -f1)\""
    assert f"-h {address}" in test[1] or f"--host {address}" in test[1], test
