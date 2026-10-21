# Copyright (c) 2026 Kenneth Stott
# Canary: 69648c50-bdc7-427d-b52c-9d5708a9ebf8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2016, REQ-2049: a demo with a setup.yaml is set up in its bundle build, with the extras it
names installed, and its Qdrant stores are carried as snapshots its run-time compose recovers; a
bundled demo is not set up again where it runs."""

# Requirements: REQ-2016, REQ-2049

from __future__ import annotations

import argparse
import contextlib
import importlib.util
import json
from pathlib import Path

import httpx
import pytest
import yaml

_REPO = Path(__file__).resolve().parents[2]
_SONNETS = _REPO / "demo" / "named" / "sonnets"


def _builder():
    spec = importlib.util.spec_from_file_location(
        "demo_bundle", _REPO / "scripts" / "demo_bundle.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


builder = _builder()


def test_a_qdrant_store_is_saved_as_one_snapshot_a_collection(tmp_path, monkeypatch):
    demo = tmp_path / "sonnets"
    demo.mkdir()
    (demo / "docker-compose.yml").write_text("services: {}\n")
    stores = builder.Stores(demo)
    config = {
        "services": {
            "qdrant": {
                "image": "qdrant/qdrant:v1.12.4",
                "labels": {"com.provisa.demo.role": "vector"},
            }
        }
    }

    def _compose(*args, **_kw):
        if args[0] == "config":
            return type("Done", (), {"stdout": json.dumps(config)})()
        assert args == ("port", "qdrant", "6333")
        return type("Done", (), {"stdout": "0.0.0.0:41234\n"})()

    asked: list[str] = []

    def _get(url, **_kw):
        asked.append(f"GET {url}")
        body = {"result": {"collections": [{"name": "sonnet_lines"}]}}
        return httpx.Response(200, json=body, request=httpx.Request("GET", url))

    def _post(url, **_kw):
        asked.append(f"POST {url}")
        body = {"result": {"name": "sonnet_lines-1.snapshot"}}
        return httpx.Response(200, json=body, request=httpx.Request("POST", url))

    @contextlib.contextmanager
    def _stream(method, url, **_kw):
        asked.append(f"{method} {url}")
        yield httpx.Response(200, content=b"snapshot bytes", request=httpx.Request(method, url))

    monkeypatch.setattr(stores, "_compose", _compose)
    monkeypatch.setattr(builder.httpx, "get", _get)
    monkeypatch.setattr(builder.httpx, "post", _post)
    monkeypatch.setattr(builder.httpx, "stream", _stream)

    assert stores.dump() == ["qdrant/sonnet_lines"]
    assert (demo / "dump" / "qdrant" / "sonnet_lines.snapshot").read_bytes() == b"snapshot bytes"
    assert asked == [
        "GET http://127.0.0.1:41234/collections",
        "POST http://127.0.0.1:41234/collections/sonnet_lines/snapshots",
        "GET http://127.0.0.1:41234/collections/sonnet_lines/snapshots/sonnet_lines-1.snapshot",
    ]


@pytest.mark.parametrize(
    ("has_setup", "replicates", "expected"),
    [
        # sonnets: its sources are read live. A setup's bitemporal views' history is exported after
        # it (REQ-1162).
        (True, False, ["up", "bundles", "set up", "export views", "dump", "down"]),
        (
            True,
            True,
            ["up", "bundles", "set up", "export views", "seed", "no empty replica", "dump", "down"],
        ),
        (False, True, ["up", "bundles", "seed", "no empty replica", "dump", "down"]),
    ],
    ids=["setup-live", "setup-replicated", "no-setup"],
)
def test_a_demo_s_setup_runs_after_its_stores_come_up_and_before_its_seed(
    tmp_path, monkeypatch, has_setup, replicates, expected
):
    named = tmp_path / "named"
    demo = named / "sonnets"
    demo.mkdir(parents=True)
    (demo / "demo.yaml").write_text("published: true\nrequires_provisa: 0.1.0a483\n")
    if has_setup:
        (demo / "setup.yaml").write_text("steps: []\n")
    done: list[str] = []
    monkeypatch.setattr(builder.Stores, "up", lambda self: done.append("up"))
    monkeypatch.setattr(builder.Stores, "dump", lambda self: done.append("dump") or [])
    monkeypatch.setattr(builder.Stores, "down", lambda self: done.append("down"))
    monkeypatch.setattr(builder, "_set_up", lambda *_a: done.append("set up"))
    monkeypatch.setattr(builder, "_export_views", lambda *_a: done.append("export views"))
    monkeypatch.setattr(builder, "_seed", lambda *_a: done.append("seed"))
    monkeypatch.setattr(builder, "_prefetch_connector_bundles", lambda *_a: done.append("bundles"))
    monkeypatch.setattr(
        builder, "_refuse_empty_replicas", lambda *_a: done.append("no empty replica")
    )
    monkeypatch.setattr(builder, "_replicates", lambda *_a: replicates)
    monkeypatch.setattr(builder.bundles, "pack", lambda _demo, _manifest: b"bundle")
    args = argparse.Namespace(
        name="sonnets", named_dir=named, out=tmp_path / "out", commit="a" * 40, timeout=60.0
    )
    assert builder.build(args) == 0
    assert done == expected


def test_a_demo_in_regions_is_seeded_without_parsing_its_config_outside_a_region(
    tmp_path, monkeypatch
):
    """REQ-2068: its config names each region's address and store, given only to _seed's nodes."""
    named = tmp_path / "named"
    demo = named / "retail"
    demo.mkdir(parents=True)
    (demo / "demo.yaml").write_text(
        "requires_provisa: 0.1.0a483\nregions: {headless: [eu], visitor: us}\n"
    )
    done: list[str] = []
    monkeypatch.setattr(builder.Stores, "up", lambda self: done.append("up"))
    monkeypatch.setattr(builder.Stores, "dump", lambda self: done.append("dump") or [])
    monkeypatch.setattr(builder.Stores, "down", lambda self: done.append("down"))
    monkeypatch.setattr(builder, "_seed", lambda *_a: done.append("seed"))
    monkeypatch.setattr(builder, "_prefetch_connector_bundles", lambda *_a: done.append("bundles"))
    monkeypatch.setattr(
        builder, "_refuse_empty_replicas", lambda *_a: done.append("no empty replica")
    )

    def _parsed(*_a):
        raise AssertionError("the config was parsed outside a region's node")

    monkeypatch.setattr(builder, "_replicates", _parsed)
    monkeypatch.setattr(builder.bundles, "pack", lambda _demo, _manifest: b"bundle")
    args = argparse.Namespace(
        name="retail", named_dir=named, out=tmp_path / "out", commit="a" * 40, timeout=60.0
    )
    assert builder.build(args) == 0
    assert done == ["up", "bundles", "seed", "no empty replica", "dump", "down"]


def test_the_bundle_action_installs_a_demo_s_extras_before_it_builds():
    action = yaml.safe_load(
        (_REPO / ".github" / "actions" / "demo-bundle" / "action.yml").read_text()
    )
    names = [step["name"] for step in action["runs"]["steps"]]
    assert names.index("Install the demo's extras") < names.index("Build the bundle")
    install = action["runs"]["steps"][names.index("Install the demo's extras")]["run"]
    assert "scripts/demo_extras.py" in install
    assert "https://download.pytorch.org/whl/cpu torch" in install


def test_a_bundled_demo_runs_only_its_environment_steps_where_it_runs():
    """Its commands and view builds ran at the bundle build, their work in the dumps; its
    environments (REQ-2053) are branches of the model in the control plane every start builds."""
    launcher = (_REPO / "start-ui-install.sh").read_text()
    block = launcher[: launcher.index("scripts/run_demo_setup.py")].rsplit("\nif ", 1)[1]
    assert '"$_NAMED_DIR/setup.yaml"' in block
    assert '"$_DEMO_BUNDLED" = true ]; then _SETUP_SCOPE="--environments-only"' in block


def test_the_sonnets_run_time_compose_loads_what_its_build_saves():
    run = yaml.safe_load((_SONNETS / "docker-compose.run.yml").read_text())
    postgres, qdrant = run["services"]["postgres"], run["services"]["qdrant"]
    # Each database the build dumped (dump/postgres/<database>.sql), loaded by the loader it wrote.
    assert "./dump/postgres:/dumps:ro" in postgres["volumes"]
    assert (
        "./dump/load-postgres.sh:/docker-entrypoint-initdb.d/10-load.sh:ro" in postgres["volumes"]
    )
    assert "./dump/qdrant:/snapshots:ro" in qdrant["volumes"]
    # Recovered at every start from the snapshot the build saved,
    # dump/<service>/<collection>.snapshot. --force-snapshot: --snapshot alone panics on a restart
    # that finds the collection already in the volume, and the store restarts forever.
    assert qdrant["command"] == [
        "./qdrant",
        "--snapshot",
        "/snapshots/sonnet_lines.snapshot:sonnet_lines",
        "--force-snapshot",
    ]
    built = yaml.safe_load((_SONNETS / "docker-compose.yml").read_text())
    assert built["services"]["qdrant"]["image"] == qdrant["image"]
    assert built["services"]["qdrant"]["labels"]["com.provisa.demo.role"] == "vector"


_REVIEWS = _REPO / "demo" / "named" / "reviews"


def test_document_search_and_graph_stores_are_dumped_by_their_role(tmp_path, monkeypatch):
    """REQ-2030: MongoDB as one archive, Elasticsearch as one snapshot copied out of its container,
    Neo4j stopped and dumped -- last, after every store that is read while running."""
    demo = tmp_path / "reviews"
    demo.mkdir()
    (demo / "docker-compose.yml").write_text("services: {}\n")
    stores = builder.Stores(demo)
    config = {
        "services": {
            "elasticsearch": {
                "image": "docker.elastic.co/elasticsearch/elasticsearch:8.13.0",
                "labels": {"com.provisa.demo.role": "search"},
                "environment": {"path.repo": "/usr/share/elasticsearch/snapshots"},
            },
            "mongodb": {
                "image": "mongo:7",
                "labels": {"com.provisa.demo.role": "document"},
                "environment": {
                    "MONGO_INITDB_ROOT_USERNAME": "root",
                    "MONGO_INITDB_ROOT_PASSWORD": "pw",
                },
            },
            "neo4j": {
                "image": "neo4j:5.19-community",
                "labels": {"com.provisa.demo.role": "graph"},
            },
            "seeder": {"image": "seed:1", "labels": {"com.provisa.demo.role": "seeder"}},
        }
    }
    ran: list[tuple] = []

    def _compose(*args, **kw):
        ran.append(args)
        if args[0] == "config":
            return type("Done", (), {"stdout": json.dumps(config)})()
        if args[0] == "port":
            return type("Done", (), {"stdout": "0.0.0.0:49200\n"})()
        if "mongodump" in args:
            kw["stdout"].write(b"archive bytes")
        return type("Done", (), {"stdout": ""})()

    asked: list[tuple] = []

    def _put(url, **kw):
        asked.append((url, kw.get("params"), kw.get("json")))
        body = {"snapshot": {"state": "SUCCESS"}} if url.endswith("/data") else {}
        return httpx.Response(200, json=body, request=httpx.Request("PUT", url))

    monkeypatch.setattr(stores, "_compose", _compose)
    monkeypatch.setattr(builder.httpx, "put", _put)

    assert stores.dump() == ["elasticsearch/data", "mongodb", "neo4j/neo4j"]
    assert (demo / "dump" / "mongodb.archive.gz").read_bytes() == b"archive bytes"
    assert asked == [
        (
            "http://127.0.0.1:49200/_snapshot/bundle",
            None,
            {"type": "fs", "settings": {"location": "/usr/share/elasticsearch/snapshots"}},
        ),
        (
            "http://127.0.0.1:49200/_snapshot/bundle/data",
            {"wait_for_completion": "true"},
            {"indices": "*,-.*", "include_global_state": False},
        ),
    ]
    commands = [a for a in ran if a[0] != "config"]
    assert commands == [
        ("port", "elasticsearch", "9200"),
        (
            "cp",
            "elasticsearch:/usr/share/elasticsearch/snapshots",
            str(demo / "dump" / "elasticsearch"),
        ),
        (
            "exec", "-T", "mongodb", "mongodump", "--quiet", "--archive", "--gzip",
            "--username", "root", "--password", "pw", "--authenticationDatabase", "admin",
        ),
        ("stop", "neo4j"),
        (
            "run", "--rm", "--no-deps", "-v", f"{demo / 'dump' / 'neo4j'}:/dumps",
            "--entrypoint", "neo4j-admin", "neo4j", "database", "dump", "neo4j",
            "--to-path=/dumps",
        ),
    ]  # fmt: skip


def test_the_reviews_run_time_compose_loads_what_its_build_saves():
    run = yaml.safe_load((_REVIEWS / "docker-compose.run.yml").read_text())["services"]
    built = yaml.safe_load((_REVIEWS / "docker-compose.yml").read_text())["services"]
    for name, role in (("mongodb", "document"), ("elasticsearch", "search"), ("neo4j", "graph")):
        assert run[name]["labels"]["com.provisa.demo.role"] == role
        assert built[name]["labels"]["com.provisa.demo.role"] == role
        assert built[name]["image"] == run[name]["image"]
        assert all(str(p).startswith("127.0.0.1:") for p in run[name]["ports"])
    assert "./dump/mongodb.archive.gz:/dump/mongodb.archive.gz:ro" in run["mongodb"]["volumes"]
    assert "./dump/elasticsearch:/snapshots:ro" in run["elasticsearch"]["volumes"]
    assert "./dump/neo4j:/dumps:ro" in run["neo4j"]["volumes"]
    # The build snapshots to the path its own store names; the run restores from /snapshots.
    assert built["elasticsearch"]["environment"]["path.repo"]
    assert run["elasticsearch"]["environment"]["path.repo"] == "/snapshots"
    assert run["neo4j"]["environment"]["NEO4J_dbms_databases_default__to__read__only"] == "true"
    # The reader config.yaml connects as is the one the run-time store makes.
    reader = (_REVIEWS / "db" / "mongo-reader.js").read_text()
    catalog = next(
        s for s in yaml.safe_load((_REVIEWS / "config.yaml").read_text())["sources"]
        if s["id"] == "catalog"
    )  # fmt: skip
    assert f'user: "{catalog["username"]}"' in reader and f'pwd: "{catalog["password"]}"' in reader


def test_the_build_hands_its_servers_the_variables_the_config_reads_and_no_other(
    tmp_path, monkeypatch
):
    """A govdata source is landed in the build with the account key its config reads; the
    servers the build starts get that variable from the build's environment, and nothing else of
    it."""
    (tmp_path / "config.yaml").write_text(
        "sources:\n- username: ${env:ASKAMERICA_API_KEY:-}\n  host: ${env:X_PG_HOST:-localhost}\n"
    )
    monkeypatch.setenv("ASKAMERICA_API_KEY", "key-1")
    monkeypatch.setenv("UNRELATED_SECRET", "not-handed-on")
    monkeypatch.delenv("X_PG_HOST", raising=False)
    assert builder._config_env(tmp_path) == {"ASKAMERICA_API_KEY": "key-1"}


def _seeded(tmp_path, rows: dict[str, int | None]):
    seeded = tmp_path / "insurance" / "seeded"
    seeded.mkdir(parents=True)
    # Every demo has one (build refuses a directory without); this one runs in no regions.
    (tmp_path / "insurance" / "demo.yaml").write_text("name: insurance\n")
    (seeded / "replicas.json").write_text(
        json.dumps(
            {
                "replicas": [
                    {
                        "source_id": "govdata",
                        "schema_name": "disasters",
                        "table_name": name,
                        "completed_rows": count,
                    }
                    for name, count in rows.items()
                ]
            }
        )
    )
    return tmp_path / "insurance"


def test_a_replica_that_landed_no_rows_fails_the_build_by_name(tmp_path):
    """REQ-2037: a TTL -1 replica is landed once; an empty one would be served empty for good."""
    demo = _seeded(tmp_path, {"wildfire_risk_by_county": 3144, "storm": 0, "nri": None})
    with pytest.raises(SystemExit) as refused:
        builder._refuse_empty_replicas(demo)
    assert str(refused.value) == (
        "insurance: these replicas landed no rows: govdata.disasters.nri, govdata.disasters.storm"
    )


def test_replicas_that_all_landed_rows_pass(tmp_path):
    builder._refuse_empty_replicas(_seeded(tmp_path, {"nri": 3142, "wildfire": 3144}))


def test_the_bundle_job_carries_the_keys_a_demo_s_build_uses():
    """A govdata table landed at the build (REQ-2031), a tour's Polly answers (REQ-2012) and a
    Salesforce table landed at the build (the hasuramigration demo's Account)."""
    workflow = yaml.safe_load((_REPO / ".github" / "workflows" / "demo-bundles.yml").read_text())
    assert workflow["jobs"]["bundle"]["env"] == {
        "ASKAMERICA_API_KEY": "${{ secrets.ASKAMERICA_API_KEY }}",
        "OPENROUTER_API_KEY": "${{ secrets.OPENROUTER_API_KEY }}",
        "SF_LOGIN_URL": "${{ secrets.SF_LOGIN_URL }}",
        "SF_CONSUMER_KEY": "${{ secrets.SF_CONSUMER_KEY }}",
        "SF_CONSUMER_SECRET": "${{ secrets.SF_CONSUMER_SECRET }}",
    }


# --- REQ-2012: a tour's Polly answers are asked at the bundle build and stored beside it ---------


def _toured(tmp_path: Path, steps: list[dict]) -> Path:
    demo = tmp_path / "insider"
    demo.mkdir()
    (demo / "config.yaml").write_text(yaml.safe_dump({"domains": [{"id": "bank"}], "sources": []}))
    (demo / "tour.yaml").write_text(yaml.safe_dump({"steps": steps}))
    return demo


_ASKING = [
    {"key": "insiderIntro", "route": "/sources", "source": "bank"},
    {"key": "insiderWhy", "polly": "Why was the top message flagged?"},
]


def test_the_steps_asking_polly_are_found(tmp_path):
    assert builder.polly_steps(_toured(tmp_path, _ASKING)) == [_ASKING[1]]
    assert builder.polly_steps(tmp_path) == []  # no tour


def test_the_answering_config_asks_polly_through_openrouter_on_the_sandbox_model(tmp_path):
    from provisa.core.settings_registry import setting

    config = yaml.safe_load(
        builder.answering_config(tmp_path, _toured(tmp_path, _ASKING)).read_text()
    )
    assert config["ai_endpoints"] == [builder._ANSWERING_ENDPOINT]
    assert config["ai_models"]["mcp_chat"] == {
        "vendor": "openrouter",
        "model": setting("sandbox.ai_model").default,
    }
    assert config["domains"] == [{"id": "bank"}]  # the demo's own config, kept


def test_the_answering_config_is_served_beside_what_the_demo_declares(tmp_path):
    """The build's server reads a demo's declared replica rows and code archives from the
    directory of the config it serves: an answering config written anywhere else left a demo
    with both a Polly step and declared rows (cloudpolicy) calling its faked sources' services,
    and its code source without its upload."""
    demo = _toured(tmp_path, _ASKING)
    (demo / "replica_rows" / "crm").mkdir(parents=True)
    (demo / "replica_rows" / "crm" / "crm.Account.tsv").write_text("Id\n1\n")
    (demo / "code").mkdir()
    (demo / "code" / "reader.tar.gz").write_bytes(b"archive")
    (demo / "secrets.yaml").write_text("{}\n")
    work = tmp_path / "work"
    work.mkdir()
    served = builder.answering_config(work, demo).parent
    assert (served / "replica_rows" / "crm" / "crm.Account.tsv").read_text() == "Id\n1\n"
    assert (served / "code" / "reader.tar.gz").read_bytes() == b"archive"
    assert (served / "secrets.yaml").is_file()


_encode = json.dumps  # the stream below takes a json= keyword, as httpx.stream does


class _Chat:
    """/admin/mcp/chat as a server streams it: the reply to each question, as text events."""

    def __init__(self, replies: dict[str, str]) -> None:
        self.replies = replies

    @contextlib.contextmanager
    def __call__(self, method, url, *, json, headers, timeout):
        assert url.endswith("/admin/mcp/chat") and headers == {"X-Provisa-Role": "org_admin"}
        reply = self.replies[json["messages"][-1]["content"]]
        events = (
            [{"type": "text", "text": reply}, {"type": "done"}] if reply else [{"type": "done"}]
        )
        body = "".join(f"data: {_encode(e)}\n\n" for e in events)
        yield httpx.Response(200, text=body, request=httpx.Request(method, url))


def test_each_polly_step_is_answered_and_stored_in_the_shape_the_tour_reads(tmp_path, monkeypatch):
    from provisa.core import demo as demo_module

    demo = _toured(tmp_path, _ASKING)
    monkeypatch.setattr(
        builder.httpx, "stream", _Chat({"Why was the top message flagged?": "It names Falcon."})
    )
    answers = builder.answer_tour("http://provisa.test", demo, builder.polly_steps(demo))
    assert answers == {
        "insiderWhy": {"question": "Why was the top message flagged?", "answer": "It names Falcon."}
    }
    demo_module.tour_answers_path(str(demo)).write_text(json.dumps(answers))
    monkeypatch.setattr(demo_module, "tour_answers_path", lambda named: demo / "tour_answers.json")
    [_, asked] = demo_module.demo_tour(demo)
    assert asked["answer"] == "It names Falcon."


def test_a_question_polly_does_not_answer_fails_the_build_naming_its_step(tmp_path, monkeypatch):
    demo = _toured(tmp_path, _ASKING)
    monkeypatch.setattr(builder.httpx, "stream", _Chat({"Why was the top message flagged?": ""}))
    with pytest.raises(SystemExit, match="insiderWhy"):
        builder.answer_tour("http://provisa.test", demo, builder.polly_steps(demo))


def test_a_tour_asking_polly_needs_the_openrouter_key(tmp_path, monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    with pytest.raises(SystemExit, match="OPENROUTER_API_KEY is not set"):
        builder._set_up(tmp_path, _toured(tmp_path, _ASKING), None, timeout=1)


def _stores_up(tmp_path: Path, services: str) -> list[tuple]:
    """What ``Stores.up`` asks docker compose, for a demo whose services are ``services``."""
    demo = tmp_path / "demo"
    demo.mkdir()
    (demo / "docker-compose.yml").write_text("services: {}\n")
    stores = builder.Stores(demo)
    asked: list[tuple] = []

    def _compose(*args, **_kw):
        asked.append(args)
        return type("Done", (), {"stdout": services})()

    stores._compose = _compose
    stores.up()
    return asked


def test_a_demo_with_a_seeder_brings_its_stores_up_then_runs_it(tmp_path):
    asked = _stores_up(tmp_path, "postgres\nseeder\n")
    assert asked[1] == ("up", "-d", "--build", "--wait", "--scale", "seeder=0")
    assert asked[2] == ("run", "--rm", "--no-deps", "seeder")


def test_a_demo_s_own_clients_are_not_started_by_the_build(tmp_path):
    """REQ-2067: the provisioning demo's consumer reads the Provisa a start runs on; the build runs
    it against its own server from a setup step instead."""
    demo = tmp_path / "demo"
    demo.mkdir()
    (demo / "docker-compose.yml").write_text(
        yaml.safe_dump(
            {
                "services": {
                    "postgres": {"labels": {"com.provisa.demo.role": "rdb"}},
                    "consumer": {"labels": {"com.provisa.demo.role": "client"}},
                }
            }
        )
    )
    stores = builder.Stores(demo)
    asked: list[tuple] = []
    stores._compose = lambda *args, **_kw: (
        asked.append(args) or type("Done", (), {"stdout": "postgres\nconsumer\n"})()
    )
    stores.up()
    assert asked[1] == ("up", "-d", "--build", "--wait", "--scale", "consumer=0")


def test_a_demo_with_no_seeder_brings_its_stores_up(tmp_path):
    """A demo whose data its init SQL loads has no seeder, and compose refuses to scale a service
    it does not have."""
    asked = _stores_up(tmp_path, "postgres\n")
    assert asked[1:] == [("up", "-d", "--build", "--wait")]


def test_the_sonnets_vectors_source_reads_with_the_stores_read_only_key():
    """The run-time Qdrant asks for a key (docker-compose.run.yml), and the source reads with the
    read-only one: without it every lines_like answered 401 (cloud-dev, 2026-10-16)."""
    run = yaml.safe_load((_SONNETS / "docker-compose.run.yml").read_text())
    reader = run["services"]["qdrant"]["environment"]["QDRANT__SERVICE__READ_ONLY_API_KEY"]
    config = yaml.safe_load((_SONNETS / "config.yaml").read_text())
    vectors = next(s for s in config["sources"] if s["id"] == "vectors")
    assert reader == "${SONNETS_QDRANT_KEY:-sonnets-reader}"
    assert vectors["password"] == "${env:SONNETS_QDRANT_KEY:-sonnets-reader}"


_MARKER = ".provisa-init-complete"


def _run_composes() -> list[Path]:
    return [
        *sorted((_REPO / "demo" / "named").glob("*/docker-compose.run.yml")),
        _REPO / "scripts" / "new_demo_template" / "docker-compose.run.yml",
    ]


def test_every_store_loaded_at_its_first_start_is_healthy_only_once_loaded():
    """A store loads its bundle's data at its first start only (an init script, a restore, a
    load). A start cut off half-way used to pass its healthcheck anyway -- the reviews demo's
    MongoDB ran with its products and none of its ratings, and no reader -- so every such store
    writes a marker in its data volume when its load has ended, and its healthcheck requires it
    (REQ-2049). Init scripts run in name order: the marker's is the last."""
    checked = 0
    for compose in _run_composes():
        for name, service in yaml.safe_load(compose.read_text())["services"].items():
            volumes = [str(v) for v in service.get("volumes") or []]
            initdb = [v for v in volumes if "/docker-entrypoint-initdb.d/" in v]
            loads = initdb or service.get("entrypoint")
            if not loads:
                continue
            checked += 1
            where = f"{compose.parent.name}/{name}"
            test = " ".join(service["healthcheck"]["test"])
            assert _MARKER in test, where
            assert service["healthcheck"]["start_period"] == "600s", where
            if initdb:
                scripts = sorted(v.split("/docker-entrypoint-initdb.d/")[1] for v in initdb)
                assert scripts[-1] == "99-complete.sh:ro", where
                assert (compose.parent / "db" / "init-complete.sh").is_file(), where
            else:
                script = next(
                    v
                    for v in volumes
                    if v.endswith(":/restore.sh:ro") or v.endswith(":/load.sh:ro")
                )
                source = compose.parent / script.split(":")[0]
                assert _MARKER in source.read_text(), where
    assert checked >= 12, checked


_VERSION = "202610160000-abc1234"


def _sync(
    tmp_path, *args: str, requested: bool = False, in_use: str | None = None, ask_on_pull=False
):
    """scripts/demo_stores_sync.sh for the weblogs demo, its pull and scripts/demo_stores.py fakes
    that record what they were asked."""
    import os
    import subprocess

    bundles = tmp_path / "bundles"
    (bundles / "weblogs" / _VERSION).mkdir(parents=True)
    (bundles / "weblogs" / _VERSION / "docker-compose.run.yml").write_text("services: {}\n")
    if requested:
        (bundles / "weblogs" / "requested").write_text("requested\n")
    if in_use is not None:
        (bundles / "weblogs" / "in-use").write_text(in_use + "\n")
    calls = tmp_path / "calls"
    fake = tmp_path / "bin"
    fake.mkdir()
    # ask_on_pull: a visitor opens weblogs while the run pulls a second demo, after weblogs was
    # synced, as the tier asks (provisa.core.demo_bundles.request): its requested file, then
    # the signal.
    if ask_on_pull:
        (bundles / "later" / _VERSION).mkdir(parents=True)
    ask = (
        f'[ "${{2%%@*}}" = later ] && [ ! -e {tmp_path}/asked ] && {{ touch {tmp_path}/asked; '
        f"echo requested > {bundles}/weblogs/requested; touch {bundles}/.wanted; }}\n"
        if ask_on_pull
        else ""
    )
    (fake / "pull").write_text(
        f'#!/usr/bin/env bash\n{ask}echo {bundles}/"${{2%%@*}}"/{_VERSION}\n'
    )
    (fake / "pull").chmod(0o755)
    (fake / "demo_stores.py").write_text(
        f"import sys\nopen({str(calls)!r}, 'a').write(' '.join(sys.argv[1:]) + '\\n')\n"
    )
    env = {
        **os.environ,
        "PROVISA_DEMO_BUNDLES": "weblogs,later" if ask_on_pull else "weblogs",
        "DEMO_BUNDLES_DIR": str(bundles),
        "DEMO_PULL": str(fake / "pull"),
        "DEMO_PULL_INTO": str(bundles),
        "DEMO_STORES": str(fake / "demo_stores.py"),
        "DEMO_STORES_SHARED": "/shared.yml",
        "DEMO_STORES_PROJECT": "provisa-demo-stores",
        "DEMO_STORES_KAFKA_PORT": "29092",
    }
    run = subprocess.run(
        ["bash", str(_REPO / "scripts" / "demo_stores_sync.sh"), *args],
        env=env,
        capture_output=True,
        text=True,
    )
    asked = calls.read_text().splitlines() if calls.exists() else []
    return run, bundles / "weblogs", asked


def test_a_demo_not_asked_for_is_pulled_and_offered_and_none_of_its_stores_started(tmp_path):
    """scripts/demo_stores_sync.sh: a demo is offered once pulled; its stores are started only once
    the tier asks for it (REQ-2049)."""
    run, demo, asked = _sync(tmp_path)
    assert run.returncode == 0, run.stderr
    assert (demo / "pulled").read_text().strip() == _VERSION
    assert not (demo / "ready").exists()
    assert asked == []


def test_a_demo_asked_for_has_its_stores_brought_up_and_its_version_marked_ready(tmp_path):
    run, demo, asked = _sync(tmp_path, requested=True)
    assert run.returncode == 0, run.stderr
    assert asked == [f"up weblogs {_VERSION} {demo / _VERSION}"]
    assert (demo / "ready").read_text().strip() == _VERSION


def test_a_demo_asked_for_during_a_run_has_its_stores_brought_up_by_that_run(tmp_path):
    """An open during a sync touches <bundles>/.wanted while the service runs, which systemd's
    path unit drops: the run goes again, so the stores come up now rather than at the timer's
    next tick (a first open on the sandbox tier waited up to 15 minutes)."""
    run, demo, asked = _sync(tmp_path, ask_on_pull=True)
    assert run.returncode == 0, run.stderr
    assert asked == [f"up weblogs {_VERSION} {demo / _VERSION}"]
    assert (demo / "ready").read_text().strip() == _VERSION
    assert not (demo.parent / ".wanted").exists()


def test_an_older_version_nothing_reads_is_taken_down_and_one_read_is_kept(tmp_path):
    bundles = tmp_path / "bundles" / "weblogs"
    for old in ("202601010000-old0001", "202601020000-old0002"):
        (bundles / old).mkdir(parents=True)
        (bundles / old / "stores.env").write_text("WEBLOGS_PG_NS=_weblogs_x\n")
    run, demo, asked = _sync(tmp_path, requested=True, in_use="202601020000-old0002")
    assert run.returncode == 0, run.stderr
    assert asked == [
        f"up weblogs {_VERSION} {demo / _VERSION}",
        "drop weblogs 202601010000-old0001",
    ]
    assert not (demo / "202601010000-old0001" / "stores.env").exists()
    assert (demo / "202601020000-old0002" / "stores.env").exists()


def test_the_nodes_start_leaves_no_demo_asked_for_or_ready(tmp_path):
    """Run before docker at the node's start (provisa-demo-stores-boot.service): none of the
    demos' stores is running, so none is ready or asked for, and none is started at boot."""
    run, demo, asked = _sync(tmp_path, requested=True)
    assert (demo / "ready").exists()
    import os
    import subprocess

    boot = subprocess.run(
        ["bash", str(_REPO / "scripts" / "demo_stores_sync.sh"), "--boot"],
        env={
            **os.environ,
            "PROVISA_DEMO_BUNDLES": "weblogs",
            "DEMO_BUNDLES_DIR": str(demo.parent),
            "DEMO_PULL": "unused",
            "DEMO_PULL_INTO": str(demo.parent),
            "DEMO_STORES": "unused",
            "DEMO_STORES_SHARED": "unused",
            "DEMO_STORES_PROJECT": "unused",
            "DEMO_STORES_KAFKA_PORT": "unused",
        },
        capture_output=True,
        text=True,
    )
    assert boot.returncode == 0, boot.stderr
    assert not (demo / "ready").exists() and not (demo / "requested").exists()
    assert (demo / "pulled").read_text().strip() == _VERSION


def test_a_demo_in_regions_refuses_an_empty_replica_of_any_region(tmp_path):
    """REQ-2068: each region's seed is checked, by name."""
    demo = tmp_path / "retail"
    demo.mkdir()
    (demo / "demo.yaml").write_text("regions: {visitor: us, headless: [eu]}\n")
    for region, rows in (("eu", 0), ("us", 5)):
        out = demo / "seeded" / "regions" / region
        out.mkdir(parents=True)
        (out / "record.json").write_text(
            json.dumps(
                {
                    "replicas": [
                        {
                            "source_id": f"{region}-shop",
                            "schema_name": "public",
                            "table_name": "customers",
                            "completed_rows": rows,
                        }
                    ]
                }
            )
        )
    with pytest.raises(SystemExit) as refused:
        builder._refuse_empty_replicas(demo)
    assert str(refused.value) == "retail: these replicas landed no rows: eu-shop.public.customers"


def test_a_build_server_is_told_where_duckdb_finds_its_extensions(tmp_path, monkeypatch):
    """Its environment is only the build's own: without HOME, DuckDB cannot place its per-user
    extension directory, and the server fails to start (icu could not be autoloaded)."""
    from provisa.federation import duckdb_extensions

    builder = _builder()
    monkeypatch.delenv("PROVISA_DUCKDB_EXT_DIR", raising=False)
    monkeypatch.delenv("HOME", raising=False)

    def not_installed(_target):
        raise ModuleNotFoundError("provisa_duckdb_ext")

    monkeypatch.setattr(duckdb_extensions, "stage_bundled_extensions", not_installed)
    assert builder._duckdb_env(tmp_path) == {"HOME": str(Path.home())}

    staged = tmp_path / "duckdb-ext"
    monkeypatch.setattr(duckdb_extensions, "stage_bundled_extensions", lambda target: target)
    assert builder._duckdb_env(tmp_path)["PROVISA_DUCKDB_EXT_DIR"] == str(staged)

    monkeypatch.setenv("PROVISA_DUCKDB_EXT_DIR", "/opt/ext")
    assert builder._duckdb_env(tmp_path)["PROVISA_DUCKDB_EXT_DIR"] == "/opt/ext"


def test_the_build_fetches_the_demos_connector_bundles_before_any_server_starts(
    tmp_path, monkeypatch
):
    """REQ-956: the bundle of each source the demo reads through one is fetched once, before the
    build's replica builds would each fetch it at the same moment; a source read without a bundle
    fetches nothing."""
    from provisa.runtime_deps import pgwire_bundles

    fetched: list[str] = []
    demo = tmp_path / "demo"
    demo.mkdir()
    (demo / "config.yaml").write_text(
        "sources:\n- {id: code, type: postgresql}\n- {id: gov, type: govdata}\n"
        "- {id: gov2, type: govdata}\n"
    )
    monkeypatch.setattr(
        pgwire_bundles.BundleResolver,
        "resolve",
        lambda _self, spec: fetched.append(spec.artifact_name) or Path("/bundles"),
    )
    builder._prefetch_connector_bundles(demo)
    assert fetched == ["pgwire-govdata"]


def test_every_demos_relational_store_is_one_the_build_dumps():
    """The build dumps a demo's relational store (role rdb) with pg_dump: its image is Postgres,
    or Postgres with pgvector. One it does not know fails the build after the demo is set up
    (cloudpolicy's pgvector store, at its first bundle build)."""
    named = Path(__file__).resolve().parents[2] / "demo" / "named"
    unknown = {
        f"{compose.parent.name}/{name}": service["image"]
        for compose in sorted(named.glob("*/docker-compose.yml"))
        # A published demo's bundle is built; an unpublished one (perf) is not.
        if yaml.safe_load((compose.parent / "demo.yaml").read_text()).get("published")
        for name, service in (yaml.safe_load(compose.read_text()).get("services") or {}).items()
        if (service.get("labels") or {}).get("com.provisa.demo.role") == "rdb"
        and not str(service.get("image", "")).startswith(builder.POSTGRES_IMAGES)
    }
    assert unknown == {}
