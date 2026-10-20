# Copyright (c) 2026 Kenneth Stott
# Canary: 0a3cf56a-218b-48e7-af83-f7697822fe06
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build, publish and pull a named demo's bundle (REQ-2049, REQ-2006).

A demo's data is landed once, when its bundle is built -- never at a visitor's request and never
where the demo runs:

``build <name> --out DIR``
    1. the demo's directory is copied to a scratch directory, and its own data stores come up from
       its docker-compose.yml on ports of the build's own, traffic generator (seeder) included --
       the weblogs demo's writes its log files into the copy's ``logs/``; a demo with a
       ``setup.yaml`` is then set up against those stores, by a Provisa server on the demo's own
       config.yaml with a throwaway control plane (scripts/run_demo_setup.py, REQ-2016) -- the
       sonnets demo's embeds its lines into Qdrant and writes its echoes; a demo with bitemporal
       views (REQ-1162) then has their append logs -- the history its setup's steps gave them --
       written out as ``seeded/views.duckdb`` and ``seeded/views.json``, with the records of the
       refreshes that built it (``seeded/refresh_lineage.parquet``, REQ-862), which its template
       and each sandbox start from;
    2. a sandbox tier runs on a throwaway embedded control plane in seeding mode
       (``PROVISA_DEMO_SEED_EXPORT=true``): it lands the demo's template from those stores and
       writes the seed -- ``seeded/replicas.duckdb`` and ``seeded/replicas.json``
       (provisa/api/demo_replicas.py);
    3. each data store is dumped by its ``com.provisa.demo.role``, for the demo's run-time compose
       file (docker-compose.run.yml) to load and restore: a Postgres store (``rdb``) one
       plain dump a database, ``dump/<service>/<database>.sql`` (with ``dump/load-postgres.sh``),
       a Qdrant store (``vector``) as one snapshot a collection,
       ``dump/<service>/<collection>.snapshot``, a MongoDB store (``document``) as one archive,
       ``dump/<service>.archive.gz``, an Elasticsearch store (``search``) as a snapshot repository,
       ``dump/<service>/``, and a Neo4j store (``graph``) as ``dump/<service>/neo4j.dump``;
    4. everything is taken down, and the copy is packed (provisa/core/demo_bundles.py) as
       ``DIR/<name>-<version>.tar.gz``, version ``<UTC minute>-<commit>``.

``push BUNDLE``
    publishes it as ``<name>-<version>`` and ``<name>-latest``; a version already published is
    refused. The publisher's credentials are PROVISA_DEMO_REGISTRY_USER and
    PROVISA_DEMO_REGISTRY_TOKEN.

``pull <name>[@<version>] --into DIR``
    unpacks the bundle (``latest`` unless a version is named) into ``DIR/<name>/<version>`` and
    prints that directory. A version already there is not fetched again: a version names one
    bundle forever.

Usage: python scripts/demo_bundle.py build weblogs --out dist/demos
       python scripts/demo_bundle.py push dist/demos/weblogs-202610141530-1a2b3c4d5.tar.gz
       python scripts/demo_bundle.py pull weblogs --into ~/.provisa/demos
"""

# Requirements: REQ-2049, REQ-2006

from __future__ import annotations

import argparse
import datetime
import gzip
import io
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
from pathlib import Path

import httpx
import yaml

_REPO = Path(__file__).resolve().parents[1]
#: What a demo's own Postgres container loads the bundle's dumps with (dump/load-postgres.sh).
LOAD_POSTGRES = _REPO / "scripts" / "demo_run" / "load-postgres.sh"
#: The images a demo's relational store is Postgres in, dumped with pg_dump: Postgres itself, and
#: Postgres with pgvector (the cloudpolicy demo's clause embeddings).
POSTGRES_IMAGES = ("postgres:", "pgvector/pgvector:")
sys.path.insert(0, str(_REPO))

from provisa.core import demo as demo_module  # noqa: E402
from provisa.core import demo_bundles as bundles  # noqa: E402

_NAMED = _REPO / "demo" / "named"
# The org the setup server runs as, whose store its views are built in.
_SETUP_ORG = "default"
# What a build leaves behind in a demo's directory that is not the demo's: an earlier build's seed
# and dumps, and the seeder's marker volume.
_NOT_COPIED = ("seeded", "dump", "data")
# A small image to hand the stores' files back with (Stores.down).
_CHOWN_IMAGE = "busybox:1.36.1"
_PORT_VAR = re.compile(r"\$\{([A-Z][A-Z0-9_]*_PORT):-\d+\}")
# A variable the demo's config.yaml reads: ${env:NAME} or ${env:NAME:-default}.
_CONFIG_VAR = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)")


def _config_env(demo: Path) -> dict[str, str]:
    """What this process holds of the variables the demo's config.yaml reads -- the account key of
    a source landed in the build (a govdata source's ASKAMERICA_API_KEY) -- for the servers the
    build starts: their environment is otherwise only the build's own. A variable this process does
    not hold is left out; the config's own default (or its refusal) then stands."""
    names = set(_CONFIG_VAR.findall((demo / "config.yaml").read_text()))
    return {name: os.environ[name] for name in sorted(names) if name in os.environ}


def _refuse_empty_replicas(demo: Path) -> None:
    """REQ-2037: a table the demo serves from a replica that landed no rows fails the build, by
    name: the bundle would serve an empty table for good (a TTL -1 replica is never landed again).
    A demo in regions, each region's seed (REQ-2068)."""
    from provisa.api.demo_region_seed import seed_dir

    regions = _demo_regions(demo)
    records = (
        [seed_dir(demo, region) / "record.json" for region in regions]
        if regions
        else [demo / "seeded" / "replicas.json"]
    )
    empty = sorted(
        f"{r['source_id']}.{r['schema_name']}.{r['table_name']}"
        for path in records
        for r in json.loads(path.read_text())["replicas"]
        if not r.get("completed_rows")
    )
    if empty:
        raise SystemExit(f"{demo.name}: these replicas landed no rows: {', '.join(empty)}")


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _protocol_ports() -> dict[str, str]:
    """A port of its own for each protocol a build's server serves beside HTTP: their defaults
    (8815, 50051, 5439, 7687) are a running Provisa's on the machine that builds."""
    return {
        name: str(_free_port())
        for name in ("FLIGHT_PORT", "GRPC_PORT", "PROVISA_PGWIRE_PORT", "PROVISA_BOLT_PORT")
    }


def _duckdb_env(work: Path) -> dict[str, str]:
    """Where a build's server finds DuckDB's extensions, named explicitly: its environment is
    otherwise only the build's own, and DuckDB without HOME cannot place its per-user extension
    directory (the icu extension then fails to load and the server does not start). As
    provisa/cli.py does for a deployment: the extensions staged offline from provisa-duckdb-ext
    when it is installed (PROVISA_DUCKDB_EXT_DIR), or else this user's own DuckDB cache, under the
    HOME this process has -- a checkout without the embedded extra installs from the network on
    first use, as it does everywhere (cli.py). An extension directory this process was given is
    passed on as it is."""
    from provisa.federation.duckdb_extensions import extension_directory, stage_bundled_extensions

    env = {"HOME": str(Path.home())}
    given = extension_directory()
    if given is not None:
        return {**env, "PROVISA_DUCKDB_EXT_DIR": given}
    try:
        staged = stage_bundled_extensions(work / "duckdb-ext")
    except ModuleNotFoundError:
        return env  # no provisa-duckdb-ext: DuckDB's own cache under HOME (cli.py's dev case)
    return {**env, "PROVISA_DUCKDB_EXT_DIR": str(staged)}


def _requires_provisa(demo: Path) -> str:
    spec = yaml.safe_load((demo / "demo.yaml").read_text())
    required = spec.get("requires_provisa")
    if not isinstance(required, str):
        raise SystemExit(f"{demo / 'demo.yaml'}: requires_provisa (the oldest Provisa it runs on)")
    bundles.provisa_version_key(required)
    return required


def _copy(demo: Path, work: Path) -> Path:
    copy = work / "named" / demo.name
    shutil.copytree(demo, copy, ignore=lambda d, _names: _NOT_COPIED if Path(d) == demo else ())
    # Logs a development run left: the build's seeder writes the bundle's own.
    logs = copy / "logs"
    if logs.is_dir():
        for path in logs.rglob("*"):
            if path.is_file() and path.name != ".gitignore":
                path.unlink()
    return copy


class Stores:
    """The demo's data stores, under a compose project of the build's own, on its own ports."""

    def __init__(self, demo: Path) -> None:
        self.demo = demo
        self.compose_file = demo / "docker-compose.yml"
        self.project = f"provisa-demo-bundle-{demo.name}-{os.getpid()}"
        text = self.compose_file.read_text() if self.compose_file.exists() else ""
        self.ports = {name: str(_free_port()) for name in sorted(set(_PORT_VAR.findall(text)))}
        # Where the stores are: what a build's server is given of them. Never this process's whole
        # environment -- a server given it would take this process's settings for its own (a test
        # session's PROVISA_MATERIALIZE_URL kept the setup server's views in the session's
        # PostgreSQL instead of its DuckDB file, CI run 39239941042).
        self.addresses = {
            "PROVISA_NAMED_DEMO_DIR": str(demo),
            **self.ports,
            **bundles.store_namespaces(demo),
        }
        # What compose and the build's client steps run with: this process's environment, and
        # where the stores are.
        self.env = {**os.environ, **self.addresses}

    def _compose(self, *args: str, **kw) -> subprocess.CompletedProcess:
        cmd = ["docker", "compose", "-p", self.project, "-f", str(self.compose_file), *args]
        return subprocess.run(cmd, env=self.env, check=True, **kw)

    def up(self) -> None:
        if not self.compose_file.exists():
            return
        services = self._compose("config", "--services", capture_output=True, text=True)
        seeded = "seeder" in services.stdout.split()
        # A demo's own clients (role client: the provisioning demo's consumer) read the Provisa a
        # start runs on; the build has none listening where they look, and runs a client a setup
        # step names against its own (run_demo_setup.py), so they are not started here.
        compose = yaml.safe_load(self.compose_file.read_text())
        clients = [
            name
            for name, service in (compose.get("services") or {}).items()
            if (service.get("labels") or {}).get("com.provisa.demo.role") == "client"
        ]
        scaled = [("seeder", seeded)] + [(name, True) for name in clients]
        scale = [arg for name, on in scaled if on for arg in ("--scale", f"{name}=0")]
        # The stores first; a demo's seeder, when it has one, runs after them.
        self._compose("up", "-d", "--build", "--wait", *scale)
        if seeded:
            # Once, to its end, with the stores it writes to left running; its exit fails the build.
            self._compose("run", "--rm", "--no-deps", "seeder")

    def dump(self) -> list[str]:
        """Each data store, by its role: a Postgres store (rdb) one dump a database in
        dump/<service>/, a Qdrant store (vector) a snapshot a collection, a MongoDB store (document) one archive, an
        Elasticsearch store (search) one snapshot of its indices, and a Neo4j store (graph) its
        database's dump. A graph store is dumped last: it is stopped to be dumped."""
        if not self.compose_file.exists():
            return []
        config = json.loads(
            self._compose("config", "--format", "json", capture_output=True, text=True).stdout
        )
        dumped = []
        graphs = []
        for name, service in sorted(config["services"].items()):
            labels = service.get("labels") or {}
            role = labels.get("com.provisa.demo.role")
            if role == "vector":
                dumped += self._snapshot(name, service)
                continue
            if role == "document":
                dumped.append(self._mongodump(name, service))
                continue
            if role == "search":
                dumped.append(self._es_snapshot(name, service))
                continue
            if role == "graph":
                graphs.append((name, service))
                continue
            if role != "rdb":
                continue
            if not str(service["image"]).startswith(POSTGRES_IMAGES):
                raise SystemExit(f"{self.demo.name}: data store {name} is not Postgres; no dump")
            env = service["environment"]
            user = env["POSTGRES_USER"]
            # One plain dump a database the store holds, each loaded into a database of its own name
            # where the bundle runs (scripts/demo_stores.py on the node, LOAD_POSTGRES in a demo's
            # own container): the retail demo keeps its data in three databases of one server.
            listed = self._compose(
                "exec",
                "-T",
                name,
                "psql",
                "-U",
                user,
                "-d",
                env["POSTGRES_DB"],
                "-Atc",
                "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname",
                capture_output=True,
                text=True,
            ).stdout.split()
            databases = [db for db in listed if db != "postgres"]
            if not databases:
                raise SystemExit(f"{self.demo.name}: data store {name} holds no database to dump")
            target = self.demo / "dump" / name
            target.mkdir(parents=True, exist_ok=True)
            for database in databases:
                with (target / f"{database}.sql").open("w") as out:
                    self._compose(
                        "exec",
                        "-T",
                        name,
                        "pg_dump",
                        "-U",
                        user,
                        "--no-owner",
                        "--no-acl",
                        database,
                        stdout=out,
                    )
            shutil.copy(LOAD_POSTGRES, self.demo / "dump" / LOAD_POSTGRES.name)
            dumped.append(name)
        for name, service in graphs:
            dumped.append(self._neo4j_dump(name, service))
        return dumped

    def _published(self, name: str, port: str) -> str:
        """The address a service's container port is published at, on this host."""
        published = self._compose("port", name, port, capture_output=True, text=True).stdout
        return f"http://127.0.0.1:{published.strip().rsplit(':', 1)[1]}"

    def _mongodump(self, name: str, service: dict) -> str:
        """MongoDB store ``name``, every database of it, as one gzipped archive,
        dump/<name>.archive.gz, read as its administrator (MONGO_INITDB_ROOT_*)."""
        if not str(service["image"]).startswith("mongo:"):
            raise SystemExit(f"{self.demo.name}: document store {name} is not MongoDB; no dump")
        env = service["environment"]
        target = self.demo / "dump" / f"{name}.archive.gz"
        target.parent.mkdir(exist_ok=True)
        with target.open("wb") as out:
            self._compose(
                "exec",
                "-T",
                name,
                "mongodump",
                "--quiet",
                "--archive",
                "--gzip",
                "--username",
                env["MONGO_INITDB_ROOT_USERNAME"],
                "--password",
                env["MONGO_INITDB_ROOT_PASSWORD"],
                "--authenticationDatabase",
                "admin",
                stdout=out,
            )
        return name

    def _es_snapshot(self, name: str, service: dict) -> str:
        """Elasticsearch store ``name``, its indices, as one snapshot (``data``) in a repository at
        its ``path.repo``, copied out of the container to dump/<name>/."""
        if not str(service["image"]).startswith("docker.elastic.co/elasticsearch/"):
            raise SystemExit(f"{self.demo.name}: search store {name} is not Elasticsearch")
        location = (service.get("environment") or {}).get("path.repo")
        if not location:
            raise SystemExit(
                f"{self.demo.name}: search store {name} sets no path.repo to snapshot to"
            )
        base = self._published(name, "9200")
        made = httpx.put(
            f"{base}/_snapshot/bundle",
            json={"type": "fs", "settings": {"location": location}},
            timeout=60,
        )
        made.raise_for_status()
        taken = httpx.put(
            f"{base}/_snapshot/bundle/data",
            params={"wait_for_completion": "true"},
            json={"indices": "*,-.*", "include_global_state": False},
            timeout=600,
        )
        taken.raise_for_status()
        if taken.json()["snapshot"]["state"] != "SUCCESS":
            raise SystemExit(f"{self.demo.name}: snapshot of {name}: {taken.text}")
        target = self.demo / "dump" / name
        target.parent.mkdir(exist_ok=True)
        self._compose("cp", f"{name}:{location}", str(target))
        return f"{name}/data"

    def _neo4j_dump(self, name: str, service: dict) -> str:
        """Neo4j store ``name``'s database (neo4j), dumped to dump/<name>/neo4j.dump: the store is
        stopped, and neo4j-admin dumps its data volume."""
        if not str(service["image"]).startswith("neo4j:"):
            raise SystemExit(f"{self.demo.name}: graph store {name} is not Neo4j; no dump")
        target = self.demo / "dump" / name
        target.mkdir(parents=True, exist_ok=True)
        self._compose("stop", name)
        self._compose(
            "run",
            "--rm",
            "--no-deps",
            "-v",
            f"{target}:/dumps",
            "--entrypoint",
            "neo4j-admin",
            name,
            "database",
            "dump",
            "neo4j",
            "--to-path=/dumps",
        )
        return f"{name}/neo4j"

    def _snapshot(self, name: str, service: dict) -> list[str]:
        """Each collection of Qdrant store ``name``, as a snapshot in dump/<name>/<collection>.snapshot
        (REQ-2016): what it holds is read live where the demo runs, restored from these."""
        if not str(service["image"]).startswith("qdrant/qdrant:"):
            raise SystemExit(f"{self.demo.name}: vector store {name} is not Qdrant; no snapshot")
        base = self._published(name, "6333")
        collections = httpx.get(f"{base}/collections", timeout=60)
        collections.raise_for_status()
        saved = []
        for collection in sorted(c["name"] for c in collections.json()["result"]["collections"]):
            made = httpx.post(f"{base}/collections/{collection}/snapshots", timeout=600)
            made.raise_for_status()
            snapshot = made.json()["result"]["name"]
            target = self.demo / "dump" / name / f"{collection}.snapshot"
            target.parent.mkdir(parents=True, exist_ok=True)
            with httpx.stream(
                "GET", f"{base}/collections/{collection}/snapshots/{snapshot}", timeout=600
            ) as fetched:
                fetched.raise_for_status()
                with target.open("wb") as out:
                    for chunk in fetched.iter_bytes():
                        out.write(chunk)
            saved.append(f"{name}/{collection}")
        return saved

    def down(self) -> None:
        if not self.compose_file.exists():
            return
        self._compose("down", "--volumes", "--remove-orphans")
        # What the stores' containers wrote into the copy (the seeder's marker, the log files) is
        # root's on Linux: given back to this user, so it can be packed and the copy removed.
        subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "-v",
                f"{self.demo}:/demo",
                _CHOWN_IMAGE,
                "chown",
                "-R",
                f"{os.getuid()}:{os.getgid()}",
                "/demo",
            ],
            check=True,
        )


def _control_plane(work: Path, name: str = "cp") -> tuple[str, str, str]:
    """A throwaway embedded control plane, started for ``work``: its host, port and URL."""
    out = subprocess.run(
        [
            sys.executable,
            "-m",
            "provisa.core.control_plane_pg",
            "start",
            str(work / name),
            "--init-sql",
            str(_REPO / "db" / "init.sql"),
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    found = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
    url = (
        f"postgresql+psycopg://provisa:{found['PG_PASSWORD']}@/provisa"
        f"?host={found['PG_HOST']}&port={found['PG_PORT']}"
    )
    return found["PG_HOST"], found["PG_PORT"], url


#: REQ-2068: the regions sandbox tier's deployment config: its platform regions and its boot
#: org's stores and regions, which a regions demo's seeding node runs with too.
_REGIONS_TIER_CONFIG = _REPO / "config" / "provisa-sandbox-regions.yaml"


def _demo_regions(demo: Path) -> list[str]:
    """The regions a demo runs in (demo.yaml ``regions``), its visitor's last; none for a demo of
    one region."""
    regions = (yaml.safe_load((demo / "demo.yaml").read_text()) or {}).get("regions")
    if not regions:
        return []
    return [*regions["headless"], regions["visitor"]]


def _config(work: Path, *, regions: bool = False) -> Path:
    path = work / "config.yaml"
    tier = yaml.safe_load(_REGIONS_TIER_CONFIG.read_text()) if regions else {}
    path.write_text(
        yaml.safe_dump(
            {
                **{key: tier[key] for key in ("platform", "stores", "regions") if key in tier},
                "federation_engine": "duckdb",
                "multitenancy": True,
                "auth": {
                    "provider": "basic",
                    "jwt_secret": "demo-bundle-build-only-secret-not-a-deployment",
                    "assignments_source": "provisa",
                },
                "naming": {"domain_prefix": False, "rules": []},
                "cache": {"enabled": False},
                "domains": [{"id": "ops", "description": "the bundle build's own"}],
                "roles": [],
                "sources": [],
                "tables": [],
            }
        )
    )
    return path


# Where a demo's stored tour answers are asked (REQ-2012): OpenRouter, whose key the build is given
# as OPENROUTER_API_KEY, with the model a demo sandbox's Polly runs (the sandbox.ai_model setting).
_ANSWERING_ENDPOINT = {
    "id": "openrouter",
    "style": "openai",
    "base_url": "https://openrouter.ai/api/v1",
    "api_key_env": "OPENROUTER_API_KEY",
}


def polly_steps(demo: Path) -> list[dict]:
    """The steps of the demo's tour that ask Polly a question (REQ-2012); none without a tour."""
    tour = demo / "tour.yaml"
    if not tour.is_file():
        return []
    return [step for step in yaml.safe_load(tour.read_text())["steps"] if "polly" in step]


def answering_config(work: Path, demo: Path) -> Path:
    """The demo's config.yaml with Polly asked through OpenRouter on the sandbox's model: what the
    build's server runs when the demo's tour asks Polly anything (REQ-2012)."""
    from provisa.core.settings_registry import setting

    config = yaml.safe_load((demo / "config.yaml").read_text())
    config["ai_endpoints"] = [*(config.get("ai_endpoints") or []), _ANSWERING_ENDPOINT]
    config["ai_models"] = {
        **(config.get("ai_models") or {}),
        "mcp_chat": {"vendor": "openrouter", "model": setting("sandbox.ai_model").default},
    }
    # In a directory of its own, beside what the demo declares beside its config.yaml: its
    # replica rows and code archives are read from the directory of the config served.
    from provisa.core.demo_bundles import link_declared

    directory = work / "answering"
    directory.mkdir()
    link_declared(demo, directory)
    path = directory / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    return path


def answer_tour(base: str, demo: Path, steps: list[dict]) -> dict[str, dict[str, str]]:
    """Each Polly step's question asked as the chat panel asks it (scripts/polly_turn), as org_admin:
    ``{step key: {"question", "answer"}}``, the shape provisa.core.demo reads. A question Polly
    does not answer fails the build, naming its step: the bundle would show the step with none."""
    from scripts.polly_turn import ask

    def stream(convo: list[dict]):
        with httpx.stream(
            "POST",
            f"{base}/admin/mcp/chat",
            json={"messages": convo},
            headers={"X-Provisa-Role": "org_admin"},
            timeout=300,
        ) as resp:
            if resp.status_code != 200:
                raise SystemExit(
                    f"{demo.name}: Polly's chat answered {resp.status_code}: {resp.read()}"
                )
            for line in resp.iter_lines():
                if line.startswith("data:"):
                    yield json.loads(line[5:].strip())

    answers = {}
    for step in steps:
        turn = ask(stream, step["polly"])
        if turn.error is not None or not turn.reply.strip():
            raise SystemExit(
                f"{demo.name}: tour step {step['key']}: Polly gave no answer to "
                f"{step['polly']!r}: {turn.error or 'an empty reply'}"
            )
        answers[step["key"]] = {"question": step["polly"], "answer": turn.reply}
    return answers


def _set_up(work: Path, demo: Path, stores: Stores, timeout: float) -> None:
    """Run the demo's setup.yaml against its running stores (REQ-2016): a Provisa server on the
    demo's own config.yaml, with a throwaway control plane of its own, set up by
    scripts/run_demo_setup.py -- what its commands write is then in the stores the build dumps.
    When its tour asks Polly anything, the same server then asks it, and the answers are stored
    beside the tour (REQ-2012): a visitor is shown them, and nothing is asked live."""
    asked = polly_steps(demo)
    if asked and not os.environ.get("OPENROUTER_API_KEY"):
        raise SystemExit(
            f"{demo.name}: its tour asks Polly {len(asked)} question(s), answered at the build "
            "through OpenRouter, and OPENROUTER_API_KEY is not set"
        )
    host, port, url = _control_plane(work, "setup-cp")
    http_port = _free_port()
    # A client step of the setup (run_demo_setup.py) reads the server over Arrow Flight.
    flight_port = _free_port()
    server_env = {
        **_config_env(demo),
        **stores.addresses,
        **({"OPENROUTER_API_KEY": os.environ["OPENROUTER_API_KEY"]} if asked else {}),
        "PROVISA_CONFIG": str(answering_config(work, demo) if asked else demo / "config.yaml"),
        "PROVISA_ENGINE": "duckdb",
        **_protocol_ports(),
        **_duckdb_env(work),
        "FLIGHT_PORT": str(flight_port),
        "ORG_ID": _SETUP_ORG,
        "PROVISA_DATA_DIR": str(work / "setup-data"),
        "PROVISA_REDIS_EMBEDDED": "1",
        "TENANT_DATABASE_URL": url,
        "PLATFORM_DATABASE_URL": url,
        "PG_HOST": host,
        "PG_PORT": port,
        # REQ-862: its refreshes are recorded, and the records of those that build a bitemporal
        # view's history are carried with it (_export_views): what each was built from.
        "OTEL_SDK_DISABLED": "false",
        "PROVISA_TELEMETRY_DIR": str(work / "setup-telemetry"),
    }
    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(http_port),
        ],
        cwd=_REPO,
        env=server_env,
    )
    base = f"http://127.0.0.1:{http_port}"
    try:
        deadline = time.monotonic() + timeout
        while True:
            if server.poll() is not None:
                raise SystemExit(f"the setup server exited early ({server.returncode})")
            try:
                if httpx.get(f"{base}/health", timeout=5).status_code == 200:
                    break
            except httpx.HTTPError:
                pass  # not listening yet; the deadline below bounds the wait
            if time.monotonic() > deadline:
                raise SystemExit(f"{demo.name}: the setup server was not up in {timeout:.0f}s")
            time.sleep(2)
        subprocess.run(
            [sys.executable, str(_REPO / "scripts" / "run_demo_setup.py"), str(demo)]
            + ["--url", base, "--flight", f"grpc://127.0.0.1:{flight_port}"],
            # A client step reaches the demo's stores where this build's compose put them.
            env=stores.env,
            check=True,
        )
        if asked:
            answers = answer_tour(base, demo, asked)
            Path(demo_module.tour_answers_path(str(demo))).write_text(json.dumps(answers, indent=2))
    finally:
        server.terminate()
        server.wait(timeout=60)
        subprocess.run(
            [sys.executable, "-m", "provisa.core.control_plane_pg", "stop", str(work / "setup-cp")],
            check=True,
        )


def _seed(work: Path, demo: Path, stores: Stores, timeout: float) -> None:
    """Land the demo's template from its running stores and write its seed into ``demo`` -- for a
    demo in regions, a seeding node per region, each writing what its region keeps (REQ-2068)."""
    regions = _demo_regions(demo)
    if not regions:
        _seed_node(work, demo, stores, timeout, record=demo / "seeded" / "replicas.json", env={})
        return
    from provisa.api.demo_region_seed import seed_dir

    region_env: dict[str, str] = {}
    for region in regions:
        region_env[f"PROVISA_DEMO_{region.upper()}_PG_URL"] = _control_plane(
            work, f"region-pg-{region}"
        )[2]
        region_env[f"PROVISA_DEMO_{region.upper()}_ADDRESS"] = f"http://127.0.0.1:{_free_port()}"
    redis = _SharedRedis()
    try:
        for region in regions:
            _seed_node(
                work,
                demo,
                stores,
                timeout,
                record=seed_dir(demo, region) / "record.json",
                control_plane=f"cp-{region}",
                env={
                    **region_env,
                    "PROVISA_REGION": region,
                    "REDIS_URL": redis.url,
                    "PROVISA_DATA_DIR": str(work / f"data-{region}"),
                },
            )
    finally:
        redis.stop()
        for region in regions:
            subprocess.run(
                [sys.executable, "-m", "provisa.core.control_plane_pg", "stop"]
                + [str(work / f"region-pg-{region}")],
                check=True,
            )


class _SharedRedis:
    """A fakeredis server the seeding nodes of a demo's regions name as their cache store."""

    def __init__(self) -> None:
        port = _free_port()
        self.url = f"redis://127.0.0.1:{port}/0"
        self._process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import sys, fakeredis; fakeredis.TcpFakeServer(('127.0.0.1', int(sys.argv[1])), "
                "server_type='redis').serve_forever()",
                str(port),
            ]
        )

    def stop(self) -> None:
        self._process.terminate()
        self._process.wait(timeout=30)


def _seed_node(
    work: Path,
    demo: Path,
    stores: Stores,
    timeout: float,
    *,
    record: Path,
    env: dict[str, str],
    control_plane: str = "cp",
) -> None:
    """Run one seeding node, on the control plane ``control_plane``, until it has written
    ``record``."""
    host, port, url = _control_plane(work, control_plane)
    http_port = _free_port()
    server_env = {
        **_config_env(demo),
        **stores.addresses,
        **_protocol_ports(),
        **_duckdb_env(work),
        "ORG_ID": "demobundle",
        "PROVISA_CONFIG": str(_config(work, regions="PROVISA_REGION" in env)),
        "PROVISA_ENGINE": "duckdb",
        "PROVISA_SANDBOX_TIER": "true",
        "PROVISA_DEMO_SEED_EXPORT": "true",
        "PROVISA_NAMED_DEMOS_DIR": str(demo.parent),
        "PROVISA_DATA_DIR": str(work / "data"),
        "PROVISA_REDIS_EMBEDDED": "1",
        "PROVISA_LOGS_ALLOWED_ROOTS": str(demo / "logs"),
        "TENANT_DATABASE_URL": url,
        "PLATFORM_DATABASE_URL": url,
        "PG_HOST": host,
        "PG_PORT": port,
        "OTEL_SDK_DISABLED": "true",
        **env,
    }
    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(http_port),
        ],
        cwd=_REPO,
        env=server_env,
    )
    try:
        deadline = time.monotonic() + timeout
        while not record.exists():
            if server.poll() is not None:
                raise SystemExit(f"the seeding run exited early ({server.returncode})")
            if time.monotonic() > deadline:
                raise SystemExit(f"{demo.name} not seeded in {timeout:.0f}s")
            time.sleep(2)
        httpx.get(f"http://127.0.0.1:{http_port}/health", timeout=10).raise_for_status()
    finally:
        server.terminate()
        server.wait(timeout=60)
        subprocess.run(
            [sys.executable, "-m", "provisa.core.control_plane_pg", "stop"]
            + [str(work / control_plane)],
            check=True,
        )


def _demo_config(demo: Path, stores: Stores):
    """The demo's config.yaml, parsed against its running stores."""
    from provisa.core.config_loader import parse_config_dict, read_config_with_includes
    from provisa.core.demo_secrets import resolving

    saved = dict(os.environ)
    os.environ.update(stores.addresses)
    try:
        # REQ-2095: the secrets it names, from its own secrets.yaml.
        with resolving(demo, demo.name):
            return parse_config_dict(read_config_with_includes(demo / "config.yaml"))
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _replicates(demo: Path, stores: Stores) -> bool:
    """Whether the demo serves any table from a replica, which its seed lands (REQ-2049): a demo's
    own sources are read live where it runs, so a demo of only those has no seed to write."""
    from provisa.api.demo_templates import replicated_tables

    return bool(replicated_tables(_demo_config(demo, stores)))


def _prefetch_connector_bundles(demo: Path) -> None:
    """Fetch the connector bundle of every source the demo reads through one, before any server
    of the build starts: its replica builds then find each bundle whole instead of several fetching
    it at once (``demo_templates.prefetch_connector_bundles`` does the same for the sandbox tier).
    A bundle that cannot be fetched fails the build, by name. Only the sources' types are read
    from the file: a demo in regions has a config that parses only on a region's node."""
    from provisa.core.config_loader import read_config_with_includes
    from provisa.runtime_deps.pgwire_bundles import (
        BUNDLE_CONNECTOR,
        BundleResolver,
        bundle_spec_for,
    )

    raw = read_config_with_includes(demo / "config.yaml")
    types = {str(source["type"]) for source in raw.get("sources") or []}
    resolver = BundleResolver()
    for source_type in sorted(types & set(BUNDLE_CONNECTOR)):
        spec = bundle_spec_for(source_type)
        print(f"connector bundle {spec.artifact_name} {spec.version}: {resolver.resolve(spec)}")


def _export_views(work: Path, demo: Path, stores: Stores) -> None:
    """REQ-1162: the setup server's bitemporal views' append logs, written out beside the seed
    (demo_replicas.export_views) -- the history its steps gave them, which the demo's template and
    each sandbox start from. Read from the setup server's store once it has stopped."""
    from provisa.api.demo_replicas import bitemporal_view_tables

    tables = bitemporal_view_tables(_demo_config(demo, stores))
    if not tables:
        return
    store = work / "setup-data" / "orgs" / _SETUP_ORG / "materialize.duckdb"
    if not store.is_file():
        # The setup server keeps its views in its org's embedded store; one it kept elsewhere is
        # said here with every store this build's work holds (CI run 39156933023).
        held = sorted(str(p.relative_to(work)) for p in work.rglob("*.duckdb"))
        raise SystemExit(
            f"{demo.name}: the setup server's store is not at {store.relative_to(work)}; "
            f"the build holds {held or 'no DuckDB store'}"
        )
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from pathlib import Path; "
            "from provisa.api.demo_replicas import export_views; "
            "export_views(sys.argv[1], Path(sys.argv[2]), sys.argv[3:])",
            _SETUP_ORG,
            str(demo),
            *tables,
        ],
        cwd=_REPO,
        env={
            **os.environ,
            "PROVISA_DATA_DIR": str(work / "setup-data"),
            # REQ-862: where the setup server landed its refresh records.
            "PROVISA_TELEMETRY_DIR": str(work / "setup-telemetry"),
        },
        check=True,
    )


def _commit() -> str:
    return subprocess.run(
        ["git", "-C", str(_REPO), "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip()


def build(args: argparse.Namespace) -> int:
    source = (args.named_dir / args.name).resolve()
    if not (source / "demo.yaml").exists():
        raise SystemExit(f"no named demo {args.name} in {args.named_dir}")
    requires = _requires_provisa(source)
    commit = args.commit or _commit()
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d%H%M")
    manifest = bundles.BundleManifest(args.name, f"{stamp}-{commit[:9]}", commit, requires)
    with tempfile.TemporaryDirectory(prefix="provisa-demo-bundle-") as scratch:
        work = Path(scratch)
        demo = _copy(source, work)
        stores = Stores(demo)
        try:
            stores.up()
            _prefetch_connector_bundles(demo)
            if (demo / "setup.yaml").exists() or polly_steps(demo):
                _set_up(work, demo, stores, args.timeout)
                _export_views(work, demo, stores)
            # REQ-2068: a demo in regions seeds each region's views as well as its replicas. Its
            # config names each region's address and store, which only _seed's nodes are given,
            # so it is not parsed here.
            if _demo_regions(demo) or _replicates(demo, stores):
                _seed(work, demo, stores, args.timeout)
                _refuse_empty_replicas(demo)
            dumped = stores.dump()
        finally:
            stores.down()
        bundle = bundles.pack(demo, manifest)
    args.out.mkdir(parents=True, exist_ok=True)
    path = args.out / f"{args.name}-{manifest.version}.tar.gz"
    path.write_bytes(bundle)
    print(f"built {path} ({len(bundle)} bytes; dumps: {', '.join(dumped) or 'none'})")
    return 0


def _manifest_in(bundle: bytes) -> bundles.BundleManifest:
    with tarfile.open(fileobj=io.BytesIO(gzip.decompress(bundle)), mode="r") as tar:
        member = tar.extractfile(bundles.MANIFEST_FILE)
        if member is None:
            raise SystemExit(f"the bundle has no {bundles.MANIFEST_FILE}")
        return bundles.BundleManifest.read(json.load(member))


def push(args: argparse.Namespace) -> int:
    bundle = args.bundle.read_bytes()
    manifest = _manifest_in(bundle)
    user, token = (
        os.environ["PROVISA_DEMO_REGISTRY_USER"],
        os.environ["PROVISA_DEMO_REGISTRY_TOKEN"],
    )
    digest = bundles.Registry(args.registry, credentials=(user, token)).push(manifest, bundle)
    print(
        f"published {manifest.demo} {manifest.version} as "
        f"{bundles.tag_of(manifest.demo, manifest.version)} and "
        f"{bundles.tag_of(manifest.demo, bundles.LATEST)} ({digest})"
    )
    return 0


def pull(args: argparse.Namespace) -> int:
    print(bundles.pull_into(args.spec, args.into, args.registry))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="A named demo's bundle (REQ-2049).")
    sub = parser.add_subparsers(dest="command", required=True)
    b = sub.add_parser("build", help="build a demo's bundle")
    b.add_argument("name")
    b.add_argument("--out", type=Path, required=True)
    b.add_argument("--named-dir", type=Path, default=_NAMED)
    b.add_argument("--commit", help="the commit built (default: this checkout's HEAD)")
    b.add_argument("--timeout", type=float, default=1800.0)
    p = sub.add_parser("push", help="publish a built bundle")
    p.add_argument("bundle", type=Path)
    p.add_argument("--registry", default=bundles.PUBLISHED_AT)
    q = sub.add_parser("pull", help="pull a demo's bundle and print its directory")
    q.add_argument("spec", help="<name> or <name>@<version>")
    q.add_argument("--into", type=Path, required=True)
    # Where the project publishes, unless PROVISA_DEMO_REGISTRY names another (as the sandbox
    # tier and start-ui-install.sh are told).
    q.add_argument(
        "--registry", default=os.environ.get("PROVISA_DEMO_REGISTRY", bundles.PUBLISHED_AT)
    )
    args = parser.parse_args(argv)
    try:
        return {"build": build, "push": push, "pull": pull}[args.command](args)
    except bundles.BundleError as refused:
        print(f"ERROR: {refused}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
