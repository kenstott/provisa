#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 1b3d7348-8b3e-4a97-b972-a2eaf10d710d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The sandbox tier's demo stores on its node: one running instance of each store type, shared by
every demo version that needs it (REQ-2049).

A demo bundle's ``docker-compose.run.yml`` names its stores. A Postgres (``rdb``), MongoDB
(``document``), Elasticsearch (``search``) or Qdrant (``vector``) store, or a Kafka broker
(``stream``), is not run as a container of the demo's own: its data is loaded into the node's one
instance of that type (``docker-compose.shared.yml``, project ``$DEMO_STORES_PROJECT``), started
the first time a demo needs it and left running. Every other service (a Neo4j store, a demo's
producer or API) runs as the demo version's own compose project; a producer joins the shared
stores' network, where it reaches the shared Kafka as ``kafka``. The shared Kafka listens at one
fixed port: a demo version an earlier release ran with a Kafka of its own, still holding it after a
newer version took over, has its own services taken down before the shared Kafka starts.

Each demo version's data in a shared instance is named with its suffix, ``_<demo>_<8 hex of its
version>``: a Postgres database ``shop`` loads as ``shop_weblogs_1a2b3c4d``, a MongoDB database
``catalog`` as ``catalog_reviews_…``, an Elasticsearch index ``reviews`` as ``reviews_reviews_…``;
a Qdrant collection and a Kafka topic the same. A demo version reads Qdrant with a token of its
own (the variable its store's ``com.provisa.demo.key_env`` names), scoped to its collections.
The suffix reaches the demo's configuration as the variable a store's ``com.provisa.demo.ns_env``
label names (``WEBLOGS_PG_NS``), written into the bundle's stores.env with where the store is.
Loading is idempotent: a namespace is dropped and loaded whole, and marked loaded only at the end;
a version's data is dropped by its suffix, so another demo's, or another version's, is untouched.

Usage (scripts/demo_stores_sync.sh runs it):
    demo_stores.py up <name> <version> <bundle dir>    start, load, write <bundle dir>/stores.env
    demo_stores.py drop <name> <version>               drop the version's data and own services

Environment:
    DEMO_STORES_PROJECT   the shared instances' compose project (provisa-demo-stores on a node)
    DEMO_STORES_SHARED    the shared instances' compose file (docker-compose.shared.yml)
    DEMO_BUNDLES_DIR      the bundles directory on this host (the Elasticsearch instance reads
                          each bundle's snapshot repository from it)
    DEMO_STORES_KAFKA_PORT  the port the shared Kafka listens, is published and advertised at
    DEMO_STORES_NETWORK   the docker network the tier is on; empty: stores are reached on
                          127.0.0.1 at their published ports
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

#: The shared store types, by the service a store's role and image make it.
SHARED_TYPES = {
    ("rdb", "postgres:"): "postgres",
    ("document", "mongo:"): "mongo",
    ("search", "docker.elastic.co/elasticsearch/"): "elasticsearch",
    ("vector", "qdrant/qdrant:"): "qdrant",
    ("stream", "confluentinc/cp-kafka:"): "kafka",
}
#: Each shared type's container port, as the shared compose file runs it.
SHARED_PORTS = {"postgres": "5432", "mongo": "27017", "elasticsearch": "9200", "qdrant": "6333"}
#: What marks a namespace loaded: written last, so a load cut off is loaded again.
LOADED = "provisa-demo-loaded"
_ES_MARKS = "provisa_demo_marks"
_MONGO_MARKS = "provisa_demo_marks"
_NAME = re.compile(r"^[a-z][a-z0-9]*$")


class StoreError(RuntimeError):
    """A demo's stores cannot be brought up or taken down as asked."""


#: How much of a failed store's log its error carries.
LOG_TAIL = "60"


def container_logs(runner: "Runner", cids: list[str]) -> str:
    """The end of each container's log, by container, for an error raised over it."""
    parts = []
    for cid in cids:
        try:
            # Both of its streams: a store says why it exits on either.
            said = runner.run(["sh", "-c", f'docker logs --tail {LOG_TAIL} "$1" 2>&1', "logs", cid])
            parts.append(f"--- docker logs {cid} ---\n{said}")
        except StoreError as unread:
            parts.append(f"--- docker logs {cid} could not be read: {unread} ---")
    return "\n".join(parts) if parts else "(no container to read a log from)"


def namespace(name: str, version: str) -> str:
    """The suffix a demo version's data is named with in a shared instance."""
    if not _NAME.match(name):
        raise StoreError(f"demo name {name!r} is not lower-case letters and digits")
    return f"_{name}_{hashlib.sha256(version.encode()).hexdigest()[:8]}"


@dataclass(frozen=True)
class Store:
    """One service of a demo's run compose file: a shared store's (``kind`` its type) or one the
    demo runs itself (``kind`` None)."""

    service: str
    kind: str | None
    labels: dict
    environment: dict


def stores_of(config: dict) -> list[Store]:
    """The services of a demo's run compose config (``docker compose config --format json``)."""
    found = []
    for service, spec in sorted(config["services"].items()):
        labels = spec.get("labels") or {}
        role = labels.get("com.provisa.demo.role")
        image = str(spec.get("image", ""))
        kind = next(
            (
                t
                for (r, prefix), t in SHARED_TYPES.items()
                if r == role and image.startswith(prefix)
            ),
            None,
        )
        if kind is not None and not labels.get("com.provisa.demo.ns_env"):
            raise StoreError(f"service {service} is a {kind} store with no com.provisa.demo.ns_env")
        environment = spec.get("environment") or {}
        found.append(Store(service, kind, labels, dict(environment)))
    return found


class Runner:
    """Runs a command, its standard input given or a file's; what it printed."""

    def run(self, cmd: list[str], *, stdin: Path | str | None = None) -> str:
        if isinstance(stdin, Path):
            with stdin.open("rb") as handle:
                done = subprocess.run(cmd, stdin=handle, capture_output=True, check=False)
        else:
            done = subprocess.run(
                cmd,
                input=None if stdin is None else stdin.encode(),
                capture_output=True,
                check=False,
            )
        if done.returncode != 0:
            raise StoreError(
                f"{' '.join(cmd[:6])}... failed ({done.returncode}): "
                f"{done.stderr.decode(errors='replace')[-2000:]}"
            )
        return done.stdout.decode(errors="replace")


class Shared:
    """The node's shared instances, one service each of the shared compose file."""

    def __init__(
        self, runner: Runner, project: str, compose_file: str, env: dict[str, str]
    ) -> None:
        self.runner = runner
        self.base = ["docker", "compose", "-p", project, "-f", compose_file]
        self.env = env

    def start(self, kind: str) -> None:
        if kind == "kafka":
            self._free_kafka_port()
        try:
            self.runner.run(["env", *self._env(), *self.base, "up", "-d", "--wait", kind])
        except StoreError as failed:
            # A store that exits before it is healthy says why only in its own log.
            cids = self.runner.run(["env", *self._env(), *self.base, "ps", "-a", "-q", kind])
            raise StoreError(f"{failed}\n{container_logs(self.runner, cids.split())}") from failed

    def _free_kafka_port(self) -> None:
        """The shared Kafka listens at one fixed port, which a demo version an earlier release ran
        with a Kafka of its own still holds after a newer version took over: that version's own
        services are taken down. A holder that is no demo version's is refused by name."""
        port = self.env["DEMO_STORES_KAFKA_PORT"]
        held = self.runner.run(
            [
                "docker",
                "ps",
                "--filter",
                f"publish={port}",
                "--format",
                '{{.Label "com.docker.compose.project"}} {{.Names}}',
            ]
        )
        for line in held.splitlines():
            project, _, container = line.strip().rpartition(" ")
            if not container or project == self.base[3]:
                continue
            if not project.startswith("provisa-demo-"):
                raise StoreError(
                    f"port {port}, where the shared Kafka listens, is held by {container}, "
                    "which is no demo version's"
                )
            self.runner.run(["docker", "compose", "-p", project, "down", "-v", "--remove-orphans"])

    def _env(self) -> list[str]:
        return [f"{k}={v}" for k, v in sorted(self.env.items())]

    def exec(self, kind: str, *cmd: str, stdin: Path | str | None = None) -> str:
        return self.runner.run(
            ["env", *self._env(), *self.base, "exec", "-T", kind, *cmd], stdin=stdin
        )

    def address(self, kind: str, network: str) -> tuple[str, str]:
        """Where a demo's configuration reaches the instance: by its container's name on the
        tier's network, else at its published port on this host."""
        cid = self.runner.run(["env", *self._env(), *self.base, "ps", "-q", kind]).strip()
        if not cid:
            raise StoreError(f"the shared {kind} instance is not running")
        if kind == "kafka":
            # Advertised as it is reached: its container's name on the tier's network, else the
            # loopback, at the one port it listens and is published on.
            port = self.env["DEMO_STORES_KAFKA_PORT"]
            if network:
                name = self.runner.run(["docker", "inspect", "-f", "{{.Name}}", cid]).strip()
                return name.lstrip("/"), port
            return "127.0.0.1", port
        if network:
            name = self.runner.run(["docker", "inspect", "-f", "{{.Name}}", cid]).strip()
            return name.lstrip("/"), SHARED_PORTS[kind]
        published = self.runner.run(
            ["env", *self._env(), *self.base, "port", kind, SHARED_PORTS[kind]]
        ).strip()
        return "127.0.0.1", published.rsplit(":", 1)[1]

    def published(self, kind: str) -> str:
        """The instance's address on this host, where this tool calls its HTTP API."""
        published = self.runner.run(
            ["env", *self._env(), *self.base, "port", kind, SHARED_PORTS[kind]]
        ).strip()
        return f"http://127.0.0.1:{published.rsplit(':', 1)[1]}"

    def network(self) -> str:
        """The shared instances' own network, which a demo's producers join to reach Kafka."""
        return f"{self.base[3]}_default"

    def join(self, kind: str, network: str) -> None:
        if not network:
            return
        cid = self.runner.run(["env", *self._env(), *self.base, "ps", "-q", kind]).strip()
        joined = self.runner.run(
            [
                "docker",
                "inspect",
                "-f",
                "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
                cid,
            ]
        ).split()
        if network not in joined:
            self.runner.run(["docker", "network", "connect", network, cid])


# -- Postgres --------------------------------------------------------------------------------------

_CREATE_ROLE = re.compile(r"^\s*CREATE ROLE (\w+) LOGIN PASSWORD '([^']*)';\s*$", re.MULTILINE)
_CONNECT = re.compile(r"^(\s*\\connect\s+)(\w+)\s*$", re.MULTILINE)


def pg_access(sql: str, suffix: str, databases: list[str]) -> tuple[str, str]:
    """A demo's access script (its roles and their grants, written for a database of its own) as
    two: the roles, created unless the shared instance has them; and the grants, each
    ``\\connect`` to one of its databases naming that database's namespaced copy."""
    roles = "".join(
        f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN "
        f"CREATE ROLE {role} LOGIN PASSWORD '{password}'; END IF; END $$;\n"
        for role, password in _CREATE_ROLE.findall(sql)
    )

    def _connect(match: re.Match) -> str:
        database = match.group(2)
        if database not in databases:
            raise StoreError(f"the access script connects to {database}, which the demo dumps not")
        return f"{match.group(1)}{database}{suffix}"

    return roles, _CONNECT.sub(_connect, _CREATE_ROLE.sub("", sql))


def pg_databases(bundle: Path, store: Store) -> list[tuple[str, Path]]:
    """Each database a Postgres store's bundle dump holds: ``dump/<service>/<database>.sql``."""
    dumps = sorted((bundle / "dump" / store.service).glob("*.sql"))
    if not dumps:
        raise StoreError(f"{bundle}: no dump/{store.service}/<database>.sql to load")
    return [(dump.stem, dump) for dump in dumps]


def pg_loaded(shared: Shared, databases: list[str]) -> bool:
    names = ",".join(f"'{d}'" for d in databases)
    marks = shared.exec(
        "postgres",
        "psql",
        "-U",
        "postgres",
        "-Atc",
        f"SELECT count(*) FROM pg_database WHERE datname IN ({names}) "
        f"AND shobj_description(oid, 'pg_database') = '{LOADED}'",
    ).strip()
    return marks == str(len(databases))


def pg_load(shared: Shared, bundle: Path, store: Store, suffix: str) -> None:
    dumped = pg_databases(bundle, store)
    targets = [f"{database}{suffix}" for database, _ in dumped]
    if pg_loaded(shared, targets):
        return
    psql = ("psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres")
    for (_, dump), target in zip(dumped, targets):
        shared.exec("postgres", *psql, "-c", f'DROP DATABASE IF EXISTS "{target}" WITH (FORCE)')
        shared.exec("postgres", *psql, "-c", f'CREATE DATABASE "{target}"')
        shared.exec("postgres", *psql, "-d", target, stdin=dump)
    access = store.labels.get("com.provisa.demo.access")
    if access:
        roles, grants = pg_access(
            (bundle / access).read_text(), suffix, [database for database, _ in dumped]
        )
        if roles:
            shared.exec("postgres", *psql, "-d", "postgres", stdin=roles)
        # A script naming no database of its own grants in the one its store creates.
        main = f"{store.environment.get('POSTGRES_DB', '')}{suffix}"
        shared.exec("postgres", *psql, "-d", main if main in targets else "postgres", stdin=grants)
    for target in targets:
        shared.exec("postgres", *psql, "-c", f"COMMENT ON DATABASE \"{target}\" IS '{LOADED}'")


def pg_drop(shared: Shared, suffix: str) -> list[str]:
    listed = shared.exec(
        "postgres",
        "psql",
        "-U",
        "postgres",
        "-Atc",
        "SELECT datname FROM pg_database WHERE NOT datistemplate",
    ).split()
    dropped = [d for d in listed if d.endswith(suffix)]
    for database in dropped:
        shared.exec(
            "postgres",
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            "postgres",
            "-c",
            f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)',
        )
    return dropped


# -- MongoDB ---------------------------------------------------------------------------------------

#: A demo's reader is created as the demo's script says, or -- held already by another version --
#: given the roles this version's script names (its databases are this version's).
_MONGO_USER = (
    "function provisaUser(spec) {\n"
    '  const admin = db.getSiblingDB("admin");\n'
    "  if (admin.getUser(spec.user)) { admin.grantRolesToUser(spec.user, spec.roles); }\n"
    "  else { admin.createUser(spec); }\n"
    "}\n"
)
_MONGO_DB = re.compile(r'(\bdb:\s*")(\w+)(")')


def mongo_access(script: str, suffix: str, databases: list[str]) -> str:
    """A demo's MongoDB reader script for the shared instance: its user created or granted the
    roles, each database it names this version's namespaced copy."""

    def _db(match: re.Match) -> str:
        name = match.group(2)
        return (
            f"{match.group(1)}{name}{suffix}{match.group(3)}"
            if name in databases
            else match.group(0)
        )

    created = script.replace('db.getSiblingDB("admin").createUser(', "provisaUser(")
    if "provisaUser(" not in created:
        raise StoreError("the MongoDB reader script creates no user (createUser)")
    return _MONGO_USER + _MONGO_DB.sub(_db, created)


def _mongosh(shared: Shared, script: str) -> str:
    return shared.exec(
        "mongo",
        "mongosh",
        "--quiet",
        "-u",
        "root",
        "-p",
        "root",
        "--authenticationDatabase",
        "admin",
        "--eval",
        script,
    )


def mongo_load(shared: Shared, bundle: Path, store: Store, suffix: str) -> None:
    ns = suffix.lstrip("_")
    marks = f'db.getSiblingDB("{_MONGO_MARKS}").loaded'
    if (
        _mongosh(shared, f'print({marks}.countDocuments({{_id: "{ns}:{store.service}"}}))').strip()
        == "1"
    ):
        return
    archive = bundle / "dump" / f"{store.service}.archive.gz"
    if not archive.is_file():
        raise StoreError(f"{bundle}: no dump/{store.service}.archive.gz to restore")
    mongo_drop(shared, suffix)
    shared.exec(
        "mongo",
        "mongorestore",
        "--quiet",
        "-u",
        "root",
        "-p",
        "root",
        "--authenticationDatabase",
        "admin",
        "--gzip",
        "--archive",
        "--nsExclude=admin.*",
        "--nsExclude=config.*",
        "--nsExclude=local.*",
        "--nsFrom=$db$.$col$",
        f"--nsTo=$db${suffix}.$col$",
        stdin=archive,
    )
    databases = [
        d[: -len(suffix)]
        for d in _mongosh(
            shared, "db.adminCommand({listDatabases: 1}).databases.forEach(d => print(d.name))"
        ).split()
        if d.endswith(suffix)
    ]
    access = store.labels.get("com.provisa.demo.access")
    if access:
        _mongosh(shared, mongo_access((bundle / access).read_text(), suffix, databases))
    _mongosh(
        shared,
        f'{marks}.replaceOne({{_id: "{ns}:{store.service}"}}, {{_id: "{ns}:{store.service}"}}, {{upsert: true}})',
    )


def mongo_drop(shared: Shared, suffix: str) -> list[str]:
    listed = _mongosh(
        shared, "db.adminCommand({listDatabases: 1}).databases.forEach(d => print(d.name))"
    ).split()
    dropped = [d for d in listed if d.endswith(suffix)]
    for database in dropped:
        _mongosh(shared, f'db.getSiblingDB("{database}").dropDatabase()')
    ns = suffix.lstrip("_")
    _mongosh(shared, f'db.getSiblingDB("{_MONGO_MARKS}").loaded.deleteMany({{_id: /^{ns}:/}})')
    return dropped


# -- Elasticsearch ---------------------------------------------------------------------------------


def _es(shared: Shared, method: str, path: str, body: dict | None = None) -> str:
    cmd = ["curl", "-sS", "-f", "-X", method, "-H", "Content-Type: application/json"]
    if body is not None:
        cmd += ["-d", json.dumps(body)]
    return shared.exec("elasticsearch", *cmd, f"http://localhost:9200{path}")


def es_load(shared: Shared, name: str, version: str, store: Store, suffix: str) -> None:
    ns = suffix.lstrip("_")
    marked = shared.exec(
        "elasticsearch",
        "curl",
        "-s",
        "-o",
        "/dev/null",
        "-w",
        "%{http_code}",
        f"http://localhost:9200/{_ES_MARKS}/_doc/{ns}:{store.service}",
    ).strip()
    if marked == "200":
        return
    es_drop(shared, suffix)
    repository = f"/bundles/{name}/{version}/dump/{store.service}"
    _es(
        shared,
        "PUT",
        f"/_snapshot/{ns}",
        {"type": "fs", "settings": {"location": repository, "readonly": True}},
    )
    _es(
        shared,
        "POST",
        f"/_snapshot/{ns}/data/_restore?wait_for_completion=true",
        {
            "indices": "*,-.*",
            "include_global_state": False,
            "rename_pattern": "(.+)",
            "rename_replacement": f"$1{suffix}",
        },
    )
    # Read only to every reader, as the demo's own store was.
    _es(shared, "PUT", f"/*{suffix}/_settings", {"index.blocks.write": True})
    _es(shared, "PUT", f"/{_ES_MARKS}/_doc/{ns}:{store.service}?refresh=true", {"loaded": True})


def es_drop(shared: Shared, suffix: str) -> None:
    ns = suffix.lstrip("_")
    _es(shared, "DELETE", f"/*{suffix}?allow_no_indices=true&expand_wildcards=all")
    shared.exec(
        "elasticsearch",
        "curl",
        "-s",
        "-o",
        "/dev/null",
        "-X",
        "DELETE",
        f"http://localhost:9200/_snapshot/{ns}",
    )
    shared.exec(
        "elasticsearch",
        "curl",
        "-s",
        "-o",
        "/dev/null",
        "-X",
        "POST",
        "-H",
        "Content-Type: application/json",
        "-d",
        json.dumps({"query": {"prefix": {"_id": f"{ns}:"}}}),
        f"http://localhost:9200/{_ES_MARKS}/_delete_by_query?refresh=true&conflicts=proceed",
    )


# -- Qdrant ----------------------------------------------------------------------------------------


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def qdrant_token(key: str, collections: list[str]) -> str:
    """A demo version's key to the shared Qdrant: a token signed with the instance's key (its
    JWT access control), reading those collections alone."""
    header = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    claims = {"access": [{"collection": c, "access": "r"} for c in collections]}
    body = _b64(json.dumps(claims, separators=(",", ":")).encode())
    signed = hmac.new(key.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest()
    return f"{header}.{body}.{_b64(signed)}"


def qdrant_collections(bundle: Path, store: Store) -> list[tuple[str, Path]]:
    """Each collection a Qdrant store's bundle dump holds: ``dump/<service>/<collection>.snapshot``."""
    snapshots = sorted((bundle / "dump" / store.service).glob("*.snapshot"))
    if not snapshots:
        raise StoreError(f"{bundle}: no dump/{store.service}/<collection>.snapshot to recover")
    return [(snapshot.stem, snapshot) for snapshot in snapshots]


def _qdrant(shared: Shared, key: str, *args: str) -> str:
    return shared.runner.run(["curl", "-sS", "-H", f"api-key: {key}", *args])


def qdrant_load(shared: Shared, bundle: Path, store: Store, suffix: str, key: str) -> list[str]:
    """Recover each collection the bundle dumped as its namespaced copy; the copies' names."""
    base = shared.published("qdrant")
    dumped = qdrant_collections(bundle, store)
    targets = [f"{collection}{suffix}" for collection, _ in dumped]
    codes = [
        _qdrant(shared, key, "-o", "/dev/null", "-w", "%{http_code}", f"{base}/collections/{t}")
        for t in targets
    ]
    if all(code.strip() == "200" for code in codes):
        return targets
    for (_, snapshot), target in zip(dumped, targets):
        _qdrant(shared, key, "-f", "-X", "DELETE", f"{base}/collections/{target}")
        _qdrant(
            shared,
            key,
            "-f",
            "-X",
            "POST",
            "-F",
            f"snapshot=@{snapshot}",
            f"{base}/collections/{target}/snapshots/upload?priority=snapshot&wait=true",
        )
    return targets


def qdrant_drop(shared: Shared, suffix: str, key: str) -> list[str]:
    base = shared.published("qdrant")
    listed = json.loads(_qdrant(shared, key, "-f", f"{base}/collections"))
    names = [c["name"] for c in listed["result"]["collections"]]
    dropped = [n for n in names if n.endswith(suffix)]
    for name in dropped:
        _qdrant(shared, key, "-f", "-X", "DELETE", f"{base}/collections/{name}")
    return dropped


# -- Kafka -----------------------------------------------------------------------------------------


def kafka_drop(shared: Shared, suffix: str) -> None:
    """Delete a demo version's topics: each ends with its suffix."""
    shared.exec(
        "kafka",
        "kafka-topics",
        "--bootstrap-server",
        "localhost:9092",
        "--delete",
        "--if-exists",
        "--topic",
        f".*{re.escape(suffix)}",
    )


# -- a demo version --------------------------------------------------------------------------------


def _own_project(name: str, version: str) -> str:
    return f"provisa-demo-{name}-{version}"


class Demo:
    """One demo version's stores on this node."""

    def __init__(self, runner: Runner, name: str, version: str, bundle: Path | None) -> None:
        self.runner = runner
        self.name = name
        self.version = version
        self.bundle = bundle
        self.suffix = namespace(name, version)
        self.network = os.environ.get("DEMO_STORES_NETWORK", "")
        self.shared = shared_instances(runner)
        self.project = _own_project(name, version)
        # Several versions' own services run at once: the port variables they name are left empty,
        # which a run file's ${VAR-default} turns into a port docker picks.
        self.blank_ports: list[str] = []
        # The names of its shared data, which its own services (a producer) name it by too.
        self.names: dict[str, str] = {}

    def _compose(self, *args: str) -> list[str]:
        assert self.bundle is not None
        return [
            "env",
            *(f"{name}=" for name in self.blank_ports),
            *(f"{name}={value}" for name, value in sorted(self.names.items())),
            "docker",
            "compose",
            "-p",
            self.project,
            "-f",
            str(self.bundle / "docker-compose.run.yml"),
            *args,
        ]

    def stores(self) -> list[Store]:
        # Not interpolated: the file names its stores' data by variables (<STEM>_NS) this reading
        # is what sets, so it cannot need them set (required as ${VAR?...} where a service reads
        # one, a producer's TOPIC_SUFFIX). A store's own labels and image are literal.
        config = self._compose("config", "--format", "json", "--no-interpolate")
        return stores_of(json.loads(self.runner.run(config)))

    def up(self) -> dict[str, str]:
        """Start what the demo version needs, load its data, and say where its stores are."""
        assert self.bundle is not None
        stores = self.stores()
        self.blank_ports = sorted(
            s.labels["com.provisa.demo.port_env"]
            for s in stores
            if s.kind is None and s.labels.get("com.provisa.demo.port_env")
        )
        # Every store's data names, before any compose command reads them: a shared store's data
        # is under the version's suffix, a store the demo runs itself holds it under its own names
        # (empty: a Postgres with pgvector, which no shared instance is, runs so).
        for store in stores:
            ns_env = store.labels.get("com.provisa.demo.ns_env")
            if ns_env:
                self.names[ns_env] = "" if store.kind is None else self.suffix
        found: dict[str, str] = dict(self.names)
        for store in stores:
            if store.kind is None:
                continue
            self.shared.start(store.kind)
            self.shared.join(store.kind, self.network)
            if store.kind == "postgres":
                pg_load(self.shared, self.bundle, store, self.suffix)
            elif store.kind == "mongo":
                mongo_load(self.shared, self.bundle, store, self.suffix)
            elif store.kind == "elasticsearch":
                es_load(self.shared, self.name, self.version, store, self.suffix)
            elif store.kind == "qdrant":
                key = self.shared.env["DEMO_STORES_QDRANT_KEY"]
                collections = qdrant_load(self.shared, self.bundle, store, self.suffix, key)
                key_env = store.labels.get("com.provisa.demo.key_env")
                if not key_env:
                    raise StoreError(
                        f"Qdrant store {store.service} names no com.provisa.demo.key_env"
                    )
                found[key_env] = qdrant_token(key, collections)
            # Kafka holds no data of a bundle's: its topics are the producers', named with the suffix.
            host, port = self.shared.address(store.kind, self.network)
            found.update(self._where(store, host, port))
        own = [s.service for s in stores if s.kind is None]
        if own:
            # The demo's own services alone: a shared store it names is not started for it.
            if any(s.kind == "kafka" for s in stores):
                # Its producers reach the shared Kafka as "kafka", on the shared stores' network,
                # joined before they first start: started on their own network alone they cannot
                # resolve it, exit and are restarted, and a container between restarts has no
                # network sandbox to connect (the streaming and provisioning walks).
                self.runner.run(self._compose("up", "--no-start", "--no-deps", *own))
                for cid in self.runner.run(self._compose("ps", "-a", "-q", *own)).split():
                    self._connect(cid, self.shared.network())
            up = self._compose("up", "-d", "--wait", "--no-deps", *own)
            try:
                self.runner.run(up)
            except StoreError:
                # A store that never turns healthy is one whose first start was cut off before its
                # load wrote its marker: it would never load the rest, as it loads only into an
                # empty volume. The data is the bundle's, so its volumes are recreated, once.
                self.runner.run(self._compose("down", "-v", "--remove-orphans"))
                try:
                    self.runner.run(up)
                except StoreError as failed:
                    cids = self.runner.run(self._compose("ps", "-a", "-q", *own)).split()
                    raise StoreError(f"{failed}\n{container_logs(self.runner, cids)}") from failed
            # Started for this boot alone: docker restarting at the node's next start starts none.
            cids = self.runner.run(self._compose("ps", "-q")).split()
            if cids:
                self.runner.run(["docker", "update", "--restart", "no", *cids])
            for store in stores:
                if store.kind is None and store.labels.get("com.provisa.demo.host_env"):
                    found.update(self._where(store, *self._own_address(store)))
        return found

    def _where(self, store: Store, host: str, port: str) -> dict[str, str]:
        labels = store.labels
        found = {labels["com.provisa.demo.host_env"]: host}
        if labels.get("com.provisa.demo.port_env"):
            found[labels["com.provisa.demo.port_env"]] = port
        return found

    def _connect(self, cid: str, network: str) -> None:
        joined = self.runner.run(
            [
                "docker",
                "inspect",
                "-f",
                "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
                cid,
            ]
        ).split()
        if network not in joined:
            self.runner.run(["docker", "network", "connect", network, cid])

    def _own_address(self, store: Store) -> tuple[str, str]:
        port = store.labels["com.provisa.demo.port"]
        cid = self.runner.run(self._compose("ps", "-q", store.service)).strip()
        if not cid:
            raise StoreError(f"{self.project}: service {store.service} is not running")
        if self.network:
            joined = self.runner.run(
                [
                    "docker",
                    "inspect",
                    "-f",
                    "{{range $k, $v := .NetworkSettings.Networks}}{{$k}} {{end}}",
                    cid,
                ]
            ).split()
            if self.network not in joined:
                self.runner.run(["docker", "network", "connect", self.network, cid])
            name = self.runner.run(["docker", "inspect", "-f", "{{.Name}}", cid]).strip()
            return name.lstrip("/"), port
        published = self.runner.run(self._compose("port", store.service, port)).strip()
        return "127.0.0.1", published.rsplit(":", 1)[1]

    def drop(self, shared_kinds: set[str]) -> None:
        """Remove the version: its data from each shared instance running, its own services."""
        if "postgres" in shared_kinds:
            pg_drop(self.shared, self.suffix)
        if "mongo" in shared_kinds:
            mongo_drop(self.shared, self.suffix)
        if "elasticsearch" in shared_kinds:
            es_drop(self.shared, self.suffix)
        if "qdrant" in shared_kinds:
            qdrant_drop(self.shared, self.suffix, self.shared.env["DEMO_STORES_QDRANT_KEY"])
        if "kafka" in shared_kinds:
            kafka_drop(self.shared, self.suffix)
        self.runner.run(["docker", "compose", "-p", self.project, "down", "-v", "--remove-orphans"])


def qdrant_key(bundles: Path) -> str:
    """The shared Qdrant's key: made once on the node, kept beside the bundles (read by this tool
    alone), never configured -- each demo version reads with a token it signs."""
    path = bundles / ".demo-stores" / "qdrant.key"
    if not path.is_file():
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        staged = path.with_name(".qdrant.key.writing")
        staged.write_text(secrets.token_hex(32))
        staged.chmod(0o600)
        staged.replace(path)
    return path.read_text().strip()


def shared_instances(runner: Runner) -> Shared:
    """The node's shared instances, as its environment names them."""
    bundles = os.environ["DEMO_BUNDLES_DIR"]
    return Shared(
        runner,
        os.environ["DEMO_STORES_PROJECT"],
        os.environ["DEMO_STORES_SHARED"],
        {
            "DEMO_BUNDLES_DIR": bundles,
            "DEMO_STORES_QDRANT_KEY": qdrant_key(Path(bundles)),
            "DEMO_STORES_KAFKA_PORT": os.environ["DEMO_STORES_KAFKA_PORT"],
            "DEMO_STORES_NETWORK": os.environ.get("DEMO_STORES_NETWORK", ""),
        },
    )


def running_shared(runner: Runner) -> set[str]:
    """The shared instances running on this node."""
    shared = shared_instances(runner)
    listed = runner.run(
        ["env", *shared._env(), *shared.base, "ps", "--services", "--status", "running"]
    )
    return set(listed.split())


def _write_atomically(path: Path, text: str) -> None:
    staged = path.with_name(f".{path.name}.writing")
    staged.write_text(text)
    staged.replace(path)


def main(argv: list[str]) -> int:
    runner = Runner()
    if len(argv) == 4 and argv[0] == "up":
        name, version, bundle = argv[1], argv[2], Path(argv[3])
        found = Demo(runner, name, version, bundle).up()
        _write_atomically(
            bundle / "stores.env", "".join(f"{k}={v}\n" for k, v in sorted(found.items()))
        )
        return 0
    if len(argv) == 3 and argv[0] == "drop":
        Demo(runner, argv[1], argv[2], None).drop(running_shared(runner))
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
