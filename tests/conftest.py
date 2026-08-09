# Copyright (c) 2026 Kenneth Stott
# Canary: be5aefb1-047c-45bf-bbd3-3d7280b5f906
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

import os
import socket
import subprocess
import sys
import time

import asyncpg
import pytest
import pytest_asyncio
import trino

from provisa.compiler import naming as _naming
from tests.env_creds import load_provider_creds
from tests.itest_stack import (
    COMPOSE_ARGS,
    acquire_stack_slot,
    reap_orphaned_projects,
    release_stack_slot,
)

# Before ANY test module is imported: the cloud-DW e2es gate on os.environ inside module-level
# skipif conditions evaluated at collection time, so live .env creds must be present now or those
# tests report as skipped while the credentials sit unused on disk. See tests/env_creds.py.
load_provider_creds()

# A native engine caches/lands into a materialization store, which MUST exist (the engine invariant).
# Positive-case tests therefore define one, built from the same PG_* the test PG uses; a negative
# test that asserts the "no store" error overrides it. setdefault so an explicit outer value wins.
os.environ.setdefault(
    "PROVISA_MATERIALIZE_URL",
    "postgresql://{u}:{pw}@{h}:{p}/{db}".format(
        u=os.environ.get("PG_USER", "provisa"),
        pw=os.environ.get("PG_PASSWORD", "provisa"),
        h=os.environ.get("PG_HOST", "localhost"),
        p=os.environ.get("PG_PORT", "5432"),
        db=os.environ.get("PG_DATABASE", "provisa"),
    ),
)

_REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")


def _ensure_odbcsysini() -> None:
    """Make the installed SQL Server ODBC driver discoverable by pyodbc for requires_sqlserver tests.

    Homebrew's unixODBC ships a broken doubled SYSINI path, so the driver registered in
    ``<brew>/etc/odbcinst.ini`` isn't found unless ODBCSYSINI points at that DIRECTORY. If pyodbc can
    already see a SQL Server driver, leave the environment alone; otherwise probe the standard brew
    prefixes for an odbcinst.ini that registers one and set ODBCSYSINI to its directory. Never edits the
    user's shell — only this process's env.
    """
    _cur = os.environ.get("ODBCSYSINI")
    # ODBCSYSINI must name the DIRECTORY holding odbcinst.ini. A value pointing AT the odbcinst.ini file
    # (Homebrew's broken doubled SYSINI default) makes unixODBC find no drivers — normalize to its dir.
    # Must run before the first pyodbc call, which caches the SYSINI. Unset is fine (host default works).
    if _cur and os.path.isfile(_cur):
        os.environ["ODBCSYSINI"] = os.path.dirname(_cur)


_ensure_odbcsysini()


def _install_subprocess_coverage_pth() -> None:
    """Drop a .pth in site-packages that fires coverage.process_startup() at every
    interpreter start. Combined with COVERAGE_PROCESS_START (set by the server-
    spawning helpers only under a coverage run), it makes spawned server subprocesses
    measure their own line coverage into a parallel .coverage.* that combine merges —
    so out-of-process Bolt/Flight/gRPC/pgwire/governed-pipeline code is counted.
    process_startup() is a no-op unless COVERAGE_PROCESS_START is set, so the .pth is
    inert for ordinary (non-coverage) runs."""
    try:
        import sysconfig

        site_dir = sysconfig.get_paths()["purelib"]
        pth = os.path.join(site_dir, "provisa_subprocess_coverage.pth")
        if not os.path.exists(pth):
            with open(pth, "w", encoding="utf-8") as f:
                f.write("import coverage; coverage.process_startup()\n")
    except OSError:
        pass  # read-only site-packages (e.g. CI wheel cache) — subprocess cov just off


_install_subprocess_coverage_pth()


def _server_coverage_env() -> dict:
    """COVERAGE_PROCESS_START for a spawned server subprocess, but only when the
    parent runs under coverage (else the subprocess would needlessly self-measure)."""
    try:
        import coverage

        if coverage.Coverage.current() is None:
            return {}
    except Exception:
        return {}
    return {"COVERAGE_PROCESS_START": os.path.abspath(os.path.join(_REPO_ROOT, "pyproject.toml"))}


# The integration tier provisions its OWN isolated stack — a dedicated compose
# project on ephemeral host ports, its own network — so it NEVER touches the local
# dev stack (the `provisa` project on default ports 5432/8080/9000/…). Core and
# marker services share this one project's default network, so Trino reaches
# kafka/mongo/etc. by service name without any external (dev) network.
# Session isolation: the compose project owns containers/networks/volumes, and every run tears
# its project down before `up` — so any SHARED name makes concurrent sessions kill each other's
# stacks mid-run (observed: `down` from one session SIGTERMs the other's Trino, failing its
# `--wait` with exit 143). Ports are per-run ephemeral and the project name is per-SESSION; see
# tests/itest_stack.py, which owns the name and reaps projects orphaned by killed sessions.
def _needs_shared_stack(items) -> bool:
    """True when this session has any item that talks to the shared compose stack.

    ``tests/unit`` touches no external service and ``tests/mvmvp`` brings up its own isolated
    Postgres (docker-compose.mvmvp.yml); everything else — integration, steps/BDD, features, e2e —
    runs against the isolated core stack. This is the SAME predicate ``_wait_for_trino`` uses to
    decide whether to block on Trino. They used to disagree: provisioning keyed on ``"integration"
    in fspath`` while the wait keyed on "outside unit/mvmvp", so a plain
    ``pytest tests/steps/steps_security.py`` never brought a stack up, then sat 360s on the Trino
    probe and errored every item at setup with connection-refused.
    """
    if not items:
        return False
    here = os.path.dirname(__file__)
    self_provisioned = tuple(os.path.join(here, sub) + os.sep for sub in ("unit", "mvmvp"))
    return not all(str(item.path).startswith(self_provisioned) for item in items)


_ITEST_COMPOSE_ARGS = list(COMPOSE_ARGS)

_MARKER_SERVICES: dict[str, list[str]] = {
    "requires_kafka": ["kafka", "schema-registry"],
    "requires_debezium": ["kafka", "schema-registry", "debezium-connect"],
    "requires_mongodb": ["mongodb"],
    "requires_elasticsearch": ["elasticsearch"],
    "requires_neo4j": ["neo4j"],
    "requires_sparql": ["fuseki"],
    "requires_prometheus": ["prometheus"],
    "requires_mariadb": ["mariadb"],
    "requires_tidb": ["tidb"],
    "requires_cockroachdb": ["cockroachdb"],
    "requires_greenplum": ["greenplum"],
    "requires_yugabytedb": ["yugabytedb"],
    "requires_sqlserver": ["sqlserver"],
    "requires_oracle": ["oracle"],
    "requires_singlestore": ["singlestore"],
    "requires_cassandra": ["cassandra"],
    "requires_clickhouse": ["clickhouse"],
    "requires_firebird": ["firebird"],
    "requires_exasol": ["exasol"],
    "requires_splunk": ["splunk"],
    "requires_airport": ["airport-shim"],
    "requires_pinot": ["pinot"],
    "requires_druid": [
        "druid-zookeeper",
        "druid-metadata",
        "druid-coordinator",
        "druid-historical",
        "druid-middlemanager",
        "druid",
    ],
    # Hive metastore (local warehouse shared with Trino) — seeding is done through Trino, so the
    # test process never reaches the metastore directly (no host-published port needed).
    "requires_hive": ["hive-metastore"],
    # S3-backed Hive: the metastore plus the core MinIO (reused) hold the table data on s3a://.
    "requires_hive_s3": ["hive-s3-metastore"],
    # REQ-1069 metadata-export targets. Marquez brings its own Postgres; OpenMetadata brings its
    # own MySQL and reuses the core-stack Elasticsearch for search.
    "requires_marquez": ["marquez-db", "marquez"],
    "requires_openmetadata": ["openmetadata-db", "elasticsearch", "openmetadata"],
    # Atlas embeds its own HBase and Solr, so the one service is the whole target.
    "requires_atlas": ["atlas"],
}
# zaychik is the Arrow Flight terminal the in-process app connects to for Flight/CTAS
# redirects; without it Flight-dependent integration tests fail with connection-refused.
_CORE_SERVICES = ["postgres", "trino", "redis", "pgbouncer", "minio", "zaychik"]

# Heavy relational/analytic engines, each mostly amd64-emulated on arm64. These are NOT
# session-provisioned: bringing every collected marker service up at once (the session
# manager below) put ~15 heavyweight DBs on the Docker VM simultaneously, exceeding its
# memory and OOM-killing containers (cassandra/yugabytedb/splunk exit 137) so the whole
# stack aborted. Instead each heavy-marked test brings up ONLY its own engine for the
# duration of that test and tears it down after (see _heavy_db_service), so at most one
# heavy DB is live at any moment regardless of how many the suite exercises.
_HEAVY_MARKERS = frozenset(
    {
        "requires_oracle",
        "requires_sqlserver",
        "requires_greenplum",
        "requires_yugabytedb",
        "requires_cockroachdb",
        "requires_tidb",
        "requires_mariadb",
        "requires_cassandra",
        "requires_clickhouse",
        "requires_firebird",
        "requires_singlestore",
        "requires_exasol",
        "requires_splunk",
        "requires_pinot",
        "requires_druid",
        "requires_hive",
        "requires_airport",
        # A 1G JVM plus MySQL plus the Elasticsearch cluster. Session-provisioning it would leave
        # that resident for every unrelated integration test in the run.
        "requires_openmetadata",
        # An embedded HBase, an embedded Solr and the Atlas JVM in one container.
        "requires_atlas",
    }
)

# Host-published services whose ephemeral port the in-process app / test clients
# read from these env vars. compose interpolates the same ${VAR} at `up` time.
_ITEST_PORT_ENV = [
    "PG_PORT",
    "TRINO_PORT",
    "REDIS_PORT",
    "MINIO_PORT",
    "MINIO_CONSOLE_PORT",
    "ZAYCHIK_PORT",
    "MONGO_PORT",
    "NEO4J_HTTP_PORT",
    "NEO4J_BOLT_PORT",
    "KAFKA_HOST_PORT",
    "ELASTICSEARCH_PORT",
    "FUSEKI_PORT",
    "SCHEMA_REGISTRY_PORT",
    "DEBEZIUM_PORT",
    "MARIADB_PORT",
    "TIDB_PORT",
    "COCKROACHDB_PORT",
    "GREENPLUM_PORT",
    "YUGABYTEDB_PORT",
    "SQLSERVER_PORT",
    "ORACLE_PORT",
    "SINGLESTORE_PORT",
    "CASSANDRA_PORT",
    "CLICKHOUSE_PORT",
    "FIREBIRD_PORT",
    "EXASOL_PORT",
    "SPLUNK_MGMT_PORT",
    "SPLUNK_HEC_PORT",
    "AIRPORT_PORT",
    "PINOT_CONTROLLER_PORT",
    "PINOT_BROKER_PORT",
    "DRUID_BROKER_PORT",
    "DRUID_COORD_PORT",
    "MARQUEZ_PORT",
    "OPENMETADATA_PORT",
    "ATLAS_PORT",
]


def _reserve_free_ports(n: int) -> list[int]:
    """Return n DISTINCT free TCP ports (sockets held open together so the kernel
    hands out a different port for each)."""
    socks: list[socket.socket] = []
    try:
        for _ in range(n):
            s = socket.socket()
            s.bind(("127.0.0.1", 0))
            socks.append(s)
        return [s.getsockname()[1] for s in socks]
    finally:
        for s in socks:
            s.close()


def _allocate_itest_ports() -> None:
    """Assign every isolated-stack host port to a fresh ephemeral port and export the
    URL-shaped env the in-process app reads, so the app never hits the dev stack."""
    # Reserve one extra port for the isolated Provisa server (PROVISA_URL) IN THE SAME batch,
    # so it is guaranteed distinct from every docker-service host port. Reserving it in a second
    # _reserve_free_ports() call would race — those sockets are already closed, so the OS could
    # hand back a port already assigned to a service, colliding at `up` time.
    _ports = _reserve_free_ports(len(_ITEST_PORT_ENV) + 1)
    for name, port in zip(_ITEST_PORT_ENV, _ports):
        os.environ[name] = str(port)
    _provisa_server_port = _ports[-1]
    # The isolated postgres is provisa/provisa/provisa by construction (see
    # docker-compose.core.yml POSTGRES_*). Export the full credential env — not just
    # the port — so in-process config loads that resolve ${env:PG_PASSWORD} (e.g.
    # sample_config.yaml) succeed without a fixed fallback in the config itself.
    os.environ.setdefault("PG_HOST", "localhost")
    os.environ.setdefault("PG_USER", "provisa")
    os.environ.setdefault("PG_PASSWORD", "provisa")
    os.environ.setdefault("PG_DATABASE", "provisa")
    # ClickHouse is a direct-driver source: the test client reads it over HTTP at
    # localhost:${CLICKHOUSE_PORT}. Export the host + the credential the compose service is
    # configured with (docker-compose.test.yml sets default/provisa so the default user is
    # reachable off-loopback) so the requires_clickhouse e2e runs instead of skipping.
    os.environ.setdefault("CLICKHOUSE_HOST", "localhost")
    os.environ.setdefault("CLICKHOUSE_PASSWORD", "provisa")
    # The in-process app reaches Postgres on the reserved host port above; the Trino coordinator is
    # a container on the compose network and cannot resolve that at all. The Provisa-owned catalogs
    # (provisa_admin/otel/results) are JDBC URLs the coordinator dials, so they take the internal
    # service address instead — the same split the fixture configs already make when they set a
    # source host of `postgres` while the app's own URL says `localhost`.
    os.environ.setdefault("PROVISA_ENGINE_CONTROL_PLANE_HOST", "postgres")
    os.environ.setdefault("PROVISA_ENGINE_CONTROL_PLANE_PORT", "5432")
    os.environ["REDIS_URL"] = f"redis://localhost:{os.environ['REDIS_PORT']}/0"
    _minio = f"localhost:{os.environ['MINIO_PORT']}"
    os.environ["PROVISA_OTEL_S3_ENDPOINT"] = f"http://{_minio}"
    os.environ["MINIO_ENDPOINT"] = _minio  # host-side minio clients (e.g. infra bdd)
    # Redirect/result-spill S3 path (test_redirect_encryption_minio) reads this; wire it to the
    # isolated stack's minio so the encryption round-trip runs instead of skipping on :9000.
    os.environ["PROVISA_REDIRECT_ENDPOINT"] = f"http://{_minio}"
    # Host-side kafka clients read these; point them at the isolated broker's port.
    _kafka = f"localhost:{os.environ['KAFKA_HOST_PORT']}"
    os.environ["KAFKA_BOOTSTRAP"] = _kafka
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = _kafka
    # The requires_provisa_server fixture reads PROVISA_URL and REUSES any server already
    # listening there. Defaulting to :8000 means a developer's running dev instance (config/
    # provisa-install.yaml on dev PG:5432) gets reused, and its governance/routing return 403s
    # and timeouts for the isolated tests. Tests must NEVER touch dev ports: pin PROVISA_URL to
    # a private ephemeral port so the fixture always starts its OWN clean sample_config server
    # and can never latch onto :8000. `setdefault` lets an explicit orchestrator (CI) override.
    _server_url = f"http://localhost:{_provisa_server_port}"
    os.environ.setdefault("PROVISA_URL", _server_url)
    # Some BDD step modules read PROVISA_BASE_URL instead of PROVISA_URL; point both at the same
    # private test server so no test path falls back to the dev port.
    os.environ.setdefault("PROVISA_BASE_URL", os.environ["PROVISA_URL"])


# Allocate + export the isolated-stack ports at IMPORT time — before any test module
# is collected, so module-level constants like `REDIS_URL = os.environ.get(...)` and
# the in-process apps capture the ephemeral ports rather than the dev defaults. The
# stack itself is only provisioned when the run actually contains integration tests
# (pytest_collection_finish); an external stack keeps whatever it published.
if not os.environ.get("PYTEST_NO_DOCKER") and not os.environ.get("PROVISA_E2E_EXTERNAL_STACK"):
    _allocate_itest_ports()


# The Calcite-derived Trino connector plugins the compose stack bind-mounts, and the Maven
# coordinates they come from. Kept in step with .github/workflows/build-dmg.yml's
# download-plugins job and ui-e2e-trino.yml — CI and the test harness must fetch the same
# build, or a connector behaves differently here than in the shipped image.
#
# Maven Central, not the kenstott/calcite GitHub release: the engine-v0.32.0 release was
# deleted mid-CI-run and every download started 404ing. Central is immutable once published,
# and unlike GitHub Packages it serves anonymously (Packages returns 401 even for public
# artifacts). Each published jar is a shaded fat jar carrying its own
# META-INF/services/io.trino.spi.Plugin and no io.trino.spi classes, so a single jar dropped
# into trino/plugins/<name>/ is a complete plugin directory.
_TRINO_PLUGIN_MAVEN = "https://repo1.maven.org/maven2/io/simpleishard"
_TRINO_PLUGIN_VERSION = "0.70.0"
_TRINO_PLUGINS = ("trino-sharepoint", "trino-splunk", "trino-file")


def _download_trino_plugins(plugins: str, missing: list[str]) -> None:
    """Fetch the plugin jars for ``missing`` into ``plugins``.

    Tests provision the services they need, and Trino is no exception: without these jars it
    aborts with "No service providers of type io.trino.spi.Plugin", which surfaces only as an
    unhealthy container two minutes into the run. Downloading here turns that into a first-run
    cost instead of an unexplained stack failure.
    """
    import urllib.request

    version = _TRINO_PLUGIN_VERSION
    for name in missing:
        url = f"{_TRINO_PLUGIN_MAVEN}/{name}/{version}/{name}-{version}.jar"
        target = os.path.join(plugins, name)
        os.makedirs(target, exist_ok=True)
        print(f"[conftest] downloading Trino plugin {name} from {url}")
        urllib.request.urlretrieve(  # noqa: S310 — pinned https Maven Central URL
            url, os.path.join(target, f"{name}-{version}.jar")
        )
        if not _has_jars(target):
            raise RuntimeError(f"{url} produced no jars in {target}")


def _has_jars(path: str) -> bool:
    return os.path.isdir(path) and any(f.endswith(".jar") for f in os.listdir(path))


def _populate_trino_plugins() -> None:
    # Gitignored plugin jars exist only where built. Resolve the primary checkout from
    # the git common dir (worktree-agnostic) and symlink any missing/empty plugin dir;
    # whatever is still missing afterwards is downloaded from the pinned release.
    plugins = os.path.join(_REPO_ROOT, "trino", "plugins")
    if not os.path.isdir(plugins):
        return
    _link_trino_plugins_from_primary(plugins)
    # Docker bind-mounts create an empty root-owned directory for a missing source, so an
    # empty dir is indistinguishable from "never populated" — check for jars, not existence.
    missing = [name for name in _TRINO_PLUGINS if not _has_jars(os.path.join(plugins, name))]
    if missing:
        _download_trino_plugins(plugins, missing)


def _link_trino_plugins_from_primary(plugins: str) -> None:
    common = subprocess.run(
        ["git", "rev-parse", "--git-common-dir"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    primary = os.path.dirname(os.path.abspath(os.path.join(_REPO_ROOT, common)))
    primary_plugins = os.path.join(primary, "trino", "plugins")
    # Identity by inode, not by path string. On macOS the same directory is reachable under
    # two distinct real paths (/Users/... and /Volumes/<root-volume>/Users/...), and a string
    # compare called the primary checkout "different" — so every plugin dir was symlinked to
    # ITSELF, and Docker then refused to mount them ("mkdir ...: file exists").
    if os.path.isdir(primary_plugins) and os.path.samefile(primary_plugins, plugins):
        return  # already the primary checkout
    if not os.path.isdir(primary_plugins):
        return  # nothing to borrow; the caller downloads what is missing
    for name in os.listdir(primary_plugins):
        src = os.path.join(primary_plugins, name)
        dst = os.path.join(plugins, name)
        if not os.path.isdir(src) or not any(f.endswith(".jar") for f in os.listdir(src)):
            continue
        if os.path.islink(dst):
            # A link that no longer reaches jars (dangling, or self-referential from the
            # path-string bug above) is worse than nothing: Docker fails the bind mount. Clear
            # it and re-link rather than skipping it forever.
            if os.path.isdir(dst) and any(f.endswith(".jar") for f in os.listdir(dst)):
                continue
            os.unlink(dst)
        if os.path.isdir(dst) and any(f.endswith(".jar") for f in os.listdir(dst)):
            continue
        if os.path.isdir(dst):
            os.rmdir(dst) if not os.listdir(dst) else None
        if not os.path.exists(dst):
            os.symlink(src, dst)


def _marker_batches(items) -> list[list[str]]:
    """The bring-up batches for this run: core services first, then one batch per marker.

    Which marker services this run needs (kafka/mongo/neo4j/…). schema-registry and debezium
    ride along with kafka in the SAME isolated project, so they reach it on the project network
    by service name — no external dev network. Heavy engines (_HEAVY_MARKERS) are deliberately
    excluded here and provisioned per-test by _heavy_db_service, so only one is ever live at a
    time.

    Batching, rather than one `up --wait` over the whole set, bounds how many containers are
    STARTING at once. Boot is where the marker services cost the most memory — several JVMs
    (kafka, elasticsearch, neo4j, debezium) sizing their heaps simultaneously peaks far above
    their steady state, and on the 11.7 GiB VM that peak is what OOM-kills them. Each batch
    reaches its healthcheck and settles before the next one starts; the steady-state set is
    unchanged, so nothing a later test needs has gone away.
    """
    batches = [list(_CORE_SERVICES)]
    started = set(_CORE_SERVICES)
    for marker, services in _MARKER_SERVICES.items():
        if marker in _HEAVY_MARKERS:
            continue
        if not any(item.get_closest_marker(marker) for item in items):
            continue
        batch = sorted(set(services) - started)
        if not batch:
            continue
        started.update(batch)
        batches.append(batch)
    return batches


class _DockerServiceManager:
    def pytest_collection_finish(self, session):
        if os.environ.get("PYTEST_NO_DOCKER"):
            return
        if not _needs_shared_stack(session.items):
            return

        batches = _marker_batches(session.items)

        # Trino's custom plugin jars (trino/plugins/*) are gitignored build artifacts:
        # present only where they were built (the primary checkout). A fresh worktree
        # mounts empty dirs and Trino fails startup ("No service providers ... in the
        # classpath"). Populate missing plugin dirs from the primary checkout, located
        # worktree-agnostically via the git common dir.
        _populate_trino_plugins()

        # Sessions killed before their own teardown leave a PID-named project behind; clear
        # those (and only those) so containers/memory are not leaked forever. A live sibling
        # session's project is never touched — that is the whole point of the per-session name.
        reap_orphaned_projects()

        # Per-session projects and ports make concurrent sessions independent of each other, but
        # they still share one Docker VM's memory. Where that memory holds only one stack this
        # blocks until the other session finishes, i.e. serializes; where it holds several,
        # sessions stay concurrent. Held until sessionfinish.
        acquire_stack_slot()

        # The project name carries this session's PID, so nothing under it can predate this
        # session — except a PID the OS recycled onto a pytest run. Every run reserves FRESH
        # ephemeral host ports, so such a leftover publishes the wrong ports and is useless:
        # `up` would RECREATE each container in place, and compose's --wait races the recreate
        # (it watches the container it saw at plan time, sees the SIGTERM exit of the old one,
        # and fails the bring-up with "dependency failed to start: ... exited (143)"). Tear our
        # own project down first so `up` always starts from nothing.
        subprocess.run(
            ["docker", "compose", *_ITEST_COMPOSE_ARGS, "down", "--volumes", "--remove-orphans"],
            cwd=_REPO_ROOT,
            check=False,
        )

        # Provision an ISOLATED stack: dedicated project + the ephemeral ports already
        # exported at import time, its own network — the dev stack is never touched. One
        # `up --wait` per batch, so each set is healthy before the next one starts booting
        # (see _marker_batches).
        for batch in batches:
            subprocess.run(
                ["docker", "compose", *_ITEST_COMPOSE_ARGS, "up", "-d", "--wait", *batch],
                cwd=_REPO_ROOT,
                check=True,
            )

    def pytest_sessionfinish(self, session, exitstatus):  # pyright: ignore
        # Tests own the services they provision — including reaping them. Tear the
        # whole isolated stack down by default so a run never leaks containers (which
        # starve later runs of memory) and never leaves anything touching dev.
        # PYTEST_DOCKER_KEEP=1 keeps it up for local iteration.
        # PYTEST_NO_DOCKER=1 means no stack was ever provisioned (collection_finish
        # returned early) and the `docker` CLI may be absent — e.g. the linux-mem lane
        # runs this suite INSIDE a container with only pgserver — so skip teardown too,
        # else sessionfinish raises FileNotFoundError('docker') and masks the results.
        if os.environ.get("PYTEST_DOCKER_KEEP") or os.environ.get("PYTEST_NO_DOCKER"):
            # PYTEST_DOCKER_KEEP leaves the stack up, so its memory is still spoken for and the
            # slot must stay held; PYTEST_NO_DOCKER never took one. Either way, nothing to
            # release here.
            return
        try:
            subprocess.run(
                ["docker", "compose", *_ITEST_COMPOSE_ARGS, "down", "--volumes"],
                cwd=_REPO_ROOT,
                check=False,
            )
        finally:
            # Only after the containers are gone is the VM memory actually free for the next
            # session — releasing before teardown would let it start provisioning into memory
            # this session has not given back yet.
            release_stack_slot()


def pytest_configure(config):
    config.pluginmanager.register(_DockerServiceManager())
    config.addinivalue_line(
        "markers",
        "requires_provisa_server: skip when Provisa server is not reachable",
    )
    config.addinivalue_line(
        "markers",
        "requires_debezium: skip when Debezium Connect is not reachable",
    )


def _dump_service_diagnostics(services: list[str]) -> None:
    """Print each service's healthcheck output and log tail to the captured test output.

    A failed ``up --wait`` says only that a container is unhealthy. The reason lives in two places
    the caller never sees: the last healthcheck invocation's own stdout (``.State.Health.Log``) and
    the engine's log. Both are printed here because the caller's cleanup removes the container
    immediately afterwards.
    """
    for service in services:
        ids = subprocess.run(
            ["docker", "compose", *_ITEST_COMPOSE_ARGS, "ps", "-aq", service],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.split()
        for container in ids:
            health = subprocess.run(
                ["docker", "inspect", "--format", "{{json .State}}", container],
                capture_output=True,
                text=True,
                check=False,
            )
            print(f"[conftest] {service} state: {health.stdout.strip()}")
        logs = subprocess.run(
            ["docker", "compose", *_ITEST_COMPOSE_ARGS, "logs", "--tail=200", service],
            cwd=_REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        print(f"[conftest] {service} logs:\n{logs.stdout}{logs.stderr}")


@pytest.fixture
def _heavy_db_service(request):  # pyright: ignore
    """Bring up ONLY this test's heavy engine, then tear it down when the test ends.

    Heavy DBs (_HEAVY_MARKERS) are excluded from the session-wide bring-up so they never
    run concurrently — the Docker VM cannot hold ~15 emulated engines at once and OOM-kills
    them. Provisioning per-test caps the live heavy set at one, at the cost of a cold start
    per heavy test. Skipped tests (skipif on license/arch) never reach setup, so an engine
    that cannot start on this host is never asked for.
    """
    if os.environ.get("PYTEST_NO_DOCKER"):
        yield
        return
    services: list[str] = []
    for marker in _HEAVY_MARKERS:
        if request.node.get_closest_marker(marker):
            services.extend(_MARKER_SERVICES[marker])
    if not services:
        yield
        return
    try:
        # `--wait-timeout` bounds each attempt so a still-booting engine fails fast and
        # deterministically instead of hanging until an external harness timeout kills the
        # whole pytest process — which skips this fixture's own `finally` teardown below and
        # leaves the container running (observed: an orphaned Atlas container still
        # "health: starting" 6 minutes after the harness had already killed the test run).
        cmd = [
            "docker",
            "compose",
            *_ITEST_COMPOSE_ARGS,
            "up",
            "-d",
            "--wait",
            "--wait-timeout",
            "180",
            *sorted(services),
        ]
        last_error: subprocess.CalledProcessError | None = None
        # Atlas (and other JVM engines) can crash on first boot on a Spring context init
        # race, exit, and get relaunched by `restart: unless-stopped` — a later boot is the
        # one that passes its healthcheck. Contention with the always-on dev Atlas container
        # (same amd64-under-QEMU image, competing for CPU/memory) has been observed to push
        # this past a single retry, so try up to three times before treating it as a real
        # failure.
        for _attempt in range(3):
            try:
                subprocess.run(cmd, cwd=_REPO_ROOT, check=True)
                last_error = None
                break
            except subprocess.CalledProcessError as exc:
                last_error = exc
        if last_error is not None:
            # `up --wait` reports only "container ... is unhealthy", and the `rm -fsv`
            # below then destroys the evidence — on CI that left an exasol boot failure
            # with no engine log and no healthcheck output at all. Dump both before the
            # container goes away.
            _dump_service_diagnostics(sorted(services))
            raise last_error
        yield
    finally:
        # Reclaim memory immediately (-s stop, -f force, -v drop anon volumes) so the next
        # heavy-DB test starts with the VM clear. Runs even when setup failed (e.g. OOM
        # exit 137) to stop crash-looping containers that would otherwise starve core services.
        subprocess.run(
            ["docker", "compose", *_ITEST_COMPOSE_ARGS, "rm", "-fsv", *sorted(services)],
            cwd=_REPO_ROOT,
            check=False,
        )
        # After removing heavy services, ensure core services recover from any OOM pressure
        # (e.g. Druid/Cassandra OOM-killing Trino/Postgres). restart: unless-stopped restarts
        # them automatically; --wait blocks until healthy so the next test never races a
        # not-yet-ready core service.
        subprocess.run(
            ["docker", "compose", *_ITEST_COMPOSE_ARGS, "up", "-d", "--wait", *_CORE_SERVICES],
            cwd=_REPO_ROOT,
            check=False,
        )


def pytest_collection_modifyitems(config, items):  # pyright: ignore
    for item in items:
        if item.get_closest_marker("requires_provisa_server"):
            item.fixturenames.insert(0, "provisa_server")
        if item.get_closest_marker("requires_debezium"):
            item.fixturenames.insert(0, "debezium_server")
        if any(item.get_closest_marker(m) for m in _HEAVY_MARKERS):
            item.fixturenames.insert(0, "_heavy_db_service")


@pytest.fixture(autouse=True)
def _reset_naming_convention():  # pyright: ignore
    """Reset global naming convention to defaults after each test.

    Tests that call _naming.configure() mutate module-level state. Without
    this reset, convention leaks across test boundaries causing failures in
    tests that rely on the default apollo_graphql (camelCase) convention.
    """
    yield
    _naming.configure(gql="apollo_graphql", sql="snake")


@pytest.fixture(autouse=True)
def _reset_login_throttle():  # pyright: ignore
    """REQ-1393: the login throttle is process-wide, so its counts must not cross tests.

    Suites that deliberately present bad credentials (the auth conformance matrix) would otherwise
    lock out the account they share and the next test would see a 429 instead of the 401 it asserts.
    """
    from provisa.auth.throttle import reset_login_throttle

    reset_login_throttle()
    yield
    reset_login_throttle()


def _server_reachable(url: str) -> bool:
    """True if the server answers liveness within a short retry budget.

    Probes the dependency-free /live endpoint (not /health, which acquires a PG
    connection and can block for several seconds when the pool is saturated by
    concurrent fixture/UI traffic). Retries so a transiently-busy but healthy
    server is not misclassified as dead.
    """
    import urllib.request

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(f"{url}/live", timeout=5)
            return True
        except Exception:
            time.sleep(1)
    return False


def _tcp_reachable(host: str, port: int) -> bool:
    import socket

    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except OSError:
        return False


def _pgbouncer_auth_ok(host: str, port: int) -> bool:  # pyright: ignore
    import asyncio

    async def _try():
        try:
            conn = await asyncpg.connect(
                host=host,
                port=port,
                user=os.environ.get("PG_USER", "provisa"),
                password=os.environ.get("PG_PASSWORD", "provisa"),
                database=os.environ.get("PG_DATABASE", "provisa"),
                timeout=5,
            )
            await conn.close()
            return True
        except Exception:
            return False

    return asyncio.run(_try())


def _trino_catalog_exists(catalog: str) -> bool:  # pyright: ignore
    import trino

    try:
        conn = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "localhost"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user="test",
        )
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        cur.fetchone()
        return True
    except Exception:
        return False


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session", autouse=True)
def _wait_for_trino(request):  # pyright: ignore
    """Block until Trino core catalogs are ready or 6 minutes elapse.

    Set PROVISA_SKIP_TRINO_WAIT=1 when the test session provisions its own
    Trino (e.g. helm/minikube tests) and external Trino is not available.

    Engaged exactly when ``_needs_shared_stack`` says this session uses the shared compose stack —
    the same predicate that decides whether the stack is brought up at all, so the wait can never
    outlive the provisioning again.
    """
    if os.environ.get("PROVISA_SKIP_TRINO_WAIT"):
        return
    # No Docker provisioned means no Trino to wait for — self-contained tests (e.g.
    # test_client_auth_login.py, which spins up its own uvicorn server) run in
    # PYTEST_NO_DOCKER=1 sessions and must not block 360s on an absent coordinator.
    if os.environ.get("PYTEST_NO_DOCKER"):
        return
    if not _needs_shared_stack(request.session.items):
        return
    host = os.environ.get("TRINO_HOST", "localhost")
    port = int(os.environ.get("TRINO_PORT", "8080"))
    deadline = time.monotonic() + 360
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            conn = trino.dbapi.connect(host=host, port=port, user="test")
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            # `system` is the one catalog Trino serves without any properties file. The probe
            # used to name sales_pg, which only resolved because a committed
            # trino/catalog/sales_pg.properties was bind-mounted; those files are Trino's own
            # FileCatalogStore output and no longer ship (REQ-1339), and the app that registers
            # the real catalogs starts *after* this gate — so a data catalog can never be the
            # readiness signal. What this must prove is that the coordinator answers metadata.
            cur.execute("SHOW SCHEMAS FROM system")
            cur.fetchall()
            conn.close()
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(3)
    raise RuntimeError(f"Trino not ready at {host}:{port} after 360s — last error: {last_exc}")


@pytest.fixture(scope="session", autouse=True)
def _reserve_flight_port():  # pyright: ignore
    """Allocate a free port for the Arrow Flight server before any test starts.

    Multiple integration tests spin up an in-process FastAPI app via
    ASGITransport. Each app instance tries to bind the Arrow Flight gRPC
    server on FLIGHT_PORT (default 8815). If port 8815 is already in use
    (e.g. by a previous test run's zombie process or the live server) the
    lifespan fails and every request returns 400. Setting a random free port
    here ensures every in-process app gets a usable socket.
    """
    port = _free_port()
    os.environ.setdefault("FLIGHT_PORT", str(port))
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    # Limit pool size per in-process app so concurrent module fixtures
    # don't exhaust PostgreSQL max_connections.
    os.environ.setdefault("PG_POOL_MIN", "1")
    os.environ.setdefault("PG_POOL_MAX", "3")
    # Platform control plane URL (global org/user/invite registry + billing).
    # Tests point it at the same Postgres server as the tenant plane but leave it
    # unscoped (default schema) — a separate engine/pool, per the control-plane
    # split. The subprocess server inherits this via {**os.environ}.
    _cp_url = (
        f"postgresql+asyncpg://{os.environ.get('PG_USER', 'provisa')}"
        f":{os.environ.get('PG_PASSWORD', 'provisa')}"
        f"@{os.environ.get('PG_HOST', 'localhost')}"
        f":{os.environ.get('PG_PORT', '5432')}"
        f"/{os.environ.get('PG_DATABASE', 'provisa')}"
    )
    os.environ.setdefault("PLATFORM_DATABASE_URL", _cp_url)
    # Tenant control-plane URL (schema-scoped to org_<id> by the fixtures). Same
    # canonical SQLAlchemy async URL as the platform plane — one place names the
    # driver, so no fixture hand-builds it.
    os.environ.setdefault("TENANT_DATABASE_URL", _cp_url)
    # REQ-594, REQ-1075: the Lemon Squeezy webhook authenticates by HMAC over the raw body keyed
    # by this secret, and verify_webhook_signature raises without it. A fixed test value lets the
    # skip-path tests reach the signature check (and get a clean 400) instead of a 500.
    os.environ.setdefault("LEMONSQUEEZY_SIGNING_SECRET", "test-signing-secret")


@pytest.fixture(scope="session")
def pg_dsn() -> str:
    return (
        f"postgresql://{os.environ.get('PG_USER', 'provisa')}"
        f":{os.environ.get('PG_PASSWORD', 'provisa')}"
        f"@{os.environ.get('PG_HOST', 'localhost')}"
        f":{os.environ.get('PG_PORT', '5432')}"
        f"/{os.environ.get('PG_DATABASE', 'provisa')}"
    )


@pytest_asyncio.fixture(scope="session")
async def tenant_db(_reserve_flight_port):
    # The tenant control plane is the SQLAlchemy-backed Database shim (execute_core,
    # advisory_xact_lock, …), replacing the former bare asyncpg pool. search_path is
    # left unset (role default: public) to match that pool exactly — tests that need
    # the org_default schema set it per-acquire (e.g. test_schema_gen). init_schema
    # scopes its own schema internally, so it does not depend on this. URL (driver
    # included) comes from TENANT_DATABASE_URL, set once in the env.
    from provisa.core.database import Database, create_engine_from_url

    engine = create_engine_from_url(os.environ["TENANT_DATABASE_URL"], pool_size=5, max_overflow=5)
    db = Database(engine, name="org", search_path=None)
    yield db
    await db.close()


@pytest.fixture(scope="session")
def trino_conn():
    conn = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="test",
        catalog="sales_pg",
        schema="public",
    )
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def docker_postgres():
    """Ensure the postgres container is running; start it if not.

    Uses `docker compose -f docker-compose.core.yml up postgres -d` which is
    safe on this machine (single named service — never `compose up` with no
    service name, which crashes Docker Engine).
    """
    pg_host = os.environ.get("PG_HOST", "localhost")
    pg_port = int(os.environ.get("PG_PORT", "5432"))

    if not _tcp_reachable(pg_host, pg_port):
        compose_file = os.path.join(os.path.dirname(__file__), "..", "docker-compose.core.yml")
        subprocess.run(
            ["docker", "compose", "-f", compose_file, "up", "postgres", "-d"],
            check=True,
        )
        # Wait up to 30 s for postgres to be ready
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if _tcp_reachable(pg_host, pg_port):
                break
            time.sleep(1)
        else:
            raise RuntimeError(
                f"Postgres did not become reachable at {pg_host}:{pg_port} within 30 s"
            )

    yield {"host": pg_host, "port": pg_port}


@pytest_asyncio.fixture(scope="session")
async def graphql_client(docker_postgres):
    """ASGI test client backed by a real Postgres pool.

    Starts an in-process Provisa app via create_app() with a real asyncpg pool
    so GraphQL queries exercise the full compiler + executor path without
    requiring a separate server process.
    """

    import provisa.api.app as app_mod
    from provisa.api.app import create_app
    from httpx import ASGITransport, AsyncClient

    the_app = create_app()

    from provisa.core.database import (
        Database as _Database,
        create_engine_from_url as _create_engine_from_url,
    )

    org_id = os.environ.get("ORG_ID", "default")
    _tenant_engine = _create_engine_from_url(os.environ["TENANT_DATABASE_URL"], pool_size=3)
    pool = _Database(_tenant_engine, name="org", search_path=f"org_{org_id}")
    from unittest.mock import AsyncMock, MagicMock

    from provisa.executor.pool import SourcePool

    _sp = MagicMock(spec=SourcePool)
    _sp.has.return_value = False
    _sp.get.side_effect = KeyError
    _sp.dialect_for.return_value = None
    _sp.source_ids = []
    _sp.execute = AsyncMock(return_value=MagicMock(rows=[]))
    _sp.execute_ddl = AsyncMock()
    _sp.add = AsyncMock()
    _sp.remove = AsyncMock()
    _sp.close_all = AsyncMock()
    _sp.close = AsyncMock()
    app_mod.state.tenant_db = pool
    app_mod.state.source_pools = _sp

    # Platform control plane (global org/user/invite registry). This fixture
    # bypasses the app lifespan, so build + seed it here the way startup does.
    from provisa.core.database import Database, create_engine_from_url
    from provisa.core.schema_admin import init_registry_schema

    admin_engine = create_engine_from_url(os.environ["PLATFORM_DATABASE_URL"], pool_size=3)
    admin_db = Database(admin_engine, name="platform")
    await init_registry_schema(admin_db, "root")
    app_mod.state.admin_db = admin_db

    transport = ASGITransport(app=the_app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    await pool.close()
    await admin_db.close()
    app_mod.state.tenant_db = None
    app_mod.state.admin_db = None


@pytest.fixture(scope="session")
def test_client(docker_postgres):  # pyright: ignore[reportUnusedParameter]
    """Synchronous ASGI test client for in-process Provisa app."""
    from unittest.mock import AsyncMock, MagicMock

    import provisa.api.app as app_mod
    from provisa.api.app import create_app
    from provisa.executor.pool import SourcePool
    from starlette.testclient import TestClient

    _sp = MagicMock(spec=SourcePool)
    _sp.has.return_value = False
    _sp.get.side_effect = KeyError
    _sp.dialect_for.return_value = None
    _sp.source_ids = []
    _sp.execute = AsyncMock(return_value=MagicMock(rows=[]))
    _sp.execute_ddl = AsyncMock()
    _sp.add = AsyncMock()
    _sp.remove = AsyncMock()
    _sp.close_all = AsyncMock()
    _sp.close = AsyncMock()
    the_app = create_app()
    app_mod.state.source_pools = _sp
    with TestClient(the_app, raise_server_exceptions=False) as client:
        yield client
    app_mod.state.tenant_db = None


@pytest.fixture(scope="session")
def debezium_server():
    """Wait for Debezium Connect to be reachable — started by _DockerServiceManager."""
    host = os.environ.get("DEBEZIUM_HOST", "localhost")
    port = int(os.environ.get("DEBEZIUM_PORT", "8083"))
    deadline = time.monotonic() + 480
    while time.monotonic() < deadline:
        if _tcp_reachable(host, port):
            yield f"http://{host}:{port}"
            return
        time.sleep(3)
    raise RuntimeError(f"Debezium Connect did not become reachable at {host}:{port} within 480s")


@pytest_asyncio.fixture(scope="session")
async def live_client(provisa_server):
    """AsyncClient that hits the running Provisa server (PROVISA_URL or localhost:8000)."""
    import httpx

    async with httpx.AsyncClient(base_url=provisa_server, timeout=120.0) as client:
        yield client


@pytest.fixture(scope="session")
def provisa_server(_reserve_flight_port):
    """Start the Provisa server subprocess if not already running.

    Used by requires_provisa_server tests — injected automatically via
    pytest_collection_modifyitems, not requested directly.

    Depends on ``_reserve_flight_port`` explicitly: that fixture is what exports
    PLATFORM_DATABASE_URL/TENANT_DATABASE_URL for the isolated stack, and being autouse does not
    order it ahead of this one. Without the dependency the subprocess inherited no
    PLATFORM_DATABASE_URL at all, fell back to the dev control plane on :5432, and died in
    ``bring_up_platform`` with connection-refused — every requires_provisa_server test erroring at
    setup with a bare "exited early (code 3)".
    """
    server_url = os.environ.get("PROVISA_URL", "http://localhost:8000")
    if _server_reachable(server_url):
        # Reusing an externally-managed server: its Flight port is whatever it was started with.
        os.environ["PROVISA_SERVER_FLIGHT_PORT"] = os.environ.get("FLIGHT_PORT", "8815")
        yield server_url
        return

    from urllib.parse import urlparse as _urlparse

    _parsed = _urlparse(server_url)
    _port = _parsed.port or 8000
    _host = _parsed.hostname or "localhost"
    if _tcp_reachable(_host, _port):
        raise RuntimeError(
            f"Port {_port} is already bound by another process but {server_url}/health "
            "is not responding — stop the existing process before running tests that "
            "require a Provisa server."
        )

    # Give the subprocess its OWN free Arrow Flight port. The session-wide FLIGHT_PORT
    # (_reserve_flight_port) is shared by every in-process ASGI app; if the subprocess inherited it,
    # its Flight bind would clash with an already-bound in-process server and silently fail (HTTP
    # comes up, Flight never binds). A dedicated free port makes the live-server Flight isolation-safe.
    _flight_port = _free_port()
    # Provision the fallback server against the isolated test config, NOT the shipped demo config
    # (config/provisa.yaml). The demo config declares sources like `pet-store-pg` that aren't
    # provisioned in the isolated stack, so its default-source lookup KeyErrors and every /data/sql
    # returns 400. sample_config.yaml's only source (`sales-pg`) resolves to the isolated PG via
    # ${env:PG_PORT}/${env:PG_PASSWORD}, and it declares the admin/analyst roles these tests use — so
    # the requires_provisa_server tests exercise a real, executable governance pipeline.
    _live_cfg = os.path.join(os.path.dirname(__file__), "fixtures", "sample_config.yaml")
    server_env = {
        **os.environ,
        "PG_PASSWORD": os.environ.get("PG_PASSWORD") or "provisa",
        "FLIGHT_PORT": str(_flight_port),
        "PROVISA_CONFIG": _live_cfg,
        **_server_coverage_env(),
    }
    # Keep the subprocess's own output. It used to go to DEVNULL, which turned every startup
    # failure into a bare "exited early (code 3)" and forced a hand-reconstruction of this env to
    # find out why. The log survives the run for a follow-up read.
    _server_log_path = os.path.abspath(
        os.path.join(_REPO_ROOT, ".pytest_cache", "provisa_server.log")
    )
    os.makedirs(os.path.dirname(_server_log_path), exist_ok=True)
    _server_log = open(_server_log_path, "w")

    def _server_log_tail(lines: int = 30) -> str:
        _server_log.flush()
        with open(_server_log_path) as fh:
            tail = fh.read().splitlines()[-lines:]
        return "\n".join(tail) or "(no output)"

    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "main:app", "--host", "0.0.0.0", f"--port={_port}"],
        cwd=_REPO_ROOT,
        stdout=_server_log,
        stderr=subprocess.STDOUT,
        env=server_env,
    )
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        if _server_reachable(server_url):
            break
        if proc.poll() is not None:
            raise RuntimeError(
                f"Provisa server exited early (code {proc.returncode}); "
                f"log {_server_log_path}:\n{_server_log_tail()}"
            )
        time.sleep(2)
    else:
        proc.terminate()
        raise RuntimeError(
            f"Provisa server did not become reachable at {server_url} within 90s; "
            f"log {_server_log_path}:\n{_server_log_tail()}"
        )

    # HTTP /health can precede the Flight gRPC bind; wait for the Flight port before yielding so
    # requires_provisa_server tests never race the bind. Publish the port for Flight/ADBC clients.
    _flight_deadline = time.monotonic() + 60
    while time.monotonic() < _flight_deadline:
        if _tcp_reachable(_host, _flight_port):
            break
        if proc.poll() is not None:
            raise RuntimeError(
                f"Provisa server exited before Flight bind (code {proc.returncode}); "
                f"log {_server_log_path}:\n{_server_log_tail()}"
            )
        time.sleep(1)
    else:
        proc.terminate()
        raise RuntimeError(
            f"Provisa Arrow Flight server did not bind {_host}:{_flight_port} within 60s"
        )
    os.environ["PROVISA_SERVER_FLIGHT_PORT"] = str(_flight_port)

    try:
        yield server_url
    finally:
        os.environ.pop("PROVISA_SERVER_FLIGHT_PORT", None)
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


@pytest.fixture
def otel_spans():
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry import trace

    # set_tracer_provider() is one-shot per process: if any earlier import or
    # fixture already installed a real SDK provider, a second set is silently
    # ignored and our exporter would never receive spans. So attach the exporter
    # to whichever real provider is active; only install a fresh one if the
    # current provider is still the API default (ProxyTracerProvider).
    exporter = InMemorySpanExporter()
    processor = SimpleSpanProcessor(exporter)
    current = trace.get_tracer_provider()
    if isinstance(current, TracerProvider):
        current.add_span_processor(processor)
        yield exporter
        processor.shutdown()  # drains + disables; provider keeps running for other tests
    else:
        provider = TracerProvider()
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)
        yield exporter
        exporter.shutdown()


@pytest.fixture
def sample_config():
    import yaml
    from pathlib import Path

    config_path = Path(__file__).parent / "fixtures" / "sample_config.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)
