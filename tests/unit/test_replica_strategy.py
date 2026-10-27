# Copyright (c) 2026 Kenneth Stott
# Canary: 988f71d1-dd93-4fe1-8f27-b922405db0d7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Connector pgwire replica strategy (REQ-954/955/956).

Covers: model.json generation per source type (955), unique port allocation (955), runtime_deps
bundle resolution/caching + version pin (956, mocked download), server lifecycle start/health/stop
(mocked subprocess), and the land-via-SELECT flow (954, mocked PG endpoint). No network / real
Calcite jar / real Postgres.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from tests.helpers import ready_org_folder

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.runtime_deps import pgwire_bundles as rd

_aio = pytest.mark.asyncio(loop_scope="session")


# -- fixtures ------------------------------------------------------------------


def _files_source(**kw) -> Source:
    return Source(**{"id": "local-files", "type": SourceType.files, "path": "/data/reports", **kw})


def _sharepoint_source(**kw) -> Source:
    return Source(
        **{
            "id": "sp-team",
            "type": SourceType.sharepoint,
            "base_url": "https://contoso.sharepoint.com/sites/team",
            "database": "tenant-abc",  # tenantId
            "username": "client-123",  # clientId
            "password": "shhh-secret",  # clientSecret
            **kw,
        }
    )


def _splunk_source(**kw) -> Source:
    return Source(
        **{
            "id": "splunk-prod",
            "type": SourceType.splunk,
            "host": "splunk.internal",
            "port": 8089,
            "password": "tok-xyz",  # token (use_token default true)
            "database": "search",  # app
            **kw,
        }
    )


def _lay_down_bundle(spec: rd.BundleSpec, dest: Path) -> None:
    """A fake downloader: create the bundle tree (launcher + model dir) the resolver verifies."""
    launcher = dest / "bin" / spec.artifact_name
    launcher.parent.mkdir(parents=True, exist_ok=True)
    launcher.write_text("#!/bin/sh\n")
    (dest / "model").mkdir(parents=True, exist_ok=True)


# -- REQ-955: model.json generation --------------------------------------------


def test_files_model_json_directory():
    model = pr.build_model_json(_files_source())
    schema = model["schemas"][0]
    assert model["defaultSchema"] == "local_files"
    assert schema["name"] == "local_files"
    assert schema["operand"]["directory"] == "/data/reports"
    assert schema["operand"]["executionEngine"] == "DUCKDB"


def test_files_model_json_s3_storage():
    src = _files_source(
        path=None,
        mapping={"storage_type": "s3", "storage_config": {"bucket": "b", "prefix": "p"}},
    )
    operand = pr.build_model_json(src)["schemas"][0]["operand"]
    assert operand["storageType"] == "s3"
    assert operand["storageConfig"] == {"bucket": "b", "prefix": "p"}
    assert "directory" not in operand


def test_files_model_json_missing_path_is_loud():
    with pytest.raises(pr.MissingConnectorConfig):
        pr.build_model_json(_files_source(path=None))


def test_files_model_json_strips_glob_from_path():
    # Calcite's FileSchemaFactory "directory" operand is a plain directory, not a glob (verified
    # live: a literal trailing "**" makes it look for a subdirectory actually named "**", finding
    # nothing) — the Sources form's "Path / Glob" field still accepts one for parity with
    # TrinoFilesConnector (which DOES glob-match), so _files_operand strips it here.
    operand = pr.build_model_json(_files_source(path="/data/reports/**"))["schemas"][0]["operand"]
    assert operand["directory"] == "/data/reports"


def test_sharepoint_model_json():
    operand = pr.build_model_json(_sharepoint_source())["schemas"][0]["operand"]
    assert operand["siteUrl"] == "https://contoso.sharepoint.com/sites/team"
    assert operand["tenantId"] == "tenant-abc"
    assert operand["clientId"] == "client-123"
    assert operand["clientSecret"] == "shhh-secret"


def test_sharepoint_missing_auth_is_loud():
    src = _sharepoint_source(password="")
    with pytest.raises(pr.MissingConnectorConfig):
        pr.build_model_json(src)


def test_sharepoint_missing_tenant_is_loud():
    src = _sharepoint_source(database="")
    with pytest.raises(pr.MissingConnectorConfig):
        pr.build_model_json(src)


# -- REQ-1693: certificate auth's three Calcite-adapter contracts --------------


def test_sharepoint_certificate_auth_names_the_adapter_and_auth_type():
    """Without authType, SharePointAuthFactory.createAuth defaults to CLIENT_CREDENTIALS and dies
    on the absent secret — the operand must say CERTIFICATE, and the schema factory must be the
    adapter's own class name."""
    src = _sharepoint_source(
        password="",
        mapping={"certificate_path": "/abs/cert.pfx", "certificate_password": ""},
    )
    model = pr.build_model_json(src)
    operand = model["schemas"][0]["operand"]
    assert operand["authType"] == "CERTIFICATE"
    assert operand["certificatePath"] == "/abs/cert.pfx"
    assert operand["certificatePassword"] == ""
    assert "clientSecret" not in operand
    assert model["schemas"][0]["factory"] == pr._SCHEMA_FACTORY["sharepoint"]


def test_sharepoint_certificate_password_is_preserved_when_non_empty():
    src = _sharepoint_source(
        password="",
        mapping={"certificate_path": "/abs/cert.pfx", "certificate_password": "s3cret"},
    )
    operand = pr.build_model_json(src)["schemas"][0]["operand"]
    assert operand["certificatePassword"] == "s3cret"


def test_sharepoint_certificate_relative_path_is_loud():
    """The pgwire server runs with its bundle directory as cwd, so a relative certificate_path
    resolves against the wrong directory and the PFX is never found — refused up front instead."""
    src = _sharepoint_source(
        password="",
        mapping={"certificate_path": "relative/cert.pfx", "certificate_password": ""},
    )
    with pytest.raises(pr.MissingConnectorConfig, match="absolute"):
        pr.build_model_json(src)


def test_sharepoint_certificate_missing_password_key_is_loud():
    """CertificateAuth calls certificatePassword.toCharArray() and createCertificateAuth rejects a
    null password, so an absent mapping key is a config error, never silently defaulted to ""."""
    src = _sharepoint_source(password="", mapping={"certificate_path": "/abs/cert.pfx"})
    with pytest.raises(pr.MissingConnectorConfig, match="certificate_password"):
        pr.build_model_json(src)


def test_sharepoint_certificate_non_string_password_is_loud():
    src = _sharepoint_source(
        password="", mapping={"certificate_path": "/abs/cert.pfx", "certificate_password": None}
    )
    with pytest.raises(pr.MissingConnectorConfig, match="certificate_password"):
        pr.build_model_json(src)


def test_sharepoint_device_code_auth_when_no_secret_or_certificate():
    src = _sharepoint_source(password="", mapping={"use_device_code": True})
    operand = pr.build_model_json(src)["schemas"][0]["operand"]
    assert operand["authType"] == "DEVICE_CODE"
    assert "certificatePath" not in operand


def test_splunk_model_json_token():
    operand = pr.build_model_json(_splunk_source())["schemas"][0]["operand"]
    assert operand["url"] == "https://splunk.internal:8089"
    assert operand["token"] == "tok-xyz"
    assert operand["app"] == "search"


def test_splunk_model_json_userpass():
    src = _splunk_source(username="admin", password="pw", mapping={"use_token": False})
    operand = pr.build_model_json(src)["schemas"][0]["operand"]
    assert operand["username"] == "admin"
    assert operand["password"] == "pw"
    assert "token" not in operand


def test_splunk_model_json_disable_ssl_and_datamodel_filter():
    """REQ-724/REQ-1694: the two Splunk mapping keys the Trino connector already carries reach the
    Calcite operand — as the types SplunkSchemaFactory casts them to (Boolean / String)."""
    src = _splunk_source(
        mapping={"disable_ssl_validation": True, "datamodel_filter": "provisa_*"},
    )
    operand = pr.build_model_json(src)["schemas"][0]["operand"]
    assert operand["disableSslValidation"] is True
    assert operand["datamodelFilter"] == "provisa_*"


def test_splunk_model_json_omits_unset_ssl_and_filter():
    operand = pr.build_model_json(_splunk_source())["schemas"][0]["operand"]
    assert "disableSslValidation" not in operand
    assert "datamodelFilter" not in operand


def test_splunk_missing_credentials_is_loud():
    src = _splunk_source(password="", mapping={"use_token": False})
    with pytest.raises(pr.MissingConnectorConfig):
        pr.build_model_json(src)


def test_non_replica_type_is_loud():
    with pytest.raises(pr.MissingConnectorConfig):
        pr.build_model_json(Source(id="pg", type=SourceType.postgresql))


# -- REQ-955: port allocation --------------------------------------------------


@pytest.fixture
def _default_scan(monkeypatch):
    """A deployment that names no ports for its servers: the scan up from the default bases."""
    monkeypatch.delenv(pr.PORTS_ENV, raising=False)


def test_ports_unique_across_sources(_default_scan):
    alloc = pr.PortAllocator(is_free=lambda _: True)
    a = alloc.allocate("src-a")
    b = alloc.allocate("src-b")
    assert a.pgwire_port != b.pgwire_port
    assert a.calcite_child_port != b.calcite_child_port
    assert a.pgwire_port == pr.PGWIRE_DEFAULT_PORT
    assert a.calcite_child_port == pr.CALCITE_CHILD_DEFAULT_PORT
    assert a.calcite_child_host == pr.CALCITE_CHILD_DEFAULT_HOST


def test_port_allocation_is_idempotent_per_source(_default_scan):
    alloc = pr.PortAllocator(is_free=lambda _: True)
    assert alloc.allocate("src-a") == alloc.allocate("src-a")


def test_port_allocation_skips_busy_ports(_default_scan):
    busy = {pr.PGWIRE_DEFAULT_PORT, pr.PGWIRE_DEFAULT_PORT + 1}
    alloc = pr.PortAllocator(is_free=lambda p: p not in busy)
    a = alloc.allocate("src-a")
    assert a.pgwire_port == pr.PGWIRE_DEFAULT_PORT + 2


def test_port_allocation_exhaustion_is_loud(_default_scan):
    alloc = pr.PortAllocator(is_free=lambda _: False)
    with pytest.raises(pr.PortAllocationError):
        alloc.allocate("src-a")


def test_a_port_another_process_listens_on_at_every_address_is_not_free():
    """A server listening on the wildcard address holds the port for loopback too: a pgwire
    server started on it would be shadowed, and a read meant for it would reach the other
    process."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as held:
        held.bind(("0.0.0.0", 0))
        held.listen()
        port = held.getsockname()[1]
        assert not pr._port_is_free(port)


def test_a_port_another_process_listens_on_at_loopback_is_not_free():
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as held:
        held.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        held.bind(("127.0.0.1", 0))
        held.listen()
        assert not pr._port_is_free(held.getsockname()[1])


def test_a_port_a_stopped_server_left_closed_connections_on_is_free():
    """A server that closes its connections first leaves them in TIME_WAIT on its port once it
    stops; a server binds there again at once, so the probe that waits for the stop must agree."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        # A port no socket holds on ANY address: bound on every address without SO_REUSEADDR,
        # the kernel hands out none another process's socket (another xdist worker's) holds on
        # some address, which the probe's own every-address bind would meet.
        server.bind(("0.0.0.0", 0))  # nosec B104 -- a test socket, closed below
        # Then reusable, as the replica server's listener is (socketserver allow_reuse_address):
        # each connection it accepts takes the option, and so does the TIME_WAIT it leaves. Linux
        # refuses a bind beside a TIME_WAIT whose socket lacked it, SO_REUSEADDR or not; macOS
        # does not look.
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.listen()
        port = server.getsockname()[1]
        client = socket.create_connection(("127.0.0.1", port))
        accepted, _ = server.accept()
        accepted.close()  # the server closes first: TIME_WAIT is on its side, on its port
        client.close()
    assert pr._port_is_free(port)


def test_a_deployment_that_names_its_ports_gets_those_alone(monkeypatch):
    monkeypatch.setenv(pr.PORTS_ENV, "23001,23002,23003")
    alloc = pr.PortAllocator(is_free=lambda p: p != 23002)
    pair = alloc.allocate("src-a")
    assert (pair.pgwire_port, pair.calcite_child_port) == (23001, 23003)
    with pytest.raises(pr.PortAllocationError, match=pr.PORTS_ENV):
        alloc.allocate("src-b")


# The ports local-dev listens on (its server, UI, pgwire, Bolt, Flight, gRPC, OTel collector) and
# the default scan its own sources' pgwire servers take ports from.
_LOCAL_DEV_PORTS = {3000, 4317, 4318, 4319, 5439, 7687, 8001, 8815, 50051} | set(
    range(pr.PGWIRE_DEFAULT_PORT, pr.CALCITE_CHILD_DEFAULT_PORT + 512 + 1)
)


def test_a_test_instance_never_gives_a_server_a_port_local_dev_uses():
    """Every test server's sources are served on ports leased for the session (tests/conftest.py),
    none of which local-dev uses -- even one that is free when it is handed out."""
    import os

    leased = {int(p) for p in os.environ[pr.PORTS_ENV].split(",")}
    assert leased and not leased & _LOCAL_DEV_PORTS


# -- REQ-956: runtime_deps bundle resolution / caching / version pin -----------


def test_every_connector_is_fetched_from_the_pin():
    # engine-v0.117.0 is the first release whose catalog counts no rows; every bundle is at the pin.
    assert rd.CONNECTOR_RELEASE == {}
    assert rd.RELEASE_TAG == "engine-v0.122.2"
    pinned = rd.RELEASE_TAG.removeprefix("engine-v")
    for source_type, connector in rd.BUNDLE_CONNECTOR.items():
        spec = rd.bundle_spec_for(source_type)
        assert spec.version == rd.RELEASE_TAG
        assert rd.BundleSpec(connector, spec.version, variant="linux-x86_64").download_url == (
            f"https://github.com/kenstott/calcite/releases/download/{rd.RELEASE_TAG}/"
            f"pgwire-{connector}-{pinned}-linux-x86_64.tar.gz"
        )
    # An explicit version still wins for any connector.
    assert rd.bundle_spec_for("salesforce", version="engine-v0.109.0").version == "engine-v0.109.0"


def test_bundle_spec_version_pin():
    spec = rd.bundle_spec_for("splunk")
    assert spec.version == rd.RELEASE_TAG
    assert spec.connector == "splunk"
    assert spec.artifact_name == "pgwire-splunk"
    assert rd.RELEASE_TAG in spec.download_url
    assert spec.download_url.startswith("https://github.com/kenstott/calcite/releases/download/")


def test_bundle_asset_is_platform_stamped():  # REQ-1690
    spec = rd.BundleSpec("file", "engine-v0.82.1", variant="linux-x86_64")
    assert spec.asset_stem == "pgwire-file-0.82.1-linux-x86_64"
    assert spec.download_url.endswith("/engine-v0.82.1/pgwire-file-0.82.1-linux-x86_64.tar.gz")


def test_bundle_variant_unbuilt_platform_is_loud(monkeypatch):  # REQ-1690
    monkeypatch.setattr(rd.platform, "system", lambda: "Linux")
    monkeypatch.setattr(rd.platform, "machine", lambda: "aarch64")
    with pytest.raises(rd.BundleUnavailable):
        rd.bundle_variant()


def test_download_unnests_the_tarball(tmp_path, monkeypatch):  # REQ-1690
    """The release tarball nests the bundle under ``<asset_stem>/``; the resolver's bundle root
    is ``dest`` itself, so the download moves that directory into place."""
    import io
    import tarfile

    spec = rd.BundleSpec("file", "engine-v0.82.1", variant="macos-arm64")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        launcher = tarfile.TarInfo(f"{spec.asset_stem}/bin/pgwire-file")
        payload = b"#!/bin/sh\n"
        launcher.size = len(payload)
        tar.addfile(launcher, io.BytesIO(payload))
    body = buf.getvalue()

    class _Resp:
        status_code = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def iter_bytes(self):
            yield body[:100]
            yield body[100:]

    import httpx

    def _fake_stream(method, url, **kwargs):
        assert method == "GET"
        assert url == spec.download_url
        assert kwargs.get("follow_redirects") is True
        return _Resp()

    monkeypatch.setattr(httpx, "stream", _fake_stream)
    dest = tmp_path / "engine-v0.82.1" / "pgwire-file"
    rd.download_release_asset(spec, dest)
    assert (dest / "bin" / "pgwire-file").read_bytes() == payload
    assert not (dest / spec.asset_stem).exists()


def test_bundle_spec_maps_each_type():
    assert rd.bundle_spec_for("sharepoint").artifact_name == "pgwire-sharepoint"
    assert rd.bundle_spec_for("splunk").artifact_name == "pgwire-splunk"
    assert rd.bundle_spec_for("salesforce").artifact_name == "pgwire-salesforce"


def test_bundle_spec_unknown_type_is_loud():
    with pytest.raises(rd.BundleUnavailable):
        rd.bundle_spec_for("postgresql")


def test_bundle_resolve_downloads_then_caches(tmp_path):
    calls: list[str] = []

    def _dl(spec, dest):
        calls.append(spec.artifact_name)
        _lay_down_bundle(spec, dest)

    resolver = rd.BundleResolver(cache_root=tmp_path, downloader=_dl)
    spec = rd.bundle_spec_for("files")
    assert not resolver.is_cached(spec)
    path = resolver.resolve(spec)
    assert path == tmp_path / spec.version / "pgwire-file"
    assert resolver.is_cached(spec)
    # second resolve is a cache HIT — downloader not called again
    resolver.resolve(spec)
    assert calls == ["pgwire-file"]


def test_bundle_resolve_download_without_launcher_is_loud(tmp_path):
    wanted = rd.bundle_spec_for("splunk")

    def _dl(spec, dest):
        assert spec == wanted
        dest.mkdir(parents=True, exist_ok=True)  # produces no launcher

    resolver = rd.BundleResolver(cache_root=tmp_path, downloader=_dl)
    with pytest.raises(rd.BundleUnavailable):
        resolver.resolve(wanted)


def test_a_bundle_missing_inside_a_request_is_downloaded_off_it(tmp_path):
    """Inside a request a miss starts one download of its own, outside the request's deadline, and
    answers BundleDownloading at once; a request after it finds the bundle."""
    import threading

    from provisa.core import request_deadline

    release = threading.Event()
    calls: list[float | None] = []

    def _dl(spec, dest):
        calls.append(request_deadline.remaining())
        release.wait(5)
        _lay_down_bundle(spec, dest)

    resolver = rd.BundleResolver(cache_root=tmp_path, downloader=_dl)
    spec = rd.bundle_spec_for("files")
    with request_deadline.within(30):
        with pytest.raises(rd.BundleDownloading):
            resolver.resolve(spec)
        with pytest.raises(rd.BundleDownloading):
            resolver.resolve(spec)  # the download in flight is not started again
        thread = rd._downloads[resolver.cached_path(spec)]
    release.set()
    thread.join(5)
    assert calls == [None]  # one download, with no request deadline over it
    with request_deadline.within(30):
        assert resolver.resolve(spec) == tmp_path / spec.version / "pgwire-file"


def test_a_bundle_download_off_a_request_that_failed_is_reported_once(tmp_path):
    import threading

    from provisa.core import request_deadline

    release = threading.Event()
    attempts: list[str] = []

    def _dl(spec, dest):
        attempts.append(spec.artifact_name)
        release.wait(5)
        dest.mkdir(parents=True, exist_ok=True)  # produces no launcher

    resolver = rd.BundleResolver(cache_root=tmp_path, downloader=_dl)
    spec = rd.bundle_spec_for("splunk")
    with request_deadline.within(30):
        with pytest.raises(rd.BundleDownloading):
            resolver.resolve(spec)
        thread = rd._downloads[resolver.cached_path(spec)]
        release.set()
        thread.join(5)
        with pytest.raises(rd.BundleUnavailable) as failed:
            resolver.resolve(spec)
        assert not isinstance(failed.value, rd.BundleDownloading)
        assert "did not produce launcher" in str(failed.value)
        with pytest.raises(rd.BundleDownloading):  # the next request tries again
            resolver.resolve(spec)
        again = rd._downloads.get(resolver.cached_path(spec))
        if again is not None:  # None: it has already ended
            again.join(5)
    assert attempts == ["pgwire-splunk", "pgwire-splunk"]


# -- REQ-955: server lifecycle -------------------------------------------------


def _spawn_returning(proc):
    """A fake ``spawn`` callable matching ``_spawn_process``'s ``(command, cwd)`` signature."""

    def _spawn(cmd, cwd, env=None):
        assert isinstance(cmd, list) and isinstance(cwd, Path)
        return proc

    return _spawn


def _health_from(get_result):
    """A fake ``health_check`` callable matching ``_tcp_health``'s ``(host, port)`` signature."""

    def _health(host, port):
        assert isinstance(host, str) and isinstance(port, int)
        return get_result()

    return _health


class _FakeProc:
    def __init__(self, exit_code: int | None = None) -> None:
        self.terminated = False
        self.waited = False
        self._exit_code = exit_code  # None: still running

    def exit_code(self) -> int | None:
        return self._exit_code

    def terminate(self) -> None:
        self.terminated = True

    def wait(self, timeout: float) -> None:
        assert self.terminated, "wait() before terminate()"
        assert timeout > 0
        self.waited = True


def _server(tmp_path, *, health: bool = True):
    spec = rd.bundle_spec_for("files")
    _lay_down_bundle(spec, tmp_path)
    proc = _FakeProc()
    spawned: list[list[str]] = []
    spawn_cwds: list[Path] = []

    def _spawn(cmd, cwd, env=None):
        spawned.append(cmd)
        spawn_cwds.append(cwd)
        return proc

    def _health_check(host, port):
        assert host == "127.0.0.1"
        assert port == 5433
        return health

    server = pr.PgwireServer(
        bundle_dir=tmp_path,
        spec=spec,
        model=pr.build_model_json(_files_source()),
        ports=pr.PortPair(5433, "127.0.0.1", 5533),
        spawn=_spawn,
        health_check=_health_check,
        port_is_free=lambda _: True,
    )
    assert spawn_cwds == []  # nothing spawned yet — server.start() does that
    return server, proc, spawned


def test_lifecycle_start_writes_model_and_spawns(tmp_path):
    server, _, spawned = _server(tmp_path)
    server.start()
    assert server.model_path.exists()
    cmd = spawned[0]
    assert "--port" in cmd and "5433" in cmd
    assert "--calcite-child" in cmd and "127.0.0.1:5533" in cmd
    # only --port and --calcite-child flags (REQ-955): no PGWIRE_PORT / --model
    assert "--model" not in cmd
    assert not any(f.startswith("PGWIRE_PORT") for f in cmd)


def test_lifecycle_health_and_stop(tmp_path):
    server, proc, _ = _server(tmp_path, health=True)
    server.start()
    assert server.health() is True
    server.stop()
    assert proc.terminated is True


def test_lifecycle_unhealthy(tmp_path):
    server, _, _ = _server(tmp_path, health=False)
    server.start()
    assert server.health() is False


def test_lifecycle_double_start_is_loud(tmp_path):
    server, _, _ = _server(tmp_path)
    server.start()
    with pytest.raises(pr.ServerLifecycleError):
        server.start()


def test_lifecycle_health_before_start_is_loud(tmp_path):
    server, _, _ = _server(tmp_path)
    with pytest.raises(pr.ServerLifecycleError):
        server.health()


def test_lifecycle_stop_before_start_is_noop(tmp_path):
    server, proc, _ = _server(tmp_path)
    server.stop()  # idempotent — no raise
    assert proc.terminated is False


# -- REQ-954: land via SELECT --------------------------------------------------


class _FakeConn:
    def __init__(self, rows) -> None:
        self._rows = rows
        self.queries: list[str] = []
        self.timeouts: list[float | None] = []
        self.closed = False

    async def fetch(self, sql, *args, timeout=None):
        self.queries.append(sql)
        self.timeouts.append(timeout)
        return self._rows

    async def close(self):
        self.closed = True


@_aio
async def test_land_via_select_returns_rows():
    conn = _FakeConn([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    async def _connect(host, port, password):
        assert (host, port) == ("127.0.0.1", 5433)
        return conn

    rows = await pr.land_via_select(
        pr.PortPair(5433, "127.0.0.1", 5533),
        "local_files",
        "reports",
        password="",
        connect=_connect,
    )
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert conn.queries == ['SELECT * FROM "local_files"."reports"']
    assert conn.closed is True


@_aio
async def test_land_via_select_is_always_bounded():
    """A wedged pgwire server must not block the landing forever: every SELECT carries a timeout."""
    conn = _FakeConn([])

    async def _connect(host, port, password):
        return conn

    await pr.land_via_select(
        pr.PortPair(5433, "127.0.0.1", 5533), "s", "t", password="", connect=_connect
    )
    await pr.land_via_select_keys(
        pr.PortPair(5433, "127.0.0.1", 5533),
        "s",
        "t",
        ["id"],
        [(1,)],
        password="",
        connect=_connect,
    )
    assert conn.timeouts == [pr.LAND_FETCH_SECONDS, pr.LAND_FETCH_SECONDS]


@_aio
async def test_land_via_select_timeout_is_tightened_by_the_request_budget():
    from provisa.core import request_deadline

    conn = _FakeConn([])

    async def _connect(host, port, password):
        return conn

    with request_deadline.within(5.0):
        await pr.land_via_select(
            pr.PortPair(5433, "127.0.0.1", 5533), "s", "t", password="", connect=_connect
        )
    (timeout,) = conn.timeouts
    assert timeout is not None and 0 < timeout <= 5.0


@_aio
async def test_connector_replica_end_to_end(tmp_path, _default_scan):
    conn = _FakeConn([{"col": "v"}])

    async def _connect(host, port, password):
        assert (host, port) == ("127.0.0.1", pr.PGWIRE_DEFAULT_PORT)
        return conn

    def _dl(spec, dest):
        _lay_down_bundle(spec, dest)

    proc = _FakeProc()
    resolver = rd.BundleResolver(cache_root=tmp_path, downloader=_dl)
    replica = pr.ConnectorReplica(
        _files_source(),
        resolver=resolver,
        allocator=pr.PortAllocator(is_free=lambda _: True),
        spawn=_spawn_returning(proc),
        health_check=_health_from(lambda: True),
        port_is_free=lambda _: True,
        connect=_connect,
    )
    rows = await replica.load("reports")
    assert rows == [{"col": "v"}]
    # server reused across a second load (not re-spawned) — one server, many SELECTs
    await replica.load("reports")
    assert conn.queries == [
        'SELECT * FROM "local_files"."reports"',
        'SELECT * FROM "local_files"."reports"',
    ]
    replica.close()
    assert proc.terminated is True


@_aio
async def test_a_connector_table_lands_only_the_rows_its_landing_filter_passes(tmp_path):
    """REQ-2062: the server reads the table's landing filter as PostgreSQL, in the SELECT."""
    from types import SimpleNamespace

    conn = _FakeConn([{"col": "v"}])

    async def _connect(host, port, password):
        return conn

    replica = pr.ConnectorReplica(
        _files_source(),
        resolver=rd.BundleResolver(
            cache_root=tmp_path, downloader=lambda spec, dest: _lay_down_bundle(spec, dest)
        ),
        allocator=pr.PortAllocator(is_free=lambda _: True),
        spawn=_spawn_returning(_FakeProc()),
        health_check=_health_from(lambda: True),
        port_is_free=lambda _: True,
        connect=_connect,
    )
    table = SimpleNamespace(
        table_name="reports", schema_name=None, landing_filter="col IN ('v', 'w')"
    )
    await replica.load(table)
    assert conn.queries == ['SELECT * FROM "local_files"."reports" WHERE "col" IN (\'v\', \'w\')']
    assert pr.make_pgwire_loader().applies_landing_filter  # replica_source lets it through
    replica.close()


@_aio
async def test_a_registered_table_lands_its_registered_columns(tmp_path):
    """The replica keeps the table's registered columns, so those are what its server is asked
    for -- not every column of the remote object (a Salesforce sObject has hundreds)."""
    from types import SimpleNamespace

    conn = _FakeConn([{"Id": "1", "Name": "a"}])

    async def _connect(host, port, password):
        return conn

    replica = pr.ConnectorReplica(
        _files_source(),
        resolver=rd.BundleResolver(
            cache_root=tmp_path, downloader=lambda spec, dest: _lay_down_bundle(spec, dest)
        ),
        allocator=pr.PortAllocator(is_free=lambda _: True),
        spawn=_spawn_returning(_FakeProc()),
        health_check=_health_from(lambda: True),
        port_is_free=lambda _: True,
        connect=_connect,
    )
    table = SimpleNamespace(
        table_name="reports",
        schema_name=None,
        landing_filter=None,
        columns=[SimpleNamespace(name="Id"), SimpleNamespace(name="Name")],
    )
    await replica.load(table)
    assert conn.queries == ['SELECT "Id", "Name" FROM "local_files"."reports"']
    replica.close()


@_aio
async def test_connector_replica_unhealthy_is_loud(tmp_path):
    def _dl(spec, dest):
        _lay_down_bundle(spec, dest)

    replica = pr.ConnectorReplica(
        _files_source(),
        resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_dl),
        allocator=pr.PortAllocator(is_free=lambda _: True),
        spawn=_spawn_returning(_FakeProc()),
        health_check=_health_from(lambda: False),
        port_is_free=lambda _: True,
        connect=None,
    )
    with pytest.raises(pr.ServerLifecycleError):
        await replica.load("reports")


@_aio
async def test_make_pgwire_loader_dispatches_per_source(tmp_path, _default_scan):
    conns = {"sp-a": _FakeConn([{"x": 1}]), "sp-b": _FakeConn([{"x": 2}])}
    seen_ports: list[int] = []

    async def _connect(host, port, password):
        assert host == "127.0.0.1"
        seen_ports.append(port)
        # source A lands on the first allocated port, B on the next — pick by port order
        return conns["sp-a"] if port == pr.PGWIRE_DEFAULT_PORT else conns["sp-b"]

    def _dl(spec, dest):
        _lay_down_bundle(spec, dest)

    loader = pr.make_pgwire_loader(
        resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_dl),
        allocator=pr.PortAllocator(is_free=lambda _: True),
        spawn=_spawn_returning(_FakeProc()),
        health_check=_health_from(lambda: True),
        port_is_free=lambda _: True,
        connect=_connect,
    )
    src_a = Source(id="sp-a", type=SourceType.files, path="/a")
    src_b = Source(id="sp-b", type=SourceType.files, path="/b")
    assert await loader(src_a, "t") == [{"x": 1}]
    assert await loader(src_b, "t") == [{"x": 2}]
    # two sources → two distinct servers on distinct ports (REQ-955 concurrency)
    assert seen_ports == [pr.PGWIRE_DEFAULT_PORT, pr.PGWIRE_DEFAULT_PORT + 1]


# -- REQ-954: engine routing ---------------------------------------------------


def test_needs_pgwire_replica_when_engine_lacks_connector():
    class _Eng:
        connectors: dict = {}

    assert pr.needs_pgwire_replica(_files_source(), _Eng()) is True


def test_no_pgwire_replica_when_engine_reads_the_type_live():
    class _Live:
        reads_in_place = True

    class _Eng:
        connectors = {"files": _Live()}

    assert pr.needs_pgwire_replica(_files_source(), _Eng()) is False


def test_pgwire_replica_when_engine_only_has_the_land_placeholder():  # issue #114
    """build_*_engine() gives every self-only warehouse a FETCH placeholder for the pgwire-replica
    types; that placeholder is the landing path, so the bridge is still needed."""
    from provisa.federation.engine import build_duckdb_engine, build_sqlalchemy_engine

    assert (
        pr.needs_pgwire_replica(_splunk_source(), build_sqlalchemy_engine("mysql://h/db")) is True
    )
    assert pr.needs_pgwire_replica(_files_source(), build_sqlalchemy_engine("mysql://h/db")) is True
    assert pr.needs_pgwire_replica(_splunk_source(), build_duckdb_engine()) is False
    assert pr.needs_pgwire_replica(_files_source(), build_duckdb_engine()) is False


def test_no_pgwire_replica_for_non_replica_type():
    class _Eng:
        connectors: dict = {}

    assert pr.needs_pgwire_replica(Source(id="pg", type=SourceType.postgresql), _Eng()) is False


# -- REQ-1690: live endpoint for an engine that attaches the server ---------------


def test_endpoint_waits_for_the_listener(tmp_path, monkeypatch, _default_scan):
    """The JVM binds seconds after spawn: endpoint() polls health until it answers, then returns
    the ports the engine attaches; the same replica hands back the same server."""
    monkeypatch.setattr(pr.time, "sleep", lambda _: None)
    answers = iter([False, False, True, True])
    proc = _FakeProc()
    replica = pr.ConnectorReplica(
        _splunk_source(),
        resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_lay_down_bundle),
        allocator=pr.PortAllocator(is_free=lambda _: True),
        spawn=_spawn_returning(proc),
        health_check=_health_from(lambda: next(answers)),
        port_is_free=lambda _: True,
    )
    ports = replica.endpoint()
    assert ports.pgwire_port == pr.PGWIRE_DEFAULT_PORT
    assert replica.endpoint() is ports
    replica.close()
    assert proc.terminated is True


def test_endpoint_never_listening_is_loud(tmp_path, monkeypatch):
    monkeypatch.setattr(pr.time, "sleep", lambda _: None)
    monkeypatch.setattr(pr, "SERVER_READY_SECONDS", 0)
    replica = pr.ConnectorReplica(
        _splunk_source(),
        resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_lay_down_bundle),
        allocator=pr.PortAllocator(is_free=lambda _: True),
        spawn=_spawn_returning(_FakeProc()),
        health_check=_health_from(lambda: False),
        port_is_free=lambda _: True,
    )
    with pytest.raises(pr.ServerLifecycleError):
        replica.endpoint()


def test_endpoint_timeout_override(tmp_path, monkeypatch):
    """REQ-1824: a caller-supplied ``timeout`` overrides SERVER_READY_SECONDS — used by the
    discovery path to fail fast rather than wait the full budget a real query needs."""
    monkeypatch.setattr(pr.time, "sleep", lambda _: None)
    replica = pr.ConnectorReplica(
        _splunk_source(),
        resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_lay_down_bundle),
        allocator=pr.PortAllocator(is_free=lambda _: True),
        spawn=_spawn_returning(_FakeProc()),
        health_check=_health_from(lambda: False),
        port_is_free=lambda _: True,
    )
    with pytest.raises(pr.ServerLifecycleError, match="within 5s"):
        replica.endpoint(timeout=5)


def test_ensure_endpoint_for_discovery_raises_still_starting(tmp_path, monkeypatch):
    """REQ-1824: a discovery call that hits an unhealthy server gets SourceStillStartingError, a
    DIFFERENT exception than the real-query path's ServerLifecycleError — so a caller (the admin
    GraphQL resolver, ultimately the frontend) can tell "still booting, poll again" apart from a
    genuine startup failure."""
    monkeypatch.setattr(pr.time, "sleep", lambda _: None)
    monkeypatch.setattr(pr, "_ENDPOINTS", {})
    monkeypatch.setattr(pr, "DISCOVERY_READY_SECONDS", 0)
    real_replica_cls = pr.ConnectorReplica

    def _fake_replica(source, allocator, key):
        return real_replica_cls(
            source,
            resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_lay_down_bundle),
            allocator=allocator,
            spawn=_spawn_returning(_FakeProc()),
            health_check=_health_from(lambda: False),
            port_is_free=lambda _: True,
        )

    monkeypatch.setattr(pr, "ConnectorReplica", _fake_replica)
    with pytest.raises(pr.SourceStillStartingError) as exc_info:
        pr.ensure_endpoint_for_discovery(_files_source(id="big-kaggle-dataset"))
    assert "big-kaggle-dataset" in str(exc_info.value)
    assert str(exc_info.value).startswith("STARTING:")


def test_a_server_that_exited_is_reported_with_its_log_not_as_still_starting(tmp_path, monkeypatch):
    """A connector server whose process has ended will never accept connections. Polled as "not
    healthy yet", discovery answered "still starting" until the UI gave up, and a query waited
    the whole ready budget to say only that nothing listened. Its exit is reported at once, with
    the exit code and the end of the server's own log."""
    monkeypatch.setattr(pr.time, "sleep", lambda _: None)
    monkeypatch.setattr(pr, "_ENDPOINTS", {})
    monkeypatch.setattr(pr, "DISCOVERY_READY_SECONDS", 60)
    real_replica_cls = pr.ConnectorReplica
    spawned_in: list[Path] = []

    def _spawn(cmd, cwd, env=None):
        spawned_in.append(cwd)
        (cwd / pr.SERVER_LOG_NAME).write_text("starting\nsplunk login failed: 401 Unauthorized\n")
        return _FakeProc(exit_code=1)

    def _fake_replica(source, allocator, key):
        return real_replica_cls(
            source,
            resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_lay_down_bundle),
            allocator=allocator,
            spawn=_spawn,
            health_check=_health_from(lambda: False),
            port_is_free=lambda _: True,
        )

    monkeypatch.setattr(pr, "ConnectorReplica", _fake_replica)
    with pytest.raises(pr.ServerExited) as exc_info:
        pr.ensure_endpoint_for_discovery(_files_source(id="broken-source"))
    assert exc_info.value.code == 1
    assert "broken-source" in str(exc_info.value)
    assert "splunk login failed: 401 Unauthorized" in str(exc_info.value)
    assert not str(exc_info.value).startswith("STARTING:")
    # The failed start is forgotten: the next call starts the server again.
    with pytest.raises(pr.ServerExited):
        pr.ensure_endpoint_for_discovery(_files_source(id="broken-source"))
    assert len(spawned_in) == 2


def test_ensure_endpoint_for_discovery_succeeds_when_ready(tmp_path, monkeypatch, _default_scan):
    """A discovery call against an already-healthy server returns normally, same as a real query."""
    monkeypatch.setattr(pr.time, "sleep", lambda _: None)
    monkeypatch.setattr(pr, "_ENDPOINTS", {})
    monkeypatch.setattr(pr, "_ENDPOINT_ALLOCATOR", pr.PortAllocator(is_free=lambda _: True))
    real_replica_cls = pr.ConnectorReplica

    def _fake_replica(source, allocator, key):
        return real_replica_cls(
            source,
            resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_lay_down_bundle),
            allocator=allocator,
            spawn=_spawn_returning(_FakeProc()),
            health_check=_health_from(lambda: True),
            port_is_free=lambda _: True,
            # The stand-in server listens nowhere: its catalog is "prepared" here, never asked
            # of whatever real process holds the default port on this machine.
            prepare_catalog=lambda ports, password: None,
        )

    monkeypatch.setattr(pr, "ConnectorReplica", _fake_replica)
    ports = pr.ensure_endpoint_for_discovery(_files_source(id="fast-source"))
    assert ports.pgwire_port == pr.PGWIRE_DEFAULT_PORT


def test_duckdb_splunk_attaches_the_pgwire_endpoint(monkeypatch):
    """The DuckDB connector attaches the server the endpoint registry started, read-only, under the
    private alias, and names the Calcite schema the tables live in."""
    from provisa.federation.connector_duckdb import DuckDBSplunkConnector

    started: list[str] = []

    def _ensure(source):
        started.append(source.id)
        return pr.PortPair(5440, "127.0.0.1", 5540)

    monkeypatch.setattr(pr, "ensure_endpoint", _ensure)
    src = _splunk_source(id="ops-splunk")
    details = DuckDBSplunkConnector().details(src)
    assert started == ["ops-splunk"]
    assert details["attach"] == (
        "ATTACH 'host=127.0.0.1 port=5440 user=provisa dbname=provisa' "
        'AS "_src_ops-splunk" (TYPE postgres, READ_ONLY)'
    )
    assert details["raw_alias"] == "_src_ops-splunk"
    assert details["remote_schema"] == "ops_splunk"


def test_stop_all_servers_closes_every_endpoint(monkeypatch):
    closed: list[str] = []

    class _Replica:
        def __init__(self, sid):
            self.sid = sid

        def close(self):
            closed.append(self.sid)

    monkeypatch.setattr(pr, "_ENDPOINTS", {"a": _Replica("a"), "b": _Replica("b")})
    pr.stop_all_servers()
    assert sorted(closed) == ["a", "b"]
    assert pr._ENDPOINTS == {}


def test_stop_endpoint_closes_and_forgets_only_that_source(monkeypatch):  # REQ-1690
    closed: list[str] = []

    class _Replica:
        def __init__(self, sid):
            self.sid = sid

        def close(self):
            closed.append(self.sid)

    from provisa.core.org_scope import source_key

    # REQ-1992: by the bound org's source of that id; another org's source of the id stays.
    elsewhere = ("another_org", "a")
    endpoints = {source_key("a"): _Replica("a"), source_key("b"): _Replica("b")}
    endpoints[elsewhere] = _Replica("a-elsewhere")
    monkeypatch.setattr(pr, "_ENDPOINTS", endpoints)
    pr.stop_endpoint("a")
    assert closed == ["a"]
    assert set(endpoints) == {source_key("b"), elsewhere}


def test_stop_endpoint_noop_when_never_started(monkeypatch):  # REQ-1690
    monkeypatch.setattr(pr, "_ENDPOINTS", {})
    pr.stop_endpoint("never-attached")  # must not raise


# -- REQ-1690: the dropdown's availability probe for a pgwire-attach connector ---------------


async def _fetch_ok(sql):
    # INSTALL/LOAD return nothing; the probe's duckdb_functions() count answers 1 (registered).
    return [{"n": 1}] if "duckdb_functions" in sql else []


def test_pgwire_connector_probe_reports_cached_bundle(tmp_path, monkeypatch):
    from provisa.federation.connector_duckdb import DuckDBSplunkConnector

    monkeypatch.setattr(rd.platform, "system", lambda: "Linux")
    monkeypatch.setattr(rd.platform, "machine", lambda: "x86_64")
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))
    result = asyncio.run(DuckDBSplunkConnector().probe(_fetch_ok))
    assert result.available is True
    assert "fetched on first use" in result.reason
    pinned_version = rd.RELEASE_TAG.removeprefix("engine-v")
    assert f"pgwire-splunk-{pinned_version}-linux-x86_64.tar.gz" in result.reason
    _lay_down_bundle(
        rd.bundle_spec_for("splunk"), rd.BundleResolver().cached_path(rd.bundle_spec_for("splunk"))
    )
    result = asyncio.run(DuckDBSplunkConnector().probe(_fetch_ok))
    assert result.available is True
    assert "cached" in result.reason


def test_pgwire_connector_probe_unavailable_on_an_unbuilt_platform(monkeypatch):
    from provisa.federation.connector_duckdb import DuckDBSharepointConnector

    monkeypatch.setattr(rd.platform, "system", lambda: "Linux")
    monkeypatch.setattr(rd.platform, "machine", lambda: "aarch64")
    result = asyncio.run(DuckDBSharepointConnector().probe(_fetch_ok))
    assert result.available is False
    assert "Linux/aarch64" in result.reason
    assert result.remediation is not None


def test_pgwire_connector_probe_unavailable_without_postgres_extension():
    from provisa.federation.connector_duckdb import DuckDBSplunkConnector

    async def _fetch_fail(sql):
        raise RuntimeError(f"cannot {sql}")

    result = asyncio.run(DuckDBSplunkConnector().probe(_fetch_fail))
    assert result.available is False
    assert "postgres did not load" in result.reason


# -- --owner-pid: a versioned launcher contract ------------------------------------------------


def test_owner_pid_flag_is_gated_on_the_bundle_release(tmp_path):
    assert pr.bundle_supports_owner_pid("engine-v0.82.0") is False
    assert pr.bundle_supports_owner_pid("engine-v0.82.1") is True
    assert pr.bundle_supports_owner_pid("engine-v1.0.0") is True
    spawned: list[list[str]] = []

    def _spawn(cmd, cwd, env=None):
        assert isinstance(cwd, Path)
        spawned.append(cmd)
        return _FakeProc()

    def _server_for(version: str):
        spec = rd.BundleSpec("file", version, variant="macos-arm64")
        _lay_down_bundle(spec, tmp_path / version)
        return pr.PgwireServer(
            bundle_dir=tmp_path / version,
            spec=spec,
            model=pr.build_model_json(_files_source()),
            ports=pr.PortPair(5433, "127.0.0.1", 5533),
            spawn=_spawn,
            health_check=_health_from(lambda: True),
            port_is_free=lambda _: True,
        )

    _server_for("engine-v0.82.0").start()
    assert "--owner-pid" not in spawned[-1]
    _server_for("engine-v0.82.1").start()
    assert spawned[-1][-2:] == ["--owner-pid", str(os.getpid())]


def test_two_orgs_with_one_source_id_have_their_own_server_state_and_ports(
    tmp_path, bind_org, monkeypatch
):
    """REQ-1992: a source id is unique only within its org. Two orgs' sources of one id each get
    their own state directory (their own model, which holds the source's credentials, and their
    own adapter state), their own ports, and their own server in the node's registry."""

    def _dl(spec, dest):
        _lay_down_bundle(spec, dest)

    resolver = rd.BundleResolver(cache_root=tmp_path / "cache", downloader=_dl)
    started: list[Path] = []

    def _spawn(cmd, cwd, env=None):
        started.append(cwd)
        return _FakeProc()

    allocator = pr.PortAllocator(is_free=lambda _: True)

    def _server_of(org: str, path: str):
        bind_org(org)
        ready_org_folder(org)
        replica = pr.ConnectorReplica(
            _files_source(id="files", path=path),
            resolver=resolver,
            allocator=allocator,
            spawn=_spawn,
            health_check=_health_from(lambda: True),
            port_is_free=lambda _: True,
        )
        return replica, replica.endpoint()

    (_a, ports_a), (_b, ports_b) = _server_of("org_a", "/data/a"), _server_of("org_b", "/data/b")
    cwd_a, cwd_b = started
    assert cwd_a != cwd_b
    # REQ-1992: each in this node's own part of its org's folder (orgs/<org>/nodes/<node>/pgwire).
    assert (cwd_a.parents[4].name, cwd_b.parents[4].name) == ("org_a", "org_b")
    assert ports_a != ports_b
    model_a = json.loads((cwd_a / "model" / "model.json").read_text())
    model_b = json.loads((cwd_b / "model" / "model.json").read_text())
    assert model_a != model_b

    # The node's registry holds one server per org's source, not one per source id.
    monkeypatch.setattr(pr, "_ENDPOINTS", {})
    monkeypatch.setattr(pr, "ConnectorReplica", lambda source, allocator, key: object())
    bind_org("org_a")
    first = pr._endpoint_replica(_files_source(id="files", path="/data/a"))
    bind_org("org_b")
    second = pr._endpoint_replica(_files_source(id="files", path="/data/b"))
    assert first is not second
    assert set(pr._ENDPOINTS) == {("org_a", "files"), ("org_b", "files")}


def test_two_orgs_stage_and_crawl_into_their_own_directories(bind_org):
    """REQ-1992: a crawl's landing directory and a staged dataset are under the org."""
    from provisa.file_source.crawl import crawl_landing_directory

    bind_org("org_a")
    ready_org_folder("org_a")
    a = crawl_landing_directory("wiki")
    bind_org("org_b")
    ready_org_folder("org_b")
    b = crawl_landing_directory("wiki")
    assert a != b and (a.parents[1].name, b.parents[1].name) == ("org_a", "org_b")


def test_each_source_server_runs_from_its_own_state_directory(tmp_path):
    """The bundle is a machine-wide download cache. Two servers of one connector (two sources,
    or two Provisa instances) used to write one ``model/model.json`` into it and run in it, so
    the Calcite adapter's per-schema state was shared and one served the other's files (REQ-955).
    Each server now runs from its own directory under the instance's data directory, with the
    bundle's code linked in, and the bundle is never written to."""

    def _dl(spec, dest):
        _lay_down_bundle(spec, dest)

    resolver = rd.BundleResolver(cache_root=tmp_path / "cache", downloader=_dl)
    started: list[tuple[list[str], Path]] = []

    def _spawn(cmd, cwd, env=None):
        started.append((cmd, cwd))
        return _FakeProc()

    def _server_of(source_id: str):
        replica = pr.ConnectorReplica(
            _files_source(id=source_id, path=f"/data/{source_id}"),
            resolver=resolver,
            allocator=pr.PortAllocator(is_free=lambda _: True),
            spawn=_spawn,
            health_check=_health_from(lambda: True),
            port_is_free=lambda _: True,
        )
        replica.endpoint()
        return replica

    first, second = _server_of("files-a"), _server_of("files-b")
    bundle = resolver.cached_path(first.spec)
    (cmd_a, cwd_a), (cmd_b, cwd_b) = started
    # Each runs in its own directory, under the instance's data directory, never the bundle.
    assert cwd_a != cwd_b
    data_dir = Path(os.environ["PROVISA_DATA_DIR"])
    assert {cwd_a.parent.name, cwd_b.parent.name} == {first.spec.version}
    assert cwd_a.is_relative_to(data_dir) and cwd_b.is_relative_to(data_dir)
    # Each reads its own model: the launcher resolves its home from its linked bin directory.
    assert cmd_a[0] == str(cwd_a / "bin" / first.spec.artifact_name)
    assert (cwd_a / "bin").resolve() == (bundle / "bin").resolve()
    model_a = json.loads((cwd_a / "model" / "model.json").read_text())
    model_b = json.loads((cwd_b / "model" / "model.json").read_text())
    assert model_a != model_b
    # The shared bundle holds no model of either.
    assert not (bundle / "model" / "model.json").exists()
    first.close()
    second.close()


def test_each_server_is_given_its_own_calcite_state_directory(tmp_path):
    """REQ-1954: unset, the launcher keeps its state under the user's ~/.local/state, which on a
    host where another user owns it stops the server at start, its error swallowed."""
    from provisa.federation import pgwire_replica as pr

    env = pr.calcite_state_environment(tmp_path / "run")
    assert env == {"PGWIRE_CALCITE_STATE_DIR": str(tmp_path / "run" / "calcite-state")}
    assert (tmp_path / "run" / "calcite-state").is_dir()


# -- a server bound off loopback (PROVISA_PGWIRE_BIND_HOST) ----------------------------------


def test_a_server_bound_off_loopback_is_reached_where_it_listens(tmp_path, monkeypatch):
    """The launcher listens on the bind host alone, so this host reaches the endpoint there --
    its health and its own connections -- and an engine in a container is told it too, unless the
    deployment names the engine another way."""
    monkeypatch.setenv(pr.BIND_HOST_ENV, "172.17.0.1")
    monkeypatch.delenv(pr.ENGINE_HOST_ENV, raising=False)
    spec = rd.bundle_spec_for("files")
    _lay_down_bundle(spec, tmp_path)
    probed: list[tuple[str, int]] = []
    ports = pr.PortPair(5433, "127.0.0.1", 5533)
    server = pr.PgwireServer(
        bundle_dir=tmp_path,
        spec=spec,
        model=pr.build_model_json(_files_source()),
        ports=ports,
        spawn=lambda cmd, cwd, env=None: _FakeProc(),
        health_check=lambda host, port: probed.append((host, port)) is None,
        port_is_free=lambda _: True,
    )
    cmd = server.command()
    assert cmd[cmd.index("--host") + 1] == "172.17.0.1"
    assert "127.0.0.1:5533" in cmd  # the Calcite child stays on loopback
    server.start()
    assert server.health() is True
    assert probed == [("172.17.0.1", 5433)]
    assert pr.engine_visible_host(ports) == "172.17.0.1"
    monkeypatch.setenv(pr.ENGINE_HOST_ENV, "provisa-host")
    assert pr.engine_visible_host(ports) == "provisa-host"


def test_unbound_a_server_is_reached_on_loopback(monkeypatch):
    monkeypatch.delenv(pr.BIND_HOST_ENV, raising=False)
    monkeypatch.delenv(pr.ENGINE_HOST_ENV, raising=False)
    ports = pr.PortPair(5433, "127.0.0.1", 5533)
    assert ports.pgwire_host == "127.0.0.1"
    assert pr.engine_visible_host(ports) == "127.0.0.1"
