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

from pathlib import Path

import pytest

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
    assert schema["operand"]["executionEngine"] == "PARQUET"


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
    """REQ-724/REQ-1692: the two Splunk mapping keys the Trino connector already carries reach the
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


def test_ports_unique_across_sources():
    alloc = pr.PortAllocator(is_free=lambda _p: True)
    a = alloc.allocate("src-a")
    b = alloc.allocate("src-b")
    assert a.pgwire_port != b.pgwire_port
    assert a.calcite_child_port != b.calcite_child_port
    assert a.pgwire_port == pr.PGWIRE_DEFAULT_PORT
    assert a.calcite_child_port == pr.CALCITE_CHILD_DEFAULT_PORT
    assert a.calcite_child_host == pr.CALCITE_CHILD_DEFAULT_HOST


def test_port_allocation_is_idempotent_per_source():
    alloc = pr.PortAllocator(is_free=lambda _p: True)
    assert alloc.allocate("src-a") == alloc.allocate("src-a")


def test_port_allocation_skips_busy_ports():
    busy = {pr.PGWIRE_DEFAULT_PORT, pr.PGWIRE_DEFAULT_PORT + 1}
    alloc = pr.PortAllocator(is_free=lambda p: p not in busy)
    a = alloc.allocate("src-a")
    assert a.pgwire_port == pr.PGWIRE_DEFAULT_PORT + 2


def test_port_allocation_exhaustion_is_loud():
    alloc = pr.PortAllocator(is_free=lambda _p: False)
    with pytest.raises(pr.PortAllocationError):
        alloc.allocate("src-a")


# -- REQ-956: runtime_deps bundle resolution / caching / version pin -----------


def test_bundle_spec_version_pin():
    spec = rd.bundle_spec_for("files")
    assert spec.version == "engine-v0.81.0"
    assert spec.connector == "file"
    assert spec.artifact_name == "pgwire-file"
    assert "engine-v0.81.0" in spec.download_url
    assert spec.download_url.startswith("https://github.com/kenstott/calcite/releases/download/")


def test_bundle_asset_is_platform_stamped():  # REQ-1690
    spec = rd.BundleSpec("file", "engine-v0.81.0", variant="linux-x86_64")
    assert spec.asset_stem == "pgwire-file-0.81.0-linux-x86_64"
    assert spec.download_url.endswith("/engine-v0.81.0/pgwire-file-0.81.0-linux-x86_64.tar.gz")


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

    spec = rd.BundleSpec("file", "engine-v0.81.0", variant="macos-arm64")
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

    monkeypatch.setattr(httpx, "stream", lambda *_a, **_k: _Resp())
    dest = tmp_path / "engine-v0.81.0" / "pgwire-file"
    rd.download_release_asset(spec, dest)
    assert (dest / "bin" / "pgwire-file").read_bytes() == payload
    assert not (dest / spec.asset_stem).exists()


def test_bundle_spec_maps_each_type():
    assert rd.bundle_spec_for("sharepoint").artifact_name == "pgwire-sharepoint"
    assert rd.bundle_spec_for("splunk").artifact_name == "pgwire-splunk"


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
    assert path == tmp_path / "engine-v0.81.0" / "pgwire-file"
    assert resolver.is_cached(spec)
    # second resolve is a cache HIT — downloader not called again
    resolver.resolve(spec)
    assert calls == ["pgwire-file"]


def test_bundle_resolve_download_without_launcher_is_loud(tmp_path):
    def _dl(spec, dest):
        dest.mkdir(parents=True, exist_ok=True)  # produces no launcher

    resolver = rd.BundleResolver(cache_root=tmp_path, downloader=_dl)
    with pytest.raises(rd.BundleUnavailable):
        resolver.resolve(rd.bundle_spec_for("splunk"))


# -- REQ-955: server lifecycle -------------------------------------------------


class _FakeProc:
    def __init__(self) -> None:
        self.terminated = False
        self.waited = False

    def terminate(self) -> None:
        self.terminated = True

    def wait(self, timeout: float) -> None:
        assert self.terminated, "wait() before terminate()"
        self.waited = True


def _server(tmp_path, *, health: bool = True):
    spec = rd.bundle_spec_for("files")
    _lay_down_bundle(spec, tmp_path)
    proc = _FakeProc()
    spawned: list[list[str]] = []

    def _spawn(cmd, cwd):
        spawned.append(cmd)
        return proc

    server = pr.PgwireServer(
        bundle_dir=tmp_path,
        spec=spec,
        model=pr.build_model_json(_files_source()),
        ports=pr.PortPair(5433, "127.0.0.1", 5533),
        spawn=_spawn,
        health_check=lambda _h, _p: health,
    )
    return server, proc, spawned


def test_lifecycle_start_writes_model_and_spawns(tmp_path):
    server, _proc, spawned = _server(tmp_path)
    server.start()
    assert server.model_path.exists()
    cmd = spawned[0]
    assert "--port" in cmd and "5433" in cmd
    assert "--calcite-child" in cmd and "127.0.0.1:5533" in cmd
    # only --port and --calcite-child flags (REQ-955): no PGWIRE_PORT / --model
    assert "--model" not in cmd
    assert not any(f.startswith("PGWIRE_PORT") for f in cmd)


def test_lifecycle_health_and_stop(tmp_path):
    server, proc, _spawned = _server(tmp_path, health=True)
    server.start()
    assert server.health() is True
    server.stop()
    assert proc.terminated is True


def test_lifecycle_unhealthy(tmp_path):
    server, _proc, _spawned = _server(tmp_path, health=False)
    server.start()
    assert server.health() is False


def test_lifecycle_double_start_is_loud(tmp_path):
    server, _proc, _spawned = _server(tmp_path)
    server.start()
    with pytest.raises(pr.ServerLifecycleError):
        server.start()


def test_lifecycle_health_before_start_is_loud(tmp_path):
    server, _proc, _spawned = _server(tmp_path)
    with pytest.raises(pr.ServerLifecycleError):
        server.health()


def test_lifecycle_stop_before_start_is_noop(tmp_path):
    server, proc, _spawned = _server(tmp_path)
    server.stop()  # idempotent — no raise
    assert proc.terminated is False


# -- REQ-954: land via SELECT --------------------------------------------------


class _FakeConn:
    def __init__(self, rows) -> None:
        self._rows = rows
        self.queries: list[str] = []
        self.closed = False

    async def fetch(self, sql):
        self.queries.append(sql)
        return self._rows

    async def close(self):
        self.closed = True


@_aio
async def test_land_via_select_returns_rows():
    conn = _FakeConn([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])

    async def _connect(host, port):
        assert (host, port) == ("127.0.0.1", 5433)
        return conn

    rows = await pr.land_via_select(
        pr.PortPair(5433, "127.0.0.1", 5533), "local_files", "reports", connect=_connect
    )
    assert rows == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    assert conn.queries == ['SELECT * FROM "local_files"."reports"']
    assert conn.closed is True


@_aio
async def test_connector_replica_end_to_end(tmp_path):
    conn = _FakeConn([{"col": "v"}])

    async def _connect(_host, _port):
        return conn

    def _dl(spec, dest):
        _lay_down_bundle(spec, dest)

    proc = _FakeProc()
    resolver = rd.BundleResolver(cache_root=tmp_path, downloader=_dl)
    replica = pr.ConnectorReplica(
        _files_source(),
        resolver=resolver,
        allocator=pr.PortAllocator(is_free=lambda _p: True),
        spawn=lambda _cmd, _cwd: proc,
        health_check=lambda _h, _p: True,
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
async def test_connector_replica_unhealthy_is_loud(tmp_path):
    def _dl(spec, dest):
        _lay_down_bundle(spec, dest)

    replica = pr.ConnectorReplica(
        _files_source(),
        resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_dl),
        allocator=pr.PortAllocator(is_free=lambda _p: True),
        spawn=lambda _cmd, _cwd: _FakeProc(),
        health_check=lambda _h, _p: False,
        connect=None,
    )
    with pytest.raises(pr.ServerLifecycleError):
        await replica.load("reports")


@_aio
async def test_make_pgwire_loader_dispatches_per_source(tmp_path):
    conns = {"sp-a": _FakeConn([{"x": 1}]), "sp-b": _FakeConn([{"x": 2}])}
    seen_ports: list[int] = []

    async def _connect(_host, port):
        seen_ports.append(port)
        # source A lands on the first allocated port, B on the next — pick by port order
        return conns["sp-a"] if port == pr.PGWIRE_DEFAULT_PORT else conns["sp-b"]

    def _dl(spec, dest):
        _lay_down_bundle(spec, dest)

    loader = pr.make_pgwire_loader(
        resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_dl),
        allocator=pr.PortAllocator(is_free=lambda _p: True),
        spawn=lambda _cmd, _cwd: _FakeProc(),
        health_check=lambda _h, _p: True,
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


def test_no_pgwire_replica_when_engine_has_connector():
    class _Eng:
        connectors = {"files": object()}

    assert pr.needs_pgwire_replica(_files_source(), _Eng()) is False


def test_no_pgwire_replica_for_non_replica_type():
    class _Eng:
        connectors: dict = {}

    assert pr.needs_pgwire_replica(Source(id="pg", type=SourceType.postgresql), _Eng()) is False


# -- REQ-1690: live endpoint for an engine that attaches the server ---------------


def test_endpoint_waits_for_the_listener(tmp_path, monkeypatch):
    """The JVM binds seconds after spawn: endpoint() polls health until it answers, then returns
    the ports the engine attaches; the same replica hands back the same server."""
    monkeypatch.setattr(pr.time, "sleep", lambda _s: None)
    answers = iter([False, False, True, True])
    proc = _FakeProc()
    replica = pr.ConnectorReplica(
        _splunk_source(),
        resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_lay_down_bundle),
        allocator=pr.PortAllocator(is_free=lambda _p: True),
        spawn=lambda _cmd, _cwd: proc,
        health_check=lambda _h, _p: next(answers),
    )
    ports = replica.endpoint()
    assert ports.pgwire_port == pr.PGWIRE_DEFAULT_PORT
    assert replica.endpoint() is ports
    replica.close()
    assert proc.terminated is True


def test_endpoint_never_listening_is_loud(tmp_path, monkeypatch):
    monkeypatch.setattr(pr.time, "sleep", lambda _s: None)
    monkeypatch.setattr(pr, "SERVER_READY_SECONDS", 0)
    replica = pr.ConnectorReplica(
        _splunk_source(),
        resolver=rd.BundleResolver(cache_root=tmp_path, downloader=_lay_down_bundle),
        allocator=pr.PortAllocator(is_free=lambda _p: True),
        spawn=lambda _cmd, _cwd: _FakeProc(),
        health_check=lambda _h, _p: False,
    )
    with pytest.raises(pr.ServerLifecycleError):
        replica.endpoint()


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
