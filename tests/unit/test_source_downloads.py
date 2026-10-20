"""REQ-2113: a CSV or Parquet source's file, given by download -- from a file mount path under a
directory the operator allows, an https:// URL or an s3:// URL -- kept in the org's folder under
files/<source>/ through the upload path, with its limit; a mount path outside the allowed
directories, or any mount path on a server of many organizations, is refused by name and nothing
is copied."""

from __future__ import annotations

import httpx
import pytest
import respx

import provisa.core.settings_catalog  # noqa: F401 -- declares the settings read here
from provisa.core import org_files, source_downloads, source_uploads


@pytest.fixture
def org(tmp_path, monkeypatch):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path / "data"))
    # A server of one organization; the test of many sets it so.
    monkeypatch.setattr(source_downloads, "many_organisations", lambda: False)
    org_files.folder("acme").mkdir(parents=True)
    return "acme"


@pytest.fixture
def shared(tmp_path, monkeypatch):
    """A directory of the server's own the operator lets files be downloaded from."""
    shared = tmp_path / "shared"
    shared.mkdir()
    monkeypatch.setenv("PROVISA_SOURCES_DOWNLOAD_ROOTS", str(shared))
    return shared


def _kept(org: str, source: str) -> list[str]:
    folder = org_files.folder(org) / "files" / source
    return sorted(p.name for p in folder.iterdir()) if folder.is_dir() else []


async def test_a_mounted_file_under_an_allowed_directory_is_copied_into_the_org_folder(org, shared):
    (shared / "supplies.parquet").write_bytes(b"PAR1data")
    path, size = await source_downloads.download(
        "supplies", str(shared / "supplies.parquet"), org_id=org
    )
    assert path == org_files.folder(org) / "files" / "supplies" / "supplies.parquet"
    assert path.read_bytes() == b"PAR1data" and size == 8
    assert source_uploads.is_upload("supplies", str(path), org_id=org)


async def test_a_mount_path_outside_the_allowed_directories_is_refused_and_nothing_copied(
    org, shared, tmp_path
):
    (tmp_path / "secret.csv").write_text("a\n1\n")
    with pytest.raises(source_downloads.DownloadRefused) as refused:
        await source_downloads.download("s", str(tmp_path / "secret.csv"), org_id=org)
    assert refused.value.code == "files.download_outside_roots"
    assert "outside the directories" in str(refused.value) and str(shared) in str(refused.value)
    # A symlink inside an allowed directory that leads out of it is refused the same way.
    (shared / "link.csv").symlink_to(tmp_path / "secret.csv")
    with pytest.raises(source_downloads.DownloadRefused):
        await source_downloads.download("s", str(shared / "link.csv"), org_id=org)
    assert _kept(org, "s") == []


async def test_no_directory_is_allowed_until_the_operator_lists_one(org, tmp_path, monkeypatch):
    monkeypatch.delenv("PROVISA_SOURCES_DOWNLOAD_ROOTS", raising=False)
    (tmp_path / "a.csv").write_text("a\n1\n")
    with pytest.raises(source_downloads.DownloadRefused, match="none are listed"):
        await source_downloads.download("s", str(tmp_path / "a.csv"), org_id=org)


async def test_a_mount_path_is_refused_on_a_server_of_many_organizations(org, shared, monkeypatch):
    (shared / "a.csv").write_text("a\n1\n")
    monkeypatch.setattr(source_downloads, "many_organisations", lambda: True)
    with pytest.raises(source_downloads.DownloadRefused) as refused:
        await source_downloads.download("s", str(shared / "a.csv"), org_id=org)
    assert refused.value.code == "files.download_mount_refused"
    assert "many organizations" in str(refused.value)
    assert _kept(org, "s") == []


async def test_an_address_of_another_kind_is_refused_by_name(org):
    for address in ("http://example.com/a.csv", "gs://bucket/a.csv", "relative/a.csv"):
        with pytest.raises(source_downloads.DownloadRefused) as refused:
            await source_downloads.download("s", address, org_id=org)
        assert address in str(refused.value)


@respx.mock
async def test_an_https_file_is_streamed_into_the_org_folder(org):
    respx.get("https://provisa.dev/demo-data/supplies.parquet").mock(
        return_value=httpx.Response(200, content=b"PAR1remote")
    )
    path, size = await source_downloads.download(
        "supplies", "https://provisa.dev/demo-data/supplies.parquet", org_id=org
    )
    assert path.name == "supplies.parquet" and path.read_bytes() == b"PAR1remote" and size == 10


@respx.mock
async def test_an_https_file_over_the_limit_or_not_found_is_refused_and_nothing_kept(
    org, monkeypatch
):
    respx.get("https://example.com/big.csv").mock(
        return_value=httpx.Response(200, content=b"x" * 10)
    )
    respx.get("https://example.com/gone.csv").mock(return_value=httpx.Response(404))
    monkeypatch.setenv("PROVISA_SOURCES_MAX_UPLOAD_BYTES", "4")
    with pytest.raises(source_uploads.UploadTooLarge):
        await source_downloads.download("s", "https://example.com/big.csv", org_id=org)
    with pytest.raises(source_downloads.DownloadRefused, match="answered 404"):
        await source_downloads.download("s", "https://example.com/gone.csv", org_id=org)
    assert _kept(org, "s") == []


async def test_an_s3_file_is_read_with_the_source_s_credentials(org, monkeypatch):
    seen = {}

    class _Body:
        def __init__(self) -> None:
            self.parts = [b"PAR1", b"s3"]

        def read(self, _n):
            return self.parts.pop(0) if self.parts else b""

        def close(self):
            seen["closed"] = True

    def _body(address, hints):
        seen["address"], seen["hints"] = address, hints
        return _Body(), 6

    monkeypatch.setattr(source_downloads, "_s3_body", _body)
    hints = {"access_key_id": "AK", "secret_access_key": "SK", "region": "us-east-1"}
    path, size = await source_downloads.download(
        "s", "s3://bucket/data/a.parquet", hints=hints, org_id=org
    )
    assert path.read_bytes() == b"PAR1s3" and size == 6
    assert seen == {"address": "s3://bucket/data/a.parquet", "hints": hints, "closed": True}


@pytest.fixture
def many(monkeypatch):
    monkeypatch.setattr(source_downloads, "many_organisations", lambda: True)


def _resolving(monkeypatch, table: dict[str, str]) -> None:
    def _getaddrinfo(host, *_args, **_kw):
        return [(None, None, None, "", (table[host], 0))]

    monkeypatch.setattr(source_downloads.socket, "getaddrinfo", _getaddrinfo)


@respx.mock
async def test_on_many_organizations_an_internal_host_is_refused_by_name(org, many, monkeypatch):
    _resolving(monkeypatch, {"metadata.internal": "169.254.169.254", "intranet": "10.0.0.7"})
    for address in ("https://metadata.internal/token", "https://intranet/a.csv"):
        with pytest.raises(source_downloads.DownloadRefused) as refused:
            await source_downloads.download("s", address, org_id=org)
        assert refused.value.code == "files.download_internal_address"
        assert address in str(refused.value)
    assert _kept(org, "s") == []


@respx.mock
async def test_on_many_organizations_each_redirect_hop_is_checked(org, many, monkeypatch):
    _resolving(
        monkeypatch, {"files.example.com": "93.184.215.14", "metadata.internal": "169.254.169.254"}
    )
    respx.get("https://files.example.com/a.csv").mock(
        return_value=httpx.Response(302, headers={"location": "https://metadata.internal/token"})
    )
    with pytest.raises(source_downloads.DownloadRefused) as refused:
        await source_downloads.download("s", "https://files.example.com/a.csv", org_id=org)
    assert refused.value.code == "files.download_internal_address"
    assert "metadata.internal" in str(refused.value)
    # A redirect off https is refused too.
    respx.get("https://files.example.com/b.csv").mock(
        return_value=httpx.Response(302, headers={"location": "http://files.example.com/b.csv"})
    )
    with pytest.raises(source_downloads.DownloadRefused, match="not https"):
        await source_downloads.download("s", "https://files.example.com/b.csv", org_id=org)
    assert _kept(org, "s") == []


@respx.mock
async def test_a_redirect_to_a_public_host_is_followed(org, many, monkeypatch):
    _resolving(monkeypatch, {"provisa.dev": "104.21.0.1", "cdn.example.com": "93.184.215.14"})
    respx.get("https://provisa.dev/demo-data/supplies.parquet").mock(
        return_value=httpx.Response(301, headers={"location": "https://cdn.example.com/s.parquet"})
    )
    respx.get("https://cdn.example.com/s.parquet").mock(
        return_value=httpx.Response(200, content=b"PAR1")
    )
    path, _ = await source_downloads.download(
        "s", "https://provisa.dev/demo-data/supplies.parquet", org_id=org
    )
    assert path.name == "supplies.parquet" and path.read_bytes() == b"PAR1"


def test_on_many_organizations_an_s3_compatible_endpoint_inside_the_network_is_refused(
    many, monkeypatch
):
    _resolving(monkeypatch, {"minio.internal": "192.168.1.20"})
    with pytest.raises(source_downloads.DownloadRefused) as refused:
        source_downloads._s3_body("s3://bucket/a.csv", {"endpoint": "http://minio.internal:9000"})
    assert refused.value.code == "files.download_internal_address"


def _source(path: str | None, **kw):
    from types import SimpleNamespace

    return SimpleNamespace(
        id="s",
        type="csv",
        path=path,
        download_from=kw.get("download_from", "https://example.com/a.csv"),
        federation_hints={},
    )


async def test_a_copy_within_its_ttl_or_kept_for_good_is_not_fetched_again(org, monkeypatch):
    import os
    import time

    fetched = []

    async def _download(source_id, address, *, hints=None, org_id=None):
        fetched.append(address)
        return None, 0

    monkeypatch.setattr(source_downloads, "download", _download)
    copy = org_files.folder(org) / "files" / "s" / "a.csv"
    copy.parent.mkdir(parents=True)
    copy.write_text("a\n1\n")
    from provisa.core.request_context import current_org

    token = current_org.set(org)
    try:
        assert not await source_downloads.refetched(
            _source("{files}/s/a.csv"), -1
        )  # once, for good
        assert not await source_downloads.refetched(_source("{files}/s/a.csv"), None)
        assert not await source_downloads.refetched(_source("{files}/s/a.csv"), 3600)
        # Its TTL lapsed since the copy was made: fetched again.
        old = time.time() - 7200
        os.utime(copy, (old, old))
        assert await source_downloads.refetched(_source("{files}/s/a.csv"), 3600)
        # A copy that is not there is fetched whatever the TTL.
        copy.unlink()
        assert await source_downloads.refetched(_source("{files}/s/a.csv"), -1)
        # A source read where it is has nothing to fetch.
        assert not await source_downloads.refetched(_source("/data/a.csv", download_from=None), 1)
    finally:
        current_org.reset(token)
    assert fetched == ["https://example.com/a.csv"] * 2


async def test_a_source_given_its_file_by_download_names_the_copy_as_its_path(org, shared):
    from provisa.api.admin.schema_common import download_source_file
    from provisa.api.admin.types import SourceInput
    from provisa.core.request_context import current_org

    (shared / "supplies.parquet").write_bytes(b"PAR1")
    made = SourceInput(
        id="supplies", type="parquet", download_from=str(shared / "supplies.parquet")
    )
    token = current_org.set(org)
    try:
        assert await download_source_file(made) is None
        assert made.path == "{files}/supplies/supplies.parquet"
        # Another source type is refused by name, and so is an address it may not be copied from.
        sqlite = SourceInput(id="db", type="sqlite", download_from=str(shared / "supplies.parquet"))
        refused = await download_source_file(sqlite)
        assert refused is not None and refused.code == "schema.file_download_refused"
        outside = SourceInput(id="x", type="csv", download_from="/etc/hosts")
        refused = await download_source_file(outside)
        assert refused is not None and "outside the directories" in refused.message
        assert outside.path is None
    finally:
        current_org.reset(token)
