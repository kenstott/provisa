# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e6f20-d47a-4c19-b5e2-9a1c7d04f8e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The demo's local test cluster: its download, checked against the pinned SHA-512, its install,
and its address as the demo's resolution reports it (REQ-2225). No network: the download is an
injected stream; HOME is a temp directory."""

# Requirements: REQ-2225
from __future__ import annotations

import hashlib
import io
import tarfile
from pathlib import Path

import pytest

from provisa.core import demo_spark, spark_cluster
from provisa.core.demo_spark import DemoSparkFailed
from provisa.core.spark_cluster import SparkClusterRefused

_BODY = b"spark distribution bytes " * 1000


def _sha512(data: bytes) -> str:
    return hashlib.sha512(data).hexdigest()


def _stream_of(data: bytes, seen: list[str] | None = None):
    def stream(url: str):
        if seen is not None:
            seen.append(url)
        for i in range(0, len(data), 4096):
            yield data[i : i + 4096]

    return stream


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.delenv("PROVISA_HOME", raising=False)
    monkeypatch.delenv("PROVISA_DEMO", raising=False)
    monkeypatch.delenv(spark_cluster.LOCAL_TEST_CLUSTER_ENV, raising=False)


class TestDownload:
    def test_a_good_checksum_is_accepted_and_the_archive_kept(self, tmp_path):
        dest = tmp_path / "dl" / "spark.tgz"
        kept = demo_spark.download(
            dest, url="https://archive.example/spark.tgz", sha512=_sha512(_BODY),
            stream=_stream_of(_BODY),
        )  # fmt: skip
        assert kept == dest
        assert dest.read_bytes() == _BODY
        assert sorted(p.name for p in dest.parent.iterdir()) == ["spark.tgz"]

    def test_a_bad_checksum_is_refused_by_name_and_the_partial_file_removed(self, tmp_path):
        dest = tmp_path / "dl" / "spark.tgz"
        with pytest.raises(DemoSparkFailed) as refused:
            demo_spark.download(
                dest, url="https://archive.example/spark.tgz", sha512="0" * 128,
                stream=_stream_of(_BODY),
            )  # fmt: skip
        assert refused.value.code == "demo_spark.checksum"
        assert _sha512(_BODY) in str(refused.value)
        assert "0" * 128 in str(refused.value)
        assert not dest.exists()
        assert list(dest.parent.iterdir()) == []

    def test_a_download_that_fails_partway_leaves_no_partial_file(self, tmp_path):
        def broken(_url: str):
            yield b"first chunk"
            raise DemoSparkFailed("download", "connection reset")

        dest = tmp_path / "dl" / "spark.tgz"
        with pytest.raises(DemoSparkFailed, match="connection reset"):
            demo_spark.download(dest, sha512=_sha512(_BODY), stream=broken)
        assert list(dest.parent.iterdir()) == []

    def test_a_non_https_url_is_refused(self):
        with pytest.raises(DemoSparkFailed) as refused:
            list(demo_spark.stream_https("http://archive.apache.org/dist/spark.tgz"))
        assert refused.value.code == "demo_spark.download"

    def test_the_pin_is_a_spark_release_on_apaches_release_mirror(self):
        assert demo_spark.URL == (
            f"https://dlcdn.apache.org/spark/spark-{demo_spark.SPARK_VERSION}/"
            f"spark-{demo_spark.SPARK_VERSION}-bin-hadoop3.tgz"
        )
        assert len(demo_spark.SHA512) == 128
        int(demo_spark.SHA512, 16)


def _distribution(executable: bool = True) -> bytes:
    """A stand-in Spark distribution archive: the pinned top directory with bin/spark-submit."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        names = ["bin/spark-submit"] if executable else ["README.md"]
        for name in names:
            data = b"#!/bin/sh\n"
            info = tarfile.TarInfo(f"{demo_spark.DISTRIBUTION}/{name}")
            info.size = len(data)
            info.mode = 0o755
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class TestInstall:
    def test_first_use_downloads_checks_and_unpacks_then_a_second_use_downloads_nothing(
        self, tmp_path
    ):
        archive = _distribution()
        seen: list[str] = []
        home = demo_spark.install(
            tmp_path / "spark", stream=_stream_of(archive, seen), sha512=_sha512(archive)
        )
        assert home == tmp_path / "spark" / demo_spark.SPARK_VERSION / demo_spark.DISTRIBUTION
        assert (home / "bin" / "spark-submit").stat().st_mode & 0o111
        assert seen == [demo_spark.URL]
        assert list((tmp_path / "spark" / "downloads").iterdir()) == []
        demo_spark.install(
            tmp_path / "spark", stream=_stream_of(archive, seen), sha512=_sha512(archive)
        )
        assert seen == [demo_spark.URL]

    def test_a_bad_checksum_installs_nothing(self, tmp_path):
        with pytest.raises(DemoSparkFailed) as refused:
            demo_spark.install(tmp_path / "spark", stream=_stream_of(_distribution()))
        assert refused.value.code == "demo_spark.checksum"
        assert not (tmp_path / "spark" / demo_spark.SPARK_VERSION).exists()
        assert list((tmp_path / "spark" / "downloads").iterdir()) == []

    def test_an_archive_without_the_distribution_is_refused_by_name(self, tmp_path):
        archive = _distribution(executable=False)
        with pytest.raises(DemoSparkFailed) as refused:
            demo_spark.install(
                tmp_path / "spark", stream=_stream_of(archive), sha512=_sha512(archive)
            )
        assert refused.value.code == "demo_spark.unpack"
        assert not demo_spark.spark_home(tmp_path / "spark").exists()


class TestServer:
    def test_the_server_binds_only_the_loopback_on_the_port_it_is_given(self, tmp_path):
        args = demo_spark.command(tmp_path / "home", 41234, tmp_path / "work")
        assert args[0] == str(tmp_path / "home" / "bin" / "spark-submit")
        assert demo_spark.CONNECT_SERVER_CLASS in args
        assert "spark.connect.grpc.binding.port=41234" in args
        assert "spark.connect.grpc.binding.address=127.0.0.1" in args
        assert "spark.driver.bindAddress=127.0.0.1" in args
        assert "local[*]" in args

    def test_it_runs_on_the_shipped_jdk4py_java(self, tmp_path):
        import jdk4py

        env = demo_spark.environment(tmp_path / "home", tmp_path / "work")
        assert env["JAVA_HOME"] == str(Path(jdk4py.JAVA_HOME))
        assert env["SPARK_HOME"] == str(tmp_path / "home")
        assert env["SPARK_NO_DAEMONIZE"] == "1"

    def test_it_is_kept_under_provisa_home(self, tmp_path, monkeypatch):
        assert demo_spark.spark_root() == tmp_path / "home" / ".provisa" / "spark"
        monkeypatch.setenv("PROVISA_HOME", str(tmp_path / "elsewhere"))
        assert demo_spark.spark_root() == tmp_path / "elsewhere" / "spark"

    def test_a_free_loopback_port_is_bindable(self):
        import socket

        port = demo_spark.free_loopback_port()
        with socket.socket() as s:
            s.bind(("127.0.0.1", port))

    def test_stop_with_nothing_started_does_nothing(self):
        demo_spark.stop()
        assert demo_spark.running_address() is None


class TestResolution:
    def test_a_production_install_has_no_local_test_cluster(self):
        assert spark_cluster.local_test_cluster() is None
        assert spark_cluster.local_test_target() is None

    def test_a_named_cluster_is_used_without_starting_one(self, monkeypatch):
        monkeypatch.setenv("PROVISA_DEMO", "true")
        monkeypatch.setenv(spark_cluster.LOCAL_TEST_CLUSTER_ENV, "sc://127.0.0.1:15999")
        monkeypatch.setattr(demo_spark, "ensure", _never)
        assert spark_cluster.local_test_cluster() == "sc://127.0.0.1:15999"
        assert spark_cluster.local_test_target() == "127.0.0.1:15999"

    def test_first_use_in_a_demo_starts_it(self, monkeypatch):
        monkeypatch.setenv("PROVISA_DEMO", "true")
        monkeypatch.setattr(demo_spark, "ensure", lambda: "sc://127.0.0.1:40001")
        assert spark_cluster.local_test_cluster() == "sc://127.0.0.1:40001"

    def test_before_first_use_the_tab_shows_its_loopback_host_without_starting_it(
        self, monkeypatch
    ):
        monkeypatch.setenv("PROVISA_DEMO", "true")
        monkeypatch.setattr(demo_spark, "ensure", _never)
        assert spark_cluster.local_test_target() == "127.0.0.1"

    def test_a_cluster_that_cannot_start_is_refused_by_name(self, monkeypatch):
        monkeypatch.setenv("PROVISA_DEMO", "true")

        def failing() -> str:
            raise DemoSparkFailed("checksum", "spark.tgz has SHA-512 x, not the pinned y")

        monkeypatch.setattr(demo_spark, "ensure", failing)
        with pytest.raises(SparkClusterRefused) as refused:
            spark_cluster.local_test_cluster()
        assert refused.value.code == "spark_cluster.local_test_failed"
        assert refused.value.status == 503
        assert "not the pinned" in str(refused.value)


def _never() -> str:
    raise AssertionError("the local test cluster was started")
