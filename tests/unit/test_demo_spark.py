# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e6f20-d47a-4c19-b5e2-9a1c7d04f8e6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The demo's local test cluster: its download of the pyspark sdist, checked against the pinned
SHA-256, its install, its refusal on Windows, and its address as the demo's resolution reports it
(REQ-2225). No network: the download is an injected stream; HOME is a temp directory."""

# Requirements: REQ-2225
from __future__ import annotations

import hashlib
import io
import sys
import tarfile
from pathlib import Path

import pytest

from provisa.core import demo_spark, spark_cluster
from provisa.core.demo_spark import DemoSparkFailed
from provisa.core.spark_cluster import SparkClusterRefused

_BODY = b"spark distribution bytes " * 1000


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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
        dest = tmp_path / "dl" / "pyspark.tar.gz"
        kept = demo_spark.download(
            dest, url="https://files.example/pyspark.tar.gz", sha256=_sha256(_BODY),
            stream=_stream_of(_BODY),
        )  # fmt: skip
        assert kept == dest
        assert dest.read_bytes() == _BODY
        assert sorted(p.name for p in dest.parent.iterdir()) == ["pyspark.tar.gz"]

    def test_a_bad_checksum_is_refused_by_name_and_the_partial_file_removed(self, tmp_path):
        dest = tmp_path / "dl" / "pyspark.tar.gz"
        with pytest.raises(DemoSparkFailed) as refused:
            demo_spark.download(
                dest, url="https://files.example/pyspark.tar.gz", sha256="0" * 64,
                stream=_stream_of(_BODY),
            )  # fmt: skip
        assert refused.value.code == "demo_spark.checksum"
        assert _sha256(_BODY) in str(refused.value)
        assert "0" * 64 in str(refused.value)
        assert "SHA-256" in str(refused.value)
        assert not dest.exists()
        assert list(dest.parent.iterdir()) == []

    def test_a_download_that_fails_partway_leaves_no_partial_file(self, tmp_path):
        def broken(_url: str):
            yield b"first chunk"
            raise DemoSparkFailed("download", "connection reset")

        dest = tmp_path / "dl" / "pyspark.tar.gz"
        with pytest.raises(DemoSparkFailed, match="connection reset"):
            demo_spark.download(dest, sha256=_sha256(_BODY), stream=broken)
        assert list(dest.parent.iterdir()) == []

    def test_a_non_https_url_is_refused(self):
        with pytest.raises(DemoSparkFailed) as refused:
            list(demo_spark.stream_https("http://archive.apache.org/dist/spark.tgz"))
        assert refused.value.code == "demo_spark.download"

    def test_the_pin_is_the_pyspark_sdist_on_pypi_with_its_sha256(self):
        assert demo_spark.URL.startswith("https://files.pythonhosted.org/packages/")
        assert demo_spark.URL.endswith(f"/pyspark-{demo_spark.SPARK_VERSION}.tar.gz")
        assert demo_spark.SHA256 == (
            "e8c517948eff7088a7aa07449aa8eafa4a15a8d50319de2525a6fd26622476c6"
        )


_CONNECT_JAR = f"spark-connect_2.13-{demo_spark.SPARK_VERSION}.jar"


def _distribution(with_connect_jar: bool = True) -> bytes:
    """A stand-in pyspark sdist: its jars under deps/, its Python client, and parts the cluster
    does not use (setup.py, deps/bin, deps/examples)."""
    top = demo_spark.DISTRIBUTION
    names = [
        f"{top}/setup.py",
        f"{top}/deps/bin/spark-submit",
        f"{top}/deps/examples/pi.py",
        f"{top}/deps/jars/spark-core_2.13-{demo_spark.SPARK_VERSION}.jar",
        f"{top}/pyspark/__init__.py",
        f"{top}/lib/py4j-0.10.9.9-src.zip",
    ]
    if with_connect_jar:
        names.append(f"{top}/deps/jars/{_CONNECT_JAR}")
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as tar:
        for directory in (f"{top}/deps/jars/", f"{top}/pyspark/"):
            info = tarfile.TarInfo(directory.rstrip("/"))
            info.type = tarfile.DIRTYPE
            info.mode = 0o755
            tar.addfile(info)
        for name in names:
            data = name.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class TestInstall:
    def test_first_use_downloads_checks_and_unpacks_then_a_second_use_downloads_nothing(
        self, tmp_path
    ):
        archive = _distribution()
        seen: list[str] = []
        home = demo_spark.install(
            tmp_path / "spark", stream=_stream_of(archive, seen), sha256=_sha256(archive)
        )
        assert home == tmp_path / "spark" / demo_spark.SPARK_VERSION / demo_spark.DISTRIBUTION
        top = demo_spark.DISTRIBUTION
        assert (
            home / "jars" / _CONNECT_JAR
        ).read_bytes() == f"{top}/deps/jars/{_CONNECT_JAR}".encode()
        assert sorted(p.name for p in home.iterdir()) == ["jars", "python"]
        assert sorted(p.name for p in (home / "jars").iterdir()) == sorted(
            [_CONNECT_JAR, f"spark-core_2.13-{demo_spark.SPARK_VERSION}.jar"]
        )
        assert (home / "python" / "pyspark" / "__init__.py").is_file()
        assert (home / "python" / "lib" / "py4j-0.10.9.9-src.zip").is_file()
        assert seen == [demo_spark.URL]
        assert list((tmp_path / "spark" / "downloads").iterdir()) == []
        demo_spark.install(
            tmp_path / "spark", stream=_stream_of(archive, seen), sha256=_sha256(archive)
        )
        assert seen == [demo_spark.URL]

    def test_a_bad_checksum_installs_nothing(self, tmp_path):
        with pytest.raises(DemoSparkFailed) as refused:
            demo_spark.install(tmp_path / "spark", stream=_stream_of(_distribution()))
        assert refused.value.code == "demo_spark.checksum"
        assert not (tmp_path / "spark" / demo_spark.SPARK_VERSION).exists()
        assert list((tmp_path / "spark" / "downloads").iterdir()) == []

    def test_an_archive_without_the_connect_server_is_refused_by_name(self, tmp_path):
        archive = _distribution(with_connect_jar=False)
        with pytest.raises(DemoSparkFailed) as refused:
            demo_spark.install(
                tmp_path / "spark", stream=_stream_of(archive), sha256=_sha256(archive)
            )
        assert refused.value.code == "demo_spark.unpack"
        assert not demo_spark.spark_home(tmp_path / "spark").exists()


class TestServer:
    def test_the_server_binds_only_the_loopback_on_the_port_it_is_given(self, tmp_path):
        args = demo_spark.submit_arguments(41234, tmp_path / "work")
        assert demo_spark.CONNECT_SERVER_CLASS in args
        assert "spark.connect.grpc.binding.port=41234" in args
        assert "spark.connect.grpc.binding.address=127.0.0.1" in args
        assert "spark.driver.bindAddress=127.0.0.1" in args
        assert "local[*]" in args

    def test_sparks_own_launcher_builds_the_command_on_the_shipped_jdk4py_java(self, tmp_path):
        import jdk4py

        launcher = demo_spark.launcher_command(tmp_path / "home", 41234, tmp_path / "work")
        assert launcher[0] == str(Path(jdk4py.JAVA_HOME) / "bin" / "java")
        assert launcher[launcher.index("-cp") + 1] == str(tmp_path / "home" / "jars" / "*")
        assert launcher[4:6] == [demo_spark.LAUNCHER_CLASS, demo_spark.SUBMIT_CLASS]
        assert launcher[6:] == demo_spark.submit_arguments(41234, tmp_path / "work")

    def test_the_server_command_drops_only_the_module_jdk4py_does_not_carry(self):
        launched = (
            b"\0\n/jdk/bin/java\0-cp\0/spark/jars/*\0-Xmx1g\0"
            b"--add-modules=jdk.incubator.vector\0--add-opens=java.base/java.lang=ALL-UNNAMED\0"
            b"org.apache.spark.deploy.SparkSubmit\0--master\0local[*]\0"
        )
        assert demo_spark.server_command(launched) == [
            "/jdk/bin/java", "-cp", "/spark/jars/*", "-Xmx1g",
            "--add-opens=java.base/java.lang=ALL-UNNAMED",
            "org.apache.spark.deploy.SparkSubmit", "--master", "local[*]",
        ]  # fmt: skip

    def test_it_runs_on_the_shipped_jdk4py_java(self, tmp_path):
        import jdk4py

        env = demo_spark.environment(tmp_path / "home", tmp_path / "work")
        assert env["JAVA_HOME"] == str(Path(jdk4py.JAVA_HOME))
        assert env["SPARK_HOME"] == str(tmp_path / "home")
        assert env["SPARK_SCALA_VERSION"] == "2.13"
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


class TestPlatform:
    @pytest.mark.parametrize("platform", ["darwin", "linux"])
    def test_macos_and_linux_are_supported(self, platform, monkeypatch):
        monkeypatch.setattr(sys, "platform", platform)
        demo_spark.require_supported_platform()

    def test_windows_is_refused_by_name_before_anything_is_downloaded(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "win32")

        def no_download(url: str):
            raise AssertionError(f"downloaded {url}")

        with pytest.raises(DemoSparkFailed) as refused:
            demo_spark.start(demo_spark.spark_root(), stream=no_download)
        assert refused.value.code == "demo_spark.unsupported_platform"
        assert "macOS and Linux only, not on Windows" in str(refused.value)
        assert "Spark tab" in str(refused.value)
        assert not demo_spark.spark_root().exists()

    def test_on_windows_the_demo_names_spark_unavailable_with_the_reason(self, monkeypatch):
        monkeypatch.setenv("PROVISA_DEMO", "true")
        monkeypatch.setattr(sys, "platform", "win32")
        with pytest.raises(SparkClusterRefused) as refused:
            spark_cluster.local_test_cluster()
        assert refused.value.code == "spark_cluster.local_test_unavailable"
        assert refused.value.status == 503
        assert "unavailable" in str(refused.value)
        assert "winutils" in str(refused.value)
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
            raise DemoSparkFailed("checksum", "pyspark.tar.gz has SHA-256 x, not the pinned y")

        monkeypatch.setattr(demo_spark, "ensure", failing)
        with pytest.raises(SparkClusterRefused) as refused:
            spark_cluster.local_test_cluster()
        assert refused.value.code == "spark_cluster.local_test_failed"
        assert refused.value.status == 503
        assert "not the pinned" in str(refused.value)


def _never() -> str:
    raise AssertionError("the local test cluster was started")
