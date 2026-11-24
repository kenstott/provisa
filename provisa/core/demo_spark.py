# Copyright (c) 2026 Kenneth Stott
# Canary: 7d2f4b91-c3a8-4e65-9f17-0b8e5a62d4c3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The local test cluster of a demo or local install: a one-node Spark, run natively (REQ-2225).

At first use the pinned Spark distribution is downloaded from Apache's release mirror into
``<PROVISA_HOME>/spark``, its SHA-512 checked against the one pinned here before it is kept, and
unpacked. A download whose sum is not the pinned one is refused by name and its partial file
removed; there is no unpinned or unchecked route. Its Spark Connect server then runs on the Java
runtime Provisa already ships (jdk4py), on a free loopback port, in a session of its own, and
:data:`provisa.core.spark_cluster.LOCAL_TEST_CLUSTER_ENV` names it while it runs. It stops with the
demo: the app's shutdown stops it, and the interpreter's exit as a backstop. Spark is never
embedded in a bundle; nothing runs in Docker.
"""

# Requirements: REQ-2225
from __future__ import annotations

import hashlib
import os
import shutil
import signal
import socket
import subprocess
import tarfile
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

#: The pinned distribution: Spark 4.3.0, the version the Spark connector's client is locked at
#: (connectors/spark/uv.lock), with its SHA-512 as Apache publishes it beside the archive.
SPARK_VERSION = "4.3.0"
DISTRIBUTION = f"spark-{SPARK_VERSION}-bin-hadoop3"
#: Apache's release mirror (its CDN). It serves the current releases only: when this pin is
#: superseded there, the download fails loudly and the pin moves to the newer release.
#: (archive.apache.org keeps every release but serves it at about 130 KB/s, 70 minutes for this
#: archive.)
URL = f"https://dlcdn.apache.org/spark/spark-{SPARK_VERSION}/{DISTRIBUTION}.tgz"
SHA512 = (
    "51a2473d09c74b6b755d3c4158275922fe4fed9c3801d9e48ade6083dc03b456"
    "93274020fe0fcf2be2a607a6a828f7159f78609ef913049e98c1882d2d0aab5f"
)
CONNECT_SERVER_CLASS = "org.apache.spark.sql.connect.service.SparkConnectServer"
LOOPBACK = "127.0.0.1"
#: How long a start waits for the server to answer Spark Connect.
START_TIMEOUT_S = 240.0
#: How long a stop waits for the server to end on SIGTERM before it is killed.
STOP_TIMEOUT_S = 20.0
SERVER_LOG_NAME = "spark-connect.log"

#: The bytes at a URL, in chunks. Injected so unit tests never touch the network.
Stream = Callable[[str], Iterator[bytes]]


class DemoSparkFailed(RuntimeError):
    """The local test cluster could not be downloaded, unpacked or started, by name."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = f"demo_spark.{code}"


def provisa_home() -> Path:
    """``$PROVISA_HOME``, else ``~/.provisa`` (the bootstrap.home setting, REQ-1913)."""
    stated = os.environ.get("PROVISA_HOME")
    return Path(stated) if stated else Path.home() / ".provisa"


def spark_root() -> Path:
    """Where the distribution is kept and the server keeps its work."""
    return provisa_home() / "spark"


def stream_https(url: str) -> Iterator[bytes]:
    """The bytes at ``url``, over https only, in chunks. Fails loud on any error."""
    import httpx

    from provisa.core.http_failure import describe_http_failure

    if not url.startswith("https://"):
        raise DemoSparkFailed("download", f"refusing a non-https download: {url}")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=300.0) as response:
            if response.status_code != 200:
                raise DemoSparkFailed(
                    "download", f"failed to download {url}: HTTP {response.status_code}"
                )
            yield from response.iter_bytes(1 << 20)
    except httpx.HTTPError as exc:
        raise DemoSparkFailed(
            "download", f"failed to download: {describe_http_failure(exc)}"
        ) from exc


def download(
    dest: Path, *, url: str = URL, sha512: str = SHA512, stream: Stream = stream_https
) -> Path:
    """``url`` saved as ``dest`` once its SHA-512 is ``sha512``. Written to a partial file first;
    a sum that is not the pinned one is refused (``demo_spark.checksum``) and the partial file
    removed, as it is on any failure, so ``dest`` only ever holds the checked archive."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, partial_name = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.", suffix=".part")
    partial = Path(partial_name)
    digest = hashlib.sha512()
    try:
        with os.fdopen(fd, "wb") as out:
            for chunk in stream(url):
                digest.update(chunk)
                out.write(chunk)
        found = digest.hexdigest()
        if found != sha512:
            raise DemoSparkFailed(
                "checksum",
                f"{dest.name} from {url} has SHA-512 {found}, not the pinned {sha512}",
            )
        os.replace(partial, dest)
    finally:
        partial.unlink(missing_ok=True)
    return dest


def spark_home(root: Path) -> Path:
    """The unpacked distribution under ``root``."""
    return root / SPARK_VERSION / DISTRIBUTION


def install(root: Path, *, stream: Stream = stream_https, sha512: str = SHA512) -> Path:
    """The pinned distribution under ``root``, downloaded, checked and unpacked on a miss. Its
    ``bin/spark-submit`` is the marker of a complete install: the archive is unpacked into a
    staging directory and moved into place whole, and the archive removed once it is."""
    home = spark_home(root)
    if (home / "bin" / "spark-submit").is_file():
        return home
    archive = download(root / "downloads" / f"{DISTRIBUTION}.tgz", sha512=sha512, stream=stream)
    home.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(dir=home.parent, prefix=".unpack-"))
    try:
        try:
            with tarfile.open(archive, mode="r:gz") as tar:
                tar.extractall(staging, filter="data")  # nosec B202 - "data" refuses unsafe members
        except (OSError, tarfile.TarError) as exc:
            raise DemoSparkFailed("unpack", f"{archive.name} could not be unpacked: {exc}") from exc
        unpacked = staging / DISTRIBUTION
        if not (unpacked / "bin" / "spark-submit").is_file():
            raise DemoSparkFailed(
                "unpack", f"{archive.name} holds no {DISTRIBUTION}/bin/spark-submit"
            )
        os.replace(unpacked, home)
    finally:
        shutil.rmtree(staging)
    archive.unlink()
    return home


def java_home() -> Path:
    """The Java runtime of the jdk4py package, which Provisa ships (the ``sparql`` extra)."""
    try:
        import jdk4py
    except ModuleNotFoundError as exc:
        raise DemoSparkFailed(
            "java",
            "the local test cluster runs on the Java runtime of the jdk4py package, which is not "
            "installed: install provisa[sparql]",
        ) from exc
    return Path(jdk4py.JAVA_HOME)


def free_loopback_port() -> int:
    """A port nothing listens on at the loopback address now."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((LOOPBACK, 0))
        return probe.getsockname()[1]


def command(home: Path, port: int, work: Path) -> list[str]:
    """The Spark Connect server's command line: local mode, every address on the loopback."""
    return [
        str(home / "bin" / "spark-submit"),
        "--class", CONNECT_SERVER_CLASS,
        "--name", "Provisa local test cluster",
        "--master", "local[*]",
        "--conf", f"spark.connect.grpc.binding.address={LOOPBACK}",
        "--conf", f"spark.connect.grpc.binding.port={port}",
        "--conf", f"spark.driver.host={LOOPBACK}",
        "--conf", f"spark.driver.bindAddress={LOOPBACK}",
        "--conf", "spark.ui.enabled=false",
        "--conf", f"spark.local.dir={work / 'tmp'}",
        "--conf", f"spark.sql.warehouse.dir={work / 'warehouse'}",
        "--driver-java-options", f"-Dderby.system.home={work / 'derby'}",
        "spark-internal",
    ]  # fmt: skip


def environment(home: Path, work: Path) -> dict[str, str]:
    """The server's environment: ours, with jdk4py's Java and the distribution's own home."""
    env = dict(os.environ)
    env.update(
        {
            "JAVA_HOME": str(java_home()),
            "SPARK_HOME": str(home),
            "SPARK_LOCAL_IP": LOOPBACK,
            "SPARK_LOG_DIR": str(work / "logs"),
            "SPARK_NO_DAEMONIZE": "1",
        }
    )
    env.pop("PYSPARK_PYTHON", None)  # never a Python of ours: the server runs no Python
    return env


@dataclass
class Running:
    """A started server: its process (its session's leader) and its Spark Connect address."""

    process: subprocess.Popen
    address: str
    log: Path


def _await_answer(running: Running, timeout: float) -> None:
    from provisa.core import spark_cluster

    deadline = time.monotonic() + timeout
    while True:
        if running.process.poll() is not None:
            raise DemoSparkFailed(
                "start",
                f"the Spark Connect server exited with {running.process.returncode} before it "
                f"answered:\n{_tail(running.log)}",
            )
        try:
            spark_cluster.probe(running.address)
            return
        except spark_cluster.SparkClusterRefused as refused:
            if time.monotonic() > deadline:
                raise DemoSparkFailed(
                    "start",
                    f"the Spark Connect server did not answer within {timeout:.0f}s: {refused}\n"
                    f"{_tail(running.log)}",
                ) from None
        time.sleep(1.0)


def _tail(log: Path, size: int = 4000) -> str:
    return log.read_text(errors="replace")[-size:] if log.is_file() else ""


def start(
    root: Path,
    *,
    port: int | None = None,
    stream: Stream = stream_https,
    timeout: float = START_TIMEOUT_S,
) -> Running:
    """Install (on a miss) and start the server under ``root``; returned once it answers."""
    home = install(root, stream=stream)
    work = root / "work"
    for sub in ("tmp", "warehouse", "logs", "derby"):
        (work / sub).mkdir(parents=True, exist_ok=True)
    bound = port if port is not None else free_loopback_port()
    log = work / SERVER_LOG_NAME
    with open(log, "a") as out:  # the child keeps its own descriptor
        process = subprocess.Popen(  # noqa: S603 - code-built arguments
            command(home, bound, work),
            cwd=str(work),
            env=environment(home, work),
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            # Its own session, so stopping it signals its whole group and never ours.
            start_new_session=True,
        )
    running = Running(process, f"sc://{LOOPBACK}:{bound}", log)
    try:
        _await_answer(running, timeout)
    except BaseException:
        stop_process(running.process)
        raise
    return running


def stop_process(process: subprocess.Popen, timeout: float = STOP_TIMEOUT_S) -> None:
    """End ``process`` and its session's group: SIGTERM, then SIGKILL past ``timeout``."""
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            return
        process.wait(timeout)


# -- the demo's one cluster -----------------------------------------------------------------------

_lock = threading.Lock()
_running: Running | None = None
_atexit_registered = False


def ensure() -> str:
    """The local test cluster's address, started (and on first use downloaded) when it is not
    running; :data:`LOCAL_TEST_CLUSTER_ENV` names it from then until :func:`stop`."""
    from provisa.core.spark_cluster import LOCAL_TEST_CLUSTER_ENV

    global _running, _atexit_registered
    with _lock:
        if _running is not None and _running.process.poll() is None:
            return _running.address
        _running = start(spark_root())
        os.environ[LOCAL_TEST_CLUSTER_ENV] = _running.address
        if not _atexit_registered:
            import atexit

            # The backstop for an exit that never reaches the app's shutdown; the app stops it.
            atexit.register(stop)
            _atexit_registered = True
        return _running.address


def running_address() -> str | None:
    """The address of the cluster this process started, while it runs."""
    with _lock:
        if _running is None or _running.process.poll() is not None:
            return None
        return _running.address


def stop() -> None:
    """Stop the cluster this process started, if it did, and stop naming it."""
    from provisa.core.spark_cluster import LOCAL_TEST_CLUSTER_ENV

    global _running
    with _lock:
        if _running is None:
            return
        stop_process(_running.process)
        if os.environ.get(LOCAL_TEST_CLUSTER_ENV) == _running.address:
            del os.environ[LOCAL_TEST_CLUSTER_ENV]
        _running = None
