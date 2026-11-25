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

At first use the pinned pyspark sdist is downloaded from PyPI into ``<PROVISA_HOME>/spark``, its
SHA-256 checked against the one PyPI publishes and pinned here before it is kept, and the parts of
Spark it carries unpacked: its jars, and its Python client. A download whose sum is not the pinned
one is refused by name and its partial file removed; there is no unpinned or unchecked route. Its
Spark Connect server then runs on the Java runtime Provisa already ships (jdk4py), on a free
loopback port, in a session of its own, and
:data:`provisa.core.spark_cluster.LOCAL_TEST_CLUSTER_ENV` names it while it runs. It stops with the
demo: the app's shutdown stops it, and the interpreter's exit as a backstop. Spark is never
embedded in a bundle; nothing runs in Docker. It runs on macOS and Linux only: on Windows it is
refused by name, and a cluster can still be set in the organization's Spark tab.
"""

# Requirements: REQ-2225
from __future__ import annotations

import hashlib
import os
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

#: The pinned distribution: the pyspark 4.3.0 sdist, Spark 4.3.0, the version the Spark connector's
#: client is locked at (connectors/spark/uv.lock). Its URL and SHA-256 are the ones PyPI's JSON API
#: publishes for it (https://pypi.org/pypi/pyspark/4.3.0/json); PyPI keeps every release.
SPARK_VERSION = "4.3.0"
#: The Scala version of the distribution's jars (their ``_2.13`` suffix). Spark's launcher reads it
#: from ``SPARK_SCALA_VERSION``, which the distribution's bin/load-spark-env.sh would set.
SCALA_VERSION = "2.13"
DISTRIBUTION = f"pyspark-{SPARK_VERSION}"
URL = (
    "https://files.pythonhosted.org/packages/46/b2/"
    "4e28aaffe28db4745ac5df3ee9115719c1b5da5cce4106bee99c82d1eb2c/"
    f"{DISTRIBUTION}.tar.gz"
)
SHA256 = "e8c517948eff7088a7aa07449aa8eafa4a15a8d50319de2525a6fd26622476c6"
#: Where the sdist keeps each part of Spark the cluster uses, and where it goes in the install,
#: laid out as a Spark distribution is: the jars, and the Python client with its py4j.
UNPACKED = {
    f"{DISTRIBUTION}/deps/jars/": "jars/",
    f"{DISTRIBUTION}/pyspark/": "python/pyspark/",
    f"{DISTRIBUTION}/lib/": "python/lib/",
}
#: The Spark Connect server's jar: the marker of a complete install.
CONNECT_JAR = f"jars/spark-connect_{SCALA_VERSION}-{SPARK_VERSION}.jar"
#: The platforms the cluster runs on (``sys.platform``).
SUPPORTED_PLATFORMS = ("darwin", "linux")
LAUNCHER_CLASS = "org.apache.spark.launcher.Main"
SUBMIT_CLASS = "org.apache.spark.deploy.SparkSubmit"
#: The JVM option of Spark's launcher that jdk4py's runtime cannot take: the launcher adds it to
#: every command, and jdk4py's runtime does not carry the module, so the JVM refuses to start
#: ("Module jdk.incubator.vector not found"). The distribution's one user of the module is netlib's
#: BLAS, which checks for it and runs without it.
ABSENT_MODULE_OPTION = "--add-modules=jdk.incubator.vector"
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
    dest: Path, *, url: str = URL, sha256: str = SHA256, stream: Stream = stream_https
) -> Path:
    """``url`` saved as ``dest`` once its SHA-256 is ``sha256``. Written to a partial file first;
    a sum that is not the pinned one is refused (``demo_spark.checksum``) and the partial file
    removed, as it is on any failure, so ``dest`` only ever holds the checked archive."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, partial_name = tempfile.mkstemp(dir=dest.parent, prefix=f".{dest.name}.", suffix=".part")
    partial = Path(partial_name)
    digest = hashlib.sha256()
    try:
        with os.fdopen(fd, "wb") as out:
            for chunk in stream(url):
                digest.update(chunk)
                out.write(chunk)
        found = digest.hexdigest()
        if found != sha256:
            raise DemoSparkFailed(
                "checksum",
                f"{dest.name} from {url} has SHA-256 {found}, not the pinned {sha256}",
            )
        os.replace(partial, dest)
    finally:
        partial.unlink(missing_ok=True)
    return dest


def spark_home(root: Path) -> Path:
    """The unpacked distribution under ``root``."""
    return root / SPARK_VERSION / DISTRIBUTION


def _unpacked_members(tar: tarfile.TarFile) -> Iterator[tarfile.TarInfo]:
    """The members of the sdist the cluster uses, each renamed to its place in the install."""
    for member in tar:
        for prefix, place in UNPACKED.items():
            if member.name.startswith(prefix) and member.name != prefix:
                yield member.replace(name=place + member.name[len(prefix) :], deep=False)
                break


def install(root: Path, *, stream: Stream = stream_https, sha256: str = SHA256) -> Path:
    """The pinned distribution under ``root``, downloaded, checked and unpacked on a miss. Its
    Spark Connect jar is the marker of a complete install: the sdist's parts are unpacked into a
    staging directory and moved into place whole, and the sdist removed once they are."""
    home = spark_home(root)
    if (home / CONNECT_JAR).is_file():
        return home
    archive = download(root / "downloads" / f"{DISTRIBUTION}.tar.gz", sha256=sha256, stream=stream)
    home.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(dir=home.parent, prefix=".unpack-"))
    try:
        unpacked = staging / DISTRIBUTION
        try:
            with tarfile.open(archive, mode="r:gz") as tar:
                members = _unpacked_members(tar)
                tar.extractall(unpacked, members=members, filter="data")  # nosec B202 - data filter
        except (OSError, tarfile.TarError) as exc:
            raise DemoSparkFailed("unpack", f"{archive.name} could not be unpacked: {exc}") from exc
        if not (unpacked / CONNECT_JAR).is_file():
            raise DemoSparkFailed(
                "unpack", f"{archive.name} holds no {DISTRIBUTION}/deps/{CONNECT_JAR}"
            )
        os.replace(unpacked, home)
    finally:
        shutil.rmtree(staging)
    archive.unlink()
    return home


def require_supported_platform() -> None:
    """Refuse (``demo_spark.unsupported_platform``) on a platform the cluster does not run on."""
    if sys.platform in SUPPORTED_PLATFORMS:
        return
    shown = "Windows" if sys.platform == "win32" else sys.platform
    raise DemoSparkFailed(
        "unsupported_platform",
        f"the local test cluster runs on macOS and Linux only, not on {shown}: Spark on Windows "
        "needs Hadoop's winutils.exe and hadoop.dll, which neither PyPI's pyspark nor Provisa "
        "ships. Set a Spark cluster in the organization's Spark tab instead.",
    )


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


def submit_arguments(port: int, work: Path) -> list[str]:
    """spark-submit's arguments for the Spark Connect server: local mode, every address on the
    loopback."""
    return [
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


def launcher_command(home: Path, port: int, work: Path) -> list[str]:
    """Spark's own launcher, as the distribution's bin/spark-class runs it: it prints the server's
    JVM command line for these spark-submit arguments."""
    return [
        str(java_home() / "bin" / "java"),
        "-Xmx128m",
        "-cp", str(home / "jars" / "*"),
        LAUNCHER_CLASS,
        SUBMIT_CLASS,
        *submit_arguments(port, work),
    ]  # fmt: skip


def server_command(launched: bytes) -> list[str]:
    """The server's command line from the launcher's output (NUL-separated, as bin/spark-class
    reads it), without the one option jdk4py's runtime cannot take."""
    parts = [part.strip("\n") for part in launched.decode().split("\0")]
    return [part for part in parts if part and part != ABSENT_MODULE_OPTION]


def command(home: Path, port: int, work: Path) -> list[str]:
    """The Spark Connect server's command line, built by Spark's launcher."""
    launched = subprocess.run(  # noqa: S603 - code-built arguments
        launcher_command(home, port, work),
        cwd=str(work),
        env=environment(home, work),
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
    )
    if launched.returncode != 0:
        raise DemoSparkFailed(
            "start",
            f"Spark's launcher exited with {launched.returncode}:\n"
            f"{launched.stderr.decode(errors='replace')[-4000:]}",
        )
    return server_command(launched.stdout)


def environment(home: Path, work: Path) -> dict[str, str]:
    """The server's environment: ours, with jdk4py's Java and the distribution's own home."""
    env = dict(os.environ)
    env.update(
        {
            "JAVA_HOME": str(java_home()),
            "SPARK_HOME": str(home),
            "SPARK_SCALA_VERSION": SCALA_VERSION,
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
            spark_cluster.answers_locally(running.address)
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
    require_supported_platform()
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
