# Copyright (c) 2026 Kenneth Stott
# Canary: a61c9e47-2b05-4f8d-93e1-c7d4058b2f19
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The demo's local test cluster, run natively (REQ-2225): first use downloads the pinned pyspark
sdist from PyPI, checks its SHA-256, unpacks the Spark jars it carries, and starts its Spark
Connect server on the shipped jdk4py Java on a free loopback port; the demo names it in PROVISA_DEMO_SPARK_CONNECT while
it runs; one Spark Connect query is answered; stopping it leaves no process behind.

Lands on the TEST instance only: PROVISA_HOME and HOME are a temp directory of this run, so
nothing reaches ~/.provisa or a running demo."""

# Requirements: REQ-2225

from __future__ import annotations

import os
import socket
import subprocess
import sys

import pytest

from provisa.core import demo_spark, spark_cluster

pytestmark = [pytest.mark.integration]

#: The query: the sdist's own Spark Connect client, on this interpreter's packages.
_QUERY = (
    "import sys\n"
    "from pyspark.sql import SparkSession\n"
    "spark = SparkSession.builder.remote(sys.argv[1]).getOrCreate()\n"
    "print('answer', spark.sql('SELECT 40 + 2 AS answer').collect()[0]['answer'])\n"
    "spark.stop()\n"
)


@pytest.fixture
def demo_home(tmp_path_factory, monkeypatch):
    home = tmp_path_factory.mktemp("demo-spark-home")
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("PROVISA_HOME", str(home / ".provisa"))
    monkeypatch.setenv("PROVISA_DEMO", "true")
    monkeypatch.delenv(spark_cluster.LOCAL_TEST_CLUSTER_ENV, raising=False)
    yield home
    demo_spark.stop()


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    return True


def _listening(port: int) -> bool:
    with socket.socket() as s:
        s.settimeout(1.0)
        return s.connect_ex(("127.0.0.1", port)) == 0


def test_the_demo_starts_queries_and_stops_its_local_test_cluster(demo_home):
    # First use: resolution downloads, checks and starts it.
    address = spark_cluster.local_test_cluster()
    assert address is not None and address.startswith("sc://127.0.0.1:")
    assert os.environ[spark_cluster.LOCAL_TEST_CLUSTER_ENV] == address
    assert demo_spark.running_address() == address
    port = spark_cluster.parse(address).port
    assert spark_cluster.local_test_target() == f"127.0.0.1:{port}"

    home = demo_spark.spark_home(demo_spark.spark_root())
    assert home.is_relative_to(demo_home / ".provisa" / "spark")
    assert (home / demo_spark.CONNECT_JAR).is_file()
    assert list((demo_spark.spark_root() / "downloads").iterdir()) == []

    # The demo is single-org: its own org is not held to public destinations (REQ-2006).
    assert spark_cluster.probe(address, org_id="acme") == demo_spark.SPARK_VERSION
    # A second use runs on the same server.
    assert spark_cluster.local_test_cluster() == address

    py4j = sorted((home / "python" / "lib").glob("py4j-*.zip"))
    query = subprocess.run(
        [sys.executable, "-c", _QUERY, address],
        env={
            **os.environ,
            "SPARK_HOME": str(home),
            "PYTHONPATH": os.pathsep.join([str(home / "python"), *map(str, py4j)]),
        },
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert query.returncode == 0, query.stderr[-4000:]
    assert "answer 42" in query.stdout

    running = demo_spark._running  # pyright: ignore[reportPrivateUsage]  # its process, to check it ends
    assert running is not None
    pgid = running.process.pid
    assert _group_alive(pgid)

    demo_spark.stop()

    assert spark_cluster.LOCAL_TEST_CLUSTER_ENV not in os.environ
    assert demo_spark.running_address() is None
    assert not _group_alive(pgid), "a process of the local test cluster outlived its stop"
    assert not _listening(port)
