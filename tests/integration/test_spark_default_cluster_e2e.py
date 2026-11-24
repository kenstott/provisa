# Copyright (c) 2026 Kenneth Stott
# Canary: 9e4a7c12-58d3-4b6f-a1e0-3d7f2c8b5a49
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The default Spark cluster's connection test against a real one-node Spark (REQ-2225).

The test calls a Spark Connect server as the admin settings page does: one AnalyzePlan call over
gRPC asking its Spark version, encoded by Provisa with no Spark client. A real server answering
proves the call is the one Spark Connect serves; a port nothing listens on is named as unreachable.

Lands on the TEST instance only: a Spark container of this run on a leased loopback port."""

# Requirements: REQ-2225

from __future__ import annotations

import os
import subprocess
import time

import pytest

from provisa.core import spark_cluster
from provisa.core.spark_cluster import SparkClusterRefused

pytestmark = [pytest.mark.integration]

#: The cluster, at the version of the client in connectors/spark/uv.lock.
_SPARK_IMAGE = "apache/spark:4.3.0"
_CONNECT_PORT = 15002
_SERVER_START_S = 240


def _free_port() -> int:
    from tests.port_lease import lease_ports

    (port,) = lease_ports(1)
    return port


@pytest.fixture(scope="module")
def cluster():
    """A single-node Spark Connect server on a port of this run; its Spark Connect address."""
    port = _free_port()
    name = f"provisa-itest-spark-default-{os.getpid()}"
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--memory", "3g", "--name", name,
         "-e", "SPARK_NO_DAEMONIZE=1", "-p", f"127.0.0.1:{port}:{_CONNECT_PORT}",
         _SPARK_IMAGE, "/opt/spark/sbin/start-connect-server.sh"],
        check=True,
        capture_output=True,
    )  # fmt: skip
    address = f"sc://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + _SERVER_START_S
        while True:
            try:
                spark_cluster.probe(address)
                break
            except SparkClusterRefused as refused:
                if time.monotonic() > deadline:
                    logs = subprocess.run(["docker", "logs", name], capture_output=True, text=True)
                    raise AssertionError(
                        f"Spark Connect did not answer within {_SERVER_START_S}s: {refused}\n"
                        f"{logs.stdout[-4000:]}{logs.stderr[-4000:]}"
                    ) from None
            time.sleep(3)
        yield address
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def test_the_connection_test_answers_the_clusters_spark_version(cluster):
    assert spark_cluster.probe(cluster) == "4.3.0"


def test_an_address_with_parameters_reaches_the_same_cluster(cluster):
    assert spark_cluster.probe(f"{cluster}/;user_id=provisa-admin") == "4.3.0"


def test_a_port_nothing_listens_on_is_named_unreachable():
    with pytest.raises(SparkClusterRefused) as refused:
        spark_cluster.probe(f"sc://127.0.0.1:{_free_port()}")
    assert refused.value.code == "spark_cluster.unreachable"
