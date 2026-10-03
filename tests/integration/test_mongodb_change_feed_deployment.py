# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""A MongoDB change feed needs a server that serves change streams (REQ-1861).

Against real servers: a standalone mongod is refused by name, saying change streams need a
replica set and naming poll as the alternative; a replica set passes; a server that cannot be
reached raises the driver's own error, never a pass. Everything here is the ``test`` instance:
two mongod containers this module starts on leased ports and removes."""

from __future__ import annotations

import os
import subprocess
import time

import pytest

pytestmark = [pytest.mark.integration]


def _start(name: str, port: int, *args: str) -> None:
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--memory", "384m", "--name", name]
        + ["-p", f"127.0.0.1:{port}:27017", "mongo:7", "--bind_ip_all", *args],
        check=True,
        capture_output=True,
    )


def _answering(port: int) -> None:
    import pymongo
    import pymongo.errors

    deadline = time.monotonic() + 120
    while True:
        try:
            pymongo.MongoClient(
                "127.0.0.1", port, directConnection=True, serverSelectionTimeoutMS=2000
            ).admin.command("ping")
            return
        except pymongo.errors.PyMongoError:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


@pytest.fixture(scope="module")
def servers():
    import pymongo

    from tests.port_lease import lease_ports

    standalone_port, replica_port, closed_port = lease_ports(3)
    standalone = f"provisa-itest-cf-standalone-{os.getpid()}"
    replica = f"provisa-itest-cf-rs-{os.getpid()}"
    try:
        _start(standalone, standalone_port)
        _start(replica, replica_port, "--replSet", "rs0")
        _answering(standalone_port)
        _answering(replica_port)
        pymongo.MongoClient("127.0.0.1", replica_port, directConnection=True).admin.command(
            "replSetInitiate",
            {"_id": "rs0", "members": [{"_id": 0, "host": f"127.0.0.1:{replica_port}"}]},
        )
        yield standalone_port, replica_port, closed_port
    finally:
        for name in (standalone, replica):
            subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def test_a_standalone_server_is_refused_by_name(servers):
    from provisa.mongodb.fetch import (
        ChangeStreamsUnavailable,
        MongoConnection,
        require_change_streams,
    )

    standalone_port, _, _ = servers
    with pytest.raises(ChangeStreamsUnavailable) as refused:
        require_change_streams(MongoConnection.build("127.0.0.1", standalone_port), "orders_db")
    assert refused.value.code == "schema.change_streams_unavailable"
    assert refused.value.params == {"source": "orders_db"}
    assert "standalone" in str(refused.value)
    assert "replica set" in str(refused.value)
    assert "poll" in str(refused.value)


def test_a_replica_set_passes(servers):
    from provisa.mongodb.fetch import MongoConnection, require_change_streams

    _, replica_port, _ = servers
    require_change_streams(MongoConnection.build("127.0.0.1", replica_port), "orders_db")


def test_a_server_that_cannot_be_reached_is_its_own_error_not_a_pass(servers):
    import pymongo.errors

    from provisa.mongodb.fetch import MongoConnection, require_change_streams

    _, _, closed_port = servers
    with pytest.raises(pymongo.errors.ServerSelectionTimeoutError):
        require_change_streams(MongoConnection.build("127.0.0.1", closed_port), "orders_db")
