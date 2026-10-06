# Copyright (c) 2026 Kenneth Stott
# Canary: 49dfd305-f678-4758-bacd-249f5233ce00
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1494: the platform fake key is never written into a log. A server started on a deployment
key writes it to the engine's key directory; the engine computes digests under it; neither the
server's log nor the engine's carries the key -- only its fingerprint may appear."""

from __future__ import annotations

import os
import secrets
import subprocess
from pathlib import Path

import pytest

from provisa.fakes.digest import digest, fingerprint
from tests.integration.worker_boot_harness import WorkerBoot
from tests.itest_stack import COMPOSE_ARGS

pytestmark = [pytest.mark.integration]


def test_neither_the_server_nor_the_engine_logs_the_key(trino_conn):
    key = secrets.token_bytes(32)
    boot = WorkerBoot(
        1,
        pg_host=os.environ.get("PG_HOST", "localhost"),
        pg_port=int(os.environ.get("PG_PORT", "5432")),
        env={"PROVISA_FAKE_KEY": key.hex(), "PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        fp = fingerprint(key)
        assert (Path(os.environ["PROVISA_FAKE_KEY_DIR"]) / f"{fp}.key").read_text() == key.hex()
        cur = trino_conn.cursor()
        cur.execute(f"SELECT provisa_digest('{fp}', 'ann@example.com')")
        assert cur.fetchall()[0][0] == digest(key, "ann@example.com")
        engine_log = subprocess.run(
            ["docker", "compose", *COMPOSE_ARGS, "logs", "--no-color", "trino"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        assert engine_log, "the engine's log was not read"
        for log in (boot.log_text(), engine_log):
            assert key.hex() not in log and key.hex().upper() not in log
    finally:
        boot.cleanup()
