# Copyright (c) 2026 Kenneth Stott
# Canary: b6815c49-1272-42d1-9fd0-5cc484d9d7c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.


"""The test instance's store credentials.

docker-compose.core.yml sets no credential of its own; a deployment's launcher generates them.
The test instance's stacks are isolated per run on leased, loopback-only ports, and the tests
connect with these fixed values. They live here, in the test harness, and nowhere in the product.
Every compose command a test or a test harness runs inherits them from the environment.
"""

from __future__ import annotations

import os

STORE_ENV = {
    "PROVISA_PG_PASSWORD": "provisa",
    "PROVISA_REDIS_PASSWORD": "provisa",
    "PROVISA_MINIO_ROOT_USER": "minioadmin",
    "PROVISA_MINIO_ROOT_PASSWORD": "minioadmin",
    "PROVISA_GRAFANA_ADMIN_PASSWORD": "provisa",
    "PROVISA_TRINO_SHARED_SECRET": "provisa-test-instance-trino-shared-secret",
    "PROVISA_STORE_BIND_IP": "127.0.0.1",
    # What the app signs in to the bundled stores with.
    "CONFIG_DB_PASSWORD": "provisa",
    "PROVISA_OTEL_S3_ACCESS_KEY": "minioadmin",
    "PROVISA_OTEL_S3_SECRET_KEY": "minioadmin",
}


def apply() -> None:
    """Export them, keeping any a run was given."""
    for name, value in STORE_ENV.items():
        os.environ.setdefault(name, value)


def redis_url(port: str | int, db: int = 0) -> str:
    return f"redis://:{os.environ['PROVISA_REDIS_PASSWORD']}@localhost:{port}/{db}"
