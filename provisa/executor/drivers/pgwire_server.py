# Copyright (c) 2026 Kenneth Stott
# Canary: b61f4d09-7c35-4e8a-9a12-3d0e5f7c8a46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The driver a source's own pgwire server is written through (REQ-1946).

The server speaks the PostgreSQL wire, so this is the PostgreSQL driver, held to what the server
carries. The server commits each write as it runs and refuses a ROLLBACK after one, so nothing
here may open a transaction: the connections are autocommit (the PostgreSQL driver's own), each
statement is one ``execute``, and the two paths of that driver that open a transaction — the
server-side cursor of a streamed read and the raw passthrough built on it — are not offered. A
pooled connection that never enters a transaction is returned without a ROLLBACK.
"""

from __future__ import annotations

from provisa.executor.drivers.postgresql import PostgreSQLDriver


class PgwireServerDriver(PostgreSQLDriver):
    @property
    def supports_streaming(self) -> bool:
        return False

    async def open_stream(self, sql: str, params: list | None = None):
        del sql, params
        raise NotImplementedError(
            "a source's pgwire server is not read through a server-side cursor: it would open a "
            "transaction the server cannot roll back"
        )
