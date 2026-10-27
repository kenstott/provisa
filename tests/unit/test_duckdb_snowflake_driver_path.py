# Copyright (c) 2026 Kenneth Stott
# Canary: 9bdb867d-5c3d-49a7-af46-1eeef21ade11
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-899: the snowflake extension is loaded with the ADBC Snowflake driver Provisa ships.

The extension reads its driver through Arrow ADBC and searches only its own locations
(~/.duckdb/extensions, ADBC manifests), never the adbc-driver-snowflake package Provisa depends
on, so every attach failed with "ADBC Snowflake driver (libadbc_driver_snowflake.so) not found"
and no statement the engine runs over a Snowflake table could read it. SNOWFLAKE_ADBC_DRIVER_PATH
names the driver to it, read from the process environment when it connects.
"""

# Requirements: REQ-899

import os

import adbc_driver_snowflake

from provisa.federation.duckdb_extensions import install_and_load

_ENV = "SNOWFLAKE_ADBC_DRIVER_PATH"


class _Con:
    """A connection that records each statement with the driver path the process held then."""

    def __init__(self) -> None:
        self.ran: list[tuple[str, str | None]] = []

    def execute(self, sql: str) -> None:
        self.ran.append((sql, os.environ.get(_ENV)))


def test_the_snowflake_extension_is_loaded_with_the_shipped_driver(monkeypatch):
    # Set then removed, so the variable the load sets is removed again after the test.
    monkeypatch.setenv(_ENV, "unset")
    monkeypatch.delenv(_ENV)
    con = _Con()
    install_and_load(con, "snowflake")  # pyright: ignore[reportArgumentType]
    shipped = adbc_driver_snowflake._driver_path()  # pyright: ignore[reportPrivateUsage]
    assert os.path.exists(shipped)
    assert ("LOAD snowflake", shipped) in con.ran
    assert os.environ[_ENV] == shipped


def test_another_extension_names_no_snowflake_driver(monkeypatch):
    monkeypatch.setenv(_ENV, "unset")
    monkeypatch.delenv(_ENV)
    install_and_load(_Con(), "mongo")  # pyright: ignore[reportArgumentType]
    assert _ENV not in os.environ
