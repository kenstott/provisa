# Copyright (c) 2025 Kenneth Stott
# Canary: f48b613b-bac5-4474-af7f-5625a4806acd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for driver registry and routing with pluggable drivers."""

import pytest

from provisa.executor.drivers.registry import (
    available_drivers,
    create_driver,
    has_driver,
)
from provisa.executor.drivers.base import DirectDriver


class TestDriverRegistry:
    def test_postgresql_has_driver(self):
        assert has_driver("postgresql")

    def test_mysql_has_driver(self):
        if not has_driver("mysql"):
            pytest.skip("aiomysql not installed")
        assert has_driver("mysql")

    def test_duckdb_has_driver(self):
        if not has_driver("duckdb"):
            pytest.skip("duckdb not installed")
        assert has_driver("duckdb")

    def test_sqlserver_has_driver(self):
        if not has_driver("sqlserver"):
            pytest.skip("aioodbc not installed")
        assert has_driver("sqlserver")

    def test_oracle_has_driver(self):
        if not has_driver("oracle"):
            pytest.skip("oracledb not installed")
        assert has_driver("oracle")

    def test_mongodb_no_driver(self):
        assert not has_driver("mongodb")

    def test_cassandra_no_driver(self):
        assert not has_driver("cassandra")

    def test_snowflake_no_driver(self):
        assert not has_driver("snowflake")

    def test_bigquery_no_driver(self):
        assert not has_driver("bigquery")

    def test_create_postgresql(self):
        driver = create_driver("postgresql")
        assert isinstance(driver, DirectDriver)

    def test_create_unknown_raises(self):
        with pytest.raises(KeyError):
            create_driver("mongodb")

    def test_available_includes_postgresql(self):
        drivers = available_drivers()
        assert "postgresql" in drivers

    def test_create_duckdb(self):
        try:
            driver = create_driver("duckdb")
            assert isinstance(driver, DirectDriver)
        except ImportError:
            pytest.skip("duckdb not installed")
