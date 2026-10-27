# Copyright (c) 2026 Kenneth Stott
# Canary: 847a135b-7346-4928-bc2d-ff5e8664cac4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Amazon Athena as a source type (REQ-1981): its driver and dialect, its catalog read through
Athena's metadata API, and the writes each table takes by its format."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.models import SourceType
from provisa.core.source_registry import SOURCE_TO_DIALECT
from provisa.executor.drivers import athena as driver_mod
from provisa.executor.drivers.registry import create_driver, has_driver
from provisa.federation import athena

# -- type, dialect and driver -----------------------------------------------------------------


def test_athena_is_a_source_type_with_its_own_dialect_and_driver():
    assert SourceType.athena.value == "athena"
    assert SOURCE_TO_DIALECT["athena"] == "athena"
    assert has_driver("athena")
    assert isinstance(create_driver("athena"), driver_mod.AthenaDriver)


def test_positional_parameters_become_pyathenas_named_ones():
    sql, bind = driver_mod._to_pyformat(
        "SELECT a FROM t WHERE b = $1 AND c LIKE 'x%' AND d = $10", list(range(1, 11))
    )
    assert sql == "SELECT a FROM t WHERE b = %(p1)s AND c LIKE 'x%%' AND d = %(p10)s"
    assert bind is not None and bind["p1"] == 1 and bind["p10"] == 10
    assert driver_mod._to_pyformat("SELECT 1", None) == ("SELECT 1", None)


@pytest.fixture
def connected(monkeypatch):
    """What the driver hands PyAthena's connect."""
    given: dict = {}

    def connect(**kwargs):
        given.update(kwargs)
        return SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(driver_mod.pyathena, "connect", connect)
    return given


async def test_the_connection_is_the_sources_own(connected):
    d = driver_mod.AthenaDriver()
    d.configure(
        {
            "region": "us-east-1",
            "workgroup": "analysts",
            "s3_staging_dir": "s3://bucket/results/",
            "catalog": "AwsDataCatalog",
            "role_arn": "arn:aws:iam::111122223333:role/reader",
        }
    )
    await d.connect("", 0, "sales", "AKIA", "secret")
    assert connected == {
        "region_name": "us-east-1",
        "schema_name": "sales",
        "work_group": "analysts",
        "s3_staging_dir": "s3://bucket/results/",
        "catalog_name": "AwsDataCatalog",
        "role_arn": "arn:aws:iam::111122223333:role/reader",
        "aws_access_key_id": "AKIA",
        "aws_secret_access_key": "secret",
    }


async def test_no_key_pair_uses_the_aws_credential_chain(connected):
    d = driver_mod.AthenaDriver()
    d.configure({"region": "us-east-1"})
    await d.connect("", 0, "sales", "", "")
    assert "aws_access_key_id" not in connected and "aws_secret_access_key" not in connected


async def test_a_missing_region_or_half_a_key_pair_is_refused_by_name(connected):
    d = driver_mod.AthenaDriver()
    with pytest.raises(ValueError, match="requires its region"):
        await d.connect("", 0, "sales", "", "")
    d.configure({"region": "us-east-1"})
    with pytest.raises(ValueError, match="given together, or neither"):
        await d.connect("", 0, "sales", "AKIA", "")


# -- the catalog, through Athena's metadata API -----------------------------------------------


class _Paginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        return self._pages


class _Client:
    """Athena's metadata API, answering two databases and a Hive and an Iceberg table."""

    def __init__(self):
        self.asked: list[tuple[str, dict]] = []

    def get_paginator(self, method):
        if method == "list_databases":
            return _Paginator(
                [{"DatabaseList": [{"Name": "sales"}]}, {"DatabaseList": [{"Name": "ops"}]}]
            )
        return _Paginator(
            [
                {
                    "TableMetadataList": [
                        {"Name": "pets", "Parameters": {"comment": "Pets", "EXTERNAL": "TRUE"}},
                        {"Name": "notes", "Parameters": {"table_type": "ICEBERG"}},
                    ]
                }
            ]
        )

    def get_table_metadata(self, **kwargs):
        self.asked.append(("get_table_metadata", kwargs))
        return {
            "TableMetadata": {
                "Columns": [
                    {"Name": "id", "Type": "int"},
                    {"Name": "name", "Type": "varchar(32)"},
                    {"Name": "tags", "Type": "array<string>"},
                    {"Name": "price", "Type": "decimal(10,2)"},
                ],
                "PartitionKeys": [{"Name": "day", "Type": "date"}],
                "Parameters": {"table_type": "ICEBERG"},
            }
        }


@pytest.fixture
def client(monkeypatch):
    fake = _Client()
    monkeypatch.setattr(athena, "athena_client", lambda source: fake)
    return fake


_SOURCE = SimpleNamespace(id="lake", federation_hints={"region": "us-east-1"})


def test_its_databases_and_tables_are_listed_without_reading_a_column(client):
    assert athena.databases(_SOURCE) == ["ops", "sales"]
    assert athena.tables(_SOURCE, "sales") == [
        athena.AthenaTable(name="notes", comment=None, table_format="iceberg"),
        athena.AthenaTable(name="pets", comment="Pets", table_format="hive"),
    ]
    assert client.asked == []


def test_a_selected_tables_columns_are_its_own_then_its_partition_keys(client):
    assert athena.columns(_SOURCE, "sales", "notes") == [
        ("id", "integer"),
        ("name", "text"),
        # REQ-1965: a native array is a JSON column
        ("tags", "json"),
        ("price", "numeric"),
        ("day", "date"),
    ]
    (call,) = client.asked
    assert call[1] == {
        "CatalogName": "AwsDataCatalog",
        "DatabaseName": "sales",
        "TableName": "notes",
    }


def test_a_column_type_with_no_ir_type_is_text():
    # REQ-2171: registration never fails on a source type; one naming none of the IR's is text.
    assert athena.ir_type("hyperloglog") == "text"


def test_the_credential_is_the_sources_and_half_a_pair_is_refused():
    lonely = SimpleNamespace(
        id="lake", federation_hints={"region": "us-east-1"}, username="AKIA", password=""
    )
    with pytest.raises(ValueError, match="given together, or neither"):
        athena.athena_client(lonely)


# -- writes, by the table's format --------------------------------------------------------------


def _table(fmt: str | None) -> dict:
    return {"source_id": "lake", "table_name": "t", "table_format": fmt}


def _writes(fmt: str | None) -> tuple[frozenset, frozenset]:
    from provisa.executor.write_capability import table_write_ops, table_write_refused_forms

    return (
        table_write_ops(_table(fmt), "athena", None, source_path=None),
        table_write_refused_forms(_table(fmt), "athena", None, source_path=None),
    )


def test_an_iceberg_table_takes_every_write_and_a_hive_table_inserts_only():
    assert _writes("iceberg") == (
        frozenset({"insert", "update", "delete"}),
        frozenset({"on_conflict", "update_from", "delete_using", "truncate"}),
    )
    assert _writes("hive") == (
        frozenset({"insert"}),
        frozenset({"on_conflict", "update_from", "delete_using", "truncate", "merge"}),
    )


def test_a_write_to_athena_returns_no_rows():
    from provisa.executor.write_capability import table_write_returns_rows

    assert table_write_returns_rows(_table("iceberg"), "athena", None, source_path=None) is False
