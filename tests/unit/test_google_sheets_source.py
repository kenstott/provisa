# Copyright (c) 2026 Kenneth Stott
# Canary: dd9e8f60-50b5-4aa8-95e7-21a8bab5c55f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for Google Sheets source mapping."""

from __future__ import annotations

from provisa.google_sheets.source import (
    GoogleSheetsSourceConfig,
    GoogleSheetsTableConfig,
    generate_catalog_properties,
    generate_table_definitions,
)


# --------------------------------------------------------------------------- #
# Helpers                                                                      #
# --------------------------------------------------------------------------- #


def _source_config(
    *,
    metadata_sheet_id: str = "meta-sheet-123",
    credentials_key: str = "gsheets-sa-key",
    tables: list[GoogleSheetsTableConfig] | None = None,
) -> GoogleSheetsSourceConfig:
    return GoogleSheetsSourceConfig(
        id="gsheets-1",
        metadata_sheet_id=metadata_sheet_id,
        credentials_key=credentials_key,
        tables=tables or [],
    )


def _table_config(
    name: str = "sales",
    sheet_url: str = "https://docs.google.com/spreadsheets/d/abc123/edit#gid=0",
    columns: list[dict] | None = None,
) -> GoogleSheetsTableConfig:
    return GoogleSheetsTableConfig(
        name=name,
        sheet_url=sheet_url,
        columns=columns or [{"name": "id", "type": "VARCHAR"}],
    )


# --------------------------------------------------------------------------- #
# TestGenerateCatalogProperties                                                #
# --------------------------------------------------------------------------- #


class TestGenerateCatalogProperties:
    def test_connector_name_is_gsheets(self):
        props = generate_catalog_properties(_source_config())
        assert props["connector.name"] == "gsheets"

    def test_metadata_sheet_id_propagated(self):
        props = generate_catalog_properties(_source_config(metadata_sheet_id="sheet-xyz"))
        assert props["gsheets.metadata-sheet-id"] == "sheet-xyz"

    def test_credentials_path_references_secret(self):
        props = generate_catalog_properties(_source_config(credentials_key="my-secret"))
        assert props["gsheets.credentials-path"] == "${secret:my-secret}"

    def test_exactly_three_keys_returned(self):
        props = generate_catalog_properties(_source_config())
        assert set(props.keys()) == {
            "connector.name",
            "gsheets.metadata-sheet-id",
            "gsheets.credentials-path",
        }


# --------------------------------------------------------------------------- #
# TestGenerateTableDefinitions                                                 #
# --------------------------------------------------------------------------- #


class TestGenerateTableDefinitions:
    def test_empty_tables_returns_empty_list(self):
        assert generate_table_definitions(_source_config(tables=[])) == []

    def test_single_table_produces_one_entry(self):
        cfg = _source_config(tables=[_table_config()])
        assert len(generate_table_definitions(cfg)) == 1

    def test_table_name_in_definition(self):
        cfg = _source_config(tables=[_table_config(name="invoices")])
        defs = generate_table_definitions(cfg)
        assert defs[0]["tableName"] == "invoices"

    def test_sheet_url_in_definition(self):
        url = "https://docs.google.com/spreadsheets/d/xyz/edit"
        cfg = _source_config(tables=[_table_config(sheet_url=url)])
        defs = generate_table_definitions(cfg)
        assert defs[0]["sheetUrl"] == url

    def test_columns_in_definition(self):
        cols = [{"name": "region", "type": "VARCHAR"}, {"name": "revenue", "type": "DOUBLE"}]
        cfg = _source_config(tables=[_table_config(columns=cols)])
        defs = generate_table_definitions(cfg)
        assert defs[0]["columns"] == cols

    def test_definition_has_expected_keys(self):
        cfg = _source_config(tables=[_table_config()])
        defs = generate_table_definitions(cfg)
        assert set(defs[0].keys()) == {"tableName", "sheetUrl", "columns"}

    def test_multiple_tables_produce_multiple_definitions(self):
        t1 = _table_config(name="sales", sheet_url="https://docs.google.com/spreadsheets/d/a")
        t2 = _table_config(name="costs", sheet_url="https://docs.google.com/spreadsheets/d/b")
        cfg = _source_config(tables=[t1, t2])
        defs = generate_table_definitions(cfg)
        assert len(defs) == 2
        assert {d["tableName"] for d in defs} == {"sales", "costs"}
