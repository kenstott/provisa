# Copyright (c) 2026 Kenneth Stott
# Canary: f1b51da3-232a-4f52-b064-91dea4b6898f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Google Sheets source mapping — sheet ranges to tables via Trino connector.

Each sheet range becomes a table. Column names come from the first row or
explicit config. The metadata sheet holds the schema definitions that the
Trino Google Sheets connector reads.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class GoogleSheetsTableConfig:
    """Table mapped from a Google Sheets range."""

    name: str
    sheet_url: str
    columns: list[dict]  # list of {"name": str, "type": str}


@dataclass
class GoogleSheetsSourceConfig:
    """Google Sheets source connection + table mappings."""

    id: str
    metadata_sheet_id: str
    credentials_key: str  # name of secret holding service account JSON
    tables: list[GoogleSheetsTableConfig] = field(default_factory=list)


def generate_catalog_properties(config: GoogleSheetsSourceConfig) -> dict[str, str]:
    """Generate Trino Google Sheets connector catalog properties."""
    return {
        "connector.name": "gsheets",
        "gsheets.metadata-sheet-id": config.metadata_sheet_id,
        "gsheets.credentials-path": f"${{secret:{config.credentials_key}}}",
    }


def generate_table_definitions(config: GoogleSheetsSourceConfig) -> list[dict]:
    """Generate table definition entries for each configured sheet range.

    Each entry corresponds to one row in the Trino metadata sheet:
    - tableName: logical table name
    - sheetUrl: Google Sheets URL or range reference
    - columns: list of {name, type} for schema enforcement
    """
    return [
        {
            "tableName": table.name,
            "sheetUrl": table.sheet_url,
            "columns": table.columns,
        }
        for table in config.tables
    ]
