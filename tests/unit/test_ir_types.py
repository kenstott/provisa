# Copyright (c) 2026 Kenneth Stott
# Canary: 9ef58813-662f-448d-81ad-b2fc4154185e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-846/932: the canonical IR type vocabulary — native → IR normalization, no silent default."""

from __future__ import annotations

import pytest

from provisa.core.ir_types import (
    IR_TYPES,
    is_ir_type,
    to_ir,
    to_physical,
    to_sqlalchemy,
    value_transform,
)


def test_platform_translators_native_to_ir():
    # SQL Server platform spellings
    assert to_ir("bit", "sqlserver") == "boolean"
    assert to_ir("nvarchar", "sqlserver") == "text"
    assert to_ir("uniqueidentifier", "sqlserver") == "uuid"
    assert to_ir("datetime2", "sqlserver") == "timestamp"
    # Trino
    assert to_ir("varbinary", "trino") == "bytea"
    assert to_ir("row", "trino") == "json"  # REQ-1965
    # DuckDB
    assert to_ir("hugeint", "duckdb") == "bigint"
    assert to_ir("blob", "duckdb") == "bytea"
    # common types fall through to generic aliases regardless of platform
    assert to_ir("integer", "trino") == "integer"
    assert to_ir("varchar", "duckdb") == "text"
    # a platform-specific spelling is unknown without the platform
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        to_ir("uniqueidentifier")


def test_value_transform_passthrough_default_and_sql_expression():
    # passthrough is the default (None = project as-is) — the common case
    assert value_transform("integer", "sqlserver") is None
    assert value_transform("varchar", "trino") is None
    assert value_transform("integer") is None  # no platform → always passthrough
    # the handful of complex-type reshapes return a SQL-EXPRESSION template ({col} placeholder),
    # so the transform runs set-based in the land/generation query — never per-cell in Python
    assert value_transform("bit", "sqlserver") == "{col} <> 0"
    # REQ-2050: an instant keeps its zone; it is not reshaped to a UTC wall-clock time.
    assert value_transform("timestamp with time zone", "trino") is None


def test_aliases_canonicalize_to_one_ir_name():
    assert to_ir("varchar") == "text"
    assert to_ir("character varying") == "text"
    assert to_ir("string") == "text"
    assert to_ir("int4") == "integer"
    assert to_ir("int8") == "bigint"
    assert to_ir("bool") == "boolean"
    assert to_ir("json") == "json"  # REQ-980: first-class JSON
    assert to_ir("jsonb") == "json"  # jsonb narrows to portable generic JSON
    assert to_ir("timestamptz") == "timestamptz"  # REQ-2050: an instant


def test_a_registered_nvarchar_column_lands_as_text_with_no_platform_named():
    """A SAP HANA (or SQL Server) source's column is registered as NVARCHAR; a landing reads the
    stored type back with no platform, and refused the swap lane's replicas on it."""
    assert to_ir("NVARCHAR(256)") == "text"


def test_strips_qualifier_and_lowercases():
    assert to_ir("VARCHAR(255)") == "text"
    assert to_ir("Numeric(10, 2)") == "numeric"


@pytest.mark.parametrize(
    "exasol",
    [
        "VARCHAR(64) UTF8",
        "VARCHAR(2000000) ASCII",
        "CHAR(1) ASCII",
        "CHAR(10) UTF8",
        "varchar(64) utf8",
    ],
)
@pytest.mark.parametrize("platform", [None, "duckdb", "exasol"])
def test_a_character_type_s_character_set_names_its_base_type(exasol, platform):
    """Exasol prints a character type's character set after it. A replica of an Exasol table landed
    in DuckDB was refused "unknown SQL type 'varchar(64) utf8' for platform 'duckdb'"."""
    assert to_ir(exasol, platform) == "text"


def test_a_character_set_after_a_type_that_is_not_character_is_still_refused():
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        to_ir("integer utf8")


def test_unknown_type_raises_no_varchar_default():
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        to_ir("hierarchyid")


def test_to_sqlalchemy_maps_ir_and_native():
    assert to_sqlalchemy("bigint").__name__ == "BigInteger"
    assert to_sqlalchemy("varchar").__name__ == "Text"  # native spelling normalized first
    # double is DOUBLE PRECISION, distinct from single-precision float (no silent precision loss)
    assert to_sqlalchemy("double precision").__name__ == "Double"
    assert to_sqlalchemy("float").__name__ == "Float"


def test_is_ir_type():
    assert is_ir_type("varchar") and is_ir_type("timestamp with time zone")
    assert not is_ir_type("hierarchyid")


def test_to_physical_renders_per_dialect():
    # IR → a platform's physical type, via SQLAlchemy's dialect compiler (no hand-maintained table)
    assert to_physical("text", "postgresql") == "TEXT"
    assert to_physical("bigint", "postgresql") == "BIGINT"
    assert to_physical("timestamp", "mysql") == "DATETIME"  # mysql renders DateTime as DATETIME
    assert to_physical("varchar", "postgresql") == "TEXT"  # native spelling normalized to IR first
    # REQ-980: first-class JSON renders natively per store dialect
    assert to_physical("json", "postgresql") == "JSONB"  # REQ-1965: the indexable JSON type
    assert to_physical("jsonb", "postgresql") == "JSONB"
    assert to_physical("json", "sqlite") == "JSON"  # SQLAlchemy JSON on sqlite (TEXT-affinity)


def test_ir_types_are_canonical_names():
    assert "text" in IR_TYPES and "bigint" in IR_TYPES
    assert "varchar" not in IR_TYPES  # varchar is an alias, not a canonical IR name


# REQ-1965: a native array, struct or map is a JSON column. Every spelling a supported source or
# engine reports for one resolves to IR ``json`` -- never ``text``, and never an unknown type.
_NESTED_SPELLINGS = [
    ("VARCHAR[]", "duckdb"),
    ("INTEGER[][]", "duckdb"),
    ("INTEGER[3]", "duckdb"),
    ("STRUCT(a INTEGER, b VARCHAR)", "duckdb"),
    ("MAP(VARCHAR, INTEGER)", "duckdb"),
    ("ARRAY", "postgresql"),
    ("_int4", "postgresql"),
    ("text[]", "postgresql"),
    ("numeric(18,2)[]", "postgresql"),
    ("ARRAY", "cockroachdb"),
    ("super", "redshift"),
    ("array(varchar)", "trino"),
    ("array(row(a integer, b varchar))", "trino"),
    ("row(a integer)", "trino"),
    ("map(varchar, integer)", "trino"),
    ("Array(String)", "clickhouse"),
    ("Array(Array(UInt8))", "clickhouse"),
    ("Tuple(String, UInt8)", "clickhouse"),
    ("Map(String, UInt8)", "clickhouse"),
    ("Nested(a String, b UInt8)", "clickhouse"),
    ("ARRAY<STRING>", "bigquery"),
    ("STRUCT<a INT64, b ARRAY<STRING>>", "bigquery"),
    ("array<string>", "databricks"),
    ("struct<a:int,b:string>", "databricks"),
    ("map<string,int>", "databricks"),
    ("array<string>", "hiveserver2"),
    ("ARRAY", "snowflake"),
    ("OBJECT", "snowflake"),
    ("VARIANT", "snowflake"),
]


@pytest.mark.parametrize(("native", "platform"), _NESTED_SPELLINGS)
def test_a_native_array_struct_or_map_is_json(native, platform):
    assert to_ir(native, platform) == "json"
    assert is_ir_type(native, platform)


@pytest.mark.parametrize(("native", "platform"), _NESTED_SPELLINGS)
def test_a_native_array_struct_or_map_is_json_without_a_platform(native, platform):
    # The later readers of a stored type (landing, Arrow, the stores) resolve it with no platform.
    del platform
    assert to_ir(native) == "json"


def test_a_scalar_whose_name_starts_like_a_nested_type_is_not_json():
    with pytest.raises(ValueError, match="not in the IR vocabulary"):
        to_ir("arraylike")
    assert to_ir("text") == "text"
    assert to_ir("varchar(3)") == "text"


@pytest.mark.parametrize(
    ("native", "platform"),
    [
        ("timestamp with time zone", None),
        ("timestamptz", "postgresql"),
        ("TIMESTAMP(6) WITH TIME ZONE", "oracle"),
        ("TIMESTAMP(6) WITH LOCAL TIME ZONE", "oracle"),
        ("datetimeoffset", "sqlserver"),
        ("TIMESTAMP_TZ", "snowflake"),
        ("TIMESTAMP_LTZ", "snowflake"),
        ("timestamp(3) with time zone", "trino"),
        ("TIMESTAMP WITH TIME ZONE", "duckdb"),
        ("TIMESTAMP", "bigquery"),
        ("timestamp", "mysql"),
    ],
)
def test_every_zoned_spelling_is_an_instant(native, platform):
    """REQ-2050: a zoned source timestamp is the IR instant, timestamptz, read back with or
    without its platform named."""
    from provisa.core.ir_types import column_typing

    stored = column_typing(native, platform).data_type
    assert to_ir(stored, platform) == "timestamptz"
    assert to_ir(stored) in ("timestamptz", "timestamp")  # a bare "timestamp" needs its platform


def test_a_civil_timestamp_stays_a_timestamp():
    assert to_ir("timestamp") == "timestamp"
    assert to_ir("datetime2", "sqlserver") == "timestamp"
    assert to_ir("TIMESTAMP_NTZ", "snowflake") == "timestamp"
    assert to_ir("datetime", "mysql") == "timestamp"
