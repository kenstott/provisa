# Copyright (c) 2026 Kenneth Stott
# Canary: 3b9e0a71-6c48-4d25-af13-5e8c2d7b9f04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Data Profiler source's settings, a table's membership, and a result table's registration
(REQ-1934), as the YAML loader validates them."""

from __future__ import annotations

import pytest

from provisa.core.models import Column, ProvisaConfig, Source, SourceType, Table
from provisa.profiler.registration import derive_result_table, validate_config
from provisa.profiler.schema import (
    PROFILE_WATERMARK_COLUMN,
    RESULT_KINDS,
    field_names,
    parse_result_table_name,
    result_table_name,
)
from provisa.profiler.source import profiler_settings


def _profiler(mapping: dict | None = None) -> Source:
    return Source(
        id="prof",
        type=SourceType.data_profiler,
        mapping=mapping or {"cron": "0 3 * * *", "low_cardinality_max": 100},
    )


def _member(name: str = "orders", profiler: str | None = "prof") -> Table:
    return Table(
        source_id="warehouse",
        domain_id="sales",
        schema="public",
        table=name,
        profiler_source_id=profiler,
        columns=[Column(name="id", data_type="bigint", visible_to=["analyst"])],
    )


def _result(name: str = "orders_7_profile_columns", columns: list[str] | None = None) -> Table:
    names = columns if columns is not None else ["run_id", "run_time", "null_share"]
    return Table(
        source_id="prof",
        domain_id="sales",
        schema="org_default",
        table=name,
        columns=[Column(name=n, data_type="varchar", visible_to=["analyst"]) for n in names],
    )


def _config(*tables: Table, source: Source | None = None) -> ProvisaConfig:
    return ProvisaConfig(
        sources=[source or _profiler(), Source(id="warehouse", type=SourceType.postgresql)],
        domains=[],
        tables=list(tables),
        roles=[],
    )


def test_settings_are_a_cron_and_an_optional_sample_size():
    assert (
        profiler_settings("p", {"cron": "0 3 * * *", "low_cardinality_max": 100}).sample_above_cells
        is None
    )
    assert (
        profiler_settings(
            "p", {"cron": "*/5 * * * *", "sample_above_cells": 1000, "low_cardinality_max": 100}
        ).sample_above_cells
        == 1000
    )


@pytest.mark.parametrize(
    "mapping,message",
    [
        ({"low_cardinality_max": 100}, "needs a cron schedule"),
        ({"cron": "every day", "low_cardinality_max": 100}, "is invalid"),
        (
            {"cron": "0 3 * * *", "sample_above_cells": 0, "low_cardinality_max": 1},
            "sample_above_cells",
        ),
        (
            {"cron": "0 3 * * *", "sample_above_cells": True, "low_cardinality_max": 1},
            "sample_above_cells",
        ),
        ({"cron": "0 3 * * *"}, "low_cardinality_max must be a positive whole number"),
        ({"cron": "0 3 * * *", "low_cardinality_max": 0}, "low_cardinality_max must be"),
        ({"cron": "0 3 * * *", "low_cardinality_max": 100, "k": 5}, "unknown setting"),
    ],
)
def test_settings_that_describe_no_schedule_are_refused(mapping, message):
    with pytest.raises(ValueError, match=message):
        profiler_settings("p", mapping)


def test_result_table_names_round_trip_and_cannot_collide():
    for kind in RESULT_KINDS:
        assert parse_result_table_name(result_table_name("orders", 7, kind)) == ("orders", 7, kind)
    # Two profiled tables of one name are told apart by id.
    assert result_table_name("orders", 7, "runs") != result_table_name("orders", 8, "runs")
    # fanout_runs is not mistaken for runs; a name with an underscore before a digit parses.
    assert parse_result_table_name("orders_2024_3_profile_fanout_runs") == (
        "orders_2024",
        3,
        "fanout_runs",
    )
    with pytest.raises(ValueError, match="not a profile result table"):
        parse_result_table_name("orders_profile_columns")


def test_a_result_table_takes_its_kinds_shipped_types_and_watermark():
    table = _result()
    derive_result_table(table)
    assert [(c.name, c.data_type) for c in table.columns] == [
        ("run_id", "varchar"),
        ("run_time", "timestamp"),
        ("null_share", "double"),
    ]
    assert table.watermark_column == PROFILE_WATERMARK_COLUMN
    assert all(c.description for c in table.columns)
    assert set(field_names("columns")) >= {c.name for c in table.columns}


def test_a_result_table_must_expose_the_run_key_and_only_its_kinds_fields():
    with pytest.raises(ValueError, match="run key"):
        derive_result_table(_result(columns=["null_share"]))
    with pytest.raises(ValueError, match="not fields of the profile columns table"):
        derive_result_table(_result(columns=["run_id", "run_time", "salary"]))


def test_the_config_registers_a_result_table_of_a_member():
    config = _config(_member(), _result())
    validate_config(config)
    assert config.tables[1].watermark_column == PROFILE_WATERMARK_COLUMN


def test_a_result_table_of_a_table_that_is_not_a_member_is_refused():
    with pytest.raises(ValueError, match="'orders' is not a member of profiler 'prof'"):
        validate_config(_config(_member(profiler=None), _result()))


def test_membership_must_name_a_profiler():
    with pytest.raises(ValueError, match="'warehouse' is not a Data Profiler source"):
        validate_config(_config(_member(profiler="warehouse")))


def test_two_members_with_one_name_both_join():
    other = _member()
    other.schema_name = "archive"
    validate_config(_config(_member(), other))


def test_a_profiler_with_invalid_settings_fails_the_config():
    with pytest.raises(ValueError, match="needs a cron schedule"):
        validate_config(_config(source=_profiler({"sample_above_cells": 10})))
