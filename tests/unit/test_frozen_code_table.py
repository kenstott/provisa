# Copyright (c) 2026 Kenneth Stott
# Canary: cc6ed342-6ceb-4c72-8cb0-ddc34961a640
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A code table kept for good is read from its replica (REQ-2031).

A code table whose cache_ttl never expires and whose replica has landed -- a demo's declared rows
(REQ-2089) -- is read only from those rows: the materialize stage does not run it, so no worker
starts and no code is called, and its parameters are not required. A condition on a parameter
filters the landed column of the same name; a parameter with no such column is refused by name.
A code table that is not kept is run as before."""

# Requirements: REQ-2031
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from provisa.federation.replica_address import ReplicaRoutes

pytestmark = pytest.mark.unit

ALIAS = 9
_KEY = ("country_alias", "default", "country_alias")


def _alias_table() -> dict:
    return {
        "id": ALIAS,
        "source_id": "country_alias",
        "schema_name": "default",
        "table_name": "country_alias",
        "cache_ttl": -1,
        "columns": [
            {"column_name": "name", "data_type": "varchar", "native_filter_type": None},
            {"column_name": "fifa_code", "data_type": "varchar", "native_filter_type": None},
            {
                "column_name": "_nf_name",
                "data_type": "varchar",
                "native_filter_type": "query_arg",
                "native_filter_required": True,
                "required_by_steward": False,
            },
            {
                "column_name": "_nf_language",
                "data_type": "varchar",
                "native_filter_type": "query_arg",
                "native_filter_required": False,
                "required_by_steward": False,
            },
        ],
    }


def _state(kept: frozenset) -> SimpleNamespace:
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={"country_alias": "python"},
        tables=[_alias_table()],
        relationships=[],
        replica_routes=ReplicaRoutes(kept_for_good=kept),
        source_cache={},
        response_cache_default_ttl=300,
    )


_READ = 'SELECT "name", "fifa_code" FROM "default"."country_alias"'


async def _materialize(state: SimpleNamespace, sql: str) -> list:
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    called: list = []

    async def read_each(state, table, argument_sets):
        called.append(argument_sets)
        raise AssertionError("a frozen code table's code was run")

    with (
        patch("provisa.codesource.table.read_each", side_effect=read_each),
        patch("provisa.api_source.engine_cache.cache_location"),
    ):
        await _materialize_api_to_engine_cache(sql, state, table_ids=[ALIAS], role_id="analyst")
    return called


async def test_a_kept_code_table_is_not_run_and_needs_no_parameter():
    assert await _materialize(_state(frozenset({_KEY})), _READ) == []


async def test_a_code_table_that_is_not_kept_is_still_refused_for_its_parameter():
    from provisa.api.data.materialization import _materialize_api_to_engine_cache

    with pytest.raises(ValueError, match=r"requires the parameter\(s\) \['name'\]"):
        await _materialize_api_to_engine_cache(
            _READ, _state(frozenset()), table_ids=[ALIAS], role_id="analyst"
        )


def test_a_kept_code_table_is_no_table_the_pipeline_reads_itself():
    from provisa.api.data.materialization import _pipeline_tables

    assert _pipeline_tables(_state(frozenset({_KEY})), [ALIAS]) == {}
    assert set(_pipeline_tables(_state(frozenset()), [ALIAS])) == {"country_alias"}


def test_each_parameter_filters_the_landed_column_of_its_name():
    from provisa.api.data.materialization import frozen_parameter_filters

    assert frozen_parameter_filters(_state(frozenset({_KEY})), [ALIAS]) == {
        "_nf_name": "name",
        "_nf_language": None,
    }
    assert frozen_parameter_filters(_state(frozenset()), [ALIAS]) == {}


def test_a_condition_on_a_frozen_parameter_filters_its_landed_rows():
    from provisa.compiler.nf_extractor import extract_nf_args

    sql, params, args = extract_nf_args(
        _READ + " WHERE \"_nf_name\" = 'West Germany'",
        [],
        {"_nf_name", "_nf_language"},
        {"_nf_name": "name", "_nf_language": None},
    )
    assert args == {} and params == []
    assert sql == (
        'SELECT "name", "fifa_code" FROM "default"."country_alias" WHERE "name" = \'West Germany\''
    )


def test_a_frozen_parameter_with_no_landed_column_is_refused_by_name():
    from provisa.compiler.nf_extractor import extract_nf_args

    with pytest.raises(ValueError, match="_nf_language.*'language'"):
        extract_nf_args(
            _READ + " WHERE \"_nf_language\" = 'de'",
            [],
            {"_nf_name", "_nf_language"},
            {"_nf_name": "name", "_nf_language": None},
        )
