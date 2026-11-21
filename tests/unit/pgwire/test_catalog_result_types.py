# Copyright (c) 2026 Kenneth Stott
# Canary: 2aa3120f-82a1-4393-bc6a-5eaa102bdc2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A catalog statement's result types where it reads a pg_catalog table function (REQ-2167): the
function's columns are its OUT parameters, so a correlated subquery over it resolves and the
statement's other columns keep the types PostgreSQL 17 declares."""

from __future__ import annotations

import pytest

from provisa.pgwire.catalog_types import result_types

pytestmark = pytest.mark.unit

#: psql's \d of a table: its FDW options, read from pg_options_to_table(c.reloptions).
_PSQL_FDW_OPTIONS = (
    "SELECT c.relname, pg_catalog.array_to_string(ARRAY(SELECT "
    "pg_catalog.quote_ident(option_name) || ' ' || pg_catalog.quote_literal(option_value) "
    "FROM pg_catalog.pg_options_to_table(c.reloptions)), ', ') FROM pg_catalog.pg_class c"
)


def test_psqls_fdw_options_keep_the_name_and_text_types():
    assert result_types(_PSQL_FDW_OPTIONS) == (("relname", "name"), ("array_to_string", "text"))


def test_a_table_functions_columns_are_its_out_parameters():
    named = result_types("SELECT * FROM pg_catalog.pg_options_to_table(ARRAY['a=b'])")
    assert [name for name, _ in named] == ["option_name", "option_value"]


def test_a_function_of_another_schema_is_not_named_as_pg_catalogs():
    named = result_types("SELECT * FROM other.pg_options_to_table(ARRAY['a=b'])")
    assert [name for name, _ in named] != ["option_name", "option_value"]
