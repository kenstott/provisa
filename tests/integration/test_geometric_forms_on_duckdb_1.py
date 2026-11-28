# Copyright (c) 2026 Kenneth Stott
# Canary: 7a4b4485-8f32-4c0f-b244-cd135b0a970c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The geometric forms on DuckDB, slice 1 of 4 of the functions, against the PostgreSQL 17 recording (REQ-2171)."""

# Requirements: REQ-2167, REQ-2171

from __future__ import annotations

import pytest

from tests.integration.geometric_engines import duckdb_form_slice, duckdb_forms

pytestmark = [pytest.mark.integration, pytest.mark.isolated]


@pytest.mark.parametrize("function", duckdb_form_slice(0))
def test_geometric_forms_on_duckdb(function: str):
    duckdb_forms(function)
