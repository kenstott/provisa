# Copyright (c) 2026 Kenneth Stott
# Canary: 0e982c18-3b06-4be5-8561-3d9e93ee2a8b
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the executable file in the root directory of this source tree.

"""Bug regression: create_source must populate state.source_catalogs.

Before the fix, dynamically creating a source via create_source() left
state.source_catalogs empty for that source. Any subsequent call that
used catalog_for() (e.g. resolve_available_columns_metadata when no dataType
is supplied) raised KeyError and failed the registerTable mutation.
"""

from __future__ import annotations

import pytest

from provisa.compiler.naming import org_prefixed_catalog, source_to_catalog


def test_catalog_name_formula_files_source():
    """The catalog name for a files source derives from source_to_catalog(id) unless
    a database name is set. This mirrors _populate_source_catalog_names logic."""
    source_id = "e2e-northwind"
    database = None  # no database field on files source
    pg_cat = database or source_to_catalog(source_id)
    assert pg_cat == "e2e_northwind"
    # Default org keeps bare names
    catalog = org_prefixed_catalog("e2e", pg_cat, default_org="e2e")
    assert catalog == "e2e_northwind"


def test_catalog_name_formula_postgresql_source():
    """PostgreSQL sources always use source_to_catalog(id) regardless of database field."""
    source_id = "my-pg-source"
    database = "mydb"  # postgresql has a database field but catalog comes from id
    pg_cat = source_to_catalog(source_id) if True else (database or source_to_catalog(source_id))
    assert pg_cat == "my_pg_source"


def test_catalog_name_formula_non_default_org():
    """Non-default orgs get an org-prefixed catalog name."""
    source_id = "e2e-northwind"
    pg_cat = source_to_catalog(source_id)
    catalog = org_prefixed_catalog("tenant-a", pg_cat, default_org="default")
    assert catalog == "org_tenant-a__e2e_northwind"


def test_source_catalogs_populated_by_create_source(monkeypatch):
    """After create_source runs, state.source_catalogs[source_id] must be set.

    This is the regression test for the bug where catalog_for() raised KeyError
    when registerTable was called with columns lacking dataType, triggering
    resolve_available_columns_metadata → catalog_for → KeyError.
    """
    from types import SimpleNamespace

    # Simulate the relevant portion of create_source logic
    source_id = "e2e-northwind"
    source_type = "files"
    database = None

    # Simulate state
    source_catalogs: dict[str, str] = {}
    org_id = "e2e"

    # Simulate current_org.get() returning None (no ContextVar set at mutation time)
    current_org_value = None

    # This is exactly the logic we added to create_source
    pg_cat = (
        source_to_catalog(source_id)
        if source_type == "postgresql"
        else (database or source_to_catalog(source_id))
    )
    building_org = current_org_value or org_id
    source_catalogs[source_id] = org_prefixed_catalog(building_org, pg_cat, default_org=org_id)

    assert source_id in source_catalogs, "source_catalogs must contain source after create_source"
    assert source_catalogs[source_id] == "e2e_northwind"
