# Copyright (c) 2026 Kenneth Stott
# Canary: 5e1d7c3a-92b4-4f60-8a1e-c4d2b7f90e35
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1921, A DRAFT IS SERVED TO ORG_ADMIN: a draft is read as a served table is by a role that
holds org_admin. A table's Associations read the table's own row of the model table
registered_tables (its class term) through the data surface as the acting role; right after a
registration that row is a draft's."""

# Requirements: REQ-1921

from __future__ import annotations

import pytest

from provisa.api._meta_views import _META_TABLE_VIEWS
from provisa.core import schema_org

_SCHEMA = "org_default"


@pytest.fixture(scope="module")
def plane(embedded_postgres, control_plane_template):
    from tests.pg_control_plane import clone

    held = clone(embedded_postgres, control_plane_template)
    engine = held.engine(f"{held.url}?options=-csearch_path%3D{_SCHEMA},public")
    t = schema_org
    with engine.begin() as conn:
        conn.execute(t.sources.insert().values(id="src", type="postgresql"))
        conn.execute(t.domains.insert().values(id="pet_store", description="Pets"))
        for table_id, name, draft in ((1, "served", False), (2, "drafted", True)):
            conn.execute(
                t.registered_tables.insert().values(
                    id=table_id,
                    source_id="src",
                    domain_id="pet_store",
                    schema_name="public",
                    table_name=name,
                    draft=draft,
                )
            )
        conn.exec_driver_sql(_META_TABLE_VIEWS["registered_tables"].replace("%", "%%"))
    yield engine
    held.close()


def test_the_model_table_holds_a_drafts_row(plane):
    """The model table a draft's class term is read from holds the draft's row: which roles read
    it is the statement's governance, not the view's."""
    with plane.begin() as conn:
        ids = [r[0] for r in conn.exec_driver_sql("SELECT id FROM registered_tables_meta")]
    assert sorted(ids) == [1, 2]
