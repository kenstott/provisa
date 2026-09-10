# Copyright (c) 2026 Kenneth Stott
# Canary: 2f1d90b4-77ac-4e2b-9c05-6b1e4a83d2f7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1695: a control-plane source row carries its password as a REFERENCE, and reads it back.

Before this, ``sources`` had no column for a password at all, so a source registered through the
Sources form reached its connector with an empty credential: Register Table listed no schema for
every connector that authenticates (the Splunk case that motivated the requirement). The column
holds ``${provider:name}``, never a credential, and the mapper below is the one place the column
name and the model field name are tied together.
"""

from __future__ import annotations

import pytest

from provisa.api.admin.schema_common import source_password_secret_name
from provisa.core.models import Source, SourceType
from provisa.core.repositories.source import _source_values, source_from_row
from provisa.core.schema_org import sources as sources_table
from provisa.core.secrets_store import NAME


def test_write_projection_carries_the_password_as_password_ref():
    values = _source_values(
        Source(
            id="splunk-demo",
            type=SourceType.splunk,
            host="localhost",
            port=8089,
            password="${secret:source_splunk_demo_password}",
        )
    )
    assert values["password_ref"] == "${secret:source_splunk_demo_password}"
    # The credential never has a column of its own to land in.
    assert "password" not in values


def test_password_ref_is_a_real_column_on_the_sources_table():
    assert "password_ref" in sources_table.c
    column = sources_table.c.password_ref
    assert column.nullable is False
    # A source that needs no password stores the empty string, so no reader deals with NULL.
    assert column.server_default.arg == ""


@pytest.mark.parametrize(
    "password",
    ["", "${env:PROVISA_DEMO_SPLUNK_PASSWORD}", "${secret:source_ui_splunk_password}"],
)
def test_row_round_trip_preserves_the_reference(password):
    original = Source(
        id="ui_splunk",
        type=SourceType.splunk,
        host="localhost",
        port=8089,
        username="admin",
        password=password,
    )
    # A row as the control plane hands it back: the write projection plus the columns the table
    # defaults, which the mapper ignores because they are not model fields.
    row = {**_source_values(original), "bound": True, "cache_enabled": True}
    assert source_from_row(row).password == password


def test_the_mapper_refuses_a_row_with_no_password_ref():
    """A row missing the column is a broken read, not a source without a password.

    The column is NOT NULL with an empty-string default, so every real row has it. Reading it with
    a default would turn a mis-projected SELECT into a source that silently authenticates with
    nothing -- exactly the failure REQ-1695 exists to end.
    """
    with pytest.raises(KeyError):
        source_from_row({"id": "x", "type": "splunk"})


@pytest.mark.parametrize(
    ("source_id", "expected"),
    [
        ("splunk", "source_splunk_password"),
        ("splunk-demo", "source_splunk_demo_password"),
        ("e2e_splunk_1757500000000", "source_e2e_splunk_1757500000000_password"),
        ("sales/crm.prod", "source_sales_crm_prod_password"),
        ("9lives", "source_9lives_password"),
    ],
)
def test_secret_name_is_derived_from_the_source_id(source_id, expected):
    assert source_password_secret_name(source_id) == expected


@pytest.mark.parametrize("source_id", ["splunk-demo", "sales/crm.prod", "9lives", "_x", "A.B-C"])
def test_every_derived_name_is_storable(source_id):
    """The vault's own grammar accepts it -- including a source id that starts with a digit, which
    the ``source_`` prefix is there to make legal."""
    assert NAME.match(source_password_secret_name(source_id))
