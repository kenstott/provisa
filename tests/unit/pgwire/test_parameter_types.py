# Copyright (c) 2026 Kenneth Stott
# Canary: a94e17c2-5d08-4b36-8f7e-3c61d02b9e45
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An uncast ``$N`` takes the type of what it meets, as PostgreSQL resolves it (REQ-2167). The
OIDs are what PostgreSQL 16.11's pg_prepared_statements said of each statement."""

# Requirements: REQ-2167

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from buenavista.postgres import TYPE_OIDS

from provisa.compiler.introspect import ColumnMetadata
from provisa.pgwire._pipeline import describe_pgwire_statement
from provisa.pgwire.parameters import placeholder_oids, resolved_oids
from provisa.pgwire.server import _described_oid
from tests.pgwire_describe_parity import registered_parameters
from tests.role_state import with_surfaces

_REGISTRY = {
    "m": [
        ("id", "integer"),
        ("label", "text"),
        ("name", "varchar(20)"),
        ("price", "money"),
        ("n", "numeric(10,2)"),
        ("seen", "timestamp"),
    ]
}


def _oids(sql: str) -> list[int]:
    return resolved_oids(
        placeholder_oids(sql), registered_parameters(sql, _REGISTRY), _described_oid, TYPE_OIDS
    )


@pytest.mark.parametrize(
    ("sql", "oids"),
    [
        ("SELECT id FROM m WHERE label = $1", [25]),
        ("SELECT id FROM m WHERE $1 = label", [25]),
        ("SELECT id FROM m WHERE (label) = ($1)", [25]),
        (
            "SELECT id FROM m WHERE id = $1 AND n > $2 AND label LIKE $3 AND price < $4",
            [23, 1700, 25, 790],
        ),
        ("SELECT id FROM m WHERE id IN ($1, $2)", [23, 23]),
        ("SELECT id FROM m WHERE label BETWEEN $1 AND $2", [25, 25]),
        ("SELECT id FROM m WHERE seen > $1", [1114]),
        # A varchar has no operators of its own: what meets one is text.
        ("SELECT id FROM m WHERE name = $1", [25]),
        ("SELECT id FROM m WHERE length(label) = $1", [23]),
        ("SELECT label FROM m t JOIN m u ON t.id = u.id WHERE u.label = $1", [25]),
        ("SELECT $1", [25]),
        ("SELECT $1 AS v FROM m", [25]),
        # Nothing to meet: LIMIT is PostgreSQL's bigint.
        ("SELECT id FROM m LIMIT $1 OFFSET $2", [20, 20]),
        # The statement's cast is the type, wherever the parameter is also compared.
        ("SELECT id FROM m WHERE label = $1::text", [25]),
        ("SELECT id FROM m WHERE id = $1 AND $1::text = label", [25]),
        ("INSERT INTO m (id, label) VALUES ($1, $2)", [23, 25]),
        ("UPDATE m SET label = $1 WHERE id = $2", [25, 23]),
        ("DELETE FROM m WHERE label = $1", [25]),
    ],
)
def test_an_uncast_parameter_is_the_type_it_meets(sql: str, oids: list[int]):
    assert _oids(sql) == oids


def test_a_statement_with_no_placeholder_is_not_read():
    assert registered_parameters("not sql at all", _REGISTRY) == {}


def test_a_type_bind_has_no_decoder_of_is_text():
    assert resolved_oids([20], {1: "inet"}, _described_oid, TYPE_OIDS) == [25]


@pytest.mark.asyncio
async def test_describe_derives_the_parameters_of_the_governed_statement():
    sql = "SELECT id FROM m WHERE label = $1"
    columns = {
        1: [
            ColumnMetadata(column_name="id", data_type="integer", is_nullable=True),
            ColumnMetadata(column_name="label", data_type="text", is_nullable=True),
        ]
    }
    ctx = SimpleNamespace(
        physical_to_sql={},
        virtual_columns={},
        columns_outside_relation=set(),
        tables={},
        nested_columns={},
    )
    governed: Any = SimpleNamespace(
        memo={},
        governed_semantic=sql,
        gov_ctx=SimpleNamespace(table_map={"m": 1}),
        ctx=ctx,
    )
    state = with_surfaces(MagicMock())
    state.tracked_functions = {}
    state.tracked_webhooks = {}
    state.schema_build_cache = {"column_types": columns}
    with (
        patch("provisa.api.app.state", state),
        patch("provisa.pgwire._pipeline.govern_statement", AsyncMock(return_value=governed)),
        patch("provisa.pgwire._pipeline._wake_before_governing", AsyncMock()),
    ):
        described = await describe_pgwire_statement(sql, "alice")
    assert described.parameters == {1: "TEXT"}
