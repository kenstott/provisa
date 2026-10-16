# Copyright (c) 2026 Kenneth Stott
# Canary: ee177702-b8cf-487c-b9ac-4e0fe416dfb9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""COPY FROM STDIN reads text and csv; any other format is refused before data is asked for."""

from __future__ import annotations

import io
from types import SimpleNamespace

import pytest

from provisa.compiler.definitions import NotAvailableHere
from provisa.pgwire.copy_handler import CopyHandler


def _handler():
    transport = SimpleNamespace(rfile=io.BytesIO(), wfile=io.BytesIO())
    ctx = SimpleNamespace(session=SimpleNamespace(role_id="analyst"))
    return transport, ctx


@pytest.mark.parametrize(
    "sql",
    [
        "COPY orders FROM STDIN (FORMAT binary)",
        "COPY orders FROM STDIN WITH (FORMAT 'binary')",
        "COPY sales.orders (id, amount) FROM STDIN FORMAT parquet",
    ],
)
def test_a_format_it_does_not_read_is_refused_naming_it(sql):
    transport, ctx = _handler()
    with pytest.raises(NotAvailableHere, match="does not read FORMAT (binary|parquet)"):
        CopyHandler(transport).handle(ctx, sql)
    # refused before the CopyInResponse: the client is never asked for data
    assert transport.wfile.getvalue() == b""


@pytest.mark.parametrize(
    ("sql", "fmt"),
    [
        ("COPY orders FROM STDIN", "text"),
        ("COPY orders FROM STDIN (FORMAT text)", "text"),
        ("COPY orders FROM STDIN WITH (FORMAT csv)", "csv"),
        ("COPY orders FROM STDIN (FORMAT CSV)", "csv"),
    ],
)
def test_text_and_csv_go_on_to_the_load(monkeypatch, sql, fmt):
    transport, ctx = _handler()
    seen = {}

    def load(self, _ctx, schema, table, cols, fmt_, role_id):
        seen.update(table=table, fmt=fmt_, role=role_id)
        return 0

    monkeypatch.setattr(CopyHandler, "_handle_copy_from", load)
    CopyHandler(transport).handle(ctx, sql)
    assert seen == {"table": "orders", "fmt": fmt, "role": "analyst"}
