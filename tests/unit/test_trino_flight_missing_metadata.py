# Copyright (c) 2026 Kenneth Stott
# Canary: d452923a-808e-4016-aed7-e979dd6042b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""column_types raises a named error when a field has no trino.type metadata.

The Zaychik proxy adds trino.type, trino.type_signature and trino.encoding to every
Arrow field. An older proxy version may not: without the metadata, column_types
raised TypeError "'NoneType' object is not subscriptable" with no hint of the cause.
The fix reports which column is missing and that the proxy is older than the server.

REQ-2194."""

# Requirements: REQ-2194

from __future__ import annotations

import pyarrow as pa
import pytest

import provisa.executor.trino_flight as trf

pytestmark = pytest.mark.unit


def _field_with_metadata(name: str) -> pa.Field:
    return pa.field(
        name,
        pa.int32(),
        metadata={
            "trino.type": "integer",
            "trino.type_signature": '{"rawType":"integer","arguments":[]}',
            "trino.encoding": "arrow",
        },
    )


def _field_no_metadata(name: str) -> pa.Field:
    return pa.field(name, pa.int32())  # metadata=None


class TestColumnTypesMissingMetadata:
    def test_field_without_metadata_raises_named_error(self):
        """TypeError 'NoneType' object is not subscriptable must never surface; a named
        EngineContractError names the column and the cause."""
        schema = pa.schema([_field_no_metadata("customer_id")])
        with pytest.raises(trf.EngineContractError) as raised:
            trf.column_types(schema)
        assert "customer_id" in str(raised.value)
        assert "Zaychik" in str(raised.value) or "proxy" in str(raised.value)

    def test_field_without_type_key_raises_named_error(self):
        """A field with metadata but missing the trino.type key also raises."""
        schema = pa.schema(
            [
                pa.field(
                    "amount",
                    pa.float64(),
                    metadata={"trino.encoding": "arrow"},  # trino.type absent
                )
            ]
        )
        with pytest.raises(trf.EngineContractError) as raised:
            trf.column_types(schema)
        assert "amount" in str(raised.value)

    def test_no_metadata_is_not_a_type_error(self):
        """The original bug was a bare TypeError; confirm it is no longer raised."""
        schema = pa.schema([_field_no_metadata("x")])
        with pytest.raises(trf.EngineContractError):
            trf.column_types(schema)

    def test_complete_metadata_still_works(self):
        """Regression: a field with correct metadata must still decode normally."""
        schema = pa.schema([_field_with_metadata("n")])
        assert trf.column_types(schema) == ["integer"]

    def test_mixed_schema_reports_the_first_bad_column(self):
        """The error names the first field missing the metadata, not the last."""
        schema = pa.schema(
            [
                _field_with_metadata("good_col"),
                _field_no_metadata("bad_col"),
                _field_no_metadata("also_bad"),
            ]
        )
        with pytest.raises(trf.EngineContractError) as raised:
            trf.column_types(schema)
        assert "bad_col" in str(raised.value)
