# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-982: parse-time capability-gate for a table's probe_type (config_loader._validate_probe_type)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.core.config_loader import _validate_probe_type


def _cfg(*, source_type, change_signal="probe", probe_type=None, watermark_column=None):
    source = SimpleNamespace(id="s1", type=source_type, change_signal=None, cdc=None)
    table = SimpleNamespace(
        source_id="s1",
        table_name="t",
        change_signal=change_signal,
        probe_type=probe_type,
        watermark_column=watermark_column,
    )
    return SimpleNamespace(sources=[source], tables=[table])


def test_valid_probe_type_passes():
    _validate_probe_type(
        _cfg(source_type="postgresql", probe_type="watermark", watermark_column="u")
    )


def test_unset_probe_type_passes():
    _validate_probe_type(_cfg(source_type="csv", probe_type=None))


def test_file_source_rejects_watermark():
    with pytest.raises(ValueError, match="not supported by source type"):
        _validate_probe_type(_cfg(source_type="csv", probe_type="watermark"))


def test_ttl_rejects_explicit_probe_type():
    with pytest.raises(ValueError, match="cadence-only"):
        _validate_probe_type(_cfg(source_type="postgresql", change_signal="ttl", probe_type="hash"))


def test_file_source_accepts_hash():
    _validate_probe_type(_cfg(source_type="csv", probe_type="hash"))
