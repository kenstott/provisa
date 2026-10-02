# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8d6f12-5c49-4e7a-9a03-1f2e8c7d4b56
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Arrow Flight compresses what it sends as the operator set ``server.flight_compression``."""

from __future__ import annotations

import inspect

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from provisa.api.flight import compression
from provisa.core import settings_catalog, settings_registry


@pytest.fixture(autouse=True)
def _config(monkeypatch):
    monkeypatch.delenv("PROVISA_FLIGHT_COMPRESSION", raising=False)
    monkeypatch.setattr(settings_registry, "_config", {})


def _set(codec: str) -> None:
    settings_registry.bind_config({"server": {"flight_compression": codec}})


def _setting():
    return next(s for s in settings_catalog.DECLARED if s.key == compression.SETTING)


def test_it_is_an_operator_setting_shipped_off():
    setting = _setting()
    assert setting.type == "enum" and setting.choices == compression.CODECS
    assert setting.default == compression.OFF
    assert setting.card == "network" and setting.effect == "live"
    assert setting.env == "PROVISA_FLIGHT_COMPRESSION"
    assert settings_registry.value(compression.SETTING) == "none"


def test_off_sends_batches_as_they_are():
    assert compression.ipc_write_options() is None


@pytest.mark.parametrize("codec", ["lz4", "zstd"])
def test_a_codec_is_applied_to_the_streams_batches(codec):
    _set(codec)
    options = compression.ipc_write_options()
    assert options is not None and options.compression == codec


def test_the_environment_sets_it(monkeypatch):
    monkeypatch.setenv("PROVISA_FLIGHT_COMPRESSION", "zstd")
    assert settings_registry.value(compression.SETTING) == "zstd"


def test_a_change_applies_to_the_next_stream():
    table = pa.table({"n": list(range(10))})
    assert compression.ipc_write_options() is None
    _set("lz4")
    assert compression.ipc_write_options() is not None
    assert isinstance(compression.record_batch_stream(table), flight.RecordBatchStream)
    assert isinstance(
        compression.generator_stream(table.schema, iter(table.to_batches())),
        flight.GeneratorStream,
    )


@pytest.mark.parametrize("module", ["provisa.api.flight.server", "provisa.api.airport.server"])
def test_every_flight_response_is_built_through_the_setting(module):
    """No Flight stream is constructed directly: each would ignore the operator's codec."""
    import importlib

    src = inspect.getsource(importlib.import_module(module))
    assert "flight.RecordBatchStream(" not in src
    assert "flight.GeneratorStream(" not in src
