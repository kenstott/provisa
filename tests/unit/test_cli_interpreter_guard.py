# Copyright (c) 2026 Kenneth Stott
# Canary: aa9df331-1c4e-447f-af01-d08aa12e6c4b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""`provisa run` aborts with the fix where pgserver has no wheel (interpreter or platform)."""

from __future__ import annotations

import pytest

from provisa.cli import _require_supported_interpreter


def test_supported_interpreter_passes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.version_info", (3, 12, 4, "final", 0))
    _require_supported_interpreter()


def test_unsupported_interpreter_aborts_with_the_fix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.version_info", (3, 13, 0, "final", 0))
    with pytest.raises(SystemExit) as exc:
        _require_supported_interpreter()
    message = str(exc.value)
    assert "requires Python 3.12" in message
    assert "this interpreter is 3.13" in message
    assert "pgserver" in message
    assert "python3.12 -m venv" in message


def test_missing_pgserver_aborts_with_the_platform_explanation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.version_info", (3, 12, 4, "final", 0))
    monkeypatch.setattr("provisa.cli.importlib.util.find_spec", lambda name: None)
    with pytest.raises(SystemExit) as exc:
        _require_supported_interpreter()
    message = str(exc.value)
    assert "pgserver" in message
    assert "linux/aarch64" in message
    assert "container tier" in message
