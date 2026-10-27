# Copyright (c) 2026 Kenneth Stott
# Canary: 4104132d-73df-41d9-9f98-9e8795c1e8f6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every operator setting the settings page lists is named and explained in English (REQ-1913).

The page (``SettingField``) reads ``settings.<key>.label`` and ``.help`` from the English catalog,
and ``.keys.<k>`` / ``.columns.<c>`` for a map's keys and a list's columns. A setting with none
there is shown under its raw dotted key."""

# Requirements: REQ-1913

from __future__ import annotations

import json
from pathlib import Path

import pytest

from provisa.core import settings_catalog

_EN = Path(__file__).resolve().parents[2] / "provisa-ui/src/i18n/locales/en/settings.json"


def _string(catalog: dict, path: str) -> str | None:
    node: object = catalog
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node if isinstance(node, str) and node.strip() else None


@pytest.mark.parametrize("setting", settings_catalog.DECLARED, ids=lambda s: s.key)
def test_each_setting_has_an_english_label_and_help(setting):
    english = json.loads(_EN.read_text())["settings"]
    wanted = [
        "label",
        "help",
        *(f"keys.{k}" for k in setting.map_keys or ()),
        *(f"columns.{c}" for c in setting.columns or ()),
    ]
    missing = [w for w in wanted if _string(english, f"{setting.key}.{w}") is None]
    assert not missing, f"settings.json has no {missing} for {setting.key}"


def _labelled(node: dict, path: list[str]):
    if isinstance(node.get("label"), str):
        yield ".".join(path)
    for key, child in node.items():
        if isinstance(child, dict) and key not in ("keys", "columns"):
            yield from _labelled(child, [*path, key])


def test_every_english_setting_entry_is_a_catalog_setting():
    """An entry no catalog setting reads is dead text the page never shows."""
    english = json.loads(_EN.read_text())["settings"]
    declared = {s.key for s in settings_catalog.DECLARED}
    # The read-only reasons are the page's own sentences, keyed by reason, not by setting.
    entries = {e for k, v in english.items() if k != "readonlyReasons" for e in _labelled(v, [k])}
    assert sorted(entries - declared) == []
