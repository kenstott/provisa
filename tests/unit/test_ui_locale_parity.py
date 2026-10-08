# Copyright (c) 2026 Kenneth Stott
# Canary: f262206a-941c-4828-b7ff-f0a9c4058a9f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every UI locale carries the keys English carries, with the placeholders English has.

A translation that drops ``{{source}}`` shows a sentence with a hole in it; one that names a
placeholder English does not have shows the braces. Two Japanese server-error strings carried a
placeholder twice where English has it once and nothing in CI said so: the UI's own tests do not
run in the public workflows, and this suite does."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_LOCALES = Path(__file__).resolve().parents[2] / "provisa-ui" / "src" / "i18n" / "locales"
_PLACEHOLDER = re.compile(r"\{\{\s*[\w.]+\s*\}\}")


def _flat(node, prefix: str = "") -> dict[str, str]:
    if isinstance(node, dict):
        out: dict[str, str] = {}
        for key, value in node.items():
            out.update(_flat(value, f"{prefix}.{key}" if prefix else key))
        return out
    return {prefix: node}


def _strings(locale: str) -> dict[str, str]:
    """Every string of ``locale``, by namespace file and key."""
    out: dict[str, str] = {}
    for path in sorted((_LOCALES / locale).glob("*.json")):
        if path.name.startswith("."):
            continue  # the translation-memory sidecar
        for key, value in _flat(json.loads(path.read_text(encoding="utf-8"))).items():
            out[f"{path.name}::{key}"] = value
    return out


def _placeholders(text) -> list[str]:
    if not isinstance(text, str):
        return []
    return sorted(re.sub(r"\s", "", found) for found in _PLACEHOLDER.findall(text))


_ENGLISH = _strings("en")
_OTHERS = sorted(p.name for p in _LOCALES.iterdir() if p.is_dir() and p.name != "en")


def test_there_are_locales_to_check():
    assert len(_OTHERS) == 12 and len(_ENGLISH) > 1000


@pytest.mark.parametrize("locale", _OTHERS)
def test_a_locale_has_the_keys_english_has(locale):
    theirs = _strings(locale)
    assert sorted(set(_ENGLISH) - set(theirs)) == [], f"{locale} is missing keys"
    assert sorted(set(theirs) - set(_ENGLISH)) == [], f"{locale} has keys English does not"


@pytest.mark.parametrize("locale", _OTHERS)
def test_a_locales_strings_carry_the_placeholders_english_carries(locale):
    theirs = _strings(locale)
    drifted = {
        key: (_placeholders(english), _placeholders(theirs[key]))
        for key, english in _ENGLISH.items()
        if key in theirs and _placeholders(english) != _placeholders(theirs[key])
    }
    assert drifted == {}
