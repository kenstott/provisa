# Copyright (c) 2026 Kenneth Stott
# Canary: 7d0b6f0e-4f0b-4a39-9a43-2c1e3f5d9b21
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The fake catalog the table editor reads (REQ-1494): each kind and method names the column
families it can fill and whether it can be stable, and each answer is the one the checks a save
runs would give -- the list offers what the save accepts, so the two never disagree."""

# Requirements: REQ-1494

from __future__ import annotations

from typing import Any, cast

import pytest

from provisa.fakes import catalog as catalog_mod
from provisa.fakes.catalog import FAMILY_TYPES, KIND_CHOICES, METHOD_CHOICES, catalog
from provisa.fakes.checks import DeclaredColumn, check_stable, check_table, family
from provisa.fakes.kinds import RULE_ONLY, FakeRefused, Method, parse
from provisa.fakes.methods import ARGUMENTS, check_method

_CATALOG = catalog()
_KINDS = {k["name"]: k for k in _CATALOG["kinds"]}
_METHODS = {m["name"]: m for m in _CATALOG["methods"]}
_FAMILIES = tuple(FAMILY_TYPES)


def test_the_families_are_the_checks_own():
    assert set(_FAMILIES) == {
        "text",
        "integer",
        "numeric",
        "boolean",
        "date",
        "timestamp",
        "other",
    }
    for fam, data_type in FAMILY_TYPES.items():
        assert family(data_type) == fam


def _method_fits(declaration_args: tuple[tuple[str, Any], ...], name: str, fam: str) -> bool:
    try:
        check_method(Method(name, declaration_args), fam)
    except FakeRefused:
        return False
    return True


@pytest.mark.parametrize("name", sorted(_METHODS))
def test_a_method_lists_the_families_check_method_accepts(name):
    """Its default call, or any argument choice for a method whose value is an argument given."""
    choices = [()] + [parse(d).args for d in METHOD_CHOICES.get(name, ())]  # type: ignore[union-attr]
    expected = [f for f in _FAMILIES if any(_method_fits(a, name, f) for a in choices)]
    assert _METHODS[name]["families"] == expected


def _kind_fits(declaration: str, fam: str, rule: bool) -> bool:
    """The save's own check: the column of ``fam`` declares it, beside a column of every family
    for a relative kind to name."""
    others = [DeclaredColumn(f"c_{f}", t) for f, t in FAMILY_TYPES.items()]
    column = (
        DeclaredColumn("x", FAMILY_TYPES[fam], rule=declaration)
        if rule
        else DeclaredColumn("x", FAMILY_TYPES[fam], fake=declaration)
    )
    try:
        check_table("t", [column, *others], {})
    except FakeRefused:
        return False
    return True


def test_every_kind_has_argument_choices():
    assert set(KIND_CHOICES) == set(_KINDS)
    for name, choices in KIND_CHOICES.items():
        assert choices and all(d.startswith(f"{name}(") for d in choices), name


@pytest.mark.parametrize("name", sorted(_KINDS))
def test_a_kind_lists_the_families_the_save_accepts(name):
    rule = name in RULE_ONLY
    expected = [f for f in _FAMILIES if any(_kind_fits(d, f, rule) for d in KIND_CHOICES[name])]
    assert _KINDS[name]["families"] == expected
    assert expected, f"{name} fills no family"


def test_the_families_answer_the_maintainers_report():
    """An integer column is offered no text-only method, a text one no number-only method."""
    assert "integer" not in _METHODS["swift"]["families"]
    assert "integer" in _METHODS["random_digit"]["families"]
    assert _METHODS["email"]["families"] == ["text"]
    assert "date" in _METHODS["date_object"]["families"]
    assert "integer" in _KINDS["poisson"]["families"]
    assert "text" not in _KINDS["poisson"]["families"]


def test_only_random_element_takes_its_type_from_its_arguments():
    """Every other portable argument sets a range, format, length or count: no value of it makes
    the method fill another family."""
    values: list[Any] = [0, 1, 5, 2.5, True, "x", "%Y", "-30d", (1, 2), (1.5, 2.5), (True, False)]
    widened = set()
    for name, args in ARGUMENTS.items():
        base = {f for f in _FAMILIES if _method_fits((), name, f)}
        for a in args:
            for v in values:
                if {f for f in _FAMILIES if _method_fits(((a, v),), name, f)} - base:
                    widened.add(name)
    assert widened == set(METHOD_CHOICES) == {"random_element"}


def _stable(declaration: str) -> bool:
    try:
        check_stable(parse(declaration, rule=True))
    except FakeRefused:
        return False
    return True


@pytest.mark.parametrize("name", sorted(_METHODS))
def test_a_method_is_stable_exactly_when_the_save_accepts_it_stable(name):
    assert _METHODS[name]["stable"] == _stable(f"{name}()")
    assert _METHODS[name]["stableNeeds"] == []


def test_a_stable_method_takes_no_arguments():
    assert not _stable("sentence(nb_words=3)")


def _given(declaration: str, name: str) -> set[str]:
    kind = parse(declaration, rule=True)
    return {a["name"] for a in _KINDS[name]["args"] if getattr(kind, a["name"], None) is not None}


@pytest.mark.parametrize("name", sorted(_KINDS))
def test_a_kind_is_stable_exactly_when_the_save_accepts_it_stable(name):
    """Stable when the kind can be and every argument it needs for it is given."""
    entry = _KINDS[name]
    for d in KIND_CHOICES[name]:
        exposed = entry["stable"] and set(entry["stableNeeds"]) <= _given(d, name)
        assert exposed == _stable(d), d


def test_stability_answers_the_maintainers_report():
    assert _METHODS["random_digit"]["stable"] is False
    assert _METHODS["email"]["stable"] is True
    assert _KINDS["pattern"]["stable"] is False
    assert _KINDS["categories"]["stableNeeds"] == ["values"]
    assert _KINDS["profile"]["stableNeeds"] == ["run"]
    assert _KINDS["after"]["stableNeeds"] == ["distance"]
    assert _KINDS["uniform"]["stable"] is True and _KINDS["uniform"]["stableNeeds"] == []


def test_the_stable_refusal_reads_as_a_sentence():
    with pytest.raises(FakeRefused) as refused:
        check_stable(parse("random_digit()"))
    text = str(refused.value)
    assert "--" not in text
    assert text.startswith("random_digit() cannot be stable: ")
    assert "email" in text


@pytest.mark.asyncio
async def test_the_catalog_route_names_each_asked_type_s_family(monkeypatch):
    from provisa.api.admin import fakes_router

    monkeypatch.setattr(
        "provisa.api.admin._hiding_guard.require_hiding_editor_request", lambda request: None
    )
    body = await fakes_router.fake_catalog(
        cast(Any, None), dataType=["integer", "varchar(20)", "date", "jsonb"]
    )
    assert body["typeFamilies"] == {
        "integer": "integer",
        "varchar(20)": "text",
        "date": "date",
        "jsonb": "other",
    }
    assert body["kinds"] == catalog_mod.catalog()["kinds"]
