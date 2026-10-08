# Copyright (c) 2026 Kenneth Stott
# Canary: dfab027e-d319-4b79-b941-369830adb740
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An environment's data modes (REQ-1942): how a source new to it is reached, what a change of
mode does to its sources' bindings and its change log, and when it needs the right to read the
parent's data."""

# Requirements: REQ-1942

from __future__ import annotations

import pytest

from provisa.core.env_classes import landing_binding
from provisa.core.env_data import DataChoiceRefused, sensitive_refusal, transition


@pytest.mark.parametrize(
    ("mode", "landing"),
    [
        ("inherit", "copied"),
        ("test_fake", "copied"),
        ("test_synthetic", "copied"),
        ("unbound", "unbound"),
        (None, "unbound"),  # prod: no parent to copy from
    ],
)
def test_a_new_source_lands_as_the_mode_says(mode, landing):
    assert landing_binding(mode) == landing


def test_an_unknown_mode_is_refused():
    with pytest.raises(ValueError, match="unknown data mode"):
        landing_binding("shadow")
    with pytest.raises(DataChoiceRefused, match="unknown data mode"):
        transition("inherit", "shadow")


def test_inherit_recopies_unbound_clears_and_the_test_modes_keep_every_source():
    assert transition("unbound", "inherit").binding == "copied"
    assert transition("inherit", "unbound").binding == "unbound"
    assert transition("inherit", "test_fake").binding is None
    assert transition("unbound", "test_synthetic").binding is None


@pytest.mark.parametrize(
    ("current", "target", "discards"),
    [
        ("inherit", "test_fake", False),
        ("test_fake", "inherit", False),
        ("inherit", "unbound", False),
        ("inherit", "test_synthetic", True),
        ("test_synthetic", "test_fake", True),
        ("test_synthetic", "test_synthetic", True),  # regenerating
    ],
)
def test_a_change_of_row_keys_discards_the_change_log(current, target, discards):
    assert transition(current, target).discards_change_log is discards


def test_only_inheriting_needs_the_right_to_read_the_parent():
    assert transition("unbound", "inherit").reads_parent
    assert not transition("inherit", "test_fake").reads_parent
    assert not transition("inherit", "unbound").reads_parent


def test_the_sensitive_refusal_names_every_uncovered_column():
    message = sensitive_refusal("qa", ["customers.email", "customers.name"])
    assert "customers.email, customers.name" in message
    assert "'qa' is Test (fake)" in message
    assert "sensitive_data" in message


def test_a_source_bound_to_a_synthetic_store_keeps_the_models_type_in_its_binding():
    """REQ-1942: its ``type`` is the store's while it is bound there; what a merge, a deploy and
    a tree carry is the type the model gives it."""
    from provisa.core.env_classes import BINDINGS, SYNTHETIC, model_type

    assert SYNTHETIC in BINDINGS
    bound = {
        "type": "postgresql",
        "binding": SYNTHETIC,
        "synthetic": {"schema": "s", "tables": [], "model_type": "openapi"},
    }
    assert model_type(bound) == "openapi"
    assert model_type({"type": "openapi", "binding": "copied", "synthetic": None}) == "openapi"


def test_a_restored_source_takes_the_type_the_model_gives_it_now():
    """REQ-1942: restoring gives back the binding and connection a source had before generating;
    its type is the model's, so a type a merge changed while it was bound to the store is kept."""
    from provisa.core.env_data import restored_binding

    before = {"type": "postgresql", "binding": "copied", "host": "db", "synthetic": None}
    assert restored_binding(before, "postgresql") == before
    assert restored_binding(before, "mysql") == {**before, "type": "mysql"}
    # Bound to its parent's synthetic store before: that binding again, the model's type in it.
    inherited = {
        "type": "duckdb",
        "binding": "synthetic",
        "host": "",
        "synthetic": {"schema": "s", "tables": [], "model_type": "postgresql"},
    }
    assert restored_binding(inherited, "mysql") == {
        **inherited,
        "synthetic": {"schema": "s", "tables": [], "model_type": "mysql"},
    }


def test_test_synthetic_is_refused_by_name_where_the_engine_cannot_generate():
    """REQ-1942: said when the mode is chosen, naming the engine and the engines that can --
    not left to fail when generation is first asked for."""
    from provisa.core.env_data import refuse_synthetic_on
    from provisa.synthetic.generate import DIALECTS

    for engine in DIALECTS:
        refuse_synthetic_on(engine, "qa")
    with pytest.raises(DataChoiceRefused) as refused:
        refuse_synthetic_on("postgres", "qa")
    said = str(refused.value)
    assert "'qa' cannot be Test (synthetic)" in said and "(postgres)" in said
    assert all(engine in said for engine in DIALECTS)


def test_both_places_a_data_mode_is_chosen_ask_the_engine():
    """Creating an environment as Test (synthetic) and changing one to it are the two choices."""
    import inspect

    from provisa.api.admin import environment_data_router, environments_router

    for module in (environments_router, environment_data_router):
        assert "refuse_synthetic_on(" in inspect.getsource(module), module.__name__
