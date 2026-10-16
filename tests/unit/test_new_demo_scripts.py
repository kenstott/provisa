# Copyright (c) 2026 Kenneth Stott
# Canary: 74e7d964-b8c4-477d-8046-2922b3377a47
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""scripts/new_demo.py scaffolds a named demo that passes the named demos' checks from the start,
but for what only its author can give it (the card's screenshot, the translations); and
scripts/check_demo.py reports, by check, what a demo is missing."""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest
import yaml

from tests.unit import test_named_demo_columns as columns
from tests.unit import test_named_demo_tour as tour

_REPO = Path(__file__).resolve().parents[2]
_NAMED = _REPO / "demo" / "named"
_LOCALES = _REPO / "provisa-ui" / "src" / "i18n" / "locales"


def _script(name: str):
    spec = importlib.util.spec_from_file_location(name, _REPO / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


new_demo = _script("new_demo")
check_demo = _script("check_demo")


@pytest.fixture
def scaffolded(tmp_path):
    """A demo "harbor" scaffolded into a copy of the named demos' and locales' directories."""
    named, locales = tmp_path / "named", tmp_path / "locales"
    named.mkdir()
    for tour_json in _LOCALES.glob("*/tour.json"):
        (locales / tour_json.parent.name).mkdir(parents=True)
        (locales / tour_json.parent.name / "tour.json").write_text(tour_json.read_text())
    home = new_demo.scaffold(
        "harbor", title="Harbor", port=28632, requires="0.1.0a486", named=named, locales=locales
    )
    return home, locales


def test_a_scaffolded_demo_passes_the_named_demo_tests(scaffolded, monkeypatch):
    home, locales = scaffolded
    tour._check_steps(home, yaml.safe_load((home / "tour.yaml").read_text())["steps"])
    tour.test_every_tour_ends_on_a_landing_page_the_ui_has(home / "tour.yaml")
    columns.test_every_column_of_a_named_demo_loads_whole(home / "config.yaml")
    monkeypatch.setattr(tour, "_LOCALES", locales)
    tour.test_every_step_has_its_words_in_every_language(home / "tour.yaml")


def test_a_scaffolded_demo_passes_every_check_but_its_authors(scaffolded):
    home, locales = scaffolded
    assert check_demo.check_examples(home) == []
    assert check_demo.check_bundle(home) == []
    assert check_demo.check_run_compose(home, "harbor") == []
    assert check_demo.check_card(home) == [
        "demo.yaml: published is not true, so the demo has no card",
        "demo.yaml: screenshot card.png is not beside it",
    ]
    untranslated = check_demo.check_locales("harbor", home, locales)
    assert len(untranslated) == len(list(locales.iterdir())) - 1  # every locale but English
    assert all("still the English words" in line for line in untranslated)


def test_the_scaffold_writes_no_template_stamp_and_no_token(scaffolded):
    home, _ = scaffolded
    for path in (p for p in home.rglob("*") if p.is_file()):
        text = path.read_text()
        assert "Canary:" not in text and not re.search(r"__[A-Z]+__", text), path
    words = json.loads((scaffolded[1] / "en" / "tour.json").read_text())["tour"]
    assert words["demos"]["harbor"]["title"] == "Harbor"


def test_a_name_no_demo_can_have_or_one_a_demo_has_is_refused(tmp_path):
    for name in ("Harbor", "9lives", "har-bor"):
        with pytest.raises(SystemExit, match="lower-case letters and digits"):
            new_demo.scaffold(name, title="t", port=1, requires="0.1.0", named=tmp_path)
    (tmp_path / "harbor").mkdir()
    with pytest.raises(SystemExit, match="a new demo needs a name no demo has"):
        new_demo.scaffold("harbor", title="t", port=1, requires="0.1.0", named=tmp_path)


def test_the_next_port_is_the_first_block_no_demo_publishes(tmp_path):
    for name, port in (("a", 28032), ("b", 28132), ("c", 28232), ("d", 28432)):
        (tmp_path / name).mkdir()
        (tmp_path / name / "docker-compose.yml").write_text(f'ports: ["{port}:5432"]\n')
    assert new_demo.next_port(tmp_path) == 28332


def test_a_block_is_taken_by_any_port_a_demo_publishes_in_it(tmp_path):
    """A demo's broker or second store takes its block too (the streaming demo's 29092), and a
    port its config.yaml names counts as one its compose does."""
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "docker-compose.yml").write_text(
        'ports: ["127.0.0.1:${A_PG_PORT:-28032}:5432", "127.0.0.1:${A_KAFKA_PORT-28192}:9092"]\n'
    )
    (tmp_path / "b").mkdir()
    (tmp_path / "b" / "config.yaml").write_text("port: ${env:B_PG_PORT:-28250}\n")
    assert new_demo.next_port(tmp_path) == 28332


def test_no_two_demos_publish_the_same_host_port():
    """Two demos defaulting to one port cannot run at once, and the next demo scaffolded would be
    handed a port already taken (opioids and datavault both took 28832, 2026-10-16)."""
    shared = {
        port: sorted(demos)
        for port, demos in new_demo.published_ports(_NAMED).items()
        if len(demos) > 1
    }
    assert shared == {}


@pytest.mark.parametrize("name", ["weblogs", "sonnets", "files"])
def test_the_shipped_demos_pass_the_static_checks(name):
    home = _NAMED / name
    assert check_demo.check_examples(home) == []
    assert check_demo.check_bundle(home) == []
    assert check_demo.check_run_compose(home, name) == []
    assert check_demo.check_locales(name, home) == []


def test_a_run_compose_off_the_loopback_or_unlabelled_is_reported(tmp_path):
    (tmp_path / "config.yaml").write_text("host: ${env:X_PG_HOST:-localhost}\n")
    (tmp_path / "docker-compose.run.yml").write_text(
        yaml.safe_dump(
            {
                "services": {
                    "postgres": {
                        "labels": {
                            "com.provisa.demo": "x",
                            "com.provisa.demo.role": "rdb",
                            "com.provisa.demo.host_env": "X_PG_HOST",
                            "com.provisa.demo.port_env": "X_PG_PORT",
                        },
                        "ports": ["28132:5432"],
                    },
                    "seeder": {"labels": {"com.provisa.demo": "y"}},
                }
            }
        )
    )
    assert check_demo.check_run_compose(tmp_path, "x") == [
        "postgres: a store with no com.provisa.demo.port label",
        "postgres: config.yaml does not read X_PG_PORT",
        "postgres: port 28132:5432 is not on the loopback (127.0.0.1:)",
        "seeder: not labelled com.provisa.demo: x",
        "seeder: no com.provisa.demo.role label",
    ]


def test_a_demo_with_stores_and_no_run_compose_is_reported(tmp_path):
    (tmp_path / "docker-compose.yml").write_text("services: {}\n")
    assert check_demo.check_run_compose(tmp_path, "x") == [
        "no docker-compose.run.yml: the stores of docker-compose.yml run from it"
    ]


def test_the_demo_s_test_cases_are_its_own_and_the_list_of_demos():
    collected = [
        "tests/unit/test_named_demo_tour.py::test_there_is_a_demo_tour_to_check",
        "tests/unit/test_named_demo_tour.py::test_every_step_names_what_the_demo_has[files]",
        "tests/unit/test_named_demo_tour.py::test_every_step_names_what_the_demo_has[weblogs]",
        "tests/unit/test_named_demo_columns.py::test_every_column_of_a_named_demo_loads_whole[weblogs]",
        "tests/unit/test_named_demo_columns.py::test_an_unquoted_comma_in_a_description_is_caught",
    ]
    assert check_demo.demo_cases(collected, "weblogs") == [
        collected[0],
        collected[2],
        collected[3],
    ]
