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
        "demo.yaml: no video tagline",
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


def test_every_published_demo_names_its_video_s_tagline():
    published = [
        spec.parent.name
        for spec in sorted(_NAMED.glob("*/demo.yaml"))
        if yaml.safe_load(spec.read_text()).get("published") is True
    ]
    assert published
    for name in published:
        video = yaml.safe_load((_NAMED / name / "demo.yaml").read_text()).get("video")
        assert isinstance(video, dict) and isinstance(video.get("tagline"), str), name
        assert video["tagline"].strip(), name


@pytest.mark.parametrize("video", [None, "Logs are Tables", {}, {"tagline": "  "}, {"tagline": 3}])
def test_a_demo_without_a_video_tagline_is_reported(tmp_path, video):
    spec = {"title": "T", "description": "D", "screenshot": "card.png", "published": False}
    if video is not None:
        spec["video"] = video
    (tmp_path / "demo.yaml").write_text(yaml.safe_dump(spec))
    assert "demo.yaml: no video tagline" in check_demo.check_card(tmp_path)


def test_a_demo_with_a_video_tagline_is_not_reported_for_it(tmp_path):
    spec = {"published": False, "video": {"tagline": "Logs are Tables"}}
    (tmp_path / "demo.yaml").write_text(yaml.safe_dump(spec))
    assert "demo.yaml: no video tagline" not in check_demo.check_card(tmp_path)


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
    assert check_demo.check_images(home) == []


def test_an_image_step_whose_picture_is_missing_or_not_a_png_is_reported(tmp_path):
    (tmp_path / "shot.png").write_bytes(b"not a png")
    (tmp_path / "tour.yaml").write_text(
        "steps:\n- {key: a, image: shot.png}\n- {key: b, image: gone.png}\n"
    )
    assert check_demo.check_images(tmp_path) == [
        "tour.yaml: step a: shot.png is not a PNG",
        f"tour.yaml: step b: {tmp_path.name}: tour image 'gone.png' is not a PNG beside tour.yaml",
    ]


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


def test_a_shared_store_without_its_namespace_or_access_is_reported(tmp_path):
    """The sandbox tier's node loads a shared store's data under the demo version's suffix, with
    the demo's own roles and grants: a Postgres store with neither (churnprod as first shipped)
    fails a visitor's walk before its first step, so check_demo reports it."""
    (tmp_path / "config.yaml").write_text(
        "host: ${env:X_PG_HOST:-localhost}\nport: ${env:X_PG_PORT:-1}\ndatabase: x\n"
    )
    store = {
        "image": "postgres:16",
        "labels": {
            "com.provisa.demo": "x",
            "com.provisa.demo.role": "rdb",
            "com.provisa.demo.host_env": "X_PG_HOST",
            "com.provisa.demo.port_env": "X_PG_PORT",
            "com.provisa.demo.port": "5432",
        },
        "ports": ["127.0.0.1:1:5432"],
    }
    run = tmp_path / "docker-compose.run.yml"
    run.write_text(yaml.safe_dump({"services": {"postgres": store}}))
    assert check_demo.check_run_compose(tmp_path, "x") == [
        "postgres: a shared postgres store with no com.provisa.demo.ns_env label",
        "postgres: a shared postgres store with no com.provisa.demo.access label",
    ]
    store["labels"] |= {
        "com.provisa.demo.ns_env": "X_PG_NS",
        "com.provisa.demo.access": "db/reader.sql",
    }
    run.write_text(yaml.safe_dump({"services": {"postgres": store}}))
    assert check_demo.check_run_compose(tmp_path, "x") == [
        "postgres: config.yaml does not name its data with X_PG_NS",
        "postgres: its access script db/reader.sql is not in the demo",
    ]
    (tmp_path / "config.yaml").write_text(
        "host: ${env:X_PG_HOST:-localhost}\nport: ${env:X_PG_PORT:-1}\ndatabase: x${env:X_PG_NS}\n"
    )
    (tmp_path / "db").mkdir()
    (tmp_path / "db" / "reader.sql").write_text("CREATE ROLE r;\n")
    assert check_demo.check_run_compose(tmp_path, "x") == []


def test_every_demo_s_run_compose_passes():
    """Each demo's run compose, as the sandbox tier's node reads it."""
    found = {
        home.name: check_demo.check_run_compose(home, home.name)
        for home in sorted(_NAMED.iterdir())
        if (home / "docker-compose.run.yml").is_file()
    }
    assert {name: problems for name, problems in found.items() if problems} == {}


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


def _demo_yaml(tmp_path: Path, **fields) -> Path:
    home = tmp_path / "zoo"
    home.mkdir()
    (home / "demo.yaml").write_text(yaml.safe_dump(fields))
    return home


def test_a_published_demo_in_regions_must_be_one_the_regions_tier_serves(tmp_path):
    # A demo in regions is served by the regions sandbox tier, which the UI server routes to only
    # for the demos PROVISA_SANDBOX_REGIONS_DEMOS names; one not named there opens on the plain
    # tier, which has no region nodes.
    home = _demo_yaml(tmp_path, published=True, regions={"visitor": "us", "headless": ["eu"]})
    assert check_demo.check_regions("zoo", home, listed=frozenset({"retail"})) == [
        "demo.yaml: in regions, but not one PROVISA_SANDBOX_REGIONS_DEMOS names "
        "(packaging/linux/first-launch.sh)"
    ]
    assert check_demo.check_regions("zoo", home, listed=frozenset({"retail", "zoo"})) == []


def test_an_unpublished_or_single_region_demo_needs_no_entry(tmp_path):
    unpublished = _demo_yaml(tmp_path, published=False, regions={"visitor": "us"})
    assert check_demo.check_regions("zoo", unpublished, listed=frozenset()) == []
    (unpublished / "demo.yaml").write_text(yaml.safe_dump({"published": True}))
    assert check_demo.check_regions("zoo", unpublished, listed=frozenset()) == []


def test_the_deployed_regions_demos_are_read_from_the_install():
    assert "retail" in check_demo.regions_demos()
