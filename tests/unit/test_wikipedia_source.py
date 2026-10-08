# Copyright (c) 2026 Kenneth Stott
# Canary: 2a6f9d13-8b4e-4c7a-9e5d-1f3b7c0a8e42
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1960: an HTML crawl on a ``files`` source, and Wikipedia as the brand it carries."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.core.models import Source, SourceType
from provisa.federation import pgwire_replica as pr
from provisa.file_source import wikipedia
from provisa.file_source.crawl import (
    CRAWL_SETTINGS,
    InvalidCrawl,
    crawl_landing_directory,
    crawl_operand,
)


def _files(**kw) -> Source:
    return Source(**{"id": "wiki", "type": SourceType.files, "path": "/data/wiki", **kw})


# -- the crawl of a files source ------------------------------------------------------------------


def test_a_crawl_reaches_the_file_adapter_under_the_adapters_own_names():
    crawl = {"start_urls": ["https://example.test/a"], "max_depth": 1, "link_selector": "a.x"}
    operand = pr.build_model_json(_files(mapping={"crawl": crawl}))["schemas"][0]["operand"]
    assert operand["directory"] == "/data/wiki"
    assert operand["crawl"] == {
        "startUrls": ["https://example.test/a"],
        "maxDepth": 1,
        "linkSelector": "a.x",
    }
    assert "crawl" not in pr.build_model_json(_files())["schemas"][0]["operand"]


def test_trino_is_given_the_same_crawl_as_the_bundle():
    from provisa.federation.trino_connectors import TrinoFilesConnector

    crawl = {"start_urls": ["https://example.test/a"], "remove_selectors": [".nav"]}
    source = _files(mapping={"crawl": crawl})
    details = TrinoFilesConnector().details(source)
    bundle = pr.build_model_json(source)["schemas"][0]["operand"]["crawl"]
    assert json.loads(details["crawl"]) == bundle
    assert details["glob"] == "/data/wiki"
    assert "crawl" not in TrinoFilesConnector().details(_files())


@pytest.mark.parametrize(
    "crawl, said",
    [
        ({"start_urls": ["https://example.test/a"], "depth": 2}, "unknown crawl setting"),
        ({"start_urls": "https://example.test/a"}, "must be a list"),
        ({"max_depth": 1}, "names no page"),
        ("https://example.test/a", "must be an object"),
    ],
)
def test_a_crawl_the_adapter_would_not_read_as_written_is_refused_by_name(crawl, said):
    with pytest.raises(InvalidCrawl, match=said):
        crawl_operand("wiki", {"crawl": crawl})


def test_a_crawls_files_land_in_the_sources_own_directory_under_the_data_directory(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    assert crawl_landing_directory("wiki") == tmp_path / "crawl" / "wiki"


# -- Wikipedia --------------------------------------------------------------------------------------


def test_a_wikipedia_source_is_a_crawl_with_the_brands_defaults():
    crawl = wikipedia.crawl_settings({"pages": ["List of tallest buildings"]}, "1.2.3")
    assert set(crawl) <= set(CRAWL_SETTINGS)  # every setting is one the adapter reads
    assert crawl["start_urls"] == ["https://en.wikipedia.org/wiki/List_of_tallest_buildings"]
    assert (crawl["max_depth"], crawl["max_pages"], crawl["request_delay"]) == (1, 25, "1 seconds")
    assert crawl["user_agent"] == "Provisa/1.2.3 (+https://provisa.dev) file-crawler"
    assert crawl["content_selector"] == "#mw-content-text .mw-parser-output"
    assert crawl["table_selector"] == "table.wikitable"
    assert crawl["follow_external_links"] is False
    # Links are chosen by what the page says each is: no pattern over addresses is set.
    assert crawl["link_selector"].startswith("a[rel='mw:WikiLink']")
    assert "link_exclude_patterns" not in crawl


def test_see_also_links_are_left_out_unless_the_operator_follows_them():
    by_default = wikipedia.crawl_settings({"pages": ["Dubai"]}, "1")
    followed = wikipedia.crawl_settings({"pages": ["Dubai"], "follow_see_also": True}, "1")
    german = wikipedia.crawl_settings({"pages": ["Dubai"], "language": "de"}, "1")
    section = "section[aria-labelledby='See_also']"
    assert section in by_default["remove_selectors"]
    assert section not in followed["remove_selectors"]
    assert "section[aria-labelledby='Siehe_auch']" in german["remove_selectors"]
    assert german["start_urls"] == ["https://de.wikipedia.org/wiki/Dubai"]


def test_an_edition_whose_see_also_heading_is_not_known_asks_for_it():
    with pytest.raises(wikipedia.InvalidWikipediaSource) as refused:
        wikipedia.crawl_settings({"pages": ["Dubaï"], "language": "fr"}, "1")
    assert refused.value.code == "wikipedia.see_also_heading_needed"
    given = wikipedia.crawl_settings(
        {"pages": ["Dubaï"], "language": "fr", "see_also_heading": "Articles_connexes"}, "1"
    )
    assert "section[aria-labelledby='Articles_connexes']" in given["remove_selectors"]
    # The operator gives the title as a page shows it; the section is found by it.
    titled = wikipedia.crawl_settings(
        {"pages": ["Dubaï"], "language": "fr", "see_also_heading": " Voir aussi "}, "1"
    )
    assert "section[aria-labelledby='Voir_aussi']" in titled["remove_selectors"]
    assert given["start_urls"] == ["https://fr.wikipedia.org/wiki/Duba%C3%AF"]


def test_what_the_operator_states_stands_in_place_of_a_default():
    crawl = wikipedia.crawl_settings(
        {
            "pages": ["https://en.wikipedia.org/wiki/Mission:_Impossible"],
            "max_depth": 0,
            "contact": "ops@example.test",
            "crawl": {"table_selector": "table", "max_pages": 3},
        },
        "1",
    )
    assert crawl["start_urls"] == ["https://en.wikipedia.org/wiki/Mission:_Impossible"]
    assert (crawl["max_depth"], crawl["max_pages"], crawl["table_selector"]) == (0, 3, "table")
    assert crawl["user_agent"].endswith("; ops@example.test")


@pytest.mark.parametrize(
    "settings, code",
    [
        ({"pages": []}, "wikipedia.no_pages"),
        ({"pages": ["Dubai"], "language": "en/../x"}, "wikipedia.unknown_language"),
        ({"pages": ["https://example.test/wiki/Dubai"]}, "wikipedia.page_not_of_edition"),
    ],
)
def test_what_cannot_be_made_into_a_crawl_is_refused_with_its_reason(settings, code):
    with pytest.raises(wikipedia.InvalidWikipediaSource) as refused:
        wikipedia.crawl_settings(settings, "1")
    assert refused.value.code == code


def test_creating_a_wikipedia_source_writes_the_whole_crawl_into_the_files_source(
    monkeypatch, tmp_path
):
    from provisa.api.admin.schema_common import _expand_wikipedia_if_needed

    monkeypatch.setenv("PROVISA_DATA_DIR", str(tmp_path))
    hints = {"brand": "wikipedia", "wikipedia": {"pages": ["Dubai"], "max_depth": 0}}
    given = SimpleNamespace(
        id="wiki",
        path=None,
        mapping_json='{"refresh_interval": "1 days"}',
        federation_hints_json=json.dumps(hints),
    )
    assert _expand_wikipedia_if_needed(given) is None
    mapping = json.loads(given.mapping_json)
    assert mapping["refresh_interval"] == "1 days"
    assert mapping["crawl"]["start_urls"] == ["https://en.wikipedia.org/wiki/Dubai"]
    assert given.path == str(tmp_path / "crawl" / "wiki") and (tmp_path / "crawl" / "wiki").is_dir()
    # What it wrote is a crawl the files source's connectors accept as written.
    assert crawl_operand("wiki", mapping)["maxDepth"] == 0

    refused = SimpleNamespace(
        id="wiki",
        path=None,
        mapping_json=None,
        federation_hints_json='{"brand": "wikipedia", "wikipedia": {}}',
    )
    answer = _expand_wikipedia_if_needed(refused)
    assert (answer.success, answer.code) == (False, "wikipedia.no_pages")

    plain = SimpleNamespace(id="f", path="/x", mapping_json=None, federation_hints_json=None)
    assert _expand_wikipedia_if_needed(plain) is None and plain.mapping_json is None
