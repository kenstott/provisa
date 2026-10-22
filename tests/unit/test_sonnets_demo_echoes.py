# Copyright (c) 2026 Kenneth Stott
# Canary: e261b3b3-c280-4569-9bef-3ce1a4b312c7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The sonnets demo links its echoes with a statement, not host code (REQ-2016, REQ-2134).

link_echoes is an INSERT...SELECT over line joined to lines_like: the line-searches relationship
feeds the search each line's words and sonnet (REQ-2099), lines_like leaves that sonnet's points
out through its "@sonnet" filter (REQ-1983), and the points carry the words they were found for,
which the join matches. A theme still searches lines_like for a phrase of its own."""

# Requirements: REQ-2016, REQ-2099, REQ-2134
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from provisa.qdrant.table import check_table

pytestmark = pytest.mark.unit

_DEMO = Path(__file__).resolve().parents[2] / "demo" / "named" / "sonnets"
_CONFIG = yaml.safe_load((_DEMO / "config.yaml").read_text())
_FUNCTIONS = {f["name"]: f for f in _CONFIG["functions"]}
_RELATIONSHIPS = {r["id"]: r for r in _CONFIG["relationships"]}
[_SEARCH] = [t for t in _CONFIG["tables"] if t["table"] == "lines_like"]


def test_the_config_holds_no_host_code():
    assert not [f["name"] for f in _CONFIG["functions"] if f["impl_kind"] in {"python", "script"}]
    assert not (_DEMO / "echoes.py").exists()


def test_link_echoes_is_a_statement_joining_each_line_to_its_search():
    command = _FUNCTIONS["link_echoes"]
    assert command["impl_kind"] == "statement"
    sql = command["binding"]["sql"]
    assert 'INSERT INTO "sonnets"."echo" (line_id, echo_line_id, score)' in sql
    assert 'JOIN "sonnets"."lines_like" AS s ON s._nf_query = l.text' in sql
    assert "s._nf_k = 3" in sql


def test_the_relationship_feeds_each_lines_words_and_sonnet_and_joins_on_the_words():
    relationship = _RELATIONSHIPS["line-searches"]
    assert (relationship["source_table_id"], relationship["target_table_id"]) == (
        "line",
        "lines_like",
    )
    assert relationship["parameters"] == {"query": "text", "sonnet": "sonnet_number"}
    assert (relationship["source_column"], relationship["target_column"]) == ("text", "_nf_query")


def test_lines_like_leaves_the_given_sonnet_out_and_is_a_valid_search():
    request = json.loads(_SEARCH["query_template"].replace("${env:SONNETS_QDRANT_NS}", ""))
    assert request["filter"] == {
        "must_not": [{"key": "sonnet_number", "match": {"value": "@sonnet"}}]
    }
    table = {
        "table_name": _SEARCH["table"],
        "query_template": _SEARCH["query_template"],
        "columns": [{**c, "column_name": c["name"]} for c in _SEARCH["columns"]],
    }
    assert check_table(table) is not None


def test_a_theme_still_searches_for_a_phrase_of_its_own():
    [theme] = [v for v in _CONFIG["views"] if v["id"] == "theme_time"]
    assert "s._nf_query = 'the ravages of time'" in theme["sql"]
