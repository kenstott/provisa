# Copyright (c) 2026 Kenneth Stott
# Canary: cb4ea239-056a-4eec-b94c-d67bf322dcc6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2017: an Elasticsearch table registered from a native search -- its parse, its arguments
bound as values, what it refuses, and the rows its buckets and hits become."""

# Requirements: REQ-2017

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.elasticsearch.native_query import (
    NativeSearchRefused,
    bind,
    check,
    check_table,
    flatten_buckets,
    hit_document,
    parse,
)

_MATCH = {
    "index": "reviews",
    "query": {"match_phrase": {"text": "{{phrase}}"}},
    "highlight": {"fields": {"text": {}}},
}


def _text(doc: dict) -> str:
    return json.dumps(doc)


def test_an_argument_is_bound_as_a_value_never_as_text():
    search = check("t", _text(_MATCH), {"phrase"})
    hostile = '"}}, "script": {"source": "ctx._source.x = 1'
    body = bind(search, {"phrase": hostile})
    # The value sits where the placeholder was, as one string: no key was added anywhere.
    assert body["query"] == {"match_phrase": {"text": hostile}}
    assert set(body) == {"query", "highlight"}
    # The stored search is untouched.
    assert search.body["query"]["match_phrase"]["text"] == "{{phrase}}"


def test_an_absent_argument_binds_null_and_a_typed_one_its_json_value():
    from datetime import date
    from decimal import Decimal

    doc = {
        "index": "orders",
        "query": {
            "bool": {
                "filter": [
                    {"range": {"placed": {"gte": "{{since}}"}}},
                    {"range": {"total": {"gte": "{{floor}}"}}},
                    {"term": {"region": "{{region}}"}},
                ]
            }
        },
    }
    body = bind(
        check("t", _text(doc), {"since", "floor", "region"}),
        {"since": date(2024, 1, 2), "floor": Decimal("9.5"), "region": None},
    )
    filters = body["query"]["bool"]["filter"]
    assert filters[0]["range"]["placed"]["gte"] == "2024-01-02"
    assert filters[1]["range"]["total"]["gte"] == 9.5
    assert filters[2]["term"]["region"] is None


@pytest.mark.parametrize(
    ("doc", "reason"),
    [
        ({"query": {"match_all": {}}}, "names its index"),
        ({"index": "_delete_by_query", "query": {"match_all": {}}}, "only searches"),
        ({"index": "reviews/_update", "query": {"match_all": {}}}, "only searches"),
        ({"index": "reviews", "query": {}, "script": {"source": "x"}}, "not \\['script'\\]"),
        ({"index": "reviews", "query": {}, "search_after": [1]}, "not \\['search_after'\\]"),
        ({"index": "reviews", "size": "{{n}}", "query": {}}, "size is the most rows"),
        ({"index": "reviews"}, "a query or aggregations"),
    ],
)
def test_what_is_not_a_search_is_refused_by_name(doc, reason):
    with pytest.raises(NativeSearchRefused, match=reason):
        parse("t", _text(doc))


def test_a_placeholder_inside_text_and_an_undeclared_or_unused_argument_are_refused():
    embedded = {"index": "r", "query": {"match": {"text": "battery {{word}}"}}}
    with pytest.raises(NativeSearchRefused, match="inside a longer string"):
        check("t", _text(embedded), {"word"})
    with pytest.raises(NativeSearchRefused, match="declares as no argument"):
        check("t", _text(_MATCH), set())
    with pytest.raises(NativeSearchRefused, match="appear nowhere"):
        check("t", _text(_MATCH), {"phrase", "extra"})


def test_nested_buckets_flatten_to_one_row_per_innermost_bucket():
    aggregations = {
        "by_brand": {
            "buckets": [
                {
                    "key": "Acme",
                    "doc_count": 3,
                    "by_month": {
                        "buckets": [
                            {
                                "key": 1704067200000,
                                "key_as_string": "2024-01",
                                "doc_count": 2,
                                "avg_rating": {"value": 3.5},
                            },
                            {
                                "key": 1706745600000,
                                "key_as_string": "2024-02",
                                "doc_count": 1,
                                "avg_rating": {"value": 2.0},
                            },
                        ]
                    },
                },
                {
                    "key": "Bolt",
                    "doc_count": 1,
                    "by_month": {"buckets": []},
                },
            ]
        },
        "overall": {"value": 3.0},
    }
    rows = flatten_buckets("t", aggregations)
    assert rows == [
        {
            "overall": 3.0,
            "by_brand": "Acme",
            "by_brand_doc_count": 3,
            "by_month": "2024-01",
            "by_month_doc_count": 2,
            "avg_rating": 3.5,
        },
        {
            "overall": 3.0,
            "by_brand": "Acme",
            "by_brand_doc_count": 3,
            "by_month": "2024-02",
            "by_month_doc_count": 1,
            "avg_rating": 2.0,
        },
    ]


def test_multi_value_metrics_composite_keys_and_keyed_buckets_take_a_column_each():
    aggregations = {
        "pairs": {
            "buckets": [
                {
                    "key": {"brand": "Acme", "year": 2024},
                    "doc_count": 4,
                    "rating": {"count": 4, "min": 1.0, "max": 5.0, "avg": 3.0, "sum": 12.0},
                    "p": {"values": {"50.0": 3.0, "95.0": 5.0}},
                }
            ]
        }
    }
    (row,) = flatten_buckets("t", aggregations)
    assert row == {
        "pairs_doc_count": 4,
        "pairs_brand": "Acme",
        "pairs_year": 2024,
        "rating_count": 4,
        "rating_min": 1.0,
        "rating_max": 5.0,
        "rating_avg": 3.0,
        "rating_sum": 12.0,
        "p_50_0": 3.0,
        "p_95_0": 5.0,
    }
    keyed = {"sentiment": {"buckets": {"bad": {"doc_count": 7}, "good": {"doc_count": 2}}}}
    assert flatten_buckets("t", keyed) == [
        {"sentiment_doc_count": 7, "sentiment": "bad"},
        {"sentiment_doc_count": 2, "sentiment": "good"},
    ]


def test_two_bucket_aggregations_side_by_side_are_refused():
    aggregations = {"a": {"buckets": []}, "b": {"buckets": []}}
    with pytest.raises(NativeSearchRefused, match="side by side"):
        flatten_buckets("t", aggregations)


def test_a_hit_is_its_source_its_id_score_and_highlights():
    hit = {
        "_id": "r1",
        "_score": 4.2,
        "_source": {"product_id": 7, "text": "the battery dies fast"},
        "highlight": {"text": ["the <em>battery dies</em> fast"], "title.raw": ["x"]},
    }
    assert hit_document(hit) == {
        "product_id": 7,
        "text": "the battery dies fast",
        "_id": "r1",
        "_score": 4.2,
        "highlight_text": ["the <em>battery dies</em> fast"],
        "highlight_title_raw": ["x"],
    }


def _table(**kw):
    columns = kw.pop("columns")
    return SimpleNamespace(
        table_name="reviews_matching",
        query_template=_text(_MATCH),
        change_signal=None,
        row_materialize=False,
        columns=[SimpleNamespace(**c) for c in columns],
        **kw,
    )


def test_a_registered_search_takes_query_arg_parameters_only():
    ok = _table(
        columns=[
            {"name": "_nf_phrase", "native_filter_type": "query_arg", "data_type": "text"},
            {"name": "text", "native_filter_type": None, "data_type": "text"},
        ]
    )
    assert check_table(ok).index == "reviews"
    wrong = _table(
        columns=[
            {"name": "_nf_phrase", "native_filter_type": "query_param", "data_type": "text"},
        ]
    )
    with pytest.raises(NativeSearchRefused, match="query_arg, not \\['query_param'\\]"):
        check_table(wrong)
    feed = _table(
        columns=[{"name": "_nf_phrase", "native_filter_type": "query_arg", "data_type": "text"}],
    )
    feed.change_signal = "native"
    with pytest.raises(NativeSearchRefused, match="no change feed"):
        check_table(feed)


def test_a_native_search_is_read_natively_whatever_the_engine():
    from provisa.api.data.materialization import is_native_read

    types = {"es": "elasticsearch"}
    assert is_native_read({"source_id": "es", "query_template": "{}", "columns": []}, types)
    assert not is_native_read({"source_id": "es", "query_template": None, "columns": []}, types)


def test_a_search_s_pages_keep_each_hit_s_score():
    """The pages are sorted (search_after), and a sorted search's hits carry no _score unless
    track_scores asks for it: the reviews demo's score column read null."""
    from contextlib import contextmanager

    from provisa.elasticsearch.fetch import search_hits

    sent: list[dict] = []

    class _Resp:
        def __init__(self, body):
            self._body = body

        def raise_for_status(self):
            return None

        def json(self):
            return self._body

    class _Client:
        def post(self, path, json=None, params=None):
            if path.endswith("/_pit"):
                return _Resp({"id": "pit-1"})
            sent.append(json)
            hits = [] if len(sent) > 1 else [{"_id": "a", "_score": 1.5, "sort": [1.5, 0]}]
            return _Resp({"pit_id": "pit-1", "hits": {"hits": hits}})

        def request(self, method, path, json=None):
            return _Resp({})

    class _Conn:
        @contextmanager
        def _client(self):
            yield _Client()

    body = {"query": {"match_phrase": {"text": "battery dies"}}, "size": 10}
    pages = list(search_hits(_Conn(), "reviews", body, 100))  # type: ignore[arg-type]
    assert pages == [[{"_id": "a", "_score": 1.5, "sort": [1.5, 0]}]]
    assert all(page["track_scores"] is True for page in sent)
    assert sent[0]["sort"] == ["_score"]
