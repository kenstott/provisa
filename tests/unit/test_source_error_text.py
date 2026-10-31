# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A source error never tells a client where the source is (REQ-2006; cloud-dev, 2026-10-31: a
visitor on the reviews demo read http://provisa-demo-stores-elasticsearch-1:9200/... in the
tour). A read that fails is refused naming its source, the detail logged; the one text every
transport answers with (client_text) drops httpx's wording and any internal address."""

# Requirements: REQ-2006, REQ-2087

from __future__ import annotations

import asyncio
import json
import logging
import re
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from provisa.api import engine_error_names
from provisa.api.data import materialization
from provisa.core.read_refusal import ReadRefused, SourceUnavailable

pytestmark = pytest.mark.unit

_URL = "http://provisa-demo-stores-elasticsearch-1:9200/reviews_reviews_6ac065b6/_pit?keep_alive=1m"
_LOCALES = Path(__file__).resolve().parents[2] / "provisa-ui" / "src" / "i18n" / "locales"


def _status_error() -> httpx.HTTPStatusError:
    request = httpx.Request("POST", _URL)
    response = httpx.Response(503, request=request)
    with pytest.raises(httpx.HTTPStatusError) as raised:
        response.raise_for_status()
    return raised.value


def test_the_incident_s_text_names_no_host_or_url():
    said = engine_error_names.client_text(str(_status_error()))
    assert said == "a source answered 503 Service Unavailable"


def test_the_text_inside_a_wrapping_message_loses_its_address_and_keeps_the_rest():
    wrapped = f"GQL remote fetch failed for 'reviews': {_status_error()}"
    said = engine_error_names.client_text(wrapped)
    assert (
        said == "GQL remote fetch failed for 'reviews': a source answered 503 Service Unavailable"
    )


@pytest.mark.parametrize(
    "address",
    [
        "http://provisa-demo-stores-elasticsearch-1:9200/x",
        "http://localhost:8000/data",
        "https://10.128.0.7:8443/",
        "http://192.168.96.2:9200/_search",
        "http://metadata.google.internal/computeMetadata/v1/",
        "http://es.provisa.svc.cluster.local:9200/",
    ],
)
def test_an_internal_address_in_any_error_is_taken_out(address):
    said = engine_error_names.client_text(f"could not reach {address} in time")
    assert said == "could not reach [internal address] in time"


def test_a_public_address_stays():
    text = "see https://docs.provisa.dev/sources/elasticsearch for its settings"
    assert engine_error_names.client_text(text) == text


def _table() -> dict:
    return {"source_id": "reviews-search", "landing_ttl": 0}


def test_a_read_a_source_fails_is_refused_naming_the_source_and_the_detail_is_logged(
    monkeypatch, caplog
):
    async def answer():
        raise _status_error()

    async def live(_tn, _state, _loc, _rewrites, _values, read):
        await read()

    monkeypatch.setattr(materialization, "landing_ttl", lambda *_: 0)
    monkeypatch.setattr(materialization, "_answer_live", live)
    state = SimpleNamespace(federation_engine=SimpleNamespace(), org_id="o1")
    monkeypatch.setattr("provisa.api_source.engine_cache.cache_location", lambda *_: None)
    monkeypatch.setattr("provisa.api_source.engine_cache.resolved_cache_catalog", lambda *_: "c")
    monkeypatch.setattr("provisa.api_source.engine_cache.org_cache_schema", lambda *_: "s")
    with caplog.at_level(logging.WARNING), pytest.raises(SourceUnavailable) as refused:
        asyncio.run(materialization._land_per_statement("t", _table(), state, {}, {}, {}, answer))
    assert isinstance(refused.value, ReadRefused)
    assert refused.value.code == "data.source_unavailable"
    assert refused.value.params == {"source": "reviews-search"}
    assert "reviews-search" in str(refused.value)
    assert "elasticsearch-1" not in str(refused.value)
    assert _URL in caplog.text  # the operator's


def test_every_locale_words_the_refusal_with_its_source_and_no_requirement_id():
    catalogs = sorted(_LOCALES.glob("*/serverErrors.json"))
    assert len(catalogs) == 13
    for catalog in catalogs:
        said = json.loads(catalog.read_text())["serverErrors"]["data"]["source_unavailable"]
        assert "{{source}}" in said, catalog
        assert not re.search(r"REQ-\d+", said), catalog
