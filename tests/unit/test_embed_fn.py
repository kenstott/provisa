# Copyright (c) 2026 Kenneth Stott
# Canary: 3366dbf0-393d-43c5-9101-010dd642692a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``embed(model, text)`` is evaluated by Provisa, never sent to a source (REQ-2002).

A literal text is embedded once, with the statement; a text of rows is a select-list item whose
distinct texts are embedded once the rows are read. The model is a registered, enabled model; its
id, provider, dimensions and version are in the audit of every statement that used it.
"""

# Requirements: REQ-2002

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from provisa.executor.result import QueryResult
from provisa.vector.embed_fn import (
    VECTOR_TYPE,
    EmbedRefused,
    RowEmbed,
    embed_rows,
    fold,
    mentions,
)
from provisa.vector.registry import VectorModel, VectorModelError, VectorModelRegistry

pytestmark = pytest.mark.asyncio

_MINI = VectorModel(id="mini", provider="openai", dimensions=3, version="2024-01")


class _Provider:
    """Embeds a text as [len, 1, 0] and records every batch it was asked for."""

    def __init__(self, version: str | None = None) -> None:
        self.batches: list[list[str]] = []
        self._version = version

    async def embed(self, texts, model):
        del model
        self.batches.append(list(texts))
        return [[float(len(t)), 1.0, 0.0] for t in texts]

    async def reported_version(self, model):
        del model
        return self._version


def _registry(*models: VectorModel) -> VectorModelRegistry:
    return VectorModelRegistry(list(models) or [_MINI])


async def test_a_literal_text_is_embedded_once_with_the_statement():
    provider = _Provider()
    folded = await fold(
        "SELECT id FROM docs WHERE cosine_similarity(vec, embed('mini', 'refund')) > 0.5",
        _registry(),
        lambda _m: provider,
    )
    assert "embed" not in folded.sql.lower()
    assert "CAST(ARRAY[6.0, 1.0, 0.0] AS REAL[])" in folded.sql
    assert provider.batches == [["refund"]]
    assert folded.rows == ()


async def test_a_text_of_rows_is_a_select_list_item_embedded_once_the_rows_are_read():
    folded = await fold(
        "SELECT id, embed('mini', subject) AS vec FROM tickets", _registry(), lambda _m: _Provider()
    )
    assert folded.sql == "SELECT id, subject AS vec FROM tickets"
    assert folded.rows == (RowEmbed(position=1, name="vec", model="mini"),)


async def test_an_unnamed_select_list_embed_keeps_its_column_name():
    folded = await fold(
        "SELECT embed('mini', subject) FROM tickets", _registry(), lambda _m: _Provider()
    )
    assert folded.sql == "SELECT subject AS embed FROM tickets"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM tickets WHERE embed('mini', subject) IS NOT NULL",
        "SELECT id FROM tickets ORDER BY embed('mini', subject)",
        "SELECT cosine_similarity(vec, embed('mini', subject)) FROM tickets",
        "SELECT id FROM (SELECT embed('mini', subject) AS v, id FROM tickets) AS t",
    ],
)
async def test_an_embed_of_rows_outside_the_select_list_is_refused_by_name(sql):
    with pytest.raises(EmbedRefused, match=r"embed\('mini', subject\).*select list"):
        await fold(sql, _registry(), lambda _m: _Provider())


async def test_a_model_not_registered_or_disabled_is_refused_naming_it():
    with pytest.raises(VectorModelError, match="'other' is not registered"):
        await fold("SELECT embed('other', 'x')", _registry(), lambda _m: _Provider())
    disabled = VectorModel(id="old", provider="openai", dimensions=3, enabled=False)
    with pytest.raises(VectorModelError, match="'old' is disabled"):
        await fold("SELECT embed('old', 'x')", _registry(_MINI, disabled), lambda _m: _Provider())


async def test_the_model_is_a_quoted_name_and_the_call_takes_two_arguments():
    with pytest.raises(EmbedRefused, match="model name in quotes"):
        await fold("SELECT embed(m, 'x') FROM t", _registry(), lambda _m: _Provider())
    with pytest.raises(EmbedRefused, match="given 1 arguments"):
        await fold("SELECT embed('mini')", _registry(), lambda _m: _Provider())


async def test_a_vector_of_other_dimensions_is_refused():
    class _Short(_Provider):
        async def embed(self, texts, model):
            return [[1.0, 0.0] for _ in texts]

    with pytest.raises(EmbedRefused, match="2 dimensions; it is registered with 3"):
        await fold("SELECT embed('mini', 'x')", _registry(), lambda _m: _Short())


async def test_the_audit_names_each_model_with_its_version():
    reported = await fold("SELECT embed('mini', 'x')", _registry(), lambda _m: _Provider("sha:1"))
    assert reported.audit == (
        {"id": "mini", "provider": "openai", "dimensions": 3, "version": "sha:1"},
    )
    stated = await fold("SELECT embed('mini', 'x')", _registry(), lambda _m: _Provider())
    assert stated.audit is not None and stated.audit[0]["version"] == "2024-01"


async def test_each_distinct_text_is_embedded_once_and_a_null_text_stays_null():
    provider = _Provider()
    result = QueryResult(
        rows=[(1, "ab"), (2, "abc"), (3, "ab"), (4, None)],
        column_names=["id", "vec"],
        column_types=["integer", "varchar"],
    )
    out = await embed_rows(result, (RowEmbed(1, "vec", "mini"),), _registry(), lambda _m: provider)
    assert provider.batches == [["ab", "abc"]]
    assert out.rows == [
        (1, [2.0, 1.0, 0.0]),
        (2, [3.0, 1.0, 0.0]),
        (3, [2.0, 1.0, 0.0]),
        (4, None),
    ]
    assert out.column_types == ["integer", VECTOR_TYPE]


def test_the_cheap_check_finds_the_call():
    assert mentions("SELECT EMBED ('m', x)")
    assert not mentions("SELECT embedding FROM t")


# -- the model's version ------------------------------------------------------------------------


async def test_a_provider_reported_version_is_recorded_before_the_stated_one():
    from provisa.vector.providers import OpenAICompatibleProvider, model_version

    assert await model_version(_MINI, OpenAICompatibleProvider()) == "2024-01"
    assert await model_version(_MINI, _Provider("digest")) == "digest"
    unstated = VectorModel(id="m", provider="openai", dimensions=3)
    assert await model_version(unstated, OpenAICompatibleProvider()) is None


async def test_ollama_reports_the_model_digest(monkeypatch):
    import httpx

    from provisa.vector.providers import OllamaProvider

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/tags"
        return httpx.Response(200, json={"models": [{"name": "nomic:latest", "digest": "abc123"}]})

    real = httpx.AsyncClient
    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        lambda **kw: real(transport=httpx.MockTransport(handler), **kw),
    )
    model = VectorModel(id="nomic", provider="ollama", dimensions=3)
    assert await OllamaProvider().reported_version(model) == "abc123"


async def test_a_local_huggingface_model_reports_its_snapshot_commit(monkeypatch):
    from provisa.vector.providers import HuggingFaceLocalProvider

    hub = SimpleNamespace(
        snapshot_download=lambda repo, local_files_only: (
            f"/cache/models--{repo}/snapshots/0f1e2d3c" if local_files_only else None
        )
    )
    monkeypatch.setitem(sys.modules, "huggingface_hub", hub)
    # The model is loaded before its snapshot is read (a host that never had it fetches it).
    monkeypatch.setitem(
        sys.modules, "sentence_transformers", SimpleNamespace(SentenceTransformer=lambda _id: None)
    )
    monkeypatch.setattr(HuggingFaceLocalProvider, "_cache", {})
    model = VectorModel(id="all-MiniLM", provider="huggingface", dimensions=3)
    assert await HuggingFaceLocalProvider().reported_version(model) == "0f1e2d3c"


def test_the_registry_reads_a_stated_version():
    registry = VectorModelRegistry.from_config(
        [{"id": "m", "provider": "openai", "dimensions": 3, "version": "v2"}]
    )
    assert registry.get("m").version == "v2"


# -- through the pipeline -----------------------------------------------------------------------


class _Routed(Exception):
    def __init__(self, sql: str) -> None:
        self.sql = sql


async def _governed_sql(monkeypatch, sql: str, provider) -> tuple[str, Any]:
    """The statement as governance hands it to routing, and the governed statement's embed."""
    import provisa.api.app as app_mod
    from provisa.pgwire import _pipeline
    from tests.unit.test_governed_sql_engine_internals import _state

    state = _state(["*"])
    state.tables[0]["columns"].append(
        {"column_name": "note", "data_type": "varchar", "visible_to": ["analyst"]}
    )
    monkeypatch.setattr(app_mod, "state", state, raising=False)
    monkeypatch.setattr(_pipeline, "vector_registry", AsyncMock(return_value=_registry()))
    monkeypatch.setattr(_pipeline, "embedding_provider", lambda _m: provider)
    governed = await _pipeline.govern_statement(sql, "analyst")

    async def _routing(exec_sql, *_args, **_kwargs):
        raise _Routed(exec_sql)

    with patch.object(_pipeline, "_optimize_and_route", new=AsyncMock(side_effect=_routing)):
        with pytest.raises(_Routed) as routed:
            await _pipeline.route_governed(governed)
    return routed.value.sql, governed.memo.get("embed")


async def test_governance_hands_routing_no_embed_call(monkeypatch):
    provider = _Provider()
    sql, folded = await _governed_sql(
        monkeypatch, "SELECT o.id, embed('mini', o.note) AS v FROM sales.orders o", provider
    )
    assert "embed" not in sql.lower()
    assert folded is not None and folded.rows == (RowEmbed(1, "v", "mini"),)


async def test_a_plan_that_embeds_rows_does_not_stream_from_its_terminal():
    from provisa.pgwire._pipeline import streams_from_terminal
    from provisa.vector.embed_fn import Folded

    plan = SimpleNamespace(embedded=None, two_step=None, scratch=(), columns=None)
    assert streams_from_terminal(plan)  # type: ignore[arg-type]
    plan.embedded = Folded("SELECT 1", audit=({"id": "mini"},))
    assert streams_from_terminal(plan)  # type: ignore[arg-type]  # a literal embed is done
    plan.embedded = Folded("SELECT 1", rows=(RowEmbed(0, "v", "mini"),))
    assert not streams_from_terminal(plan)  # type: ignore[arg-type]


async def test_the_audit_record_names_the_models_used():
    from provisa.pgwire._pipeline import _enforced_with_limit_outcome
    from provisa.vector.embed_fn import Folded

    used = ({"id": "mini", "provider": "openai", "dimensions": 3, "version": "v"},)
    plan = SimpleNamespace(limit_outcome=None, embedded=Folded("SELECT 1", audit=used))
    resolved = _enforced_with_limit_outcome({"row_cap": 10}, plan)  # type: ignore[arg-type]
    assert resolved() == {"row_cap": 10, "embed": used}


async def test_the_audit_row_names_the_models_without_a_row_limit(monkeypatch):
    """REQ-2002: every statement that embedded says so, not only one whose row limit fired."""
    from provisa.audit.pipeline import PendingAudit
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route
    from provisa.vector.embed_fn import Folded

    used = ({"id": "mini", "provider": "openai", "dimensions": 3, "version": "v"},)
    written: list = []

    async def write_audit(audit, *_a, **_k):
        written.append(audit)

    monkeypatch.setattr("provisa.audit.pipeline.write_audit", write_audit)
    monkeypatch.setattr("provisa.observability.request_facts.observe_plan", lambda *_a, **_k: None)
    plan = _pipeline._Plan(route=Route.DIRECT, sql="SELECT 1", source_id="pg", dialect="postgres")
    plan.audit = PendingAudit(
        user_id="u",
        surface="sql",
        role_id="analyst",
        query_text="SELECT 1",
        table_ids=[],
        started=0.0,
        model_stamp=1,
        enforced={"row_cap": None},
    )
    plan.embedded = Folded("SELECT 1", audit=used)
    await _pipeline.finalize_audit(plan, 200, SimpleNamespace())
    (audit,) = written
    enforced = audit.enforced() if callable(audit.enforced) else audit.enforced
    assert enforced["embed"] == used
