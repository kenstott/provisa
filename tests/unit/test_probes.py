# Copyright (c) 2026 Kenneth Stott
# Canary: ccf671af-c6d4-45b8-8efd-718e8f0f949e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-982: probe_type taxonomy — capability matrix, shape implication, defaults, SQL transports."""

from __future__ import annotations

import pytest

from provisa.events import probes


# -- capability matrix -----------------------------------------------------------------------------


def test_sql_source_supports_all_probe_types():
    assert probes.probe_capabilities("postgresql") == probes.VALID_PROBE_TYPES


def test_file_source_only_hash_or_none():
    assert probes.probe_capabilities("csv") == frozenset({probes.HASH, probes.NONE})


def test_http_api_supports_all():
    assert probes.probe_capabilities("openapi") == probes.VALID_PROBE_TYPES


def test_streaming_source_has_no_probe_capability():
    assert probes.probe_capabilities("kafka") == frozenset()


# -- shape implication -----------------------------------------------------------------------------


def test_watermark_implies_append_others_replace():
    assert probes.probe_shape(probes.WATERMARK) == "append"
    assert probes.probe_shape(probes.HASH) == "replace"
    assert probes.probe_shape(probes.COUNT) == "replace"
    assert probes.probe_shape(probes.NONE) == "replace"


def test_probe_shape_rejects_unknown():
    with pytest.raises(ValueError, match="invalid probe_type"):
        probes.probe_shape("bogus")


# -- resolve_probe_type: validation + defaults -----------------------------------------------------


def test_ttl_forces_none():
    assert (
        probes.resolve_probe_type(
            None, source_type="postgresql", change_signal="ttl", has_watermark=True
        )
        == probes.NONE
    )


def test_ttl_rejects_explicit_non_none():
    with pytest.raises(ValueError, match="cadence-only"):
        probes.resolve_probe_type(
            probes.WATERMARK, source_type="postgresql", change_signal="ttl", has_watermark=True
        )


def test_default_sql_with_watermark_is_watermark():
    assert (
        probes.resolve_probe_type(
            None, source_type="postgresql", change_signal="probe", has_watermark=True
        )
        == probes.WATERMARK
    )


def test_default_sql_without_watermark_is_count():
    assert (
        probes.resolve_probe_type(
            None, source_type="postgresql", change_signal="probe", has_watermark=False
        )
        == probes.COUNT
    )


def test_default_api_is_hash():
    assert (
        probes.resolve_probe_type(
            None, source_type="openapi", change_signal="ttl_probe", has_watermark=False
        )
        == probes.HASH
    )


def test_default_file_is_hash():
    assert (
        probes.resolve_probe_type(
            None, source_type="csv", change_signal="probe", has_watermark=False
        )
        == probes.HASH
    )


def test_explicit_type_outside_capability_is_rejected():
    # a file source cannot watermark
    with pytest.raises(ValueError, match="not supported by source type"):
        probes.resolve_probe_type(
            probes.WATERMARK, source_type="csv", change_signal="probe", has_watermark=False
        )


def test_push_source_probe_type_is_inert():
    assert (
        probes.resolve_probe_type(
            None, source_type="postgresql", change_signal="kafka", has_watermark=True
        )
        == probes.NONE
    )


# -- SQL transports --------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_watermark_token_is_max():
    captured = {}

    async def scalar(sql):
        captured["sql"] = sql
        return 42

    token = await probes.sql_watermark_token(scalar, '"cat"."s"."t"', "updated_at")
    assert token == "42"
    assert 'MAX("updated_at")' in captured["sql"] and 'FROM "cat"."s"."t"' in captured["sql"]


@pytest.mark.asyncio
async def test_count_token_is_count():
    async def scalar(sql):
        assert "COUNT(*)" in sql
        return 7

    assert await probes.sql_count_token(scalar, '"cat"."s"."t"') == "7"


@pytest.mark.asyncio
async def test_watermark_token_none_when_empty():
    async def scalar(sql):
        return None

    assert await probes.sql_watermark_token(scalar, "ref", "wm") is None


# -- build_probe: composition + degrade ------------------------------------------------------------


@pytest.mark.asyncio
async def test_build_probe_watermark_reads_scalar():
    async def scalar(sql):
        return "2026-07-10"

    probe = probes.build_probe(
        probes.WATERMARK, query_scalar=scalar, ref="ref", watermark_column="wm"
    )
    assert await probe() == "2026-07-10"


@pytest.mark.asyncio
async def test_build_probe_hash_on_sql_degrades_to_none():
    async def scalar(sql):  # would be called only by watermark/count
        raise AssertionError("hash probe on SQL must not query")

    probe = probes.build_probe(probes.HASH, query_scalar=scalar, ref="ref")
    assert await probe() is None  # degrade → TTL cadence; REQ-981 output gate covers correctness


@pytest.mark.asyncio
async def test_build_probe_none_never_queries():
    probe = probes.build_probe(probes.NONE)
    assert await probe() is None


@pytest.mark.asyncio
async def test_build_probe_watermark_without_scalar_degrades():
    probe = probes.build_probe(probes.WATERMARK, query_scalar=None, ref=None, watermark_column="wm")
    assert await probe() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("probe_type", sorted(probes.VALID_PROBE_TYPES))
@pytest.mark.parametrize("watermark", [None, "updated_at"])
@pytest.mark.parametrize("sentinel", [None, "/data/_SUCCESS"])
@pytest.mark.parametrize("glob", [(None, None), ("/data", "*.csv")])
@pytest.mark.parametrize("from_loader", [False, True])
async def test_can_probe_says_what_build_probe_can_ask_for(
    probe_type, watermark, sentinel, glob, from_loader, monkeypatch
):
    """REQ-855: a replica follows its probe only when the probe has a token to ask for. The
    predicate and the builder must agree, case by case: a transport that asks is one that is
    not the builder's own no-token answer."""
    from provisa.events import sentinel_probe

    asked: list[str] = []

    async def scalar(_sql: str):
        asked.append("sql")
        return 1

    async def glob_token(_path: str, _glob: str, _storage_options):
        asked.append("glob")
        return "g"

    async def sentinel_token():
        asked.append("sentinel")
        return "s"

    async def loader_token():
        asked.append("loader")
        return "l"

    monkeypatch.setattr(probes, "glob_token", glob_token)
    monkeypatch.setattr(sentinel_probe, "build_sentinel_probe", lambda _path: sentinel_token)
    source_path, file_glob = glob
    given = {
        "watermark_column": watermark,
        "sentinel_path": sentinel,
        "source_path": source_path,
        "file_glob": file_glob,
    }
    transport = probes.build_probe(
        probe_type,
        query_scalar=scalar,
        ref='"s"."t"',
        loader_probe=loader_token if from_loader else None,
        **given,
    )
    token = await transport()
    assert probes.can_probe(probe_type, loader_probe=from_loader, **given) is bool(asked)
    assert (token is not None) is bool(asked)


@pytest.mark.parametrize("source_type", ["google_workspace", "microsoft_365"])
def test_a_mail_source_probes_by_its_providers_token_or_not_at_all(source_type):
    """A mailbox has no cursor to watermark by and no count that sees an edit."""
    assert probes.probe_capabilities(source_type) == frozenset({probes.HASH, probes.NONE})
    for signal in ("probe", "ttl_probe"):
        resolved = probes.resolve_probe_type(
            None, source_type=source_type, change_signal=signal, has_watermark=False
        )
        assert resolved == probes.HASH
    with pytest.raises(ValueError, match="not supported by source type"):
        probes.resolve_probe_type(
            "count", source_type=source_type, change_signal="probe", has_watermark=False
        )


@pytest.mark.asyncio
async def test_a_hash_probe_asks_the_sources_own_adapter_when_it_has_a_probe():
    async def from_gmail() -> str:
        return "history-token"

    transport = probes.build_probe(probes.HASH, loader_probe=from_gmail)
    assert await transport() == "history-token"
    assert await probes.build_probe(probes.HASH)() is None  # nothing to ask: no token


def test_loader_probe_is_what_the_sources_adapter_gives_for_the_table():
    from types import SimpleNamespace

    async def token() -> str:
        return "t"

    def loader(*_a):
        return []

    loader.freshness_probe = lambda source, table: token if table.table_name == "messages" else None
    loaders = {"google_workspace": loader, "rss": lambda *_a: []}
    mail = SimpleNamespace(type="google_workspace")
    assert probes.loader_probe(loaders, mail, SimpleNamespace(table_name="messages")) is token
    assert probes.loader_probe(loaders, mail, SimpleNamespace(table_name="events")) is None
    assert probes.loader_probe(loaders, SimpleNamespace(type="rss"), None) is None
    assert probes.loader_probe(loaders, SimpleNamespace(type="postgresql"), None) is None
