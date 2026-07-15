# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-981 output gate + REQ-982 input probe axis — the two ends that bracket the event loop.

981 is the OUTPUT gate (post-land): a replace whose landed content is byte-identical to the prior
land neither re-posts nor re-writes; append/delta ripples are never suppressed (new rows by
definition). 982 is the INPUT axis (pre-produce): probe_type authoritatively selects the detection
transport AND the landing shape (watermark → append, hash/count/none → replace), superseding the old
watermark_column-presence heuristic; a watermark type without a watermark_column fails loud.
"""

from __future__ import annotations

import pytest

from provisa.events import handlers, probes
from provisa.events.content_hash import content_hash
from provisa.events.handlers import make_mv_generate, make_source_land
from provisa.federation import store_writer

_COLS = [("id", "bigint"), ("status", "text")]


def _dsn(tmp_path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'store.db'}"


# -- REQ-981: content-hash output gate -------------------------------------------------------------


@pytest.mark.asyncio
async def test_unchanged_replace_land_skips_write_and_repost(tmp_path, monkeypatch):
    rows = [{"id": 2, "status": "b"}, {"id": 1, "status": "a"}]

    async def fetch(pending):
        return rows

    land = make_source_land(
        _dsn(tmp_path),
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl",
        watermark_column=None,
        pk_columns=["id"],
        fetch=fetch,
        probe_type="none",  # → replace
    )
    prior = content_hash(rows, ["id"])
    writes = 0
    real_land = store_writer.land

    async def counting_land(*args, **kwargs):
        nonlocal writes
        writes += 1
        return await real_land(*args, **kwargs)

    monkeypatch.setattr(handlers.store_writer, "land", counting_land)
    result = await land([{"e": 1}], prior_hash=prior)
    assert result is None  # unchanged → no re-post
    assert writes == 0  # REQ-981: the redundant store write is skipped


@pytest.mark.asyncio
async def test_changed_replace_land_posts_and_persists_new_hash(tmp_path):
    rows = [{"id": 1, "status": "a"}]

    async def fetch(pending):
        return rows

    land = make_source_land(
        _dsn(tmp_path),
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl",
        watermark_column=None,
        pk_columns=["id"],
        fetch=fetch,
        probe_type="none",
    )
    result = await land([{"e": 1}], prior_hash="stale-digest")
    assert result is not None
    event_type, _payload, digest = result
    assert event_type == "replace"
    assert digest == content_hash(rows, ["id"]) != "stale-digest"


def test_content_hash_is_pk_order_stable():
    a = [{"id": 1, "v": "x"}, {"id": 2, "v": "y"}]
    b = [{"id": 2, "v": "y"}, {"id": 1, "v": "x"}]  # reordered rows, same content
    assert content_hash(a, ["id"]) == content_hash(b, ["id"])
    # a genuine change flips the digest
    assert content_hash([{"id": 1, "v": "z"}], ["id"]) != content_hash(a, ["id"])


@pytest.mark.asyncio
async def test_append_shape_is_not_gated(tmp_path):
    # REQ-981: append/delta landings carry no content hash and are never suppressed by the gate,
    # even when prior_hash happens to equal the hash of the fetched rows.
    rows = [{"id": 1, "status": "a"}]

    async def fetch(pending):
        return rows

    land = make_source_land(
        _dsn(tmp_path),
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl_probe",
        watermark_column="updated_at",
        pk_columns=["id"],
        fetch=fetch,
        probe_type="watermark",  # → append
    )
    result = await land([{"e": 1}], prior_hash=content_hash(rows, ["id"]))
    assert result is not None
    event_type, _payload, digest = result
    assert event_type == "append"
    assert digest is None  # append → no content hash → gate never engages


@pytest.mark.asyncio
async def test_mv_generate_gates_identical_recompute(tmp_path):
    async def run_query():
        return [{"id": 7, "status": "agg"}]

    generate = make_mv_generate(
        _dsn(tmp_path), schema="", table="mv_daily", columns=_COLS, run_query=run_query
    )
    _et, _p, digest = await generate([{"e": 1}], prior_hash=None)
    assert await generate([{"e": 2}], prior_hash=digest) is None  # unchanged recompute → no ripple


# -- REQ-982: probe_type input axis ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("probe_type", "shape"),
    [("watermark", "append"), ("hash", "replace"), ("count", "replace"), ("none", "replace")],
)
def test_probe_type_implies_landing_shape(probe_type, shape):
    assert probes.probe_shape(probe_type) == shape


@pytest.mark.asyncio
async def test_probe_type_overrides_old_watermark_heuristic(tmp_path):
    # A watermark_column is present, but probe_type=none is authoritative → REPLACE, not the old
    # heuristic's APPEND. Landing shape follows probe_type, and a content hash is carried (replace).
    rows = [{"id": 1, "status": "a"}]

    async def fetch(pending):
        return rows

    land = make_source_land(
        _dsn(tmp_path),
        schema="",
        table="orders",
        columns=_COLS,
        change_signal="ttl",
        watermark_column="updated_at",  # would have implied append under the old heuristic
        pk_columns=["id"],
        fetch=fetch,
        probe_type="none",
    )
    event_type, _payload, digest = await land([{"e": 1}], prior_hash=None)
    assert event_type == "replace"
    assert digest == content_hash(rows, ["id"])


def test_watermark_type_without_watermark_column_fails_loud_at_build(tmp_path):
    async def fetch(pending):
        return []

    with pytest.raises(ValueError, match="requires a watermark_column"):
        make_source_land(
            _dsn(tmp_path),
            schema="",
            table="orders",
            columns=_COLS,
            change_signal="probe",
            watermark_column=None,
            pk_columns=["id"],
            fetch=fetch,
            probe_type="watermark",
        )


def test_watermark_type_without_watermark_column_fails_loud_at_config():
    with pytest.raises(ValueError, match="requires a watermark_column"):
        probes.resolve_probe_type(
            probes.WATERMARK,
            source_type="postgresql",
            change_signal="probe",
            has_watermark=False,
        )


@pytest.mark.asyncio
async def test_probe_transport_selection_by_type():
    captured: dict[str, str] = {}

    async def scalar(sql):
        captured["sql"] = sql
        return 5

    wm = probes.build_probe(probes.WATERMARK, query_scalar=scalar, ref="ref", watermark_column="wm")
    assert await wm() == "5"
    assert 'MAX("wm")' in captured["sql"]
    cnt = probes.build_probe(probes.COUNT, query_scalar=scalar, ref="ref")
    assert await cnt() == "5"
    assert "COUNT(*)" in captured["sql"]
    # hash on SQL and none never query → None token (TTL degrade; REQ-981 output gate covers it)
    assert await probes.build_probe(probes.HASH)() is None
    assert await probes.build_probe(probes.NONE)() is None
