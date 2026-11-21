# Copyright (c) 2026 Kenneth Stott
# Canary: 31577376-c354-445d-8eb8-1feb9f598231
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where the last completed read of a mail source reached (REQ-1923): one token for each
(source, mailbox, scope, kind), kept in the org's state store and gone with its source."""

from __future__ import annotations

import pytest
from sqlalchemy import insert, select

from provisa.core import env_classes, store_sides
from provisa.core.schema_org import mail_sync_state as table
from provisa.core.schema_org import sources
from provisa.federation import mail_sync_state as kept

pytestmark = pytest.mark.unit

A, B = "megan@contoso.com", "sam@contoso.com"


@pytest.fixture
async def db(control_plane):
    made = control_plane.org()
    yield made
    await made.close()


async def test_what_a_read_reached_replaces_what_was_kept_for_the_source(db):
    link = "https://graph.microsoft.com/v1.0/users/x/mailFolders/f/messages/delta?$deltatoken=" + (
        "t" * 4000
    )
    async with db.acquire() as conn:
        assert await kept.read_all(conn, "mail") == {}
        await kept.replace(
            conn,
            "mail",
            {(A, "f1", kept.DELTA): link, (A, "", kept.DELTA): "l0", (B, "", kept.PROBE): "h7"},
        )
        await kept.replace(conn, "other", {(A, "", kept.PROBE): "h1"})
        assert await kept.read_all(conn, "mail") == {
            (A, "f1", kept.DELTA): link,  # a link is long, and kept whole
            (A, "", kept.DELTA): "l0",
            (B, "", kept.PROBE): "h7",
        }
        # The next completed read no longer saw mailbox B, nor folder f1 of A.
        await kept.replace(conn, "mail", {(A, "f2", kept.DELTA): "l2", (A, "", kept.DELTA): "l9"})
        assert await kept.read_all(conn, "mail") == {
            (A, "f2", kept.DELTA): "l2",
            (A, "", kept.DELTA): "l9",
        }
        assert await kept.read_all(conn, "other") == {(A, "", kept.PROBE): "h1"}
        await kept.forget(conn, "mail")
        assert await kept.read_all(conn, "mail") == {}


async def test_a_kind_of_token_nobody_reads_is_refused_and_nothing_is_changed(db):
    async with db.acquire() as conn:
        await kept.replace(conn, "mail", {(A, "", kept.DELTA): "l0"})
        with pytest.raises(ValueError, match="no such kind of token: cursor"):
            await kept.replace(conn, "mail", {(A, "", "cursor"): "x"})
        assert await kept.read_all(conn, "mail") == {(A, "", kept.DELTA): "l0"}


async def test_what_was_kept_goes_with_its_source(db):
    """REQ-2248 (amended 2026-11-15): deleting the source retires it, and what was kept moves with
    it to the key it is retired under, so a new source at its id starts from nothing."""
    from provisa.core.repositories import source as source_repo

    async with db.acquire() as conn:
        await conn.execute_core(insert(sources).values(id="mail", type="microsoft_365"))
        await kept.replace(conn, "mail", {(A, "", kept.DELTA): "l0"})
        assert await source_repo.delete(conn, "mail") is True
        [retired] = await source_repo.list_all(conn, retired=True)
        rows = (await conn.execute_core(select(table.c.source_id))).fetchall()
        assert rows == [(retired["id"],)]


def test_it_is_state_of_one_environment_and_region_not_part_of_the_model():
    assert "mail_sync_state" in store_sides.STATE
    assert "mail_sync_state" in env_classes.NEVER_RUNTIME
    assert "mail_sync_state" not in env_classes.CARRIED


async def test_every_table_a_source_is_building_says_what_its_read_is_waiting_out(db):
    """One read feeds every table of the source it builds: while its service is throttling
    it, each running build says so, and stops saying so when the wait is over or it ends."""
    from datetime import UTC, datetime

    from provisa.federation import replica_state
    from provisa.federation.replica_state import RetryPolicy

    now = datetime(2026, 10, 10, 12, tzinfo=UTC)
    building = [("mail", "default", "messages"), ("mail", "default", "threads")]
    idle = ("mail", "default", "folders")
    other = ("crm", "public", "accounts")
    async with db.acquire() as conn:
        for key in (*building, idle, other):
            await replica_state.request_build(conn, key, replica_state.REASON_MODEL)
        for key in (*building, other):
            await replica_state.claim(conn, key, holder="h:1", now=now, retry=RetryPolicy(60, 3600))
        await replica_state.record_throttled(conn, "mail", seconds=87, by="Microsoft", now=now)
        for key in building:
            assert (await replica_state.read(conn, key)).build_wait == {
                "by": "Microsoft",
                "seconds": 87,
                "until": "2026-10-10T12:01:27+00:00",
            }
        assert (await replica_state.read(conn, idle)).build_wait is None  # not being built
        assert (await replica_state.read(conn, other)).build_wait is None  # another source's
        await replica_state.record_throttled(conn, "mail", seconds=0, by="Microsoft", now=now)
        assert (await replica_state.read(conn, building[0])).build_wait is None
        await replica_state.record_throttled(conn, "mail", seconds=30, by="Microsoft", now=now)
        await replica_state.record_failed(conn, building[1], error="stopped", now=now)
        assert (await replica_state.read(conn, building[1])).build_wait is None


# --- a read that lands a part at a time keeps each mailbox's place as it goes ---


async def test_a_parts_place_is_kept_beside_what_the_mailbox_already_had(db):
    async with db.acquire() as conn:
        await kept.keep_links(conn, "mail", A, {"inbox": "l1"})
        await kept.keep_links(conn, "mail", "Megan@Contoso.com", {"sent": "l2", "inbox": "l1b"})
        await kept.keep_links(conn, "mail", B, {"": kept_left_out()}, kind=kept.PROBE)
        assert await kept.links_of(conn, "mail") == {A: {"inbox": "l1b", "sent": "l2"}}
        assert (await kept.read_all(conn, "mail"))[(B, "", kept.PROBE)] == kept_left_out()
        with pytest.raises(ValueError, match="no kind"):
            await kept.keep_links(conn, "mail", A, {"x": "y"}, kind="grant")


def kept_left_out() -> str:
    return "left_out"


async def test_a_mailbox_read_again_from_its_start_keeps_nothing_and_its_approval_stands(db):
    async with db.acquire() as conn:
        await kept.keep_links(conn, "mail", A, {"inbox": "l1", "": "l0"})
        await kept.keep_links(conn, "mail", B, {"inbox": "l9"})
        await kept.keep_grant(conn, "mail", A, "the-owners-approval")
        await kept.forget_account(conn, "mail", "MEGAN@contoso.com")
        assert await kept.links_of(conn, "mail") == {B: {"inbox": "l9"}}
        assert await kept.grant(conn, "mail", A) == "the-owners-approval"


async def test_a_first_build_is_a_replica_once_its_first_part_has_landed(db):
    """The record says there is a copy to read in this store while the build goes on, and how
    many of the source's mailboxes have been read to their end; the completion ends both."""
    from datetime import UTC, datetime

    from provisa.federation import replica_state
    from provisa.federation.replica_state import RetryPolicy

    key = ("mail", "default", "messages")
    now = datetime.now(UTC)
    async with db.acquire() as conn:
        await replica_state.request_build(conn, key, replica_state.REASON_MODEL)
        await replica_state.claim(conn, key, holder="h:1", now=now, retry=RetryPolicy(60, 3600))
        assert (await replica_state.read(conn, key)).exists_in("store-a") is False
        made = await replica_state.record_partial(
            conn,
            key,
            store="store-a",
            now=now,
            definition_hash="d1",
            built_columns=[["id", "text"]],
        )
        again = await replica_state.record_partial(
            conn, key, store="store-a", now=now, definition_hash="d1", built_columns=[]
        )
        await replica_state.record_parts(conn, key, done=1, total=40, unit="mailboxes", first=True)
        record = await replica_state.read(conn, key)
        assert (made, again) == (True, False)  # said once
        assert record.exists_in("store-a") and record.build_state == "building"
        assert record.partial and record.build_progress == {
            "done": 1,
            "total": 40,
            "unit": "mailboxes",
            "first": True,
        }
        assert record.built_columns == [["id", "text"]]
        await replica_state.record_completed(
            conn,
            key,
            rows_copied=10,
            method="apply_units",
            content_hash=None,
            store="store-a",
            next_refresh_at=None,
            now=now,
        )
        done = await replica_state.read(conn, key)
        assert done.build_progress is None and not done.partial and done.build_state == "idle"


async def test_a_later_pass_over_a_replica_that_exists_is_not_partial(db):
    from datetime import UTC, datetime

    from provisa.federation import replica_state
    from provisa.federation.replica_state import RetryPolicy

    key = ("mail", "default", "messages")
    now = datetime.now(UTC)
    retry = RetryPolicy(60, 3600)
    async with db.acquire() as conn:
        await replica_state.request_build(conn, key, replica_state.REASON_MODEL)
        await replica_state.claim(conn, key, holder="h:1", now=now, retry=retry)
        await replica_state.record_completed(
            conn,
            key,
            rows_copied=10,
            method="apply_units",
            content_hash=None,
            store="store-a",
            next_refresh_at=None,
            now=now,
        )
        await replica_state.request_build(conn, key, replica_state.REASON_REFRESH)
        await replica_state.claim(conn, key, holder="h:1", now=now, retry=retry)
        assert (
            await replica_state.record_partial(
                conn, key, store="store-a", now=now, definition_hash="d", built_columns=[]
            )
            is False
        )
        await replica_state.record_parts(conn, key, done=2, total=40, unit="mailboxes", first=False)
        assert (await replica_state.read(conn, key)).partial is False
