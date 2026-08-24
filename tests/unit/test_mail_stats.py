# Copyright (c) 2026 Kenneth Stott
# Canary: 9d4b3a71-52c6-4c0f-8e1d-c7f6a25b3e90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1576: the stats answer "is mail going out" out of the record of real attempts.

Configuration proves nothing -- a correct-looking transport with a rejected key sends nothing --
so what the Email settings page reports is what actually happened, failures included.
"""

from datetime import datetime, timedelta, timezone

import pytest

from provisa.core.database import Database, create_engine_from_url
from provisa.core.mail_stats import MailAttempt, record, stats
from provisa.core.schema_admin import mail_events


@pytest.fixture
async def db():
    pool = Database(create_engine_from_url("sqlite+aiosqlite:///:memory:"), name="mail-stats-test")
    async with pool.engine.begin() as conn:
        await conn.run_sync(mail_events.create)
    yield pool
    await pool.close()


async def _seed(pool, *, sent_at, succeeded, error=None, kind="invite", recipient="a@b.test"):
    async with pool.acquire() as conn:
        await conn.execute_core(
            mail_events.insert().values(
                sent_at=sent_at,
                provider="smtp",
                kind=kind,
                recipient=recipient,
                succeeded=succeeded,
                error=error,
            )
        )


class TestTheCounts:
    async def test_an_empty_record_reports_zero_rather_than_nothing(self, db):
        """A deployment that has never sent must read as "nothing sent", not as a broken panel."""
        s = await stats(db)
        assert s["total"] == {"attempted": 0, "delivered": 0, "failed": 0}
        assert s["windows"]["day"]["attempted"] == 0
        assert s["last_success"] is None and s["last_failure"] is None
        assert s["recent"] == []

    async def test_attempts_are_counted_by_window(self, db):
        now = datetime.now(timezone.utc)
        await _seed(db, sent_at=now - timedelta(hours=2), succeeded=True)
        await _seed(db, sent_at=now - timedelta(hours=3), succeeded=False, error="550 no")
        await _seed(db, sent_at=now - timedelta(days=3), succeeded=True)
        await _seed(db, sent_at=now - timedelta(days=30), succeeded=True)

        s = await stats(db)
        assert s["windows"]["day"] == {"attempted": 2, "delivered": 1, "failed": 1}
        assert s["windows"]["week"] == {"attempted": 3, "delivered": 2, "failed": 1}
        assert s["total"] == {"attempted": 4, "delivered": 3, "failed": 1}


class TestTheLastFailure:
    async def test_the_transport_words_are_kept_verbatim(self, db):
        """An operator fixes an unverified sender domain from the words the provider used."""
        now = datetime.now(timezone.utc)
        await _seed(db, sent_at=now - timedelta(minutes=5), succeeded=False, error="550 5.7.1 no")
        s = await stats(db)
        assert s["last_failure"]["error"] == "550 5.7.1 no"
        assert s["last_failure"]["succeeded"] is False

    async def test_a_later_success_does_not_erase_the_last_failure(self, db):
        """Both are reported: "it worked at 10:02" and "it failed at 10:01" are different facts,
        and hiding the second is how a half-broken transport looks healthy."""
        now = datetime.now(timezone.utc)
        await _seed(db, sent_at=now - timedelta(minutes=9), succeeded=False, error="timeout")
        await _seed(db, sent_at=now - timedelta(minutes=1), succeeded=True)
        s = await stats(db)
        assert s["last_failure"]["error"] == "timeout"
        assert s["last_success"] is not None
        assert s["last_success"]["succeeded"] is True


class TestRecording:
    async def test_a_recorded_attempt_shows_up_in_the_stats(self, db):
        await record(
            db,
            MailAttempt(
                provider="resend",
                kind="test",
                recipient="ops@example.test",
                succeeded=False,
                error="422 domain not verified",
                requested_by="root",
            ),
        )
        s = await stats(db)
        assert s["total"]["failed"] == 1
        assert s["recent"][0] == {
            "sent_at": s["recent"][0]["sent_at"],
            "provider": "resend",
            "kind": "test",
            "recipient": "ops@example.test",
            "org_id": None,
            "succeeded": False,
            "error": "422 domain not verified",
            "requested_by": "root",
        }
        assert s["recent"][0]["sent_at"] is not None

    async def test_recording_never_fails_the_send_it_observes(self):
        """A registry the writer cannot reach must not turn a delivered message into a reported
        failure, so ``record`` absorbs its own storage error."""
        pool = Database(
            create_engine_from_url("sqlite+aiosqlite:///:memory:"), name="mail-stats-no-table"
        )
        try:
            await record(
                pool,
                MailAttempt(provider="smtp", kind="invite", recipient="a@b.test", succeeded=True),
            )
        finally:
            await pool.close()

    async def test_the_most_recent_attempts_come_back_newest_first(self, db):
        now = datetime.now(timezone.utc)
        for i in range(3):
            await _seed(
                db,
                sent_at=now - timedelta(minutes=i),
                succeeded=True,
                recipient=f"user{i}@example.test",
            )
        s = await stats(db)
        assert [r["recipient"] for r in s["recent"]] == [
            "user0@example.test",
            "user1@example.test",
            "user2@example.test",
        ]
