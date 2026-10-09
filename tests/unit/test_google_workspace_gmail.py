# Copyright (c) 2026 Kenneth Stott
# Canary: 1eecf25c-cd58-46a4-a20c-782fd78c4340
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading one mailbox through the Gmail API (REQ-1923): paging to the last page, pacing under
Google's units a minute, and what each of Google's refusals does. Google is a stand-in that
answers what the test gives it; no call leaves the process."""

# Requirements: REQ-1923
from __future__ import annotations

import datetime as dt

import httpx
import pytest

from provisa.google_workspace import gmail
from provisa.google_workspace.gmail import (
    Gmail,
    GmailNotFound,
    GmailRefused,
    GmailUnavailable,
    Pacer,
    QuotaExhausted,
    search_query,
)
from provisa.google_workspace.settings import MailSettings

pytestmark = pytest.mark.asyncio

ACCOUNT = "ada@example.test"
ALL_MAIL = MailSettings("full", None, (), None, False)


def _refusal(status: int, reason: str | None = None) -> httpx.Response:
    if reason is None:
        return httpx.Response(status, text="no")
    return httpx.Response(status, json={"error": {"code": status, "errors": [{"reason": reason}]}})


class Google:
    def __init__(self, *answers) -> None:
        self.answers = list(answers)
        self.requests: list[httpx.Request] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        answer = self.answers.pop(0)
        return answer if isinstance(answer, httpx.Response) else httpx.Response(200, json=answer)


class Waits:
    """A clock that only moves when something waits on it."""

    def __init__(self) -> None:
        self.now, self.waited = 1000.0, []

    def clock(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.waited.append(seconds)
        self.now += seconds


def _mailbox(*answers, waits: Waits | None = None, pacer: Pacer | None = None):
    google, waits = Google(*answers), waits or Waits()

    async def token() -> str:
        return "made-up-access-token"

    client = httpx.AsyncClient(transport=httpx.MockTransport(google.handle))
    pacer = pacer or Pacer(clock=waits.clock, sleep=waits.sleep)
    return Gmail(ACCOUNT, token, client, pacer=pacer, sleep=waits.sleep), google, waits


async def _pages(mailbox: Gmail, mail: MailSettings = ALL_MAIL) -> list[list[str]]:
    return [page async for page in mailbox.message_ids(mail)]


class TestCalls:
    async def test_a_call_names_the_mailbox_and_carries_the_token(self):
        mailbox, google, _ = _mailbox({"emailAddress": ACCOUNT, "historyId": "9"})
        assert (await mailbox.profile())["historyId"] == "9"
        (request,) = google.requests
        assert str(request.url) == f"https://gmail.googleapis.com/gmail/v1/users/{ACCOUNT}/profile"
        assert request.headers["authorization"] == "Bearer made-up-access-token"

    async def test_a_message_is_asked_for_whole_or_as_headers_only(self):
        mailbox, google, _ = _mailbox({"id": "m1"}, {"id": "m1"})
        await mailbox.message("m1", ALL_MAIL)
        await mailbox.message("m1", MailSettings("headers", None, (), None, False))
        assert [r.url.params["format"] for r in google.requests] == ["full", "metadata"]
        assert google.requests[0].url.path.endswith("/messages/m1")


class TestListing:
    async def test_every_page_is_read_to_the_last(self):
        mailbox, google, _ = _mailbox(
            {
                "messages": [{"id": "a", "threadId": "t"}, {"id": "b", "threadId": "t"}],
                "nextPageToken": "p2",
            },
            {"messages": [{"id": "c", "threadId": "u"}], "nextPageToken": "p3"},
            {"resultSizeEstimate": 0},
        )
        assert await _pages(mailbox) == [["a", "b"], ["c"]]
        assert [r.url.params.get("pageToken") for r in google.requests] == [None, "p2", "p3"]
        assert {r.url.params["maxResults"] for r in google.requests} == {"500"}

    async def test_an_empty_mailbox_has_no_pages(self):
        mailbox, _, _ = _mailbox({"resultSizeEstimate": 0})
        assert await _pages(mailbox) == []

    async def test_nothing_stated_asks_gmail_for_all_mail(self):
        mailbox, google, _ = _mailbox({})
        await _pages(mailbox)
        assert dict(google.requests[0].url.params) == {"maxResults": "500"}

    async def test_what_narrows_the_mail_is_asked_of_gmail(self):
        mail = MailSettings(
            "full", "from:bo@example.test", ("Clients", "Invoices"), dt.date(2026, 1, 1), True
        )
        labels = {
            "labels": [
                {"id": "Label_1", "name": "Clients"},
                {"id": "Label_2", "name": "Invoices"},
                {"id": "INBOX", "name": "INBOX"},
            ]
        }
        mailbox, google, _ = _mailbox(labels, {})
        await _pages(mailbox, mail)
        listing = google.requests[1].url.params
        assert listing.get_list("labelIds") == ["Label_1", "Label_2"]
        assert listing["includeSpamTrash"] == "true"
        assert listing["q"] == "from:bo@example.test after:1767225600"  # 2026-01-01T00:00:00Z

    async def test_a_label_the_mailbox_does_not_have_is_refused_by_name(self):
        mail = MailSettings("full", None, ("Clients", "Invoises"), None, False)
        mailbox, google, _ = _mailbox({"labels": [{"id": "Label_1", "name": "Clients"}]})
        with pytest.raises(GmailRefused, match="no label named Invoises"):
            await _pages(mailbox, mail)
        assert len(google.requests) == 1  # nothing was listed

    async def test_the_search_is_the_operators_own_and_the_day_is_stated_in_seconds(self):
        assert search_query(ALL_MAIL) is None
        assert search_query(MailSettings("full", "is:unread", (), None, False)) == "is:unread"
        since = MailSettings("full", None, (), dt.date(2025, 12, 31), False)
        assert search_query(since) == "after:1767139200"


class TestPacing:
    async def test_calls_within_the_minutes_units_do_not_wait(self):
        waits = Waits()
        pacer = Pacer(100, clock=waits.clock, sleep=waits.sleep)
        for _ in range(5):
            await pacer.spend(20)
        assert waits.waited == []

    async def test_a_call_past_them_waits_until_the_minute_has_room(self):
        waits = Waits()
        pacer = Pacer(100, clock=waits.clock, sleep=waits.sleep)
        for _ in range(5):
            await pacer.spend(20)
            waits.now += 1
        await pacer.spend(20)
        assert waits.waited == [55.0]  # the first call was 5 seconds ago and frees at 60
        assert waits.now == 1060.0

    async def test_a_call_that_could_never_fit_is_an_error(self):
        with pytest.raises(ValueError, match="never fits"):
            await Pacer(10).spend(20)

    async def test_each_call_is_priced_as_google_prices_it(self):
        spent: list[int] = []

        class Counting(Pacer):
            async def spend(self, units: int) -> None:
                spent.append(units)

        mailbox, _, _ = _mailbox(
            {}, {"labels": []}, {"messages": [{"id": "a"}]}, {"id": "a"}, pacer=Counting()
        )
        await mailbox.profile()
        await mailbox.labels()
        await _pages(mailbox)
        await mailbox.message("a", ALL_MAIL)
        assert spent == [1, 1, 5, 20]
        assert gmail.UNITS_PER_MINUTE == 6000

    async def test_a_call_tried_again_is_paid_for_again(self):
        spent: list[int] = []

        class Counting(Pacer):
            async def spend(self, units: int) -> None:
                spent.append(units)

        mailbox, _, _ = _mailbox(_refusal(429), {"id": "a"}, pacer=Counting())
        await mailbox.message("a", ALL_MAIL)
        assert spent == [20, 20]


class TestRefusals:
    @pytest.mark.parametrize(
        "refusal",
        [_refusal(429), _refusal(403, "rateLimitExceeded"), _refusal(403, "userRateLimitExceeded")],
    )
    async def test_a_rate_refusal_is_tried_again_after_a_wait_that_doubles(self, refusal):
        mailbox, google, waits = _mailbox(refusal, refusal, {"id": "a"})
        assert await mailbox.message("a", ALL_MAIL) == {"id": "a"}
        assert waits.waited == [1.0, 2.0]
        assert len(google.requests) == 3

    async def test_a_rate_refusal_every_time_fails_the_read_with_googles_reason(self):
        refusal = _refusal(403, "userRateLimitExceeded")
        mailbox, google, waits = _mailbox(*[refusal] * 6)
        with pytest.raises(QuotaExhausted) as raised:
            await mailbox.message("a", ALL_MAIL)
        assert raised.value.reason == "userRateLimitExceeded, tried 6 times"
        assert ACCOUNT in str(raised.value) and "messages.get" in str(raised.value)
        assert waits.waited == [1.0, 2.0, 4.0, 8.0, 16.0]
        assert len(google.requests) == 6

    async def test_the_projects_daily_limit_is_not_tried_again(self):
        mailbox, google, waits = _mailbox(_refusal(403, "dailyLimitExceeded"))
        with pytest.raises(QuotaExhausted) as raised:
            await mailbox.message("a", ALL_MAIL)
        assert raised.value.reason == "dailyLimitExceeded"
        assert waits.waited == [] and len(google.requests) == 1

    @pytest.mark.parametrize(
        ("refusal", "reason"),
        [
            (_refusal(403, "domainPolicy"), "domainPolicy"),
            (_refusal(401, "authError"), "authError"),
            (_refusal(400, "failedPrecondition"), "failedPrecondition"),
            (_refusal(403), "HTTP 403"),
        ],
    )
    async def test_a_refusal_that_waiting_does_not_change_is_not_tried_again(self, refusal, reason):
        mailbox, google, waits = _mailbox(refusal)
        with pytest.raises(GmailRefused) as raised:
            await mailbox.profile()
        assert type(raised.value) is GmailRefused and raised.value.reason == reason
        assert waits.waited == [] and len(google.requests) == 1

    async def test_googles_own_failure_is_tried_again_and_then_named(self):
        mailbox, _, waits = _mailbox(_refusal(503, "backendError"), {"id": "a"})
        assert await mailbox.message("a", ALL_MAIL) == {"id": "a"}
        assert waits.waited == [1.0]
        failing, _, _ = _mailbox(*[_refusal(500, "backendError")] * 6)
        with pytest.raises(GmailUnavailable) as raised:
            await failing.message("a", ALL_MAIL)
        assert raised.value.reason == "backendError, tried 6 times"

    async def test_a_message_gone_since_it_was_listed_is_said_to_be_gone(self):
        mailbox, _, _ = _mailbox(_refusal(404, "notFound"))
        with pytest.raises(GmailNotFound):
            await mailbox.message("a", ALL_MAIL)

    async def test_a_page_refused_midway_fails_the_listing_and_is_not_a_short_list(self):
        refusal = _refusal(403, "dailyLimitExceeded")
        mailbox, _, _ = _mailbox({"messages": [{"id": "a"}], "nextPageToken": "p2"}, refusal)
        with pytest.raises(QuotaExhausted):
            await _pages(mailbox)

    async def test_no_refusal_carries_the_token(self):
        mailbox, _, _ = _mailbox(_refusal(401, "authError"))
        with pytest.raises(GmailRefused) as raised:
            await mailbox.profile()
        assert "made-up-access-token" not in str(raised.value)
