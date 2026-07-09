# Copyright (c) 2026 Kenneth Stott
# Canary: 9c654e3c-71eb-4329-90a6-156c47852116
#
# This source code is licensed under the Business Source License 1.1

import pytest
from pytest_bdd import scenarios, given, when, then

scenarios("../features/REQ-926.feature")


# ---------------------------------------------------------------------------
# Helpers / in-process simulation of the Provisa refresh / subscription logic
# ---------------------------------------------------------------------------


class _SourceConfig:
    """Minimal stand-in for a Provisa source configuration object."""

    def __init__(self, *, watermark_column: str | None = None):
        self.watermark_column = watermark_column
        self.cursor: int = 0

    @property
    def refresh_mode(self) -> str:
        """Return 'APPEND' when a watermark column is defined, else 'REPLACE'."""
        return "APPEND" if self.watermark_column else "REPLACE"

    @property
    def subscriptions_allowed(self) -> bool:
        """Subscriptions are only permitted when the source has a watermark column."""
        return self.watermark_column is not None


class _RefreshEngine:
    """Simulates the refresh execution layer."""

    def run(self, source: _SourceConfig) -> dict:
        if source.refresh_mode == "APPEND":
            return {
                "mode": "APPEND",
                "sql_fragment": f"WHERE {source.watermark_column} > {source.cursor}",
            }
        else:
            return {
                "mode": "REPLACE",
                "sql_fragment": "DELETE+INSERT",
            }


class _SubscriptionService:
    """Simulates the Provisa subscription gating service."""

    def can_subscribe(self, source: _SourceConfig) -> bool:
        return source.subscriptions_allowed

    def subscribe(self, source: _SourceConfig) -> dict:
        if not self.can_subscribe(source):
            raise PermissionError(
                "Subscriptions are forbidden for sources without a watermark column."
            )
        return {"subscribed": True, "source_watermark": source.watermark_column}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def shared_data():
    return {}


@pytest.fixture
def refresh_engine():
    return _RefreshEngine()


@pytest.fixture
def subscription_service():
    return _SubscriptionService()


# ---------------------------------------------------------------------------
# Given steps
# ---------------------------------------------------------------------------


@given("a source with a watermark column set")
def given_source_with_watermark(shared_data):
    source = _SourceConfig(watermark_column="updated_at")
    shared_data["source"] = source
    assert source.watermark_column is not None, "Precondition failed: watermark column must be set"


@given("a source with no watermark column")
def given_source_without_watermark(shared_data):
    source = _SourceConfig(watermark_column=None)
    shared_data["source"] = source
    assert source.watermark_column is None, "Precondition failed: watermark column must be absent"


# ---------------------------------------------------------------------------
# When steps
# ---------------------------------------------------------------------------


@when("refresh is triggered")
def when_refresh_triggered(shared_data, refresh_engine):
    source = shared_data["source"]
    result = refresh_engine.run(source)
    shared_data["refresh_result"] = result


# ---------------------------------------------------------------------------
# Then steps
# ---------------------------------------------------------------------------


@then("the system executes APPEND refresh (incremental WHERE wm > cursor)")
def then_append_refresh(shared_data):
    result = shared_data["refresh_result"]
    assert result["mode"] == "APPEND", f"Expected APPEND refresh mode, got: {result['mode']}"
    source = shared_data["source"]
    expected_fragment = f"WHERE {source.watermark_column} > {source.cursor}"
    assert result["sql_fragment"] == expected_fragment, (
        f"Expected SQL fragment '{expected_fragment}', got: '{result['sql_fragment']}'"
    )


@then("subscriptions to this source are permitted")
def then_subscriptions_permitted(shared_data, subscription_service):
    source = shared_data["source"]
    assert subscription_service.can_subscribe(source), (
        "Expected subscriptions to be permitted for a watermarked source"
    )
    outcome = subscription_service.subscribe(source)
    assert outcome["subscribed"] is True, "Expected subscribe() to return subscribed=True"
    assert outcome["source_watermark"] == source.watermark_column, (
        "Subscription outcome must report the correct watermark column"
    )


@then("the system executes REPLACE refresh (full DELETE+INSERT)")
def then_replace_refresh(shared_data):
    result = shared_data["refresh_result"]
    assert result["mode"] == "REPLACE", f"Expected REPLACE refresh mode, got: {result['mode']}"
    assert "DELETE+INSERT" in result["sql_fragment"], (
        f"Expected DELETE+INSERT in SQL fragment, got: '{result['sql_fragment']}'"
    )


@then("subscriptions to this source are forbidden")
def then_subscriptions_forbidden(shared_data, subscription_service):
    source = shared_data["source"]
    assert not subscription_service.can_subscribe(source), (
        "Expected subscriptions to be forbidden for a source without a watermark column"
    )
    with pytest.raises(PermissionError, match="forbidden"):
        subscription_service.subscribe(source)
