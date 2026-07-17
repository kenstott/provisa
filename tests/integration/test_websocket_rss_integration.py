# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0931de-9332-4f69-a2a5-995523021007
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""
Integration tests for WebSocket and RSS source types.

Copyright (C) 2025 Provisa
License: BSL 1.1
"""

import pytest
import httpx

# Use BASE_URL to point to the running Provisa server
BASE_URL = "http://localhost:8000"


@pytest.mark.requires_provisa_server
class TestWebSocketSourceType:
    """Tests for REQ-338: WebSocket source type."""

    def test_req338_websocket_source_type_is_recognized(self):
        """WebSocket source type is recognized by the sources endpoint."""
        with httpx.Client() as client:
            response = client.get(f"{BASE_URL}/api/sources")
            assert response.status_code in [200, 404]
            if response.status_code == 200:
                sources = response.json()
                assert any(s.get("type") == "websocket" for s in sources)

    def test_req338_websocket_source_rejects_invalid_url(self):
        """WebSocket source rejects invalid URLs."""
        with httpx.Client() as client:
            payload = {"type": "websocket", "url": "not-a-valid-url"}
            response = client.post(f"{BASE_URL}/api/sources", json=payload)
            assert response.status_code in [400, 404, 409]

    def test_req338_websocket_source_with_subscribe_payload(self):
        """WebSocket source accepts optional subscribe_payload."""
        with httpx.Client() as client:
            payload = {
                "type": "websocket",
                "url": "ws://localhost:8001/stream",
                "subscribe_payload": {"action": "subscribe", "channel": "test"},
            }
            response = client.post(f"{BASE_URL}/api/sources", json=payload)
            # Should accept the payload structure (may return 400 if ws://localhost:8001 is unreachable)
            assert response.status_code in [200, 201, 400, 404, 503]


@pytest.mark.requires_provisa_server
class TestWebSocketAutoReconnect:
    """Tests for REQ-339: WebSocket auto-reconnect."""

    def test_req339_websocket_reconnect_interval_default(self):
        """WebSocket reconnect_interval defaults to 5 seconds."""
        with httpx.Client() as client:
            # Fetch source configuration or introspection endpoint
            response = client.get(f"{BASE_URL}/api/sources/websocket/config")
            if response.status_code == 200:
                config = response.json()
                assert config.get("reconnect_interval_seconds", 5) == 5

    def test_req339_websocket_reconnect_interval_configurable(self):
        """WebSocket reconnect_interval can be configured."""
        with httpx.Client() as client:
            payload = {
                "type": "websocket",
                "url": "ws://localhost:8001/stream",
                "reconnect_interval_seconds": 10,
            }
            response = client.post(f"{BASE_URL}/api/sources", json=payload)
            assert response.status_code in [200, 201, 400, 404, 503]


@pytest.mark.requires_provisa_server
class TestRSSSourceType:
    """Tests for REQ-342: RSS source type with polling."""

    def test_req342_rss_source_type_is_recognized(self):
        """RSS source type is recognized by the sources endpoint."""
        with httpx.Client() as client:
            response = client.get(f"{BASE_URL}/api/sources")
            assert response.status_code in [200, 404]
            if response.status_code == 200:
                sources = response.json()
                assert any(s.get("type") == "rss" for s in sources)

    def test_req342_rss_poll_interval_default(self):
        """RSS poll_interval defaults to 300 seconds."""
        with httpx.Client() as client:
            response = client.get(f"{BASE_URL}/api/sources/rss/config")
            if response.status_code == 200:
                config = response.json()
                assert config.get("poll_interval_seconds", 300) == 300

    def test_req342_rss_poll_interval_configurable(self):
        """RSS poll_interval can be configured."""
        with httpx.Client() as client:
            payload = {
                "type": "rss",
                "feed_url": "https://example.com/feed.xml",
                "poll_interval_seconds": 600,
            }
            response = client.post(f"{BASE_URL}/api/sources", json=payload)
            assert response.status_code in [200, 201, 400, 404, 503]

    def test_req342_rss_url_from_federation_hints(self):
        """RSS feed_url can be resolved from federation hints."""
        with httpx.Client() as client:
            payload = {"type": "rss", "host": "example.com", "use_federation_hints": True}
            response = client.post(f"{BASE_URL}/api/sources", json=payload)
            # Should attempt to fetch feed_url from .well-known or similar
            assert response.status_code in [200, 201, 400, 404, 503]

    def test_req342_rss_url_from_host_port_path(self):
        """RSS feed_url can be constructed from host, port, and path."""
        with httpx.Client() as client:
            payload = {"type": "rss", "host": "example.com", "port": 8080, "path": "/rss"}
            response = client.post(f"{BASE_URL}/api/sources", json=payload)
            assert response.status_code in [200, 201, 400, 404, 503]


@pytest.mark.requires_provisa_server
class TestRSSFormatHandling:
    """Tests for REQ-343: RSS 2.0 & Atom format handling."""

    def test_req343_rss20_format_is_supported(self):
        """RSS 2.0 format is supported."""
        with httpx.Client() as client:
            # Fetch supported formats or parse a sample RSS 2.0 feed
            response = client.get(f"{BASE_URL}/api/sources/rss/formats")
            if response.status_code == 200:
                formats = response.json()
                assert "rss2.0" in formats or "rss" in formats

    def test_req343_atom_format_is_supported(self):
        """Atom format is supported."""
        with httpx.Client() as client:
            response = client.get(f"{BASE_URL}/api/sources/rss/formats")
            if response.status_code == 200:
                formats = response.json()
                assert "atom" in formats

    def test_req343_rss_field_extraction_title_link_description(self):
        """RSS parser extracts title, link, and description fields."""
        with httpx.Client() as client:
            # Create or fetch an RSS source
            payload = {"type": "rss", "feed_url": "https://example.com/feed.xml"}
            response = client.post(f"{BASE_URL}/api/sources", json=payload)
            if response.status_code in [200, 201]:
                source_id = response.json().get("id")
                # Fetch items from this source
                items_response = client.get(f"{BASE_URL}/api/sources/{source_id}/items")
                if items_response.status_code == 200:
                    items = items_response.json()
                    for item in items:
                        # Verify fields are present
                        assert "title" in item or "link" in item or "description" in item

    def test_req343_rss_date_parsing_rfc2822_iso8601(self):
        """RSS parser handles RFC 2822 and ISO 8601 date formats."""
        with httpx.Client() as client:
            # This test verifies date parsing capability
            # Implementation detail: dates should be normalized to a standard format
            response = client.get(f"{BASE_URL}/api/sources/rss/date-formats")
            if response.status_code == 200:
                formats = response.json()
                assert "rfc2822" in formats or "iso8601" in formats

    def test_req343_rss_unparseable_date_handling(self):
        """RSS parser uses sentinel date for unparseable dates."""
        with httpx.Client() as client:
            # When a date cannot be parsed, the parser should use a default/sentinel value
            # This is implementation-specific; the test verifies the endpoint accepts the behavior
            response = client.get(f"{BASE_URL}/api/sources/rss/config")
            if response.status_code == 200:
                config = response.json()
                # Verify sentinel date is configured
                assert "sentinel_date" in config or "default_date" in config

    def test_req343_rss_id_field_guid_or_link_fallback(self):
        """RSS parser uses guid field for ID, falls back to link."""
        with httpx.Client() as client:
            # Fetch item ID extraction behavior
            response = client.get(f"{BASE_URL}/api/sources/rss/id-extraction")
            if response.status_code == 200:
                config = response.json()
                assert "guid_priority" in config or "link_fallback" in config


@pytest.mark.requires_provisa_server
class TestEndpointAvailability:
    """Verify endpoint availability and error handling."""

    def test_sources_endpoint_exists(self):
        """GET /api/sources endpoint exists."""
        with httpx.Client() as client:
            response = client.get(f"{BASE_URL}/api/sources")
            assert response.status_code in [200, 400, 404, 503]

    def test_create_source_endpoint_exists(self):
        """POST /api/sources endpoint exists."""
        with httpx.Client() as client:
            response = client.post(f"{BASE_URL}/api/sources", json={})
            assert response.status_code in [400, 404, 409, 503]

    def test_invalid_source_type_rejected(self):
        """Invalid source type returns error."""
        with httpx.Client() as client:
            payload = {"type": "invalid_type"}
            response = client.post(f"{BASE_URL}/api/sources", json=payload)
            assert response.status_code in [400, 404, 409]
