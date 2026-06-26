# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-219 — Hasura v2 Parity: Medium-Complexity Features
  # Subscriptions via Server-Sent Events (SSE) -- `GET /data/subscribe/<table>` endpoint using FastAPI StreamingResponse. Po…

  Scenario: REQ-219 default behaviour
    Given a client connected to GET /data/subscribe/<table>
    When INSERT, UPDATE, or DELETE events occur on that table
    Then the events are streamed to the client via SSE using PostgreSQL LISTEN/NOTIFY
