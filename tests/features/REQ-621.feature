# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-621 — Hasura Migration Converters
  # [SUPERSEDED by REQ-417, 2026-10-17 -- the v2 converter invents no connection for a source the export does not carry. Kep…

  Scenario: REQ-621 default behaviour
    Given a completed Hasura v2 or DDN conversion
    When the output config is inspected
    Then a DDN source carries placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}), and a v2 source whose connection the export does not carry has no invented host and its report says to enter it
