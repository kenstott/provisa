# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-580 — pgwire Server
  # pgwire simple-query protocol supports multi-statement queries. Semicolon-separated statements are split and executed seq…

  Scenario: REQ-580 default behaviour
    Given a JDBC tool or psql script sending semicolon-separated statements in a single message
    When the pgwire server receives the message
    Then statements are split and executed sequentially
