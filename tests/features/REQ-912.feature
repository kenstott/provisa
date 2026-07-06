# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-912 — ClickHouse Federation Engine
  # The ClickHouse federation runtime supports three interchangeable execution backends selected by URL scheme at initializa…

  Scenario: REQ-912 default behaviour
    Given ClickHouseFederationRuntime.from_url with URL scheme
    When the URL scheme is clickhouse://
    Then connect via clickhouse-connect HTTP client to port 8123
    When the URL scheme is clickhouse+native://
    Then connect via clickhouse-driver native TCP client to port 9000
    When the URL scheme is chdb:// or chdb:///path
    Then initialize embedded chdb in-process, optionally persisting to path
    And all three backends support identical SQL execution and integration engines
