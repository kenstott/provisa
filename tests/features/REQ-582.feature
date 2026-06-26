# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-582 — pgwire Server
  # pgwire DDL dispatches to one of two paths based on `ddl_catalog`. Trino path (non-registered catalog such as `iceberg`,…

  Scenario: REQ-582 default behaviour
    Given a DDL statement submitted over pgwire
    When ddl_catalog is a Trino catalog only CREATE TABLE and CREATE VIEW are allowed; when it is a
      registered source ID full DDL is supported
    Then the statement is dispatched to the correct path
