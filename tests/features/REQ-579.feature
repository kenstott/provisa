# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-579 — pgwire Server
  # The pgwire server reports server version `14.0.provisa` to connecting clients. Tools that gate features on the PostgreSQ…

  Scenario: REQ-579 default behaviour
    Given a client connecting to pgwire
    When the server reports its version
    Then `14.0.provisa` is returned so tools behave as though connected to PostgreSQL 14
