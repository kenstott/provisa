# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-527 — pgwire Server
  # pgwire server is disabled by default and starts only when `PROVISA_PGWIRE_PORT` environment variable is set to a non-zer…

  Scenario: REQ-527 default behaviour
    Given PROVISA_PGWIRE_PORT is not set or is zero
    When the server starts
    Then the pgwire listener does not bind; when the variable is set to a non-zero integer it binds
      to 0.0.0.0
