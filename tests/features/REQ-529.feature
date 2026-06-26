# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-529 — pgwire Server
  # pgwire authentication uses PG auth type 3 (cleartext password) for both trust and simple modes. Trust mode: username bec…

  Scenario: REQ-529 default behaviour
    Given a pgwire connection with a provider other than none or simple
    When authentication is attempted
    Then a FATAL login error is returned
