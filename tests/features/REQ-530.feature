# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-530 — pgwire Server
  # pgwire TLS is enabled by setting `PROVISA_PGWIRE_CERT` and `PROVISA_PGWIRE_KEY` to PEM certificate and key paths. When b…

  Scenario: REQ-530 default behaviour
    Given PROVISA_PGWIRE_CERT and PROVISA_PGWIRE_KEY are set
    When a client connects
    Then the connection is wrapped in TLS; when absent the server replies N to SSL negotiation
