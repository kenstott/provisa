# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-890 — Federation Engine Abstraction
  # Pgwire auth must be a pluggable provider interface selected at launch (trust | local | oidc), superseding today's fixed…

  Scenario: REQ-890 default behaviour
    Given pgwire launched with the 'oidc' auth provider (issuer URL + audience configured)
    When a client presents an OIDC ID token (JWT) as the password
    Then the token is verified against the issuer JWKS and mapped to a role via resolve_role

    Given an invalid or tampered OIDC token
    When authentication is attempted
    Then a FATAL 28P01 is returned and no session is established

    Given the cleartext/simple provider is not explicitly enabled
    When a client attempts cleartext auth
    Then the connection is refused
