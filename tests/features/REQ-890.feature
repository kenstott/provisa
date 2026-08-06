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

    Given any configured auth provider
    When a client presents an ordinary password in the startup packet
    Then it is validated through the provider's basic presentation and the role comes from the validated identity, not the supplied username

    Given a client presents a personal access token as the password
    When authentication is attempted
    Then the token prefix selects the bearer presentation and the token validates as an ordinary bearer credential

    Given auth.default_role is not configured
    When an authenticated identity matches no role-mapping rule
    Then the connection fails rather than landing on an arbitrary role
