# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-529 — pgwire Server
  # pgwire authentication routes through the same pluggable AuthProvider contract as every other surface — no provider-name…

  Scenario: REQ-529 default behaviour
    Given pgwire is launched with provider `none`
    When a client connects with any password
    Then the username becomes the role_id and the session is established

    Given pgwire is launched with a credential provider (`simple` or `basic`)
    When a client presents a valid password for a known user
    Then the credential is verified through the provider and a role is resolved

    Given pgwire is launched with an OIDC-family provider
    When a client presents a bearer token as the password
    Then the token is validated through AuthProvider.validate_token and a role is resolved

    Given any configured provider and an invalid credential
    When authentication is attempted
    Then a FATAL 28P01 is returned and no session is established
