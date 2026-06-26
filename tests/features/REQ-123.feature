# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-123 — Authentication
  # Generic OAuth 2.0 / OIDC — works with any OIDC-compliant provider (PingFederate, Okta, Azure AD, Auth0). OIDC discovery…

  Scenario: REQ-123 default behaviour
    Given a generic OIDC provider is configured with a discovery URL
    When a request arrives with a JWT access token
    Then the token is validated via JWKS and roles are mapped using the configured claim mapping
