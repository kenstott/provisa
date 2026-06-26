# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-122 — Authentication
  # Keycloak OIDC — validates JWT access tokens from Keycloak via OIDC discovery + JWKS. Realm roles + client roles → Provis…

  Scenario: REQ-122 default behaviour
    Given Keycloak is configured as the OIDC provider
    When a request arrives with a Keycloak JWT access token
    Then the token is validated via OIDC discovery and JWKS, and realm/client roles are mapped to Provisa roles
