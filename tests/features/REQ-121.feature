# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-121 — Authentication
  # Firebase Authentication — validates Firebase ID tokens via firebase-admin SDK. Supports all Firebase auth methods (email…

  Scenario: REQ-121 default behaviour
    Given Firebase is configured as the auth provider
    When a request arrives with a Firebase ID token
    Then the token is validated via firebase-admin SDK and the identity is resolved
