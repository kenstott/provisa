# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1090 — Encryption
  # Enterprises can register custom encryption providers (e.g., in-house KMS/HSM endpoints) without core changes via registe…

  Scenario: REQ-1090 default behaviour
    Given a custom KMS provider module is installed
    When build_encryption_service is called with the custom provider name
    Then the custom provider is instantiated and used for encryption operations
