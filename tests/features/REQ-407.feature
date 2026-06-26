# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-407 — API & Integration
  # OpenAPI source backend accepts optional `spec_content: str` on `OpenAPIRegisterRequest` and `OpenAPIPreviewRequest`; whe…

  Scenario: REQ-407 default behaviour
    Given a registration request with spec_content provided
    When the backend processes the request
    Then the inline spec is parsed (YAML then JSON fallback) and path is stored as ":inline:"
