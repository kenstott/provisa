# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-137 — Large Result Redirect & CTAS
  # Client-controlled redirect via `X-Provisa-Redirect-Format` and `X-Provisa-Redirect-Threshold` headers. Format without th…

  Scenario: REQ-137 default behaviour
    Given a client sets X-Provisa-Redirect-Format and optionally X-Provisa-Redirect-Threshold
    When the query executes
    Then results are redirected to the specified format; format alone forces redirect regardless of size
