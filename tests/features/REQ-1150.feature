# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1150 — Embedded CLI Startup
  # The `provisa run` console command signals startup completion by polling the API /ready endpoint (the warm gate, not /hea…

  Scenario: REQ-1150 default behaviour
    Given `provisa run` is launched with --demo
    When the embedded API and UI servers start
    Then _announce_ready polls /ready until 200 or 300s timeout
    And once /ready returns 200, "✓ Provisa is ready — http://127.0.0.1:3000/?tour=1" is printed
    And the browser opens to that URL automatically
    And the guided tour starts (App.tsx reads ?tour=1)

    Given `provisa run --no-browser` is launched
    When the servers become ready
    Then the completion line is still printed
    And no browser opens
    And the user can manually open the URL

    Given a failure during readiness polling (network error, etc.)
    When the failure occurs
    Then it is caught and silently logged
    And the URL is printed as a fallback
    And the servers keep running
