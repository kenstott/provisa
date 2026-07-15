# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-959 — Table Processor (Shared)
  # Claim scheduling and failover rest on ONE correctness anchor — a compare-and-set on the claim's processor_name at commit…

  Scenario: REQ-959 default behaviour
    Given a claim owned by a worker with a deadline and optional heartbeat
    When the owner crashes and its heartbeat lapses
    Then a peer may CAS-take-over before the deadline
    When the owner restarts and reasserts a still-owned claim
    Then the CAS matches and it resumes; if a peer took it the CAS matches zero and it drops
    When the owner is alive but missed deadline+grace without completing
    Then a peer reclaims it and fires
    And a superseded owner's late commit fails the ownership CAS and applies nothing
