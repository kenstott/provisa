# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-887 — RDBMS Stored Procedure Auto-Discovery
  # Extend database source introspection to auto-discover stored procedures and routines (via information_schema.routines, p…

  Scenario: REQ-887 default behaviour
    Given a user connects a Postgres source with stored procedures (identified via pg_proc)
    When introspection runs and analyzes prokind (p=procedure, f=function) and provolatile (stable/immutable vs volatile)
    Then Provisa auto-registers stable/immutable procedures as parameterized relations (proc arguments as query params, same shape as OpenAPI GET tables) and volatile procedures as mutations/tracked functions, making results queryable via GraphQL and SQL with governance applied identically to hand-registered procs
