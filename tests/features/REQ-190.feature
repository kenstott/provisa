# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-190 — Hasura Migration Converters
  # v2 auth conversion via optional `--auth-env-file` flag. JWT with `jwk_url` -> Provisa `provider: oauth`. JWT `claims_map…

  Scenario: REQ-190 default behaviour
    Given a Hasura v2 auth config with JWT jwk_url, claims_map, or admin secret
    When the v2 converter runs with --auth-env-file
    Then JWT becomes provider: oauth with role_mapping[], admin secret becomes superuser, and webhook auth emits a warning
