# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-859 — Freshness Module
  # MV and the ad-hoc API/pg cache freshness checks (provisa/openapi/pg_cache.py) become FreshnessSubjects (via the StateSub…

  Scenario: REQ-859 default behaviour
    Given a materialized view and an API/pg cache entry, each with its own last-refresh state When their freshness is evaluated Then both expose that state as a FreshnessSubject and the TTL decision is produced by the one shared FreshnessPredicate, yielding the same fresh/stale result as the prior per-consumer checks.
