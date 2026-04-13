---
name: test-tiers
description: Test tier contract — placement rules, infrastructure requirements, and docker-compose services. Auto-triggers when writing, reviewing, or placing tests.
---

# Test Tiers

## Tier Contract

| Tier | Location | Requirement | Constraint |
|---|---|---|---|
| unit/ | `tests/unit/` | No I/O, pure logic | Milliseconds; no network, no disk, no DB |
| integration/ | `tests/integration/` | Real docker-compose stack | Must use live services — no mocks |
| e2e/ | `provisa-ui/e2e/` | Full HTTP round-trips through live app | Playwright; must import from `./coverage` |

Misplacement is a bug. A test that belongs in unit/ but hits a real service is an integration test placed wrong. A test that belongs in integration/ but mocks docker-compose services is a unit test placed wrong.

## docker-compose Services (Never Mock in integration/)

These services are available via docker-compose and must be used directly in integration tests:

- `postgres` + `pgbouncer`
- `redis`
- `trino`
- `kafka` + `schema-registry`
- `debezium-connect`
- `mongodb`
- `minio`
- `zaychik`

`pytest.skip` for any of these is not acceptable. If the service is unavailable, fix the environment — do not skip the test.

## Rules

- Tests belong in exactly one tier
- Integration tests use real infrastructure — no substitutes
- Unit tests are the only tier where mocks are appropriate
- Test errors must be resolved whether preexisting or not
- Never remove tests to make the suite pass
