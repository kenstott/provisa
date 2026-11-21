# Bolt Conformance

**Suite:** neo4j-python-driver integration tests 6.2.0 (tests/integration).

**Pass rate:** 100.0%: 286 passed, 9 skipped by design, 0 failed, of 295 cases.

<!-- conformance-results:begin -->
## Skipped by design

| Skipped by design | Count | Decision |
| --- | --- | --- |
| Digits below the microsecond | 5 | "Every temporal value, read from a column or built in a query, has microsecond precision, what the engines and their drivers hold; its nanosecond property is its microseconds times 1000." |
| Encryption examples the suite skips | 2 | neo4j-python-driver 6.2.0 (tag at commit c2454236512448d7b55abc6b10e33e2eaae3b8bf): tests/integration/examples/test_config_secure_example.py:55 and test_config_trust_example.py:53 call pytest.skip("re-enable when we can test with encrypted=True on Docker") first, unconditionally, for every server. |
| Kerberos example the suite skips | 1 | neo4j-python-driver 6.2.0 (tag at commit c2454236512448d7b55abc6b10e33e2eaae3b8bf): tests/integration/examples/test_kerberos_auth_example.py:40 calls pytest.skip("Currently no way to test Kerberos auth") unconditionally, for every server. |
| Service-unavailable example the suite skips | 1 | neo4j-python-driver 6.2.0 (tag at commit c2454236512448d7b55abc6b10e33e2eaae3b8bf): tests/integration/examples/test_service_unavailable_example.py:56 calls pytest.skip("Fix better error messages for the user. Be able to kill the server.") unconditionally, for every server. |

<details><summary>Digits below the microsecond (5)</summary>

| Case | Reason |
| --- | --- |
| tests.integration.examples.test_temporal_types_example::test_datetime | AssertionError: assert '2021-11-02T0...0004000-04:00' == '2021-11-02T0...0004123-04:00' |
| tests.integration.examples.test_temporal_types_example::test_time | AssertionError: assert '07:47:00.000004000-04:00' == '07:47:00.000004123-04:00' |
| tests.integration.examples.test_temporal_types_example::test_local_datetime | AssertionError: assert '2021-11-02T0...:00.000004000' == '2021-11-02T0...:00.000004123' |
| tests.integration.examples.test_temporal_types_example::test_local_time | AssertionError: assert '07:47:00.000004000' == '07:47:00.000004123' |
| tests.integration.examples.test_temporal_types_example::test_duration_example | AssertionError: assert 'P1Y2DT3S' == 'P1Y2DT3.000000004S' |

</details>

<details><summary>Encryption examples the suite skips (2)</summary>

| Case | Reason |
| --- | --- |
| tests.integration.examples.test_config_secure_example::test_example | skipped by the suite: re-enable when we can test with encrypted=True on Docker |
| tests.integration.examples.test_config_trust_example::test_example | skipped by the suite: re-enable when we can test with encrypted=True on Docker |

</details>

<details><summary>Kerberos example the suite skips (1)</summary>

| Case | Reason |
| --- | --- |
| tests.integration.examples.test_kerberos_auth_example::test_example | skipped by the suite: Currently no way to test Kerberos auth |

</details>

<details><summary>Service-unavailable example the suite skips (1)</summary>

| Case | Reason |
| --- | --- |
| tests.integration.examples.test_service_unavailable_example::test_example | skipped by the suite: Fix better error messages for the user. Be able to kill the server. |

</details>


## Adapted cases

<details><summary>The examples' graph (11 of 11 passed)</summary>

Setup became: The labels and relationship types the examples create are declared in a domain of the job's fixture model that holds them alone: Person, Company and Greeting, KNOWS (Person to Person, with the from property the driver introduction example writes) and WORKS_FOR (Person to Company), each relationship a junction table. The driver's user acts as a role whose domain access is that domain alone, writing it, so each example's MATCH (_) DETACH DELETE _ cleanup empties the examples' graph and nothing else.

| Case | Outcome |
| --- | --- |
| tests.integration.examples.test_autocommit_transaction_example::test_example | passed |
| tests.integration.examples.test_hello_world_example::test_hello_world_example | passed |
| tests.integration.examples.test_pass_bookmarks_example::test | passed |
| tests.integration.examples.test_read_write_transaction_example::test_example | passed |
| tests.integration.examples.test_result_consume_example::test_example | passed |
| tests.integration.examples.test_result_retain_example::test_example | passed |
| tests.integration.examples.test_session_example::test_example | passed |
| tests.integration.examples.test_transaction_function_example::test_example | passed |
| tests.integration.examples.test_transaction_metadata_config_example::test_example | passed |
| tests.integration.examples.test_transaction_timeout_config_example::test_example | passed |
| tests.integration.test_readme::test_should_run_readme | passed |

</details>

<details><summary>Sign-in cost (1 of 1 passed)</summary>

Setup became: The test user's password is hashed at the minimum bcrypt cost; reconnect-per-cancel is the driver's behaviour.

| Case | Outcome |
| --- | --- |
| tests.integration.mixed.test_async_cancellation::test_async_cancellation_does_not_leak | passed |

</details>


## Failed cases

None.
<!-- conformance-results:end -->

The official Neo4j Python driver's own integration tests (`tests/integration` of github.com/neo4j/neo4j-python-driver, Apache-2.0), at a pinned release tag, run against Provisa's Bolt endpoint. The driver's tests assume an empty Neo4j database they may write to; a test that creates its own labels fails here, because a Provisa graph's labels and relationship types are its model's.

## Deliberate differences

### Writes

Writes are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name. `BEGIN`/`COMMIT`/`ROLLBACK` are a no-op by design.

### Vocabulary fixed by the model

Server-side procedures that mutate labels or create schema elements are not supported. Declare the structure in the model.

### Add-ins

APOC and GDS procedures delivered over Bolt are not available inline. Procedures refused by name appear in the by-design table on this page.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Neo4j Browser | — | — | Covered by driver (Neo4j JavaScript driver) |
| Neo4j Bloom | — | — | Covered by driver (Neo4j JavaScript driver) |
| DBeaver (Bolt) | — | — | Not yet verified |
