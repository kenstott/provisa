# pgwire Drivers Conformance

Real PostgreSQL drivers run their own upstream test suites against a Provisa test instance over pgwire, each at a pinned release. Every test is in one of four counts:

- **passed**: the test passed.
- **skipped by design**: the test fails because of a deliberate difference, listed below with the decision behind it.
- **failed**: the test fails and no design decision covers it: an open defect.
- **not tested**: the suite skipped the test itself, for example because it needs a module that is not installed or a server feature the suite checks for first.

Each driver keeps a list of the tests expected to fail, each with its reason. A run fails when a test not on the list fails, or when a listed test passes and the list must be corrected. The results below are generated from the CI run, never typed by hand.

<!-- driver-results:pgjdbc:begin -->
## pgjdbc

No run recorded yet.
<!-- driver-results:pgjdbc:end -->

<!-- driver-results:psycopg:begin -->
## psycopg

No run recorded yet.
<!-- driver-results:psycopg:end -->
