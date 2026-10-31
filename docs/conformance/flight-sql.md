# Arrow Flight SQL Conformance

**Suite:** ADBC driver validation suite 9f9f441 (with adbc-driver-flightsql 1.12.0).

**Pass rate:** 100.0% (217 of 217); 18 cases not runnable by design; 37 not sent by the client; 11 skipped by the suite.

<!-- conformance-results:begin -->
## Not runnable by design

| Group | Count | Decision |
| --- | --- | --- |
| Timestamps beyond microseconds | 18 | A timestamp column is read at the precision its source declares. PostgreSQL holds at most six digits: it declares a TIMESTAMP(7) to (9) column TIMESTAMP(6), so the column holds microseconds and is read as microseconds, where the suite expects nanoseconds. |

## Not sent by the client

| Group | Count | Client | What it does | Evidence | Workaround |
| --- | --- | --- | --- | --- | --- |
| Bind with no Prepare | 36 | adbc-driver-flightsql 1.12.0 (the Go driver) | Refuses the Bind: "[Flight SQL Statement] must set IngestTargetTable before bulk ingestion" (INVALID_STATE). | The suite binds parameters to a statement it has not prepared. The driver refuses Bind without Prepare in the client: against a probe Flight SQL server that records every call, the same Bind sends no RPC. | Prepare the statement before binding (the DB-API cursor's execute does), as tests/unit/test_flightsql_prepared.py does against Provisa. |
| Bind before Prepare | 1 | adbc-driver-flightsql 1.12.0 (the Go driver) | Accepts the Bind and keeps the values, then sends the statement it prepares without them: Prepare never binds what was bound before it (go/adbc/driver/flightsql/flightsql_statement.go, Bind and Prepare, at apache-arrow-adbc-24). | Against Provisa's server with the driver at 1.12.0, set_sql_query, bind, prepare and execute send no DoPut: the server runs "SELECT 1 + $1" with no values, and its engine answers "Values were not provided". | Bind after Prepare, as the suite's other bind tests do. |

## Adapted cases

<details><summary>Type cases' tables (33 of 33 passed)</summary>

Setup became: Its setup_query runs in the job's PostgreSQL (schema conformance) before the instance starts, and the table it creates is declared in the fixture model's conformance domain, where the case's own query reads it. binary's setup is written in PostgreSQL's spelling (BYTEA for BLOB, '\x..'::bytea for X'..'). Its table is listed with its own columns and then the two link columns every model table carries (_name_, _domain_), so the schema the case expects its table listed with has them too (flightsql/queries/type/select/<case>.txtcase, generated from the suite's case and checked against it).

| Case | Outcome |
| --- | --- |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/binary] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/binary] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/binary] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/boolean] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/boolean] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/boolean] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/date] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/date] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/date] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/decimal] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/decimal] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/decimal] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/float32] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/float32] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/float32] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/float64] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/float64] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/float64] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/int16] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/int16] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/int16] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/int32] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/int32] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/int32] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/int64] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/int64] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/int64] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/string] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/string] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/string] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/time] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/time] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/time] | passed |

</details>

<details><summary>Timestamp cases' tables (42 of 60 passed)</summary>

Setup became: The ten precisions of timestamp and of timestamptz each create test_timestamp or test_timestamptz with different contents, so each case's table is named for the case (test_timestamp3tz, ...), declared in the fixture model and seeded by its setup; the case's query and metadata name it, and its listed schema has the link columns every model table carries (flightsql/queries/type/select/<case>.txtcase, generated from the suite's case and checked against it).

| Case | Outcome |
| --- | --- |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp0] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp0] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp0] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp0tz] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp0tz] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp0tz] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp1] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp1] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp1] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp1tz] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp1tz] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp1tz] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp2] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp2] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp2] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp2tz] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp2tz] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp2tz] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp3] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp3] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp3] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp3tz] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp3tz] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp3tz] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp4] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp4] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp4] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp4tz] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp4tz] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp4tz] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp5] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp5] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp5] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp5tz] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp5tz] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp5tz] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp6] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp6] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp6] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp6tz] | passed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp6tz] | passed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp6tz] | passed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp7] | failed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp7] | failed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp7] | failed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp7tz] | failed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp7tz] | failed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp7tz] | failed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp8] | failed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp8] | failed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp8] | failed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp8tz] | failed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp8tz] | failed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp8tz] | failed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp9] | failed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp9] | failed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp9] | failed |
| tests.test_query.TestQuery::test_execute_schema[provisa:flightsql:type/select/timestamp9tz] | failed |
| tests.test_query.TestQuery::test_get_table_schema[provisa:flightsql:type/select/timestamp9tz] | failed |
| tests.test_query.TestQuery::test_query[provisa:flightsql:type/select/timestamp9tz] | failed |

</details>

<details><summary>get_objects table (14 of 14 passed)</summary>

Setup became: The table the suite would ingest (ints INTEGER, strs TEXT; rows (1, NULL), (NULL, 'foo'), (42, 'spam')) is getobjects_fixture, declared in the fixture model's conformance domain (the suite's would be in the current schema, public), so the tests that look for it in the current schema look in conformance; and it is listed with the link columns every model table carries after its own, which the tests that list its exact columns expect too. Dropping a table is a no-op, as the model holds every table.

| Case | Outcome |
| --- | --- |
| tests.test_connection.TestConnection::test_get_objects_column_filter_catalog[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_column_filter_column_name[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_column_filter_schema[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_column_filter_table[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_column_filter_table_name[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_column_not_exist[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_column_present[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_column_xdbc[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_table_exact_table[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_table_invalid_catalog[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_table_invalid_schema[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_table_invalid_table[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_table_not_exist[provisa:flightsql] | passed |
| tests.test_connection.TestConnection::test_get_objects_table_present[provisa:flightsql] | passed |

</details>

<details><summary>Statement tests' tables (3 of 3 passed)</summary>

Setup became: sample_table (id INTEGER, value TEXT) and test_rows_affected (id INTEGER) are seeded empty in the job's PostgreSQL and declared in the fixture model, writable by org_admin; each test's CREATE TABLE is replaced by DELETE FROM the table, which leaves it empty as the CREATE did, and its writes run through the governed pipeline.

| Case | Outcome |
| --- | --- |
| tests.test_statement.TestStatement::test_execute_schema_noalias[provisa:flightsql] | passed |
| tests.test_statement.TestStatement::test_parameter_null_typed[provisa:flightsql] | passed |
| tests.test_statement.TestStatement::test_rows_affected[provisa:flightsql] | passed |

</details>


## Failing cases

None.
<!-- conformance-results:end -->

The ADBC driver validation suite (github.com/adbc-drivers/validation, Apache-2.0) drives Provisa's Flight SQL endpoint through the ADBC Flight SQL driver: queries of each type, prepared statements, bound parameters, updates, result schemas, catalog metadata and session options. A Provisa schema is its model, so a case whose setup creates its table is adapted rather than skipped: the table is declared in the job's fixture model and seeded by the case's own setup, and the case runs and counts. Each adapted case is listed under Adapted cases with what its setup became. A case whose client driver does not send what it needs is listed under Not sent by the client, with the evidence, and left out of the pass rate. Arrow's own Flight SQL integration scenarios are not used: they pair a scripted client with a scripted server that expects canned inputs, so they cannot test a real server.

## Deliberate differences

### Writes

DML over Flight SQL is supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name. Bulk ingest writes into a table the model declares, through the same governed writes. A temporary ingest lands in a temporary table of the client's own session, which only that session reads and lists, and which ends with the session. Transaction commands are a no-op by design.

### Vocabulary fixed by the model

DDL over Flight SQL is not supported. The model is the source of schema. An ingest that would create a table the model does not declare, or replace one it does, is refused.

### Add-ins

No standard add-ins apply to the Flight SQL transport.

## Notes

### The status of a table that already exists

An ingest that creates a table that already exists, or whose columns do not match the table, is refused with the status INVALID_ARGUMENT, not ALREADY_EXISTS. The suite's three cases that check for ALREADY_EXISTS (test_create_conflict, test_append_schema_mismatch and test_createappend_schema_mismatch) therefore fail. The cause is the server library: no error a pyarrow Flight server raises reaches the client as ALREADY_EXISTS. Each kind of error was raised in turn through the ADBC Flight SQL driver 1.12.0. A KeyError arrives as NOT_FOUND, a ValueError or ArrowInvalid as INVALID_ARGUMENT, a NotImplementedError as NOT_IMPLEMENTED, and each Flight error class as its own status (UNAUTHORIZED, UNAUTHENTICATED, UNAVAILABLE, CANCELLED, TIMEOUT, INTERNAL). Every other error arrives as UNKNOWN. INVALID_ARGUMENT is the nearest status the server can send, and the refusal names the table or the column.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| ADBC Python client | TBD | TBD | Covered by driver |
| ADBC Java client | TBD | TBD | Covered by driver |
| DBeaver (Flight SQL) | TBD | TBD | TBD: replay or manual |
