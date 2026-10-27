# Arrow Flight SQL Conformance

**Suite:** ADBC driver validation suite 9f9f441 (with adbc-driver-flightsql 1.12.0).

**Pass rate:** 37.7% (103 of 273); 10 cases not runnable by design.

<!-- conformance-results:begin -->
## Failing cases

<details><summary>tests.test_connection.TestConnection (21)</summary>

| Case | Reason |
| --- | --- |
| test_current_catalog[provisa:flightsql] | adbc_driver_manager.ProgrammingError: NOT_FOUND: [Flight SQL] failed to get current catalog: Unknown: [FlightSQL] EOF (Unknown; GetSessionOptions) |
| test_current_db_schema[provisa:flightsql] | adbc_driver_manager.ProgrammingError: NOT_FOUND: [Flight SQL] failed to get current db schema: Unknown: [FlightSQL] EOF (Unknown; GetSessionOptions) |
| test_get_info[provisa:flightsql] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_get_info_arrow_version[provisa:flightsql] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_catalog[provisa:flightsql] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_column_filter_catalog[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_column_filter_column_name[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_column_filter_schema[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_column_filter_table[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_column_filter_table_name[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_column_not_exist[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_column_present[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_column_xdbc[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_schema[provisa:flightsql] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_table_exact_table[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_table_invalid_catalog[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_table_invalid_schema[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_table_invalid_table[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_table_not_exist[provisa:flightsql] | adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_objects_table_present[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_get_table_schema_not_found[provisa:flightsql] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |

</details>

<details><summary>tests.test_query.TestQuery (142)</summary>

| Case | Reason |
| --- | --- |
| test_query_bind_dictionary[provisa:flightsql:type/bind/large_string] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query_bind_dictionary[provisa:flightsql:type/bind/string] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query_bind_stream[provisa:flightsql:type/bind/large_string-multiple_batches] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query_bind_stream[provisa:flightsql:type/bind/large_string-multiple_batches_with_empty] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query_bind_stream[provisa:flightsql:type/bind/large_string-single_batch] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query_bind_stream[provisa:flightsql:type/bind/string-multiple_batches] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query_bind_stream[provisa:flightsql:type/bind/string-multiple_batches_with_empty] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query_bind_stream[provisa:flightsql:type/bind/string-single_batch] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/binary] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/binary_view] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/boolean] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/date] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/decimal] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/fixed_size_binary] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/float16] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/float32] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/float64] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/int16] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/int32] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/int64] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/large_binary] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/large_string] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/string] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/string_view] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/time_ms] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/time_ns] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/time_s] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/time_us] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/timestamp_ms] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/timestamp_ns] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/timestamp_s] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/timestamp_us] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/timestamptz_ms] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/timestamptz_ns] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/timestamptz_s] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/bind/timestamptz_us] | adbc_driver_manager.ProgrammingError: INVALID_STATE: [Flight SQL Statement] must set IngestTargetTable before bulk ingestion |
| test_query[provisa:flightsql:type/literal/binary] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/boolean] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/date] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/decimal] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/float32] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/float64] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/int16] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/int32] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/int64] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/string] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/time] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/timestamp] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/literal/timestamptz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/binary] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/binary] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/binary] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/boolean] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/boolean] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/boolean] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/date] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/date] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/date] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/decimal] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/decimal] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/decimal] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/float32] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/float32] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/float32] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/float64] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/float64] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/float64] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/int16] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/int16] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/int16] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/int32] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/int32] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/int32] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/int64] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/int64] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/int64] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/string] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/string] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/string] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/time] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/time] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/time] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp0] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp0] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp0] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp0tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp0tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp0tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp1] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp1] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp1] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp1tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp1tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp1tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp2] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp2] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp2] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp2tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp2tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp2tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp3] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp3] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp3] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp3tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp3tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp3tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp4] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp4] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp4] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp4tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp4tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp4tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp5] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp5] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp5] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp5tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp5tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp5tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp6] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp6] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp6] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp6tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp6tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp6tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp7] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp7] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp7] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp7tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp7tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp7tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp8] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp8] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp8] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp8tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp8tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp8tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp9] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp9] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp9] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_execute_schema[provisa:flightsql:type/select/timestamp9tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_get_table_schema[provisa:flightsql:type/select/timestamp9tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_query[provisa:flightsql:type/select/timestamp9tz] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |

</details>

<details><summary>tests.test_statement.TestStatement (7)</summary>

| Case | Reason |
| --- | --- |
| test_execute_schema_noalias[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_nonascii_queries[provisa:flightsql] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] 'NoneType' object is not iterable. Detail: Python exception: Traceback (most recent call last): |
| test_parameter_execute[provisa:flightsql] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_parameter_null_typed[provisa:flightsql] | failed on setup with "adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |
| test_parameter_schema[provisa:flightsql] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_prepare[provisa:flightsql] | adbc_driver_manager.OperationalError: UNKNOWN: [FlightSQL] EOF (Unknown; Prepare). Vendor code: 2 |
| test_rows_affected[provisa:flightsql] | adbc_driver_manager.NotSupportedError: NOT_IMPLEMENTED: [FlightSQL] . Detail: Python exception: Traceback (most recent call last): |

</details>

<!-- conformance-results:end -->

The ADBC driver validation suite (github.com/adbc-drivers/validation, Apache-2.0) drives Provisa's Flight SQL endpoint through the ADBC Flight SQL driver: queries of each type, prepared statements, bound parameters, result schemas and catalog metadata. Its queries that create their own tables are skipped, since a Provisa schema is its model. Arrow's own Flight SQL integration scenarios are not used: they pair a scripted client with a scripted server that expects canned inputs, so they cannot test a real server.

## Deliberate differences

### Writes

DML over Flight SQL is supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name. Transaction commands are a no-op by design.

### Vocabulary fixed by the model

DDL over Flight SQL is not supported. The model is the source of schema.

### Add-ins

No standard add-ins apply to the Flight SQL transport.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| ADBC Python client | TBD | TBD | Covered by driver |
| ADBC Java client | TBD | TBD | Covered by driver |
| DBeaver (Flight SQL) | TBD | TBD | TBD: replay or manual |
