# pgwire Conformance

**Suite:** Postgres conformance suite plus psycopg and JDBC driver test suites. TBD: version recorded from CI.

**Pass rate:** TBD: generated from CI.

## Deliberate differences

### Writes

`INSERT`, `UPDATE`, and `DELETE` are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name. `BEGIN`/`COMMIT`/`ROLLBACK` are a no-op by design.

### Vocabulary fixed by the model

`CREATE TABLE`, `ALTER TABLE`, and DDL statements are not supported. The model is the source of schema.

Postgres extensions are not loadable via `CREATE EXTENSION`. Where an extension function is commonly needed, Provisa offers it as a command.

### Add-ins

Extension functions (`pg_trgm`, `pg_vector`, PostGIS, etc.) are not available unless exposed as commands. TBD: specific cases from CI.

### Server version

`server_version_num` reports `140000` and `version()` reports PostgreSQL 14. Clients choose their catalog queries by this number, and the catalog queries Provisa answers are the ones PostgreSQL 14 clients send.

## Known anomalies

### Numeric precision on DuckDB

A PostgreSQL `numeric` column declared without a precision is registered as `numeric(38, s)`, where `s` is the largest scale found among its values (10 for a table with no rows). The DuckDB engine reads it exactly up to 38 significant digits. A value with more digits is rounded to 38.

Workaround: declare the column with its precision and scale at the source, or read the table through the PostgreSQL engine.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| psql | TBD | TBD | Verified by replay |
| DBeaver | TBD | TBD | Covered by driver (Postgres JDBC) |
| Tableau | TBD | TBD | Covered by driver (Postgres JDBC/ODBC) |
| DataGrip | TBD | TBD | TBD: replay or manual |
