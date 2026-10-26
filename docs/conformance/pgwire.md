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

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| psql | TBD | TBD | Verified by replay |
| DBeaver | TBD | TBD | Covered by driver (Postgres JDBC) |
| Tableau | TBD | TBD | Covered by driver (Postgres JDBC/ODBC) |
| DataGrip | TBD | TBD | TBD: replay or manual |
