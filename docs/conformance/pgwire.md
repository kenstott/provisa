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

### geometry and geography in binary format

A `geometry` or `geography` column is reported with its PostGIS type name and is sent in text format only, as WKT. PostGIS's binary format (EWKB) is not sent; a client that asks for the column in binary format gets an error naming it.

### Catalog functions

Provisa intercepts the scalar catalog functions tools call to introspect schemas and format type names. Each one is answered from the role's governed model view:

| Function | Behavior |
|---|---|
| `format_type(typid, typmod)` | Returns the type name tools display in schema panels. Subtypes report by their subtype name (`inet`, `macaddr`, `money`), not the base type. |
| `col_description(relid, colnum)` | Returns the column's description from the model. Columns with no description return `NULL`. |
| `pg_get_constraintdef(oid)` | Returns a text rendering of the constraint — `PRIMARY KEY (col)`, `FOREIGN KEY (col) REFERENCES other(col)`, or `UNIQUE (col)` — derived from the model's declared keys and joins. |
| `pg_get_expr(expr, relid)` | Returns the default expression of a column read from `pg_attrdef.adbin`, such as `now()`, `current_setting('app.user')` or `'new'::character varying`. A default is the value your role's insert grant presets on the column. A column with no preset, and any other expression column, returns `NULL`. |
| `has_table_privilege([user,] table, privilege)` | Answers for your role. `SELECT` is held on every table you are served. `INSERT`, `UPDATE` and `DELETE` are held where your role has the `write` right and a write grant for that operation on a table whose source takes it. A list such as `'INSERT, UPDATE'` is true when any item is held. `REFERENCES`, `TRIGGER`, `TRUNCATE` and any `WITH GRANT OPTION` are false. The table is an oid or a name, and the user must be your own role. A table that does not exist, or another user, returns `NULL`. |
| `current_schema()` | Returns `'public'`. |
| `obj_description(oid, catalog)` | Returns the description of the named catalog object (table or schema) from the model, or `NULL`. |

Each function is answered in the catalog intercept layer alongside `pg_catalog` and `information_schema` tables; none reaches a backend engine. A call with arguments Provisa cannot resolve returns `NULL`, matching PostgreSQL's own behavior for missing objects.

Every overload of every `pg_catalog` function is a conformance case, and each case is passed, skipped by design or failed. No `pg_catalog` function is skipped by design: one not served counts as failed until it is served, whatever its family. The suite calls each overload the way PostgreSQL requires it, with the `VARIADIC` form, `WITHIN GROUP`, `OVER`, or a real argument where PostgreSQL refuses a `NULL`. An overload no call can reach, such as one taking an `internal` argument or a trigger function, is listed as failed by name. The failed cases are listed by family under the pass rate.

### Advisory locks

`pg_advisory_lock` and its shared, transaction-level and `try` variants work as they do in PostgreSQL among the sessions of one Provisa process. The lock table lives in that process's memory. Two Provisa processes, such as two replicas behind a load balancer, do not see each other's locks, so a lock taken on one does not make a session on the other wait. Clients that use advisory locks to serialize work across processes must connect to one Provisa process.

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
