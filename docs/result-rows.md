# Where Rows Pass Through Python

Provisa writes every result to its client with encoders written in Rust. They read Arrow record
batches column by column. When the engine or source hands Provisa its result as Arrow, the batches
go to the encoder as they arrived. When it hands back rows, Python builds one object per value
first, and the encoder then reads those objects.

## What a Python row costs

Every value in the result becomes a Python object on the worker thread that runs the query. A
result of a million rows and ten columns is ten million objects, each allocated, read once by the
encoder and freed. The cost grows with rows times columns. An Arrow batch holds each column in one
buffer, so the encoder reads it without allocating anything per value.

You pay this most on wide and long results. Exports and large scans over pgwire hit it hardest. A
query that returns a few hundred rows spends its time in the engine, and the conversion does not
show.

Some clients take rows whatever the engine returns. GraphQL and JSON:API build their response
from rows, so Provisa builds rows from the Arrow batches for them. That conversion happens once, as
the response is written.

## Engines

The engine runs every query that joins sources or that a source cannot answer alone.

| Engine | Result arrives as | Where rows pass through Python |
| --- | --- | --- |
| DuckDB | Arrow | Only for a client that takes rows |
| Trino, embedded or your own | Arrow, through the Flight SQL proxy | Only for a client that takes rows |
| Snowflake | Arrow | SQL over HTTP and pgwire: each batch becomes rows as it streams. Flight SQL streams the Arrow |
| Databricks | Arrow | Same as Snowflake |
| BigQuery | Arrow, through the Storage Read API | Same as Snowflake |
| ClickHouse, embedded or server | Arrow | Same as Snowflake |
| Microsoft Fabric, Azure Synapse | Rows, from the ODBC driver | Every route. Flight SQL packs the rows into Arrow batches |
| PostgreSQL | Rows, from a server-side cursor | Every route |
| SQLAlchemy engines (MySQL, Oracle, SQL Server, Redshift and the rest) | Rows | Every route |

## Sources read directly

A query that one source can answer alone goes to that source's own client, without an engine.
Provisa reads the result in one of two ways. A bounded result is read whole. A large scan streams
a batch at a time, so memory holds one batch.

| Source | Whole result | Streamed result |
| --- | --- | --- |
| DuckDB | Arrow | Rows built from Arrow batches |
| Snowflake | Arrow | Rows |
| Databricks | Arrow | Rows |
| BigQuery | Arrow | Rows |
| PostgreSQL | Rows. Over pgwire, Provisa forwards the source's own row messages without decoding them | Rows, forwarded the same way over pgwire |
| ClickHouse | Rows | Rows |
| MySQL, MariaDB | Rows | Rows |
| SQL Server | Rows | Rows |
| Oracle | Rows | Rows |
| Microsoft Fabric, Azure Synapse | Rows | Rows |
| Amazon Athena | Arrow, read from the result file in the query output location | Arrow, read whole: Athena has no streamed read |
| Exasol | Rows | Rows |
| Hive | Rows | Rows |
| JDBC sources | Rows | Rows |
| SQLAlchemy sources (Redshift, SAP HANA, Trino as a source and the rest) | Rows | Rows |

Snowflake sends some results as JSON instead of Arrow. Those are the status line of a DDL
statement and the listing from `SHOW` and `DESCRIBE`. Its client has no Arrow reader for those, so
they arrive as rows. They are a few rows each.

Athena writes each result to a file in the query output location, and Provisa reads that file as
Arrow. The source's credentials must be able to read that location. When you add an Athena source,
Provisa runs one statement and reads its result. If the credentials cannot read the location, the
source is refused, and the message names the location.

## Choosing for large results

If you move large results out of a warehouse, read them over Flight SQL. On Snowflake, Databricks,
BigQuery and ClickHouse that route carries the engine's Arrow to your client with no Python row in
between. Over pgwire and SQL over HTTP, the same engines convert each batch to rows before it is
encoded.
