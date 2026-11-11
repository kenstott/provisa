# Conformance

Provisa checks every protocol and query language it serves against a public test suite. Each suite runs in CI against a test instance after every green build, and each page here reports the results: the suite, its version, the current pass rate, and every deliberate difference with its reason and workaround.

## How conformance is measured

After every green build, CI provisions a fresh test instance and runs the conformance suite against it. Pass rates come directly from that output and are never typed by hand. Each page reflects what the suite last reported.

Provisa fixes failures or lists them. No failure is left unnamed.

## Function results by engine

Each `pg_catalog` function overload has a result on each engine: exact, equivalent under a numbered rule, refused by name, or differs. An engine that refuses a function by name has given that engine's result. A wrong answer on any engine is a defect.

A literal alone does not show that a function works on a column, because a form can fold a literal at transpile time and still refuse a column of the same type. So each overload with a sample value is also called with that value read from a column of a registered table. An overload that passes on literals but not on the column is listed as literals only. The list does not change its result.

<!-- function-results:begin -->
| Engine | Exact | Equivalent | Refused by name | Differs: wrong answer | Differs: engine error |
| --- | ---: | ---: | ---: | ---: | ---: |
| Postgres | 2647 | 14 | 10 | 55 | 22 |
| DuckDB | 1778 | 13 | 120 | 94 | 743 |
| Trino | 955 | 4 | 335 | 49 | 1405 |
| ClickHouse | 717 | 1 | 493 | 155 | 1382 |

Counts are `pg_catalog` overloads against PostgreSQL 17.11 (Debian 17.11-1.pgdg13+2), one row per engine. Each result is that engine's own, and the rows do not add up to a product result. The footnotes for each equivalent result are on the [pgwire page](pgwire.md#function-results-by-engine).
<!-- function-results:end -->

## Why differences exist

Deliberate differences group by cause, which also indicates the workaround.

**Writes.** Provisa supports writes that map onto the model through governed table writes. A write needing structure the model lacks is refused with a message naming what is missing. Transaction statements (`BEGIN`, `COMMIT`, `ROLLBACK`) are a no-op by design; the governed pipeline does not expose transaction boundaries to callers.

**Vocabulary fixed by the model.** A new class, property, or relationship type belongs in the model as a table, column, or ontology term. The data store holds instances; vocabulary goes in the model.

**Add-ins.** APOC procedures, GDS algorithms, custom SPARQL functions, and Postgres extensions appear in conformance suites. Where a procedure is commonly needed, Provisa offers it as a command. For others, run it in your own store by querying Provisa live (via `SERVICE` in SPARQL or `apoc.bolt.load` in Neo4j) or by loading the governed graph into it.

## Polyfills

Most engines Provisa serves are not PostgreSQL, so a pgwire client's PostgreSQL behaviour is supplied by a polyfill. A feature's read polyfill computes its values and operations on the engine. Its write polyfill carries values into the engine in literals, parameters, predicates and writes. A polyfill states its limitations, and a feature that cannot be polyfilled is refused by name with a SQLSTATE.

The [polyfill pages](polyfill/index.md) show the grid, one page per engine, generated from the code the translator runs. The conformance runs check it: a feature the grid marks native or polyfilled that answers otherwise than PostgreSQL fails the run, and so does a refusal or difference the grid does not state.

## Tool verification

Each tool entry gives the tool version and the date it was verified, plus how it was verified.

**Covered by its driver.** The open-source driver the tool sits on runs in CI with its own test suite where one exists. Tableau and DBeaver are verified this way through Postgres JDBC/ODBC. Neo4j Browser and Bloom are verified through the Neo4j JavaScript driver. Protégé and Stardog Studio are verified through SPARQL over HTTP.

**Verified by replay.** The real tool is run once against a test instance and its protocol conversation is captured. CI replays that conversation on every subsequent build, so a change breaking what the tool relies on fails the replay. A licence is needed only at capture time.

**Verified manually.** The tool has been run against a test instance and confirmed to work.

## Surfaces

| Surface | Suite | Page |
| --- | --- | --- |
| SPARQL 1.1 | W3C SPARQL 1.1 test suite | [sparql.md](sparql.md) |
| RDF 1.1 syntax | W3C rdf-tests | [rdf-syntax.md](rdf-syntax.md) |
| Cypher | openCypher TCK | [cypher.md](cypher.md) |
| Bolt | Neo4j driver test kit | [bolt.md](bolt.md) |
| GraphQL | graphql-http compliance audit | [graphql.md](graphql.md) |
| pgwire | pgspec v1.5.11 against PostgreSQL 17.11 | [pgwire.md](pgwire.md) |
| pgwire drivers | pgjdbc and psycopg, each its own test suite | [drivers.md](drivers.md) |
| Arrow Flight SQL | ADBC driver validation suite | [flight-sql.md](flight-sql.md) |
| REST / OpenAPI | No public suite runs today | [rest.md](rest.md) |
| JSON:API | No public suite runs today | [jsonapi.md](jsonapi.md) |
| gRPC | No public suite runs today | [grpc.md](grpc.md) |
| MCP | No public suite runs today | [mcp.md](mcp.md) |
