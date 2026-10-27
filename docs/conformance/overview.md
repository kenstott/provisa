# Conformance

Provisa checks every protocol and query language it serves against a public test suite. Each suite runs in CI against a test instance after every green build, and each page here reports the results: the suite, its version, the current pass rate, and every deliberate difference with its reason and workaround.

## How conformance is measured

After every green build, CI provisions a fresh test instance and runs the conformance suite against it. Pass rates come directly from that output and are never typed by hand. Each page reflects what the suite last reported.

Provisa fixes failures or lists them. No failure is left unnamed.

## Why differences exist

Deliberate differences group by cause, which also indicates the workaround.

**Writes.** Provisa supports writes that map onto the model through governed table writes. A write needing structure the model lacks is refused with a message naming what is missing. Transaction statements (`BEGIN`, `COMMIT`, `ROLLBACK`) are a no-op by design; the governed pipeline does not expose transaction boundaries to callers.

**Vocabulary fixed by the model.** A new class, property, or relationship type belongs in the model as a table, column, or ontology term. The data store holds instances; vocabulary goes in the model.

**Add-ins.** APOC procedures, GDS algorithms, custom SPARQL functions, and Postgres extensions appear in conformance suites. Where a procedure is commonly needed, Provisa offers it as a command. For others, run it in your own store by querying Provisa live (via `SERVICE` in SPARQL or `apoc.bolt.load` in Neo4j) or by loading the governed graph into it.

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
| pgwire | Postgres conformance suite + driver suites | [pgwire.md](pgwire.md) |
| Arrow Flight SQL | Apache Arrow Flight SQL integration scenarios | [flight-sql.md](flight-sql.md) |
| REST / OpenAPI | TBD: suite to be chosen | [rest.md](rest.md) |
| JSON:API | TBD: suite to be chosen | [jsonapi.md](jsonapi.md) |
| gRPC | TBD: suite to be chosen | [grpc.md](grpc.md) |
| MCP | TBD: suite to be chosen | [mcp.md](mcp.md) |
