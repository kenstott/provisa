# Changelog

All notable changes to Provisa are documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/).

## v0.1.0-alpha.499

Released 2026-11-04.

### Changes

#### admin

- Gate the ERD layout routes; drop requirement ids from fetch-paths
- ERD layout reads open to every signed-in role

#### api

- A query node serves no UI or UI backend

#### bench

- plan-cache hit/miss knob and a rows-and-masks governance variant
- The mask share counts the contract's columns, not every deployment column

#### codesource

- Code images carry provisa._encoders from the provisa wheel

#### config-watch

- Stop reading an org reaped during a check

#### conformance

- An overload with no compared answer is not checked, never a pass
- Side effects compared on pg, type and form for values that cannot be
- Every overload is passed, skipped by design or failed
- random()'s form holds for random() alone
- Real arguments for the overloads called with NULL only
- The 2026-11-03 decisions and the catalog's model skipped by design
- Not checked and not reached count as not tested, not failed
- An adapted case is marked with the outcome it counts as

#### cypher

- The maximum depth defaults to 1000
- A walk along a relationship of a table to itself passes the guard with its trail check

#### demo

- A built-in view ending in a comment no longer swallows the script terminator
- A template's build adopts its seed before starting replica convergence
- Drop "landed once when the demo is built" from source descriptions

#### demos

- Carry a demo's scans for terms in its bundle

#### deploy

- Verify sends a page-load request

#### docs

- Count 76 source types after Generic SQL and QuickBooks merged

#### encoders

- JSON surfaces answer from the Rust encoders; Python serializers deleted
- The plan's columns are the statement's first; integration and unit tests on the encoders

#### erd

- A saved layout's time is set by the database on every save
- A failed layout delete frees its confirm; the Layout menu's hinted trigger is listed
- The delete confirm comes before the Save-as dialog, so no save's await sits inside its busy span
- The automatic layout overlaps no two tables of a domain

#### executor

- DuckDB reads hand the terminal Arrow, rows built at the consumer
- DuckDB async rows built off the event loop; DIRECT driver reads Arrow
- Columnar reads answer PostgreSQL's error met mid-stream; 24:00 times

#### experiments

- Pgwire encoder benchmark and results
- Pgwire encoder benchmark and results
- Pgwire encoder PostgreSQL floats, named zones, ns units
- Pgwire encoder writes every type pgwire sends

#### explore

- OData and OpenAPI follow the navbar domain selection; Polly drives OData

#### federation

- Ship each generic engine kind's dialect as a named extra

#### glossary

- Fill the expert picker from a glossary_rw roster, not the team page's
- List only the caller in the shared sandbox's picker roster

#### go

- A Go source's reader and the manifest's Go rules
- Pinned Go toolchain; a stream yields a row and an error
- The Go host, compiled with the code linked in, and its sandbox

#### golden

- The GraphQL front gate's expected answer is the encoder's

#### graph

- A dragged SPARQL node stays pinned and the drawing is kept

#### graphql_remote

- Land connection tables at the page size registration priced

#### grpc

- Write values exactly
- A zoned nanosecond timestamp is floored to the microsecond

#### ir_arrow

- Read an Arrow UTC timestamp as offset zero, not a region

#### jdbc_source

- The attach check reads from the source before it passes
- The JDBC form picks a driver, takes a jar file, names no engine

#### org-registry

- Grant and register the registry view under the hold that rebuilt it

#### pg engine

- ordered-set aggregates, overlaps, character(n), json text, bounded arrays, interval elements
- Overlaps and quoted table functions reach PostgreSQL as written
- Catalog reads in regclass arguments, the catalog's function oids, pg_get_viewdef's size, clock and NULL types
- Amvalidate from PostgreSQL 17's opclasses, database_to_xml without Provisa's domains, decided errors

#### pgwire

- Hashes of time, timestamp, timetz, uuid and pg_lsn, float8 finals of a column
- ClickHouse reads date, time and timestamp literals, refusing only what it cannot read
- A time literal in a ClickHouse statement of these forms is read by time_in
- Zoneinfo timestamptz text, float4out for REAL, UUID bytes
- Delete the Python DataRow writers; every cell is the Rust encoder's
- A schema-named function in FROM is no table; regexp_replace reads its flags beside N
- Sequence functions and six-argument regexp_replace refused by name, skipped by design
- Lastval from the session's own inserts
- A bounded array sent as PostgreSQL sends it; lastval documented
- Inet text is PostgreSQL's whatever Python's ipaddress writes
- Advisory locks show in pg_locks; a lock with constant keys may sit in a larger statement
- set_config keeps a session's setting; current_setting(name, true) of an unknown one is NULL
- A sleep's time may be read from the clock
- A simple query's error ends at its ReadyForQuery
- LISTEN, UNLISTEN, NOTIFY and pg_notify refused by name
- A pytz-zoned value and a UUID's 16 bytes are sent as main's writers sent them
- pg_cursor lists the session's cursors and its open portal; pg_column_size of a computed value; json_object is the engine's
- A zoned nanosecond value is floored to the microsecond, a UUID is written as main's _uuid_text writes it
- Notices from the current lock; no requirement id in fetch-paths' heading
- ReadyForQuery reports 'E' after a failed statement in a transaction
- DuckDB answers System functions as PostgreSQL 17 does
- Answer an engine error met mid-stream, keep the connection
- pg_timezone_names left as it was, pending the maintainer's zone list
- regexp_instr's UDF is DuckDB's alone; the abbreviations' rows name their own columns
- DuckDB PostgreSQL-function leftovers
- pg_timezone_names in FROM is its rows; a table's XML row unpacked
- pg_timezone_names' rows name their own columns
- Timetz text, zone abbreviations and pg_timezone_abbrevs in FROM as PostgreSQL 17
- DECLARE, CLOSE and MOVE over the extended protocol run as the simple Query's
- A regprocedure's argument types compare as text; amvalidate waits on a decision

#### pgwire catalog

- PostgreSQL 17's predefined roles, strict catalog functions and statistics
- Reg types write meta's names; information_schema at PostgreSQL 17's oid
- A predefined role's privileges and acldefault answer as PostgreSQL 17
- Tablespace location, object identity, updatable columns and input checks answer as PostgreSQL 17
- The functions and descriptions PostgreSQL 17 ships are in meta
- pg_get_catalog_foreign_keys and pg_settings_get_flags answer as PostgreSQL 17
- pg_get_viewdef gives a system view's definition as PostgreSQL 17
- The indexes and constraints of PostgreSQL 17's catalogs are in meta
- *_is_visible follows search_path as PostgreSQL does
- A privilege function asked about another model role answers from its grants
- A source's enum types are types of the catalog

#### polly

- Read and operate the screens added since alpha.498

#### profiler

- A rebuild drops superseded result relations after its commit, 50 to a transaction; a sandbox copies only current ones

#### quickbooks

- Live sandbox test passes: quoted numbers, api_sources type check, one page size

#### reference-library

- Read eCFR's XML

#### sandbox

- The VM idle stop counts an open under way and the last sandbox request
- An open times its ask for a newer version and its waits for stores and template
- The VM idle stop counts an open under way and the last sandbox request
- An open times its ask for a newer version and its waits for stores and template
- A template is served as built only when it holds its bundle's profile runs
- A sandbox opens holding its template's Kafka-landed rows, its listener continuing from there
- A dropped open answers starting, and an open builds none of its template's runtimes
- A template's runtime build keeps the profile runs its bundle carries
- A patch keeps demo templates built under the same DDL
- The DDL fingerprint covers every source of an org schema's DDL
- The sandbox VM is not stopped under an open sandbox or another tier's visitor

#### serialize

- Exact JSON values in GraphQL and the shared JSON path
- Exact JSON numbers, json as text, integers by type

#### sources

- Generic SQL source type over any SQLAlchemy URL
- A held org's Generic SQL URL carries no driver options
- Held orgs give only allowed Generic SQL URL options; URL is pinnable
- Generic SQL form and Detect
- A customer JDBC driver behind Generic SQL
- The Generic SQL floor, read by column_value parameters
- A branded source's setup is Source.brand_setup, not a hint
- Read a Shopify source saved with its setup in federation_hints
- Generic SQL lands on Unit against main

#### start

- Build provisa._encoders before a source start runs the API

#### start-ui

- List every query surface in the startup banner

#### table-search

- Find a remote source's tables by description; no stale form problem
- The table named for the word ranks above narrower kinds of it

#### tables

- The table picker lists lookups apart

#### term-finding

- A table of terms and definitions, read with no model

#### tour

- find_terms, scan and library steps for a named demo

#### transpiler

- J455 slope is infinite on a vertical line, doubles compare as numbers
- J455 circle, lseg, path, polygon, line forms and polygon containment
- J455d the geometric forms left and width_bucket, on columns
- J455c2 the geometric forms plan in seconds on Trino and ClickHouse
- J455e Trino defines each geometric helper as a routine of the statement
- J455e a Trino statement's routine head survives what wraps it; floats replay within 1e-14
- J455f geometric bare names are refused rows; main's tests read Trino routines and ClickHouse SETTINGS
- regexp_instr's sqlglot-written forms are the OWN step

#### trino

- A list field's empty default is text like its json_format aggregate again (#286,)

#### tz

- One IANA release (2026f) for zoneinfo, pytz and the Rust zone tables, brought forward from fix/tz-one-release
- The JSON encoders' zone tests and lock at IANA 2026f, from fix/tz-one-release

#### ui

- OData viewer strings in every locale
- OData viewer keeps a query being built open on Escape
- A coordinator-served UI sends its data calls to a chosen data node
- A detail panel's delete works like its save
- A glossary term's Delete works like a save
- Modeling wizards relate each satellite to its hub and each effectivity satellite to its link
- ERD image controls in a hover bar, the open item highlighted, domains packed side by side
- The ERD keeps a domain the domain filter does not offer (ops)
- A table's definition is badged as defined in the table
- Move the ERD's hint off the diagram into the toolbar's (?) help

## v0.1.0-alpha.498

Released 2026-11-02.

### Changes

#### audit

- A governance read on the writer thread runs in the request org

#### bench

- A role chain lists what the role holds

#### demo

- Ground "group of connected clients" in groupexposure

#### notebook

- Read and write build text as UTF-8 on every OS

#### odata

- OData V4 service, first slice
- $filter, $orderby and the odata request timeout
- $expand, nested paging and the e2e test
- Show the endpoint in the startup log and the admin console
- Carry statement warnings in the body as @Provisa.warnings

#### pgwire

- Enums by a table's schema, audit the text sent, casts refused by name
- Timestamptz text, SHOW and ParameterStatus follow the session zone
- Type operators reach Trino and ClickHouse
- A Trino instant reads into UTC, a built-in type is no enum, a timetz keeps its encoder
- float4out takes Trino's digits from text that reads back
- Exact binary numeric past 28 digits, integer interval day split

#### polly

- Add the /odata page to Polly's page table

#### release

- Unblock Build Provisa Packages (guava CVE, arm64 docker save)

#### rights

- Viewer holds no catalog_read; a role map fits its panel on every open
- The Cypher and catalog-fact checks read only what their branch needs

#### startup

- The boot reads a schema's catalog once, not once a table

#### transpiler

- A call by pg_catalog is the call of the function on an engine without that schema
- The round-trip forms in PostgreSQL syntax, a numeric literal keeps its digits
- Numeric forms in PostgreSQL syntax, refused by name where a replay differs
- String and date forms in PostgreSQL syntax, refused by name where a replay differs
- Position edges, first match, and refusals from the replay on real engines
- The second replay on real engines read back into forms and refusals
- The third replay read back: a NULL part, zoned literals, an answer's scale refused
- Nameconcatoid keeps its text type, jsonb_numeric restored
- A large object form reads its bytes as bytea on Trino
- A bytea argument of a server form stays bytes
- A time beside a date stays refused on ClickHouse's server
- A server's state answered, and regexp_replace's Nth match, recorded on postgres:17.11
- regexp_replace's Nth match compiles on ClickHouse, repeat is a text on Trino
- A tid's and an aclitem's forms reach the engine past the held-type refusal
- A source's enum keeps its type on the wire, its functions on every engine
- A captured group reaches the first match on Trino
- Repeat has a form on every dialect, so only the rule's own step goes without
- A positioned regexp_replace reaches its row past the first-match mark
- Repeat of NULL is NULL for any count, N overrides the g flag on DuckDB

#### ui

- A copy button names its hint, the one tooltip
- CopySymbolButton keeps its title

## v0.1.0-alpha.497

Released 2026-11-01.

### Features

#### Business Glossary & Ontology

- A term's lifecycle is one status: draft, live, deprecated or retired.
    - PII on terms is its refusals in every UI locale
    - A term's status -- draft, live, deprecated, retired
    - A term's status -- Draft, Live, Deprecated, Retired -- and Deprecate, with a retired term's record
    - Term status is set by people, grounding is computed; agents see approved grounded vocabulary
    - Term list chips are icons (Box, Tag, Lightbulb, Eye, PencilLine)
- The ontology reports what a curator should look at as findings: GET /admin/glossary/findings (glossary_read, scoped to the caller's domains) returns each term with its findings, each a kind, a message code with its parameters, and a message.
    - Findings -- what a curator should look at, per term
    - To review reads the server's findings; Retire takes a successor and asks about lost tags
    - A product's status -- draft, live, deprecated, retired -- its lifecycle dialogs, findings and withdrawal from catalogs

#### Column Masking

- Every regex a user gives Provisa, a mask's pattern and replacement among them, is written in one universal dialect: the portable subset of literals,., sets [...] with ranges and negation, the greedy quantifiers * + ? {n} {n,} {n,m}, the anchors ^ and $ (the whole value), groups ( ) and (?: ), alternation |, and \d \s \w as their ASCII classes; a replacement is literal text with $1-$9 and $$.
    - One universal regex dialect, refused by name at save and lowered per engine
    - Preset placeholders as {first}/{last}; alternation warnings in patterns with ok true
    - A regex mask fails closed -- an unmatched value is fully masked
    - A null result value arrives unset, not as its type's zero

#### File & Lake Sources

- A files source reads each Excel workbook as the tables it holds.
    - A files source's Excel hints, on its form and in its mapping

### Changes

#### 3d

- A 3D stage draws on demand and nothing while unseen, so an idle page costs no CPU
- A turning stage stops after 10 s with no input on the page, and the core e2e lane runs with reduced motion
- Every network graph opens on its best view, turns about its spread plane's normal, and has a 2D view

#### admin

- The catalog and a table save stay within the acting role's grants
- A column's dependents are answered as their caller may read them
- Refusals name only what their caller may read; new tables answered at once
- The profiler, DQ and findings answer within the caller's grants
- fakes, synthetic data and the glossary within the caller's grants
- profiling, audit text and observability within the caller's grants
- Edits and delegations stay within the caller's reads
- Refusal texts name only what their caller reads
- capgrants10 -- a request's acting role reaches every surface
- capgrants11 -- the workbook answers a table saved since the build
- An OpenAPI source whose document is out of reach fails no Tables listing; its tables say why

#### agentdata

- Ask over two weeks, where customers do order twice

#### aggregates

- An aggregate or group-by with its nodes is one statement

#### analyze

- Cap the drawn plan tree under Mermaid's 500-edge limit

#### api

- The build loads each domain's stored graphql_alias
- Answer a MERGE/form or RETURNING write refusal as a 400 by name
- A client's error never names an internal schema
- An error naming no org word has no internal schema to take out

#### askamerica

- Prepare the adapter's catalog on first use; a key-check timeout is AskAmericaUnavailable

#### athena

- registerTable records the table format; a Hive MERGE is refused as a MERGE

#### audit

- A governance read on the writer thread runs in the request org

#### auth

- The login throttle keeps no failure past its window
- A named role set acts as its meta-role on every route, unsecured
- Unsecured gRPC and Flight bind a derived role's attribute; Bolt databases name derived roles
- A principal's role assignments are read one way on every transport
- HTTP's rules after the read apply on every transport

#### bench

- The lifecycle sizes every device's boot disk with one flag
- A ready device waiting for the others keeps its idle hold; each device's setup log is kept before destroy
- The backends device can be given its own machine type, its stores being sized for a large box
- One service builds the perf Postgres image; the engine runs it, so a parallel build no longer fails on the shared tag
- The backends agent is started when it does not answer, and the serve stop matches no shell of its own
- Seeding is its own job stored as an image; runs boot from it, checkpoint each variant and resume after a preemption
- Soak profile, sampled server tree, leak probe maps and pools
- The features test's first CI run, triaged
- Masks are checked per table, and MCP is pinned every role a features request names
- A local soak validation on the test instance, its contract readable everywhere
- A step's first error names the request it answered
- A sparql transport, its own dialect, rendered from the same contract
- The row-level refusal on gRPC, table TTLs in the apply phase, optimistic cells
- The Artillery ramps check their answers; the Cypher ramp uses live labels
- A saturation ramp's failed expectations do not end the sweep
- Bolt saturation coverage, Cypher cache opt-in, GraphQL ramp, no pg stand-ins
- The three-device server names a public URL for the SPARQL vocabulary
- The governance pass reads only orders, with columns to mask

#### bolt

- One statement per RUN; a transaction's results pulled by qid
- The browser's graph totals are unknown, not 0, when a count fails
- A commit and an auto-commit result end with a bookmark
- Temporal and point values travel as Bolt structures
- Only the parse decides a statement is a write; a write's own error is its answer
- neo4j names the home database
- Summary counters for Cypher writes on both write paths
- A Bolt RUN runs in its principal's graph session
- Answer CREATE/DROP DATABASE and select a created database

#### bolt, cypher

- Bolt and the Cypher route run a write through written()

#### build

- The model stamp is read after the build's own effective-name writes
- A reload writes no relationship name already stored
- A column transform's grants are folded over each role's ancestors

#### calendars

- The saved message names a calendar's version once ('v1', not 'vv1')

#### catalogs

- A Catalogs panel holds each catalog connection once, by domain

#### clickhouse

- A query that matches no row still names its columns
- The engine runtime's query names its columns when ClickHouse sends no block

#### codesource

- No engine attaches a code host; the image's front end serves no reads
- Code sources answer their own statements, direct and attached
- Code rows streamed end to end, a connection per concurrent call
- A code statement is complete only when read direct; a null test names an aggregate's expression
- Credit flow control, and a code source's DuckDB under the operator's bounds
- The read context, (inputs, context) for every table and stream, no parameter-as-filter
- Speed and memory as built, measured
- SQL and GraphQL mutations on a code table, routed to its commands
- A statement that fails stops reading the function's rows
- A worker closes its socket only once its reader has left it
- Connecting a remote host dials it to pair and to check, no third time
- A worker closes a lane's socket only once its reader has left it
- A code host's DuckDB is opened capped, by its source's own bounds
- The image copies core's engine_memory beside its front end; no symlink
- A column transform streams its read through the workers in 1000-row batches
- A column transform in a CTE is a staged, selective step
- An INSERT... SELECT whose CTE holds a column transform is refused saying so
- A staged CTE is typed by the model, its reads take their own parameters, embed() folds with them
- A pulled call keeps the request that asked for it
- A dropped pairing connection is a named refusal

#### commands

- SQL expression functions in SQL statements, acceptance test, locales
- Implicit meta reach for schema build and command admission only; defaults fill without refusing
- APOC path walks either way from a label back to it; docs for defaults
- A function the engine cannot write is refused by name at compile; sources route around; anomalies page generated
- A domain-prefixed command field is registered under the alias the schema names it by
- VOID commands; implicit-argument CALL in Cypher

#### compiler

- A where states a relationship's condition, compiled to EXISTS over its target; ilike matches with case ignored
- A session's temporary table comes ahead of a model table of its name
- Dropping an API table's union branches keeps a recursive CTE valid (#312)
- A relationship target is read under its join alias
- Pg engine reads a key-bounded nested field once

#### config-export

- A contract's created_at is the store's stamp, not written into the config export

#### conformance

- Type the pgwire fixture's columns as registration does
- The pgwire seed types a subtype as registration does, never sampling money
- TCK setups run with their query in one ephemeral session over Bolt
- The TCK's Bolt sessions sign in as a user mapped to tck_empty
- Bolt examples run as written under a role of their own
- TCK session setup ignores post-query steps; compare side effects
- TCK named graphs run their script in a session; test procedures are commands
- Every case is passed, skipped by design or failed
- Client and suite skips are skipped by design
- Cite driver and suite source for client and suite skips
- A TCK procedure's command is assigned to the roles that call it
- A control query on an empty graph reads the empty session
- A Bolt date time at offset zero is written Z, naming no region
- A pg_catalog function not served is failed
- Skip internal and index catalog functions by design
- Hash functions leave the Index functions group
- Form checked, a third status in the functions run
- Catalog function results by engine with rule footnotes

#### contract

- A column is declared, never promised; Promise means a data quality check
- Panels in three tiers on a shared layout

#### contracts

- A contract keeps its versions and names its breaking changes

#### core-lane

- DuckDB databases opened capped; a Kaggle token is its own credential; provision.py argv; tour clickBefore once; a single-file draft table
- Every DuckDB the base opens bare is capped too; OpenRouter passed once; Fuseki's heap sized for the e2e

#### cypher

- A GraphQL query of several root fields is a Cypher statement per root
- A GraphQL root's Cypher means what its SQL means
- A junction root is Cypher only for its declared types; one-table columns and IN lists
- A Cypher box of several statements answers one result per statement, each under its own tab
- A Cypher read answers what the statement says
- A single-hop type alternation over one junction's types
- A filtered root keeps its filter; a junction field or condition is its relationships
- A table semantic SQL names before its physical alias has its label
- $3 is the parameter named "3", not the number 3
- A query's Cypher carries the values its SQL binds
- A read using the engine's functions is answered by the engine
- A list function answers on DuckDB, and a slice is 0-based
- A map's key is read with.key, and a carried variable returns only itself
- A junction field's end node, built as one JSON object, has its Cypher form
- Open as query keeps a to-many field's own order and limit, or refuses
- A WITH right after a WITH reads the rows that WITH carries
- An UNWIND variable is each element, not its UNNEST's whole row
- An unlabelled node read backward is the relationship's target
- A node a property narrows to one type is built from its arm; a property of a node is a value, not the node (#292,)
- DESC puts nulls first; LIKE exported exactly; DuckDB LIKE escapes as PostgreSQL
- =~ matches the whole string; ENDS WITH is literal; Trino LIKE escapes as PostgreSQL
- A read missing a required parameter is a 400 naming it, each parameter once
- Every graph-route error is the app's error body; the frame says the reason and lists each access violation
- A mistake in the user's statement is refused at translation in their own terms
- An unbound name that is a bound variable's property is offered as it
- Every surface offers a bound variable's property for an unbound name
- A returned map literal answers its values, not their JSON encoding
- A property the label has not is null; every node's key is its _id
- Every node object and every walk holds a node's whole key
- What a read cannot answer has a code -- a failed conversion is null, Infinity is a 400, an engine's refusal is a 400 on every engine
- A read refused by name is answered with its own status and code, not a 500
- A refusal by name in a CALL, a write or the Neo4j endpoint is answered as itself
- A MATCH takes comma-separated patterns
- An OPTIONAL MATCH's WHERE is that match's condition and never removes a row
- An OPTIONAL MATCH takes comma-separated patterns and matches whole or not at all
- length(p) of a recursive path is its hop count; an undirected walk recurses both ways
- A statement with no pattern is a SELECT with no FROM
- Map arguments in CALL; number literal forms, ^, RETURN *, WITH DISTINCT, deep literals; parse errors are refusals
- mixed-type list literals, time values, deep-literal and map-temporal refusals by name; write refusals name the forms read
- Unaliased RETURN expressions named as written; sequence() is engine-only
- Relationships written without brackets (--, <--, <-->) are read; a single dash is refused
- A variable naming two things, an aggregate in a list comprehension and length() of a node are refused
- A WITH carries each node it names as that node (#293,)
- date(), datetime(), localdatetime(), time() and localtime() are casts or the engine's current value
- Column name conflicts, UNION column and kind mismatches, integer overflow, unknown string escapes, non-boolean operands, aggregates of rand() and a value matched as a node are refused
- Every unaliased RETURN expression, a property included, is named as written (a.name)
- Durations -- ISO-8601 answers, properties, duration(), between/inMonths/inDays/inSeconds and arithmetic, on the engine's interval
- Temporal values from component maps, Cypher's written forms and other values; answered as Cypher writes them
- date.truncate, datetime.truncate and the rest, with components put in after
- A date's, a time's and a date time's parts read as properties
- A zone the query names is computed with and presented; microsecond precision, nanoseconds as zero
- size() of a list written in the statement is counted there, size(null) is null; tested on DuckDB, PostgreSQL and Trino
- size() of a registered text column is its character count on every engine
- size() of any string -- a string function's value, a concatenation, a variable bound to one -- is its character count
- An edge touching a table SQL alone serves is no relationship of the graph
- A float divided by zero is Infinity or NaN, as openCypher answers; an integer divided by zero stays an error
- A statement that is not Cypher is "Invalid input", as Neo4j says it
- A node matched with no label keeps each property's type, and has labels
- XOR is two booleans differing, written as SQL reads it
- An integer divided by an integer is an integer
- IN, string predicates and IS NULL bind between + and a comparison
- List quantifiers over nulls are three-valued
- A list + an element appends it, an element + a list prepends it
- reverse() of a list reads it last to first on every engine
- keys() of a map answers its keys; m['k'] reads the key's value
- Values of two kinds are never equal and have no order
- A function inside collect() or another renamed one is lowered too
- properties() of a map is the map, of null null
- A CREATE answers its RETURN, and a junction relationship is written between matched nodes
- A string + a value is the two as one string
- time.truncate() of a time or a date time keeps its offset
- A node CREATE without a variable is a write
- duration() reads a computed ISO-8601 string with negative or fractional parts
- collect() skips nulls, and collects nothing as []
- A relationship CREATE reads each matched node apart
- toInteger() cuts toward zero; SPARQL's xsd:integer casts as XPath does
- Values the statement writes wholly are equal, in, and ordered as Cypher says
- A CREATE giving its node no property is refused naming the forms read
- A map a WITH carries keeps keys that differ in case apart
- A list of several types built from lists answers its values
- CASE, string predicates and indexes across kinds; UNWIND null; RETURN * by name
- An item of a list of several types has its size
- A key every unwound map literal gives one type reads as that type
- An item of a list of several types answers its value, in openCypher's order
- toString() of an item of a mixed list, and a key of a nested map literal
- NaN is equal to nothing and has no order, on every engine
- A string's \uXXXX escapes are read; the TCK runner reads cells as Gherkin writes them
- range() with a step of 0 is refused by name
- [*..] is refused as [*] is; unbounded patterns are by design
- A list or map literal of any depth is read; MAX_LITERAL_DEPTH is gone
- range() with a computed step of 0 is refused by the engine
- An OPTIONAL MATCH from a variable bound to null matches nothing
- A deep literal with computed items reaches the engine without its nesting
- A list slice counts a negative bound from the end
- Temporal values beyond the engine's bounds are refused by name
- A computed list index is 0-based, as a written one is
- A key of a map read from a list of several types is read
- A string indexed like a list is refused; a string parameter is a string
- A map or key bound to null indexes to null
- datetime.fromepoch() and datetime.fromepochmillis() are read
- Values the client gives are read by the engine, never written into the SQL
- A collect inside size() collects; an answer of no rows names its columns
- ORDER BY reads ASCENDING/DESCENDING and nested items; a pattern is a predicate
- A collect() after WITH... ORDER BY collects in that order
- One graph session per principal, the same on every client
- Graph session id is hex, grants read through the one rule, locales
- A kept translation is reused only for parameters of the same temporal kind and zone
- A junction CREATE may end on rows the same statement creates
- Governed DETACH DELETE of model nodes and junction-row DELETE
- A MERGE reads unrelated leading MATCHes apart
- REMOVE on model nodes follows Class labels and properties refused by name
- A node of several labels carries the columns its readers use
- An unlabelled match over no node type reads an empty relation
- Row counts of openCypher TCK scenarios over a session's graph
- A graph session walks a path function's unlabelled end
- An empty node union registers its columns like an arm does
- Session caps refused by name; TCK scenarios start empty; duration lists
- An undirected variable-length walk takes only relationships its start can reach (#311)
- A path search needs one end bound or labelled; a pattern no relationship matches is no rows (#313)
- A session path from an end the model places walks the model's relationships (#313)
- An undirected hop reads both ends; an unlabelled match keeps every node
- A path variable read as a value
- Arguments the engine has no overload for
- ORDER BY ranks nodes, relationships and paths in openCypher's order
- A WITH's WHERE reads the variables before the WITH
- A comprehension's bound target is that node; a union node keeps its types
- A list of graph elements and literals is a list of several types
- one-hop pattern comprehensions with unnamed ends, a named relationship or several types
- Ordering comparison of two lists is element by element
- TCK side effects as observed; Bolt session reads keep graph identities
- One command-call runner; a standalone YIELD of a VOID command is refused
- A node only a session holds carries no _id
- SET of maps and session nodes answered with every label
- Nodes a returned map holds get their own Bolt ids
- CALL as openCypher reads it; a Bolt read of no rows names its columns
- MERGE writes ON CREATE / ON MATCH properties of the session's objects
- openCypher value semantics for the TCK expression features
- SKIP/LIMIT expressions, keyword variables, carried unlabelled nodes
- A conversion of a kind it does not take is a TypeError
- keys() and n[key] of a session relationship read with any ends
- A projection is read by what it keeps; a shadowed alias is its new value
- A node a WITH carries is read on as that WITH's node
- Maps a WITH carries; a WITH ordered by nothing
- Lists of union elements; variables a WITH drops; sub-microsecond orderings named
- collect() is the elements, count() counts rows
- A date moves by a duration's whole days of time; a session time keeps its accessors
- A session node's _id is left out of the answer, not its reads
- Session DELETE of any target, REMOVE keys, CREATE {} and graph functions
- A read after a write finds unlabeled session nodes among the session's; DeletedEntityAccess
- A session hop's ends are the session's nodes and the model labels its relationships end at
- Session reads the TCK found wanting: types told apart by case, r:T, deleted type(r), WITH *, map expressions

#### data

- Reading over GraphQL and SQL needs no query_development

#### data-contracts

- The meta goldens, the editor's service levels in form fields, the page's mocks
- A contract's column and type stay on one line in its card

#### data-products

- A product's quality claims on its detail panel, from the meta domain
- The claim field's hint on its FormField within 90 characters, the claims' SQL name built apart, the read view's exemption for the claim label
- The panel's query text has a bounded height, so the sections around it pack
- One Inputs section, and Related lists the directly related tables only
- Edge lists keyed by the edge's id; a source input is named by its address
- Every reference the panel shows is the model's name; cards hold their content; related detail clamps
- Cards keep their content and remember their state; the product graph has the Ontology View's controls
- The Knowledge graph card is compact again, the full explorer in its expanded view; the product's lineage DAG is back across the panel's foot
- Input Ports are one row per upstream table, its columns behind an expand; the lineage DAG names tables as the catalog does, heads its lanes in words and opens legible; no Queries card
- A lineage relation is named by the catalog when it is a registered table, by its relation when it is not; the knowledge graph's outside tables too
- Each Related Tables entry is held to three lines; the Knowledge graph card is a glance with no controls
- The knowledge graph card is drawn from the panel's own meta read
- The panel reads relationships, source types and DQ rules from meta
- A product's lineage is its own endpoint, read by any reader of it
- A product's data quality is its expectations, promises, related and unattached checks
- The data quality section is three rows, Expectations, Promises and Related
- A check's latest run sits beside what it scans, so neither is clipped
- The owner sees the product's own side; any other reader its promises
- Edit follows the right to edit a product, not the owner's view
- A product's lineage names its consumers, one hop downstream
- The owner sees who reads the product, one hop downstream
- data_product_owned_rw edits the products whose owner role the caller holds
- Each output port declares its own versioned ODCS contract; the product's prose SLA moves into it
- ODPS document complete; created_at shown read-only
- Contracts panel as ODCS section rows; derived ODCS 3.1 elements in the exported contract
- Read contracts.json only for a product with a contract
- Contract summaries use i18n plurals; schema properties open to every element
- Contract Quality card spans two grid columns at 3+
- Contract cards flow in columns; Promises is a full-width panel of separate items
- The promises table builds its results name through the shared helper
- A contract's sections are peer cards and its Promises count is its rows
- A contract has no consumers and its Columns runs full width
- How to use replaces a contract's Servers section

#### database

- The LISTEN thread's wake pipe is closed only once the thread has ended

#### demo

- groupexposure's terms step opens the ontology page
- Publish groupexposure, its card from its real answer
- The sandbox tier serves groupexposure, now published
- An open during a template's preparation waits for it, never builds it again
- Insurance and weblogs vocabularies grounded by narrowing and synonym edges
- Raise DEMO_FORMAT to 3 and requires_provisa to a496 for demos using role parents or code inputs
- Refuse a bundle in a newer demo format than this Provisa reads
- Provisioning close step reads true inside its window too

#### demo warm-up

- Each tour statement is sent with the time travel the steps before it set

#### demo-bundles

- A seeding run that times out names each template replica's build state
- A regions demo is set up and seeded on one set of region stores
- A regions demo's bundle carries its bitemporal views' setup history
- A regions demo's seeding nodes run together, each on its region's address
- A seeding run that cannot seed its template ends the build at once, by name
- A seeding run builds each view it keeps that nothing else builds
- A regions demo's setup history is found in its region stores, not its config
- A region's views are seeded after its replicas land; a regions sandbox runs in its region's store

#### demo-stores

- A taken-down version's ES marks are deleted; a missing index is restored

#### demo-templates

- A node without a ready template's folder prepares it again
- The version a template was built from is the control plane's, never a node's file
- A regions tier records its finished template, so an open does not replace it
- One step writes every carried setting a demo declares to its template

#### demos

- Publish the data room demo on the sandbox tier
- first-launch's sandbox tier offers the data room demo too
- Publish the reproduce demo on the sandbox tier
- Publish the opioids demo: its card, the sandbox tier's bundles, the first launch and the live demos page
- Publish the pharma demo: its card, the sandbox tier's bundles, the first launch and the live demos page
- A scaffolded demo's init script is executable; the walk's unpublished example is perf
- groupexposure, a bank's loan book rolled up through Wikidata by LEI
- groupexposure's borrowers are fictional, their owners real
- Publish the retail demo on the regions sandbox tier
- A seeding run that never finished shows its node's log, the tables it re-created and its embedded store file
- A seeding node's whole log is printed into the build's output once it stops
- The regions sandbox tier runs a node per visitor region and serves banking
- A published demo in regions must be one the regions tier serves
- The regions check reads each demo of name=region entries
- "Give an agent your data" named demo on the MCP explorer
- The profiling demo's card is its run's terrain
- A bundle started locally loads the profile runs it carries
- The visitor walk sends agentdata's Connect step, so its bundle can be published
- A live demo comes up from cold with a true wait and no failure on the way
- A source save rebuilds the schemas once, and slow steps are named
- Shared connector servers, a consumer group per listener, public limits
- per-schema bootstrap lock, one commit per seeded table, no template runtime without stores

#### deploy

- The deploy package ships the vendored buenavista with provisa

#### deploy-cloud

- The regions sandbox tier gets the deployed tree and its own config
- Drop each org schema in its own transaction so the reset never runs out of locks
- The sandbox reset drops sandbox schemas in the databases each store holds

#### deps

- Black 26.10.1, graphql-core 3.2.13, setuptools 84.2.1, virtualenv 21.14.13 past their CVEs
- Filelock 4.2.4 and opentelemetry 1.45.1 past their CVEs
- msgpack, dnspython, pymongo and oauthlib past their CVEs
- Pypdf 6.21.4 and pyasn1 0.6.5 past their CVEs

#### discovery

- Registration records the keys the source declares
- FK tracking raises all but a starting server; tests recorded
- A catalog read cancelled while the source prepares its catalog is retried, not left unindexed
- No adapter read is sent while its catalog is being prepared; answered STARTING at once
- A PostgreSQL source's keys are read from pg_catalog by the table's name

#### docker

- The image trusts Microsoft's 2025 signing key, which signs its Debian 13 repository

#### docs

- Fix gen_docs_index.py to rewrite docs/ fragment links
- Conformance overview as overview.md (docs/*/index.md is gitignored), unblocking the site deploy

#### dq

- A checker table's latest run, in the meta domain
- A preflight calls Provisa's services in-process, dq among them; a view may hold on failing checks
- Name tables as the model does, in what dq reads and answers
- A read governance refuses is a 403 naming why
- A checker the role does not reach is left out, not read
- A table with no contract stores dq_rules as SQL NULL
- Each checker run logs its checker, its time and how it ended
- A checker source's connection is what its steward entered; the model never resolves it (#276)

#### dq,types

- A checker's empty password is stored empty; Exasol's character-set spellings are text (#276)

#### dq-claims

- The merged panel, its fixtures and the corpus agree with main

#### duckdb

- A statement's null order is a sort key, never NULLS FIRST/LAST (#294,)
- What the server store's own connection writes is readable through the engine's attach
- PostgreSQL's json_build_object and jsonb_build_object run on DuckDB as json_object
- A division inside another's divisor divides and is guarded too
- A table landed in a server store is seen through the engine attach

#### embedded-pg

- A sign-extended segment key matches the key the system lists
- A segment key with its top bit set is read as the system lists it
- A segment key with its top bit set is found, and so released

#### engine

- A statement the engine answers answers as PostgreSQL does -- division by zero, infinity, a function aliased in FROM
- A function named in pg_catalog is that function
- A UUID read beside a date is the uuid.UUID it is

#### ephemeral

- A session's ephemeral graph store, typed and widening
- The governing rule and label resolution for a CREATE
- An ephemeral CREATE makes its nodes and relationships in the session
- The read overlay, and proof the governed path is unchanged
- A node naming two model tables is the session's, carrying both
- A match of any node reads each node once
- A mixed CREATE splits across the governed and session paths
- Bolt holds a connection's ephemeral graph; SET and REMOVE labels
- A CREATE and a label SET/REMOVE are read from the graph IR
- A session's graph is written in place, its tables indexed on their keys
- MERGE is MATCH, then CREATE under the same rules
- An unlabeled node is every label's, session relationships with an unlabeled end read; several CREATE clauses are one
- store.key_of is public, so a restore indexes a table on its key
- DELETE and DETACH DELETE over the session's graph
- A session relation is one CTE, an instance's own conditions inside it
- A label's resolution lives in the session's lookup tables
- A session graph is indexed where it is looked up, and counted
- A session table not created yet reads as an empty table
- A session write followed by reads is one statement; session property REMOVE and indexes

#### errors

- An engine's error is answered in the model's names
- Every transport answers an engine's error in the model's names
- An error is named only by the org its request bound
- A client's error is named from the model-wide context, not every role's
- A JSON error body stays JSON when its names are the model's

#### events

- A second landing before the rewire runs keeps the first landing's views

#### explore

- A restored result comes back only beside the exact query that produced it

#### explorer

- The Cypher pane prints the read's graph IR in its three projections
- A read's nodes and relationships are its graph pattern; a graph view opens as one

#### export

- Each catalog is published as a role; the workbook is its caller's
- A role's publish keeps every domain the role reaches
- Governance tags name the standard roles too
- The tag roster is every role that reaches a domain

#### fabric

- Wait until Fabric serves a resumed capacity; suspend only from Active
- A resumed capacity's warehouse is waited for by its own answer

#### fakes

- The pg engine's fake functions import with no sqlglot

#### federation

- The DuckDB Snowflake attach creates the secret it names; the Kaggle and warehouse e2e clean up as their own
- The DuckDB snowflake extension is loaded with the ADBC driver Provisa ships
- Apollo subgraph served as the org's declared role
- The subgraph SDL publishes no operation directive
- A store table's time with its offset keeps the offset through the engine's copy
- Unity Catalog 429 is waited out for every REST call
- Send a source hint to Trino only if it is a session property
- The pg engine's ClickHouse attach uses the source's declared port
- Only the @provisa vocabulary reaches an engine session
- Load the crypto extension through the duckdb owner

#### flight

- A worker's Flight servers are held by wait(), never serve()
- Each relayed send is one bounded non-blocking call into room it already has
- A Flight RPC's unexpected failure is logged with its traceback
- Flight SQL statement query and SqlInfo on the Flight endpoint
- Flight SQL prepared statements, bound parameters through DoPut, statement schemas
- Flight SQL catalogs, database schemas, tables, table types and keys, as the role is served
- Flight SQL statement and prepared statement updates through the governed pipeline
- Flight SQL session options: the current catalog and schema, as pgwire answers them
- Flight SQL transaction actions, acknowledged and holding nothing
- Flight SQL sessions: SetSessionOptions, CloseSession, the session's current schema
- Flight SQL bulk ingest into the model's tables, through the governed pipeline
- Flight SQL temporary ingest into the session's own temporary table
- An ingest writes timestamps as their instants, and a missing table does not exist
- A lazy stream is pulled in the caller's Flight session
- A column transform plan streams from Flight's engine route too
- A Flight SQL prepared statement answers its parameter schema
- Flight SQL lists each column's nullability as the model records it
- An ingest writes the model's table by its schema

#### front door

- The waking page's streams are binary, drawn from one glyph sprite

#### glossary

- A class term grounds whole tables and views
- No requirement id inside the class-term view's SQL
- A table grounds its class term, in the model and the admission rule
- A term's list row and detail count the tables it grounds
- A term is a Class or a Property, never both
- Abstract is derived from grounding, the flag retired
- Tables propose their Class; keys are the Class's
- A proposal is a stored sync grounding on an uncurated term, not a name
- RELATED_TO is the last resort; resolving shapes offered
- Choosing RELATED_TO opens the alternatives
- Find terms in a document, with a review queue
- Ontology drift from the model history

#### governance

- A role's row limit bounds every list a read nests
- Cypher's collect() and COLLECT { } lists are bounded by the row limit too
- One governance context per role and model version; catalog views out of the landing list
- Every caller reads the one role governance context
- The kept context is read-only; the scan finds any naming of its builder

#### gql

- A table with no column to identify its rows by is refused, not given "id"
- A WHERE's ISO date-time string compares as a TIMESTAMP again
- A variable-length path keeps its lower bound (#283)
- The graph depth limit is an operator setting, and a result cut there says so
- Path expressions, path modes and label expressions in the graph IR
- RDF terms, typed literals, variable predicates and CONSTRUCT in the graph IR
- A SPARQL term and the depth limit's cut flag pass through the graph rewrite
- A path expression's step relation holds a node's whole key
- A relationship to a node the statement already holds is a condition, not a second join (#303)
- A node or relationship variable is null by its own row, not a column of its name (#304)
- Two node or relationship variables are compared by identity, not by table alias (#305)
- A later MATCH that starts at a node not yet held joins that node's table (#306)
- A stored edge keeps its direction; a read of every relationship types its edges
- Two anonymous nodes of one label are two nodes
- find_paths keys the relationships a path has used by mapping, not type name

#### gql-ir

- Graph writes are clauses of the graph IR

#### grants

- An empty grant is no role on every list; a registration grants its registrant and Viewer
- Listed names, the registration grant on new tables, and every list naming its roles
- Command fields and Flight metrics read the one assignment rule; an empty list names no role
- A command's response column with no grant is the command's own; tests name their roles
- batch-2b integration fallout of empty grant = no role
- A role served no tables is not refused

#### graph

- A label the engine could not count is said, with why; the total is null then
- A refused graph-counts request is shown in the sidebar, not stored as counts
- A refused graph-schema request is shown in the sidebar, not read as an empty schema
- A parameterized table is uncounted in /data/graph-counts, not a 500
- A result frame's query shows Copy and Delete on hover
- An Overview pill picks out its label's nodes, or its type's edges, in its frame
- GraphFrame no longer imports CopySymbolButton, which both its uses moved into FrameParts
- A node's properties are every column under its own name, id included; an OPTIONAL MATCH that matched nothing is null
- A frame of rows counts its rows, never "0 nodes · 0 rels"
- An expansion names only the relationship types the server registers for the role (#307, #308)
- The mouse wheel over any graph zooms it and never scrolls the page
- A label dropped on a frame expands from every variable the query binds
- A comparison of two kinds is one IR operation whose language states it
- A LIMIT needs no full union; graph.union_limit defaults to 256
- A registry entry with no columns says nothing of its key
- Paths skip parameterized tables; a join declared twice is one edge
- ?s ?p ?o stays refused; recursive walks tested where the join guard admits them

#### graph-session

- A cleared session never repeats a plan-cache key

#### graph3d

- One neighbourhood, two hops; a product's counted from its member tables
- Each legend glyph is the shape its nodes are drawn with -- a data product a hexagon
- A summary select's closed text stays on one line, ending in an ellipsis
- The product panel is the Graph panel, with All, Ontology and Products presets and no Lineage preset
- A product's graph draws its lineage from the product lineage endpoint
- A product's graph draws its column lineage from the product lineage endpoint
- The 2D view is a lane diagram
- Every lane node informs and links; 2D shows only 2D controls
- Lane view fixture and fallback use term status, not live
- Lane nodes without a status carry no status attribute

#### graphql

- A nested field's where, order_by, offset and distinct_on hold at every depth
- A nested field is built as one JSON value; a list with no rows is []
- A nested field read through its own join is answered under its alias
- Each nested column of a query's SQL is named by its response path
- GraphQL front end of the graph IR and the Cypher printer
- Aggregate roots lowered to the graph IR; nodes keep a column's alias
- GraphQL over HTTP recommendations on /data/graphql
- A column typed character varying is a String
- Prefix the ontology and ops schemas' SDL names on_ and op_

#### graphql-front

- The fields projection names each flat property itself; tests follow the one naming rule; LangChain reads deptId

#### graphql-http

- GET forwards no time-travel headers, and the test's state has no config file

#### graphql-remote

- A remote table is replicated row by row where its schema fetches rows by keys

#### graphs

- Each graph's detail panel links its item and its connections to their own pages

#### groupexposure

- Owners reach their group by a declared relationship, and Tier 1 is read as a value, not cross-joined

#### grpc

- An aggregate over no rows arrives unset, not as zero
- A row read lowers to GraphQL, and a where compares text as its column's type
- A row read honours the offset on the servicer and the proxy

#### guard

- Branches reading the same instances are one alternative

#### helm

- The chart's analyst role and row rule say what they are for

#### i18n

- Record English sources for translated graph3d, kaggle, serverErrors, sourceDetailPanel and tour keys
- re-record English sources for graph3d and banking tour keys
- The contracts list's strings in every locale

#### import

- A Hasura v2 source keeps its pool's minimum connections

#### install

- banking's regions node, run as its visitor region warehouse
- The regions tier lists banking

#### integrity

- Declare the term-finding foreign keys; a LINK standing for SET NULL

#### introspect

- Redshift lists its tables from pg_catalog; a Synapse pool offers its views

#### json

- The JSON family of catalog functions on DuckDB
- jsonb_concat of an empty value, a NULL key of jsonb_exists_all
- A call is rewritten once, whatever its nesting

#### kafka

- A landing's offsets name the topic's partitions from the metadata fetched for them

#### landing

- A failed landing or an unparsable rewrite fails the read
- Whether a table is landed is read from its store's catalog

#### leak-probe

- Live threads by name, so a rise in threads names its pool
- Thread stacks on request, and a named background thread keeps its name

#### lineage

- Every lineage read is cut to the caller's read rights
- Column lineage is answered only by the lineage reads that cut it
- A native box without its source's details is named by the source and the names it gives the object

#### live teardown

- An external link reuses only a parent location; UC listings wait out 429; Dataplex terms approved

#### llm

- Raise on None message content; give reasoning model enough max_tokens in LLM eval

#### locks

- A CrossLoopLock is never held for a waiter that cannot take it

#### masks

- Common regex masks beside a regex entry, and the server's universal-dialect verdict as it is typed
- A refusal reads under both regex fields; the guards list the regex fields' menu and its not-a-form row

#### materialization

- A nested statement drops none of the outer statement's scratch tables

#### mcp

- A loopback call's statements are audited, as the anonymous user's
- A loopback call naming a derived role acts as its meta-role and binds its attribute
- A local call naming a role set acts as its meta-role, derived roles bound
- Cypher runs in the caller's graph session; clear_graph_session
- Hold the catalog search index per org and scope its roles to the org

#### merge

- Restore main's pipeline lost in the 449e8d99a merge

#### meta

- Product inputs, related tables, derivations and checker states
- A glossary term reaches its edges out and in, and an edge its two terms; an empty _or holds for no row, an empty _and for every row
- A registered table reaches its sensitive columns (sensitiveColumns)
- A column, not its table, reaches its sensitivity (table_columns -> sensitive_coverage by column_key)
- Glossary refs and domain links are junctions; a table's class term is registered_tables.class_term_id
- The semantic-address resolver is the meta domain's resolve
- Register governed metrics as meta.metric_definitions
- A promoted primary key is NOT NULL, meta golden carries the catalog views

#### metadata export

- Provisa assigns its terms to data as each catalog's automated assignment
- Each term's ontology role on every glossary target
- The role property is declared on glossaryTerm; a role not exported fails the publish
- The glossary on Purview's Unified Catalog, Dataplex and DataZone
- A Dataplex definition link waits, bounded, for its table's entry

#### metadata-export

- Views seat in the derived layer's container; OpenMetadata reads the contract team as members; an escaped refusal fails its target by name
- OpenMetadata gets a contract's latency in its own units; the Purview e2e reads its product back by the adapter's id
- OpenMetadata searches its own Elasticsearch 9; Atlas assignment errors carry Atlas's body
- Atlas assignments are ACTIVE; Dataplex term entries by project number; OpenMetadata members through its bulk asset calls
- Dataplex links a term once its catalog entry appears
- A failed pre-write read refuses that asset; every adapter keeps what a steward owns
- Atlan is shown as "Atlan (beta)", from one flag on its adapter
- DataHub owners name their source in OwnershipSource; Atlas finds Provisa's unbound terms
- A Unified Catalog data asset is created with its name and type
- A Unified Catalog data product has a type, a data asset names its Data Map source
- Unified Catalog terms and products go by the ids the service gives them
- Atlas, OpenMetadata and DataHub e2e publish what the real services accept
- OpenMetadata views and contract SLAs import; harness audience is the org's roles
- OpenMetadata data product members through its asset calls; the ODCS read-back asserts what 2.0.5 keeps
- A Unified Catalog data product relates each data asset by the flat relationship body; the e2e expects the status the service stores
- Purview counts its Unified Catalog terms apart from its Data Map terms
- Purview tells its term relationships from a steward's by the record of what it wrote
- A mask tag names only the roles the column is served to
- Name gRPC, JSON:API, OpenAPI and SPARQL as contract servers

#### model

- No table label may equal a domain label

#### mongodb

- The driver reader reads each column at its document path

#### mutations

- An update's or delete's where takes every operator a read's does; a ServiceNow API document is fetched as the instance serves it

#### mv

- A view never answers its own build
- A deleted view leaves no build; one unresolvable view never fails the org's wiring
- A view's build is pruned by the view it was built from, never by its id
- A first build creates its store table keyed as the reconcile keys it
- A materialized view is indexed as a replica is

#### mysql

- A TIMESTAMP is read as its instant in UTC, with its zone

#### naming

- No domain's computed alias is another's; ontology and ops are named apart

#### nl

- non-strict NL is the guard bypass's, and no panel shows a query with an error
- A V002 refusal names the condition as written, and a refused NL query is retried with the registered path between its tables
- non-strict NL's Cypher is refined against the role's label map

#### ontology

- The Ontology View reads one root, its terms with the edges leaving them
- A relationship can say the rows it joins are the same entity
- Mark a relationship same entity in its editor; the gap on the term and the Ontology view
- Data on keeps the graph's spread and the camera frames it all
- Taxonomy draws a tree, its columns under their terms
- The Ontology View's choices as dropdowns; Loose–Tight; Set Defaults; one toolbar order, guarded
- A term is read, or edited as one form: Edit, then Save or Cancel; Claim columns
- No Abstract in the UI: a term is a Concept, a Class or a Property
- A term and the list are read again together; the glossary spec waits a round-trip's time
- A key column follows from its table's class; a property claims attribute columns
- The Ontology View reads its search on the server, two hops deep, within a budget
- The searched view's edge filter compiles in a nested where; the spec reads the view's own query
- One To review bucket in place of Gaps, its findings in the term's panel
- The page reopens as it was left -- its view, search and To review
- A term in a domain drawn is connected, whatever other layers are on
- Step 1 -- the glossary's definitions are the ontology schema's
- Step 3 -- SPARQL serves the ontology schema as RDFS/OWL/SKOS
- -- SPARQL joins the ontology's axioms to instance data
- Each Class term is a table of the ontology schema
- A Class table never takes a model table's bare label
- A Class table is read only where the ontology schema is named
- A bare name a Class shares with a table names the table
- Refuse a Class that takes a definition table's GraphQL name
- No requirement id in the terms view's SQL
- A catalog description names no requirement

#### ontology-view

- The owner-tables layer reads each column's table and the relationships declared from it

#### opioids

- The tour's answers put the counties with a rate first, and show the ratio

#### ops

- Ontology quality standard report

#### orgs

- Serialize registry-view rebuild with org schema drops

#### otel

- Define the native traces view without opening a batch
- The audit writer's own work exports no trace
- A request scope ends with its request span
- A read of the native traces never opens a batch compaction removed

#### packaging

- First launch offers groupexposure, as the sandbox tier's list does

#### perf

- Starting the backends' host agent leaves the ssh session free
- The server's serve stops on SIGINT; a stop that does not end says why
- The checkpoint copy runs in one process; gsutil -m hangs on macOS
- GraphQL audit rows reuse their governance; bench gRPC callers own their connections; the agent's backlog holds the RTT probe

#### pgwire

- A read of ops/meta tables keeps its bound values (#279)
- trust-mode auth reads the deployment org's derived roles
- PostgreSQL wire conformance -- arrays, error fields, extended errors, cancel, transaction status, exact numerics
- COPY on temporary tables, more temporary-table types, cancel without a torn message, length of a bytea on DuckDB, RAISE NOTICE
- A passthrough statement deallocated behind it is prepared again; length() of a list on DuckDB
- A catalog column and a catalog function answer as the type PostgreSQL 17 declares
- A catalog column is stored as the type PostgreSQL 17 declares
- A regproc or regtype catalog column is its name as text and its oid as binary
- oid[], oidvector and int2vector catalog columns are written and sent as PostgreSQL does
- An int2vector or oidvector catalog column is subscripted from 0, as in PostgreSQL
- pgjdbc's getIndexInfo lists an index's columns
- aclitem[], "char"[], inet and pg_lsn catalog columns are sent as PostgreSQL does
- The catalog relations psql, pgAdmin and DBeaver browse with, with PostgreSQL 17's columns
- The statistics relations pgAdmin and DBeaver read, empty, with PostgreSQL 17's columns
- The text search, extension, large object and parameter ACL catalogs, with PostgreSQL 17's columns
- Every remaining PostgreSQL 17 catalog relation, with its columns
- A command on a subtyped model answers its command, not a read's shape
- A catalog function a client calls is answered on its own
- The server functions act on pgwire's own connections
- A server function's pid is evaluated on a capped DuckDB, through its owner
- The catalog functions clients call that had no answer
- record-returning statistics functions and pg_sleep answer as PostgreSQL types them
- pg_sleep opens DuckDB through the capped factory
- Answer has_table_privilege and pg_get_expr from governed grants
- A discovery read is never sent behind another of ours; it is STARTING at once
- Send numeric(p,s) typmods and keep varchar/char native types
- An oid column no longer breaks the catalog
- Serve every catalog relation with PostgreSQL 17's shape
- Catalog functions answer PostgreSQL's type and semantics
- Serve the client-called server functions as PostgreSQL answers them
- Catalog gaps - regclass vs name, typelem, key action codes, masked rolpassword, pg_stat_activity session
- Serve pg_catalog function families as PostgreSQL answers them
- Serve the remaining pg_catalog families as PostgreSQL answers them
- Evaluate only constant arguments outside governance; test every signature
- Answer the server functions as PostgreSQL 17.11 answered them
- Tand is never minus zero; to_regclass gives a catalog relation PostgreSQL's oid
- Serve money through the IR as PostgreSQL answers it
- Serve the xml type, its syntax, xpath, xmltable and the governed SQL/XML mapping
- float8 text as float8out, literals hold no placeholders, xids start at 3
- A mapped document is xml on a PostgreSQL target
- Metadata lives in meta, its functions in the compiler
- The catalog views are listed to no person
- pg_catalog and information_schema as views in meta
- No catalog view in a subscription or a proto message
- Placeholders with no space between them parse
- A catalog route is not kept for its shape; types the wire has no form for
- List a json column as json in the metadata type forms
- Column privileges are the catalog's, xml is the compiler's; a catalog view refused off the admin terminal
- Only named metadata reads the control plane; a statement with no table is the engines
- Held types after functions; provisa_raise does not stop a pg engine; query errors in every locale
- Every client statement on the admin terminal runs as the client role
- A ClickHouse time is a time on the wire

#### pgwire, flight

- A column transform plan streams to the caller

#### pgwire,flight

- Every catalog a client sees is the served database

#### pipeline

- A statement waits only for the servers its engine reads through
- A statement that reads no source is the engine's; the default-source fallback is gone
- A compiled read of a session temporary table goes to the engine
- A compiled plan is kept per temporary-table session

#### polly

- Every Explore page and the ERD, ontology, products read and set by Polly

#### profiler

- A window whose MAD is 0 has no spread to measure a distance in
- Columns classified from their sampled values; a steward accepts or rejects each
- The classification statement's read is recorded as classification_attempts
- No requirement id in the classification_attempts column's description
- A run read as "Role: All" is read as the selected set

#### query

- The GraphQL page's Semantic SQL panel opens its SQL on the SQL page (→)

#### ranges

- Ranges and multiranges on every federation engine
- Multiranges from columns are SQL for each dialect; ranges land on DuckDB
- Forms corrected on real Trino, ClickHouse, MariaDB and SQL Server

#### rebuild

- A role surface is built against the runtime that registered it

#### redis

- A failed connect gives its pool slot back once (#284)

#### reference-library

- Read PDF and Word documents
- Lay the document domains out with the shared form layout
- Declare documents, their domains and passages in the model-store inventory
- re-embed documents of an earlier model from stored passages

#### regions tier

- It names its demos with the sandbox tier's domain prefix

#### relationship-names

- Names a change of naming convention respells are written in a model change

#### replica

- A replica's name is lower case, so Trino reaches it
- A floored MongoDB source's replica is built over its driver

#### replicas

- A table declared again while its replica is dropped is built again
- A replica's build lock is its region's, so each region builds its own copy
- A held build and a waiting template say why
- A reconcile keeps a landed replica whose key is not built yet
- A replica re-created empty is recorded unbuilt and built again
- A DuckDB engine's replicas are identified by the store they are written into
- The landing reconcile leaves a row-level table's row cache alone
- A build claim's locks are free once its close returns
- A row-level replica's first read and first fetch need no schema made before
- The pg engine's store is its own database; row landing runs in sets

#### replication

- Log every replica build's start and end at WARNING

#### report

- The model workbook's Ontology, Data Products and Data Profiling sheets, naming only what the caller is served
- The model workbook's Data Quality sheet, and a product's inputs as its panel finds them
- The model workbook's Data Contracts sheet and each term's axioms

#### residency

- A read waiting on a build of a table with cache_ttl 0 is answered by its landing

#### rights

- A column is read by the roles its grant names; scopes are domain, listed and public
- A right is exercised in the domains of the roles that carry it, for relationships too
- A listed role reads meta and ontology as a "*" role does
- System domains are capabilities, flattened into domain_access

#### roles

- CI fixes for the standard roles
- The sandbox manages no environment; integration suites follow the standard roles

#### routing

- Sources share a direct connection only under the same login
- A statement is landed unless it passes every gate
- The source contract decides trusted_to_answer; Qdrant keeps its exact reads
- A route hint written as a SQL or Cypher comment is applied on every SQL and Cypher transport

#### row-level

- A filter's record is created in its schema when the schema is not there yet
- The key pushdown probe keeps a statement's own LEFT JOIN single

#### row-materialize

- A source type declares whether it fetches rows by keys
- A keyed fetch that lands no row says so in the server log; the flight healthcheck reads each answer until it is fully loaded
- A join-reached row-level replica exists before the key-pushdown probe reads it

#### sandbox

- The sandbox tier merges the traces it lands
- A region template's store is pinned once its model is loaded
- The disk cannot fill and a source error names no address

#### sandboxes

- A demo's visitor carries the attributes its demo.yaml declares

#### scheduler

- A closing holder releases its lock before its session ends

#### schema

- An existing relationships table gains same_entity at startup
- A derived view's columns are the ones it is registered with, not an engine's information_schema
- table_columns.subtype in schema.sql, as schema_org has it
- A per-domain schema offers only the asked domains' root fields; meta filters like any domain

#### scripts

- The startup banner lists the SPARQL endpoint

#### security

- Standard roles composed by union over several parents
- Owned environment management, dq_read for profile reads, tour standard roles
- A role's panel shows its capabilities with the role each comes from
- A capability comes from every branch it enters in; the role lineage reads at body size
- column_served is the only column-visibility rule
- The read_restricted capability is removed
- A role's Data access tab and a table's Access section
- A role's LLM budget: a per-call cap and rolling windows counted in the control plane
- An LLM service call's output is held to its role's allowance, never withheld
- A row rule has a right, and U and D rules hold data and model writes
- A row rule carries a set of rights
- The Owner field gives RW on data products with generated row rules
- A role's refusal names no requirement; tests follow the read signature and the capability refusal

#### server

- The health probe answers fast and says busy; a rebuild compiles with no connection held
- A request still waiting for a thread says what holds them all
- On a timer, what every request awaits and where every thread is

#### servicenow

- Offer only import sets the instance can read; the e2e grants its writes
- A companion's operations are offered once it is saved; the e2e writes name published columns

#### settings

- Every catalog setting is named and explained, in every locale
- Network and TLS opens General and HTTP at first
- Drop the settings text no catalog setting reads

#### site

- Every page's navigation has the home page's "How it works"
- Hero shows one hook pair even with a stale cached stylesheet
- Six problem cards in two rows of three; one-model copy says keep your tools in sync, or use Provisa alone
- Download section: one-line licence pointer; sandbox sentence without the stale hosted-service plan
- Starburst comparison reflects Gravity, data products, MCP; contrasts SQL-analytics centre with one model across tools
- Comparison sources are the verified ones; drop an unverified Denodo claim
- Provisa defines federation; the engine you choose runs it
- Shorter nav labels (Works, Demos) to fit the navbar
- Crawler copy states per-format table detection (confirmed by the maintainer)
- One Download in the navbar (the button)
- Catalogs page states what Collibra Protect and Atlan actually push, with sources
- Compare hub engines row: partial reach, varying governance, data gravity, cloud lock; adds Fabric
- Engines row: pros and cons, use the best one for each job and switch when wrong
- Space between consecutive section paragraphs
- Four language panels across, narrower padding and gap
- Every page carries the home page's footer
- The How it works page's own nav says How too
- Landing pages carry the home nav label and footer
- Consecutive spotlight paragraphs are spaced apart

#### sources

- A source save is one createSource/updateSource carrying every setting
- A deleted source's catalog walk is stopped before its catalog goes
- A deleted source's server is stopped and none outlives its deletion

#### sparql

- Type a table's columns by its bindings' XSD datatypes
- Push a statement's conditions and LIMIT into a SPARQL table's query
- A source's HermiT reasoner serves inferred classes and properties as tables
- Every call to a SPARQL endpoint names Provisa in its User-Agent
- A source's tables are asked for JSON results, and an answer that is not JSON says what it is
- The SPARQL front end, rdflib's algebra lowered to the graph IR
- The SPARQL 1.1 Protocol endpoint, GET and POST /data/sparql
- GROUP BY, HAVING and aggregates
- Results serialize to a buffer, so the writer returns bytes; requirements.md regenerated
- Every SPARQL refusal is the app's error body with its own code
- GET /data/sparql/vocabulary, the role's vocabulary as IRIs
- A negated set of forward and inverse edges, !(a|^b)
- REGEX flags, and REGEX finds its pattern anywhere in the text
- GROUP BY an expression
- Scalar functions -- string, numeric, date parts, NOW and xsd casts
- A group's FILTER and a BIND that reads an OPTIONAL's variable are read after the OPTIONALs (#301)
- An OPTIONAL binds its variables only where it matches as a whole (#302)
- An OPTIONAL nested in an OPTIONAL is lowered, never dropped
- The response-cache opt-in and provisa_stats on /data/sparql; the bench profiles and caches SPARQL
- The SPARQL tab under Explore -- the graph visualizer with SPARQL's vocabulary
- Open as query exports a GraphQL read as SPARQL, exact or refused
- A read with no SPARQL form is answered with why, never a failed compile
- A local install sets server.public_url; the refusal names where and what
- The SPARQL tab exports RDF and SPARQL results, not Neo4j
- Integer and decimal division by zero is an expression error
- A term-bound column is published under the term's property IRI
- The open pattern answers what a role can name, with no self-join
- Selecting a node lists its properties; classes grouped by prefix
- A variable predicate reads as its parts in every position

#### sparql-tab

- The vocabulary is read as main's endpoint answers it, with superclasses; a CONSTRUCT's edges are drawn
- One vocabulary pill per IRI; the e2e CONSTRUCT gives its subject a class
- A SPARQL frame reads as SPARQL and offers no auto-impute

#### sql

- A batch on /sql answers one result per statement, each under its own tab
- A result's columns are keyed by position, so two of one name each keep their value

#### sql-canvas

- Select every column at once; joins built from what SQL allows

#### store

- A session's temporary tables are dropped through the embedded store's broker

#### subscriptions

- A subscription keeps delivering

#### tables

- Registering a checker table asks why the check exists, its quality claim
- A snapshot calendar's version is named once in the picker; its create dialog's close gets the test's budget

#### tags

- Pii on a Property term covers its columns; a move confirms the tags it takes
- A move asks to confirm only a tag the column stops carrying
- Pii terms in environment trees, the REQ, and the rest of the tests
- No requirement id in view SQL; a ref move reports its tag changes

#### term-finding

- A document scan always finishes or stops saying why
- schema.sql carries the scan progress columns
- One dense batch no longer fails a scan

#### time-travel

- Time travel as arguments of the relation
- One versions read, on the governed statement, at each reference's time
- A bound time keeps the relation's name as written; the banking test reads by arguments

#### tour

- A profile graph step stops at once when the table has no run
- A relationship step on the open page searches its own alias
- Demo tours run what they explain and cancel what they show

#### tour,e2e

- The Deep Dives menu reads the cached catalog; two core-lane specs stop paying cold loads they do not test

#### traces

- One process compacts a trace folder at a time, an input at a time, and every test server lands its own

#### transpile

- An instant literal reaches the MySQL family as its UTC wall time
- Snowflake, MySQL and ClickHouse write a null-on-failure cast too; every route transpiles

#### transpiler

- A JSON value read whole and a filtered list survive the semantic SQL's rewrites
- One rule for PostgreSQL functions; shapes, tsvector, inet and money as forms
- time(x) reads the function table on every dialect
- Crypto hashes give PostgreSQL's answer or are refused by name
- operator-named functions as one generated table
- PostgreSQL json functions as a table of forms, replayed on DuckDB, Trino and ClickHouse
- The array functions as forms, replayed on DuckDB, Trino and ClickHouse
- VARIADIC multiranges, multirange_cmp and range_union of a NULL
- The enum functions, a source's enum resolved in its catalog, gate fixes
- No function gives a wrong answer on the replayed engines
- A bound percentile fraction is the engine's to check
- A JSON aggregate's FILTER is its inner aggregate's on DuckDB
- A timestamp in JSON answers the same at any depth on DuckDB
- ts_filter's row has its own name, refusals pinned
- A list of timestamps stays a JSON array on DuckDB
- Trino's regexp_extract is NULL where it finds nothing

#### trino

- A MySQL-family catalog names no database in its URL
- A JDBC catalog sets statistics.enabled only where the connector takes it
- An instant read live through Trino is carried at UTC with its zone
- A ClickHouse source gets a Trino catalog; a JDBC source with no URL is refused by name

#### ttl

- A parameterized table keeps its rows only for a TTL of its own

#### types

- GraphQL and proto map a long-form SQL type as the Arrow catalog does
- IR subtypes recorded at registration, reported by pgwire and its catalog
- A timestamp column keeps its declared precision and is read at its unit
- One subtype per distinct PostgreSQL type
- A source type that fits no IR type registers as text
- Subtypes keep native semantics; land in their own form or not at all
- Trino's inet emulation from the functions Trino 481 has
- An inet or cidr operation on Trino is refused by name
- A time column keeps its declared precision and is read at its unit
- Register ClickHouse Nullable/LowCardinality IPv4/IPv6 as inet; document subtype mappings

#### ui

- A checker table is assigned to a product as any member is
- The Glossary toolbar is search, an Ontology view icon, + Term, one AI menu, then help
- Glossary is renamed Ontology, at /admin/ontology, in the UI, the tours, Polly and the docs
- A SPARQL source names its reasoner and OWL axioms
- The page chunk warm-up starts after the opened route's page, never ahead of it
- Every 3D graph remembers its view settings across reloads, per graph, with a reset
- The page chunk warm-up stops with the app and leaves no import in flight
- The Ontology View's Data layer shows each product's dataset columns; a Tables checkbox draws the tables of the columns drawn
- A table is a cylinder in every graph, a profile result a torus; the Tables checkbox is remembered with the graph's settings
- The legend's cylinder and ring are drawn with radius and border alone, so an exported image paints them
- Every 3D view exports an image or a recorded turn
- The export's scale, length and last formats are remembered, the last formats checked
- The Ontology View opens as its query (→): GraphQL as it runs, SQL and Cypher as it compiles, SPARQL later; its filters are in the query
- Open as query says why a focus leaves it closed
- The product graph is one query with its domain, tag, sensitivity and edge type filters in it, and opens as that query (→)
- A term grounded in tables, a table's rows as instances of a term, and each term's role
- A search focus with Tables stops at each table it reaches
- A hint held open waits for its target; a page that fails says so in place
- The graph sidebar shows the page it can, when a domain deselected shortens its lists
- The app's splash runs the front door's binary streams, from one shared renderer
- Pii on a term or a column, and one tag-loss question for every move a person makes
- A linked item's panel leaves the focus in a dialog open over the page
- Escape in a popover or menu inside a detail panel closes only it
- Every icon-only button is borderless but five drawn filled or bordered on purpose
- The splash's binary streams inline into index.html only, so the build's other HTML entries build again
- The SQL explorer's schema list filters meta and ops by the Domains menu like any domain
- The SQL canvas wires one join per relationship, so removing a join clears its line
- Ids come from crypto.getRandomValues, so pages render outside a secure context
- The GraphQL page never scrolls past GraphiQL
- A glossary term's read view draws its neighbourhood as a layered graph
- The Ontology view names a relationship by its served Cypher type, never its id
- An assembling-cube loader for long waits, on the demo-opening page
- js-yaml is declared for the typecheck
- The listed names use the naming module; tests name the roles their grants and masks are for
- A table's access column picker is a form field, its explanation the hint below it
- The lifecycle status type lives in types/, so types/admin.ts imports no component
- Every read the picker makes acts as the selected roles, "Role: All" included
- A column's subtype beside its base wherever its type is shown
- Layered layout for network graphs, Force/Layered control, lateral arcs
- A layered layout opens face-on and fitted in 3D, refitted when the layout kind changes
- SPARQL page pills by direction, domain selector and per-label colours
- SPARQL answers drawn as the Cypher view draws them
- SPARQL nodes captioned by name or IRI tail, drawn SELECT labelled by class
- SPARQL class nodes captioned by local name, compacted name kept
- A graph caption longer than its node scales down before it truncates, full name on hover
- The literals toggle is a toolbar chip, as the graph explorers' toggles are
- A graph caption stays inside its node or goes beneath it in two lines, no font scaling
- A role's LLM budget on its form, and each window's use on its page
- One caption source for the node canvas; its placement is no longer overridden per node
- The Cypher page lists the graph session's labels and types, dashed and counted, with Clear session
- A table whose source's API document could not be read says so: a mark in its row, the reason beside the endpoint
- The endpoint-error mark uses the shared Tip, not the library Tooltip
- Only the session's language catalog is loaded; English stays bundled
- Ontology term chips show a ground icon in place of the "grounded" text chip
- SPARQL vocabulary terms and concepts follow the checked domains
- Three random demo-sandbox loaders (cube, cardboard box, gift box)
- Explorer groups ontology/ops root fields by SDL prefix alone
- Escape Mermaid chart labels; show an unparsable chart in place
- Graph detail names wrap; crossing minimisation in the layered and lane layouts
- Readable 3D graph labels; the Ontology graph follows the navbar's domains
- 3D graph keeps its layout; crowded pairs are moved apart in 2D only
- Keep every Analyze operator row on screen for a deep plan
- The Ontology view draws only what a shown term reaches
- Plan diagram zooms to the pane width; tall plans scroll
- A product's contracts are a list beneath its lineage (WIP)
- Open in ERD from the Ontology table detail
- The ground symbol shows on Concept terms only
- Explorer clear-query button; SPARQL class/property pills run their own result card
- Contract version types live with the admin types, so the graph renderer build reaches no api code
- Graph labels at half the size they were drawn
- Term review queue meets form and collapsible guards
- The SPARQL explorer as an RDF person expects it
- One ERD view in place on Tables and Relationships, each with its own panel, and a focus that is a link
- A contract shows Versions and changes right after its Details
- Input Ports and Consumers cards always shown
- Show ontology drift with the quality report
- SPARQL endpoint prefixes use the shared collapsible; guard entries for in-place inputs
- The page filter dims what the ERD does not match, and is not in the link
- The explorer fork's content is part of Vite's optimizer hash, so an edit reaches the page
- Meta queries answer under the GraphQL field names
- Meta rows arrive under camelCase keys in any convention
- Graph and panel table labels use catalogTableName
- Open a live demo for every browser, signed in or not
- SPARQL page reads as Cypher: panel prefixes on Run, class prefixes, group union, plain relationship names
- Browser storage cannot crash or stop the app
- List HTTP surfaces in health, multiselect term status
- A pill dropped on a SPARQL frame edits its query
- Read the node under a dragged node before the nudge moves it
- A tour step pending stands alone, a visit opens in no environment, the nav holds one line
- Correct why a visit starts in no environment
- A full browser store gives up other control planes' entries, oldest first

#### ui-dev

- The dev server forwards a product's own reads (/data-products/…) to the backend

#### ui-server

- A regions sandbox or demo whose region no node serves is refused by name

#### warehouse lane

- No Snowflake credit cap; Databricks stop re-sent until taken; Dataplex teardown polls only operations

#### General

- A rewired replica runner stops the one it replaces; schema_gen binds its config's settings
- The vulnerabilities demo grounds CWE in the weakness class; bounded-memory reads are ordered
- A deleted org's entries leave every process cache; one org's row counts are never another's
- A DuckDB order by an output name a source column shares sorts by the output; the pg engine lands into a schema its store does not hold yet
- Team page title keeps a gap above its subtitle (J206)
- Integration fallout -- composites land as JSON, system schemas take no domain's alias, tests pin their state

## v0.1.0-alpha.495

Released 2026-10-22.

### Features

#### Column-Level Lineage Graph

- Lineage reaches below the registered table to the source itself.
    - Native hop beneath each source column, element links
    - A remote GraphQL table's native hop names its required arguments
    - A remote GraphQL table's native hop names its required arguments

#### Data Catalog Integration

- Data products are part of the built-in meta domain.
    - Data products in the meta domain, reached by relationships
    - A checker table reaches the table its contract scans
- The meta domain has a view-only data_product_edges view, related to data_products_meta by product_id.
    - data_product_edges and the build-written lineage edge table
    - data_product_edges names every table as the model does
- The meta domain has a view-only column_lineage_edges view with one row per column-level lineage edge: the source relation and column, the target relation and column, and the view that derives the target.
    - column_lineage_edges in the meta domain
    - The page and its panel's core are read from the meta domain as the user
    - The graph and panel as rendered: readable domains and edges, a smaller halo, the full graph's foot in view, columns named with their table, column lineage once a table is expanded, the panel's queries a selection a line
    - A column lineage edge's ends are source and target, not its column names

#### Derived / MV Processor

- A materialized view's build takes the same route decision a query with its definition takes: a definition one source reads whole runs at that source, on any engine kind, and only its result rows come back; they are staged beside the view's table and every build step reads them there.
    - A view's build takes the query route decision; a single-source build runs at its source and stages only its rows
- A calendar can declare the local time its day closes at, in its time zone (an 18:00 close of business in Europe/London); a day then runs from the previous day's close to its own, every grain's boundary moves with it, and a daily view refreshes at the close.
    - A calendar's day can close at a declared local time instead of midnight
- A config declares its calendars in a calendars: section (name and version required, then base_system, tz, fiscal_anchor_month, fiscal_anchor_day, retail_anchor, week_start, holidays, weekend and day_close), stored before the tables whose views name one and refused for exactly what the createCalendar mutation refuses; the model export writes them back.
    - A calendar is declared in config; refreshMv builds a view sealed at a business day's close
    - banking's setup commits each day before it is sealed; a seal that records nothing is refused

#### Materialization Store

- A table or a view is bitemporal by mapping three values to its columns: business_key (what identifies the same thing across versions), valid_time (the date the business gives the record: when it was agreed, booked or took effect) and recorded_time (when this version was entered or changed).
    - A table or view declares business_key, valid_time and recorded_time in place of the view-level bitemporal mode, key and source time
    - A table whose rows are its versions is read as of valid time and as recorded, on every surface
    - A versioned table's read picks versions by its key and times whatever the role sees, answering only its columns; the compiled stage is tested
    - A view over a versioned table inherits its history; each refresh recomputes only the valid dates recorded since, reads travel on valid time
    - Every table and view setting at creation, the Time Travel section in create and edit, the HISTORY badge, the history note and the bitemporal how-to
    - A table is registered or updated with every setting in one transaction; a refusal leaves nothing applied
    - One save is one transaction; view checks before commit; naming from the server
    - The register form's registration is not a busy flag; the mask-list exemption follows the split
    - A table updated in place keeps its versions in a captured log
    - The build loads what a table maps; a version log's interval and lateness are set on its row
    - The version log's settings follow the form rules; banking's init script is executable
    - reproduce's "as filed" is cut at the report's first build; debug lines for the as-recorded audit
    - A live Kafka table may keep the time Provisa lands each message
- A pgwire session sets its time travel with SET provisa.as_of and SET provisa.as_recorded (RESET either, or set it TO DEFAULT); every statement the session then runs, COPY included, reads as a request carrying the matching X-Provisa-As-Of / X-Provisa-As-Recorded headers does.
    - SET provisa.as_of / provisa.as_recorded is the session's time travel for every statement it runs

#### Source Registration

- A CSV or Parquet source can be given its file by download, a third way beside a path and an upload.
    - A CSV or Parquet source given its file by download, its table set up as a draft
    - The draft table is the Register Table form's default, its snake_case aliases too
    - The download choice reads as a field, its long words behind (?), and only a download source reads its TTL
    - A template another Provisa release built is replaced, not loaded
    - set-up-as-draft no longer reaches through the resolver's None self

### Changes

#### 3d

- The 3D views draw their colours true, on a stage set apart from the page

#### auth

- The right to a named environment is judged by the roles the org assigns
- A role set's meta-role is built in the environment the request names, unsecured and in the default org
- The default org's prod is served unbound, so a meta-role there needs no runtime readied

#### churnprod

- The tour's join step finds its relationship on any page; no refused step

#### code

- @columns batch column transforms for Node and Python sources
- A loader declared on any kind but a table is refused by name

#### code-hosts

- The remote host writes its certificate's serial in its one DER form

#### code-sources

- An uploaded archive keeps its file's name; the read view shows it; demo code pinned so its image can be built
- A code source its config declares is pinned when its model is built; its form lists the code's exports
- The Linux sandbox launcher runs by its file's path, not as a module
- The server directory's long explanation is help behind its (?), not a hint
- Pydantic row types read on purpose; field docs become column descriptions

#### codehost

- The front end speaks pgwire, its catalog its functions

#### codesource

- Pushdown declarations and name tables for code tables
- A name that only looks like a pushdown stays a plain parameter
- A read of one code table goes DIRECT, its residual applied by DuckDB
- One Python front end in every built image, the residual applied on the host
- A lockfile may be given beside any code that carries none; npm-shrinkwrap.json counts as the code's own
- A function's loader takes a join's keys in batches
- Each runtime resolution stages under a name of its own
- An engine reads a host's pinned certificate whoever wrote it
- An engine reads the organisation's client certificate whoever wrote it
- Closing a table's or stream's rows closes its call, not the collector

#### commands

- Host code is off unless the operator allows it

#### config

- The shipped configs' landed tables have a landing TTL, and every shipped config is checked on its engines
- dim_pet and fact_pet_inquiries read pets and inquiries by their names in the model

#### cypher

- An unlabeled relationship's property is read from its own properties, not its JSON
- An untyped relationship from a labeled node reads only that node type's relationships
- A path or an unlabeled relationship never crosses a table's link to its registration

#### data-products

- The data product graph's engine: layers, presets, filters and its 3D scene
- The product graph reads the meta domain as the user, by the names the server gives
- The graph panel reads its product when it comes into view; product links and lineage lanes from data_product_edges
- The graph's legend lists what is drawn, colliding labels yield to the higher-ranked one, and the panel's core leaves checkers to the quality layer

#### demo

- Banking -- end-of-day reporting without copying the ledger
- The profiling drift step shows what each drifting measure relates, the test checks every one involves postal_code
- Publish the banking demo; its tour reaches every step
- Banking is unpublished, like retail
- The demo keeps its own data directory, and its reset takes its environments' model history
- A notice's version time is the newest recorded time the dataset holds; debug lines removed

#### demo-bundles

- The node always pulls a demo's latest published version; <name>/wanted is gone

#### demo-stores

- An ask during a sync is served by that run; shared postgres and mongo healthy only once their real server answers
- The shared stores' health checks use the first address, as the sandbox node gives them two networks
- A Kafka demo's own services join the shared stores' network before they first start
- A superseded version's own Kafka on the shared Kafka's port is taken down before it starts
- A version taken down loses its bundle directory and the images no container still uses
- Recreate a shared store whose network is gone; say a failed start to the visitor

#### demo-tours

- churn's and retail's GraphQL and notebook steps read as the role their words name

#### demos

- Each published demo's video names its tagline, checked by check_demo
- cloud.provisa.dev serves worldcup, cloudpolicy and churnprod; a published demo must be in a tier's list
- Every demo shell script is executable, so a store's init marker is written on Docker Desktop
- The profiling demo shows drift between two runs and a table generated from the profile
- A demo's profile runs travel in its bundle and its template loads them
- A sandbox's tour warm-up sends each step as the role the tour has chosen by then
- Starthere ships supplies.parquet for its org's folder; the tour's live source reads it in place
- The open page waits for a demo still starting and opens it once it is ready
- A demo's stores are taken down sandbox.demo_stores_linger after its last sandbox closes
- A demo whose stores lingered out keeps its images
- A template's jobs wait while its demo's stores are taken down

#### deploy-cloud

- A deploy to the sandbox VM resets the sandbox tier
- Drop each sandbox schema in its own transaction
- A failed sandbox reset never leaves the tier down
- The sandbox reset wakes the coordinator first
- The sandbox-reset arm checks the tier's node first, as every arm

#### deps

- Pillow 13.0.0 and certifi 2026.10.19 for the CVE audit

#### embedded-pg

- PostgreSQL 16.11 via pixeltable-pgserver 0.5.1; pg_duckdb v1.1.1 (DuckDB v1.4.3)
- A killed postmaster's semaphore sets are released with its segment
- System V semaphore sets are macOS's alone

#### engine

- A native terminal's query reissues nothing beside a coordinator of another

#### entitlements

- No feature gate without the commercial plugin

#### environments

- A failed creation takes the branch it started; a branch with no environment is refused by name

#### federation

- The engine attaches a built code host as a Postgres catalog
- A code host is attached only for a built source that asks for it
- Every instance of a code-host connector reads the identity the schema build wrote
- Beside an engine of another user, its secret files are its group's

#### flight

- A read refused by name is answered UNAVAILABLE with its message

#### gcp-saas

- The wildcard certificate is kept in Secret Manager and renewed there

#### glossary

- The Ontology View: the graph engine scoped to the whole glossary

#### governance

- A table read through a physical alias is registered under its schema.table
- A table read through a physical alias is registered under its schema.table

#### graph

- A flat layout (tree, lanes) is seen from the front; shapes carry a little light of their own
- A tree is centred and spread so its names sit side by side; the frontal camera fits the layout to its field of view
- A filter of several values stays one line high; the Ontology View's e2e against the real meta views
- A glossary term's domains and references are null when it has none
- The Ontology View opens on everything, follows the page's search, draws the columns terms ARE, and lays out tighter or looser

#### graph-schema

- Its context and root fields come from one surface, taken once
- Root fields for every table the role queries, junctions included

#### graphql

- A root limit chooses root rows before a lateral join expands them (#278)
- A mutation the role holds no grant for is refused as ungranted, not as one its source cannot take

#### insider

- Match the mail with a code resolver, not host code
- Pair messages and deals through a declared screening scope
- match_deals reads the stored view by its registered name
- match_deals joins only along declared relationships

#### join-guard

- A union's columns are read once per check, not per column

#### kafka

- A landing-time column a store's own pipeline would leave unfilled is refused

#### meta

- A term's domains and its groundedness in the meta domain
- The glossary meta views follow the readable-term rule

#### metadata-export

- Org tier stored with the org; production-path e2e for every target
- Every ODPS field of a data product reaches each target
- Several targets per org; data products as ODPS and ODCS
- OpenMetadata 2.0.5, each member's ODCS contract imported
- A view member publishes to Atlas and Atlan with no source container; e2e harness reads as org_admin
- Google Dataplex data products replace bigquery_dataplex
- Microsoft Purview Unified Catalog data products
- Amazon DataZone data products
- DataZone revises its assets and products by the ids it recorded
- Databricks Unity Catalog data products
- Unity Catalog tags a view's column through ALTER TABLE and reads a share's full names

#### naming

- Store only the preset a convention names

#### nl

- A refused NL question is said by its code in the reader's words, not as its body

#### node

- Bun runs a source's code installing nothing

#### notebook

- matplotlib's font cache is built silently before handed-over cells that draw
- JupyterLab's splash removal no longer throws once the splash is gone
- Networkx ships with the in-browser Python

#### oracle

- A TIMESTAMP WITH TIME ZONE is fetched as a local time zone value, its instant kept
- A TIMESTAMP WITH TIME ZONE is cast to its instant in the database
- A statement is parsed for its zoned columns, then run once

#### orgs

- A destroyed org's jobs stop before its stores are dropped
- A destroyed org's jobs stop after its stores pass the delete's checks, before anything is dropped

#### packaging

- The image carries the regions sandbox tier's config

#### pgwire

- Trust mode asks for no password, so a checker with none connects
- The meta terminal binds the governed statement's parameters
- The meta terminal binds the governed statement's parameters

#### polly

- Every UI action through the admin API, as the chatting user
- admin_request uploads a file as its own bytes, as the app's upload routes take it
- read_screen reads the screen the user is looking at
- read_screen waits for a route's editor still loading, never reads it as absent
- A choice closed without picking is answered as closed, never as an empty message

#### profile

- The profile runs modal's tables sort and group, results expand, tabs down the side; View Profile Runs in the read-only panel
- The profile runs modal's tables filter by column and download as xlsx

#### profiler

- A measure that differs only in floating-point rounding is not drifting

#### replicas

- A pass whose runtime was dropped under it is left to the next runtime

#### sandboxes

- A release change reaps the open visitors' sandboxes at the tier's start
- The demos' embedding models load at boot on a bundled tier
- An open whose visitor left is closing; the front door outlasts the UI proxy
- An open names each stage as it starts and times it as it ends; a store lock wait names the store

#### secrets

- No product-written ${env:}/${secret:} reference without a documented convention

#### sonnets

- A join feeds lines_like its query; link_echoes is a statement

#### sources

- No pgwire write server for a source its config fakes with declared rows
- The Code group is Code Resolvers, "Business logic in Python or Node"; every source group is translated

#### sql

- A refused statement is said by its code in the reader's words, not as its body

#### synthetic

- A synthetic environment generates no profiler's or checker's results

#### telemetry

- Compaction finds a signal table whose bucket holds no metadata file
- The hourly reclaim leaves a table whose bucket holds no metadata file

#### tour

- Core tour steps act, then explain; a step builds a source live
- A sql step is ready on its statement's refusal and shows it
- A step's anchor rendered again is lit again where it now is
- Each demo's steps say only what serves the feature it shows
- The 12 locales say each demo's steps as the English now does
- The agriculture notebook step points at its figure, scrolled into view, and its words carry no results
- The profiling demo shows its run as terrain and postal_code's drift as strata
- A step whose Next picks a role is not the end because the role it leaves may open nothing after it

#### types

- A character type's character set names its base type (Exasol's VARCHAR(64) UTF8)

#### ui

- The splash halo follows the P's loop every frame, so the status note's reflow can't leave it off centre
- A code source's Remote host panel is on the Connection tab alone
- The tour steps import their prerequisites with a JSON import attribute
- A list's row actions are pinned at the row's end; the Materialized Store list's are never cut
- The Materialized Store list fits its pane at 1280 with no sideways scroll
- Backport monaco-editor 0.57.0's catch on the word highlighter's Delayer.trigger calls
- A product's checkers are read through the tables they scan
- An empty one-to-many in a meta read is null; the 3D stage gets WebGL in CI and says when it has none
- The Environments admin page and nav item are named DataOps
- Dialogs that never scroll; one vertical menu; panels and stages fill; terrain column choice; ontology labels are titles
- Every page's search bar reads one way: any word matches, a "quoted phrase" is one item
- A Grid checkbox on every 3D network graph shows or hides its floor grid

#### ui-core

- A checker source loads without its password set; an input-object action argument converts; the files spec waits for its server
- A checker names no password under trust mode; an import naming an unset variable is refused by name

#### views

- A view applying a column transform keeps its reference through governance

## v0.1.0-alpha.494

Released 2026-10-20.

### Features

#### Source Connectors

- Provisa's services to a code source's code are a module always present where the code runs: provisa_services in Python and @provisa/services in Node, Bun and Deno, with the same calls -- secrets.get, settings.get (the source's declared non-secret settings, mapping settings, edited on its form), cache.get/set/delete with a ttl (scoped to the source and shared by its workers, JSON values of at most 1 MB), vectors.embed/upsert/search/delete (the source's own vector collection, embedded through the org's vector models as routes them; never a governed table's data, which reaches code only as parameters), http.get/post (a fetch Provisa makes for the code, held to the egress allow-list the worker's own egress is, through the deployment's outbound proxy and CA bundle, recorded in the org's trail), llm and log and metric (attached to the request the call serves).
    - Provisa's services module in every worker, and code-source telemetry

### Changes

#### auth

- A role set's meta-role is built in the environment the request names
- An environment the request cannot be served by is refused where its meta-role would be built

#### demo-stores

- The sandbox node sets every store's data names before compose reads them
- The shared Kafka starts: its KRaft cluster id is a UUID; a store that exits says why

#### demos

- A sandbox's tour warm-up no longer revives it once its tab is closing
- A failed template is refused as not ready; the walk takes a read-window refusal as its step's answer and keeps the tier's log
- The bundle build dumps a Postgres with pgvector, and the node names its data unsuffixed

#### polly

- Polly operates the screen when asked to show, demo or teach

#### synthetic

- A generated table and a closeness sample land with their org's vault bound

## v0.1.0-alpha.493

Released 2026-10-20.

### Features

#### Demo Provisioning

- A configuration may declare the code its code sources run: code/<name>.tar.gz (or.tgz or.zip) beside its config.yaml, a source naming it as uploaded code by the content hash of the code it unpacks to (code: {kind: upload, upload: <hash>}).
    - A demo's declared code archives are kept as its org's uploads
    - The bundle build's answering config is served beside the demo's declared rows and code

#### Performance

- Request deadlines are watched by one shared watchdog thread per process instead of a threading.Timer per request; a request that passes its deadline is cancelled as before, on every transport that registers a cancel.

#### Relationships

- A relationship may feed its target table's parameters from its source table: its parameters map names each fed parameter and the source column whose value it is given (several parameters, each from its own column).
    - A relationship feeds a code table's parameters from its source rows

#### Source Connectors

- A Node or Python code source declares the names its code may read (secrets in its mapping): a list of names, each the org secret of that name, or a mapping of name to vault reference (${secret:NAME} or ${env:NAME}).
    - A code source's declared secrets, read at use through Provisa's services
    - The egress proxy answers Provisa's services through a CONNECT tunnel
    - A declared secret picked by name stays a name; the product spells no reference
- Provisa's services to a code source's worker include an OpenAI-compatible LLM service at $PROVISA_SERVICES/llm/v1: POST chat/completions and embeddings, so any SDK works with its base URL there and no key, the worker's scoped token being in the address; the egress proxy answers it for a local worker.
    - An OpenAI-compatible LLM service for a code source's worker

#### UI

- The named demo churnprod puts the churn demo's model in production.
    - The churn model as a Python package and its Node twin
    - churnprod, the churn model in production
    - churnprod's store names its data per version; check_demo requires it
- The named demo cloudpolicy reviews a cloud security policy after the fact, read only.
    - The cloud policy's reader and the cloudpolicy demo's data generator
    - cloudpolicy, a cloud security policy reviewed across three clouds
    - cloudpolicy's card, Superset dashboard picture, published
- A named demo, World Cup: public sources about the World Cup whose country names disagree become one governed graph.
    - World Cup, public sources made to agree and queried as one graph

### Changes

#### bench

- Each step reports paging beside its CPU per request
- A masked column is one that is not the primary key; the fake admin answers only what the schema takes
- An MCP transport, run_sql over Streamable HTTP, in the perf contract
- The Postgres-only pass leaves out relationships naming a gone table by alias or via table
- The three-device harness runs a governance pass, its devices sized by one flag

#### cloud

- cloud.provisa.dev's sandboxes run Polly on OpenRouter's Claude Haiku; a tier node without OPENROUTER_API_KEY is refused by name

#### codesource

- Build a code source's image and run it on a remote host

#### cypher

- An inline property map matches as the WHERE it means

#### demo

- The provisioning bundle's setup runs its consumer told its topics' suffix

#### demo-stores

- Shared Qdrant and Kafka, finished: tests to the shared design, no compose default

#### docker

- The image trusts Microsoft's 2025 signing key, which signs its Debian 13 repository

#### environments

- Create with a data mode, not an "Inherit connections" box

#### join guard

- A column a view passes through relates along the view's relationships
- A union's branches fill its columns by position

#### materialization

- A code table kept for good is read from its replica, never run

#### perf

- Each request stage reports its thread CPU; the bench reads it per step

#### tour

- The core tour registers a source and a table; Skip works while a step waits
- Query it everywhere covers nine surfaces: Python and the Notebook
- pollyOpen is an explicit flag on the steps that show Polly's panel; "Just ask" stays shut

#### ui

- The graph renderer type-checks without the app's client: ResultRedirect is its own type
- No domain picker offers meta or ops as the domain to put something into
- A source staged from a Kaggle dataset shows as Kaggle in the Sources list

## v0.1.0-alpha.492

Released 2026-10-19.

### Features

#### Column-Level Masking

- A meta-domain view, sensitive_coverage, lists every sensitive column of the org (any tag with the Sensitive data option, pii included), one row each, read through the same statement that decides a Test (fake) environment's uncovered columns (provisa.security.sensitive.sensitive_assignments): its tags; has_fake and has_synthetic_rule; the fake as declared, its kind, that it is deterministic, whether it is stable across engines and whether it is computed from the real value; the synthetic rule as declared; and a status of covered, no_fake or inadequate with the reason.
    - sensitive_coverage meta view and fake adequacy checks
    - fake_adequacy_checks is classified, inventoried and in schema.sql

#### Demo Provisioning

- A configuration may declare a replica's rows instead of landing them from its source: replica_rows/<source_id>/<schema>.<table>.tsv beside its config.yaml, its first line the table's columns in their order, tab separated, each line after one row (\N is NULL; \t, \n, \r and \\ a tab, newline, carriage return and backslash; an integer, floating or boolean column's cells read as that type, every other as its text).
    - A config's declared replica rows land as its replica; its source is never read
    - A config's declared rows land in its model build, beside the config a test server serves
    - Declared rows land before the schema build publishes its replica routes
    - Hasuramigration and provisioning bundles build

#### MCP & AI Integration

- The MCP server offers five statistics tools over a dataset the caller defines in SQL: describe (per column count, missing, distinct, mean, sd, quantiles and top values), correlate (pearson or spearman matrix and n), regress (linear: coefficients, standard errors, p-values, R²; logistic: the same with pseudo R² and AUC), compare (Welch's t-test for two groups, one-way ANOVA for more, with Cohen's d or eta squared) and forecast (exponential smoothing, ETS, with prediction intervals).
    - describe, correlate, regress, compare and forecast over governed SQL
    - stats.forecast casts its ETS fit to its results, which pyright refused as a union

#### Security

- Central redaction: one list of rules is applied to text before it leaves Provisa.
    - Central redaction of spans, logs, audit text and client errors
    - Redaction rules -- one name column width, monospace values with plain labels
    - Redaction -- a bearer token is 16+ token characters; names resolve through the backend
- Judgments under review (docs/arch/judgment-review-design.md, phases 1-3).
    - Judgments under review -- why, bases, findings, the review and posture
    - Judgment review -- nullable JSON stores SQL NULL; exports compare without the server's stamps
    - A profiler member never run gives no evidence instead of failing the seed
- A named demo declares its secrets in secrets.yaml beside its config.yaml (secrets: a mapping of each name to its value), carried in its bundle with the rest of its directory.
    - A demo seeds its secrets; testdata reads prod as Provisa's own secret

#### Source Connectors

- Package registries reach a proxy such as Artifactory and the Git servers GitLab, Bitbucket Cloud and Bitbucket Data Center as well as GitHub.
    - Package registries reach Artifactory, GitLab and Bitbucket, behind a private CA

### Changes

#### bench

- Governance knobs in the benchmark contract, set up in the deployment for each run

#### bundles

- Concurrent fetches of one connector bundle download it once and each finds it whole

#### codesource

- A host's hello names its protocol and code hash, refused by name otherwise
- A remote host over mutual TLS, paired once, dialled by each Provisa process
- A Python source's rows travel as Arrow IPC batches, protocol 2
- A Node source's rows travel as Arrow IPC batches, protocol 2

#### compiler

- A lowered reference names its own source's table, not another of the same name
- A draft is refused under the catalog-physical name its served table answers to

#### demo

- Cards for provisioning and retail, from their real answers
- Agriculture -- the notebook measures how much of the portfolio's losses the weather explains
- Agriculture -- the 2012-like scenario prices the portfolio's latest book, and each empty step says which
- Agriculture -- the scenario is the data's driest season, priced at each county's latest premium; the answer is captured before the notebook runs
- Agriculture -- a pull of every county season (320 rows) is not cut at 100
- Agriculture -- the card is the notebook's plot, and the tour says what the notebook measured

#### demo bundle

- A build's servers are given the stores' addresses, not the build's whole environment

#### demo-stores

- A demo's stores start at its first open, its data in one shared instance of each store type

#### demos

- opioids' public dashboard is a view that reads no allocation
- Publish provisioning and hasuramigration
- Publish insurance
- Start here, the standard demo's objects with every Deep Dive
- Start here's links to the standard demo's files are tracked
- Publish Start here, listed first
- Live tour steps -- profile runs reach every sandbox, the REST step waits for its docs, a table step opens the panel it describes
- Starthere ships its example query; the card images site/demos.html names are committed
- The data room, a retailer's own systems shown to each bidder with no exports
- Testdata over three environments; a demo's Test (synthetic) environment is generated once and carried to every sandbox

#### docs

- Each locale's search reads only that locale's pages

#### events

- A view over a view not built yet is not probed; it awaits that input's first build

#### install

- A rerun of first launch keeps the node's provisa.env settings it did not generate

#### mcp

- The forecast's ETS fit is typed as its results

#### mv

- A view is materialized in its own org's store schema, not the deployment org's

#### pipeline

- A statement waits for no server of a source read from its landed replicas alone

#### push

- A push listener stops with its runtime and its org's next build wires it again
- A retired runtime's listeners are stopped through a hook the app registers

#### regions tier

- Its auth block is the sandbox tier's

#### runtime

- A background job walks only the org runtimes that are built
- A runtime being built is resolved only by its own build

#### sandbox

- A bundle ready after the tier started has its template built
- A demo sandbox keeps its template's code uploads

#### sandboxes

- Opening a sandbox counts places under the control plane's advisory lock, which holds across request threads
- Two visitors opening one demo while its template is replaced both wait their turn
- The tier builds its demos' templates a few at a time, an opened demo first
- A demo's readiness is asked without opening it
- A restarted sandbox tier serves its unchanged templates as they were built

#### security

- A demo sandbox visitor's token naming a role it does not hold is refused and recorded, not suspended

#### sources

- An engine whose driver library is absent is unavailable, and the Sources page still answers

#### subscriptions

- A sandbox's live subscription reaches the browser frame by frame

#### tour

- An image step's popover is placed again once its picture loads, and the picture fits the window

#### ui

- The splash halo centres on the P's loop, not the logo's box

## v0.1.0-alpha.491

Released 2026-10-18.

### Features

#### Infrastructure

- Trino internal TLS is an operator option, off by default.
    - Name the coordinator Service's HTTP port, so internal TLS's second port is accepted
    - Trino internal TLS uses a chart-generated certificate naming the coordinator Service and the namespace's pods; no address pinned
- Engine catalogs and their credentials live only in the engine's memory.
    - A worker gets no catalog store, which it refuses

#### MCP Authentication

- An MCP agent never acts beyond the roles it was given.
    - A stdio call acts only within the roles the server was pinned to
    - Derived Roles in the Roles list; every role and row rule says what it is for
- Naming a role the caller does not hold is a high-severity security event, detected on every transport (the HTTP API, MCP, pgwire, Bolt, Arrow Flight, the airport Flight service and gRPC).
    - Naming a role the caller does not hold is a high-severity security event, refused, recorded, notified, and suspending the credential where no browser shares it
    - On the HTTP API a refused role is named to every credential but a personal access token

#### Role Assignments

- A role grant Provisa holds (a user_role_assignments row) may be time-limited, and a role may remove its grants once they go unused.
    - A role grant can expire, and a role removes its grants once unused
    - The role form and detail panel carry remove grants unused for N days
    - An org schema made before role grant expiry gets its columns before the org registry view reads them
    - Each org's grant table is reconciled before the view reads its expiry

#### SQL & Multi-Protocol Client Access

- The Python client reads a governed SQL, GraphQL or Cypher statement into a pandas DataFrame with one call, query(text, role=None), the same on a desktop Python and in the browser (Pyodide), with aquery as its awaitable form.
    - The notebook and browser-reads requirement is

#### Security

- A role constant (session_vars) may be a ${env:NAME} or ${secret:NAME} reference, resolved through the same resolver as every other reference (provisa.core.secrets.resolve_secrets), at use: when a request's session variables are formed, when the regional administrator's are, and when a meta-role inlines a member's constant.
    - A role constant may be a ${env:} or ${secret:} reference, resolved at use
    - Saving a role refuses a ${secret:} constant, which takes part in the query and can be logged; the docs and the role editor say so
    - A role constant's ${secret:} reference is saved like any other; its records are covered by central redaction

#### Write Admission

- Per-table write grants.
    - per-table write grants replace column writable_by
    - A meta role's write grants fold as its reads do; presets proved on GraphQL
    - A table's Writes section reads and edits its write grants
    - A table's mutations name its columns as its reads do

### Changes

#### admin

- The admin GraphQL API answers HTTP alone; a request nobody was resolved for is answered no catalog

#### auth

- With no auth provider the admin catalog is the named role's; a demo seeded into an org reaches its own sources
- A first start with no config declares no derived roles

#### bitemporal

- Read a view as recorded at a time, beside as of

#### cdc

- A captured table carries its source commit time, source_committed_at
- An upsert of the row a landed table already holds is no change, and fires no trigger
- Each org environment holds its own logical replication slot

#### client

- query() reads as a DataFrame on desktop and in Pyodide; %%sql/%%graphql/%%cypher magics; connection from env or profile
- The SQLAlchemy dialect reflects the SQL catalog the role is served

#### config

- A relationship the configuration declares is registered or the load fails by its name

#### cypher

- A node's property is its column's published name

#### database

- A pooled connection drops its prepared statements when its org scope changes

#### demo

- Hasuramigration -- a Hasura v2 storefront imported in one step, then joined to Salesforce
- hasuramigration's tour visits Salesforce before it signs in as a role
- Hasuramigration requires 0.1.0a491
- Reproduce -- a regulator's exposure report traced and reproduced as filed
- A sandbox carries the refresh records of the history it was copied from
- reproduce's card, the report as filed, from its real answers
- Churn -- governed, masked features into pandas and a model trained in the Notebook tab; a named demo ships its own notebook

#### demo bundle

- A build's servers are told where DuckDB finds its extensions
- A setup server's store missing where the views are exported from is said with every store the build holds

#### demo bundles

- A build's servers take their own Flight, gRPC, pgwire and Bolt ports

#### demo regions

- A template's replica store is read for the template org, whoever waits for it
- A template's first build lands in its region's store

#### demos

- The agriculture demo, does weather explain crop insurance losses
- The provisioning demo -- datasets told ready on Kafka and pulled over Arrow Flight as of that moment
- main's failing demo and UI guards; the provisioning demo needs 0.1.0a491
- A setup step runs a demo's own client; the provisioning bundle records a pull for each team
- Every demo's stores publish ports no other demo's do
- A sandbox's "tour warmed" line shows at the hosted tier's WARNING level
- The hosted sandbox tier offers the quality demo
- The pharma demo -- which of our products face shortage or recall risk
- A regions sandbox tier, so a demo in regions runs live
- pharma's store publishes 29532 and is healthy only once loaded
- Opioids registers the columns its landing filters name
- The sandbox tier loads the demos' embedding models at startup
- Storefront -- one model serving an app's backend and its BI
- storefront's other locales drop the scaffold's step words English no longer has
- storefront's GraphQL example runs as the customer, as its words say
- provisioning's close joins along its relationships; its feeds ready before Provisa reads them
- opioids' fiscal year term no longer relates to itself
- Opioids reads the two govdata year columns as the text govdata serves
- agriculture's answer relationship names its view by its registered name
- A season with indemnity and none for dry weather has a dry share of 0; the relationship check skips no demo
- Publish vulnerabilities, agriculture, streaming, storefront, hasuramigration and churn

#### deploy

- The overlay publishes a sandbox VM's tier on its VPC address
- A coordinator with a sandbox VM gets no demo stores of its own
- The API push leaves out the checkout's _graph_render symlink, which the image holds as a directory
- Private plugins go to the containers on each node; the sandbox VM's tier gets them there

#### duckdb

- A PostgreSQL store is attached by the URL libpq reads, without its driver

#### embedded pg

- A killed postmaster's shared memory segment is released before its directory goes

#### engine

- Trino keeps catalogs in memory; Provisa reissues them after a coordinator restart
- Kafka client properties are Trino's alone, on a shared tmpfs; stack start and cloud deploy remove the on-disk catalogs
- A Kafka source's SASL settings reach a hosted engine shard through its in-memory Secret; a restored terminal reissues the Kafka catalogs

#### events

- A view over a view not yet built is wired when that view's first build lands
- A view over a replica not yet landed is wired when the replica lands
- Each org's event loop is held by its own runtime

#### explore

- Data Science area, Python tab: Pyodide in a Web Worker over governed data, as the page's user
- The Data Science area fills its space, editor and output split by a draggable rule; no repeated heading
- Data Science area, Jupyter Notebook tab: JupyterLite on the shared Pyodide, reading as the page's user
- Python and Notebook tabs of their own, kept alive between visits; Polly writes into and runs either
- The Python tab holds several scripts as tabs, kept across reloads

#### federation

- A table whose server was starting is attached once the statement has waited for it

#### flight

- A Flight ticket reads bitemporal views as of a time

#### govdata

- Every connection to the connector server signs in with the source's AskAmerica key
- A floored AskAmerica source's replica is built through its server

#### governance

- Faked columns read under every published name; joins compare real keys
- An ORDER BY of an output column orders the rows under every role

#### graphql

- A subscription field takes its table's query arguments
- A table's similar input is built once per schema; integration setups follow the product they test

#### helm

- A change to the Trino config rolls the workers, as it does the coordinator

#### import

- A Hasura v2 import that basically works, with a three-part report
- Hasura session variables are claim-supplied attributes of their own
- Name each environment variable a converted handler URL reads

#### jira

- Jira Cloud is a source type read and written through its Calcite connector

#### jsonapi

- A resource's id is its primary key, and what it includes is linked from it

#### kafka

- A table's first listener reads from the earliest offset, missing nothing published after its landing
- What reads a topic onward starts exactly where the table's landing stopped
- A Kafka source without a schema registry is refused when saved for an org on an engine shard, and never stops the boot

#### launcher

- --list-demos lists the named demos; the README says how to try one

#### lineage

- Refresh lineage records are queryable on DuckDB, under the names Trino gives

#### live

- A live Kafka output resolves its broker's address
- A live Kafka output publishes a row with times and decimals

#### mcp

- describe_role answers the role an agent acts as and the roles it may switch to

#### meta

- A table links to its registration by its domain and its name as queried

#### mv

- Time travel on the source's clock, and over a range
- A view the event loop builds is recorded on its region's build row
- A snapshot view's correction at the same source time is the version in force

#### native engine

- The ops signals are never attached as the provisa-otel source

#### notebook

- Assets rebuild when what they are built from changes; the notebook wears the app's colour scheme
- Embed JupyterLab so documents open inside the tab; dev server redirects directory URLs
- A new notebook opens on the starter cell under JupyterLab
- The notebook takes the app's colour scheme only once JupyterLab is restored and its splash gone

#### ops

- Each org reads its own ops signals

#### orgs

- Each org's views and table settings are held on its own runtime
- What each org's build records about its sources and tables is held on its runtime

#### packaging

- provisa-notebook is part of every install
- The macOS DMG builds retry hdiutil create on the runner's Resource busy, bounded

#### pg engine

- A read reports its columns' types, so a view's output is learned and the view scheduled

#### pgwire

- COPY FROM STDIN refuses a format it does not read
- pg_get_constraintdef answers each constraint's definition as PostgreSQL writes it
- A source's server is never given a port another process listens on
- A replica is landed from its server's registered columns
- A stopped server's port is free while its closed connections sit in TIME_WAIT

#### polly

- Polly writes Python into the Data Science area, runs it and reads what it printed, returned and raised

#### provisioning

- The broker's CLUSTER_ID is a 16-byte base64 UUID
- Version time and margin carry no edge to themselves

#### provisioning demo

- The producer reads the orders topic's partitions once the broker has described them
- Each team pulls the whole dataset, not the row limit's first rows

#### push

- A push listener resolves its source's address; a push-fed table's node asks for no build
- Prod and no environment are one listener and one batch poll

#### quality demo

- The demo ends on a data product of gold, silver and the checks

#### regions

- Eu builds its views from what lands once, its build record outlasts the sweep, and a reader is not held to another region's view's inputs
- A region's node reads the ops views over the record and state it keeps
- Trino reads a region's ops views over the record and state it keeps
- A demo org's region stores are bound before its model is reset

#### relationships

- A relationship a remote source offers is not kept beside the declared one it is

#### replica

- A landing filter's identifiers are quoted in the read that applies it
- Two processes ensuring one foreign table converge on it

#### replicas

- A SQLite file's table is read through its driver by its name alone

#### retail

- eu-shop lands once and is never re-read, so it states cache_ttl -1
- A us node reads eu's sources, a grouped view keys only on what it outputs, a regions demo builds
- Each request edge has its own type, so the US one is registered and subject_access's US join is approved

#### roles

- main's new role fixtures carry descriptions; the last pseudo names go; the grant picker's test reads its open popover

#### row limit

- A stream drained after its request returned is checked in the request's org

#### runtime-deps

- Every pgwire bundle is fetched from engine-v0.118.0

#### salesforce

- The write route reads its result types from a pgwire server

#### sandbox

- A sandbox VM publishes its tier on its VPC address, where the coordinator reaches it
- The sandbox VM starts its demo-stores sync at setup, not at its next boot
- A request that finds the sandbox VM stopping starts it once it is down

#### security

- A role's constant session values
- A request acting as several roles reads and writes each row as the roles whose row filter admitted it
- An approved join reads the real key, and fakes and kept writes are read row by row as the roles that admitted each row

#### sources

- The type list reads its engine reach from the source kinds, not the platform's engine settings
- A Redis source is refused when saved for an org on an engine shard and never stops the boot; no description files for Elasticsearch, which reads none

#### sql

- A table's _name_ and _domain_ read in SQL as GraphQL reads them
- Refuse a statement whose clauses are out of order

#### subscriptions

- A landing reaches a subscription bound to prod

#### telemetry

- Metric points and log records record the org they were written for
- Compaction keeps a metric point's and a log record's org as its tenant
- A signal table whose metadata cannot be read is said and left alone

#### terraform

- A node waking with the startup script it last completed sets nothing up again
- Docker waits for the coordinator's object-store disk

#### tour

- A step groups the graph on screen by a property; the reviews tour reads its graph by degree
- A demo tour step opens a data product on its page; the quality tour ends on its product
- A demo tour step can show a picture
- A role step off the SQL explorer picks the role in the header's picker
- The step counter counts the itinerary the tour started with

#### triggers

- A config file's on: {table, event} trigger loads

#### ui

- A refused page offers the held roles that grant it
- Claim roles saves a typed claim type, says when it saved, and explains itself plainly

#### views

- A user's read of a stored view outside its read window is refused on every call

#### General

- A visitor's first requests record once; __typename is the object's type; walks are per org

## v0.1.0-alpha.490

Released 2026-10-16.

### Features

#### Caching

- The load management window also applies to a materialized view, meaning: outside the window the view may not be read by a user''s workload.
    - A materialized view's load management window says when a user's read may read it

#### Demo Provisioning

- A visitor's demo sandbox is released soon after its tab closes.
    - A visitor's sandbox is released soon after its tab closes

#### Tag Registry

- Tag IDs are emitted into the bracketed comment suffix alongside the modeling role, so a catalog surface shows `[dimension, entity, natural_language]` where it shows `[fact, scd2]` today (append_modeling_tag, provisa/core/modeling_tags.py,) — reaching pg_description, GraphQL introspection and the Flight/MCP catalog description through the one shared formatter.
    - Tag ids ride in the catalog description suffix

#### URI Dereferencing API

- Governed dereference API for semantic data-location URIs: REST and MCP tool `resolve(uri, as_of?)` that takes a Provisa semantic-address URI and returns the governed value(s) it addresses, optionally at a point in time.
    - Dereference a semantic address on GraphQL, REST and MCP
    - Every role's service resolves a semantic address

### Changes

#### api

- A vector model error on GraphQL answers 400 naming the model, as SQL does

#### audit

- A GraphQL request's row records the status its client was answered

#### auth

- A member's attributes -- the org's record over the provider's claims
- pseudo-roles -- a role held for what a member's attributes say about a declared claim type
- A regional node's own work carries its region pseudo-role

#### billing

- The current bill and the next charge are amounts
- A billing date shows its calendar day in every time zone

#### data

- A still-starting source answers 503 on /data/sql and REST; demo tests wait for govdata

#### demos

- A demo with no seeder bundles
- The insider review demo
- Reviews and vulnerabilities name their fields as their sandbox does
- A demo's profiler runs at its template build and its sandboxes carry the runs
- A sandbox's tour is warmed in the background as it opens
- A demo's setup makes an environment, and its tour picks one
- The testdata demo -- realistic test data without touching production
- The sonnets Qdrant store recovers its snapshot at every start
- The sonnets vectors source reads with the store's read-only key
- The local government vertical -- opioid settlement spending against overdose deaths
- A demo store is healthy only once its first-start load has ended
- A bitemporal view's history travels with its demo's bundle
- The data vault demo, Northwind in two sources, its vault declared as views
- The data vault declares its masks as a column's masks list

#### deploy

- Every cloud deploy refreshes the demo-stores sync and its units
- A cloud deploy reaches the sandbox VM too

#### deps

- Mcp 1.30.0, past CVE-2026-59951

#### domains

- A domain tree -- a business unit and the organizations within it
- The domains query reads its rows' steward and parent; no requirement id in a view
- The parent picker's explanation is its field's hint

#### environments

- A whole-model merge request stores its selection as SQL NULL

#### explorer

- Show SQL shows the statement the compiled pipeline runs
- Show SQL is governed by the compiled pipeline's own stage

#### fakes

- A hash of an integer column narrower than BIGINT fits its type (#272)

#### federation

- The attach walk leaves alone a table Provisa reads itself
- A statement on a source whose server is still starting waits, then answers 503 by name
- An incomplete attach walk waits its retry interval, and logs a failure once

#### files

- Connector bundles download off the request path; an unanswered catalog query is retried

#### governance

- A condition on a union's output relates no branch to another (#271)

#### graphql

- A PostgreSQL double precision column maps to Float

#### grpc

- A PostgreSQL double precision column is a proto double

#### install

- The control-plane compose splits into the control plane and the sandbox tier by profile

#### join guard

- A join on a column a view computes relates along the view's relationship (#270)

#### join-guard

- A filter on a value computed over a union view relates nothing

#### kafka

- A Kafka live table lands every message it can read, in every org that declares it
- Every org reads a CDC topic whole, and keeps its own SingleStore pipelines

#### landing

- A pgwire connector table (govdata among them) lands through its landing filter

#### masking

- A regex mask replaces every match on every engine

#### openapi

- A config-declared OpenAPI source states its auth

#### orgs

- Destroying an org stops its connector servers

#### pgwire

- A server bound off loopback is reached where it listens; endpoint fixtures ready the org folder
- A caller that waits has an unanswered first catalog query sent again within its wait

#### profiler

- A configuration names a profile result table by its member and kind

#### replicas

- A build waits for a source server that takes minutes to start
- Each environment has its own build runner; one built after prod no longer replaces prod's (#273)

#### sandbox

- A demo sandbox carries its template's environments, and its visitor switches between them

#### sandbox-tier

- The tier finds the deployment's demo SQLite data

#### sandboxes

- One connector server per demo version, shared by its sandboxes
- A sandbox tier on a VM of its own stops that VM when no sandbox is used

#### security

- Composite grants -- an entry a+b is held by a reader holding all of it
- A column's masking is one ordered mask list, first entry held applies

#### sla

- service-level limits are settings, breaches are stored and listed in ops

#### subscriptions

- A subscription on a push-landed table re-runs when a batch lands

#### tables

- A table may fix the arguments its source takes by name
- A table's landing filter is applied when its replica lands

#### temp-tables

- session_temp_schemas is kept in the region's state store

#### terraform

- The sandbox tier on a VM of its own

#### ui

- A scoped merge's object ticks sit in the form's FormChecks
- The table form sets a table's landing filter and fixed arguments
- The grant picker's combine button takes its hint from Tip

#### ui-server

- A request for the sandbox tier starts its VM when it finds it down

## v0.1.0-alpha.489

Released 2026-10-15.

### Features

#### Change Signals

- A table read where it lies (not replicated) that declares a probing change signal -- probe or ttl_probe, its own else its source's -- is a signal-only node of the event loop: its probe (MAX of its watermark column, a count, or its probe_query) runs on its cache TTL, and a change is posted to the materialized views that read it, which recompute and ripple on to theirs.
    - A live table that declares a probe is a signal-only node for its views
    - The quality demo, data you can trust, driven by landing alone

#### Triggers

- A trigger says when something runs, and what runs is an ordinary command (python or statement), a SQL statement or a webhook.
    - A trigger fires on a table's change or event, and runs a command
    - A table floored by its own replicate builds its replica (#269); a write to a native-feed table fires its triggers from the feed alone
    - The admin Triggers page -- every automatic run in one place, read and edited in its panel

### Changes

#### api-source

- An API-backed table's date and timestamp columns are typed (#267)

#### auth

- Rights follow the roles a request acts as, never every role held
- Selecting an environment is judged by every role held, as it is decided before the acting roles exist

#### demo

- Cap the reviews run-time stores' memory for the cloud node

#### demos

- A bundle build lands a live account's tables with its key, and refuses an empty one
- The insurance vertical demo -- property exposure against hazard risk
- A tour's Polly questions are answered at the bundle build and stored
- A scaffolded demo's example step opens on the GraphQL page
- A scaffolded demo's example step opens the GraphQL page

#### deploy

- The embedding-model hub cache on the host; a sonnets sandbox searches by meaning (CI)

#### duckdb

- A MongoDB column named apart from its field is found (#266)

#### explorers

- A demo visitor's OpenAPI page loads, and GraphQL reopens on its last tab

#### gcp-saas

- The control-plane Postgres runs on the control-plane VM; Cloud SQL removed

#### governance

- A star reads only the columns the role is served; RETURNING is governed

#### jdbc

- Drop the connection mode setting, which had one value and changed nothing

#### mv

- A view the event loop refreshes is marked fresh in the MV registry (#268)

#### replicas

- Replica builds as ops tables, with a build log and failures per source and day

#### roles

- A role's max_rows is set by the nearest role that declares it, as rate_limit is

#### row-level

- A row-level table is read through ANY filter, fetched by that filter; DIRECT reads live

#### sandbox

- cloud.provisa.dev offers weblogs, files, sonnets and reviews
- A demo's own embedding models reach its sandboxes
- A demo sandbox is deleted after an hour without use (was a day)

#### temp-tables

- A session's temporary tables are bounded, and a gone session's are reaped

#### ui

- js-yaml is a dependency of its own, so the live demos page check runs

#### writes

- A source with no direct driver is written through the engine

## v0.1.0-alpha.488

Released 2026-10-15.

### Features

#### Table Registration

- A TTL of -1 means never expires: the table lands once and its replica is never stale after that. -1 can be set on a source, which its tables inherit as they inherit any TTL, or on a table.
    - -1 never expires

### Changes

#### demo

- The reviews named demo -- MongoDB, Elasticsearch and Neo4j joined on the product
- Publish the reviews demo

#### demo-bundles

- MongoDB, Elasticsearch and Neo4j stores are carried in a demo's bundle

#### demos

- No engine-state poll for a demo sandbox or the anonymous demo open
- scripts/new_demo.py scaffolds a named demo, scripts/check_demo.py runs its checks

#### deploy

- Restart the sandbox tier, seed the overlay from the image, name the org in the engine probe

#### deps

- graphql-core stays below 3.3, the version the tests run on

#### duckdb

- A source whose server is down fails no other source's listings (#256)

#### elasticsearch

- A native search keeps each hit's score

#### graphql

- A table's parameters are in the read's WHERE, before ORDER BY and LIMIT

#### image

- Install the locked versions from uv.lock

#### replicas

- The table's read panel shows its replica and indexes, as its form does

#### ui

- The OpenAPI page types a table's parameters into their textarea

## v0.1.0-alpha.487

Released 2026-10-15.

### Changes

#### image

- The app image carries Java 21, so a files source's connector server runs wherever it does

## v0.1.0-alpha.486

Released 2026-10-15.

### Changes

#### boot

- The platform fake key is held before the engine is woken

#### deploy

- The cloud sandbox tier runs the deployed app tree and offers weblogs and files

#### deps

- Bound sqlglot to 30.23, and run on it (scope set_operation_scopes, lineage schema)

#### federation

- An org on its own engine is walked by its own registry, not the deployment's file

#### sandbox

- A sandbox's AI uses the tier's one endpoint and model, its key from the deployment org's vault
- A visitor cannot choose the model the deployment's AI endpoint runs
- The sandbox tier never provisions an engine

## v0.1.0-alpha.485

Released 2026-10-14.

### Changes

#### demo

- Publish the sonnets demo, its card the echo network
- The sonnets run-time stores say where the config reads them
- The sonnets run-time Qdrant is the build's version, so its snapshot restores

#### demos

- A demo's own sources are read live, from stores the sandbox tier's node runs
- Qdrant 1.19.1, the server the 1.19 client supports
- A refusal names no requirement id
- A demo's setup.yaml runs in its bundle build, and Qdrant stores are carried as snapshots
- The node pulls with the package's own module; the sandbox data is a host directory
- The weblogs run-time database publishes on the loopback only
- A bundle in an older demo format is refused by name at load, not per request
- The sonnets run-time stores publish on the loopback only
- A live demo runs as its own visitor, never as the browser's Provisa login
- A live demo is opened anonymously, the browser's login is not sent
- The pull's help text names no requirement id
- The files demo, an instant data lake

#### events

- The event loop types a view's columns from its governed build

#### freshness

- An abandoned logical slot is dropped at start; the kafka signal on a database source is received
- A load-protected table is refreshed only in its off-peak window
- The signal change signal's receiver is a webhook

#### graphql

- A view column named like another type's join is not counted as one (#265)

#### launcher

- Starting a named demo stops every other demo's stores first

#### mcp

- The glossary, data product, metric and proposal tools read the model store

#### mv

- An aggregate is answered from a view that pre-aggregates its table, re-aggregated exactly
- /data/sql is answered from materialized views through the one pipeline

#### polly

- Cap every model call's output, and search the catalog before saying there is no tool
- Claim nothing beside the call that does it, explain a failure only from its error
- Never show the text the model writes beside a tool call
- Rows with every file, an answer from the last round, and a bake-off that resumes
- The last round carries no tools, and the built-in tables say whose record they are
- A blank final reply ends with a note, and a page opens besides the answer
- The last round is told to answer, and neither a late call nor a written one stands

#### search

- Each read of a search table is read under its own arguments

#### site

- Every page carries the home page's navigation, with a test that keeps them in step

#### tour

- A named demo's tour ends on a choice: the Deep Dives, or the demo's own page

#### vector

- A local embedding model is used by one request at a time
- A local model's version is read once the model is fetched

#### views

- A view named in a string literal is not expanded

## v0.1.0-alpha.484

Released 2026-10-14.

### Changes

#### demos

- A demo template is landed when its source tables are, not its views
- A sandbox is shown its own demo's example queries and tour
- A sandbox's visitor sees what the demo shows its admin, and holds the demo's own roles

#### freshness

- A debezium table is kept current by its stream; native is refused where nothing receives it
- A PostgreSQL table on the native signal follows its database's logical replication

#### polly

- Keep narration apart from answers, require graph relationships, confirm only what results show
- Draw graphs in the chat and hand over results as files

#### replica

- A table's preflight check runs in its replica build, over the rows it streams
- A checked build's abort and quarantine are the event loop's own events

## v0.1.0-alpha.483

Released 2026-10-14.

### Changes

#### core

- The host file lock works on Windows

#### cypher

- Two variables' same-named properties are two columns
- Two variables' same-named properties are two columns

#### demo

- The sonnets named demo, and a demo's setup.yaml and extras
- The weblogs columns' descriptions load whole
- The sonnets tour, its integration test and its CI lane
- The sonnets echo network returns its IN edges, so sonnets are drawn joined

#### demos

- Each published demo is a bundle published to GHCR and pulled where it runs
- A bundle build's seeding run reads its demo's logs, and hands back what the stores wrote

#### elasticsearch

- A table registered from a native search

#### graph

- graph-model holds no browser storage, so graph-render builds

#### guard

- A join on a view's constant column relates along the view's relationship
- A constant a view reads from a view it reads is its own
- Mark the views the governed expansion inlines too
- A view's mark reaches every branch of its union

#### mv

- Every view build and a stale view's read go through the one pipeline

#### org-runtime

- A retired org that owns its engine closes it

#### registration

- A table its native query produces is not listed from its source

#### sandbox

- The sandbox tier owns the demo sandboxes' DuckDB files
- The cloud control plane runs the sandbox tier as its own single-worker service
- Demo templates, opening a sandbox from one, its limits, and its reaper
- A template is ready when every table has a landed replica, and ready hooks run then
- Proven on a local test instance; the fixes the proof found
- A signed-in person opens their own named sandbox of a demo
- The ordinary API refuses a sandbox by name; a visitor lands a source of their own
- /demos/<name> opens a demo sandbox, and its welcome names every limit it runs under
- A demo is seeded when the release is built, and a deployment runs none of its sources

#### security

- A sandbox org's sources reach only its demo's own destinations and public addresses
- A sandbox org connects to the address its check resolved; what cannot be pinned is refused

#### sources

- Two creates of one source id at once -- one is created, the other refused by name

#### tour

- Demo tours tell the scenario, how it fits, and one answer across surfaces
- A step's anchor wait notices a mark set on an element already there
- The weblogs payoff is SQL and REST over the demo's views
- The weblogs track's summary names the answer it shows (SQL and REST), not Polly's

#### types

- An instant is timestamptz, carried as UTC with its zone
- An interval is read as the duration it is, its months kept

#### ui

- Register Table offers an Elasticsearch native search, as a MongoDB pipeline
- The IR vocabulary has timestamptz, an instant

## v0.1.0-alpha.482

Released 2026-10-14.

### Features

#### API & Integration

- GraphQL DB-backed fields used as nested paths must expose root-query controls (`limit`, `offset`, `where`, `order_by`, `distinct_on`) and apply them during SQL compilation.

#### Admin Configuration

- Metadata export admin surface: an Admin tab that configures the per-org export target (provider, endpoint, credentials, reconcile schedule), runs a connection health check against the live catalog, triggers an on-demand full reconcile, and shows the last publish outcome including the per-asset errors a partial publish returns.
- Provisa supports configuring custom AI/LLM endpoints reachable over either the OpenAI-compatible or Anthropic-compatible wire protocol.

#### Admin UI

- Hover tooltips on domain groups, table nodes, and column items display their descriptions.
- The ERD modal provides download buttons for exporting the current diagram as SVG, PNG, or JSON.

#### Aggregate & Group-by Queries

- A group-by is ordered by its keys and its aggregates.
    - The weblogs demo opens the GraphQL explorer on its example queries

#### Approvals

- A relationship request is decided by the domains it touches.
- A creation request of any type -- relationship, view, table, source or webhook -- is carried out only through its approvals, and carrying it out creates what it asks for.

#### Authentication

- The break-glass superuser of can sign in from a browser under any auth provider.

#### Authorization

- No organization can be left with zero org_admins by an ordinary administrative act.
- A user may delete their own Provisa account.
- A user can never change their own role in an org.
- An org id is validated on creation and immutable thereafter.

#### Branded API Sources

- A Google Workspace or Microsoft 365 source reads, beside mail, the calendars and To Do / Google Tasks of its accounts, and writes their events, attendees and tasks.

#### Cache

- Per-user-class staleness tolerance as a table attribute.
    - createSource judges the definition first, then the id

#### Change Data Capture

- A Debezium topic encoded in Avro with a schema registry is read as its rows.

#### Client Access

- A role is the one the request runs as, on every data route, and a catalog is listed for the caller it is listed to.

#### Column-Level Masking

- Masking has a consistent form.

#### Commands

- A command can be implemented by a Provisa SQL statement with typed arguments (implementation kind statement): the statement is governed and run through the one pipeline under the caller's rights like any statement, its arguments bound as values, never spliced.

#### Control Plane & Deployment

- Schema clustering (`provisa/schema_clusters.py`): uses networkx Louvain community detection to group registered tables into semantic clusters (l1/l2/l3 cluster labels).

#### Data Catalog Integration

- Premium-gated metadata export: the metadata export connector is an enterprise/premium tier entitlement, extending.
- New DataProduct entity, domain-scoped: id, domain_id (required FK — a data product has exactly one owning domain), name, owner (person accountable for this product, distinct from Domain.steward), description.
    - createDataProduct refuses an existing id; an edit saves through updateDataProduct
- New bigquery_dataplex.py MetadataExport adapter, the BigQuery counterpart to's snowflake_horizon.py: populates BigQuery Dataplex catalog entries and registers each DataProduct as a BigQuery Analytics Hub listing, backed by a dataset containing the product's member tables.
- The Data Product detail panel shows a computed, read-only "Lineage" section rendering the same interactive LineageDag graph visualization used on the Lineage page ("model / lineage"), scoped to a subgraph of the whole federation-wide column lineage graph (GET /admin/lineage/federation, no domains filter, so cross-domain ancestor chains are never severed): [SUPERSEDED by 2026-10-03 -- draws contributors in lanes -1, -2... by longest upstream path, so the subgraph is the full upstream ancestry plus one hop downstream.

#### Data Discovery

- Provisa shall document and expose the HTML crawler capability from the underlying Calcite file adapter (org.apache.calcite.adapter.file.converters.HtmlCrawler) as a first-class, general-purpose source-discovery method for registering sources that expose only HTML index pages with linked data files (CSV, Parquet, etc.).

#### Data Governance

- Tag-scoped RLS rules (follow-on to and existing table/domain-scoped RLS/041/402): an RLS rule may attach to a tag; at compile time it resolves to all tables carrying that tag and its predicate is AND-ed with any table- and domain-scoped predicates (most-restrictive-wins, no precedence tiers; multiple matching tags each contribute AND-ed predicates).
    - A row filter attaches to a tag and applies to every table carrying it
    - The tag rule's table count is its field's hint, as the form rules ask

#### Data Quality Sources

- A DATA PROFILER is a source type, registered and used the way the data-quality checker sources are.
    - A source that owns no replica is never promoted or rebuilt
    - A source that owns no replica is no landed node of the event loop

#### Data Residency

- Data residency (the rule in force is the SIMPLE RULE amendment at the end). [SUPERSEDED by the SIMPLE RULE amendment below, 2026-10-02 -- the maintainer reduced residency to regions, the row and column rules, and a region on a table for whole-table copies.

#### Demo Provisioning

- `start-ui-install.sh --demo` accepts an optional named demo argument: `--demo` alone selects the standard demo (unchanged behavior), and `--demo <name>` (e.g. `--demo perf`) selects a named demo variant with its own data/source setup, spliced into config the same way the existing `--source=<name>` mechanism splices in demo/sources/<name>/fragment.yaml. [SUPERSEDED by 2026-09-24 -- named-demo tooling moved from scripts/bench-data/ to demo/named/perf/, the fixed layout below.
    - The weblogs named demo, a web shop's access and database logs joined to its tables

#### Domains

- A domain is deleted only when nothing refers to it, and the operator is shown everything that does.

#### DuckDB Connectors

- The pg engine should gain a PgDuckdbMotherDuckConnector (Mechanism.ATTACH_RW) using pg_duckdb's own FOREIGN DATA WRAPPER duckdb TYPE 'motherduck' SERVER + USER MAPPING (token) mechanism, confirmed present in pg_duckdb's own source (src/pgduckdb_fdw.cpp's FdwType::MD, test/pycheck/motherduck_test.py) — pg_duckdb can live-attach a MotherDuck-hosted DuckDB database today, [SUPERSEDED by amendment, 2026-10-03 -- PgDuckdbMotherDuckConnector exists and is registered (provisa/federation/connector_duckdb.py:713-772, provisa/federation/engine.py:698).
    - The Postgres engine reads the table pg_duckdb's sync creates, in the schema of its naming
    - Wait for pg_duckdb's sync to settle; both MotherDuck e2es run in the warehouse lane

#### Engine Streaming Terminals

- Warehouse federation runtimes (Snowflake, Databricks, BigQuery, MSSQL, ClickHouse) must implement genuinely streaming `run_sync` terminals using their respective server-side chunk APIs.

#### Enterprise Sources

- Salesforce is a source type.
- Cloud inventory is a source type.
- ServiceNow is a source type.

#### Environments

- An organization runs its governed model in more than one environment -- a dev environment, a test environment, a staging environment, whatever it names -- without becoming more than one organization.
- An environment is physically a schema of its own.
    - A name the organization already has is refused as taken, not as invalid
- Copying an environment carries the governed model and nothing else, and what it carries is stated as an allow-list rather than an exclusion list, so a table added to the org schema later does not travel by default.
- Copying into an environment that already exists is a merge, not a replacement.
- Where an environment points is a per-environment fact that no copy ever supplies.
- Every environment other than production owns stores of its own, because a non-prod environment needs data that is neither a read of production nor absent.
- An environment populates a table from its own model when no source holds one.
- An environment can be protected, and a merge into a protected environment waits for an approval by someone other than the person who requested it. prod is protected when the organization has more than one member; any environment can be protected by an org_admin.
- An environment name is bounded, and so is the number of them. makes an environment a schema of its own, so a free-form name is a schema name and an unbounded count is unbounded schemas: the short-lived per-developer environments an org actually creates -- dev-alice, dev-1482-refund-fix -- accumulate until nothing reaps them.
- Every environment has a repository, and the repository is a PROJECTION of the environment, never an authority over it.
- The exclusion of bindings and secrets from what an environment carries is an exclusion of COLUMNS, and it cannot reach inside one.
- The serialized model is a DIRECTORY OF FILES, one file per entity, and never a single document.
- Authority over a model is scoped to an ENVIRONMENT, and the ordinary way to acquire it is to create one.
- An organization's environments come in TWO KINDS, and the difference is who made them.
    - A capped PostgreSQL source's DuckDB pool holds its share of the cap and waits
- A DEVELOPER IS ALWAYS LIMITED BY DOMAIN MEMBERSHIP.
- EVERY MUTATION THAT NAMES A DOMAIN MUST BE GATED ON IT, AND EVERY MUTATION THAT OUGHT TO NAME ONE MUST NAME ONE. makes a role's domain_access the limit on what a member may change; a limit is only real where it is checked, and today the check runs in two places.
    - Assigning or removing a tag is checked against its target's domains
    - Saving a metric is checked against the domains its expression reads
    - createRole refuses an existing id; a role's edit saves through updateRole
- THE BROWSER MUST BE ABLE TO SAY WHICH ENVIRONMENT IT IS READING, AND EVERY REQUEST IT MAKES MUST SAY THE SAME ONE. makes the environment a request-scoped choice carried in ``x-provisa-env``; until now nothing in the UI set it, so every screen read prod and the branch was reachable only from curl.
- THE ORG'S ENVIRONMENTS MUST BE ADMINISTRABLE FROM THE ADMIN UI, AND ITS REPOSITORY MUST BE READABLE THERE.
- THE CHOICE CALLS BASE-OR-BRANCH MUST BE PRESENTED AS "INHERIT CONNECTIONS", AND MUST DEFAULT TO OFF.
- What a person may do to DATA inside an environment is what their ROLES say, and an environment never overrides it.
- A merge may END the environment it came from.
- Because every model change commits, an environment can be MOVED ALONG ITS OWN HISTORY rather than edited back by hand.
- A BRANCH SAYS WHETHER IT HOLDS WORK THE REMOTE DOES NOT, and an operator can send it or end it.
    - An org's environments are served once the org is ready on this node
    - The org is readied after the guard authorizes, never for a refused caller
- UPDATING LOCAL FROM THE REMOTE IS AN APPLY, not a ref write.
- THERE IS ONE VERB AND IT IS "APPLY A SHA".
- WHEN THE TARGET BRANCH IS GOVERNED BY PULL REQUESTS, REVIEW IS ASKED FOR ON THE HOST.
- A MERGE THAT OVERWRITES SOMEBODY ELSE'S WORK SAYS SO.
- A PULL SAYS WHAT IT OVERWROTE, AND A REFUSED PULL SAYS WHAT COLLIDED.
- A NODE WHOSE PROJECTION DOES NOT HOLD THE COMMIT STILL LISTS ITS ENVIRONMENTS.
- A synthetic dataset is a model object that fills an environment with generated data shaped like the estate it is drawn from, at a chosen scale, so tests run over larger volumes, or smaller ones, with similar processing characteristics.
- Every environment other than production has one data mode, chosen when it is created and changeable afterwards: Inherit, Unbound, Test (fake) or Test (synthetic).
- Changing the domain of a registered table or view requires the caller to hold the registering right in the domain it is in AND in the domain it is going to.

#### Feature

- A Kaggle source's token is resolved via provisa/core/secrets.py resolve_secrets (${env:KAGGLE_API_TOKEN} token pattern), never a raw os.environ read.
- An end-user-facing script (scripts/install-pg-ext.sh or similar) that stages provisa_pg_ext's bundled FDW/extension binaries into an OPERATOR-SUPPLIED, external Postgres instance — the same manual sequence performed live this session (pip install provisa-pg-ext; docker cp the platform-matched lib/*.so + share/extension/* files into the target's pg_config --pkglibdir/--sharedir; ALTER SYSTEM SET shared_preload_libraries for pg_duckdb; restart; CREATE EXTENSION).

#### Federation Engine

- SaaS runs ONE engine architecture on one GKE cluster.
- An isolated engine idles to zero and wakes on traffic, so the org pays for compute it uses rather than 730 node-hours a month.
- The shared engine is one multi-tenant Trino deployment on the GKE cluster, serving every Starter org from a single cluster whose node pool grows as customers sign up.
- The cold-shard wake is reconciled with the executor's retry budget by ORDERING, not by widening the budget.

#### Federation Engine Abstraction

- Pgwire auth must be a pluggable provider interface selected at launch (trust | local | oidc), superseding today's fixed MD5/cleartext wiring while keeping trust/cleartext as the baseline.

#### Federation Engines

- MotherDuck is a federation engine, the production counterpart of the embedded DuckDB engine: an organization that chooses it has its replicas kept in a database of its own MotherDuck account and its governed SQL run there, in the same DuckDB dialect it runs in on the embedded engine, so what was built and checked on local DuckDB in development runs unchanged in production.
    - MotherDuck is an engine -- the DuckDB engine with its store in MotherDuck
    - A MotherDuck store's token reference is resolved at use; complete

#### File & Lake Sources

- File connector sources accept a directory glob pattern to enumerate CSV files.
- The pg engine's snowflake/databricks/bigquery source types should be reachable as a live, zero-copy ATTACH_R via each warehouse's Iceberg REST catalog (Snowflake Iceberg Tables' catalog integration or SYSTEM$GET_ICEBERG_TABLE_INFORMATION, Databricks Unity Catalog's Iceberg REST endpoint / UniForm, BigQuery BigLake Metastore), instead of only Mechanism.DIRECT (native driver, materialize-required).

#### File Sources

- A files source may declare an HTML crawl, and Wikipedia is a source type carried by it.

#### Form Layout

- Every form in the UI is laid out by one shared form layout, and a field is as wide as what it holds, not as wide as the form.
    - A tick with a (?) takes no withAsterisk; the e2e specs add a source through the type picker

#### Governance Column Visibility

- The admin API answers a caller with the catalog that caller may see, as every data surface does.

#### Graph Query

- A junction table is declarable as a first-class Cypher relationship, and its own columns are that relationship's attributes.

#### HTTPS/TLS Configuration

- GCP/Docker cluster deployment must serve all endpoints over HTTPS with TLS encryption.

#### Helm Auth Configuration

- Helm chart deployment MUST NOT require Firebase.

#### Infrastructure

- Helm chart for production Kubernetes: horizontal Trino worker scaling, resource groups, HPA autoscaling.

#### Large Result Redirect & CTAS

- HTTP-addressable local materialization.
    - A large result with no object store lands in the org's folder again, served to its owner
- Per-username access scoping on served redirect files.
- Age-based reaping of local redirect files.
- Extend engine-native ctas_redirect beyond Trino to all object-store-capable engines.
- Sink-tier selection rule. ctas_redirect targets the engine's native object-store sink when a reachable object store is configured (PROVISA_REDIRECT_ENDPOINT + credentials); otherwise it falls back to Provisa's local_dir + HTTP-serve.
- Buffered transports (GraphQL, JSON:API, Bolt) surface materialize/CTAS handles via protocol-native side-channels; streaming transports (pgwire streaming, Flight SQL, airport) opt out (deliver=None).

#### Materialization Store

- Delta fetch for materialization_store REPLICA incremental refresh: an INCREMENTAL RELOAD strategy for datasets whose federation strategy is MATERIALIZED — data is landed/cached, so full re-pull is the cost delta avoids.
- Write-surface isolation.
    - A table its query produces is not listed from its source
    - A csv or parquet source floored to its replica builds it from the file
    - The file reader is a floored file source's only; any other keeps the engine's in-place copy
    - A source with no listing keeps the columns its registration supplies
- A replica is indexed as its table is used: its declared primary key, enforced as the key, and the columns its declared relationships join on -- the foreign key of each relationship whose source is the table, the target column of one whose target it is when that column is not the key, and both foreign keys of a junction table a relationship traverses.
    - A replica carries its key and its join columns' indexes, built after it lands
    - A landing's index plan reads the model through its own store

#### Model Propagation

- A model or governance change reaches every worker and every instance.

#### Multi-Method Authentication

- Login page surfaces multiple simultaneous authentication providers (Google, GitHub, email/password), advertised by the server as a list.

#### Object Deletion

- One rule governs the deletion of every kind of object.

#### OpenTelemetry Instrumentation

- Lean tracing by default, detailed tracing in a debug mode.

#### Operator Settings

- Every operator setting has a UI surface.

#### Org Identity & Subdomain Addressing

- The subdomain `{org}.provisa.dev` is the org identity.
- The subdomain never authorizes — it only names the org.

#### Org Selection

- Under multitenancy a request acts in the org it names, and there is no implied org.

#### Performance Benchmark

- Randomized workload benchmark with tuning knobs.
    - The sizing and tuning guide, workload sizing, and the worker sweep in the report
    - The guide counts its cores in the singular and points at the models below
    - The report reads a replica run's idle.json where the run writes it, and its TTL from the run
    - Read the server's CPU from another host through server_cpu_agent.py
    - The three-device optimistic benchmark harness and terraform/perf
    - cross-source joins under each replication setting as a knob; engine as a sweep dimension
    - The three-device harness ready to run: its contract, device 1's Bolt port, a dry run
    - terraform/perf: Spot by default, idle shutdown, and a startup script that leaves no hand steps
    - The three-device benchmark as one command: apply, wait for ready, run, copy back, destroy

#### Process Roles

- Process roles.

#### Query Cost Control & Monetization

- Hard query-cost caps as a monetization gate: each org tier enforces ceilings on query cost (CPU time, peak memory, bytes scanned, execution time) AND on OUTPUT ROWS returned.

#### Query Governance

- No code path returns query or action-invocation rows without a role bound to them.
- A read that a role's row limit cut short says so.

#### Regions

- Regions -- separate clusters on one shared model.

#### Registration & Governance

- NL-assisted table candidate discovery for registration.

#### Release Integrity

- A release is made only by the Release: cut workflow, run by hand on main with the version.

#### Replication

- Replication is bounded, single and survivable.

#### Requirements Taxonomy

- A feature layer sits above the requirements, because the requirements are too detailed to explain the product. docs/arch/features.yaml lists the customer-facing features, each with a name, its section, a visibility (feature, quality, or internal) and an audience (consumer, modeler, admin or operator; none for an internal feature), told in a few short conversational paragraphs, one concept each.
    - A feature layer above the requirements

#### Result Handling & Streaming

- Engine-agnostic query results implement a ResultStream Protocol (column_names, column_types, batches(), iter_rows(), rows); two implementations separate fully-materialized QueryResult (bounded metadata/catalog paths) from lazy StreamingQueryResult (row batches, once-consumed iterator, stateless for shared-pool SaaS deployment).

#### Row-level materialization

- A registered table may opt in (row_materialize, per-table, never a global engine mode) to a row-level, query-driven cache: an individual row is fetched from the live source and upserted into a landed replica strictly when a query's compiled plan resolves a concrete primary-key equality/IN bound against that table — never eagerly, never a background pre-fetch of a key nothing has queried.

#### SaaS Billing

- Pro is sold as three fixed sizes -- S (4 vCPU / 32GB), M (8/64), L (16/128) -- expressed as the node pool machine type and the pod requests/limits of the org's engine, each a plan-fixed constant rather than a control the org adjusts, since each plan is a fixed setting and orgs do not tune their own entitlements.
- A Starter org signs itself up with a credit card and can answer, without asking anyone, what it owes right now and when the card will be charged.

#### Security

- A SECRETS SERVICE, WITH PROVISA'S OWN STORE AS THE ONE THAT IS ALWAYS THERE. ${provider:reference} already names a provider, and a registry already exists to hold more than one -- [SUPERSEDED by amendment, 2026-10-03 -- Before-state narrative; the secrets registry now registers five backends and ${secret:...} resolves through the configured one.
- ADMIN / SECRETS: NAMES GO IN, VALUES NEVER COME BACK OUT.
- A SECRET BELONGS TO SOMEONE, AND WHO IT BELONGS TO IS PART OF ITS ADDRESS.
- A tag definition carries a Sensitive data option, and a column carrying any tag with that option set is a sensitive column.
- A standard data_steward role is seeded in every organisation: the owner of a domain's governance.

#### Semantic Interchange (Apache Ossie)

- The model export offers a dbt sources file (sources.yml) written by Provisa directly, for dbt projects that do not use the semantic layer and so cannot load the semantic manifest of.

#### Session

- A browser session's stored bearer is cleared only when the identity provider says the credential is dead (signed out, revoked, or a refresh rejected for the credential itself).

#### Source Connectors

- MongoDB sources can optionally use MongoDB's native change streams ($changeStream) to achieve near-real-time freshness for materialized and cached data, instead of relying solely on TTL-based polling refresh.
- A table on a MongoDB source can be registered from an aggregation pipeline instead of a collection.
- Amazon Athena is a source type.
    - Ported onto main: an array parameter's GraphQL argument stays a list; main's later tests take a native array as JSON
- Azure Cosmos DB for NoSQL is a source type (cosmosdb): an account endpoint, a database, and either the account key (a secret reference) or a Microsoft Entra service principal (tenant, client id, client secret as a secret reference), each set on the Sources form.
- Qdrant is a source type.
- Node is a source type.
- A MongoDB source can be reached by a connection string, so a MongoDB Atlas cluster can be registered.
- A query command can run a MongoDB aggregation pipeline whose output has no stable shape (implementation kind source_query).
- Python is a source type, the Python counterpart of the Node connector, on the same language-neutral worker protocol.
- A statement whose tables all belong to one Qdrant source routes DIRECT to Qdrant, as any single-source statement routes to its source: the governed statement is lowered to Qdrant's API.

#### Source Registration

- A file-based source (DuckDB, SQLite, CSV and Parquet: the sources whose path names a file) can be given its file by uploading it.
    - MotherDuck on the Sources form, and the file-source and MotherDuck docs

#### State Store

- The state store.

#### Table Registration

- A table declares its parameters: the values a caller passes to the source to read the table.
    - Ported onto main: an array parameter stays repeated of its element type; the landing fixture's columns carry embedding
    - A parameter column is named _nf_<name>; only the context's parameters are native arguments
    - A refusal names each missing parameter once

#### Tenancy Architecture

- Org is the canonical tenancy subject in Provisa; "tenant" terminology is retired.

#### Testing

- The fabric e2e test (provisa-ui/e2e/source-to-query-cloud-warehouse.spec.ts) was skipped on a "[28000]... system update...

#### UI

- Every results grid -- the SQL explorer's results, the admin reports and the governed table viewer, which share one grid -- filters each column Excel-style, typed by the column's type, beyond today's case-insensitive contains.
- Every admin page that lists items (sources, tables, security roles and rules, relationships, views, commands, metrics, data products, team, requests and the like) presents its list in one style, the style of the Register Tables page -- the same list component, header row, row height, spacing, typography, action placement, selection and expand behaviour, empty state and loading state.
- The product tour is a short core tour followed by a menu of topics.

#### UI & Design Patterns

- On a list page a row's detail opens in a right-hand panel beside the list, not under the row.
- The head of a detail panel is pinned: the item's name, then its actions.
- An item's detail is divided into sections shown as top-level tabs, not as one scrolling form with expanding sections.
- The column editor is four tabs: Naming, Type and keys, Access, Testing.
- Creating an object is short.
- The explorers are outside the form and detail-panel rules: the pages at /query, /graph, /sql, /jsonapi, /openapi, /explore and /lineage are a different experience and keep their own layout.
- The application has one tooltip style: the style of the ERD icon button on the Tables page.

#### Vector Search

- A source's native vector capability is detected when its driver connects, at boot and when the source is registered (executor.pool.SourcePool.add): what its own SQL computes a cosine_similarity search with.
- A `cosine_similarity(column, query)` function is available in Provisa SQL (and so in Cypher, which compiles to it).
- A table served from its replica is the fallback for a vector search: where the engine reads the replica through its store's own SQL, the replica holds an embedding column as a vector with an HNSW index for cosine distance.
    - An embedding column is exempt from the JSON rule end to end
- A replica's vector index is as fresh as the replica: it is built with each replica build and swapped in with it, and the DuckDB engine re-indexes its copy whenever the store changes.
- Embedding columns are subject to the full Provisa governance stack: row filters, domain boundary rules, and sensitivity tiers.
- Query-time vectorization must be supported: when a similarity search is expressed with a text string rather than a raw vector, Provisa calls the declared embedding model to generate the query vector before executing the search.
- Column masking does not apply to embedding columns — a vector cannot be partially masked and remain semantically meaningful.
- embed(model, text) is a function of Provisa SQL that returns the embedding of text by a model from the registry, its dimensions those of the model.

#### Write Admission

- One write admission.

### Changes

#### admin

- A numeric size no numeric can have is refused with a code, naming the column
- A REST role or user create whose id or username is taken is refused by name
- A REST role create finds an existing role as the router's other checks do
- A REST role create judges the definition first, then the id

#### api

- A binary value is written to JSON as \\x and hex

#### askamerica

- Trino reads an AskAmerica source live through its PostgreSQL connector, once proven

#### auth

- A token name one of the owner's live tokens in the org has is refused

#### cache

- The response-cache opt-in in HTTP request headers on every HTTP surface
- A Flight cache hit types each registered column as its miss does
- A plan answered from the cache carries its statement's result shape

#### catalog

- The type a catalog lists for a column is the type a read of it returns

#### commands

- A create on a command or webhook name that exists is refused; an edit saves through its PUT

#### compiler

- Two relationships named for one GraphQL field are refused at the schema build, naming both
- A command named for a GraphQL root field another command or a table already is, is refused at the build
- Two sources' tables of one schema and name each lower to their own on the DIRECT route

#### cypher

- A write on a compiled surface is routed as a write, to its source's driver
- A hot API table reads through Cypher and SQL *

#### demo

- No demo's domain in another demo or org: shelter is the standard demo config's, not a seed

#### discovery

- Foreign keys are read by one PostgreSQL query, for every source that speaks its wire protocol
- A bundle-served source's tables are listed by its server as soon as it listens

#### domains

- createDomain refuses an existing id by name, and the add form says so

#### duckdb

- A deleted source's attaches go with it; a replica copy lists no other catalog

#### federation

- A source registered at runtime carries its live-read cap to the attach

#### glossary

- PostgreSQL admits every glossary edge type, so a config's DERIVED_FROM edge no longer stops boot

#### graph

- The explorer draws the first "max nodes" rows and says so over the canvas
- A drawn read is never redirected and is cut at max nodes counted as distinct nodes
- The page draws a node result up to max nodes; the setting is uncapped, 300 by default
- A redraw lays a collapsed group's neighbours around its supernode where it was dragged
- A drop onto a result keeps the result's own ORDER BY, SKIP and LIMIT

#### graphql

- An aggregate field carries the row type's name for its column
- A group-by is ordered by its keys and its aggregates

#### install

- NixOS hosts are told the settings they need, and the Linux download starts on stock NixOS

#### join-guard

- A scalar subquery over a derived table is a value, not a cross join

#### json

- A read of a natively nested column is the backend's own to-JSON expression
- A replica lands a natively nested column as JSON the source or engine produces
- Registration refuses another type for a nested column; GraphQL lists from the attributes; refusals by code
- The hidden attributes in the authoritative DDL and every locale; exact JSON text in the container tests
- gRPC carries a nested column as a JSON string; decimal list elements are strings; binary elements read as the scalar does
- PostgreSQL hands over finite floats, Trino keeps a timestamp's microseconds

#### materialization

- A TTL of 0 is 0 and reads live; a small answer inlined, a large one in a scratch table
- A gRPC remote query's TTL is the first one set, never 300 by itself

#### metrics

- upsertMetric becomes createMetric and updateMetric; a create refuses an existing name

#### model

- A registration stores a numeric's size in one spelling and refuses an impossible one

#### motherduck

- MotherDuck in place on the DuckDB engine; hybrid through the shared connection
- The tour counts 72 source types with MotherDuck in

#### native

- The native launchers run an embedded PostgreSQL control plane

#### oracle

- A CLOB or BLOB is read as its value

#### polly

- Prompt caching on every model call, a usage event per turn, the price table, and the model bake-off
- A Cypher statement is checked before it reaches the Graph page, and Polly sets the page's view controls

#### reads

- The row stream reads each column as its registered type
- Only a result column that is a registered column is typed by the model
- A value that is not its column's registered type is a read refusal with a code
- Flight's buffered answers read each registered column as its registered type
- An Airport scan reads each registered column as its registered type
- An engine's Arrow stream reads each registered column as its registered type
- An undeclared numeric is cast to double by its source, in the governed statement
- A read's shape types each column on its own
- A decimal with no reported size is floating point; its type depends on no row
- A registered table whose columns are not known leaves its columns underived
- A column the statement states is floating point is typed with or without rows
- A published name differing from its physical column only in case is lowered (issue #140)

#### registration

- A native array, struct or map registers as json with its depth, element type and native type
- A type spelling only its platform has registers as its IR name

#### relationships

- A save ends in success or a refusal by name, never a spinner
- upsertRelationship becomes createRelationship and updateRelationship; ids and GraphQL aliases are refused by name when taken

#### roles

- A held role with no data surface is no member of a meta-role
- One rule for a role with no data surface in a meta-role

#### schema

- Every enum CHECK admits the same values in both schemas and the code

#### server

- /setup/status answers when every data thread is busy; a refusal logs what holds them

#### settings

- Polly's price table is edited as a table

#### site

- A live demos page, one card per published named demo
- The Sources page's spelled-out connector counts come from SOURCE_TYPES
- A Live demos link in every page's navigation

#### sources

- A rename onto another source's id is refused by name; onto its own id it changes nothing
- createSource refuses an existing id; updateSource provisions the source on the bound engine again
- A remote schema's refresh hint stays open while the refresh runs

#### sqlserver

- The listing statements bind their schema; a value with no placeholder is refused

#### synthetic

- A fixed pool holds as many distinct values as it declares, spread over its range

#### tables

- A column alias is unique within its table, and so is each column's GraphQL field

#### tags

- upsertTag becomes createTag and updateTag; a create refuses an existing id

#### tour

- A named demo tells its own story first; the weblogs demo's track
- Deep dives are offered only where the org has what they show; a step's opener waits for what it opens
- A read of what the org has that a store reset cut short is read again; a failed one does not leave the tour preparing

#### types

- A numeric column's precision and scale have one stored spelling, one writer, one reader
- A decimal's precision and scale reach the model from what the engine reports
- Oracle's NUMBER is a numeric, sized or not
- Every Oracle type registers as a type every reader knows

#### ui

- A domain new since the last visit stays checked after a reload
- A numeric column's precision and scale are set beside its type
- Ported onto main: the numeric size inputs sit in main's column type tab; a reusable control under the form layout guard
- Browser state is saved per control plane and org
- A statement with its own LIMIT runs as written; the SQL page hides the sample controls for it
- The graph explorer says when a result went to a file, instead of drawing nothing
- A busy flag set before an await is cleared in a finally; refusals are said, not shown as success
- A deployment started as no named demo has no demo track
- The tour starts when it is started twice at once; a registered table opens by the id it was given
- A table edit saves only the table settings that changed
- The GraphQL explorer keeps its introspected schema in memory, not in sessionStorage
- Schema discovery's columns are clickable in the source's detail panel
- Open in SQL runs and shows the handed statement on a page it opens

#### weblogs

- Customers in four sales regions; short unique customer and product names

#### writes

- An INSERT's target takes no alias, so its column list survives

#### General

- A server's shutdown closes every connection it opened, also after its config was re-applied
- The engine-state poll is answered beside the health probes and never queues behind itself
- The swap lane's exasol case -- an edited source keeps its certificate pin, nvarchar is text, a stored sqlite path is resolved where it is read
- A single aggregate root answers in the data envelope; three tests' fakes and seeds catch up
- A checker's result table registers without a source listing; a held role with no data surface adds nothing

## v0.1.0-alpha.481

Released 2026-10-13.

### Features

#### Source Connectors

- Logs is a source type that turns log files into tables.
    - Shared log-parser library, core formats, multi-line records, header-declared columns
    - Logs source -- files by glob and format, read on across rotations, kept in place
    - The Logs source form, its preview, settings strings, and preset families by module
    - A record of several entries is a row per entry, keyed by _entry
    - Files in UTF-16 by their byte-order mark, a hook for binary formats, python-evtx
    - Source counts with Logs and Qdrant in (69), and the form's long hints as help
    - Amazon log presets
    - Microsoft log presets
    - Google log presets
    - Web server and proxy log presets
    - Database and runtime log presets
    - Windows Event Log (.evtx) preset
    - Container and Linux log presets
    - Network security monitor log presets
    - embed(model, text), evaluated by Provisa; a registered model's version
    - main's form of the evtx record end, for alpha.481's merge

### Changes

#### codesource

- The worker sandbox and egress proxy; Node part 4b
- A lockfile given beside a package reference
- Node and Python sources' @table functions are tables; part 5
- A code table with no parameters is read whole, as a replica reads it
- The Linux sandbox allows socketpair(), which a Python host's event loop needs

#### commands

- A command implemented by a Provisa SQL statement
- A statement command's read panel shows its statement
- A statement command's contract, derived at every build; broken; its write in lineage
- Each statement command run leaves its provenance record

#### compiler

- An array parameter takes a list

#### cosmosdb

- A hierarchical partition key has one parameter per level

#### dq

- A table's Quality tab lists the checkers whose contracts scan it

#### graphql

- similar: {column, text | vector} and _similarity on every table with an embedding column

#### grpc

- An array parameter is a repeated field of {Type}Params

#### image

- Canvas 3.2.3, which publishes a linux-arm64 prebuild

#### limits

- A scheduled trigger's run has its own request timeout

#### openapi

- An operation's JSON body declared without a schema is one untyped `body`
- A GET without a response schema is a table whose columns the steward declares

#### org-files

- One data folder shared by every node
- A node releases its mount of an org folder when it shuts down

#### pgwire

- Each connector server keeps its launcher state in its own run directory

#### pipeline

- An INSERT... SELECT reading another source is read, then written to its target

#### python

- A Python source's functions read and served; part P1
- CPython and uv pinned; PyPI indexes are package registries; part P2a
- A Python source's code fetched and its dependencies installed; part P2b

#### qdrant

- Qdrant as a source -- collections and searches
- A text query is embedded through the vector module as it now stands
- A live-only source type, run in Qdrant and written there
- An INSERT's aliased target names its columns; a search table is the engine's without asking Qdrant

#### replicas

- A Google Workspace source's mail replicas are kept in place, as Microsoft 365's are

#### scheduler

- A scheduled trigger calls one registered command, audited as the trigger

#### servicenow

- A ServiceNow instance is a source type, read through its own server
- A ServiceNow table may be bound to one of the instance's import sets
- The instance's own APIs and their OpenAPI documents are read from it
- A companion OpenAPI source's document is assembled from the instance
- The companion OpenAPI source follows its ServiceNow source
- The ServiceNow source page has an APIs section
- The pgwire-servicenow bundle comes from engine-v0.109.7
- The pgwire-servicenow bundle comes from engine-v0.110.0

#### settings

- Each connector's bundled server folder is an operator setting

#### sources

- Upload a file-based source's file, read every location, refuse locked writes
- Read a table's source path with its type, not from the runtime's map
- File paths are written relative to the org's folder

#### synthetic

- A table with no generated rows is refused as that before the API stage

#### ui

- Register Table lets a steward declare an OpenAPI table's columns
- A source's panel is in sections, and lists its tables
- Deployment settings cards fold as their groups do
- One collapsible, one look, each panel opens as it was left
- The code source form's long explanations are help, behind a (?)

#### vector

- cosine_similarity through the one pipeline: governed, bound, lowered per terminal
- A served table's columns carry their embedding declaration
- A replica's HNSW index is the fallback for a vector search
- A PostgreSQL replica holds an embedding as REAL[], indexed through its vector cast
- Every statement that embedded names its models in its audit row, not only a cut one

## v0.1.0-alpha.480

Released 2026-10-12.

### Features

#### MCP & AI Integration

- The external MCP server reaches the graph surface as the UI does. run_cypher runs Cypher under the caller's role through the pipeline every Cypher surface uses and answers its rows and the graph they hold as Cytoscape elements (nodes and edges with the graph page's ids and only the properties the role is served). render_graph draws those elements as the graph page draws them and answers a PNG as MCP image content -- the page's own Cytoscape stylesheet and its force (fcose) or hierarchy (breadthfirst) layout, or the ERD's ELK layout, on the page's light background by default or its dark one when the caller passes theme=dark.

### Changes

#### admin

- A row rule naming no role or an unknown role is refused with a message
- A table save checks the caller's right in the table's current domain

#### api

- No requirement id in what the API package says to a user or to the assistant
- A request sees one build of a role's surface, never the schema of one and the context of another

#### athena

- Amazon Athena is a source type, reached through its own API

#### audit

- The query audit log names each table a statement read, as it was then

#### codesource

- One worker for every code source; Node part 4

#### compiler

- Govern a view's SQL as part of the one semantic statement

#### compiler,rest,jsonapi

- A parameterized table's aggregate and group-by take its parameters; params are keyed by parameter name

#### core

- No requirement id in what the core package says to a user

#### cosmosdb

- Azure Cosmos DB for NoSQL as a source
- A container table follows its change feed
- Register a Cosmos DB source and its tables in the UI

#### cypher

- An embedded object of an unregistered type is a node property; a registered reference stays an edge
- A declared object column is returned as a map on every engine

#### databricks

- Read a Databricks source in place from Unity Catalog, by the steward's choice

#### dq

- Checker scans accumulate in the control plane, read in place

#### drafts

- A draft's record carries the writes its source takes, so its preview builds

#### duckdb

- A sorted page of a Postgres-attached relation holds its rows, not only the NULL-key ones

#### events

- A table that inherits its source's TTL is polled at that TTL (#209)

#### export

- The governed catalog downloads as a dbt sources file

#### federation

- Changes to several landed tables are applied together
- A group of changes removes rows named by part of their key
- A read gated on freshness waits for a first build that is still filling its replica
- DuckDB reads Fabric Warehouse and Synapse SQL in place over TDS, signed in with Entra

#### freshness

- A probe change signal states the cache TTL its probe is asked on (#212)

#### google-workspace

- One read of a mailbox gives every message table
- A mail table's probe asks Gmail whether the mailbox changed
- A source reads the organisation's mailboxes with its service account
- A recipient's position counts from 1, and an attachment always has an id
- The organisation's key is read from its own vault, whoever calls
- The service account's key is a field that keeps what was typed
- A mailbox's approval is read from the state store, not the source
- A label that vanishes is skipped, and a mailbox is judged unreadable only at its profile
- A read waits out Google's throttle as long as the operator set, and says so
- A Gmail mailbox's mail lands a part at a time and is kept in place

#### graphql_remote

- A column's object type and list flag are stored, so a reload names the same references

#### grpc

- Each organisation is served its own gRPC service, as its model stands
- Reflection describes a caller the service of its role
- A role's service is what it may call
- A role's service calls what its Query message lists
- A Timestamp column is answered with the class of the organisation's own Timestamp
- A table's parameters are a {Type}Params message on its request

#### i18n

- A mail platform's refusals are said in the reader's language
- translation-memory entries written under the wrong key are under the key the tool reads
- Translation memory keys of the scan-interval strings name their locale file

#### install

- The Linux AppImage's first launch finds the payload it carries
- A question the AppImage's first launch asks is not part of its answer
- What an install starts names no path outside the install (#201)

#### mail

- Both mail kinds can be checked for changes, so a probing signal on them is taken
- An organisation that only reads its Google mailboxes enters no OAuth client
- A mail platform's client secret is resolved in its own organisation's vault, whoever calls
- The field-named refusal is its own function, beside the resolver the gate already has
- An owner's approval is sign-in state, kept in the organisation's state store
- A replica's read waits out a throttle as long as the operator allows, and says it is waiting
- A mail table is kept by key and stamps each row with when it landed
- A pass that keeps a mail source's replicas in place, a unit at a time
- A Microsoft 365 source's replicas are kept in place, readable while they fill
- A read in units skips a folder removed mid-read, and cached answers are dropped as parts land
- calendars, events and tasks of Google Workspace and Microsoft 365, read and written

#### mcp

- run_cypher answers a Cypher query's graph as Cytoscape elements; render_graph hands back the UI's layout to draw it
- render_graph draws the graph as the UI's graph page does, as a PNG, with Node in the image

#### microsoft-365

- One listing of a mailbox gives every table that comes from its messages
- A source reads the organisation's mailboxes with the organisation's own credential
- A group's members are listed with the header and count Graph requires
- Everyone in the directory is its members, not its guests
- A refused listing says which permissions the client's token carries
- A mailbox is read as delta rounds, and a completed build keeps where it reached
- The check for changes says unchanged only when Graph ends the round with nothing
- A folder the mailbox does not have is absent, and never makes the mailbox unreadable
- One mailbox never has more than Microsoft's four requests in flight from this process
- A mailbox's messages are read as units that land together
- Only a replica's build stamps its rows; a live fetch gives the reader's columns
- The four-request cap counts by the mailbox a reader reads, not by its address
- A source may hold nothing received before a date, as a Google source may
- With a date, reading past old mail costs a line a message, not the message

#### mongodb

- Reach a MongoDB source by a connection string
- Tables registered from an aggregation pipeline
- Query commands that run an aggregation pipeline
- The change-feed listener connects as every other reach does

#### node

- Package registries are an organisation setting with its own admin page
- A Node source's code is fetched pinned, hashed and installed once, its runtime fetched pinned and checked
- A Node bundle's tables, commands and streams are read from its declarations

#### ossie

- The export and import use the document shape Ossie has now (#205)
- An imported field that is a computation is named, not proposed as a column (#205)

#### otel

- A collector that cannot be reached is one line, and one more when it is back
- Traces and logs that cannot be sent are reported as metrics are, by one shared piece

#### pg-engine

- A materialized view on the Postgres engine has its schema

#### pgwire

- The SQL plane offers only the columns a remote GraphQL table's relation holds

#### profiler

- A profile reads only the columns its table's relation holds
- Read a temporal column as its recorded type (#238)

#### release

- Release notes generated from the range between two tags
- A commit reverted before the release is left out of its notes, with its revert
- A revert must name the commit it undoes; the hand-written pair is excluded by name

#### replicas

- Several replicas can be built from one read of their source
- A runner builds the replicas of one read in one job
- A group table is always built by the group read; a failed read removes every build table
- One reader for a group's tables; a table's own reason survives a failed read
- The wait before a failed build is tried again is at least a second
- A probing table's replica is rebuilt when its probe says so, not every TTL (#210)
- A copy its probe found unchanged is current as of the probe (#211)
- A source that could not keep where its read reached says so on the replica
- A server that read its records before another completed the build does not ask for it again

#### replication

- A build says as many things of its copy as it has to say

#### rest,jsonapi

- A table's parameters are read from ?params=, and filter values travel as variables

#### roles

- A model rebuild does not take a meta-role from under a request acting as it

#### schema

- The build's wait is a column of the replica state in the DDL too

#### secrets

- The Kaggle token and the Sheets credentials are fields that hold what was typed
- A secret of an organisation the platform does not hold is refused as not found

#### seed

- A boot view whose columns moved is rebuilt, not refused

#### shutdown

- A second stop signal cannot deadlock the main thread on the deadline expiry (#237)

#### site

- Privacy statement covers license registration, Resend and email sign-in
- The site's source picker shows AskAmerica's bundled logo, and Google Workspace's mark

#### start

- A native demo names no OTel exporter endpoint
- A demo names no MinIO or OTel receiver, and the Docker-demo lines are gone
- The perf bench names its own OTel receiver; a demo start runs no docker compose down

#### state

- An organisation's API sources and endpoints belong to its runtime

#### synthetic

- Read a temporal column as its recorded type in the closeness sample (#238)

#### tables

- A table's Preview and Profile are read as their caller, and a draft can be previewed by those who release it
- The viewer of a draft filters, sorts, groups and pages -- the server writes each statement
- A draft is not refused its preview for how its source is reached
- A draft's preview lands its data the way any read does
- A preview whose copy is still landing says so, in the role's own time
- A draft view is previewed as a view in service is read
- A draft view is previewed as a view in service is read

#### ui

- The Microsoft 365 form's second shape, the mailboxes a source reads for an organisation
- A mail platform's entry has a switch for reading the organisation's mailboxes
- The Microsoft 365 entry says what the administrator grants for organisation mailboxes
- A column's filter shows one filter icon, and its button takes the text box's background
- A column with a filter applied shows a filled filter icon, not a coloured button
- The Microsoft redirect help says where a localhost address works and that organisation sources use none
- The register form's "+ Table" button is as wide as its label
- The register-table form ends in Save and Cancel
- The source form ends in Save and Cancel, when creating as when editing
- The Replicate option's help says what a Hot entry does
- A Google mail source chooses to check for changes before it is re-read
- Replicate's -1 option reads "Live if possible", and its help says each option once
- A source's Cache TTL is sent with the source's own save
- One shared form layout -- fields flow in columns and identifiers get a section of their own
- One form footer -- actions at the start of the bottom line, as wide as their labels
- The source form flows in columns, with the source's ID and type set apart at the top
- The OpenAPI source fields flow in columns
- The SPARQL source fields flow in columns
- The cloud-ops source fields flow in columns
- The Iceberg catalog fields flow in columns
- The Salesforce source fields flow in columns
- The websocket and RSS source fields flow in columns
- The Wikipedia source fields flow in columns
- The data-profiler source fields flow in columns
- The Kaggle source fields flow in columns
- The register-table form flows in columns, with what names the table set apart at the top
- The Google Workspace source fields flow in columns
- The source's load-management fields flow in columns
- The Microsoft 365 source fields flow in columns
- A table that will be a replica states its Cache TTL when it is registered (#208)
- A refused save shows the reader's wording of the refusal, and a source's Cache TTL is tested at the submit (#208)
- A Google Workspace source may read the organisation's mailboxes
- An explanation sits below its field or behind a (?), never between label and control
- The source form reads as rows of aligned fields, and is never cut off
- An action at the end of a field sits on the line of the field's control
- A results grid's column names and filter row stay at the top while its rows scroll
- A credential field shows again what it holds
- A Draft box among a table's actions, and a draft's Preview that works
- A draft's Preview is the full viewer, read through the admin preview
- A group of fields can take a line to itself, and a section or nested layout takes the full row
- The register-table form reads as rows of aligned fields
- The table edit form and its sections follow the form rules
- One help icon in every form, and the required mark right after the label text
- One Save and one Cancel for every form, with a spinner while saving
- The register-table form shows "Columns" only when there are columns
- "Freshness Query Guarantee", and notes out of labels, in the source and register-table forms
- A mail source for the organization keeps Check at the end of "Which mailboxes"
- A native dropdown in a form looks and sits exactly like the others
- The table edit form reads as rows -- fields, then every tick, then the sections
- Live delivery and incremental reload are ticks with the table's other ticks
- A table that is replicating says so, with a ring that shows how far the build has got
- A field's help is its description, not part of its name
- Admin > Email -- a mail platform is entered in rows of sized fields
- Admin > Email -- the mail Provisa sends is set in rows of sized fields
- A mail source's form offers what the organisation's entry can do, and opens on it
- The replicating ring says when the build is waiting out its source's throttle
- A mail replica says it is partial and how many mailboxes have been read
- The incremental-reload tick reads "Incremental reload"
- Admin > Email -- every scope to authorize is readable, and Port is a short field
- The register-table form keeps its five identity fields together
- The columns table of the register-table form no longer repeats its headers in every row
- Admin > Email -- a mail platform asks first what it is for, and shows only those fields
- The top navigation fits a laptop-width window
- Admin > Deployment settings -- every setting is a sized field with its explanation behind a (?)
- The settings cards of Admin > Federation, Domains, Cache and Maintenance are rows of sized fields
- Admin > Federation -- the engine is chosen and configured in rows of sized fields
- Admin > Security > Posture -- the mode is a sized field and the save is the shared Save
- Admin > Security > Authentication -- the provider is configured in rows of sized fields
- Admin > Maintenance -- the banner form is laid out by the shared form layout
- Admin > Cache -- the storage settings of each tab are rows of sized fields
- Admin > Security > Encryption -- the provider is a sized form with the shared Save
- Admin > Org Engine -- an engine of your own is entered in rows of sized fields
- Admin > Org Secrets -- the Org Key panel uses the shared form layout
- Admin > Org Secrets and Your Secrets -- "+ Add secret" is above the list and the dialog ends Save, Cancel
- Admin > Tags -- "+ Add tag" opens a form that ends Save, Cancel
- Admin > Security > Local Users -- "+ Create user" above the list opens a form that ends Save, Cancel
- A form whose first action is not a save keeps that action first, before the shared Cancel
- Admin > Orgs -- the organization and invite forms open from their button and end with their action and Cancel
- Admin > Scheduler -- "+ Add task" opens a form that starts with Identity and ends Save, Cancel
- Admin > Email -- a browser does not fill a mail platform's client or key
- The source form's cache tick reads "Result Cache Allowed"
- The Uniques bar carries the same (?) as every form
- Admin > Observability -- the tracing settings are rows of sized fields with the shared Save and Cancel
- Admin > Export -- the catalog connection is entered in rows of sized fields
- Admin > Import -- the upload line and the mapping rows use the shared form layout
- Admin > Reports -- the add-report dialog starts with Identity and ends with the shared Save and Cancel
- The replicating ring measures a build that lands a mailbox at a time by its mailboxes
- Admin > AI Models -- keys and model assignments are rows of sized fields with the shared Save
- The replicating ring turns until the first mailbox is read to its end
- The Gmail source form says what a search costs once one is typed
- Admin > Billing -- a plan's button is as wide as its words and the change dialog ends Change plan, Cancel
- Admin > Domains -- "+ Add Domain" above the list opens a labelled form that ends Save, Cancel
- Admin > Cache > Materialized Store -- "+ View" is at the start of its row, above the list
- The platform administrator's welcome dialog ends with its action first, at the start of the row
- Admin > Observability > Debug tracing -- both rows use the shared form layout
- The table form's Column Presets row is named, sized fields with Add at its end
- The shared confirmation dialog ends Confirm, Cancel at the start of its row
- The guided-tour prompt ends Start tour, Maybe later at the start of its row
- The add-relationship form is one layout that starts with Identity and ends Save, Cancel
- The junction-table panel of a relationship is two rows of sized fields
- The data product form's panels hold sized fields and the form ends with the shared Save and Cancel
- A data product's custom properties are rows of key, value and Remove
- Products -- "+ Data Product" hides while its form is open and the delete dialog ends Delete, Cancel
- The command form's fields flow in rows of sized boxes
- Commands -- "+ Command" opens a form that starts with Identity and ends Save, Cancel
- Security -- the role and row-rule forms open from their button and end Save, Cancel
- The guided tour closes the new-role form with the form's Cancel
- Admin asks before it deletes a domain, an organization role or a scheduled task
- A list with a resizable detail panel on its right
- Tables -- the selected table opens in a detail panel on the right
- A role, an RLS rule and a relationship are deleted only on confirm
- The column editor shows a column's attributes on three tabs, with its full definition on the name
- Polly opens narrower, and its resize is one shared mechanism
- Tables -- the detail panel has a pinned action bar, expands, holds Preview, and gives way to nothing but itself
- The detail panel opens at 60% of its row, not of the window
- A source read only on schedule is a Replicate choice, not a tick beside it
- Admin > Email -- Microsoft's "own mailbox" tick is on and fixed while the organization's is on
- Admin > Deployment settings -- a tick that needs a restart says so beside it
- Admin > Federation -- an engine's secret is typed into a masked box
- Tables -- a table's detail shows its columns on the same three tabs as the editor
- Tables -- a create form in the panel is titled for what it is
- A table read only on schedule is a Replicate choice; one off-peak window control
- Admin > Email -- Microsoft's "own mailbox" tick is on and fixed while the organization's is on
- Admin > Deployment settings -- a tick that needs a restart says so beside it
- Admin > Federation -- an engine's secret is typed into a masked box
- Admin > Environments -- the Synthetic data tab offers the environments that exist
- The lineage page and the sandbox sign-in, notice and sign-out are translated
- Relationships -- a relationship opens in the right-hand panel
- The off-peak window's label is a field label like its neighbours'
- Tables -- the new-model form's Dimensions label matches its other labels
- The Sources page shows a source's detail in a panel at the right of the list
- A form open in a detail panel has its Save and Cancel in the panel's head
- Security -- a role or an RLS rule opens in the right-hand panel
- Metrics -- a metric opens in the right-hand panel
- The Commands page shows a command's detail in a panel at the right of its lists
- Relationships -- the list keeps its columns beside the panel
- A new view is created by a short step and opens in edit; its definition is edited in the panel
- "+" asks only for what a relationship, a role or a metric needs to exist
- The profile dialog is on the shared form layout and no longer offers an email preference that nothing honours
- The Maintenance and Billing tabs and the command form's dataset columns are translated
- Billing -- the trial's and data transfer's prices are written the same in every language
- Polly's panel is translated
- Commands is a full-height page, a new command is asked its domain, and the profile's Save stands clear of the table below it
- One tooltip -- the shared Tip, and HelpBubble rebuilt on its surface
- Every form field's (?) help is drawn by the one tooltip
- A table's column list has one tab bar of four, with Testing the fourth
- The Tables page's two confirmations put their action first, and its preview is one component
- The Schema Explorer's graph takes the app's fonts and colours
- A form's Save in a panel's head can name the form it submits
- Relationships, Security, Metrics -- Save and Cancel in the panel head
- Admin -- every tooltip on the admin tabs is the one tooltip
- The shared pieces, the nav bar, Polly and data products use the one tooltip
- The explorers use the one tooltip
- Sources -- the detail panel, the type picker and schema discovery use the one tooltip
- The explorers' last native hover titles use the one tooltip
- A tip inside a menu's or popover's trigger lets the trigger through; four more files converted
- A control's own click runs before its trigger's; the four hinted triggers are click-verified and guarded
- Every list fills the width it has, and three follow-ups
- Relationships and Metrics -- the detail panels' hints use the one tooltip
- Leaving a form in a detail panel with unsaved changes asks first
- Relationships -- a refused save keeps the form open and says why
- Security > RLS -- Save on a rule that lacks a field names what is missing
- A list's columns are capped and give up width before the list passes the window
- A detail panel holds no discard dialog until a leave is attempted
- Text drawn beside a badge or above a dimmed line is held like a cell of text
- A list's text with a space in it wraps at its spaces, however short
- A detail panel's head actions are borderless, one style, after a divider
- Local users -- a long username is cut on its expand button and shown whole in the hint
- A table's detail is six section tabs, the same in its read view and its edit form
- A table's Quality tab shows the Data Profiler, its contract and its uniques open
- The MongoDB host explanation is the field's help
- "+ Table" is a short step: what names the table, then its editor
- Sources -- Refresh Schema is an icon in the panel's head, beside the other actions
- A view's Load tab shows Refresh and Snapshot Schedule open, with no toggle
- Relationships -- the panel shows every value of a relationship, and a junction edge's Cypher type
- Synthetic data with no environment to hold a dataset offers "Create environment"
- Invalidate Cache is a table's head action in its detail panel, not a list column
- Relationships -- the domain filter reads the source and target domains, not an owner nothing sets
- The Sources form's Load Management panel opens flat, with no toggle
- Security › Roles -- the panel shows every value of a role, on the grid every panel uses
- The Commands edit form's lists are laid out as the shared form lays out a list
- Security › RLS Rules -- the panel shows every value of a rule; both Security heads name their actions in the shared hint
- runSql says a result landed in the results store, and fails loud on an answer with no columns
- Metrics -- the panel shows every value of a metric, under its form's labels, on the shared grid
- Commands -- a function's and a webhook's panels show every value, under the form's labels
- Glossary -- a term's panel shows its domains and its states beside what it edits
- Data Products -- a product's detail shows every value of it, its ID among them, under the form's labels
- "What's new" in the user menu shows the running release's notes
- Tables -- every hint on the page is the shared Tip; none is the library's or the browser's
- Amazon Athena in the source picker, with its form
- The form's column metadata type carries a column's path
- A table's Load and timeliness tab reads every setting it stores
- The source picker shows AskAmerica's own logo, bundled, in place of its lettered tile
- Preview, Profile and Policies are icon actions in a table's panel head
- A table's Query tab reads every setting it stores
- A table's Quality tab reads every setting it stores
- A files source's table reads its glob and source-file column on Identity
- Tour counts 66 source types; drop the guard exemption for a label the form no longer has
- A source's read view is its edit form, read
- The JSON:API Explorer takes a table's parameters
- Read views keep their values near their labels
- The JSON:API Explorer lists a table's parameters only under Parameters
- Deployment settings panels fold by their headers
- A grouped column is ordered the way it is sorted (#248)
- The read-policy explanation is the field's hint, per the form rules
- + Source opens the type picker first

#### views

- A view's inputs are in service for its readers
- A draft view keeps its build, and its steward's preview asks for it
- A view over a table its reader may read nothing of is refused naming it
- A materialized draft view's preview builds it on the Postgres engine
- A view stored by an engine with no catalog in its SQL is named without one
- A bitemporal view is read from its log on every read, its definition governed for the reader (#246)

#### General

- A registered parameter records whether its source requires it; Preview and Profile ask first (#204)
- An unrecorded parameter of a remote GraphQL table is required (#204)
- A mail source states its Cache TTL, and a refused table says where it is set (#208)
- A sentinel path is offered only where a source can have one
- A probing change signal on a mail source is refused until its check is wired
- No requirement id in what a user reads -- the catalogs and the direct refusals
- No requirement id in what the remaining packages say to a user; the guard has no list left
- A credential field keeps what was typed -- the product names no secret and writes no vault
- Table parameters, required by the source or a steward
- Profile and preview read as the roles the user acts as; a refusal is a 403

## v0.1.0-alpha.479

Released 2026-10-09.

### Features

#### Read Refusals

- A read that needs a table whose replica could not be built is refused, naming the table and the error the build recorded: HTTP 503 with code query.replica_build_failed and the table and reason as parameters, gRPC UNAVAILABLE, and the same message on the transports that carry only a message.

#### Role Assignments

- A multi-tenant deployment reads role assignments from Provisa, never from sign-in claims: a configuration that sets multitenancy with auth.assignments_source claims, stated or by default, does not load, the deployment does not start on it, and a settings change that would produce it is refused, each naming both settings and the value to use.

### Changes

#### api

- A float subclass in a response body is encoded as its number

#### api-source

- A throttled call waits as long as the remote asks
- An API source may sign in with a refresh token or as a Google service account
- A refresh-token exchange returns the issuer's replacement, and a client may state no secret
- An access token for a source whose refresh token the issuer replaces on use
- A sign-in on a deployment without identities stores its token with no actor

#### auth

- A multi-tenant deployment that reads assignments from sign-in claims does not start

#### catalog

- The catalog types a column by the product's own vocabulary; a nested column is JSON
- The role's catalog is served over HTTP, and the JDBC driver reads it there

#### catalog-cache

- One rule for every source kind -- column names are loaded lazily
- A source registered from a specification is indexed with its fields

#### config

- The default public address is where a local install's UI is served
- An empty public address is not stated, and the shipped configs default to the served one

#### demo

- The demo model lets the org's admin write a prospective adopter

#### deps

- strawberry-graphql 0.327.2 clears CVE-2026-107727 and CVE-2026-107728; feature file regenerated

#### google-workspace

- The settings a Google Workspace source is set up with
- Reading a mailbox through the Gmail API, and a message into the canonical facts
- A Google Workspace source is added from the picker and its mail tables registered
- A source is connected with the organisation's Google client, and its form asks nothing about sign-in

#### governance

- A column's scope is changed only with the right to change who is served it (#194)
- Only widening a column's scope needs the right to grant it (#194)

#### mail-platforms

- An organisation enters a mail platform's client once
- The vault keeps a client secret its platform entry names

#### microsoft-365

- A mailbox read through Microsoft Graph as the six canonical mail tables
- A message whose HTML body Graph will not give keeps its row
- Microsoft 365 is a source type with its six mail tables and its loader
- A Microsoft 365 source is connected with the organisation's client and read

#### openapi

- A success answer declared as 2XX gives the table its columns

#### reads

- A table whose replica cannot be built refuses the read by name, not as a server fault
- The Neo4j endpoint and the table profile answer a refused read as the other surfaces do

#### replication

- A failed replica build is retried with a growing wait, and never given up
- A build carries what its source says of the rows it read
- A completed build's note is recorded on the replica and shown beside its state

#### secrets

- Personal secrets are addressed only within an organisation the caller belongs to
- A central secrets service is read inside the organisation's own prefix
- A refused secret name is answered 400 with its code; the API source auth writer checks it

#### settings

- A retry ceiling below the interval is refused where settings are loaded too

#### source-sign-in

- A source is signed in to its issuer by a person's approval
- An issuer's addresses may depend on the source, and the refresh token is written under the refresh lock

#### sources

- The canonical mail, calendar and task tables as one module with its enum translator

#### sql

- /data/sql takes a statement's values apart from its text, and binds them
- A statement sent without values is governed by the call it always was

#### sql-explorer

- A column of a result that reports no types is typed from its values
- "filters apply to the rows loaded" follows what the server said, not the editor's text
- A value checklist over part of a result says so and takes a value typed by hand

#### ui

- The table editor opens for a registrar who is not shown governance
- The Google Workspace setup, and the page a source sign-in returns to
- The table viewer binds every value and quotes every identifier through one function (#198)
- The source picker finds Google Workspace by gmail, mail and email, and its tile names Gmail
- The Google Workspace setup shows only what it reads
- Admin › Email holds the organisation's mail platforms beside the mail Provisa sends
- Admin › Email names Microsoft 365 and labels its tenant setting

## History

### Phase A: Infrastructure
- Docker Compose setup (PostgreSQL + Trino), dev and prod overlay
- Test infrastructure: conftest.py, fixture factories, sample config
- Dockerfile for stateless Provisa container
- Trino coordinator config, PG connector, demo seed data

### Phase B: Config & Auto-Catalog
- YAML config loader with idempotent PG upsert
- Trino dynamic catalog API (auto-create/delete catalogs on source add/remove)
- Pluggable secrets provider (`${env:VAR}` syntax, extensible to Vault/K8s)
- Pydantic models: Source, Domain, Table, Column, Relationship, Role, RLSRule
- CRUD repositories for all config entities

### Phase C: Schema Generation
- GraphQL SDL generation from registration model + Trino INFORMATION_SCHEMA
- Per-role schema: domain-scoped visibility, column filtering
- Relationship fields (many-to-one as object, one-to-many as list)
- Root query fields with `where`, `order_by`, `limit`, `offset` arguments
- Trino type to GraphQL scalar mapping
- Auto-generated shortest unique names, regex rules, alias override

### Phase D: Query Compilation & Trino Execution
- GraphQL to PG-style SQL compiler (single SQL statement, no N+1)
- SQLGlot transpilation (PG SQL to Trino SQL)
- Nested relationship compilation via LEFT JOIN / subquery
- Parameterized queries (never interpolates values)
- JSON serializer: reconstruct nested GraphQL response from JOIN rows
- FastAPI app factory, `/data/graphql` and `/health` endpoints

### Phase E: Routing & Direct Execution
- Smart routing: single RDBMS source direct, multi-source via Trino
- SQLGlot transpilation to target dialects (PostgreSQL, MySQL, SQL Server, DuckDB, Snowflake, BigQuery)
- Connection pool per registered RDBMS source (configurable min/max)
- Steward route override hint

### Phase F: Security Layers
- RLS WHERE clause injection per role per table
- Schema visibility enforcement (unauthorized tables/columns excluded from SDL)
- Rights model with 8 independent capabilities
- Column stripping for unauthorized columns

### Phase G: Mutations
- INSERT/UPDATE/DELETE via GraphQL (`insert_<table>`, `update_<table>`, `delete_<table>`)
- Mutations always route direct to RDBMS (never Trino)
- RLS injected into UPDATE/DELETE WHERE clauses
- Write permission enforcement per column per role
- NoSQL and cross-source mutations rejected

### Phase H: Persisted Query Registry & Pre-Approval
- Query submission with developer metadata and business purpose
- Stable identifier on approval
- Approval ceiling enforcement (restrict within, reject exceed)
- Governance modes: registry-required vs pre-approved tables
- Deprecation with replacement pointer
- Registration changes flag affected entries for re-review

### Phase I: Output Formats & Arrow Flight
- Content negotiation: JSON, NDJSON, CSV, Parquet, Arrow IPC
- Arrow Flight gRPC endpoint (port 8815) with streaming record batches
- Flight SQL connector modes: `catalog` (metadata discovery), `approved` (persisted queries), default (full execution)
- Zaychik Flight SQL proxy integration for end-to-end Arrow streaming

### Phase J: Large Result Redirect
- S3-compatible blob storage redirect with presigned URLs
- Threshold-based conditional redirect
- Trino CTAS for Parquet/ORC (data never passes through Provisa)
- Multi-root query support (some fields inline, some redirected)
- Pre-approved tables cannot use redirect

### Phase K: Admin API (Strawberry)
- Strawberry GraphQL endpoint at `/admin/graphql`
- CRUD for sources, tables, relationships, roles, RLS rules
- Schema regeneration triggered on config changes
- Config download/upload (`GET/PUT /admin/config`)

### Phase L: Connection Pooling Hardening
- PgBouncer for PostgreSQL sources
- Driver-level pooling for non-PG RDBMS
- Per-source pool sizing configuration

### Phase M: Production Infrastructure
- Helm chart for Kubernetes deployment
- Trino StatefulSet with configurable workers, HPA autoscaling

### Phase N: UI
- React app (Vite + TypeScript) with role-driven rendering
- Capability-driven views: Sources, Tables, Relationships, Security, Query Development, Approval
- GraphiQL query editor with Provisa plugin (submit, compile, redirect controls)
- GraphQL Voyager schema explorer (iframe with CDN bundle)
- Confirmation dialogs for destructive actions
- Approval queue with rejection reasons

### Phase O: Query Result Caching
- Redis-backed query result cache
- Security-partitioned cache keys (role + RLS context)
- Cache invalidation on registration changes and mutations
- `X-Provisa-Cache: HIT|MISS` response headers with age

### Phase P: Materialized View Optimization
- Transparent SQL-level MV rewrite (invisible in GraphQL SDL)
- Join pattern matching with partial match support
- Scheduled background refresh via Trino CTAS
- Stale MV bypass (no silent stale data)
- Optional SDL exposure for MVs with computed semantics
- RLS applied on MV-backed queries

### Phase Q: Column-Level Masking
- Per-column per-role data masking at SQL level
- Mask types: regex (REGEXP_REPLACE), constant (literal/NULL/MAX/MIN), truncate (DATE_TRUNC)
- Type validation (regex only on strings, NULL only on nullable)
- Masking injected into SELECT projection; WHERE uses raw values

### Phase R: LLM Relationship Discovery
- Claude API-powered FK candidate suggestion
- Scope control: per-table, per-domain, cross-domain
- Candidate lifecycle: suggested, accepted, rejected, expired
- Sample data validation for high-confidence candidates
- Admin endpoints for discovery trigger, review, accept/reject

### Phase S: gRPC Query Endpoint
- Auto-generated `.proto` schemas from registration model (per role)
- gRPC server on port 50051 with server reflection
- Streaming queries (one message per row), unary mutations
- Full security pipeline (RLS, masking, sampling, governance)
- Proto files served at `GET /data/proto/{role_id}`

### Phase T: Documentation
- README with quick start, feature overview, supported sources
- Architecture guide with request pipeline and routing decision tree
- Configuration reference (all YAML sections)
- API reference (all HTTP, Flight, gRPC endpoints)
- Security model deep dive
- Source type reference

### Phase U: API Sources (REST, GraphQL, gRPC)
- Register API endpoints as data sources with auto-discovery
- OpenAPI spec parsing, GraphQL introspection, gRPC reflection
- PG cache with TTL (endpoint > source > global)
- Primitive columns native, complex objects as JSONB
- JSONB field promotion via PG generated columns
- Stale-while-revalidate caching strategy

### Phase V: Kafka Sources & Sink
- Kafka topics as read-only tables via Trino Kafka connector
- Schema Registry integration (Avro, Protobuf, JSON Schema)
- Discriminator-based topic splitting (multiple types per physical topic)
- Default time window auto-injection (`default_window`)
- Query result publishing to Kafka topics (sink)

### Phase W: Authentication
- Pluggable auth providers: Firebase, Keycloak, OAuth 2.0/OIDC, simple (username/password)
- Superuser bootstrap access (works with any provider)
- JWT validation with JWKS rotation support
- Role mapping from identity claims to Provisa roles
- Login page with provider-specific UI

### Phase X: JDBC Arrow Flight Transport
- JDBC driver wired to Arrow Flight (`grpc://host:8815`) for streaming results
- Auto-detection with silent fallback to HTTP
- Batch-by-batch streaming (memory bounded to one record batch)
- Both `approved` and `catalog` modes supported

### Phase Y: Admin Page Global Features
- MV manager: list, enable/disable, manual refresh, error display
- Global cache controls: stats, purge all, purge by table
- System health panel: Trino, PG pool, Redis status
- Scheduled task overview with enable/disable toggles

### Phase Z: Per-Source & Per-Table Cache Configuration
- Hierarchical TTL resolution: table > source > global default
- `cache_enabled` toggle per source (disables all child tables)
- UI display of effective (resolved) TTL with inheritance chain
- Admin mutations: `updateSourceCache`, `updateTableCache`

### Phase AA: Quick Wins (AD1-AD2 only)
- **AD1 - Naming Convention**: `none`, `snake_case`, `camelCase`, `PascalCase` at global, source, and table level; explicit alias always wins
- **AD2 - OrderBy Alignment**: `{column: direction}` format with 6-value enum (`asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`); relationship ordering via JOIN
- Upsert mutations (`upsert_<table>`) via `INSERT ... ON CONFLICT DO UPDATE`
- `distinct_on` argument for root query fields
- Column presets: auto-inject values from headers, `now()`, or literals on insert/update
- Inherited roles with `parent_role_id` and capability flattening
- Scheduled triggers with cron expressions (APScheduler)
- Direct-route dialect expansion (ClickHouse, MariaDB, SingleStore, Redshift, Databricks, Hive, Druid, Exasol)
