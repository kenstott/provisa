# Provisa
## Design Specification

---

## Name and Origin

Provisa derives from the Latin "provisus," the past participle of "providere": to foresee, to provide for in advance, to govern with forethought. The root combines "pro" (before, forward) and "videre" (to see). In Roman legal tradition, a "provisum" was a formal advance provision, a decree established beforehand that governed future action. Not a permission granted at the moment of request, but a standing authorization determined in advance by competent authority.

The name describes the product's defining security philosophy precisely. In Provisa, production queries are not evaluated against permissions at request time. They are pre-approved or they do not run. The production endpoint does not evaluate requests. It executes operations that were already seen, already provided for, already authorized before they could ever reach production. That is provisus. That is Provisa.

---

## The Pre-Approval Model

The most distinctive property of Provisa is not its execution engine or its GraphQL interface. It is the pre-approval model that governs every query in production.

In conventional API and data access systems, security is evaluated at request time. A user submits a query, the system checks permissions, and the query executes or is denied. The attack surface is the full query language available to every authenticated user. Security is a runtime filter applied to an unbounded input space.

Provisa inverts this model completely. Queries are defined in a development environment against a typed schema. They pass through a formal authorization gate before they can exist in production. The production endpoint does not accept arbitrary queries. It accepts only read operations that are members of the persisted query registry, pre-approved by an authorized data steward. If a query is not in the registry, it does not execute. Mutations are governed separately through user rights and do not require registry approval. There is no runtime evaluation, no permission check, no policy decision. The production query surface is finite, enumerable, and fully auditable before a single production request is made.

This has three consequences that conventional systems cannot achieve.

The attack surface in production is bounded and known. An auditor can enumerate every operation the production system will ever execute. Security review is a review of the registry, not a review of runtime behavior.

Authorization failures happen at development time, not production time. A query that would not be approved is rejected at the authorization gate with a clear explanation. It never reaches production. There are no production security incidents caused by queries that should not have been permitted.

The production environment is operationally simpler because it makes fewer decisions. It receives a known operation identifier, looks it up in the registry, and executes the pre-compiled, pre-authorized, pre-validated operation. The complexity lives in the development and authorization workflow, where it is manageable, not in the production path, where it is dangerous.

The pre-approval model is a platform-level property, not a user rights feature. Even a superuser with maximum privileges cannot execute an unregistered query in production. The restriction is not about who you are. It is about what the query is. User rights govern what data flows through an approved operation for a given user. Query approval governs which operations exist at all. The two layers are orthogonal and neither substitutes for the other. A system with strong user rights but no query approval has a large attack surface bounded only by the query language. A system with query approval but no user rights has a bounded attack surface but no data-level isolation between users. Provisa enforces both independently at different layers for different threat models.

---

## Three-Phase Workflow

The pre-approval model organizes the product into three distinct phases with one-way gates between them.

In the development phase, a developer opens the GraphQL query builder against the test endpoint. The test endpoint presents the governed schema surface: only registered tables, only authorized columns, scoped to the developer's domain access. The developer defines a query using standard GraphQL syntax against the typed schema. The test endpoint executes the query with full guards: RLS injection, column filtering, schema scoping. The developer sees exactly what production will return, against a controlled dataset, before authorization is requested.

In the authorization phase, the developer submits the query to the persisted query registry for approval. A data steward with appropriate rights reviews the query definition, its scope, its target tables, and its parameter bindings. The steward approves or rejects with a documented reason. Approved queries enter the registry with a stable identifier. Rejected queries return to the developer with clear feedback. No query moves to production without passing this gate.

In the production phase, clients submit the pre-approved query identifier and its parameter bindings. The production endpoint looks up the identifier in the registry, confirms the binding types match the registered parameter schema, applies RLS and column security at the executor layer, and executes. The query language is not evaluated in production. The query was already compiled, validated, and authorized. Production executes what development defined and authorization approved.

---

## Product Positioning

Provisa is an open source, enterprise-class replacement for Hasura v2, built on a purpose-built GraphQL compiler with Trino as the federated execution backend. It preserves the Hasura v2 developer experience that the displaced community valued: register sources, register tables, develop queries in a GraphQL builder, deploy as a single container.

Hasura v2 was a capable product. It had genuine cross-source federation with predicate pushdown through its Remote Join implementation. It handled single-source workloads with well-optimized SQL pushdown. For moderate workloads and typical enterprise deployments it performed well and was widely trusted. The limitations that displaced its community were not primarily technical. They were the DDN transition, which traded the simplicity that made v2 compelling for an enterprise architecture that required adopting a new mental model, a new configuration format, and effectively a new product.

Where v2 hit genuine technical limits was at the intersection of federation and scale. Its cross-source execution model lived in a single process. Very large result sets, memory-bounded workloads, highly concurrent analytical queries, and workloads requiring cost-based join strategy selection across heterogeneous sources at enterprise data volumes exceeded what a single-process execution model could reliably handle. There was no distributed execution across workers, no spill to disk under memory pressure, no workload isolation between concurrent heavy queries.

Trino replaces that single-process ceiling with distributed execution. Horizontal scaling across workers, memory management through spill, cost-based join strategy selection, workload isolation through resource groups, and concurrent query management at scale. The federation capability is familiar to v2 users. The scale ceiling is removed.

Provisa's additions over v2 are the governed query registry for analytical and sensitive table access, the large result redirect with multiple output types and gRPC Arrow Flight for bulk provisioning, and the pre-approval security model that v2 never provided.

It is not Hasura DDN. DDN traded simplicity for power. Provisa adds power without removing simplicity, and adds a security model that neither v2 nor DDN provided.

Provisa is strictly a governed data provisioning and query layer. It has no multi-step reasoning engine, no NLQ pipeline, no domain DAG, no glossary or NER infrastructure. Those capabilities belong to separate products. This product's governance scope is exactly what the provisioning use case requires: source registration rights, table and view registration with ownership, relationship definition, column and row level security, and the pre-approval model governing production queries that warrant explicit steward review.

---

## Architecture Overview

The product composes four layers. The registration and governance layer handles source onboarding, table registration, relationship definition, security configuration, and the persisted query authorization workflow through a UI with appropriate access controls. The compiler layer is a purpose-built GraphQL compiler that generates a GraphQL SDL from the registration model using Trino's unified INFORMATION_SCHEMA, and compiles GraphQL queries to PG-style SQL at execution time. The transpilation and routing layer uses SQLGlot to translate PG-style SQL to the appropriate target dialect and route operations to the correct execution backend. The execution layer uses Trino for federated cross-source read queries and direct RDBMS connections for single-source reads and mutations.

Each layer has a single clearly defined responsibility. No layer does work that belongs to another. There are no third-party dependencies at the core compilation layer. The compiler is purpose-built for the actual use case.

---

## Component Stack

**The Provisa GraphQL Compiler** is a purpose-built two-pass compiler owned entirely by the product. The schema generation pass runs at registration time and produces a GraphQL SDL from the registration model. The query compilation pass runs at execution time and produces PG-style SQL from a validated GraphQL operation. The compiler has no dependency on PostGraphile, DuckDB, or any third-party GraphQL server framework. It uses graphql-js or equivalent for GraphQL AST parsing and validation, and produces SQL against the registration model's type and relationship graph.

**Trino** is both the schema introspection surface and the federated read execution engine. At registration time, Trino's unified INFORMATION_SCHEMA provides the column names, types, nullability, and intra-source constraint metadata for every registered source regardless of the underlying database dialect. Trino normalizes the type system across sources, eliminating per-source introspection handling. At execution time, Trino handles cross-source joins, distributed execution across worker nodes, predicate pushdown to source systems, join strategy selection, and memory management for large result sets. Trino supports Arrow Flight natively, enabling direct columnar output without intermediate format conversion for high-performance consumers. Users never interact with Trino directly.

**SQLGlot** handles all SQL dialect transpilation. The compiler emits PG-style SQL as its canonical output. SQLGlot translates that output to Trino SQL for the cross-source read path and to the target RDBMS dialect for single-source reads and mutations. The source type captured at table registration time determines the target dialect. SQLGlot is a well-maintained open source Python library with broad dialect coverage including PostgreSQL, MySQL, SQL Server, Trino, DuckDB, Snowflake, and BigQuery.

**Direct RDBMS connections** handle single-source reads and all mutation execution. Single-source queries and mutations bypass Trino entirely and execute against the registered source database using connection details captured at table registration time. Connection pooling maintains warm connections per registered source to minimize latency.

---

## Source Registration

Source registration is a privileged operation requiring appropriate rights. The registration UI accepts source type, selected from the pre-loaded connector set in the Trino Docker image, and connection details. On submission, the UI validates the connection, calls the Trino dynamic catalog API to register the catalog without restart, and makes the source immediately available for table registration. No restart is required. The new source appears in the catalog browser within seconds of confirmation.

Source registration does not expose any data. It makes a source available for governed onboarding through the table registration workflow. No table in a registered source is queryable until a steward explicitly registers it.

---

## Table Registration

Table registration is the primary governance act. It is the explicit decision by an authorized steward that a specific table or view in a registered source is part of the governed query surface. Unregistered tables do not exist from the product's perspective. They cannot be referenced in queries, they do not appear in the schema browser, and they cannot be the target of mutations.

When a steward registers a table, the registration UI queries Trino's INFORMATION_SCHEMA for the table's column names, types, nullability, and available constraint metadata. The steward reviews the column set and sets column visibility per role: which columns are exposed to which roles. Columns excluded from all roles do not appear in the generated GraphQL schema. The registration captures the Trino fully-qualified catalog.schema.table name, the per-role column visibility rules, the source type and direct connection details for single-source routing, and the steward's identity and timestamp.

At registration time, the steward sets the table's query governance mode. A table marked as pre-approved for direct query allows users with appropriate read rights to query it in production without registry approval, identical to how mutations are governed. The user's column visibility rules and RLS rules are the complete governance layer for queries against pre-approved tables. This mode is appropriate for operational tables serving UI data needs where the governed schema surface is sufficient authorization for read access. A table not marked as pre-approved requires all production queries against it to be members of the persisted query registry. This mode is appropriate for sensitive or analytical tables where steward review of query logic, join patterns, and output types is warranted.

Publication of the table registration triggers the schema generation pass for the affected schema surface. The GraphQL SDL is regenerated to reflect the newly registered table. The table becomes immediately available in the query builder for users with Query Development capability.

NoSQL sources are handled through automatic Parquet materialization. When a NoSQL source table is registered, the product extracts the data to Parquet and attaches it to the Trino catalog. From the registration model's perspective NoSQL tables are identical to RDBMS tables. NoSQL sources are read only. Mutations against NoSQL sources are not supported.

---

## Relationship Registration

Relationship registration is the act of encoding navigable connections between registered tables into the registration model. Relationships are what make the GraphQL schema a graph rather than a flat collection of disconnected types. Without registered relationships, queries can only address individual tables. With registered relationships, queries can traverse from one type to another through JOIN operations compiled from the relationship definition.

Trino's INFORMATION_SCHEMA provides foreign key constraint metadata for sources that expose it. The compiler uses this metadata to infer candidate intra-source relationships automatically and presents them to the steward for confirmation or rejection. Confirmed inferred relationships enter the registration model without manual definition. Rejected inferred relationships are excluded even if the technical constraint exists, giving the steward control over what the schema surface exposes semantically rather than just structurally.

Cross-source relationships have no technical artifact to infer from. They exist only in the steward's knowledge of the business domain. The steward defines them explicitly in the relationship builder: select a source table, select a join field, select a target table, select a join field, define cardinality (one-to-one, many-to-one, one-to-many). The relationship is stored in the registration model and used by the compiler to generate the corresponding GraphQL relationship fields and JOIN clauses.

This means the GraphQL schema reflects business intent, not database implementation. A steward who defines relationships is making a deliberate semantic decision about what the governed schema surface means. Two tables connected by a foreign key in the database may not have a registered relationship in Provisa if the steward determines that relationship should not be navigable through the query surface. The registration model is the source of truth, not the database schema.

Relationships carry the same governance properties as tables. They are owned by the steward who defined them, they are versioned in the registration model, and schema changes that affect a relationship's join fields flag the relationship for re-review.

---

## Compiler Architecture

The Provisa compiler is a purpose-built two-pass pipeline. It has no dependency on third-party GraphQL server frameworks. It owns the full compilation surface from GraphQL SDL generation through PG-style SQL output.

The schema generation pass runs at registration time and whenever the registration model changes. It queries Trino's INFORMATION_SCHEMA for all registered tables, applies the per-role column visibility rules from the registration model, incorporates the registered relationship graph, and produces a GraphQL SDL that defines the complete typed schema surface. Each registered table becomes a GraphQL object type. Column names become field names. Trino's normalized type system maps to GraphQL scalar types: VARCHAR to String, INTEGER to Int, BOOLEAN to Boolean, TIMESTAMP to a DateTime scalar, JSONB to a JSON scalar, with nullability preserved from the column registration. Registered relationships become GraphQL relationship fields with cardinality reflected in the field type: one-to-many produces a list field, many-to-one produces a single object field. Each registered table generates root query fields with filter arguments derived from the column types and pagination arguments. Mutation types are generated for RDBMS-sourced tables: insert with a typed input object, update with a primary key argument and a patch input object, delete with a primary key argument. NoSQL-sourced tables generate query fields only.

The query compilation pass runs at execution time for the test endpoint and at registry submission time for production queries. It accepts a GraphQL operation string and the current SDL, parses it to a validated AST using graphql-js, and walks the selection set to produce PG-style SQL. Field selections map to column projections. Arguments map to WHERE clauses, ORDER BY, LIMIT, and OFFSET. Nested relationship fields map to JOIN clauses resolved against the relationship registry: the compiler looks up the join keys and cardinality for each traversed relationship and emits the corresponding SQL JOIN. Fragment spreads expand to their field sets inline. The entire operation compiles to a single SQL statement with no resolver chain and no N+1 concern. The compiled SQL is PG-style regardless of the target execution backend. SQLGlot handles dialect translation downstream.

Mutation compilation follows the same pattern. INSERT mutations compile from the input object fields to a typed INSERT statement. UPDATE mutations compile to an UPDATE with a WHERE clause on the primary key and SET clauses from the patch object. DELETE mutations compile to a DELETE with a WHERE clause on the primary key.

The compiler validates every operation against the registration model before producing SQL. References to unregistered tables, excluded columns, undefined relationships, or type mismatches in arguments are rejected at compile time with precise error messages. For production registry submissions, compilation failure means the query cannot be submitted for approval. The approval workflow only receives queries that compile correctly against the current schema.

---

## Persisted Query Registry

The persisted query registry is the authorization gate between development and production. It is the operational embodiment of the pre-approval model.

A query enters the registry through a formal submission and approval workflow. The developer submits a compiled query definition from the test endpoint. The submission captures the full query text, the compiled SQL, the target tables, the parameter schema including names, types, and nullability, the permitted output types, and the requesting developer's identity. The submission enters a review queue visible to data stewards with authorization rights over the relevant tables.

The reviewing steward examines the query definition, confirms it is scoped to registered tables within their authorization, confirms the parameter schema is correctly typed and bounded, confirms which output types are appropriate for the query's intended use, and approves or rejects with a documented reason. Approved queries receive a stable identifier and enter the active registry. The identifier is the handle clients use in production. The query text is never transmitted in production requests. Mutations are not submitted to the registry. They are governed entirely by user rights and execute directly against the registered source. Queries against pre-approved tables are not submitted to the registry. They execute directly based on user rights, identical to mutations. The registry is reserved for queries that warrant explicit steward review: analytical queries, cross-source queries, large result provisioning, and queries against sensitive tables not marked for pre-approved access.

The registry is an auditable artifact. Every entry records who defined the query, who approved it, when it was approved, which output types are permitted, the routing hint (single-source direct or cross-source Trino), and what version of the registration model it was approved against. When registration changes occur, affected registry entries are flagged for re-review. A query approved against a prior registration version does not silently continue running against a changed schema. The steward re-confirms the query remains appropriate for the updated surface.

Queries can be deprecated from the registry by authorized stewards. Deprecated queries return a clear error to clients directing them to the replacement. Queries cannot be deleted from the registry history, only deprecated, preserving the audit trail.

An approved query defines a permitted ceiling, not an exact required shape. The approval establishes the maximum surface the operation may touch: the tables it references, the columns it may return, the row scope before RLS is applied, and the output types it may produce. Clients may submit parameter bindings that further restrict the result set within that ceiling: a reduced column projection, additional filter conditions, a row limit. These restrictions stay within the approved boundary and do not require re-approval. The security property holds because restriction is one-directional. A client can ask for less than the approved ceiling but cannot ask for more. They cannot add tables, expand the column set beyond what was approved, circumvent RLS, or request an output type not sanctioned by the approval. This design avoids the governance overhead of requiring a separate registry entry for every minor variation of what is structurally the same operation, while preserving the guarantee that no execution can exceed the approved boundary.

---

## Client Entry Points

Provisa exposes three entry points into the same governed execution layer. The pre-approval registry, RLS enforcement, column security, and execution pipeline are identical regardless of which entry point the client uses. The interface differs. The governance does not.

The GraphQL endpoint is the primary entry point and the development surface. Application consumers, UI backends, and developers use the GraphQL query builder against the test endpoint and submit pre-approved operation identifiers against the production endpoint. This entry point is the natural fit for consumers already working within a GraphQL ecosystem.

The presigned URL redirect path serves large result consumers who need bulk data delivery. The client submits a pre-approved operation identifier through the GraphQL endpoint with a redirect preference and output type. Trino executes the query, writes the result to blob storage in the requested format, and the response returns a presigned URL rather than inline data. The client follows the URL for a direct storage download. The presigned URL carries a TTL enforcing time-bounded access without additional authorization infrastructure.

The gRPC endpoint with Arrow Flight serves high-throughput programmatic consumers: data pipelines, microservices, and analytical runtimes that prefer a typed binary protocol over a GraphQL interface. The client submits a pre-approved operation identifier and parameter bindings as a typed protobuf message. The result streams back as an Arrow Flight stream over gRPC. Trino supports Arrow Flight natively, producing columnar Arrow output directly without intermediate format conversion. For consumers using Pandas, Polars, Spark, or DuckDB, this is the zero-copy path: the data arrives in the format the runtime already uses internally, eliminating parsing, type conversion, and memory copy overhead at scale.

---

## Result Set Output Types

Large result sets support four output types reflecting the different consumer profiles that request bulk data. The approved query ceiling applies to output type: the steward specifies at approval time which output types are permitted for a given query. A client requesting an output type not sanctioned by the approval is rejected before execution.

JSON preserves the native GraphQL result structure: nested objects, arrays of children, relationships expressed as object hierarchy. No transformation is applied. This is the natural output for application consumers who handle JSON natively and want the relational structure preserved. NDJSON is the streaming variant: one JSON object per line, same nested structure, suitable for large results where the consumer reads line by line rather than loading the full document into memory.

Normalized tabular flattens the GraphQL result to relational tables while preserving entity structure. Each entity type becomes its own table with foreign keys maintaining the relationships between them. The consumer receives multiple files, one per entity type, with join keys intact. This is the appropriate output for consumers loading into a relational database or data warehouse who need entities separated but relationships preserved. Output format is Parquet or CSV per entity type.

Denormalized tabular produces a fully flattened single table. Every row is a complete record with all related fields inlined. One-to-many relationships produce multiple rows per parent, one per child, with parent fields repeated. The consumer receives a single flat file optimized for analytical tools: Pandas, Spark, DuckDB, or any system expecting a flat rectangular dataset. Output format is Parquet or CSV, single file or partitioned by a registered partition key. Parquet's columnar compression is particularly effective against denormalized output.

Arrow buffer is the highest-performance output type for programmatic consumers using modern analytical runtimes. Apache Arrow is the in-memory columnar format that Pandas, Spark, DuckDB, Polars, and most analytical runtimes use natively. Returning an Arrow buffer eliminates serialization and deserialization overhead entirely for those consumers. Trino produces Arrow output natively through its Arrow serialization mode, making this path efficient end to end without intermediate format conversion. Arrow output is delivered through the gRPC Arrow Flight endpoint rather than the presigned URL path.

The output type and the entry point are orthogonal choices within the bounds of what is technically coherent. JSON and normalized and denormalized tabular are available through the GraphQL presigned URL path. Arrow is available through the gRPC Arrow Flight endpoint. The client selects the combination that fits its consumption pattern, subject to the approved ceiling registered for the query.

---

## Query Execution Path

In production, a client submits a query through one of the three entry points. The executor applies a three-path decision at the entry point before any execution begins.

For registry-approved queries, the client submits a registry identifier and parameter bindings. The executor confirms the identifier exists in the active registry. If it does not, the request is rejected. There is no fallback, no partial execution, no helpful error suggesting alternative queries. For confirmed registry members, the executor retrieves the pre-compiled PG-style SQL, validates the parameter bindings against the registered parameter schema, validates the requested output type against the approved output types, injects RLS and column security, and routes to the appropriate execution backend.

For queries against pre-approved tables, the client submits a GraphQL query directly without a registry identifier. The executor confirms every table referenced in the compiled SQL carries the pre-approved flag. If any referenced table is not pre-approved, the request is rejected. For fully pre-approved queries, the executor compiles the GraphQL operation to PG-style SQL, injects RLS and column security based on the requesting user's rights, and routes to the execution backend. The query must be single-source to execute on the direct path. A multi-source query referencing only pre-approved tables routes through Trino. Pre-approved queries do not support large result redirect or Arrow output. Those output types require registry approval.

For all other queries, the request is rejected with a clear message indicating that the query references tables requiring registry approval.

The test endpoint executes queries with the same compilation and security pipeline as production but does not require registry membership or pre-approved table status. It accepts arbitrary queries against the registered schema with full guards applied. It is never exposed in production.

---

## Execution Routing

The routing decision between the direct path and the Trino path is deterministic and structural. The executor inspects the compiled SQL to determine how many registered sources the query touches.

Single-source queries route to the direct RDBMS connection for that source. This covers all queries that touch one or more tables within a single registered source: simple lookups, intra-source joins, aggregations against one source. The source database handles the query natively with its own optimizer, indexes, and statistics. SQLGlot transpiles the PG-style SQL to the target RDBMS dialect using the source type captured at table registration time. The warm connection pool provides the connection. Latency is equivalent to a direct database query: sub-100ms to low hundreds of milliseconds depending on query complexity.

Cross-source queries route to Trino. Any query that references tables from more than one registered source goes through Trino's federated execution engine. Trino handles the cross-source join strategy, predicate pushdown to each source, distributed execution across workers, and result assembly. SQLGlot transpiles the PG-style SQL to Trino SQL. Target latency for typical cross-source queries is 300-500ms reflecting the coordinator overhead and cross-source join coordination that Trino was designed to handle.

Large result sets above the configured threshold, regardless of routing path, redirect to blob storage with a presigned URL response rather than returning inline. Target latency for large result provisioning is asynchronous: seconds to minutes depending on result set size and cross-source complexity.

A steward override hint on the registry entry handles edge cases where the default structural routing is inappropriate. The override is the exception. The structural routing rule is the default and applies to the vast majority of queries.

Mutations always route to the direct RDBMS connection regardless of the structural routing rule. Trino is never involved in mutation execution.

---

## Mutation Execution Path

The compiler generates INSERT, UPDATE, and DELETE mutations from every registered RDBMS table. Mutations are structurally single-source by definition: a mutation targets a specific registered table, and a registered table belongs to exactly one registered source. Mutations always bypass Trino and always execute via the direct RDBMS connection. There is no routing decision, no routing hint, and no registry approval required for mutations.

Mutations are governed entirely by user rights. Four enforcement layers apply at the executor hook before any mutation reaches the source database.

Write rights determine whether the requesting user is permitted to mutate the target table at all. A user without write rights to a table cannot execute any mutation against it regardless of the operation type.

Column visibility rules define which columns can appear in INSERT input objects and UPDATE patch objects. The compiler generates mutation input types that reflect only the columns the requesting user's role is permitted to write. A column excluded from the user's write surface cannot be referenced in a mutation input. The compiler rejects the reference at parse time.

Row-level security rules define which rows the requesting user can affect. RLS WHERE clauses are injected into UPDATE and DELETE operations at the executor hook before execution, ensuring that a user cannot update or delete rows outside their authorized row scope even if they know the primary key.

Registered table scope ensures that mutations can only target tables that have been explicitly registered and governed. Mutations against unregistered tables are structurally impossible: the compiler does not generate mutation types for unregistered tables and the executor rejects any operation referencing an unregistered target.

SQLGlot transpiles the PG-style SQL mutation to the target RDBMS dialect using the source type captured at table registration time. The mutation executes directly against the source using a warm connection from the connection pool. Trino is not involved.

Cross-source transactions are not supported. A mutation targets a single registered RDBMS source. Users requiring cross-source transactional writes must handle that coordination in their application layer. NoSQL sources do not support mutations.

---

## Security Model

Provisa's security model has three independent enforcement layers. Each layer provides a guarantee that does not depend on the layers above it functioning correctly.

The pre-approval layer is the outermost and most fundamental. No query executes in production that has not been reviewed and approved by an authorized data steward. This layer eliminates the category of security incidents caused by queries that should not have been permitted. It cannot be bypassed by crafting a clever query, by escalating privileges, or by exploiting an authorization edge case. If the query is not in the registry, it does not execute. This is a platform-level enforcement. No level of user privilege overrides it.

The schema visibility layer ensures that unauthorized tables and columns do not appear in the GraphQL schema or the query builder. The compiler generates the SDL from the per-role column visibility rules in the registration model. A user whose role excludes a column cannot reference that column in any query. The compiler rejects the reference at parse time. The user cannot see what they are not permitted to see.

The SQL enforcement layer provides a runtime guarantee independent of the schema layer. The executor injects row-level security WHERE clauses and strips unauthorized columns from the compiled SQL before dialect translation and execution. This enforcement is applied to every execution regardless of what the client submitted, ensuring that no data outside the user's authorized boundary reaches the execution backend even if a pre-approved query is invoked by a user with narrower rights than the query's original author.

Row-level security rules are defined at table registration time as PG-style SQL filter expressions mapped to user roles. Column-level security rules are defined at table registration time and enforced through both the SDL generation and the executor SQL filter. Rights required for source registration, table registration, relationship definition, security configuration, query development, query authorization, and query execution are distinct and independently configured.

User rights and query governance address different threat models. User rights answer: what data is this user permitted to see? Query governance answers: what operations are permitted in production and under what conditions? Three governance modes cover the full spectrum. Mutations and pre-approved table queries are governed by user rights alone, appropriate where the registered and governed table surface is sufficient authorization. Registry-approved queries carry explicit steward review, appropriate for analytical complexity, cross-source scope, large result delivery, and sensitive tables. A compromised user account inherits that user's data rights but cannot execute registry-required queries without approval or access tables not marked for pre-approved access.

---

## Deployment Model

Provisa ships as two deployment artifacts targeting different operational contexts.

A Docker Compose file provides the development and small-team deployment target. The Compose file brings up the Provisa container including the compiler, the executor, the registration UI, and the query builder, alongside a Trino coordinator and a configurable number of Trino worker containers on a single host. All Trino connectors are pre-loaded in a curated Docker image shipped with the product. Worker count is a configuration parameter. A developer on a laptop runs one command and has a fully functional stack with production-equivalent behavior. The test endpoint is available immediately. No Trino knowledge is required.

A Helm chart provides the production deployment target for Kubernetes environments. The chart composes the Provisa container and the Trino cluster with horizontally scaled workers across cluster nodes. Trino's resource groups enforce per-tenant or per-workload resource isolation. Autoscaling is configured through standard Kubernetes HPA against Trino worker pods. The Helm chart references the official Trino Helm chart as a dependency and adds the Provisa components on top.

The Provisa container is stateless in both deployment targets. It points at a Trino endpoint for cross-source execution and schema introspection. The deployment topology behind that endpoint is a configuration concern, not an architectural one. The product behaves identically whether Trino is running in Compose on a laptop or in a Kubernetes cluster across multiple nodes.

---

## Connection Management

Each registered RDBMS source maintains a warm connection pool to minimize single-source read and mutation latency. Pool initialization occurs at source registration time. Minimum pool size is a registration parameter with a sensible default, overridable by the registering user based on expected query and mutation volume. Connection pooling for PostgreSQL sources uses PgBouncer included in the stack configuration. For other RDBMS types, driver-level pooling or source-appropriate proxies handle pool management.

The Trino read path maintains a single persistent connection to the Trino coordinator, reused across all cross-source read queries. Trino manages internal worker coordination and query distribution from the coordinator connection.

---

## Explicit Capability Boundaries

Single-source queries execute directly against the registered RDBMS source with native database performance. Cross-source queries execute against Trino with full federation and distributed execution. Mutations execute against registered RDBMS sources directly, one source per mutation, with full transactional semantics within that source. NoSQL sources are read only. Cross-source transactions are not supported. Large result sets above the configured threshold redirect to presigned blob storage URLs or stream via Arrow Flight over gRPC. Production read queries must either be pre-approved members of the persisted query registry or target tables marked as pre-approved for direct query access. Queries against non-pre-approved tables outside the registry are rejected. Mutations are governed by user rights and do not require registry approval. Pre-approved table queries do not support large result redirect or Arrow output, which require registry approval.

These boundaries are documented as first-class product constraints, not footnotes. Users who need capabilities outside these boundaries are directed to appropriate alternatives rather than encountering runtime surprises.

---

## User Interface

Provisa ships a branded, custom React-based UI as the primary interaction surface for all governed operations. The UI is not a generic admin panel. It is a purpose-built interface whose rendered surface is determined entirely by the requesting user's assembled role set. A user sees only the capabilities they have been granted. No capability is visible, discoverable, or accessible beyond what the role composition permits.

The UI is the rights model made visible. There is no gap between what a user is permitted to do and what the UI presents to them as possible.

---

### Role Composition System

The admin role composition system is the governance foundation of the UI layer. An admin assembles each user's capability set from the available building blocks. Capabilities are independently assignable and stack additively. A user may hold any combination of capabilities. The UI renders the union of all surfaces their capability set permits.

The available capabilities and their corresponding UI surfaces are:

Source Registration grants access to the source onboarding UI. The user can add new data sources, select from the pre-loaded connector types, supply connection details, validate connections, and trigger catalog registration.

Table Registration grants access to the table and view registration UI. The user browses available tables and views within registered sources, selects which to expose in the governed schema, reviews the column set from Trino's INFORMATION_SCHEMA, and sets column visibility per role.

Relationship Registration grants access to the relationship builder. The user reviews inferred candidate relationships from Trino's constraint metadata, confirms or rejects them, and defines cross-source and semantic relationships manually. Relationship Registration is typically assigned alongside Table Registration.

Security Configuration grants access to the RLS rule builder and column-level restriction controls. The user defines row-level security filter expressions mapped to roles and sets column visibility per role against registered tables.

Query Development grants access to the GraphQL query builder against the test endpoint. The user browses the governed schema surface, constructs queries and mutations, executes them against the test endpoint with full guards applied, and submits queries to the persisted query registry for approval.

Query Approval grants access to the approval queue. The approval capability carries a scope dimension: global or table-scoped. A global approver sees every query submitted for approval. A table-scoped approver sees only queries touching tables within their approval scope. Approval scope is assigned at capability composition time. A steward who registered a set of tables receives table-scoped approval authority over those tables as a natural extension of their stewardship. The approver reviews the full query definition, parameter schema, target tables, requested output types, and requesting developer identity. They approve or reject with a documented reason.

Admin grants access to the full UI surface including all capabilities above plus the user management panel where role compositions are assembled and assigned.

---

### UI Surfaces by Capability

The Source Management surface presents registered sources with connection status, connector type, and registration metadata. The add source flow walks through source type selection, connection detail entry, connection validation, and catalog registration confirmation.

The Table Registration surface presents a schema browser for each registered source showing available tables and their registration status. The steward selects tables to register, reviews the column set as reported by Trino's INFORMATION_SCHEMA, sets column visibility per role, and publishes the registration. Publication triggers the schema generation pass and makes the table immediately available in the query builder.

The Relationship Registration surface presents candidate inferred relationships from Trino's constraint metadata alongside the manual relationship builder. The steward confirms or rejects inferred relationships and defines cross-source relationships explicitly. Published relationship changes trigger schema regeneration for affected types.

The Security Configuration surface presents the registered table list with RLS rule management per table. The rule builder accepts PG-style SQL filter expressions with role bindings. Column restriction controls present a matrix of columns against roles with visibility toggles.

The Query Builder surface presents the governed schema surface scoped to the requesting user's authorized tables. The GraphQL query builder provides schema introspection, field selection, variable definition, and query execution against the test endpoint. Execution results display inline with applied RLS and column filters visible in the response metadata. The submission flow captures the query definition, parameter schema, intended output types, routing preference, and a description for the approver's review.

The Approval Queue surface presents pending query submissions with full detail: compiled query definition, parameter schema, target tables, requested output types, routing preference, and requesting developer identity. The approver approves or rejects with a required documented reason. Approved queries enter the active registry immediately.

The Registry surface presents the active persisted query registry: all approved queries with their identifiers, parameter schemas, permitted output types, routing hints, approval metadata, and usage statistics. Available to admins and approvers. Query developers see their own approved queries.

The Admin surface presents user management, role composition, source overview, table overview, relationship overview, registry overview, and system configuration. The role composition panel for each user presents capability toggles and the approval scope selector. A rendered preview shows the user's resulting UI surface before the composition is saved.

---

### Design Principles

The UI is branded and custom. It does not use generic admin UI frameworks or default component library aesthetics. The visual identity is consistent with the Provisa product family.

Every destructive or consequential action requires explicit confirmation. Source deregistration, table deregistration, relationship removal, query deprecation, and role composition changes present a summary of the consequence before committing.

The test endpoint execution surface makes the governed nature of the query visible. RLS filters applied, columns excluded, and schema scope enforced are shown in the execution result metadata so the developer understands they are seeing exactly what production will return for their role.

The approval queue is designed for steward efficiency. A steward should be able to review a submission, understand its scope and intent, and make an approval decision without consulting external documentation. The rejection reason is constrained to be specific and actionable so the developer knows exactly what to change on resubmission.

---

## Open Source and Commercial Positioning

The core product is open source. The Docker Compose deployment, the Helm chart, the registration UI, the purpose-built GraphQL compiler, the SQLGlot transpilation layer, the persisted query registry, and the Trino execution backend are all open source components composed into an open source product. This positions Provisa as a credible home for the Hasura v2 community displaced by the DDN transition.

A SaaS tier built on the open source core provides a hosted control plane for source registration, schema management, query authorization, and persisted query registry management, with a customer-hosted data plane option for enterprises with data residency requirements. An enterprise tier adds SLA guarantees, dedicated support, advanced audit logging, compliance reporting, and registry governance workflows for regulated industries.

The open source core is the top of the funnel. The SaaS and enterprise tiers are the revenue model. The product does not ask the community to trust a proprietary foundation before the value is proven.

The pre-approval model is the enterprise differentiator. For regulated industries, financial services, healthcare, government, the ability to enumerate every operation the production system will ever execute before a single production request is made is not a feature. It is a compliance requirement. Provisa is the only governed GraphQL data layer that makes that guarantee by design.
