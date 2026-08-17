# Defensive Publication — Active Semantic Layer Query Compilation and Delivery

**Author:** Kenneth Stott
**First published:** 2026-08-17
**Status:** Public technical disclosure. Published to establish prior art.
**Scope:** Design disclosure. A reference implementation exists, but nothing here depends on it —
each mechanism is described so that a person skilled in query compilation and distributed data
systems can implement it from this document alone.

---

## 0. Purpose and legal character of this document

This is a defensive publication. It describes, in enabling detail, a set of mechanisms for
compiling, governing, routing, and delivering queries in a system where the description of a data
estate and the executor of queries against that estate are the same artifact.

It is published so that the mechanisms below are part of the public state of the art as of the date
above. It is not a patent application, does not grant any license to the accompanying software, and
does not disclaim the author's own priority in any jurisdiction.

Terms used throughout:

- **Semantic model** — a declarative registration of sources, tables, columns, relationships,
  aliases, descriptions, and policy. State, not a program.
- **Semantic SQL** — SQL written against semantic names (`domain.table`), the only reference form
  a client is permitted to use.
- **IR** — the intermediate representation into which every accepted query is lowered before
  governance. Concretely a relational algebra tree; the disclosures below assume node types for
  projection, filter, join, aggregate, set operation, table reference, CTE, and limit.
- **Governed IR** — the IR after the governance pass has run. Every downstream stage consumes only
  governed IR.
- **Physical SQL** — dialect-specific SQL emitted from governed IR for one execution target.
- **Federation engine** — any engine capable of executing a query spanning more than one registered
  source. Treated throughout as a replaceable component behind an interface.

---

## 1. Background and the problem addressed

A semantic layer that only produces definitions delegates enforcement to whatever executes the
query. Each executor holds its own interpretation of the model. The interpretations diverge, and
the divergence is silent: a policy that holds in one client does not hold in another, and nothing
in the system reports the gap.

The conventional responses are integration projects (wire a metrics tool to an engine to a policy
service, and maintain the seams) or proxies that filter results after execution. Both leave a path
that reaches the data without passing through the policy.

The mechanisms below describe a different arrangement: one lowering pipeline that every language,
every API shape, and every wire protocol funnels into; governance applied as a compiler pass over
the IR rather than as a downstream filter; and physical execution delegated to interchangeable
engines that receive only already-governed plans.

---

## 2. Disclosure 1 — Convergent lowering of API shapes, query languages, and transports into one governed IR

### 2.1 Problem

A system that accepts several request shapes normally grows one enforcement path per shape. Policy
then has as many implementations as there are front doors, and they drift.

### 2.2 Mechanism

Requests converge in two stages before any policy logic runs.

**Stage A — API canonicalization.** Resource-oriented and RPC-oriented API shapes (an OpenAPI
operation, a JSON:API resource request, a gRPC method) are mapped to a graph selection set — a tree
of entity references, field selections, arguments, and nested traversals. GraphQL's selection set
is used as that canonical form because it already expresses exactly the entity-traversal-projection
triple that the mapping needs, and because generating the other three shapes' schemas from a
registration is then a projection of one artifact rather than three.

The mapping for each API shape:

- *OpenAPI*: path template and method select an entity and an operation kind; path parameters
  become equality predicates on the entity's key; query parameters map to filter, sort, and
  pagination arguments by a declared parameter-role annotation; the response schema's `$ref` graph
  determines the selection set's nesting.
- *JSON:API*: the primary resource type selects the entity; `include` becomes nested selections
  along registered relationships; `fields[type]` becomes the projection list; `filter`, `sort`,
  `page` map to their argument equivalents.
- *gRPC*: the method's request message fields carry the same parameter roles by annotation; the
  response message's nested message structure determines the selection set.

**Stage B — Language lowering.** Three query languages lower to the IR:

- *Graph selection set* → IR by walking the tree: each entity reference becomes a table node
  resolved through the semantic model; each nested selection becomes a join whose predicate is
  read from the registered relationship connecting the two entities; each scalar field becomes a
  projection; arguments become filter, sort, and limit nodes.
- *Cypher* → IR by mapping node patterns to table nodes via a label-to-entity map, relationship
  patterns to joins via registered relationships (including junction-table expansion for
  many-to-many), `WHERE` to filters, and `RETURN` to projections; variable-length paths lower to
  recursive CTEs bounded by the pattern's hop limits.
- *SQL* → IR by parsing to a relational tree and resolving every table reference through the
  semantic model.

**The single-reference-form rule.** Only semantic names are accepted. A reference to a physical
source catalog is rejected at parse time, before governance, with a diagnostic. This is what makes
the governance pass total: policy binds to semantic entities, so if a physical reference could
survive lowering it would reach the source unbound. The check is structural — walk every table node
in the parsed tree and reject any whose leading qualifier matches a registered physical source
catalog.

**Transport as a frame decoder only.** Wire protocols (a PostgreSQL wire implementation, Bolt,
Arrow Flight, gRPC, JDBC, HTTP, WebSocket, MCP) are implemented as decoders that produce two
things and nothing else: a query payload and an authenticated principal. Each transport normalizes
its own credential form — a startup message, a bearer token, an OAuth token presented by an
autonomous agent, a session cookie — into one principal record carrying role, org, and session
variables. From that point the transport is out of the path. There is no transport-local branch in
governance, routing, or delivery.

### 2.3 Consequences worth stating explicitly

Because an agent's token resolves to the same principal record as a person's, an agent is governed
by the same pass, with no second policy engine and no service account holding wider rights than the
principal it acts for.

Because every front door produces IR before policy runs, adding a front door cannot add a policy
bypass; the new door either produces IR or it does not work at all.

### 2.4 Admissible pairing matrix

Language and transport are independent, but the product is deliberately sparse. Some pairings are
rejected at the transport border rather than supported through wrappers, because honoring them
requires an encoding that defeats the point of the transport:

- A hierarchical selection set over a tabular row-frame protocol would require encoding nesting in
  a JSON-typed column, which the tabular client's metadata and type system cannot describe.
- A graph query returning paths and vertices over a tabular protocol has no result type to land in.
- A deeply nested selection set over a columnar zero-copy transport requires flattening to record
  batches, which discards the property the transport exists for.

The disclosed design is therefore a declared routing table of admissible (language, transport)
pairs, consulted at the border, rejecting an inadmissible pairing with a protocol-native error
before the parser runs. The sparseness is part of the design, not a gap in it.

---

## 3. Disclosure 2 — Governance as an IR pass, applied before optimization and routing

### 3.1 Problem

Policy applied after execution has already moved the rows. Policy applied by a proxy that rewrites
query text is fragile against the text's structure — subqueries, set operations, correlated
predicates, and CTEs each offer a place for a rewrite to miss a reference. Policy expressed as
per-role database views multiplies objects by roles and does not survive federation.

### 3.2 Mechanism

Governance is a transformation over the IR that visits every relation node, including those inside
subqueries, set-operation branches, and CTE definitions. The pass is total by construction: it
recurses on the tree rather than pattern-matching on text, and a second bottom-up sweep re-visits
subquery nodes whose parent was rewritten before the child was reached, so that a parent rewrite
cannot shadow an ungoverned child.

Layers applied within the pass, in order:

1. **Introspection filtering.** Catalog and schema-listing results are filtered to the entities the
   principal may see, so an unauthorized entity is absent from discovery rather than present and
   denied. This closes the inference channel where an error message confirms existence.
2. **Access scoping.** Each referenced entity is checked against its declared visibility — public,
   domain-scoped, or role-scoped. A reference the principal cannot reach fails compilation, not
   execution.
3. **Relationship legality.** A join between two entities is admitted only if a registered
   relationship connects them, or the query declares an explicit join predicate the model permits.
   An unregistered traversal does not compile. This is the property that distinguishes the design
   from federation: an illegal path is not executed-and-filtered, it has no compilation.
4. **Row-level security.** For each governed relation, the policy predicates bound to that entity
   and principal are conjoined into the relation's filter. Predicates may reference session
   variables; those are resolved to literals during the pass, from the principal record, so that
   the emitted plan carries no unresolved session state into an engine that would not know how to
   resolve it.
5. **Column masking.** A masked column's projection is replaced with the masking expression
   declared for that column and principal — redaction, hashing, partial reveal, or a null literal —
   at every position the column appears, including inside expressions and aggregates.
6. **Result ceilings.** A limit node is injected or lowered to the most restrictive of the
   role-level ceiling and any per-entity ceiling on a referenced entity. Where the query's existing
   limit is a parameter or expression whose value is unknown at compile time, the relation is
   wrapped in a subquery carrying a constant outer limit, so the ceiling binds regardless and the
   emitted SQL stays valid on engines that reject expressions in a limit clause.
7. **Pre-execution predicate guard.** Before emission, the governed tree is re-walked to assert that
   every relation carries its required policy predicates and that no masked column survives
   unmasked. A tree failing this assertion is refused. The guard exists because layers 4 and 5
   depend on the traversal having reached every node; the guard checks the outcome rather than
   trusting the traversal.

**Refusals are recorded.** A compilation refused by any layer writes an audit record naming the
principal, the statement, and the layer that refused. A denial is evidence, not an absence.

**Stage ordering is the inventive point.** The order is: govern, then optimize, then select the
execution topology. Optimization runs over already-governed IR and therefore cannot introduce an
ungoverned path, and topology selection observes the post-optimization plan rather than the
submitted one. Stated without reference to this design: policy is applied to a plan before the plan
is optimized, and the plan's execution target is chosen after. Disclosure 3 is what that ordering
makes available.

---

## 4. Disclosure 3 — Plan-level source elimination as an execution-topology decision

### 4.1 Problem

Systems that answer a query from more than one data source have more than one way to execute it. A
plan whose relations all reside in a single source can usually be handed to that source's own
engine as one statement in its native dialect. A plan spanning two or more sources cannot, and must
be executed by a layer above them — a federation coordinator, a distributed engine, a wrapper that
pulls one side into the other. That layer costs an extra hop, serialization at both ends, a join
executed outside any source's own optimizer, and memory held for the duration.

The choice between those topologies is conventionally made from the query as written. A query
joining a large fact relation in one source to a small dimension relation in another references two
sources, so it is executed distributed — even when the second input is a few hundred rows and the
entire distributed apparatus exists to move them.

The disclosure is that the topology choice need not be made from the query as written. It can be
made from the plan as optimized, and an optimization exists that changes the answer.

### 4.2 General mechanism

Let a plan reference relations resident in N distinct data sources, N > 1.

1. **Bounding.** Identify a relation R in the plan whose full contents are provably small — bounded
   in cardinality and in serialized size against configured ceilings.
2. **Literalization.** Replace R's reference in the plan with an equivalent *inline constant
   relation*: a literal tuple list carried in the plan itself, semantically equal to R, referencing
   no source.
3. **Re-derivation.** Recompute the plan's source set from the rewritten plan. R's source no longer
   appears in it — unless another relation independently holds it. N decreases.
4. **Re-selection.** Choose the execution topology from the recomputed set rather than the original
   one. When N has fallen to 1, the plan is emittable as a single statement in that one source's
   dialect and executable by that source's own engine. The layer above the sources leaves the
   execution path entirely — not as a coordinator, not as a pass-through.

The mechanism is not specific to any architecture beyond the precondition that more than one
execution topology is available. It applies to:

- a distributed query engine that can also push an entire query into a single connector, where
  literalizing one catalog's small relation makes a two-catalog query fully pushable into the other;
- a database with foreign data wrappers, whose join pushdown requires both relations to be on the
  same foreign server — literalizing the local side makes a local-to-foreign join pushable;
- a lakehouse or analytical engine joining a catalog-managed table to an external one;
- a gateway or semantic layer choosing between a native driver to a source and a federating engine
  above several.

The byte ceiling in step 1 is not merely a cost heuristic. The literalized relation must be
embeddable in a statement the *target* source's parser will accept; the ceiling is what makes the
rewrite emittable, and it is measured on the serialized form because a ceiling on row count admits
arbitrarily wide rows.

Step 3 is the substantive one. Steps 1 and 2 alone are a local rewrite of a plan; they yield a
smaller scan and nothing else. It is recomputing the source set *from the rewritten plan*, and
deferring the topology decision until after that recomputation, that converts the rewrite into a
change of which system executes the query.

### 4.3 Other source-eliminating reductions

Literalization is one way to remove a source from a plan; the disclosure covers the class. Any
optimization that eliminates a source's participation feeds the same re-derivation:

- **Materialization into an execution-local store.** A relation copied into a store the executing
  engine owns no longer references its origin source.
- **Branch elimination.** A union branch, or a disjunct, dropped because its contributing source
  provably returns nothing under the predicates in play removes that source.
- **Constant-folding a scalar subquery.** A subquery reduced to a literal removes the source it
  read from.
- **Partition and predicate pruning to empty.** A source whose every partition is excluded by the
  plan's predicates contributes no rows and no source.

Where a reduction instead points the plan at a relation only the upper layer can reach, that
reduction's own reachability constraint overrides the source-count rule and the topology stays
distributed. The re-derivation therefore carries reachability alongside identity, not just a count.

### 4.4 Instance — small-relation literalization as a `VALUES` CTE

The remainder of this section describes one concrete realization of §4.2, at the level of detail
required for enablement. Nothing in §4.2 depends on these particular choices.

**Small-relation identification.** A relation qualifies as inlinable when both of the following
hold, evaluated against a configured row ceiling and a serialized-byte ceiling:

- its cardinality is at or below the row ceiling, established by any of: an explicit registration
  marking the entity as small; a count taken at model-load time; or observation of a prior query
  whose result for that relation came in under the ceiling, which promotes the relation on first
  use rather than requiring a scan up front;
- its serialized form is at or below the byte ceiling, measured on the serialized payload rather
  than inferred from row count, because a narrow ceiling on rows admits arbitrarily wide rows.

Additionally, an entity that is the target of a registered many-to-one relationship is a candidate
by structure alone: being the "one" side of a many-to-one is what dimension-ness means in the
model, and it is available from the registration without touching data.

Qualifying relations are held in a cache with a time-to-live and are invalidated on write to the
underlying entity. The cached payload is encrypted at rest; see §5.3.

**Inlining.** When a governed plan references an inlinable relation, the reference is replaced,
structurally on the tree, with a CTE whose body is a literal `VALUES` list of the cached rows. The
substitution is done on the parsed tree: locate the table node by name, rename it to the CTE, and
attach the CTE definition to the query's `WITH` node — never by matching or splicing query text,
because text matching cannot distinguish the intended occurrence from a coincidental one and fails
loudly only sometimes. An empty cached relation inlines as a typed empty set (`SELECT NULL, …
WHERE 1=0`), preserving column arity and join semantics. An unaliased reference receives an alias
equal to its original name, so column qualifiers elsewhere in the query still resolve after the
relation is renamed.

**Raw rows in the CTE, and why that is safe.** The cached rows are inlined verbatim, ungoverned.
This is sound only because of the stage order in §3: governance has already wrapped the surrounding
query, so its filters and masking expressions apply to the CTE's rows exactly as they applied to
the live relation. The alternative — caching a governed copy per principal — multiplies storage by
role count and creates a store holding pre-filtered per-principal data, which is a disclosure
surface the design does not need. Order of stages substitutes for per-role materialization.

**Re-derivation and route selection over the reduced set.** Per §4.2 step 3, the source set is
recomputed from the rewritten plan; a relation that has become a literal `VALUES` list contributes
no source. Over the recomputed set, topology selection is a classification:

- zero remaining sources, or a hit in the result cache keyed on the governed IR — serve without
  execution;
- one source with a direct driver and a dialect the plan can be emitted in — direct;
- one source with no direct driver (a document store, a file-backed source, a stream, a lakehouse
  catalog, an HTTP API) — engine, since only the engine's connector reaches it;
- more than one source — engine;
- any write — direct, never through the engine.

The result-cache key is derived from the governed IR rather than the submitted text, so two
differently-written queries that govern to the same plan share a cache entry, and two identical
texts submitted by principals with different policy do not.

### 4.5 Distinguishing prior practice

Shipping a small relation somewhere to make a join cheaper is long-established. Each established
form optimizes execution *within* an already-chosen topology. The distinguishing property here is
that the rewrite is applied *before* the topology is chosen, and changes what is chosen.

| Technique | What is shipped | What it changes | Topology after |
|---|---|---|---|
| Broadcast / replicated join | The small side, to every worker | Data movement for the join | Unchanged — still distributed, same engine |
| Dynamic filtering / semi-join reduction | Build-side values, **as a predicate** | Rows scanned on the probe side | Unchanged — join still executed distributed |
| Materialized view or cached copy of the dimension | Nothing at plan time | Where the relation is read from | Unchanged — it is still a relation in a source, and still counts toward the source set |
| Constant folding of a scalar subquery | Nothing | Plan size | Unchanged in general — folding is not applied with source-count reduction as its objective |
| **This disclosure** | The relation itself, **as a plan-embedded constant relation** | The source set, hence the execution topology | Changed — the distributed layer leaves the path |

The second distinguishing property is ordering. The rewrite must sit after policy application and
before topology selection. A pipeline that selects topology first cannot benefit: the choice is
already fixed when the rewrite happens. A pipeline that applies policy after topology selection
cannot perform the rewrite safely: the literalized rows would enter the plan ungoverned, and the
soundness argument for inlining raw rows in §4.4 is unavailable. The govern → optimize →
select ordering is therefore not an arbitrary arrangement of independent stages; it is the
precondition that makes source-eliminating optimization both possible and safe.

---

## 5. Disclosure 4 — Threshold-triggered out-of-band materialization returned as a protocol-native delivery handle

### 5.1 Problem

In an environment where clients compose queries dynamically — an analyst at a prompt, a generated
dashboard query, an autonomous agent — result size is not known before execution and is not bounded
by anything the client did. The failure this produces is not a slow query; it is a gateway holding
a multi-gigabyte serialization in memory, missing health checks, being declared dead, and taking
its connection pool down with it.

The conventional responses are all bad in a specific way. A silent row cap corrupts every
downstream computation while reporting success. A hard timeout discards the compute already spent
and returns nothing. An explicit bulk-unload command requires the client to have known in advance
that it would need one, which is exactly the knowledge it lacks. And for an agent, a large inline
result does not merely slow the session, it consumes the context window and ends it.

### 5.2 Mechanism

**The decision is an IR-level directive, not a transport branch.** Whether a result is delivered
inline or materialized is attached to the governed plan as a delivery directive, before execution.
Every transport inherits it because every transport executes through the same terminal. There is
no per-transport "should I redirect" logic anywhere in the design; adding a transport cannot add a
divergent answer.

The directive is populated from either of two policies:

- *Caller-requested*: a transport-specific side channel (a request header, transaction metadata, a
  tool-call argument) asks for materialization unconditionally, optionally naming a format and a
  threshold.
- *Automatic*: where large-result delivery is enabled in system configuration, every result on a
  buffered transport is subject to the configured row threshold, evaluated at the terminal.

**Sink tier selection, evaluated once at the terminal.** Two tiers, chosen by whether the execution
target can write the requested format itself:

*Object-store tier.* When the requested file format is one the engine writes natively (columnar
formats such as Parquet or ORC) and an execution engine is connected, the terminal issues a
create-table-as-select over the governed physical SQL, targeting a generated prefix in an
S3-compatible object store. The engine writes the result files directly to object storage. The
gateway then lists the prefix, mints a time-limited presigned URL for the data file, and returns a
delivery handle. **Zero result rows pass through the gateway's memory on this path.** The gateway
handled a plan, a prefix, and a signature.

*Local sink tier.* Where the format is not engine-native or no object store is reachable, the
result is written to a local sink served by an authenticated endpoint with per-principal scoping
and time-to-live reaping. This tier exists so that the mechanism is exercisable in a development
configuration without standing up object storage, and so that the delivery contract does not vary
with deployment shape.

**Governance is inherited, not re-applied.** The statement handed to the engine's create-table-as-
select is the governed physical SQL — row-level predicates already conjoined, masking expressions
already substituted, ceilings already bound. The object written to storage is therefore governed at
the moment it is written. There is no second authorization at the storage layer, and no possibility
of the exported copy carrying more than the principal could have read inline. This is the property
that makes out-of-band delivery safe rather than a hole.

**The handle, and protocol-native delivery of it.** The terminal returns an opaque handle:
`{sink, locator, row_count, expires_in, content_type}`. Each surface renders that handle into its
own envelope, and this is where the design differs from an asynchronous job API:

- a tabular wire protocol returns it as a structured error whose fields carry the locator, so an
  existing driver surfaces it to the caller instead of hanging or truncating;
- an HTTP API returns it as a payload-too-large response carrying the locator;
- an agent tool call returns it as a small structured result naming the locator and the row count,
  so the agent learns the size and the address without the payload entering its context.

The handle is a normal, expected outcome of a query, delivered in the transport's own vocabulary.
The client did not have to ask for it, did not have to know the result would be large, and did not
have to change its connection.

**Lifecycle.** A background task deletes the objects under the prefix after the URL's lifetime
expires. Where the write went through an engine-managed table, the table definition is deliberately
not dropped at hand-off time: on catalogs where dropping purges the underlying data files, an
immediate drop would invalidate a URL that has already been given to the client. The reaper handles
both.

### 5.3 Role-bound envelope encryption of the out-of-band payload

Optionally, the payload is envelope-encrypted before it lands, and the encryption is bound to the
principal rather than to the storage system:

- a data encryption key is generated and used to encrypt the payload under an authenticated cipher;
- the data key is sealed, together with the identity of the creating principal, under a master key,
  producing an opaque integrity-protected grant;
- the delivery handle carries the grant and the address of an authenticated unwrap endpoint, not
  the key;
- to read, the client presents the grant to the unwrap endpoint, which opens it, verifies the
  caller is the creating principal or an administrator, and returns the data key; the client
  decrypts locally.

The consequences are the point: a leaked locator alone yields ciphertext; an operator with
administrative access to the object store alone yields ciphertext; and the grant cannot be
re-scoped to a different principal because the principal is sealed inside it under integrity
protection. The same envelope construction is applied to the small-relation cache of §4.4, so
cached dimension rows are ciphertext at rest.

### 5.4 Distinguishing prior practice

Warehouses spill intermediate execution state to local storage under memory pressure; that is
internal memory management and produces no client-visible artifact. Warehouses also offer explicit
unload and presigned-URL functions; those require the client to have decided in advance. Some HTTP
APIs return a redirect or an accepted-status with a polling location for known-large exports; those
are asynchronous job submissions, not the outcome of an ordinary synchronous query, and they are
specific to HTTP.

What is disclosed here is: an automatic, threshold-triggered decision, attached to the plan before
execution and therefore uniform across every transport; executed by the engine directly to object
storage with no result rows in the gateway; carrying compile-time governance into the written
object; and delivered as a protocol-native terminal frame that an unmodified client already knows
how to surface.

---

## 6. Disclosure 5 — The execution engine as a trait-declaring, replaceable component

### 6.1 Problem

A system that hardcodes an engine inherits that engine's reach, cost model, and lifecycle. Worse,
planner decisions get written against one engine's specific behavior and become undocumented
dependencies on it.

### 6.2 Mechanism

An engine is registered as a named object carrying a connector set and a set of **declared traits**.
The traits are orthogonal dimensions, not an engine identity:

- *reach* — the class of sources the engine can address at all;
- *scale* — whether execution is massively parallel;
- *storage* — whether the engine scans file and object sources in place, or requires them landed
  first;
- *pooling* and *transactionality* — whether connections are pooled, and whether the engine
  supports transactional writes;
- *native store* — the source type the engine materializes into natively, or none for a pure
  federator;
- *transports* — which result-delivery transports the engine advertises, such as a columnar stream.

The planner reads traits. It never branches on an engine's name, and no per-engine table lives in
the planning code. A trait that a decision depends on must be declared by the engine, so the
dependency is explicit and enumerable rather than discovered when a swap breaks.

Concrete terminal behavior — execute, dialect emission, lifecycle, introspection, create-table-as-
select — sits behind a backend interface attached to the engine instance. This keeps every
engine-specific reference inside that engine's own object and out of the compiler.

The consequence: the same governed IR runs against a distributed query engine, an embedded
analytical engine, a warehouse acting as its own federator, or a relational database with foreign
data wrappers, by changing a registration. The semantic model, the policy, and the join topology
are unchanged, because they were never expressed in the engine's terms.

---

## 7. Disclosure 6 — Assisted model construction with human ratification

### 7.1 Problem

Cataloging programs fail on labor. The philosophy was never disputed; the method asked humans to
type what a machine could read, and finished after the estate had moved.

### 7.2 Mechanism

- **Structure is read.** Registering a source discovers its tables and columns from the source
  itself. Registering a file location discovers what a crawl of that location finds.
- **Declared constraints become traversals.** Existing foreign keys register as relationships in
  both directions, named for the concept rather than the constraint.
- **Undeclared relationships are proposed.** A language model reads the schema and proposes missing
  relationships, each carrying a cardinality, a confidence score, and its reasoning.
- **Proposals are candidates, never live edges.** A proposal has no effect on compilation until a
  steward accepts it. Reviewing a proposed join is bounded work; discovering it is not.
- **Descriptions are drafted, not typed.** Every description field carries a draft action; a
  glossary can be drafted in one pass; terms are normalized from physical field names.
- **Explicit overrides discovered.** A steward's definition takes precedence over a discovered one,
  so correcting a machine proposal is an edit rather than an escalation.
- **Registration is additive.** Adding a source, a relationship, or a policy rule is an addition to
  declarative state, not a re-modeling of what is already registered. A domain registers
  independently of every other domain, and a path between two domains registers when someone needs
  to walk it.

The disclosed combination is: automated structural discovery, model-proposed semantic edges with
attached confidence and reasoning, a candidate state distinct from live state, human ratification
as the only transition into live state, and precedence of explicit over discovered — arranged so
that catalog coverage accretes as a side effect of individually-funded work rather than requiring a
cataloging program.

---

## 8. Summary of what is disclosed

1. Convergent lowering of multiple API shapes into one canonical graph selection set, of three
   query languages into one IR, and of many wire transports into one principal record and one
   payload — such that policy has exactly one implementation regardless of front door, and an
   autonomous agent's token is governed by the same pass as a person's.
2. A single-reference-form rule that rejects physical source references at parse time, which is
   what makes the governance pass total.
3. Governance as a recursive IR pass with seven enumerated layers, a post-pass structural guard
   that validates the outcome rather than trusting the traversal, and audit of refusals.
4. The stage ordering govern → optimize → select-topology, where optimization may remove sources and
   topology selection observes the reduced set — applicable to any system with more than one
   available execution topology, not to any particular architecture.
5. Plan-level source elimination as a topology decision: bounding a relation, replacing it with an
   inline constant relation carried in the plan, re-deriving the plan's source set from the
   rewritten plan, and selecting the execution topology from the reduced set — so that a nominally
   cross-source query becomes a single statement executed by one source's own engine and the layer
   above the sources leaves the execution path. The class covers any source-eliminating reduction
   (literalization, materialization into an execution-local store, branch elimination, scalar
   folding, pruning to empty), with reachability carried alongside identity in the re-derivation.
6. As one enabling instance: identification of small relations by row ceiling, serialized-byte
   ceiling, registered many-to-one target position, or first-use promotion, and structural
   replacement of such a relation with a literal `VALUES` CTE on the parsed tree.
7. Inlining raw cached rows rather than per-principal governed copies, sound because governance
   precedes inlining in the stage order.
8. A delivery directive attached to the governed plan before execution, evaluated once at a single
   terminal, so that inline-versus-materialized is uniform across every transport by construction.
9. Threshold-triggered materialization executed by the engine directly to object storage with zero
   result rows in the gateway, carrying compile-time governance into the written object, returned
   as a protocol-native terminal frame — a structured error on a tabular wire protocol, a
   payload-too-large response over HTTP, a small structured handle to an agent — so that an
   unmodified client surfaces it without hanging, truncating, or exhausting a context window.
10. Role-bound envelope encryption of out-of-band payloads and cached relations, where the grant
    seals the creating principal alongside the data key and is opened only by an authenticated
    endpoint, so that a leaked locator or storage-level access alone yields ciphertext.
11. Engines registered as declared orthogonal traits behind a backend interface, with the planner
    reading traits and never engine identity.
12. Assisted model construction with model-proposed edges carrying confidence and reasoning, held
    as candidates until human ratification, with explicit definitions taking precedence over
    discovered ones.

---

## 9. Publication record

| Field | Value |
| --- | --- |
| First public disclosure | 2026-08-17 |
| Repository | this document's containing repository, public |
| Archival | tagged release with a persistent identifier minted at publication |

Any correction or extension to this document is published as an amendment with its own date. The
original text is not rewritten, so the disclosure date of each mechanism stays determinable.
