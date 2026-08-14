# RAG features — chonk integration design

Working design notes for integrating the sibling project `chonk`
(`/Volumes/main/Users/kennethstott/PycharmProjects/chonk`) as Provisa's document
retrieval layer. Nothing here is a requirement yet; requirement entries get
written into `docs/arch/requirements.yaml` once the design settles.

## The configuration surface

The goal: **two column tags + document URNs + one admin scheduler tab are the
whole configuration of chonk.** No separate chonk config file, no per-source
knobs, nothing a user has to learn twice.

| Input | Where the user sets it | What it produces in chonk |
| --- | --- | --- |
| `entity:{type}` column tag | Tag registry (REQ-1414, REQ-1467) | one `NerPipeline.add_from_db` vocabulary query per entity type |
| `natural_language` column tag | Tag registry (REQ-1414) | `loader.load_from_db` per-row documents |
| Document folder URN | Sources page, `kind = documents` | a crawled source in the namespace build |
| Store / loader / index / interval | Admin tab | `ChonkConfig.store`, `.loader`, `.index`, refresher interval |
| Embedding model | Existing `/admin/ai-models` tab | `EmbedConfig.model` |

The entity types themselves are not a separate surface: they are the value list
the `entity` tag carries in the tag registry, editable on the Tags tab even
though `entity` is a system tag with no row of its own (REQ-1467). The list is
closed, so a steward picks a type rather than typing one — a misspelt
`entity:custmoer` would reach chonk as a real type and produce a graph whose
customers are silently split in two.

`ChonkConfig.sources` and `ChonkConfig.namespaces` are **derived and read-only** —
they are computed from the registered document URNs and the org's domains. The
user never edits them.

`EnhancedSearch` carries roughly thirty retrieval knobs (lane toggles, scoring
weights, expansion budgets, similarity floors). None of them are user
configuration. Provisa picks defaults, and a caller tunes retrieval per query
through the MCP tool arguments — `k`, `query_text`, `mode` — not through a
settings page. The knobs exist for ablation benchmarking; exposing them would
break the claim above.

## Document folders live in Sources

Sources splits on a `kind` discriminator above `type`:

- `kind = relational` — the existing ~35 `SourceType` connectors.
- `kind = documents` — a document folder, whose "type" is a URI scheme
  (`file` / `s3` / `ftp` / `sftp` / `hdfs`), not a connector.

They go in a **separate `document_sources` table** sharing the id namespace with
`sources`. Rationale:

- The adapter contract (`generate_catalog_properties`,
  `generate_table_definitions` in `provisa/source_adapters/registry.py`) does not
  apply to a document folder — there is no catalog and no table definition.
- `sources` has no domain column; a document folder needs one, because domain is
  the chonk namespace.

### `UNIQUE (urn, domain)`

The constraint is the whole multi-domain mechanism. Two registry rows may carry
the same URN with different domains, which puts one folder into two namespaces.

This is deliberately **undocumented**. The UI shows one domain per entry, so the
user's default reading is one-URN-one-domain; the second entry is available to
anyone who tries it but is not advertised.

URN normalization at registration is a correctness requirement, not a nicety:
document identity in chonk is the document *name*, which derives from the path.
An unnormalized duplicate path registers as a second folder and reindexes
everything. A path change is a delete plus a re-register, never an edit.

### No per-source cadence columns

Chonk's `NamespaceRefresher` (`chonk/lifecycle.py:173-260`) holds a single global
`interval_seconds` shared across every namespace. Per-source cadence columns
would be configuration Provisa stores and chonk cannot honor. The interval is one
field on the Admin tab.

## Derived sources need no registration

The column tag *is* the registration. Nothing extra is entered anywhere.

**`entity:{type}` tag** → one entity-vocabulary query *per entity type*. Single
column, no primary key, `SELECT DISTINCT`. The tag carries its parameter
(REQ-1467): `entity:customer` on `CUSTOMER.NAME` and `entity:employee` on
`SALES_REP.NAME` are two types, and the columns of each type group into one
query. That grouping is what `add_from_db` takes — its `queries` argument is a
`dict[entity_type, sql]` (`chonk/ner/_schema_vocab.py:412-453`), so the loader
passes `{"customer": …, "employee": …}` and chonk labels every match with the
type the query came from. The types are the values the maintainer holds on the
`entity` tag; the ids chonk mints are `{type}:{slug}`, which is why a customer
named Mercury and an employee named Mercury stay distinct entities. Without the
parameter every value would arrive under the default type `term` and the graph
would merge them.

The query results accumulate into `self._data_terms` in memory
(`chonk/ner/_schema_vocab.py:406-409`) and never
persist. Only *matched* entities are written
(`chonk/ner/_build.py:206-246`), and a match requires the text to already be in
that namespace — so the vocabulary itself discloses nothing. It is rebuilt live
at every namespace build.

**`natural_language` tag** → per-row documents. Primary-key bearing, one document
per row, named `schema.table.column/<pk>` with SQL
`SELECT column FROM schema.table WHERE pk = '<pk>'`. Fed to
`loader.load_from_db` (`chonk/loader.py:260-315`), which makes each query a
separate document keyed by the dict key.

## Authorization: domain-grain namespace, then row-grain re-check

### Namespace pre-filter

The caller's allowed domains — read from `user_role_assignments`, with `*`
expanded server-side — become the `namespaces` argument on every chonk search.

Rules:

- **Pre-filter, never post-filter.** The namespace list is an argument to the
  search, not a mask applied to results.
- **An empty set denies.** It must never degrade to `None`: omitting `namespaces`
  searches *everything* (`mcp_chonk_server.py:479-486`). The inverted default is
  the hazard.
- `CHONK_DB_CONFIG` stays a single entry.

### Row-grain re-check — via `chunk_filter`

Domain grain alone would make tagging `natural_language` on an RLS-protected
table a declassification — row rules lost at ingest, only domain reachability
surviving. That is avoidable, so it is not the design.

A chunk derived from a row already carries its identity in the document name
(`schema.table.column/<pk>`). Chonk indexes the full corpus, and retrieval
re-authorizes through a hook chonk already provides:
`EnhancedSearch(chunk_filter=...)`, a `(list[ScoredChunk]) -> list[ScoredChunk]`
callable applied at the end of every `search()` (`chonk/search/_enhanced.py:77-86`).

Provisa's filter:

1. Partitions the cohort into row-keyed and free-standing chunks by document name.
2. Resolves `pk IN (...)` against the source table under the caller's RLS, inside
   `_govern_and_route`. Rows the caller cannot see drop out.
3. Returns free-standing chunks untouched — the namespace pre-filter already
   authorized those.

Over-fetch on `k`, since the cohort is assembled and ranked before the filter
runs, so a request for 10 otherwise returns fewer.

The authorization decision never leaves the settled query pipeline; there is no
second governance path. This check is narrower than the namespace filter and
layers under it, applying only to chunks that have a row to check — exactly the
set domain grain was too coarse for.

**`ask()` bypasses `chunk_filter`.** Chonk's own docstring states the filter is
not applied when `search()` is called internally by `ask()`; that path takes
`redaction_filter`, an `(Answer) -> Answer` hook applied after generation.
Wiring RLS only into `chunk_filter` would therefore leave the entire `ask()` path
unauthorized.

Provisa does not call `ask()`. It calls `search()`, where `chunk_filter` always
runs, and owns generation itself — Provisa already has the model registry, the
prompt path and the audit surface, and generation is where citations must be
reconciled against what the caller may see. `redaction_filter` remains relevant
for sovereign deployments (see below) but is not the RLS mechanism.

**Why the corpus is not split.** An earlier draft routed in-table text to native
vector columns (REQ-421) and left chonk the standalone documents. That fails:
graph edges are chunk co-occurrence over `chunk_entities` within a namespace
(`chonk/graph/_context_graph.py:78-95`), so two chunks link only if they share an
entity in the same index. Withholding row-keyed text does not yield two smaller
graphs — it yields one graph with its connective tissue removed, since entity
mentions are densest exactly in that text. A support ticket would never cluster
with the contract it refers to.

Native vector columns keep a separate job: `cosine_similarity()` as a semantic
predicate inside SQL. They are not a retrieval lane.

### Residual: inference through cluster structure

Entity vocabulary and community membership are computed over the full corpus, so
cluster shape is influenced by rows the caller cannot read. No content crosses —
the row-grain re-check removes the chunks themselves — but the existence and
rough association of an unreadable row is inferable from structure.

This is an inference channel, not disclosure, and it is the price of a shared
graph. Recorded here because the namespace boundary is otherwise the only one the
design names.

Rename cleanup is a user concern. `documents` carries no `source_id` and
`list_documents` is unscoped, so there is no orphan sweep to build — a renamed
document is a new document and the old one is removed by the user.

## Admin tab

Owns `store` / `loader` / `index` and the refresher interval. Gated on
`platform_settings`, because the build kwargs are shared across every namespace
in the deployment — one org's chunk sizing would otherwise reach into another's.

`embed.model` belongs on the existing `/admin/ai-models` tab instead, with a
forced-full-reindex warning: the embedding dimension is baked into the DDL as
`embedding FLOAT[{dim}]`, so changing the model invalidates every stored vector.
`CHONK_CHAT_MODEL` and `TOGETHER_BASE_URL` belong there too.

## What chonk retrieval actually does

`EnhancedSearch` (`chonk/search/_enhanced.py`) is not a vector store wrapper, and
the integration should not treat it as one.

### Four-dimensional cohort assembly

One `search()` call assembles candidates across four dimensions, each
independently switchable:

| Dimension | Source | Priority |
| --- | --- | --- |
| Seed | vector similarity, plus BM25 when `query_text` is passed | 1.0 |
| Structural | next / prev / parent chunk adjacency | 0.9 |
| Entity-adjacent | chunks sharing an entity, via `EntityIndex` | 0.7 |
| Cluster-adjacent | cluster neighbours, budget-limited | 0.5 |
| Context-graph | graph traversal expansion, off by default | 0.6 |

Ranking is a composite of relevance, source priority and marginal coverage
(default weights 0.5 / 0.2 / 0.3) with an MMR redundancy penalty
(`lambda_diversity`). Passing `query_text` alongside the embedding is not
optional decoration — it turns on the BM25 lane, which is what anchors exact
names, identifiers and codes.

### Three modes

- **`vector_first`** (default) — similarity seeds, expansions widen.
- **`graph_first`** — `RelationshipIndex` traversal drives, vector reranks. For
  questions about how things connect rather than what mentions a term.
- **`global`** — searches community-summary chunks only. With
  `map_reduce_global_context` (map an LLM over each community summary, score,
  reduce to the top) this answers corpus-wide sensemaking: "what themes run
  through these documents." **The semantic layer cannot answer this class of
  question at all** — it has no aggregate over unstructured text. This is the
  strongest single argument for the integration.

### Structured context, not a chunk list

`assemble_graph_context` returns MS-GraphRAG-style sections — Entities |
Relationships | Community Reports | Source Text — each trimmed to a token budget.
For an agent consumer this is a better payload than raw chunks, and it is what
the MCP context tool should return.

### Retrieval trace

`search(return_trace=True)` returns a `RetrievalTrace` carrying
`final_provenance`: which dimension produced each returned chunk. This is
retrieval lineage, and it belongs in `/admin/observability` next to query
observability (REQ-1160/1161) rather than being discarded. It is also the
diagnostic for a bad cohort — a high `entity_adjacent` share at low scores means
the entity lane is over-expanding.

### LLM namespace routing, subordinate to entitlement

`namespace_filter_llm_fn` / `domain_filter_llm_fn` ask an LLM to pick relevant
namespaces from their descriptions, which chonk reports as the highest-leverage
accuracy lever on a cross-domain corpus. Provisa already computes domain
descriptions (`provisa/api/mcp/tools.py:62`).

The wiring has a constraint: chonk only runs the routing callable when
`namespaces` is `None`, and Provisa must always pass an explicit entitled set.
So **Provisa runs the routing itself, over the entitled set, and passes the
intersection.** Routing narrows within what the caller may see; it can never
widen. The routing callable is never handed the full namespace list.

### Sovereign perimeter

`redaction_filter` is documented as the trust boundary for the sovereign pattern:
a frontier model plans, a sovereign model retrieves and generates, and only
filtered answer text crosses the perimeter — raw chunks never leave. This lines
up with `provisa/security/high_security.py` and should be wired to it rather than
reinvented, for deployments that plan with an external model.

## MCP tool expansion

Provisa's MCP server (`provisa/api/mcp/tools.py`) exposes relational tools:
`list_schemas`, `list_tables`, `describe_table`, `run_sql`, `explain_sql`,
`list_metrics`, `query_metric`, `search_catalog`, `search_terms`.

The chonk tools mirror the real retrieval API rather than flattening it to one
search box. Every one resolves namespaces from the caller's domains, denies on
empty, and goes through the same `require_role` / `_role_domains` path as the
relational tools. None accepts a namespace argument from the caller.

- **`search_documents`** — cohort search. Exposes `k`, `query_text` (BM25 lane),
  and `mode`. Returns chunks with their provenance dimension.
- **`explore_topics`** — `mode="global"` plus `map_reduce_global_context`.
  Corpus-wide themes. The tool with no relational equivalent.
- **`trace_connections`** — `mode="graph_first"`. How entities relate, rather
  than what mentions them.
- **`document_context`** — `assemble_graph_context` for a query: entities,
  relationships, community reports and source text within a token budget.
- **`search_entities`** — matched entities and the chunks they appear in.
- **`list_documents`** — documents in reach, with version state.
- **`index_status`** — last build, staleness, per-namespace document counts.

Two things to carry into the tool descriptions rather than lose: chonk's
docstrings contain calibrated agent guidance (issue one atomic sub-query per
call; name entities explicitly; state the answer type; widen `k` before
concluding evidence is absent) — that guidance is what makes the difference
between good and poor retrieval, and MCP tool descriptions are where an agent
will actually read it.

## chonk issues to fix

Tracked in [chonk-concerns.md](chonk-concerns.md) — eight findings from code
review, with the file and line establishing each.

Two are blocking security defects on paths this design depends on: SQL injection
in the BM25 lane (`chonk/storage/_pg.py:637-651`), which every search turns on,
and `ask()` silently bypassing `chunk_filter` (`chonk/search/_enhanced.py:875-885`),
which is the RLS hook. Two more break the refresh contract in opposite
directions: unchanged documents are re-embedded on every build, and changed
remote documents are never re-crawled.
