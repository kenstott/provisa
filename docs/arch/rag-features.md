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
| `entity` column tag | Tag registry (REQ-1414) | `NerPipeline.add_from_db` vocabulary query |
| `natural_language` column tag | Tag registry (REQ-1414) | `loader.load_from_db` per-row documents |
| Document folder URN | Sources page, `kind = documents` | a crawled source in the namespace build |
| Store / loader / index / interval | Admin tab | `ChonkConfig.store`, `.loader`, `.index`, refresher interval |
| Embedding model | Existing `/admin/ai-models` tab | `EmbedConfig.model` |

`ChonkConfig.sources` and `ChonkConfig.namespaces` are **derived and read-only** —
they are computed from the registered document URNs and the org's domains. The
user never edits them.

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

**`entity` tag** → an entity-vocabulary query. Single column, no primary key,
`SELECT DISTINCT`. Fed to `NerPipeline.add_from_db`, which accumulates into
`self._data_terms` in memory (`chonk/ner/_schema_vocab.py:406-409`) and never
persists. Only *matched* entities are written
(`chonk/ner/_build.py:206-246`), and a match requires the text to already be in
that namespace — so the vocabulary itself discloses nothing. It is rebuilt live
at every namespace build.

**`natural_language` tag** → per-row documents. Primary-key bearing, one document
per row, named `schema.table.column/<pk>` with SQL
`SELECT column FROM schema.table WHERE pk = '<pk>'`. Fed to
`loader.load_from_db` (`chonk/loader.py:260-315`), which makes each query a
separate document keyed by the dict key.

## Authorization is domain-grain

The caller's allowed domains — read from `user_role_assignments`, with `*`
expanded server-side — become the `namespaces` argument on every chonk search.

Rules:

- **Pre-filter, never post-filter.** The namespace list is an argument to the
  search, not a mask applied to results.
- **An empty set denies.** It must never degrade to `None`: omitting `namespaces`
  searches *everything* (`mcp_chonk_server.py:479-486`). The inverted default is
  the hazard.
- `CHONK_DB_CONFIG` stays a single entry.

Consequence worth stating plainly: **tagging `natural_language` on an
RLS-protected table is a declassification.** Row-level rules are lost at ingest;
what survives is domain-grain reachability. That is the intended trade, but it
should be surfaced at tag time.

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

## MCP tool expansion

Provisa's MCP server (`provisa/api/mcp/tools.py`) exposes relational tools:
`list_schemas`, `list_tables`, `describe_table`, `run_sql`, `explain_sql`,
`list_metrics`, `query_metric`, `search_catalog`, `search_terms`.

Chonk capabilities to add alongside them, each taking the caller's allowed
domains as `namespaces` and denying on empty:

- **`search_documents`** — semantic / hybrid chunk search over the namespaces.
- **`list_documents`** — registered documents in reach, with version state.
- **`get_document`** — fetch chunks for one document by name.
- **`search_entities`** — matched entities, and the chunks they appear in.
- **`list_communities`** — community clusters, for topic-level navigation.
- **`index_status`** — last build, staleness, per-namespace document counts.

Design constraints carried over: same `require_role` / `_role_domains` path as
the relational tools, no separate auth story, and no tool that accepts a
namespace argument from the caller.

## chonk issues to fix

Tracked here rather than in the chonk repo so the Provisa dependency is visible.

| # | Issue | Status |
| --- | --- | --- |
| 1 | **Document versioning is not wired in.** `sync_document` and `register_document` (`chonk/storage/_vector.py:655-775`) are correct and have *no internal caller*. `indexer.py:_run` crawls and calls `add_document` without ever consulting `content_hash`, so every build reindexes every document. This breaks the stated contract: index once, recheck versions on a schedule, and on change remove the original entries and reindex. | open |
| 2 | **Row limit on the entity query.** `add_from_db` defaulted to `row_limit=10_000`. Deduplication happens in Python *after* the fetch (`_schema_vocab.py:74-104`), so the cap truncated raw rows rather than distinct terms — worst on a low-cardinality column over a large table, exactly the case where the vocabulary should be complete and small. A truncated vocabulary weakens matching silently instead of failing. | fixed — default is now `None` (uncapped) in `_schema_vocab.py` and `_pipeline.py` |
| 3 | **Remote-scheme freshness is skipped.** `namespace_cache_valid` (`chonk/storage/_store.py:525-568`) skips the mtime check for `http`, `https`, `github`, `s3`, `ftp`, `sftp`. Needs an ETag or HEAD check instead of an unconditional skip. | open |
| 4 | **`hdfs://` missing from that prefix list.** Absent from the skip list *and* unable to answer an mtime probe, so an HDFS source rebuilds on every pass. | open |
| 5 | **SQL injection in `_search_hybrid()`** (`chonk/storage/_pg.py`). Critical. | open |
