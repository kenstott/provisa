# chonk — code review concerns

Findings from reading chonk source, not its documentation. Each item cites the
file and line that establishes it. Companion to [rag-features.md](rag-features.md),
which covers the integration design; this document covers what has to be true of
chonk before that design can ship.

Reviewed against `chonk` at `d03adaef` (2026-06-20), working tree, at
`/Volumes/main/Users/kennethstott/PycharmProjects/chonk`.

## Summary

| # | Finding | Severity | Blocks integration |
| --- | --- | --- | --- |
| 1 | SQL injection in `_search_hybrid()` | Critical | yes |
| 2 | `ask()` silently bypasses `chunk_filter` | Critical | yes — this is the RLS hook |
| 3 | Document versioning is write-only; every build re-embeds everything | High | yes |
| 4 | Remote sources are never re-crawled | High | yes |
| 5 | `hdfs://` rebuilds on every pass | Medium | no |
| 6 | Refresh loop swallows failures with no log | Medium | no |
| 7 | Entity vocabulary query was truncated at 10 000 rows | Medium | fixed |
| 8 | Indexer phase failures are indistinguishable from empty sources | Low | no |

---

## 1. SQL injection in `_search_hybrid()` — Critical

`chonk/storage/_pg.py:637-651`. The caller's query text is interpolated into SQL
after single-quote doubling:

```python
safe_query = query_text.replace("'", "''")
bm25_where = (...) + f"fts_vec @@ plainto_tsquery('english', '{safe_query}')"
```

and again inside the `ts_rank(...)` ORDER BY at `:648`.

Escaping by doubling is the wrong mechanism, and the same function shows the
right one twelve lines earlier: the vector lane at `:623-633` passes
`query_vec` and `candidate_limit` as `%s` parameters. Quote doubling does not
hold under `standard_conforming_strings = off`, and it is unnecessary when the
driver already parameterises.

**Why it matters for Provisa specifically.** `query_text` is caller-supplied and
reaches this function on the BM25 lane — which the integration design turns *on*
for every search, because BM25 is what anchors exact names and identifiers. The
injection sits directly under an MCP tool argument.

**Fix.** Pass `safe_query` as a bound parameter in both positions and drop the
`replace()`. Parameter ordering needs care since `filter_params` is already
positional.

## 2. `ask()` silently bypasses `chunk_filter` — Critical

`chonk/search/_enhanced.py:875-885`:

```python
# Bypass chunk_filter — ask() applies redaction_filter on the generated Answer instead.
chunks = self.search(..., _bypass_chunk_filter=True)
```

`chunk_filter` is the hook the integration uses for the row-grain RLS re-check.
`ask()` disables it by design and offers `redaction_filter` in its place — but
`redaction_filter` is optional and defaults to `None` (`:889-891`), so
constructing `EnhancedSearch(chunk_filter=rls_check)` and calling `ask()` returns
an answer generated from unfiltered chunks with no error and no warning.

Even when `redaction_filter` is set it is not a substitute. It receives generated
prose and can only pattern-match over it; it cannot make an authorization
decision about which chunk a sentence came from, and it cannot restore a
suppression that should have happened before generation.

**Fix in Provisa, not chonk.** Do not call `ask()`. Call `search()`, where
`chunk_filter` always runs, and own generation. This is already the design in
[rag-features.md](rag-features.md) — recorded here because the failure is silent
and someone will reach for `ask()` as the convenient one-shot call.

**Fix in chonk, if upstreamed.** Raise when `chunk_filter` is set and `ask()` is
called without a `redaction_filter`, rather than degrading quietly.

## 3. Document versioning is write-only — High

The pieces exist and are correct. `get_document_hash`, `register_document` and
`sync_document` (`chonk/storage/_vector.py:655-775`) implement content-hash
versioning and return `skipped` / `added` / `updated`. Nothing reads them to make
a decision.

`sync_document` has no internal caller anywhere in chonk — every reference is an
export or a docstring example.

The queue worker (`chonk/_ingest_worker.py:66-74`) computes the hash *after*
loading, chunking and embedding, then registers it:

```python
content_hash = hashlib.sha256(...)
emb = _embed_texts([c.content for c in chunks], embed_model, batch_size)
backend.add_chunks(chunks, emb, namespace=namespace)
backend.register_document(chunks[0].document_name, content_hash, ...)
```

The hash is recorded and never consulted. The expensive step — embedding — has
already run.

The main indexer path (`chonk/indexer.py:134-215`) is worse: it crawls, embeds
every chunk unconditionally, and calls `add_document`. It never calls
`register_document` at all, so on that path no version record exists.

**Impact.** The contract the integration assumes — index a document once, recheck
versions on a schedule, and on change remove the original entries and reindex —
does not hold. Every build re-embeds the whole corpus. This is the single largest
running cost of the integration, and it is entirely avoidable with code already
written.

**Fix.** In the indexer crawl loop, per document: resolve the content hash (or an
ETag / Last-Modified, which `sync_document` already accepts in place of bytes),
call `sync_document`, and skip embedding on `skipped`. On `updated`, delete the
document's existing chunks before adding.

## 4. Remote sources are never re-crawled — High

`chonk/storage/_store.py:548-558`:

```python
if not any(uri.startswith(p) for p in
           ("http://", "https://", "github://", "s3://", "ftp://", "sftp://")):
    try:
        mtime = os.path.getmtime(uri)
        if mtime > last_crawled.timestamp():
            return False
    except OSError:
        return False
```

For every listed scheme the freshness check is skipped entirely, so the source is
treated as current forever. A changed remote document is never picked up. The
cache is only invalidated by a local file's mtime.

This is not a churn problem — it is a staleness problem, and it is silent. The
namespace reports as valid while serving content that no longer matches the
source.

**Fix.** Replace the skip with a scheme-appropriate freshness probe: HTTP `HEAD`
for ETag or `Last-Modified`, `HeadObject` for S3, the commit SHA for `github://`.
`sync_document` already takes an ETag as the content hash, so the two fixes
compose — finding 3 provides the mechanism this one needs.

## 5. `hdfs://` rebuilds on every pass — Medium

Same block. `hdfs://` is absent from the prefix list, so `os.path.getmtime()` is
called on an HDFS URI, raises `OSError`, and the `except` returns `False` —
cache invalid, unconditionally, every time.

The two branches produce opposite failures: a scheme in the list is never
refreshed, a remote scheme missing from it is always rebuilt.

**Fix.** Folded into finding 4 — a real freshness probe per scheme removes both
the allowlist and its gap.

## 6. Refresh loop swallows failures with no log — Medium

`chonk/lifecycle.py:222-237`:

```python
try:
    global_db_path = self._db_path_fn("global")
except Exception:
    return
...
except Exception:
    return
```

Two bare `except Exception: return` blocks with no logging. `_loop`
(`chonk/lifecycle.py:217-219`) then waits out the interval and tries again.

With the default `interval_seconds = 3600`, a persistent failure — a moved DB
path, a permissions change — means the corpus silently stops refreshing and
nothing surfaces it. The next signal is a user noticing stale answers.

**Fix.** Log at error level with the exception before returning. The
`index_status` MCP tool planned in [rag-features.md](rag-features.md) should also
report last-successful-refresh so the silence is visible from outside.

## 7. Entity vocabulary truncated at 10 000 rows — Medium, fixed

`add_from_db` defaulted to `row_limit: int = 10_000` and wrapped every query
unconditionally:

```python
limited = f"SELECT * FROM ({sql}) _q LIMIT {row_limit}"
```

Deduplication happens in Python *after* the fetch (`chonk/ner/_schema_vocab.py:74-104`),
so the cap truncated raw rows, not distinct terms. The damage is worst on a
low-cardinality column over a large table — exactly the case where the
vocabulary should have been complete and small. A partial vocabulary weakens
entity matching silently rather than failing, and entity matching feeds the
entity-adjacent retrieval lane and the co-occurrence graph, so the loss
propagates past NER into ranking.

**Fixed** in this working tree: default is now `row_limit: int | None = None`
(uncapped) in `chonk/ner/_schema_vocab.py:368,406-407` and
`chonk/ner/_pipeline.py:155`. A caller may still cap deliberately.

**Outstanding.** `README.md:1945` still documents the old default
(`# max rows per query (default 10 000)`). The `training/` JSONL corpora also
carry the old signature; those are training data, not source, and are out of
scope.

## 8. Indexer phase failures look like empty sources — Low

`chonk/indexer.py:158-161, 179-181, 200-208`. Each phase catches `Exception`,
calls `self._on_error(phase, exc)` and `return 0`.

The callback fires, so the failure is not fully lost — but the return value is
`0`, identical to a source that legitimately produced no chunks. A caller
counting indexed chunks to decide whether a namespace built correctly cannot
distinguish "this folder is empty" from "the crawl threw."

**Fix.** Raise after the callback, or return a result type that carries the
phase outcome. For the integration, `index_status` should read the error
callbacks rather than infer health from chunk counts.

---

## Consequence for the integration

Findings 1 and 2 are security defects on the exact paths the design depends on —
the BM25 lane and the `chunk_filter` hook. Neither can ship unfixed.

Findings 3 and 4 together mean the stated refresh contract does not hold in
either direction: unchanged documents are re-embedded on every build, and changed
remote documents are never re-crawled at all.

Adopting chonk is therefore not "integrate a library." It is taking ownership of
a codebase, and the integration estimate should carry that cost explicitly.
