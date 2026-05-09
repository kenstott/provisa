---
name: domain-model
description: Provisa domain knowledge and conceptual model. Auto-triggers when working with core business logic.
---

# Provisa Domain Model

## What Provisa Does
Provisa is a governed data provisioning and GraphQL query layer. It compiles GraphQL queries to SQL, executes them against registered data sources via Trino (cross-source) or direct RDBMS connections (single-source), and enforces a pre-approval security model where production queries must be authorized before they can execute.

## Core Concepts

### Pre-Approval Model
Production queries against non-pre-approved tables must be members of the persisted query registry. No runtime evaluation — the query was already compiled, validated, and authorized. Mutations and pre-approved table queries are governed by user rights alone.

### Three-Phase Workflow
```
Development → Authorization → Production
```
1. **Development** — Developer builds queries in GraphQL builder against test endpoint with full guards
2. **Authorization** — Data steward reviews and approves/rejects queries for production use
3. **Production** — Clients submit pre-approved query identifiers; system executes pre-compiled operations

## Component Stack
| Component | Role |
|-----------|------|
| GraphQL Compiler | Purpose-built two-pass compiler: SDL generation + query compilation |
| Trino | Schema introspection + federated cross-source read execution |
| SQLGlot | PG-style SQL → target dialect transpilation |
| Direct RDBMS | Single-source reads + all mutations |

## Registration Model
- **Sources** — registered via Trino dynamic catalog API
- **Tables** — registered by stewards with column visibility per role and governance mode (pre-approved or registry-required)
- **Relationships** — inferred from FK metadata or manually defined; encode navigable connections between tables

## Security Layers
| Layer | Enforcement |
|-------|-------------|
| Pre-approval | Query must be in registry or target pre-approved tables |
| Schema visibility | SDL generated per role; excluded columns invisible |
| SQL enforcement | RLS WHERE clauses + column stripping injected at execution |

## Execution Routing
- **Single-source** → direct RDBMS connection (SQLGlot transpiles to target dialect)
- **Cross-source** → Trino (SQLGlot transpiles to Trino SQL)
- **Mutations** → always direct RDBMS, never Trino
- **Large results** → redirect to blob storage with presigned URL

## Client Entry Points
1. **GraphQL endpoint** — primary interface for queries and mutations
2. **Presigned URL redirect** — large result delivery via blob storage
3. **gRPC Arrow Flight** — high-throughput columnar streaming

## Key Abstractions
- Registration model is source of truth for schema, not the database
- Compiler has no third-party GraphQL framework dependency
- PG-style SQL is the canonical intermediate representation
- User rights and query governance are orthogonal enforcement layers
