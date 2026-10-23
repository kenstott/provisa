# Ontology

The ontology is a living vocabulary over your data model. Every physical column in the model resolves to a term, and columns that carry the same concept share one term, however differently they spell it. Each term can hold a definition, a set of typed relationships to other terms, and a list of subject-matter experts who own its meaning.

It is a graph of concepts, their definitions and the relationships between them. Provisa maps it to the database structures that hold the values, which is what lets it answer questions about your data. A concept is a draft until it reaches data, directly or through other concepts. The ontology is separate from the metrics: a term says what a concept is, a metric says how a number is computed.

That shared vocabulary connects business language to physical data. An AI agent that knows "customer" names every column carrying a customer identifier doesn't have to guess which of `cust_id`, `customerId` and `CUSTOMER_KEY` is the right one. They all resolve to the same term, and the term carries the definition.

## How terms are derived

Provisa initially creates a draft term from every column name automatically, using a deterministic
normalization rule: case folding, separator and camelCase tokenization, abbreviation expansion, and
stripping of trailing proxy tokens.

**Abbreviation expansion** maps common enterprise shorthands to their full forms: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` and so on. Both `id` and `key` expand to `identifier`. The table is fixed and cautious: ambiguous shorthands like `st`, `min` and `no` stay as written, so nothing is guessed wrong.

**Proxy-token stripping** removes a trailing `identifier`, `code`, `index`, or `reference` token.
A column named `cust_id` is not naming the identifier itself; it is naming a customer through a
surrogate value. Stripping the proxy lands both `cust_id` and `customerId` on the term `customer`.
Only trailing tokens strip, and never the last remaining token: a bare `id` column expands to
`identifier` and stays there.

**Deduplication** is the point. The normalization rule is deterministic, so `cust_id`, `customerId` and `CUSTOMER_KEY` all produce `customer`. Each column gets a ref on that one term, and curation has a single place to add the definition instead of three.

### Generic phrases

Some normalized phrases are too generic to be a concept on their own. A bare `name`, `date` or `identifier` column names an attribute of its table's concept, which doesn't exist apart from that table. Employees have names, and products have names, but they aren't the same thing.

When a phrase falls in the generic set and a table context is available, the term qualifies to
`<table concept> <phrase>`: `employees.first_name` normalizes to `employee first name`, and
`orders.id` normalizes to `order`, because the proxy strip then collapses the qualified phrase
onto the concept it identifies. That last case is important: the primary key of `orders` and every
foreign key `order_id` on other tables all land on `order`, with no extra curation needed.

The generic set covers attribute nouns (`name`, `date`, `status`, `type`, `amount`, `quantity`),
audit-trail phrases (`created_at`, `modified_by`, `submitted_timestamp`), and a handful of others
that appear on nearly every table.

### The business name, not the physical name

A derived term follows the column's **business name**: its alias when the modeler set one, and its physical name otherwise. When `usr_nm` is aliased to `user name`, the derived term is `user name`, where expanding `usr_nm` might have given `user number`.

Aliasing a column is the stronger correction. An alias travels to every surface that reads the column (SQL, GraphQL, AI agents, the catalog), so the model describes itself correctly everywhere. Renaming the term fixes one catalog entry and leaves the column reading `usr_nm` for the next reader. The proposed-term banner in the UI says so: alias the column first, and rename the term only when the column name is right and the vocabulary isn't.

Re-aliasing a column re-derives its proposed term, so the ontology follows the model and never asks for the same correction twice. Once a curator has added a definition, a relationship or an expert to a term, an alias edit leaves the ref where it is, because that work is the curator's and it stays.

### Access-path table names

Some table names describe an access path instead of a concept: `user_by_name` is a user reached through a name lookup, and no separate kind of entity. When Provisa derives the table concept for qualifying generic phrases, it cuts the name at the connective, so `user_by_name` becomes `user` and `orders_by_customer` becomes `order`.

Without the cut, the surrogate key on `user_by_name` would normalize to `user name` and collide with the real `users.name` attribute, giving one term that holds both a thing and one of its own fields. The cut applies to table concepts only. In a column name, `by` is part of the compound noun, so `pet_by_name` and `pet_name` normalize to the same term, `pet name`.

## What makes a term curated

A term born from column normalization starts out blank, as a proposal that isn't vocabulary yet. It becomes curated when any of the following is true:

- A definition has been saved.
- A relationship edge has been added.
- A subject-matter expert has been assigned.
- A curator has manually retired it.

Curation matters for the term's lifecycle. When a curated term's last physical column leaves the model, the term is deprecated and kept: it goes out of service, keeps the content its editors supplied, and comes back automatically if the same column reappears. An uncurated term with no columns left is simply removed.

## Re-syncing from tables

Every time a table is saved or reloaded, `sync_table_refs` reconciles that table's columns against
the existing refs. New columns create-or-link terms; departed columns drop their refs; and the
remove-or-deprecate rule settles any term that loses its last ref.

Only uncurated terms are re-derived. If you aliased a column and the proposed term now differs, the ref moves to the new term. If the term is curated, the link stays, because an alias edit doesn't override the curator's choice of term.

An abstract term whose only path to physical data ran through a departing term is deprecated rather
than removed, preserving the conceptual structure until it is rewired.

## Relationships

Terms relate to other terms through typed edges. The supported relationship types are:

| Type | Meaning |
| --- | --- |
| `KIND_OF` | The source term is a kind of the target term. |
| `PART_OF` | The source term is a component of the target term. |
| `SYNONYM_OF` | The two terms are interchangeable in this domain. |
| `RELATED_TO` | A loose association — no stronger claim fits. |
| `VALID_VALUE_OF` | The source is an allowed value of the target enumeration or domain. |
| `DERIVED_FROM` | The source is computed or sourced from the target. |
| `REPLACES` | The source supersedes the deprecated target. |
| `PREFERRED_TERM_FOR` | The source is the preferred term over the discouraged target. |
| `TRANSLATION_OF` | The source is a locale or language translation of the target. |
| `ANTONYM_OF` | The source is the semantic opposite of the target. |

Relationships are directional. The UI shows both outgoing edges (this term → another) and incoming
edges (another term → this term), labeling each direction with its own plain-language phrase.

The edges live in `glossary_term_edges`, an associative table declared as a junction relationship: its `rel_type` column is the discriminator, so each of the types above is a separate Cypher relationship type between two `GlossaryTerm` nodes rather than a property on a reified node. The table is provisioned with the rest of the metadata schema and is not shown as a node in graph clients: it is the edge. Nothing about it is ontology-specific: it is declared the same way you would declare a junction over your own tables, and it is read by the same code.

## Abstract terms

An abstract term has no physical column refs of its own. Use one for a business concept that spans multiple concrete terms: an umbrella you then wire to the specific terms that do hold columns. `revenue`, for example, might be abstract, with `PART_OF` edges from `order amount`, `adjustment amount`, and `refund amount` pointing to it.

An abstract term that can't reach any physical column through the relationship graph is a dangling proposal. It doesn't appear in agent term search or in metadata export, because a term that names no data can't answer anything.

## The admission rule for consuming surfaces

A term a consuming surface may offer must satisfy three conditions:

1. **In service** — not retired (a curator removed it from service) and not deprecated (it lost its
   last column and was held only because deleting it would leave something dangling).
2. **Defined** — it carries a definition. A term derived from a column name is a token, not a
   meaning. Without a definition, it is a proposal awaiting a curator, never vocabulary an agent
   can ground a question on.
3. **Grounded** — connected, over in-service terms, to at least one term that holds a physical
   column ref. The ontology is an entry point into the data, so every chain must terminate at a
   column.

Connectivity spreads through the graph: an abstract term reaches data through any in-service neighbor that does. Terms that are out of service don't conduct, so a retired term doesn't keep its dependents alive.

## Metadata export

The ontology publishes to external data catalogs as part of metadata export. The same admission rule applies, narrowed in one way: a term's rootedness is judged only against columns that actually publish. A term whose columns are all withheld from export, because their tables aren't marked as data products or because technical filters exclude them, isn't rooted for export, even if it holds refs in the control plane.

Relationship edges publish only when both endpoint terms publish.

Column assets export independently. A term being excluded does not hide the underlying data.

### Excluding a term from export

Some columns carry plumbing rather than business data: ETL batch identifiers, row versions,
ingestion timestamps. A term derived from such a column may have a perfectly accurate definition
that is simply not business vocabulary. The **Exclude from metadata export** control
withholds the term and any relationship edges that end on it from the catalogs Provisa publishes
to, while the columns themselves still export as assets.

What counts is whether the business uses the word; how good the definition is doesn't decide it. An ETL batch identifier has a clear meaning that belongs in the ontology for engineers, but it doesn't belong in a business catalog next to `customer` and `revenue`.

## Working with the ontology

Open **Ontology** in the UI's navigation bar. The left panel lists every term; click one to open its detail
view. From there:

- **Rename** the term to change its wording without moving its columns.
- **Add a definition** by typing one or clicking the AI draft button to generate a starting point
  from the term's name, its physical columns, and its relationships. The draft is not saved until
  you confirm it.
- **Move a ref** to consolidate two terms: pick the target term from the dropdown next to any
  physical ref. If the source term loses its last ref, it is settled under the remove-or-deprecate
  rule automatically.
- **Add a relationship** between this term and another, choosing the type from the closed set.
  Retype an existing edge in place rather than deleting and re-adding it.
- **Assign experts** by user ID, with a kind of `expert` or `author`.
- **Retire** a term to take it out of service. It keeps its columns and remains editable here, but
  agent term search and metadata export both skip it. Restore it later if the concept comes back.
- **Bulk-generate definitions** (**Definitions** in the ✨ menu) to fill every blank definition in one pass. Only empty definitions
  are written; human text is never overwritten.
- **Bulk-generate relationships** (**Relationships** in the ✨ menu) to propose typed edges across the full term list. Malformed
  proposals — unknown term names, self-edges, unrecognized types — are dropped automatically.

The **Proposed** banner on a term with no definition tells you whether the term is undefined
(alias the column or add a definition) or ungrounded (relate it to a term that has columns).
When you see it, the term is not yet reachable by agents or catalogs.

## See also

- [Metadata Export](metadata-export.md) — how terms and relationships publish to external
  data catalogs, including which terms the export admission rule admits.
- [Column-Level Lineage](lineage.md) — the lineage explorer and how `columnDependents`
  reports ontology bindings as dependents of a physical column.
