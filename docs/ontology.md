# Ontology

The ontology is a living vocabulary over your data model. Every physical column resolves to a term, and columns that carry the same meaning share one term, however differently they spell it. Each term can hold a definition, typed relationships to other terms, a list of subject-matter experts, and tags such as `pii` that every column with that meaning carries.

It is a graph of concepts, their definitions and the relationships between them. Provisa maps it to the tables and columns that hold the values, which is what lets it answer questions about your data. A term is a draft until it has a definition and reaches data. The ontology is separate from the metrics: a term says what a concept is, a metric says how a number is computed.

That shared vocabulary connects business language to physical data. An AI agent that knows "customer" names every column carrying a customer identifier doesn't have to guess which of `cust_id`, `customerId` and `CUSTOMER_KEY` is the right one. They all resolve to the same term, and the term carries the definition.

## Classes, properties and concepts

Every term has exactly one role, derived from what it grounds. You never set it.

| Role | Grounded in | Example |
| --- | --- | --- |
| **Class** | One or more tables or views. Its instances are their rows. | `customer`, grounding `customers` and `crm_contacts` |
| **Property** | Columns that are not keys. | `customer email`, holding `customers.email` |
| **Concept** | Nothing directly. It reaches data through its relationships. | `revenue`, with `PART_OF` edges from `order amount` and `refund amount` |

A term is one or the other. Provisa refuses to ground a table on a term that holds non-key columns, and refuses to move a non-key column onto a term grounded in tables. Each refusal names the term and what it already holds.

### Every keyed table proposes a Class

When a table with a primary key is saved, Provisa grounds it in a draft Class term named for the table's concept: `orders` proposes `order`, `order_lines` proposes `order line`. Tables of the same concept share one Class. A table without a primary key can't be a Class, and grounding one is refused, naming the table.

A grounding you make yourself, in the UI, in configuration or through Polly, is curated. Sync never moves it, and grounding a table on a curated term replaces its draft proposal. When a concept is already a Property with non-key columns, its tables get no proposal; the ontology lists them under **To review** instead.

### Keys belong to the Class

A primary key has no term of its own. Each of its columns, the parts of a composite key included, puts its reference on its table's Class.

A foreign key works the same way. Every column of a declared relationship's many side puts its reference on the target table's Class, whatever the column is called. `manager_id` on `employees`, pointing back at `employees`, lands on `employee`; the role "manager" lives on the relationship's name. A column with no declared relationship is a Property, even when its name matches a Class.

Provisa places key references by derivation alone, whenever a table syncs, a relationship changes, or a table's Class changes. You can't move a key column by hand, and the refusal names the column, its table and its Class. Ground a table on another term and its keys follow at once.

### The same entity in several tables

Instances from different tables are distinct until you say otherwise. When `customers` and `crm_contacts` both describe one customer, mark the relationship joining them **same entity**. It must join two tables the same Class grounds, one-to-one, on a key of each: the primary key or a declared unique constraint.

While a same-entity relationship stands on a table, its Class can't be withdrawn or replaced. Unmark the relationship first.

A Class grounded in several tables that same-entity relationships don't all join is flagged under **To review** as *unlinked tables*.

## How terms are derived

Provisa creates a draft term from every column name automatically, using a deterministic normalization rule: case folding, separator and camelCase tokenization, abbreviation expansion, and stripping of trailing proxy tokens.

**Abbreviation expansion** maps common enterprise shorthands to their full forms: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` and so on. Both `id` and `key` expand to `identifier`. The table is fixed and cautious. Ambiguous shorthands like `st`, `min` and `no` stay as written, so nothing is guessed wrong.

**Proxy-token stripping** removes a trailing `identifier`, `code`, `index`, or `reference` token. A column named `cust_id` doesn't name the identifier itself; it names a customer through a surrogate value. Stripping the proxy lands both `cust_id` and `customerId` on the term `customer`. Only trailing tokens strip, and never the last remaining token, so a bare `id` column expands to `identifier` and stays there.

**Deduplication** is the point. The rule is deterministic, so `cust_id`, `customerId` and `CUSTOMER_KEY` all produce `customer`. Each column gets a reference on that one term, and curation has a single place to add the definition instead of three.

Key columns skip this rule. Their references go to a Class, as described above.

### Generic phrases

Some normalized phrases are too generic to be a concept on their own. A bare `name`, `date` or `identifier` column names an attribute of its table's concept, which doesn't exist apart from that table. Employees have names, and products have names, but they aren't the same thing.

When a phrase falls in the generic set and a table context is available, the term qualifies to `<table concept> <phrase>`: `employees.first_name` normalizes to `employee first name`.

The generic set covers attribute nouns (`name`, `date`, `status`, `type`, `amount`, `quantity`), audit-trail phrases (`created_at`, `modified_by`, `submitted_timestamp`), and a handful of others that appear on nearly every table.

### Terms follow the business name

A derived term follows the column's **business name**: its alias when the modeler set one, and its physical name otherwise. When `usr_nm` is aliased to `user name`, the derived term is `user name`, where expanding `usr_nm` might have given `user number`.

Aliasing a column is the stronger correction. An alias travels to every surface that reads the column (SQL, GraphQL, AI agents, the catalog), so the model describes itself correctly everywhere. Renaming the term fixes one catalog entry and leaves the column reading `usr_nm` for the next reader. The Draft banner in the UI says so: alias the column first, and rename the term only when the column name is right and the vocabulary isn't.

Re-aliasing a column re-derives its draft term, so the ontology follows the model and never asks for the same correction twice. Once a curator has added a definition, a relationship, an expert or a tag to a term, an alias edit leaves the column where it is, because that work is the curator's and it stays.

### Access-path table names

Some table names describe an access path instead of a concept: `user_by_name` is a user reached through a name lookup, and no separate kind of entity. When Provisa derives a table's concept, it cuts the name at the connective, so `user_by_name` becomes `user` and `orders_by_customer` becomes `order`.

Without the cut, `user_by_name` would propose a Class `user name` that collides with the real `users.name` attribute. The cut applies to table concepts only. In a column name, `by` is part of the compound noun, so `pet_by_name` and `pet_name` normalize to the same term, `pet name`.

## Status

A term has one status at a time.

| Status | Meaning | How it gets there |
| --- | --- | --- |
| **Draft** | Not available to agents, search or catalogs until it has a definition and reaches data. | Every derived term starts here. |
| **Live** | Admitted: offered to agents, search and catalogs. | Defined and grounded, and in service. |
| **Deprecated** | On its way out. | A curator deprecates a live term, or a curated term loses its last column. |
| **Retired** | Out of service, kept as a record. | A curator retires it and names a successor. |

### The admission rule

A term is live when it meets three conditions.

1. **In service.** It isn't retired, and it hasn't been deprecated by losing its last column. A term a curator deprecated stays in service until its planned retirement.
2. **Defined.** It carries a definition. A term derived from a column name is only a token. Without a definition it is a draft awaiting a curator, never vocabulary an agent can ground a question on.
3. **Grounded.** It is connected, over terms in service, to a term holding a column or a table. The ontology is an entry point into the data, so every chain must end at the data.

Connectivity spreads through the graph. A Concept reaches data through any in-service neighbor that does. Terms out of service don't conduct, so a retired term doesn't keep its dependents alive.

### Deprecating a term

Deprecate a live term when it is on its way out but still in use. You give a reason, which is required, and optionally a planned retirement date, today or later. The term stays in service, and keeps conducting the admission rule, until that date. Then the scheduler retires it.

A term due for retirement that still holds columns or tables can't be retired by date, because only a successor can take them. It waits, and **To review** lists it as *retirement blocked* until you retire it with a successor.

A curated term that loses its last column is deprecated automatically and kept. It goes out of service, keeps the content its editors supplied, and comes back if the same column reappears. An uncurated term with no columns left is removed.

### Retiring a term

Retiring a term moves everything attached to it to a **successor**, then takes it out of service.

- A Property's columns move to the successor. It must be a Property or a Concept.
- A Class's tables move to the successor, and their keys follow them. It must be a Class or a Concept.
- A Concept holds nothing, so its successor is optional.

The retirement needs a reason, and it writes a `REPLACES` edge from the successor to the retired term. The retired term stays as an inert record: its definition, tags, relationships, experts and the reason. It conducts nothing.

Because its columns now belong to the successor, a retired term's panel shows what it **last applied to**: its columns and tables as the model held them in the commit before the retirement. Its panel also links to the term that replaces it.

### Restoring a term

**Restore** undoes what a curator asserted. A deprecated term is no longer deprecated. A retired term comes back as an empty Concept and a draft, since its columns went to its successor, and its `REPLACES` edge is removed.

### What makes a term curated

A term born from column normalization starts out blank. It becomes curated when it has any of these:

- a saved definition
- a relationship edge
- an assigned subject-matter expert
- a tag, such as `pii`
- a retirement

Curated terms are kept when their columns leave. Sync never re-derives a curated term's columns elsewhere.

## Re-syncing from tables

Every time a table is saved or reloaded, Provisa reconciles that table's columns against the ontology. New columns create or link terms, departed columns drop their references, and any term left with no references is removed or deprecated by the rule above. Key columns go to their Class, and a keyed table without a Class proposes one.

Only uncurated terms are re-derived. If you alias a column and its draft term changes, the column moves to the new term. If the term is curated, the column stays, because an alias edit doesn't override a curator's choice.

When removing a term would leave a Concept with no path to data, Provisa deprecates that term instead, so the structure you built survives until you rewire it.

## Relationships

Terms relate to other terms through typed edges.

| Type | Meaning |
| --- | --- |
| `KIND_OF` | The source term is a kind of the target term. |
| `PART_OF` | The source term is a component of the target term. |
| `SYNONYM_OF` | The two terms are interchangeable in this domain. |
| `RELATED_TO` | A loose association, when no stronger claim fits. |
| `VALID_VALUE_OF` | The source is an allowed value of the target enumeration or domain. |
| `DERIVED_FROM` | The source is computed or sourced from the target. |
| `REPLACES` | The source supersedes the target. Retiring a term writes one from its successor. |
| `PREFERRED_TERM_FOR` | The source is the preferred term over the discouraged target. |
| `TRANSLATION_OF` | The source is a locale or language translation of the target. |
| `ANTONYM_OF` | The source is the semantic opposite of the target. |

Relationships are directional. The UI shows both outgoing edges (this term → another) and incoming edges (another term → this term), labeling each direction with its own plain-language phrase.

The edges live in an associative table declared as a junction relationship. Its relationship-type column is the discriminator, so each type above is a separate Cypher relationship type between two term nodes. Nothing about it is ontology-specific: it is declared the way you would declare a junction over your own tables, and the same code reads it.

## Concepts

A Concept has no columns or tables of its own. Use one for a business idea that spans several concrete terms, then wire it to the terms that do hold data. `revenue`, for example, might be a Concept, with `PART_OF` edges from `order amount`, `adjustment amount` and `refund amount`.

A Concept that can't reach any column or table through the relationship graph names no data. It doesn't appear in agent term search or in metadata export, and **To review** lists it as *no data*. Domains apply to a Concept only. Every other term takes its domains from the tables it grounds or the columns it holds.

## PII on meanings

PII is a property of what a column means, so Provisa lets you mark it once, on the term, and covers every column with that meaning, including columns registered later.

The `pii` tag goes on a column or on a Property term, never on a source, a table, a Class or a Concept. A term must be defined and in service (live or deprecated) to take it. Marking a draft term is refused with "Define *term* to mark it", and you can't clear the definition of a tagged term until you unmark it.

Every column of a tagged term carries `pii`, and `sensitive` with it, wherever tags are read: governance, masking, sensitivity, export and the tag views. Nothing is copied to the columns. A column moved onto the term carries the tag at once, and a column moved off no longer does. Each column says where its tag comes from: set on the column, with its reason, or through a term, naming it.

When you mark a column whose term is a Property, the UI asks whether to mark the other columns that mean the same thing.

- **Yes** puts the tag on the term.
- **Just this one** marks the column alone, and asks why.

A column that is PII through its term can't be unmarked on its own. Unmark the term, or move the column to the term that matches what it holds.

### Moves that lose tags

Moving a column to another term can take a tag off it: moving `customers.email` from `customer email` (tagged `pii`) to an untagged term would leave the email column unprotected. Every move a person makes is checked first. That covers claiming or moving a column, retiring a term, and saving or deleting a relationship that re-homes key columns.

When a column would stop carrying `pii` altogether, the move is refused until you decide. The UI shows one summary of what each moved column loses and gains, losses first.

- **Move** goes ahead, with each lost tag confirmed.
- **Move and mark** carries the lost tags to the term the columns move to, in the same change.
- **Cancel** moves nothing.

A column that keeps the tag through its own mark needs no confirmation. Moves the model makes itself, such as re-deriving a draft's columns, never change an inherited tag.

## To review

The ontology reports what needs your attention as findings. A term with any finding is **To review**, and the term list filters on it. A plain draft isn't a finding; drafts are the ontology's ordinary state.

| Finding | What it means | The fix |
| --- | --- | --- |
| Unlinked tables | A Class grounds several tables that same-entity relationships don't all join. | Mark same-entity relationships between them. |
| Class and attribute | A Class holds columns that aren't its keys, or a keyed table has no Class because its concept is a Property. | Move the columns to a Property, or reground the table. |
| Tag on wrong kind | A term that isn't a Property carries a tag only a Property passes to columns. | Unmark the tag. |
| Retirement blocked | A deprecated term is past its planned retirement but still holds columns or tables. | Retire it with a successor. |
| No data | A Concept in service reaches no data. | Relate it to a term that holds data. |

Findings report; they change nothing until you act.

## Metadata export

The ontology publishes to external data catalogs as part of metadata export. Only live terms publish, judged on the columns and tables that actually publish. A term whose columns are all withheld from export, because their tables aren't data products or because technical filters exclude them, isn't grounded for export, even if it holds columns in the model store.

A deprecated term publishes with its status, its reason and its planned retirement date, so each catalog can map it to its own deprecation marker.

Relationship edges publish only when both endpoint terms publish. Column assets export independently, so excluding a term doesn't hide the underlying data.

### Excluding a term from export

Some columns carry plumbing, such as ETL batch identifiers, row versions and ingestion timestamps. A term derived from such a column may have an accurate definition and still not be business vocabulary. The **Exclude from metadata export** control withholds the term, and any relationship edges ending on it, from the catalogs Provisa publishes to. The columns themselves still export as assets.

What counts is whether the business uses the word. An ETL batch identifier has a clear meaning that belongs in the ontology for engineers, but it doesn't belong in a business catalog next to `customer` and `revenue`.

## Working with the ontology

Open **Ontology** in the navigation bar. The left panel lists every term with its role and, unless it is live, its status. The **Deprecated** and **Draft** boxes show or hide those terms, and **To review** narrows the list to terms with findings. Click a term to open its panel.

The panel shows the term's role, status and, for a Property, its PII mark with the number of columns it covers. A deprecated or retired term shows who changed its status and why. A term with findings opens with a **To review** section: click a finding to see what was found and the action that fixes it.

**Edit** opens the whole term as one form. Save applies every change together.

- **Rename** the term to change its wording without moving its columns.
- **Add a definition** by typing one, or click the AI draft button to generate a starting point from the term's name, its columns and its relationships. The draft isn't saved until you save the form.
- **Claim columns** to move columns from other terms onto this one, or pick **Move to term** beside one of its own columns. Each column belongs to one term. Key columns follow their Class and can't be moved.
- **Ground tables** on a Class, or remove them.
- **Mark PII** on a Property.
- **Add a relationship**, choosing its type. Retype an existing edge in place instead of deleting and re-adding it.
- **Assign experts** by user ID, as `expert` or `author`.

Beside Edit, the panel offers the status actions the term allows: **Deprecate** a live term, **Retire** any term in service, and **Restore** a deprecated or retired one.

From the ✨ menu, **Definitions** fills every blank definition in one pass and never overwrites a person's text. **Relationships** proposes typed edges across the full term list; proposals naming unknown terms, the same term twice or an unrecognized type are dropped.

The **Draft** banner on a term tells you whether it is undefined (alias the column or add a definition) or ungrounded (relate it to a term that has data). While you see it, agents and catalogs can't reach the term.

## Querying the ontology

The ontology is a schema you can query, named `ontology`, beside the `meta` schema that describes the model's structure. Its tables start with an underscore, so a Class's own table never shares a name with one of them.

| Table | One row per |
|---|---|
| `_terms` | Term, with its status, its definition and whether it is a Class, a Property or both |
| `_term_refs` | Binding of a Property to a column, with the column's table and domain |
| `_term_edges` | Relationship between two terms |
| `_domains` | Domain a term belongs to |
| `_experts` | Expert or author assigned to a term |
| `_axioms` | Statement of the ontology in RDFS, OWL and SKOS |

A binding links a term to a column in the `meta` schema, so you can join `ontology._term_refs` to `meta.table_columns` and `meta.registered_tables`. The ontology's tables are read-only. They change as you curate terms.

You read a term only when you reach one of its domains. A term with no domain is visible to everyone, and an enterprise term (`*`) is too. A relationship or an axiom is visible when both of its terms are.

### The ontology in SPARQL

SPARQL answers the ontology in the standard vocabularies, so an RDF tool reads it as it reads any OWL ontology:

- a Class is an `owl:Class`, and a kind-of relationship between Classes is `rdfs:subClassOf`;
- a Property is an `owl:DatatypeProperty`, or an `owl:ObjectProperty` when every column it binds is a key to a table with a Class;
- a Property's `rdfs:domain` is the Class of each table it binds a column of, and its `rdfs:range` is the Class its keys lead to or the XML Schema type of its columns;
- a kind-of relationship between Properties is `rdfs:subPropertyOf`;
- every term has its name as `skos:prefLabel` and its definition as `skos:definition`.

A term is named by its vocabulary IRI, `<your Provisa address>/vocab/term/<name>`, the IRI that types the rows of the tables it grounds. A retired term states nothing.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?class ?parent ?label WHERE {
  ?class a owl:Class ; rdfs:subClassOf ?parent .
  ?parent skos:prefLabel ?label .
}
```

The same statements are rows of `ontology._axioms` in SQL. GraphQL, Cypher and the other query languages don't offer `_axioms`. Query a term's statements and its tables' rows in separate SPARQL queries.

## See also

- [Metadata Export](metadata-export.md) covers how terms and relationships publish to external data catalogs.
- [Column-Level Lineage](lineage.md) covers the lineage explorer and how a physical column reports the terms bound to it.

## The ontology schema

The `ontology` system schema makes the vocabulary queryable from SQL, GraphQL, Cypher, REST, and MCP — the same surfaces you use for your own data. It sits alongside `meta`, which holds the model's structure; `ontology` holds its meaning.

Two parts make up the schema: definition views and class tables.

### Definition views

Five views expose the glossary's contents:

| View | Contents |
| --- | --- |
| `_terms` | Every live term: its name, role (Class, Property, Concept), definition, and status. |
| `_term_refs` | Every term-to-column binding: the term name, the source, the table, and the column. |
| `_term_edges` | Every relationship edge: source term, edge type, and target term. |
| `_domains` | Domain membership: the term name and each domain it belongs to. |
| `_experts` | Expert assignments: the term name, user ID, and role (`expert` or `author`). |

A sixth view, `_axioms`, holds every RDFS/OWL/SKOS triple the schema can express — one row per triple, with columns for subject, predicate, and object. TBD: exact column names and datatypes for `_axioms`. Tools that prefer triples over tabular rows read from here.

A **term binding** — a term bound to a column, recorded in `_term_refs` — is different from a command binding. A term binding says what a column means; a command binding wires a function into the query pipeline.

### Class tables

Each live Class term becomes a view in the `ontology` schema named for that term, with spaces replaced by underscores. A Class grounded in one table yields one view; a Class grounded in several tables yields the union of those tables.

In a union view, every source table's rows appear, with a column identifying which table each row came from. A Property column that one source table does not bind is `NULL` for its rows. A Property bound to several columns of one table becomes an array column. TBD: the name of the source-table column.

Every column in a Class view corresponds to a bound Property. The view is governed through the same pipeline as any other read.

**Example.** A `customer` Class grounded in `sales.customers` and `crm.contacts`, with a `name` Property bound to `customers.name` and `contacts.full_name`, and a `segment` Property bound only to `customers.segment`:

```sql
-- rows from both tables; segment is NULL for crm.contacts rows
SELECT _source_table, name, segment
FROM ontology.customer
WHERE segment = 'enterprise';
```

### Read rights

The same governance rules that apply to your own tables apply here.

A term appears in the definition views only if you reach at least one of its domains. A term with no domain is visible to everyone. A binding row appears only when its column is served to you. An edge appears only when both its terms are visible to you. A Property column in a Class view appears only when its underlying columns are served.

No special access is needed. If you can query a table, you can query the Class terms grounded in it.

### SPARQL

The SPARQL endpoint serves the `ontology` schema as a standard RDFS/OWL/SKOS ontology. Every Class term becomes an `owl:Class`, every kind-of edge becomes `rdfs:subClassOf`, every Property term becomes `owl:DatatypeProperty` or `owl:ObjectProperty` with `rdfs:domain` and `rdfs:range`, sub-property edges become `rdfs:subPropertyOf`, names become `skos:prefLabel`, and definitions become `skos:definition`. The same read rights apply.

The base IRI for all terms is derived from the `server.public_url` setting, under the `/vocab/term/` path. A Class term named `customer` has the IRI `{public_url}/vocab/term/customer`.

Three queries an ontologist would write:

**Find all subtypes of Party, at any depth:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?c WHERE {
  ?c rdfs:subClassOf* :Party .
}
```

**Find customers with a segment value, using the term IRI:**

```sparql
PREFIX : <{public_url}/vocab/term/>

SELECT ?x ?s WHERE {
  ?x a :Customer ;
     :segment ?s .
}
```

**List all properties declared on Customer with their ranges:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?p ?range WHERE {
  ?p rdfs:domain :Customer ;
     rdfs:range ?range .
}
```

Replace `{public_url}` with your deployment's public address as configured in `server.public_url`.
