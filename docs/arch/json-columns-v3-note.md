# v3 note: two hidden column attributes instead of a JSON Schema

Status: superseded 2026-10-09 by the final `json-columns-design.md` (the array attribute became
an integer depth; `native_type` was adopted). Kept on record. Original status: note, 2026-10-09. Beside `json-columns-design.md` (v2). Everything in v2 not mentioned
here stands: the datasource cast, the operations and rule table, refusals, plan notes,
tradeoffs, wires.

Proposal (maintainer): the visible semantic type of the column stays JSON on every engine and
wire. A column that is natively a one-level array of a primitive carries two hidden attributes,
recorded at registration from the source's native type and available to any translator:

- `is_array` (true / false)
- the element's semantic SQL type, with precision and scale where it has them (REQ-1961 spelling)

No JSON Schema is generated or stored. Array of arrays, struct, map and array of struct are
plain JSON columns with neither attribute.

Evidence tags as in v2: `[code]`, `[doc]`, `[probe]`, `[mem]` (unverified).

## 1. Are the two attributes sufficient for arrays of primitives

Inputs the translation has: the two attributes, the dialect of the backend the statement is
routed to, and whether the address being read is the registered source or a replica of it.

| What the schema did | With the two attributes | Sufficient |
|---|---|---|
| Cast out of an extracted or unnested element | cast to `to_physical(element type, dialect)` (`provisa/core/ir_types.py:308`) | yes |
| Cast in of a compared literal | the same physical type | yes, with the exception below |
| Native-form rewrite | the backend's array type is the array of that physical type; the rules are v2 section 4.2 | yes, with the exception below |
| Exact decimal | precision and scale are in the element type; the datasource cast writes decimals as strings | yes |
| To-JSON datasource cast and its element pre-cast | generated from the element type | yes, with the exception below |
| Which form applies (native or JSON) | native when the address read is the source and the source kind has arrays; JSON for a replica, which is landed through the datasource cast | yes, derived, nothing stored |

What they cannot express:

| Missing fact | Does any cast need it |
|---|---|
| The source's exact native spelling, when two native types share one semantic type | **Yes, in three places.** (a) A timestamp with time zone and one without are both semantic `timestamp`; the value transform that makes one UTC is keyed by the native type (`ir_types.py:164-172` `[code]`), and applying it to the other kind shifts the value. (b) PostgreSQL's array operators take two arrays of the same type: `varchar[] @> text[]` has no operator `[mem]`, so the literal side of a native contains must be the column's own type, or be written so the server infers it. (c) A write casts the JSON given into the native type, which needs that type (`char(10)[]`, DuckDB `INTEGER[3]`) |
| That a column is native and not JSON at its source, when it is a struct, map or nested array | **Yes.** Without it the translator cannot know to apply the to-JSON cast, and a DuckDB `STRUCT` column would reach the wire as a struct instead of JSON. `is_array = false` does not say it |
| Element nullability | no cast needs it |
| Fixed length | only the write in (c) |
| NULL array versus empty array | a property of values, not of the type |
| Drift | detectable only as a change in the two attributes; a change that keeps both (int4 to a domain over int4, timestamp to timestamptz) is not seen |

So the two attributes are sufficient for reading arrays of primitives except where the native
spelling decides the cast. One more hidden attribute closes every row above: `native_type`, the
source's own spelling, which v2 already has. With it, `is_array` and the element type are what
the translators read for the common case, and `native_type` is what the three exceptions, the
struct/map to-JSON cast and drift read. Decision D1 below.

## 2. What struct, map and nested arrays lose with no schema

| Still works | Lost |
|---|---|
| Whole-value read as JSON, on every route (given the row above: the translator must know the column is native) | Typed extraction from the schema: the user writes the cast, `JSON_VALUE(c, '$.amount' RETURNING numeric(18,2))` |
| Every JSON operation, untyped: element and field access, length, contains, unnest, construct, aggregate | Cast in of literals: a comparison is on text unless the user casts |
| Plan notes | The native form: a field of a native struct is reached through to-JSON and a JSON function, so no source index or native speed for it |
| | Exact spellings inside the value: the datasource cast cannot rewrite fields it does not know, so decimals are JSON numbers and timestamps are in the backend's own format. A decimal inside a struct read back through DuckDB or SQLite goes through a double `[probe]`; PostgreSQL keeps it `[doc]` |

Acceptable as stated, with one sentence for the documentation: inside a struct, map or nested
array, numbers are JSON numbers and their exactness is the backend's. REQ-1965's sentence "a
decimal with a declared precision and a timestamp keep their exact value inside JSON" then
holds for arrays of primitives only.

## 3. Where the attributes live and how they reach the translation

| Question | Answer |
|---|---|
| In the model | fields on `Column` beside `data_type` (`provisa/core/models.py:822` `[code]`), stored with the column as `object_fields` is (`provisa/core/schema_org.py:341` `[code]`). V1: the table definition changes, no migration |
| Set by | registration only, from the structured native type the introspection parser returns. Not accepted from a model file for a source column unless they match what the source reports |
| To the translation | per-column types already travel in the governance context as `(name, type)` pairs (`gov_ctx.all_columns`, read at `provisa/compiler/stage2.py:488,517,558` `[code]`). The attributes travel beside them. Nothing under `provisa/transpiler` reads column types today, and the rewrite needs the routed backend, which is known only after routing. So stage 2 records, on the plan, the attributes of each JSON column the statement references; the rule-table rewrite in the transpile stage reads that map with the dialect. Derived once, kept with the governed plan (REQ-1877) |
| In the UI | hidden: not in the registration form. The column's detail view may show "array of text" read-only, for support; nothing edits them |
| In catalogs | not listed: the listed type is JSON |
| For a column that is JSON at its source | a registrar may set `is_array` and the element type by hand, so a MySQL `JSON` list of strings gets typed extraction too. Decision D2 |

## 4. Reduced decisions

Recommendation first. D-numbers restart for this note.

| # | Decision | Choices |
|---|---|---|
| D1 | The source's native spelling | **a third hidden attribute, `native_type`**, read by the three casts of section 1, by the to-JSON cast for struct/map/nested columns, and by drift; two attributes only, with timestamps with time zone refused as array elements, native contains written so the server infers the type, and a separate flag for "native, needs the to-JSON cast" |
| D2 | A column that is JSON at its source | **a registrar may declare it an array of a primitive**; attributes only ever come from a native type |
| D3 | Spelling in semantic SQL | **SQL/JSON standard functions, with PostgreSQL's operators accepted as the same operations**; PostgreSQL operators only |
| D4 | Decimals with a declared precision inside a JSON array | **strings**; JSON numbers |
| D5 | PostgreSQL physical type for `json` | **`jsonb`**; `JSON` as today |
| D6 | Drift | **refuse reads of the column by name until re-registered**; re-derive the attributes at reconcile |
| D7 | Plan notes | **in the plan only**; also on the warning channel of every execution |
| D8 | Cypher / Bolt | **the JSON text as a string**; parse to lists at that surface |
| D9 | NaN and infinities in float elements | **the translation raises by name**; write them as strings |

Gone from v2: the JSON Schema itself, its subset and editor, the declared-schema conformance
check, and the decision about enforcing a declared schema.

## 5. What changes in REQ-1965

| Sentence | Becomes |
|---|---|
| "Every column that is natively an array, struct or map carries a JSON Schema ... generated from the native type" | a column that is natively a one-level array of a primitive carries two hidden attributes (and, under D1, its native type) |
| "a column that is JSON at its source may carry one declared by a registrar" | per D2 |
| "The schema is stored with the column in the model and editable in the UI" | the attributes are stored with the column and are not edited |
| "The schema is an input to that translation" | the attributes are |
| "A decimal with a declared precision and a timestamp keep their exact value inside JSON" | inside an array of primitives |
| "GraphQL serves it typed from the schema" | removed (earlier ruling) |
