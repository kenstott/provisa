# Hidden columns kept out by construction

Design note for REQ-039, amendment of 2026-11-04 ("HIDDEN COLUMNS ARE KEPT OUT BY CONSTRUCTION").
Job J615. No code has changed yet; the open product questions are at the end.

## The change in one paragraph

Today stage 2 (`provisa/compiler/stage2.py`) leaves each registered table where the statement
names it and governs the references to it one by one: a hidden column becomes NULL, a masked one
becomes its mask, the row rule is ANDed into the WHERE of the SELECT that names the table. That
only works where the compiler finds every reference and knows which relation PostgreSQL will
resolve it to. RV597 and RV597b show the compiler getting that wrong in a dozen shapes
(parenthesised joins, CTE bodies, sibling FROM items, LATERAL items, literal output names,
data-modifying CTEs). The amendment removes the prediction: every reference to a registered
relation is replaced by the role's governed relation, a derived table that exposes only the
columns the role is served, masked, with the role's row rule applied. A hidden column is then not
in the statement the engine runs, so no scoping rule can reach it. V003 stays as the early,
readable refusal; it is no longer the boundary.

## The governed relation

For a registered table `t` read by a role under alias `a`, the governed relation is:

```sql
(SELECT <served column 1, masked if a mask applies> AS <its name>,
        <served column 2> AS <its name>, ...
   FROM <t> AS <a>
  WHERE <the role's row rule on t, qualified to a>) AS a
```

- It keeps the alias the statement gave the table (or the table's own name when there was
  none), so every reference the role wrote that names a served column still resolves, and the
  outer statement needs no column renaming.
- Each served column is projected under the name the statement uses for it (its published name,
  `apply_sql_name` of the physical one), so `_read_projected_names` stops being needed: the
  relation exposes the names the role already writes.
- The row rule is evaluated inside, on the real values, before any mask. Row rules and masks are
  the administrator's and may read any column (REQ-039); inside the derived table they can.
- Masks are applied once, at the relation. Every clause outside (WHERE, JOIN ON, GROUP BY,
  ORDER BY, windows, aggregates) then reads the masked value without stage 2 finding the
  reference. That is what `_govern_references` does today reference by reference; the result is
  the same, without the search.
- The answer's double-precision read of an undeclared numeric (`_as_read`, REQ-1961) stays where
  it is, on the outermost projection only: inside the relation the column keeps its exact type
  so filters and joins compute on the exact value.
- Anything attached to the table node itself (TABLESAMPLE, a time-travel argument lifted by
  `lift_statement`, `ONLY`) moves inside, onto the base table.
- A table whose every column is hidden from the role is refused before this point (V000/V003),
  as `_expand_star` refuses it now; a derived table with no columns is never built.

### One mechanism, not two

`_project_faked`, `_project_kept` and `_project_members` already put a derived table in a table's
place, under the same alias, marked `_GOVERNED`. The governed relation is the last stage of that
same chain, and it becomes unconditional:

1. base table, with the row rule (on real values)
2. faked projection, when the table has faked columns (REQ-1494)
3. kept-mutation overlay, in a Reversible environment (REQ-1942)
4. member projection, for a meta-role whose members read differently (REQ-1620)
5. served-column projection with masks: drops every hidden column

Today steps 2 to 4 each keep every column and the per-reference pass hides the rest afterwards.
With step 5 always present, the per-reference pass for visibility and masking goes away
(`_govern_references`, most of `_is_column_visible` / `_maybe_mask_column` as applied to outer
references, `_read_projected_names`). Step 5 is the only place a column is dropped.

The three existing builders compose SQL text with f-strings and re-parse it. REQ-913 says SQL
rewrites are SQLGlot AST only, so the chain is rebuilt with `exp.select(...)` builders as part
of this job; the rendered SQL stays as it is now apart from step 5.

### Every reference, wherever it stands

The replacement walks every `exp.Table` in the tree, not each SELECT's own FROM list. That
covers a parenthesised join (sqlglot parses `(t JOIN u ...)` as a `Subquery` whose `this` is a
Table with joins, which `_get_tables_from_select` skips today: RV597b C3), a CTE body, a LATERAL
item, a set-operation branch, a scalar or EXISTS subquery, and the FROM/USING of a write.
The exceptions are deliberate and few:

- a CTE reference (a `Table` naming a CTE in lexical scope, resolved by `db` as well as name, so
  `bank.customer` is never the CTE `customer`: RV597b H2);
- a table inside a row rule or a mask, which is the administrator's (it carries `_GOVERNED`);
- a table inside an expanded view body, handled below.

The walk is one pass over the tree, deepest first, replacing each node once; a replaced node is
marked so a later pass does not wrap it again.

## Where it happens in the pipeline

Inside `apply_governance`, which is where stage 2 already runs. For the raw-SQL path that is
`govern_statement` in `provisa/pgwire/_pipeline.py`; for GraphQL and Cypher it is
`_govern_and_route_compiled`. In order:

1. parse, lift time-travel arguments, localize inline commands, expand metrics (unchanged)
2. refusals on the statement as written: physical refs, view writes, unbound writes (unchanged,
   but see writes below for the nested-DML check)
3. view expansion: `semantic_statement` makes one semantic statement (unchanged)
4. V-rules: `validate_sql` (unchanged; V003 still refuses early with a readable message)
5. **stage 2: admit any write, then replace every registered relation with its governed
   relation; then the row cap and list caps** (the change)
6. bitemporal reads (`read_versions`), column transforms, `embed()` folding (unchanged: they
   find the base table inside the governed relation, since they use `find_all`)
7. route stage: lowering to catalog-physical, `_nf_` parameter extraction, row-level PK bounds,
   response cache, MV reinject, routing, transpile per engine

So the wrapping is after views expand and before the optimize stage and MV reinject. It has to
be after view expansion, because a view body names registered tables that must be governed too,
and before anything that reads predicates for performance, because those must see the governed
statement (REQ-863: governance, then optimization, then routing).

### View bodies

REQ-039 says an administrator's view body is the administrator's: only its select list is judged
against the reader, and its other clauses may use columns hidden from the reader. If view bodies
were wrapped like the reader's own SQL, a view that joins or filters on a hidden column would
fail for that reader. So inside an expanded view body a registered table is replaced by a
relation that applies the reader's row rule and masks but keeps the hidden columns; the body's
own select list is then governed as today (a hidden output is NULL or refused). The expanded view
is itself a derived table in the reader's statement, so nothing in it is reachable from outside
except its select list. That needs the governed statement to carry `VIEW_MARK` on each expanded
body; today only the validated copy carries it (`semantic_statement` returns the marked text only
as `validated`). The mark is per-process random and is never in text the role writes (RV597b M3).

## Joins on hidden columns

A role cannot join on a column it cannot see: after the change, `ON a.k = b.k` with `k` hidden
from the role fails as an unknown column (V003 refuses it first). That is the intent for a join
the role writes itself.

Registered relationships are different. REQ-603 lets a role reach related rows through an
approved relationship even when its key is hidden or masked: today `approved_join_key` leaves
such a reference untouched and the engine joins on the real value, which is not returned.
Under construction there is no real value left to join on unless the relation carries it. This
is a product question (Q1 below). The recommended answer: the governed relation carries the real
key, under an internal name, only when the statement makes an approved join on it, and the join's
equality is rewritten to that name. `_project_members` already does exactly this for meta-roles
(`__real_<name>` columns and `real_keys`), so the mechanism exists. The internal name carries a
per-process random part, as `VIEW_MARK` does, so the role cannot write it. A star, a whole-row
reference (`SELECT a`, `row_to_json(a)`) and a NATURAL or USING join never include it: stars are
expanded by governance over the relation's served columns, a whole-row reference to a relation
carrying an internal key is rewritten to `ROW(<served columns>)`, and NATURAL/USING are matched on
served names only.

A masked key follows the same rule: an approved join reads the real value, any other join reads
the masked one, as today.

## Writes

A derived table cannot be the target of an UPDATE or DELETE in PostgreSQL (or in most of the
sources Provisa writes to), so the governed relation does not stay the target. The write is
rewritten to the base table, and the row rule stays in the base WHERE exactly where
`admit_write` puts it today (the grant's rows predicate and the role's U or D rule, REQ-2074 and
REQ-2188).

Keeping the base table as the target brings back the scoping problem for whatever the role wrote
inside the write: any unqualified name in its WHERE, SET or RETURNING, including inside a nested
subquery, resolves outward to the target, which has every column. So the role's own expressions
are moved out of the target's scope:

```sql
-- the role writes
UPDATE bank.customer SET name = upper(name) WHERE <p> RETURNING <r>

-- runs as
WITH __k AS (
  SELECT id, upper(name) AS __new_name
    FROM <governed relation of bank.customer> AS customer
   WHERE <p>
), __w AS (
  UPDATE bank.customer
     SET name = (SELECT __new_name FROM __k WHERE __k.id = bank.customer.id)
   WHERE id IN (SELECT id FROM __k) AND <grant rows predicate> AND <U rule>
  RETURNING <served columns, masked>
)
SELECT <r> FROM __w AS customer
```

- A CTE body cannot see the statement it is attached to, so `<p>` and the SET expressions are
  evaluated where only the governed relation (and the role's own, also wrapped, subqueries) is
  in scope. A hidden column named there is unknown.
- The write itself contains only compiler-written expressions: key equality, the grant
  predicate, the rule.
- RETURNING is read over `__w`, which exposes only served, masked columns of the new rows. This
  replaces `_govern_returning`'s per-reference check.
- The key is the table's registered primary key. A write whose WHERE or SET reads a column of a
  table with no registered key is refused (V006) rather than rewritten.
- DELETE has the same shape without SET. INSERT ... VALUES has no role expressions over the
  target. INSERT ... SELECT is refused by write admission today; if it is admitted later, its
  SELECT is a read and is wrapped like any read. `ON CONFLICT DO UPDATE` may read `EXCLUDED` and
  constants only: a reference to the existing row is refused (RV597b H3's fourth statement).
  MERGE follows the same rule where admission admits it: its source is a read and is wrapped,
  and its WHEN conditions and SET may read the source and constants, not the target row.

Sources whose dialect cannot run a leading WITH on a write, or a writable CTE, run the same
thing in two steps through the existing key-read path (`_code_write_plan` reads "the governed
write's own FROM and WHERE, read as any read is" and then writes by key). Q2 asks whether that
is acceptable. In a Reversible environment the kept-mutation path (REQ-1942) receives the
rewritten statement; implementation checks that it applies the key-set form unchanged, and
otherwise hands it the two-step form.

### A data-modifying statement anywhere in the tree

`is_write` checks only the root (`write_admission.py:159`), and so do the pipeline's mutation
checks (`_reject_view_writes`, `route_governed`'s `_is_mutation`). That is RV597b C4. The check
becomes a walk: a statement holding an INSERT, UPDATE, DELETE, MERGE or TRUNCATE anywhere is a
write, is admitted, routes DIRECT, is never cached, explained or delivered, and is audited as a
write. The recommended scope (Q3) admits one data-modifying statement per statement, either at
the root or as `WITH w AS (<write> RETURNING ...) SELECT ... FROM w`, which is the shape above;
any other placement is refused with V006.

## Predicate pushdown and each engine

The governed relation is a projection with a filter: no aggregate, DISTINCT, window, LIMIT or
set operation. Every engine Provisa uses merges such a derived table into the outer query, so the
plan is the same as for the base table read with the row rule in its WHERE:

- **PostgreSQL (DIRECT and passthrough):** the planner pulls up simple subqueries
  (`pull_up_subqueries`), including on the nullable side of an outer join. It will not pull up
  a subquery whose select list holds a volatile function; no built-in mask is volatile, and a
  mask that is would already be evaluated per row today.
- **Trino:** derived tables are inlined at planning; predicates and projections are pushed into
  the connector as before. A predicate over a masked column is not pushed into the connector,
  which is also true today, because the mask is in the predicate either way.
- **DuckDB:** filter pushdown and projection pruning go through projections; no change.
- **ClickHouse:** `enable_optimize_predicate_expression` (on by default) pushes outer WHERE
  predicates into subqueries, and the analyzer prunes unused columns. Implementation checks that
  PREWHERE still applies to a row rule on a MergeTree table.
- **Other DIRECT sources** (MySQL 8, SQL Server, Snowflake and the rest): all merge simple
  derived tables (MySQL's `derived_merge` is on by default).

Implementation verifies each with EXPLAIN on one statement per engine against the base-table
form, and records the plans in the job report.

Provisa's own passes that read predicates look at "the SELECT that names the table in its
FROM", which is now the governed relation's own SELECT holding only the row rule. Each has to
read through a governed relation to the outer predicates, mapping an outer `a.col` to the inner
column when the relation serves it unmasked:

- row-level PK bounds (`pk_bounds.scope_predicates`, REQ-1865), or a row-level table read loses
  its cache bound and a federated read of it is refused;
- `_nf_` native-parameter extraction (`extract_nf_args`, `separate_parameter_reads`): the
  relation exposes the table's `_nf_` parameter columns, which are virtual and served with every
  table (`SERVED_WITH_EVERY_TABLE`), so the outer conditions still resolve and are still lifted;
- result-shape typing (`derive_result_shape`), or columns read through a relation come back
  untyped;
- MV reinject, below.

One helper in stage 2 answers "is this derived table the governed relation of a registered table,
and which one": a `Subquery` whose body reads exactly one registered table and nothing else, with
the alias it took. Because the governed SQL passes between stages as text, it is recognised by
shape, not by AST meta.

## MV rewrite and caching

`reinject` (`mv/view_reinject.py`) matches only a statement whose FROM root is a bare `Table`
with a catalog. After the change every root is a derived table, so no materialized view would
ever answer a read. Q4 asks whether that is fixed in the same change (recommended) or after it.
Fixing it means teaching `_covered` and the aggregate catalog to read a governed relation as its
table: the inner table is the view's table, the inner row rule and masks are the "predicate or
mask over a column the view keeps" that reinject already carries onto the view's column, and the
outer predicates are carried the same way. Reinject already runs after governance and already
handles governance in the statement, so this is a change in how it finds the table, not in what
it does with it.

Caching:

- **Response cache (REQ-544, REQ-1897):** keyed on the governed statement, so every key changes
  once; entries written before the change are not hit after it. Two roles with different
  visibility still get different keys.
- **Kept plans (`PlanSlot`) and the compiled-query cache (REQ-1877):** keyed on the statement as
  written plus role and schema generation, both in process memory; a deploy starts them empty.
- **Prepared statements:** governed once, executed many times; unchanged.

## Behaviour a role can see

- A column hidden from the role, named in the role's SQL, is refused by V003 as now. Where V003
  misjudges scope, the engine now answers "column does not exist" instead of returning data.
- A name hidden in an inner relation can now resolve outward to an outer relation that serves a
  column of the same name, as PostgreSQL scoping would for any missing column. The value read is
  one the role is served.
- A join the role writes on a hidden column fails. Relationships: see Q1.
- `SELECT *` and `t.*` return the same columns as today.
- Masked values in WHERE, GROUP BY and ORDER BY behave as today.

## Tests and goldens

- **GraphQL and Cypher goldens** (`tests/fixtures/graphql_golden`, `tests/fixtures/cypher_golden`)
  do not change: they record the semantic SQL before governance and the answers read as the
  administrator (`scripts/graphql_golden.py`, "the baseline of what each read answers, not of
  governance").
- **Unit tests asserting governed SQL text** change: 74 test modules call `apply_governance`,
  chiefly `test_stage2.py`, `test_stage2_complex.py`, `test_rls.py`, `test_governance_whole_statement.py`,
  `test_raw_sql_column_governance.py`, `test_mask_pushdown.py`, `test_masked_join_keys.py`,
  `test_fake_reads.py`, `test_meta_role_member_rows.py`, `test_star_column_visibility.py` and the
  write-admission tests. Where a test checks the text of governed SQL, it is changed to check
  what the statement returns, run on embedded PostgreSQL, so the next change of shape does not
  repeat this.
- **New tests:** every statement in RV597-findings.md and RV597b-findings.md, run through the
  whole pipeline on embedded PostgreSQL against `bank.customer(id, name, ssn)` with `ssn` hidden
  and a row rule: none returns `ssn`, none changes its answer with the value of `ssn`, and each
  row rule holds. Statements that V003 refuses are also run with V003 bypassed, so the
  construction is tested on its own.
- **Integration and e2e:** the REQ-039/REQ-040 suites run unchanged and must pass; the EXPLAIN
  checks per engine above; one MV-answered read per kind (join-pattern full, partial, aggregate)
  still answered from its view.

## Product questions

**Q1. Relationships whose join key is hidden from the role.**

- (A, recommended) The governed relation carries the real key under an internal name only when
  the statement makes an approved join on it (REQ-603); the join reads the real value, which is
  never returned. Every relationship and every GraphQL nesting works as today.
- (B) A relationship whose key is hidden from a role is not offered to that role: dropped from
  its SDL, and a SQL join on it fails. Strictest and simplest; models that hide foreign keys lose
  those relationships for those roles.

**Q2. Writes on sources that cannot run the single-statement form.**

- (A, recommended) Single statement (leading WITH key-set read plus writable CTE) where the
  source's dialect supports it; two steps through the existing key-read path elsewhere. A write
  whose WHERE or SET reads columns of a table with no registered key is refused.
- (B) Two steps everywhere, for one behaviour on every source. Not atomic: a row can change
  between the key read and the write. The grant predicate and the U/D rule are still in the
  write's WHERE, so the write never exceeds the grant.

**Q3. Data-modifying statements inside a statement.**

- (A, recommended) One data-modifying statement per statement, at the root or as
  `WITH w AS (<write> RETURNING ...) SELECT ... FROM w`; any other placement refused (V006).
- (B) Any number, anywhere, each admitted and governed. Several writes in one statement can
  reach different sources, which no route runs atomically.

**Q4. Materialized-view answering during the change.**

- (A, recommended) Teach reinject, PK bounds, `_nf_` extraction and result-shape typing to read
  a governed relation as its table in the same change, so no read loses its view or its row
  cache.
- (B) Land the boundary first with those passes not seeing through governed relations (no MV
  answers a read of a registered table, row-level reads go live or are refused), and restore
  them in a follow-up job. Security lands sooner; reads are slower until the follow-up.
