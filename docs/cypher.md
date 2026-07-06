# Cypher Query Support

Provisa translates a subset of openCypher to SQL via the `provisa/cypher/` module. (REQ-345, REQ-347) Queries are parsed by a custom recursive-descent parser (no external Cypher library) (REQ-571), schema-resolved against the semantic layer (REQ-351), and emitted as SQLGlot expression trees in PostgreSQL dialect. (REQ-066, REQ-347) The router then transpiles to the target execution dialect (Trino) via SQLGlot. (REQ-066, REQ-067)

## Implemented Features

### Clauses

| Clause | Status | Notes |
|--------|--------|-------|
| `MATCH (n:Label)` | ✓ | Node patterns with labels, variables, inline properties |
| `OPTIONAL MATCH` | ✓ | Emits LEFT JOIN |
| `WHERE` | ✓ | Full expression support; applied after MATCH |
| `RETURN` | ✓ | Star, property access, expressions, aliases |
| `RETURN DISTINCT` | ✓ | Emits SELECT DISTINCT |
| `WITH` | ✓ | Emits a named CTE (`_w0`, `_w1`, …); supports `WITH … WHERE` |
| `ORDER BY` | ✓ | ASC / DESC |
| `SKIP` / `LIMIT` | ✓ | Maps to SQL OFFSET / LIMIT |
| `UNION` / `UNION ALL` | ✓ | Recursive union across sub-ASTs |
| `CALL { … }` | ✓ | Top-level call subquery decomposition via `cypher_calls_to_sql_list` |
| `CALL { WITH x … }` | ✓ | Correlated subquery → `CROSS JOIN LATERAL`; see §Correlated CALL |
| `CALL db.labels()` | ✓ | Returns node labels from the semantic layer; no SQL translation (REQ-572) |
| `CALL db.relationshipTypes()` | ✓ | Returns relationship types from the semantic layer (REQ-572) |
| `CALL db.propertyKeys()` | ✓ | Returns all property key names across all node types (REQ-572) |
| `UNWIND` | ✓ | Array-to-rows expansion; first item becomes FROM, subsequent become CROSS JOIN UNNEST |

### Match Patterns

| Pattern | Status | Notes |
|---------|--------|-------|
| `(n)` — unlabeled node | ✓ | UNION ALL over all known types |
| `(n:Label)` | ✓ | Maps to the registered table for that GraphQL type |
| `(n:Label {prop: val})` | ✓ | Inline property filter becomes WHERE |
| `(a)-[:TYPE]->(b)` | ✓ | Directed, single hop |
| `(a)<-[:TYPE]-(b)` | ✓ | Backward traversal; join columns reversed |
| `(a)-[]->(b)` | ✓ | Any directed relationship a→b; UNION ALL if multiple types match |
| `(a)-[]-(b)` | ✓ | Bidirectional; expands to UNION ALL of all forward and backward relationships |
| `(a)-[:TYPE*..N]->(b)` | ✓ | Variable-length with upper bound; recursive CTE for self-referential, flat JOIN otherwise |
| `(a)-[]->(b)-[]->(c)` | ✓ | Multi-hop chained JOINs |
| `(n:DomainLabel)` | ✓ | Domain label → UNION ALL subquery over all types in the domain |
| `(n:A\|B)` | ✓ | Label alternation → ad-hoc domain injected into label map; UNION ALL over matching types |
| `shortestPath(…)` | ✓ | Flat JOIN for heterogeneous endpoints; WITH RECURSIVE CTE for same-type/self-referential |
| `allShortestPaths(…)` | ✓ | Same as shortestPath without LIMIT 1 |

### Expressions and Predicates

| Feature | Status | SQL mapping |
|---------|--------|------------|
| Property access `n.prop` | ✓ | `n."prop"` |
| Parameters `$name` | ✓ | Positional `$N` |
| Legacy parameters `{name}` | ✓ | Normalised to `$name` at parse time |
| Comparison `=`, `<>`, `<`, `>`, `<=`, `>=` | ✓ | Direct |
| `AND`, `OR`, `NOT` | ✓ | Direct |
| `IS NULL` / `IS NOT NULL` | ✓ | Direct |
| `IN [list]` | ✓ | SQL IN; Cypher `[...]` bracket syntax rewritten to `(...)` |
| `STARTS WITH` | ✓ | `starts_with(col, val)` |
| `ENDS WITH` | ✓ | `col LIKE CONCAT('%', val)` |
| `CONTAINS` | ✓ | `strpos(col, val) > 0` |
| `=~` regex | ✓ | `regexp_like(col, pattern)` |
| `exists(n.prop)` | ✓ | `(n.prop) IS NOT NULL` |
| `EXISTS { MATCH … }` | ✓ | Correlated `EXISTS (SELECT 1 FROM …)` subquery |
| `COUNT { MATCH … }` | ✓ | Correlated `(SELECT count(*) FROM …)` subquery |
| `COLLECT { MATCH … RETURN x }` | ✓ | Correlated `ARRAY(SELECT x FROM …)` subquery |
| `id(n)` | ✓ | Resolved to the node's configured ID column |
| `labels(n)` | ✓ | `ARRAY['Label']` |
| `keys(n)` | ✓ | `ARRAY['prop1', 'prop2', …]` |
| `type(r)` | ✓ | Resolved at compile time to `'REL_TYPE'` string literal; no runtime column |
| `length(p)` | ✓ | `_t.hops` for recursive CTE paths; `1` for flat JOIN paths |
| `CASE WHEN … THEN … ELSE … END` | ✓ | Direct (searched and simple forms) |
| Implicit GROUP BY | ✓ | Non-aggregated RETURN items become GROUP BY keys when any item has an aggregate |

### Map Projections

| Syntax | SQL mapping |
|--------|------------|
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])` — expanded from schema |
| `n { .*, extra: expr }` | All schema props plus named key; combined MAP |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

### Aggregation Functions

| Cypher | SQL |
|--------|-----|
| `count(*)`, `count(x)` | direct |
| `count(DISTINCT x)` | `count(DISTINCT x)` |
| `collect(x)` | `array_agg(x)` |
| `avg`, `sum`, `min`, `max` | direct |
| `stDev(x)` | `stddev_samp(x)` |
| `stDevP(x)` | `stddev_pop(x)` |
| `percentileCont(x, p)` | `approx_percentile(x, p)` |
| `percentileDisc(x, p)` | `approx_percentile(x, p)` |

### String Functions

| Cypher | SQL |
|--------|-----|
| `toLower(x)` | `lower(x)` |
| `toUpper(x)` | `upper(x)` |
| `ltrim(x)`, `rtrim(x)`, `trim(x)` | direct |
| `replace(x, a, b)` | direct |
| `reverse(x)` | direct |
| `split(x, d)` | direct |
| `left(x, n)` | `left(x, n)` |
| `right(x, n)` | `right(x, n)` |
| `substring(x, start, len)` | `substr(x, start+1, len)` (0→1 index) |
| `size(string)` | `char_length(string)` |
| `size(list)` | `cardinality(list)` |

### Type Conversion Functions

| Cypher | SQL |
|--------|-----|
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`, `toIntegerOrNull`, `toFloatOrNull`, `toBooleanOrNull` | `TRY_CAST` variants |

### Math Functions

| Cypher | SQL |
|--------|-----|
| `log(x)` | `ln(x)` (natural log) |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`, `sqrt`, `ceil`, `floor`, `round`, `sign` | passed through |

### List Functions

| Cypher | SQL |
|--------|-----|
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### List Comprehensions

| Syntax | SQL mapping |
|--------|------------|
| `[x IN list \| f(x)]` | `transform(list, x -> f(x))` |
| `[x IN list WHERE p(x)]` | `filter(list, x -> p(x))` |
| `[x IN list WHERE p(x) \| f(x)]` | `transform(filter(list, x -> p(x)), x -> f(x))` |
| `any(x IN list WHERE p(x))` | `any_match(list, x -> p(x))` |
| `all(x IN list WHERE p(x))` | `all_match(list, x -> p(x))` |
| `none(x IN list WHERE p(x))` | `none_match(list, x -> p(x))` |
| `single(x IN list WHERE p(x))` | `cardinality(filter(list, x -> p(x))) = 1` |
| `reduce(acc = init, x IN list \| expr)` | `reduce(list, init, (acc, x) -> expr, acc -> acc)` |

### Pattern Comprehensions

| Syntax | SQL mapping |
|--------|------------|
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | type-inferred from semantic layer; same ARRAY subquery form |

### Correlated CALL Subqueries

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` translates to `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`. (REQ-573) Rules:
- The outer-scope variable (`x`) must appear in `WITH`
- Multiple imported vars (`WITH a, b`) are supported
- The first relationship in the inner MATCH whose source is a lateral-bound var determines the inner `FROM` and join condition
- Non-correlated top-level `CALL { ... }` blocks (without `WITH`) are handled by `cypher_calls_to_sql_list`

---

## Limitations

### Design constraints

1. **Read-only.** `CREATE`, `MERGE`, `SET`, `DELETE`, and `REMOVE` are not supported. (REQ-346) Cypher is a read path only; mutations go through the GraphQL mutation API. (REQ-346, REQ-032)

2. **No relationship properties.** Relationships (`-[r:TYPE]->`) exist solely as join metadata in the semantic layer. (REQ-574) They carry no stored attributes, so `WHERE r.since > 2020` or `RETURN r.weight` has no meaning and is not supported.

3. **Bidirectional traversal** `(a)-[]-(b)` rewrites to the forward+backward UNION ALL of all matching directed relationships from the semantic layer. (REQ-575) Every relationship in the semantic layer is directional; bidirectional syntax is sugar that expands to both directions. Extra branches are emitted at the outermost query level — subsequent MATCH patterns in the same query are not duplicated across branches (limitation for multi-MATCH bidirectional).

4. **Recursive paths require a bound.** Variable-length patterns (`[*]`) must include an upper bound (e.g. `[*..10]`). (REQ-348) Unbounded traversal is rejected at parse time to prevent runaway recursive CTEs.

### Behaviour notes

5. **`shortestPath` on non-self-referential paths uses flat JOIN, not hops ordering.** When start and end types differ and no self-referential relationship exists in the schema, the translator emits a flat JOIN chain (the shortest schema path). (REQ-576) It does not emit `ORDER BY hops` because hops are not tracked in that code path. The result is the structurally shortest schema path, not the data-shortest path across multiple rows.

6. **Multiple schema paths produce `UNION ALL`.** When two schema paths of equal hop count connect the same start and end types (e.g. `Person -[WORKS_AT]-> Company` and `Person -[MANAGES]-> Company`), both are emitted as `UNION ALL` branches. (REQ-577) Deduplication of rows that appear in both branches is not performed.

7. **One `RelationshipMapping` per source→target pair and rel\_type combination.** If two GraphQL fields on the same source type produce the same `rel_type` string (after uppercasing) to the same target type, the second registration overwrites the first in `CypherLabelMap.relationships`. The relationship key includes source and target type names, so distinct source/target pairs with the same type name each get their own entry and are not affected.

8. **`WITH` clause CTEs are named `_w0`, `_w1`, …** (REQ-578) Names are assigned positionally within a single translation call. Composing multiple translated queries (e.g. in a batch) can produce colliding CTE names if they are concatenated naively.

### Expression and pattern coverage (REQ-913)

Cypher expressions are parsed into an AST and lowered node-to-node to SQL (`provisa/cypher/expr_parser.py`, `provisa/cypher/expr_visitor.py`). The grammar follows the openCypher `oC_Expression` precedence tower. Supported: literals, parameters, property access, `n.prop`, index and slice, arithmetic (`+ - * / % ^`), comparison, `IN`, `STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`, `IS [NOT] NULL`, boolean `AND` / `OR` / `XOR` / `NOT`, `CASE`, list and map literals, list and pattern comprehensions (including the `p = (…)` path binding), map projection, `reduce`, the `all` / `any` / `none` / `single` quantifiers, existential subqueries, and function calls.

9. **Labels are fixed; you cannot create object types through Cypher.** A label resolves to a known domain, a known object type, or a qualified `domain:object_type` — the closed set defined by the registered schema. Cypher never introduces a new label or type. Instance creation is possible only for types already defined within a writable data source, and only through the mutation API, not through Cypher (which is read-only per constraint 1). Both label forms are accepted and mean the same test: the postfix `n:Label` and the verbose `n IS :Label` (and their negation `n IS NOT :Label`). A qualified label is written `n:domain:object_type`.

10. **`shortestPath` and `allShortestPaths` are supported only inside `MATCH`, not as expressions.** In a pattern (`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`) they translate to a `WITH RECURSIVE` CTE and require labeled source and target nodes. Used in expression position — for example `RETURN shortestPath((a)-[*]->(b))` or `WHERE length(shortestPath((a)-[*]->(b))) < 5` — they are not supported, because the recursive rewrite is driven off the `MATCH` clause rather than a correlated subquery.

11. **List comprehensions, `REDUCE`, and quantifiers run against list values; pattern comprehensions traverse.** `reduce(...)`, `all/any/none/single(...)`, and the list comprehension `[x IN list | …]` operate over a list expression and lower to the engine's higher-order list functions — they do not themselves walk the graph. The **pattern** comprehension `[(a)-[:R]->(b) WHERE p | e]` does traverse: its graph pattern is addressed out as a correlated subquery, so it is a comprehension whose source is a traversal. Feed traversal results into the list forms with `nodes(p)` / `relationships(p)` / `collect(...)`, or use a pattern comprehension directly.
