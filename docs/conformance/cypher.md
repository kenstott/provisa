# Cypher Conformance

**Suite:** openCypher TCK 2024.3.

**Pass rate:** 100.0%: 3570 passed, 310 skipped by design, 0 failed, of 3880 cases.

<!-- conformance-results:begin -->
## Skipped by design

| Skipped by design | Count | Decision |
| --- | --- | --- |
| Labels the model does not have | 27 | A label no table the role reads has is refused by name (cypher.unknown_label), offering the registered labels; it is never read as an empty match. |
| Relationship types the model does not have | 36 | "SQL and Cypher queries that traverse a join not backed by a registered relationship are rejected at compile time." |
| Temporal values below the microsecond | 213 | "Every temporal value ... has microsecond precision ...; its nanosecond property is its microseconds times 1000." |
| Unbounded variable-length patterns | 30 | "Maximum hop depth is enforced at compile time; unbounded `[*]` is rejected." |
| Dates beyond the years Provisa holds | 2 | TEMPORAL VALUES ARE BOUNDED BY THE ENGINE AND IANA: a date beyond years 0001 to 9999 is refused by name. |
| Times at offsets beyond the engine's | 1 | TEMPORAL VALUES ARE BOUNDED BY THE ENGINE AND IANA: a time at an offset of 16 hours or more is refused by name. |
| Historical zone offsets as IANA gives them | 1 | TEMPORAL VALUES ARE BOUNDED BY THE ENGINE AND IANA: a named zone's offset is the IANA time zone database's (Europe/Stockholm in 1818 is +01:12:12), not another database's. |

<details><summary>Labels the model does not have (27)</summary>

| Case | Reason |
| --- | --- |
| Delete1 - Deleting nodes::[5] Ignore null when deleting node | refused (400): There is no label :DoesNotExist: this role can read no table. [in a session, empty graph] |
| Match3 - Match fixed length patterns::[28] Matching from null nodes should return no results owing to matches being filtered out | refused (400): There is no label :TheLabel. Registered labels: :. [in a session] |
| Match5 - Match variable length patterns over given graphs scenarios::[25] Handling a variable length relationship and a standard relationship in chain, longer 3 | refused (400): setup: There is no label :D: this role can read no table. [in a session] |
| Match5 - Match variable length patterns over given graphs scenarios::[26] Handling mixed relationship patterns and directions 1 | refused (400): setup: There is no label :A: this role can read no table. [in a session] |
| Match5 - Match variable length patterns over given graphs scenarios::[27] Handling mixed relationship patterns and directions 2 | refused (400): setup: There is no label :D: this role can read no table. [in a session] |
| Match5 - Match variable length patterns over given graphs scenarios::[28] Handling mixed relationship patterns 1 | refused (400): setup: There is no label :D: this role can read no table. [in a session] |
| Match5 - Match variable length patterns over given graphs scenarios::[29] Handling mixed relationship patterns 2 | refused (400): setup: There is no label :D: this role can read no table. [in a session] |
| Match7 - Optional match::[22] MATCH after OPTIONAL MATCH | refused (400): There is no label :NonExistent. Registered labels: :Single, :C, :B. [in a session] |
| Match7 - Optional match::[28] Handling optional matches with inline label predicate | refused (400): There is no label :NonExistent. Registered labels: :Single, :C, :B. [in a session] |
| Remove1 - Remove a Property::[5] Ignore null when removing property from a node | refused (400): There is no label :DoesNotExist: this role can read no table. [in a session, empty graph] |
| Remove2 - Remove a Label::[5] Ignore null when removing a node label | refused (400): There is no label :DoesNotExist: this role can read no table. [in a session, empty graph] |
| Return6 - Implicit grouping with aggregates::[18] Handle returned variables inside an expression which contains an aggregation expression | refused (400): There is no label :Person: this role can read no table. [in a session, empty graph] |
| Return6 - Implicit grouping with aggregates::[19] Handle returned property accesses inside an expression which contains an aggregation expression | refused (400): There is no label :Person: this role can read no table. [in a session, empty graph] |
| ReturnOrderBy6 - Aggregation expressions in order by::[2] Handle returned aliases inside an order by item which contains an aggregation expression | refused (400): There is no label :Person: this role can read no table. [in a session, empty graph] |
| ReturnOrderBy6 - Aggregation expressions in order by::[3] Handle returned property accesses inside an order by item which contains an aggregation expression | refused (400): There is no label :Person: this role can read no table. [in a session, empty graph] |
| ReturnSkipLimit2 - Limit::[5] ORDER BY with LIMIT 0 should not generate errors | refused (400): There is no label :Person. Registered labels: :Terms, :Queries, :Experts. |
| Set1 - Set a Property::[8] Ignore null when setting property | refused (400): There is no label :DoesNotExist: this role can read no table. [in a session, empty graph] |
| Set3 - Set a Label::[8] Ignore null when setting label | refused (400): There is no label :DoesNotExist: this role can read no table. [in a session, empty graph] |
| Set4 - Set all properties with a map::[5] Ignore null when setting properties using an overriding map | refused (400): There is no label :DoesNotExist: this role can read no table. [in a session, empty graph] |
| Set5 - Set multiple properties with a map::[1] Ignore null when setting properties using an appending map | refused (400): There is no label :DoesNotExist: this role can read no table. [in a session, empty graph] |
| With1 - Forward single variable::[5] Forwarding null | refused (400): There is no label :Start: this role can read no table. [in a session, empty graph] |
| With6 - Implicit grouping with aggregates::[6] Handle projected variables inside an expression which contains an aggregation expression | refused (400): There is no label :Person: this role can read no table. [in a session, empty graph] |
| With6 - Implicit grouping with aggregates::[7] Handle projected property accesses inside an expression which contains an aggregation expression | refused (400): There is no label :Person: this role can read no table. [in a session, empty graph] |
| WithOrderBy4 - Order by in combination with projection and aliasing::[17] Handle projected variables inside an order by item which contains an aggregation expression | refused (400): There is no label :Person: this role can read no table. [in a session, empty graph] |
| WithOrderBy4 - Order by in combination with projection and aliasing::[18]  Handle projected property accesses inside an order by item which contains an aggregation expression | refused (400): There is no label :Person: this role can read no table. [in a session, empty graph] |
| Aggregation5 - Collect::[2] OPTIONAL MATCH and `collect()` on node property | refused (400): There is no label :DoesNotExist. Registered labels: :DoesExist. [in a session] |
| Graph3 - Node labels::[7] `labels()` on null node | refused (400): There is no label :DoesNotExist: this role can read no table. [in a session, empty graph] |

</details>

<details><summary>Relationship types the model does not have (36)</summary>

| Case | Reason |
| --- | --- |
| Delete2 - Deleting relationships::[4] Ignore null when deleting relationship | refused (400): There is no relationship type :DoesNotExist: this role can traverse no relationship. [in a session, empty graph] |
| Match2 - Match relationships::[7] Matching twice with conflicting relationship types on same relationship | refused (400): There is no relationship type :Y. Nearest registered types: :T. [in a session] |
| Match5 - Match variable length patterns over given graphs scenarios::[3] Handling single bounded variable length match 1 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[4] Handling single bounded variable length match 2 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[5] Handling single bounded variable length match 3 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[6] Handling upper and lower bounded variable length match 1 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[7] Handling upper and lower bounded variable length match 2 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[8] Handling symmetrically bounded variable length match, bounds are zero | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[9] Handling symmetrically bounded variable length match, bounds are one | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[10] Handling symmetrically bounded variable length match, bounds are two | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[11] Handling upper and lower bounded variable length match, empty interval 1 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[12] Handling upper and lower bounded variable length match, empty interval 2 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[13] Handling upper bounded variable length match, empty interval | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[14] Handling upper bounded variable length match 1 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[15] Handling upper bounded variable length match 2 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[16] Handling lower bounded variable length match 1 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[17] Handling lower bounded variable length match 2 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[18] Handling lower bounded variable length match 3 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[19] Handling a variable length relationship and a standard relationship in chain, zero length 1 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[20] Handling a variable length relationship and a standard relationship in chain, zero length 2 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[21] Handling a variable length relationship and a standard relationship in chain, single length 1 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[22] Handling a variable length relationship and a standard relationship in chain, single length 2 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[23] Handling a variable length relationship and a standard relationship in chain, longer 1 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match5 - Match variable length patterns over given graphs scenarios::[24] Handling a variable length relationship and a standard relationship in chain, longer 2 | refused (400): There is no relationship type :LIKES. Nearest registered types: :REPLACES, :DQ_RULES, :EDGES. |
| Match7 - Optional match::[2] OPTIONAL MATCH with previously bound nodes | refused (400): There is no relationship type :NOT_EXIST: this role can traverse no relationship. [in a session] |
| Match7 - Optional match::[16] Optionally matching named paths - null result | refused (400): There is no relationship type :X. Nearest registered types: :REL, :LOOP. [in a session] |
| Match7 - Optional match::[18] Named paths inside optional matches with node predicates | refused (400): There is no relationship type :X. Nearest registered types: :REL, :LOOP. [in a session] |
| Match7 - Optional match::[21] Handling optional matches between nulls | refused (400): There is no relationship type :NOR_THIS. Nearest registered types: :REL, :LOOP. [in a session] |
| Match7 - Optional match::[27] Handling optional matches between optionally matched entities | refused (400): There is no relationship type :NOR_THIS. Nearest registered types: :REL, :LOOP. [in a session] |
| Match7 - Optional match::[30] Satisfies the open world assumption, single relationship | refused (400): There is no relationship type :SUPPORTS. Nearest registered types: :PLAYS_FOR. [in a session] |
| Return6 - Implicit grouping with aggregates::[6] Handle aggregates inside non-aggregate expressions | refused (400): There is no relationship type :FATHER: this role can traverse no relationship. [in a session, empty graph] |
| Aggregation5 - Collect::[1] `collect()` filtering nulls | refused (400): There is no relationship type :NOT_EXIST: this role can traverse no relationship. [in a session] |
| ExistentialSubquery1 - Simple existential subquery::[3] Simple subquery without WHERE clause, not existing pattern | refused (400): There is no relationship type :NA. Nearest registered types: :R. [in a session] |
| Graph4 - Edge relationship type::[3] `type()` on null relationship | refused (400): There is no relationship type :NOT_THERE: this role can traverse no relationship. [in a session] |
| Graph5 - Node and edge label expressions::[5] Label expression on null | refused (400): There is no relationship type :TYPE: this role can traverse no relationship. [in a session] |
| Graph9 - Retrieve all properties as a property map::[3] `properties()` on null | refused (400): There is no relationship type :NOT_THERE: this role can traverse no relationship. [in a session, empty graph] |

</details>

<details><summary>Temporal values below the microsecond (213)</summary>

| Case | Reason |
| --- | --- |
| WithOrderBy1 - Order by a single variable::[13] Sort local times in ascending order | expects digits below the microsecond; rows differ: got [['10:35'], ['12:30:14.645876'], ['12:31:14.645876']] |
| WithOrderBy1 - Order by a single variable::[14] Sort local times in descending order | expects digits below the microsecond; rows differ: got [['12:35:13'], ['12:31:14.645876'], ['12:31:14.645876']] |
| WithOrderBy1 - Order by a single variable::[15] Sort times in ascending order | expects digits below the microsecond; rows differ: got [['12:35:15+05:00'], ['12:30:14.645876+01:01'], ['12:31:14.645876+01:00']] |
| WithOrderBy1 - Order by a single variable::[16] Sort times in descending order | expects digits below the microsecond; rows differ: got [['10:35-08:00'], ['12:31:14.645876+01:00'], ['12:31:14.645876+01:00']] |
| WithOrderBy1 - Order by a single variable::[17] Sort local date times in ascending order | expects digits below the microsecond; rows differ: got [['0001-01-01T01:01:01'], ['1980-12-11T12:31:14'], ['1984-10-11T12:30:14']] |
| WithOrderBy1 - Order by a single variable::[18] Sort local date times in descending order | expects digits below the microsecond; rows differ: got [['9999-09-09T09:59:59.999999'], ['1984-10-11T12:31:14.645876'], ['1984-10-11T12:30:14']] |
| WithOrderBy1 - Order by a single variable::[19] Sort date times in ascending order | expects digits below the microsecond; rows differ: got [['0001-01-01T13:00:01Z'], ['1980-12-12T00:30:14Z'], ['1984-10-11T12:14:14.645876Z']] |
| WithOrderBy1 - Order by a single variable::[20] Sort date times in descending order | expects digits below the microsecond; rows differ: got [['9999-09-08T22:00:59.999999Z'], ['1984-10-11T12:15:14Z'], ['1984-10-11T12:14:14.645876Z']] |
| WithOrderBy1 - Order by a single variable::[35] Sort by a local time variable projected from a node property in ascending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00'}), '10:35'], [('node', frozenset({'D'}), {'time': '12:30:14.645876'}), '12:30:14.645876'], [('node', frozenset({'B'}), {'time': '12:31:14.645876'}), '12:31:14.645876']] [in a session] |
| WithOrderBy1 - Order by a single variable::[35] Sort by a local time variable projected from a node property in ascending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00'}), '10:35'], [('node', frozenset({'D'}), {'time': '12:30:14.645876'}), '12:30:14.645876'], [('node', frozenset({'C'}), {'time': '12:31:14.645876'}), '12:31:14.645876']] [in a session] |
| WithOrderBy1 - Order by a single variable::[35] Sort by a local time variable projected from a node property in ascending order #3 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00'}), '10:35'], [('node', frozenset({'D'}), {'time': '12:30:14.645876'}), '12:30:14.645876'], [('node', frozenset({'C'}), {'time': '12:31:14.645876'}), '12:31:14.645876']] [in a session] |
| WithOrderBy1 - Order by a single variable::[36] Sort by a local time variable projected from a node property in descending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'E'}), {'time': '12:31:15'}), '12:31:15'], [('node', frozenset({'C'}), {'time': '12:31:14.645876'}), '12:31:14.645876'], [('node', frozenset({'B'}), {'time': '12:31:14.645876'}), '12:31:14.645876']] [in a session] |
| WithOrderBy1 - Order by a single variable::[36] Sort by a local time variable projected from a node property in descending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'E'}), {'time': '12:31:15'}), '12:31:15'], [('node', frozenset({'B'}), {'time': '12:31:14.645876'}), '12:31:14.645876'], [('node', frozenset({'C'}), {'time': '12:31:14.645876'}), '12:31:14.645876']] [in a session] |
| WithOrderBy1 - Order by a single variable::[37] Sort by a time variable projected from a node property in ascending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'time': '12:35:15+05'}), '12:35:15+05:00'], [('node', frozenset({'E'}), {'time': '12:30:14.645876+01:01'}), '12:30:14.645876+01:01'], [('node', frozenset({'B'}), {'time': '12:31:14.645876+01'}), '12:31:14.645876+01:00']] [in a session] |
| WithOrderBy1 - Order by a single variable::[37] Sort by a time variable projected from a node property in ascending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'time': '12:35:15+05'}), '12:35:15+05:00'], [('node', frozenset({'E'}), {'time': '12:30:14.645876+01:01'}), '12:30:14.645876+01:01'], [('node', frozenset({'C'}), {'time': '12:31:14.645876+01'}), '12:31:14.645876+01:00']] [in a session] |
| WithOrderBy1 - Order by a single variable::[37] Sort by a time variable projected from a node property in ascending order #3 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'time': '12:35:15+05'}), '12:35:15+05:00'], [('node', frozenset({'E'}), {'time': '12:30:14.645876+01:01'}), '12:30:14.645876+01:01'], [('node', frozenset({'B'}), {'time': '12:31:14.645876+01'}), '12:31:14.645876+01:00']] [in a session] |
| WithOrderBy1 - Order by a single variable::[38] Sort by a time variable projected from a node property in descending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00-08'}), '10:35-08:00'], [('node', frozenset({'B'}), {'time': '12:31:14.645876+01'}), '12:31:14.645876+01:00'], [('node', frozenset({'C'}), {'time': '12:31:14.645876+01'}), '12:31:14.645876+01:00']] [in a session] |
| WithOrderBy1 - Order by a single variable::[38] Sort by a time variable projected from a node property in descending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00-08'}), '10:35-08:00'], [('node', frozenset({'C'}), {'time': '12:31:14.645876+01'}), '12:31:14.645876+01:00'], [('node', frozenset({'B'}), {'time': '12:31:14.645876+01'}), '12:31:14.645876+01:00']] [in a session] |
| WithOrderBy1 - Order by a single variable::[39] Sort by a local date time variable projected from a node property in ascending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T01:01:01'}), '0001-01-01T01:01:01'], [('node', frozenset({'E'}), {'datetime': '1980-12-11T12:31:14'}), '1980-12-11T12:31:14'], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'}), '1984-10-11T12:30:14']] [in a session] |
| WithOrderBy1 - Order by a single variable::[39] Sort by a local date time variable projected from a node property in ascending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T01:01:01'}), '0001-01-01T01:01:01'], [('node', frozenset({'E'}), {'datetime': '1980-12-11T12:31:14'}), '1980-12-11T12:31:14'], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'}), '1984-10-11T12:30:14']] [in a session] |
| WithOrderBy1 - Order by a single variable::[39] Sort by a local date time variable projected from a node property in ascending order #3 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T01:01:01'}), '0001-01-01T01:01:01'], [('node', frozenset({'E'}), {'datetime': '1980-12-11T12:31:14'}), '1980-12-11T12:31:14'], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'}), '1984-10-11T12:30:14']] [in a session] |
| WithOrderBy1 - Order by a single variable::[40] Sort by a local date time variable projected from a node property in descending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'datetime': '9999-09-09T09:59:59.999999'}), '9999-09-09T09:59:59.999999'], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:31:14.645876'}), '1984-10-11T12:31:14.645876'], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'}), '1984-10-11T12:30:14']] [in a session] |
| WithOrderBy1 - Order by a single variable::[40] Sort by a local date time variable projected from a node property in descending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'datetime': '9999-09-09T09:59:59.999999'}), '9999-09-09T09:59:59.999999'], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:31:14.645876'}), '1984-10-11T12:31:14.645876'], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'}), '1984-10-11T12:30:14']] [in a session] |
| WithOrderBy1 - Order by a single variable::[41] Sort by a date time variable projected from a node property in ascending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T13:00:01+00:00'}), '0001-01-01T13:00:01Z'], [('node', frozenset({'E'}), {'datetime': '1980-12-12T00:30:14+00:00'}), '1980-12-12T00:30:14Z'], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'}), '1984-10-11T12:14:14.645876Z']] [in a session] |
| WithOrderBy1 - Order by a single variable::[41] Sort by a date time variable projected from a node property in ascending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T13:00:01+00:00'}), '0001-01-01T13:00:01Z'], [('node', frozenset({'E'}), {'datetime': '1980-12-12T00:30:14+00:00'}), '1980-12-12T00:30:14Z'], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'}), '1984-10-11T12:14:14.645876Z']] [in a session] |
| WithOrderBy1 - Order by a single variable::[41] Sort by a date time variable projected from a node property in ascending order #3 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T13:00:01+00:00'}), '0001-01-01T13:00:01Z'], [('node', frozenset({'E'}), {'datetime': '1980-12-12T00:30:14+00:00'}), '1980-12-12T00:30:14Z'], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'}), '1984-10-11T12:14:14.645876Z']] [in a session] |
| WithOrderBy1 - Order by a single variable::[42] Sort by a date time variable projected from a node property in descending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'datetime': '9999-09-08T22:01:00+00:00'}), '9999-09-08T22:01Z'], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:15:14+00:00'}), '1984-10-11T12:15:14Z'], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'}), '1984-10-11T12:14:14.645876Z']] [in a session] |
| WithOrderBy1 - Order by a single variable::[42] Sort by a date time variable projected from a node property in descending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'datetime': '9999-09-08T22:01:00+00:00'}), '9999-09-08T22:01Z'], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:15:14+00:00'}), '1984-10-11T12:15:14Z'], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'}), '1984-10-11T12:14:14.645876Z']] [in a session] |
| WithOrderBy1 - Order by a single variable::[45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #7 | expects digits below the microsecond; rows differ: got [[False]] |
| WithOrderBy1 - Order by a single variable::[45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #8 | expects digits below the microsecond; rows differ: got [[False]] |
| WithOrderBy2 - Order by a single expression::[13] Sort by a local time expression in ascending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00'})], [('node', frozenset({'D'}), {'time': '12:30:14.645876'})], [('node', frozenset({'C'}), {'time': '12:31:14.645876'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[13] Sort by a local time expression in ascending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00'})], [('node', frozenset({'D'}), {'time': '12:30:14.645876'})], [('node', frozenset({'B'}), {'time': '12:31:14.645876'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[13] Sort by a local time expression in ascending order #3 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00'})], [('node', frozenset({'D'}), {'time': '12:30:14.645876'})], [('node', frozenset({'C'}), {'time': '12:31:14.645876'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[14] Sort by a local time expression in descending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'E'}), {'time': '12:31:15'})], [('node', frozenset({'C'}), {'time': '12:31:14.645876'})], [('node', frozenset({'B'}), {'time': '12:31:14.645876'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[14] Sort by a local time expression in descending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'E'}), {'time': '12:31:15'})], [('node', frozenset({'C'}), {'time': '12:31:14.645876'})], [('node', frozenset({'B'}), {'time': '12:31:14.645876'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[15] Sort by a time expression in ascending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'time': '12:35:15+05'})], [('node', frozenset({'E'}), {'time': '12:30:14.645876+01:01'})], [('node', frozenset({'C'}), {'time': '12:31:14.645876+01'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[15] Sort by a time expression in ascending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'time': '12:35:15+05'})], [('node', frozenset({'E'}), {'time': '12:30:14.645876+01:01'})], [('node', frozenset({'C'}), {'time': '12:31:14.645876+01'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[15] Sort by a time expression in ascending order #3 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'time': '12:35:15+05'})], [('node', frozenset({'E'}), {'time': '12:30:14.645876+01:01'})], [('node', frozenset({'C'}), {'time': '12:31:14.645876+01'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[16] Sort by a time expression in descending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00-08'})], [('node', frozenset({'C'}), {'time': '12:31:14.645876+01'})], [('node', frozenset({'B'}), {'time': '12:31:14.645876+01'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[16] Sort by a time expression in descending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'A'}), {'time': '10:35:00-08'})], [('node', frozenset({'C'}), {'time': '12:31:14.645876+01'})], [('node', frozenset({'B'}), {'time': '12:31:14.645876+01'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[17] Sort by a local date time expression in ascending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T01:01:01'})], [('node', frozenset({'E'}), {'datetime': '1980-12-11T12:31:14'})], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[17] Sort by a local date time expression in ascending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T01:01:01'})], [('node', frozenset({'E'}), {'datetime': '1980-12-11T12:31:14'})], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[17] Sort by a local date time expression in ascending order #3 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T01:01:01'})], [('node', frozenset({'E'}), {'datetime': '1980-12-11T12:31:14'})], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[18] Sort by a local date time expression in descending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'datetime': '9999-09-09T09:59:59.999999'})], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:31:14.645876'})], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[18] Sort by a local date time expression in descending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'datetime': '9999-09-09T09:59:59.999999'})], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:31:14.645876'})], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:30:14'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[19] Sort by a date time expression in ascending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T13:00:01+00:00'})], [('node', frozenset({'E'}), {'datetime': '1980-12-12T00:30:14+00:00'})], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[19] Sort by a date time expression in ascending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T13:00:01+00:00'})], [('node', frozenset({'E'}), {'datetime': '1980-12-12T00:30:14+00:00'})], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[19] Sort by a date time expression in ascending order #3 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'C'}), {'datetime': '0001-01-01T13:00:01+00:00'})], [('node', frozenset({'E'}), {'datetime': '1980-12-12T00:30:14+00:00'})], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[20] Sort by a date time expression in descending order #1 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'datetime': '9999-09-08T22:01:00+00:00'})], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:15:14+00:00'})], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'})]] [in a session] |
| WithOrderBy2 - Order by a single expression::[20] Sort by a date time expression in descending order #2 | expects digits below the microsecond; rows differ: got [[('node', frozenset({'D'}), {'datetime': '9999-09-08T22:01:00+00:00'})], [('node', frozenset({'A'}), {'datetime': '1984-10-11T12:15:14+00:00'})], [('node', frozenset({'B'}), {'datetime': '1984-10-11T12:14:14.645876+00:00'})]] [in a session] |
| Temporal1 - Create Temporal Values from a Map::[5] Should construct local time #1 | expects digits below the microsecond; rows differ: got [['12:31:14.123456']] |
| Temporal1 - Create Temporal Values from a Map::[5] Should construct local time #2 | expects digits below the microsecond; rows differ: got [['12:31:14.645876']] |
| Temporal1 - Create Temporal Values from a Map::[6] Should construct time #1 | expects digits below the microsecond; rows differ: got [['12:31:14.123456Z']] |
| Temporal1 - Create Temporal Values from a Map::[6] Should construct time #2 | expects digits below the microsecond; rows differ: got [['12:31:14.645876Z']] |
| Temporal1 - Create Temporal Values from a Map::[6] Should construct time #3 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal1 - Create Temporal Values from a Map::[6] Should construct time #9 | expects digits below the microsecond; rows differ: got [['12:31:14.645876+01:00']] |
| Temporal1 - Create Temporal Values from a Map::[7] Should construct local date time #1 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.123456']] |
| Temporal1 - Create Temporal Values from a Map::[7] Should construct local date time #2 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876']] |
| Temporal1 - Create Temporal Values from a Map::[7] Should construct local date time #3 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14']] |
| Temporal1 - Create Temporal Values from a Map::[7] Should construct local date time #10 | expects digits below the microsecond; rows differ: got [['1984-03-07T12:31:14.645876']] |
| Temporal1 - Create Temporal Values from a Map::[7] Should construct local date time #17 | expects digits below the microsecond; rows differ: got [['1984-07-20T12:31:14.645876']] |
| Temporal1 - Create Temporal Values from a Map::[7] Should construct local date time #24 | expects digits below the microsecond; rows differ: got [['1984-08-14T12:31:14.645876']] |
| Temporal1 - Create Temporal Values from a Map::[8] Should construct date time with default time zone #1 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.123456Z']] |
| Temporal1 - Create Temporal Values from a Map::[8] Should construct date time with default time zone #2 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876Z']] |
| Temporal1 - Create Temporal Values from a Map::[8] Should construct date time with default time zone #9 | expects digits below the microsecond; rows differ: got [['1984-03-07T12:31:14.645876Z']] |
| Temporal1 - Create Temporal Values from a Map::[8] Should construct date time with default time zone #16 | expects digits below the microsecond; rows differ: got [['1984-07-20T12:31:14.645876Z']] |
| Temporal1 - Create Temporal Values from a Map::[8] Should construct date time with default time zone #23 | expects digits below the microsecond; rows differ: got [['1984-08-14T12:31:14.645876Z']] |
| Temporal1 - Create Temporal Values from a Map::[9] Should construct date time with offset time zone #1 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876+01:00']] |
| Temporal1 - Create Temporal Values from a Map::[9] Should construct date time with offset time zone #8 | expects digits below the microsecond; rows differ: got [['1984-03-07T12:31:14.645876+01:00']] |
| Temporal1 - Create Temporal Values from a Map::[9] Should construct date time with offset time zone #15 | expects digits below the microsecond; rows differ: got [['1984-07-20T12:31:14.645876+01:00']] |
| Temporal1 - Create Temporal Values from a Map::[9] Should construct date time with offset time zone #22 | expects digits below the microsecond; rows differ: got [['1984-08-14T12:31:14.645876+01:00']] |
| Temporal1 - Create Temporal Values from a Map::[10] Should construct date time with named time zone #1 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876+01:00[Europe/Stockholm]']] |
| Temporal1 - Create Temporal Values from a Map::[10] Should construct date time with named time zone #8 | expects digits below the microsecond; rows differ: got [['1984-03-07T12:31:14.645876+01:00[Europe/Stockholm]']] |
| Temporal1 - Create Temporal Values from a Map::[10] Should construct date time with named time zone #15 | expects digits below the microsecond; rows differ: got [['1984-07-20T12:31:14.645876+02:00[Europe/Stockholm]']] |
| Temporal1 - Create Temporal Values from a Map::[10] Should construct date time with named time zone #22 | expects digits below the microsecond; rows differ: got [['1984-08-14T12:31:14.645876+02:00[Europe/Stockholm]']] |
| Temporal1 - Create Temporal Values from a Map::[11] Should construct date time from epoch | expects digits below the microsecond; rows differ: got [['1970-01-05T19:46:19.999999Z', '1977-07-15T13:34:33.987Z']] |
| Temporal1 - Create Temporal Values from a Map::[12] Should construct duration #8 | expects digits below the microsecond; rows differ: got [['P14DT1M10S']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[2] Should select local time #1 | expects digits below the microsecond; rows differ: got [['12:31:14.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[2] Should select local time #2 | expects digits below the microsecond; rows differ: got [['12:31:14.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[2] Should select local time #3 | expects digits below the microsecond; rows differ: got [['12:31:42.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[3] Should select time #1 | expects digits below the microsecond; rows differ: got [['12:31:14.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[3] Should select time #2 | expects digits below the microsecond; rows differ: got [['12:31:14.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[3] Should select time #3 | expects digits below the microsecond; rows differ: got [['12:31:14.645876+05:00']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[3] Should select time #4 | expects digits below the microsecond; rows differ: got [['12:31:42.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[3] Should select time #5 | expects digits below the microsecond; rows differ: got [['12:31:42.645876+05:00']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[5] Should select time into local date time #1 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[5] Should select time into local date time #2 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:42.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[6] Should select date and time into local date time #1 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[6] Should select date and time into local date time #2 | expects digits below the microsecond; rows differ: got [['1984-10-28T12:31:42.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[6] Should select date and time into local date time #9 | expects digits below the microsecond; rows differ: got [['1984-03-07T12:31:14.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[6] Should select date and time into local date time #10 | expects digits below the microsecond; rows differ: got [['1984-03-28T12:31:42.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[6] Should select date and time into local date time #17 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[6] Should select date and time into local date time #18 | expects digits below the microsecond; rows differ: got [['1984-10-28T12:31:42.645876']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[9] Should select time into date time #1 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[9] Should select time into date time #2 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876+05:00']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[9] Should select time into date time #3 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:42.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[9] Should select time into date time #4 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:42.645876-10:00[Pacific/Honolulu]']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #1 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #2 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876+05:00']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #3 | expects digits below the microsecond; rows differ: got [['1984-10-28T12:31:42.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #4 | expects digits below the microsecond; rows differ: got [['1984-10-28T12:31:42.645876-10:00[Pacific/Honolulu]']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #17 | expects digits below the microsecond; rows differ: got [['1984-03-07T12:31:14.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #18 | expects digits below the microsecond; rows differ: got [['1984-03-07T12:31:14.645876+05:00']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #19 | expects digits below the microsecond; rows differ: got [['1984-03-28T12:31:42.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #20 | expects digits below the microsecond; rows differ: got [['1984-03-28T12:31:42.645876-10:00[Pacific/Honolulu]']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #33 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #34 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14.645876+05:00']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #35 | expects digits below the microsecond; rows differ: got [['1984-10-28T12:31:42.645876Z']] |
| Temporal3 - Project Temporal Values from other Temporal Values::[10] Should select date and time into date time #36 | expects digits below the microsecond; rows differ: got [['1984-10-28T12:31:42.645876-10:00[Pacific/Honolulu]']] |
| Temporal5 - Access Components of Temporal Values::[3] Should provide accessors for local time | expects digits below the microsecond; rows differ: got [[12, 31, 14, 645, 645876, 645876000]] [in a session] |
| Temporal5 - Access Components of Temporal Values::[4] Should provide accessors for time | expects digits below the microsecond; rows differ: got [[12, 31, 14, 645, 645876, 645876000, '+01:00', '+01:00', 60, 3600]] [in a session] |
| Temporal5 - Access Components of Temporal Values::[5] Should provide accessors for local date time | expects digits below the microsecond; rows differ: got [[1984, 4, 11, 45, 1984, 11, 316, 7, 42, 12, 31, 14, 645, 645876, 645876000]] [in a session] |
| Temporal5 - Access Components of Temporal Values::[6] Should provide accessors for date time | expects digits below the microsecond; rows differ: got [[1984, 4, 11, 45, 1984, 11, 316, 7, 42, 12, 31, 14, 645, 645876, 645876000, 'Europe/Stockholm', '+01:00', 60, 3600, 469020674, 469020674645]] [in a session] |
| Temporal5 - Access Components of Temporal Values::[7] Should provide accessors for duration | expects digits below the microsecond; rows differ: got [[1, 5, 16, 1, 10, 1, 61, 3661, 3661111, 3661111111, 3661111111000, 1, 1, 4, 3, 1, 1, 111, 111111, 111111000]] [in a session] |
| Temporal6 - Render Temporal Values as a String::[2] Should serialize local time | expects digits below the microsecond; rows differ: got [['12:31:14.645876', True]] |
| Temporal6 - Render Temporal Values as a String::[3] Should serialize time | expects digits below the microsecond; rows differ: got [['12:31:14.645876+01', True]] |
| Temporal6 - Render Temporal Values as a String::[4] Should serialize local date time | expects digits below the microsecond; rows differ: got [['1984-10-11 12:31:14.645876', True]] |
| Temporal6 - Render Temporal Values as a String::[5] Should serialize date time | expects digits below the microsecond; rows differ: got [['1984-10-11 11:31:14.645876+00', True]] |
| Temporal6 - Render Temporal Values as a String::[6] Should serialize duration #1 | expects digits below the microsecond; rows differ: got [['P12Y5M14DT16H13M10S', True]] |
| Temporal6 - Render Temporal Values as a String::[7] Should serialize timezones correctly | expects digits below the microsecond; rows differ: got [['2017-08-08 10:31:14.645876+00']] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[2] Should add or subtract duration to or from local time #1 | expects digits below the microsecond; rows differ: got [['04:44:24', '20:18:04']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[2] Should add or subtract duration to or from local time #2 | expects digits below the microsecond; rows differ: got [['04:20:24', '20:42:04']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[2] Should add or subtract duration to or from local time #3 | expects digits below the microsecond; rows differ: got [['22:29:27.5', '02:33:00.5']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[3] Should add or subtract duration to or from time #1 | expects digits below the microsecond; rows differ: got [['04:44:24+01:00', '20:18:04+01:00']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[3] Should add or subtract duration to or from time #2 | expects digits below the microsecond; rows differ: got [['04:20:24+01:00', '20:42:04+01:00']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[3] Should add or subtract duration to or from time #3 | expects digits below the microsecond; rows differ: got [['22:29:27.5+01:00', '02:33:00.5+01:00']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[4] Should add or subtract duration to or from local date time #1 | expects digits below the microsecond; rows differ: got [['1997-03-26T04:44:24', '1972-04-26T20:18:04']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[4] Should add or subtract duration to or from local date time #2 | expects digits below the microsecond; rows differ: got [['1984-10-29T04:20:24', '1984-09-24T20:42:04']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[4] Should add or subtract duration to or from local date time #3 | expects digits below the microsecond; rows differ: got [['1997-10-11T22:29:27.5', '1971-10-12T02:33:00.5']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[5] Should add or subtract duration to or from date time #1 | expects digits below the microsecond; rows differ: got [['1997-03-26T04:44:24+01:00', '1972-04-26T20:18:04+01:00']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[5] Should add or subtract duration to or from date time #2 | expects digits below the microsecond; rows differ: got [['1984-10-29T04:20:24+01:00', '1984-09-24T20:42:04+01:00']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[5] Should add or subtract duration to or from date time #3 | expects digits below the microsecond; rows differ: got [['1997-10-11T22:29:27.5+01:00', '1971-10-12T02:33:00.5+01:00']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #1 | expects digits below the microsecond; rows differ: got [['P24Y10M28DT32H26M20S', 'PT0S']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #2 | expects digits below the microsecond; rows differ: got [['P12Y6MT32H2M20S', 'P12Y4M28DT24M']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #3 | expects digits below the microsecond; rows differ: got [['P25Y4M43DT50H11M23.5S', 'P-6M-15DT-17H-45M-3.5S']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #4 | expects digits below the microsecond; rows differ: got [['P12Y6MT32H2M20S', 'P-12Y-4M-28DT-24M']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #6 | expects digits below the microsecond; rows differ: got [['P13Y15DT49H47M23.5S', 'P-12Y-10M-43DT-18H-9M-3.5S']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #7 | expects digits below the microsecond; rows differ: got [['P25Y4M43DT50H11M23.5S', 'P6M15DT17H45M3.5S']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #8 | expects digits below the microsecond; rows differ: got [['P13Y15DT49H47M23.5S', 'P12Y10M43DT18H9M3.5S']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #9 | expects digits below the microsecond; rows differ: got [['P25Y10M58DT67H56M27S', 'PT0S']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[7] Should multiply or divide durations by numbers #1 | expects digits below the microsecond; rows differ: got [['P12Y5M14DT16H13M10S', 'P12Y5M14DT16H13M10S']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[7] Should multiply or divide durations by numbers #2 | expects digits below the microsecond; rows differ: got [['P24Y10M28DT32H26M20S', 'P6Y2M22DT13H21M8S']] [in a session] |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[7] Should multiply or divide durations by numbers #3 | expects digits below the microsecond; rows differ: got [['P6Y2M22DT13H21M8S', 'P24Y10M28DT32H26M20S']] [in a session] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #73 | expects digits below the microsecond; rows differ: got [['1984-10-11T00:00Z']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #76 | expects digits below the microsecond; rows differ: got [['1984-10-11T00:00+01:00']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #79 | expects digits below the microsecond; rows differ: got [['1984-10-11T00:00Z']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #82 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:00-01:00']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #85 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:00Z']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #88 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31-01:00']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #91 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31Z']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #94 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14+01:00']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #96 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #98 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14+01:00']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #100 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #102 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14+01:00']] |
| Temporal9 - Truncate Temporal Values::[2] Should truncate datetime #104 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #49 | expects digits below the microsecond; rows differ: got [['1984-10-11T00:00']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #51 | expects digits below the microsecond; rows differ: got [['1984-10-11T00:00']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #53 | expects digits below the microsecond; rows differ: got [['1984-10-11T00:00']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #55 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:00']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #57 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:00']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #59 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #61 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #63 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #65 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #67 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #69 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #71 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14']] |
| Temporal9 - Truncate Temporal Values::[3] Should truncate localdatetime #73 | expects digits below the microsecond; rows differ: got [['1984-10-11T12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #1 | expects digits below the microsecond; rows differ: got [['00:00']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #3 | expects digits below the microsecond; rows differ: got [['00:00']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #5 | expects digits below the microsecond; rows differ: got [['12:00']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #7 | expects digits below the microsecond; rows differ: got [['12:00']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #9 | expects digits below the microsecond; rows differ: got [['12:00']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #11 | expects digits below the microsecond; rows differ: got [['12:00']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #13 | expects digits below the microsecond; rows differ: got [['12:31']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #15 | expects digits below the microsecond; rows differ: got [['12:31']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #17 | expects digits below the microsecond; rows differ: got [['12:31']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #19 | expects digits below the microsecond; rows differ: got [['12:31']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #21 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #23 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #25 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #27 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #29 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #31 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #33 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #35 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #37 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #39 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #41 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[4] Should truncate localtime #43 | expects digits below the microsecond; rows differ: got [['12:31:14']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #1 | expects digits below the microsecond; rows differ: got [['00:00Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #3 | expects digits below the microsecond; rows differ: got [['00:00Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #5 | expects digits below the microsecond; rows differ: got [['12:00Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #8 | expects digits below the microsecond; rows differ: got [['12:00Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #11 | expects digits below the microsecond; rows differ: got [['12:00Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #14 | expects digits below the microsecond; rows differ: got [['12:00Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #17 | expects digits below the microsecond; rows differ: got [['12:31Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #19 | expects digits below the microsecond; rows differ: got [['12:31Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #21 | expects digits below the microsecond; rows differ: got [['12:31Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #23 | expects digits below the microsecond; rows differ: got [['12:31Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #25 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #27 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #29 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #31 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #33 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #35 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #37 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #39 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #41 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #43 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #45 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |
| Temporal9 - Truncate Temporal Values::[5] Should truncate time #47 | expects digits below the microsecond; rows differ: got [['12:31:14Z']] |

</details>

<details><summary>Unbounded variable-length patterns (30)</summary>

| Case | Reason |
| --- | --- |
| Delete4 - Delete clause interoperation with other clauses::[2] Undirected variable length expand followed by delete and count | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match4 - Match variable length patterns scenarios::[2] Simple variable length pattern | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match4 - Match variable length patterns scenarios::[4] Matching longer variable length paths | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match4 - Match variable length patterns scenarios::[5] Matching variable length pattern with property predicate | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match4 - Match variable length patterns scenarios::[8] Matching relationships into a list and matching variable length using the list | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match5 - Match variable length patterns over given graphs scenarios::[1] Handling unbounded variable length match | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. |
| Match5 - Match variable length patterns over given graphs scenarios::[2] Handling explicitly unbounded variable length match | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. |
| Match6 - Match named paths scenarios::[20] Variable length relationship without bounds | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match7 - Optional match::[12] Variable length optional relationships | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match7 - Optional match::[13] Variable length optional relationships with bound nodes | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match7 - Optional match::[15] Variable length patterns and nulls | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match7 - Optional match::[19] Optionally matching named paths with single and variable length patterns | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match7 - Optional match::[20] Variable length optional relationships with bound nodes, no matches | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match9 - Match deprecated scenarios::[5] Variable length pattern with label predicate on both sides | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match9 - Match deprecated scenarios::[6] Matching relationships into a list and matching variable length using the list, with bound nodes | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match9 - Match deprecated scenarios::[7] Matching relationships into a list and matching variable length using the list, with bound nodes, wrong direction | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match9 - Match deprecated scenarios::[8] Variable length relationship in OPTIONAL MATCH | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Match9 - Match deprecated scenarios::[9] Optionally matching named paths with variable length patterns | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| MatchWhere4 - Non-Equi-Joins on variables::[2] Join with disjunctive multi-part predicates including patterns | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Return6 - Implicit grouping with aggregates::[8] Handle aggregation on functions | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Return6 - Implicit grouping with aggregates::[13] Returning the minimum length of paths | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[12] Aggregation of named paths | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| With6 - Implicit grouping with aggregates::[4] Implicit grouping with single path variable as grouping key and single aggregation | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| WithWhere4 - Non-Equi-Joins on variables::[2] Join with disjunctive multi-part predicates including patterns | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Pattern1 - Pattern predicate::[7] Matching on a specific type of a variable length outgoing directed connection | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Pattern1 - Pattern predicate::[8] Matching on a specific type of variable length undirected connection | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Pattern1 - Pattern predicate::[9] Matching on a specific type of variable length incoming directed connection | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Pattern1 - Pattern predicate::[16] Matching two nodes on a specific type of a variable length outgoing directed connection | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Pattern1 - Pattern predicate::[17] Matching two nodes on a specific type of variable length undirected connection | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |
| Pattern2 - Pattern Comprehension::[9] Use a variable-length pattern comprehension in WITH | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. [in a session] |

</details>

<details><summary>Dates beyond the years Provisa holds (2)</summary>

| Case | Reason |
| --- | --- |
| Temporal10 - Compute Durations Between two Temporal Values::[9] Should handle large durations | refused (400): '-999999999-01-01' is beyond the dates Provisa holds, years 0001 to 9999 |
| Temporal10 - Compute Durations Between two Temporal Values::[10] Should handle large durations in seconds | refused (400): '-999999999-01-01' is beyond the dates Provisa holds, years 0001 to 9999 |

</details>

<details><summary>Times at offsets beyond the engine's (1)</summary>

| Case | Reason |
| --- | --- |
| Temporal2 - Create Temporal Values from a String::[3] Should parse time from string #8 | refused (400): '22+18:00' is at an offset beyond the times Provisa holds, -15:59:59 to +15:59:59 |

</details>

<details><summary>Historical zone offsets as IANA gives them (1)</summary>

| Case | Reason |
| --- | --- |
| Temporal2 - Create Temporal Values from a String::[6] Should parse date time with named time zone from string #5 | rows differ: got [['1818-07-21T21:40:32.142+01:12:12[Europe/Stockholm]']] |

</details>


## Adapted cases

<details><summary>Setups that build their graph (784 of 900 passed)</summary>

Setup became: the scenario's setup and its query run in one ephemeral session over Bolt, acting as a role that reaches no model domain: the setup's CREATEs make the session's own nodes and relationships, and the query reads them.

| Case | Outcome |
| --- | --- |
| Call1 - Basic procedure calling::[4] In-query call to procedure that takes no arguments and yields no results and consumes no rows | passed |
| Create2 - Creating relationships::[5] Create a single relationship between two existing nodes | passed |
| Create2 - Creating relationships::[6] Create a single relationship between two existing nodes in the reverse direction | passed |
| Create2 - Creating relationships::[10] Create a single self loop on an existing node | passed |
| Create2 - Creating relationships::[11] Create a single relationship and an end node on an existing starting node | passed |
| Create2 - Creating relationships::[12] Create a single relationship and a starting node on an existing end node | passed |
| Create3 - Interoperation with other clauses::[1] MATCH-CREATE | passed |
| Create3 - Interoperation with other clauses::[2] WITH-CREATE | passed |
| Create3 - Interoperation with other clauses::[3] MATCH-CREATE-WITH-CREATE | passed |
| Create3 - Interoperation with other clauses::[4] MATCH-CREATE: Newly-created nodes not visible to preceding MATCH | passed |
| Create3 - Interoperation with other clauses::[5] WITH-CREATE: Nodes are not created when aliases are applied to variable names | passed |
| Create3 - Interoperation with other clauses::[6] WITH-CREATE: Only a single node is created when an alias is applied to a variable name | passed |
| Create3 - Interoperation with other clauses::[7] WITH-CREATE: Nodes are not created when aliases are applied to variable names multiple times | passed |
| Create3 - Interoperation with other clauses::[8] WITH-CREATE: Only a single node is created when an alias is applied to a variable name multiple times | passed |
| Delete1 - Deleting nodes::[1] Delete nodes | passed |
| Delete1 - Deleting nodes::[2] Detach delete node | passed |
| Delete1 - Deleting nodes::[3] Detach deleting connected nodes and relationships | passed |
| Delete1 - Deleting nodes::[7] Failing when deleting connected nodes | passed |
| Delete2 - Deleting relationships::[1] Delete relationships | passed |
| Delete2 - Deleting relationships::[2] Delete optionally matched relationship | passed |
| Delete2 - Deleting relationships::[3] Delete relationship with bidirectional matching | passed |
| Delete2 - Deleting relationships::[5] Failing when deleting a relationship type | passed |
| Delete3 - Deleting named paths::[1] Detach deleting paths | passed |
| Delete4 - Delete clause interoperation with other clauses::[1] Undirected expand followed by delete and count | passed |
| Delete4 - Delete clause interoperation with other clauses::[2] Undirected variable length expand followed by delete and count | failed |
| Delete4 - Delete clause interoperation with other clauses::[3] Create and delete in same query | passed |
| Delete5 - Delete clause interoperation with built-in data types::[1] Delete node from a list | passed |
| Delete5 - Delete clause interoperation with built-in data types::[2] Delete relationship from a list | passed |
| Delete5 - Delete clause interoperation with built-in data types::[3] Delete nodes from a map | passed |
| Delete5 - Delete clause interoperation with built-in data types::[4] Delete relationships from a map | passed |
| Delete5 - Delete clause interoperation with built-in data types::[5] Detach delete nodes from nested map/list | passed |
| Delete5 - Delete clause interoperation with built-in data types::[6] Delete relationships from nested map/list | passed |
| Delete5 - Delete clause interoperation with built-in data types::[7] Delete paths from nested map/list | passed |
| Delete6 - Persistence of delete clause side effects::[1] Limiting to zero results after deleting nodes affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[2] Skipping all results after deleting nodes affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[3] Skipping and limiting to a few results after deleting nodes affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[4] Skipping zero results and limiting to all results after deleting nodes does not affect the result set nor the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[5] Filtering after deleting nodes affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[6] Aggregating in `RETURN` after deleting nodes affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[7] Aggregating in `WITH` after deleting nodes affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[8] Limiting to zero results after deleting relationships affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[9] Skipping all results after deleting relationships affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[10] Skipping and limiting to a few results after deleting relationships affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[11] Skipping zero result and limiting to all results after deleting relationships does not affect the result set nor the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[12] Filtering after deleting relationships affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[13] Aggregating in `RETURN` after deleting relationships affects the result set but not the side effects | passed |
| Delete6 - Persistence of delete clause side effects::[14] Aggregating in `WITH` after deleting relationships affects the result set but not the side effects | passed |
| Match1 - Match nodes::[2] Matching all nodes | passed |
| Match1 - Match nodes::[3] Matching nodes using multiple labels | passed |
| Match1 - Match nodes::[4] Simple node inline property predicate | passed |
| Match1 - Match nodes::[5] Use multiple MATCH clauses to do a Cartesian product | passed |
| Match2 - Match relationships::[2] Matching a relationship pattern using a label predicate on both sides | passed |
| Match2 - Match relationships::[3] Matching a self-loop with an undirected relationship pattern | passed |
| Match2 - Match relationships::[4] Matching a self-loop with a directed relationship pattern | passed |
| Match2 - Match relationships::[5] Match relationship with inline property value | passed |
| Match2 - Match relationships::[6] Match relationships with multiple types | passed |
| Match2 - Match relationships::[7] Matching twice with conflicting relationship types on same relationship | failed |
| Match3 - Match fixed length patterns::[1] Get neighbours | passed |
| Match3 - Match fixed length patterns::[2] Directed match of a simple relationship | passed |
| Match3 - Match fixed length patterns::[3] Undirected match on simple relationship graph | passed |
| Match3 - Match fixed length patterns::[4] Get two related nodes | passed |
| Match3 - Match fixed length patterns::[5] Return two subgraphs with bound undirected relationship | passed |
| Match3 - Match fixed length patterns::[6] Matching a relationship pattern using a label predicate | passed |
| Match3 - Match fixed length patterns::[7] Matching nodes with many labels | passed |
| Match3 - Match fixed length patterns::[8] Matching using relationship predicate with multiples of the same type | passed |
| Match3 - Match fixed length patterns::[9] Get related to related to | passed |
| Match3 - Match fixed length patterns::[10] Matching using self-referencing pattern returns no result | passed |
| Match3 - Match fixed length patterns::[11] Undirected match in self-relationship graph | passed |
| Match3 - Match fixed length patterns::[12] Undirected match of self-relationship in self-relationship graph | passed |
| Match3 - Match fixed length patterns::[13] Directed match on self-relationship graph | passed |
| Match3 - Match fixed length patterns::[14] Directed match of self-relationship on self-relationship graph | passed |
| Match3 - Match fixed length patterns::[15] Mixing directed and undirected pattern parts with self-relationship, simple | passed |
| Match3 - Match fixed length patterns::[16] Mixing directed and undirected pattern parts with self-relationship, undirected | passed |
| Match3 - Match fixed length patterns::[17] Handling cyclic patterns | passed |
| Match3 - Match fixed length patterns::[18] Handling cyclic patterns when separated into two parts | passed |
| Match3 - Match fixed length patterns::[19] Two bound nodes pointing to the same node | passed |
| Match3 - Match fixed length patterns::[20] Three bound nodes pointing to the same node | passed |
| Match3 - Match fixed length patterns::[21] Three bound nodes pointing to the same node with extra connections | passed |
| Match3 - Match fixed length patterns::[22] Returning bound nodes that are not part of the pattern | passed |
| Match3 - Match fixed length patterns::[23] Matching disconnected patterns | passed |
| Match3 - Match fixed length patterns::[24] Matching twice with duplicate relationship types on same relationship | passed |
| Match3 - Match fixed length patterns::[25] Matching twice with an additional node label | passed |
| Match3 - Match fixed length patterns::[26] Matching twice with a duplicate predicate | passed |
| Match3 - Match fixed length patterns::[28] Matching from null nodes should return no results owing to matches being filtered out | failed |
| Match4 - Match variable length patterns scenarios::[1] Handling fixed-length variable length pattern | passed |
| Match4 - Match variable length patterns scenarios::[2] Simple variable length pattern | failed |
| Match4 - Match variable length patterns scenarios::[3] Zero-length variable length pattern in the middle of the pattern | passed |
| Match4 - Match variable length patterns scenarios::[4] Matching longer variable length paths | failed |
| Match4 - Match variable length patterns scenarios::[5] Matching variable length pattern with property predicate | failed |
| Match4 - Match variable length patterns scenarios::[6] Matching variable length patterns from a bound node | passed |
| Match4 - Match variable length patterns scenarios::[7] Matching variable length patterns including a bound relationship | passed |
| Match4 - Match variable length patterns scenarios::[8] Matching relationships into a list and matching variable length using the list | failed |
| Match4 - Match variable length patterns scenarios::[9] Fail when asterisk operator is missing | passed |
| Match4 - Match variable length patterns scenarios::[10] Fail on negative bound | passed |
| Match5 - Match variable length patterns over given graphs scenarios::[25] Handling a variable length relationship and a standard relationship in chain, longer 3 | failed |
| Match5 - Match variable length patterns over given graphs scenarios::[26] Handling mixed relationship patterns and directions 1 | failed |
| Match5 - Match variable length patterns over given graphs scenarios::[27] Handling mixed relationship patterns and directions 2 | failed |
| Match5 - Match variable length patterns over given graphs scenarios::[28] Handling mixed relationship patterns 1 | failed |
| Match5 - Match variable length patterns over given graphs scenarios::[29] Handling mixed relationship patterns 2 | failed |
| Match6 - Match named paths scenarios::[1] Zero-length named path | passed |
| Match6 - Match named paths scenarios::[2] Return a simple path | passed |
| Match6 - Match named paths scenarios::[3] Return a three node path | passed |
| Match6 - Match named paths scenarios::[4] Respecting direction when matching non-existent path | passed |
| Match6 - Match named paths scenarios::[5] Path query should return results in written order | passed |
| Match6 - Match named paths scenarios::[6] Handling direction of named paths | passed |
| Match6 - Match named paths scenarios::[7] Respecting direction when matching existing path | passed |
| Match6 - Match named paths scenarios::[8] Respecting direction when matching non-existent path with multiple directions | passed |
| Match6 - Match named paths scenarios::[9] Longer path query should return results in written order | passed |
| Match6 - Match named paths scenarios::[10] Named path with alternating directed/undirected relationships | passed |
| Match6 - Match named paths scenarios::[11] Named path with multiple alternating directed/undirected relationships | passed |
| Match6 - Match named paths scenarios::[12] Matching path with multiple bidirectional relationships | passed |
| Match6 - Match named paths scenarios::[13] Matching path with both directions should respect other directions | passed |
| Match6 - Match named paths scenarios::[14] Named path with undirected fixed variable length pattern | passed |
| Match6 - Match named paths scenarios::[15] Variable-length named path | passed |
| Match6 - Match named paths scenarios::[16] Return a var length path | passed |
| Match6 - Match named paths scenarios::[17] Return a named var length path of length zero | passed |
| Match6 - Match named paths scenarios::[18] Undirected named path | passed |
| Match6 - Match named paths scenarios::[19] Variable length relationship without lower bound | passed |
| Match6 - Match named paths scenarios::[20] Variable length relationship without bounds | failed |
| Match7 - Optional match::[2] OPTIONAL MATCH with previously bound nodes | failed |
| Match7 - Optional match::[3] OPTIONAL MATCH and bound nodes | passed |
| Match7 - Optional match::[4] Optionally matching relationship with bound nodes in reverse direction | passed |
| Match7 - Optional match::[5] Optionally matching relationship with a relationship that is already bound | passed |
| Match7 - Optional match::[6] Optionally matching relationship with a relationship and node that are both already bound | passed |
| Match7 - Optional match::[7] MATCH with OPTIONAL MATCH in longer pattern | passed |
| Match7 - Optional match::[8] Longer pattern with bound nodes without matches | passed |
| Match7 - Optional match::[9] Longer pattern with bound nodes | passed |
| Match7 - Optional match::[11] Return two subgraphs with bound undirected relationship and optional relationship | passed |
| Match7 - Optional match::[12] Variable length optional relationships | failed |
| Match7 - Optional match::[13] Variable length optional relationships with bound nodes | failed |
| Match7 - Optional match::[14] Variable length optional relationships with length predicates | passed |
| Match7 - Optional match::[15] Variable length patterns and nulls | failed |
| Match7 - Optional match::[16] Optionally matching named paths - null result | failed |
| Match7 - Optional match::[17] Optionally matching named paths - existing result | passed |
| Match7 - Optional match::[18] Named paths inside optional matches with node predicates | failed |
| Match7 - Optional match::[19] Optionally matching named paths with single and variable length patterns | failed |
| Match7 - Optional match::[20] Variable length optional relationships with bound nodes, no matches | failed |
| Match7 - Optional match::[21] Handling optional matches between nulls | failed |
| Match7 - Optional match::[22] MATCH after OPTIONAL MATCH | failed |
| Match7 - Optional match::[23] OPTIONAL MATCH with labels on the optional end node | passed |
| Match7 - Optional match::[24] Optionally matching self-loops | passed |
| Match7 - Optional match::[25] Optionally matching self-loops without matches | passed |
| Match7 - Optional match::[26] Handling correlated optional matches; first does not match implies second does not match | passed |
| Match7 - Optional match::[27] Handling optional matches between optionally matched entities | failed |
| Match7 - Optional match::[28] Handling optional matches with inline label predicate | failed |
| Match7 - Optional match::[29] Satisfies the open world assumption, relationships between same nodes | passed |
| Match7 - Optional match::[30] Satisfies the open world assumption, single relationship | failed |
| Match7 - Optional match::[31] Satisfies the open world assumption, relationships between different nodes | passed |
| Match8 - Match clause interoperation with other clauses::[1] Pattern independent of bound variables results in cross product | passed |
| Match8 - Match clause interoperation with other clauses::[2] Counting rows after MATCH, MERGE, OPTIONAL MATCH | passed |
| Match8 - Match clause interoperation with other clauses::[3] Matching and disregarding output, then matching again | passed |
| Match9 - Match deprecated scenarios::[1] Variable length relationship variables are lists of relationships | passed |
| Match9 - Match deprecated scenarios::[2] Return relationships by collecting them as a list - directed, one way | passed |
| Match9 - Match deprecated scenarios::[3] Return relationships by collecting them as a list - undirected, starting from two extremes | passed |
| Match9 - Match deprecated scenarios::[4] Return relationships by collecting them as a list - undirected, starting from one extreme | passed |
| Match9 - Match deprecated scenarios::[5] Variable length pattern with label predicate on both sides | failed |
| Match9 - Match deprecated scenarios::[6] Matching relationships into a list and matching variable length using the list, with bound nodes | failed |
| Match9 - Match deprecated scenarios::[7] Matching relationships into a list and matching variable length using the list, with bound nodes, wrong direction | failed |
| Match9 - Match deprecated scenarios::[8] Variable length relationship in OPTIONAL MATCH | failed |
| Match9 - Match deprecated scenarios::[9] Optionally matching named paths with variable length patterns | failed |
| MatchWhere1 - Filter single variable::[1] Filter node with node label predicate on multi variables with multiple bindings | passed |
| MatchWhere1 - Filter single variable::[2] Filter node with node label predicate on multi variables without any bindings | passed |
| MatchWhere1 - Filter single variable::[3] Filter node with property predicate on a single variable with multiple bindings | passed |
| MatchWhere1 - Filter single variable::[4] Filter start node of relationship with property predicate on multi variables with multiple bindings | passed |
| MatchWhere1 - Filter single variable::[5] Filter end node of relationship with property predicate on multi variables with multiple bindings | passed |
| MatchWhere1 - Filter single variable::[6] Filter node with a parameter in a property predicate on multi variables with one binding | passed |
| MatchWhere1 - Filter single variable::[7] Filter relationship with relationship type predicate on multi variables with multiple bindings | passed |
| MatchWhere1 - Filter single variable::[8] Filter relationship with property predicate on multi variables with multiple bindings | passed |
| MatchWhere1 - Filter single variable::[9] Filter relationship with a parameter in a property predicate on multi variables with one binding | passed |
| MatchWhere1 - Filter single variable::[10] Filter node with disjunctive property predicate on single variables with multiple bindings | passed |
| MatchWhere1 - Filter single variable::[11] Filter relationship with disjunctive relationship type predicate on multi variables with multiple bindings | passed |
| MatchWhere1 - Filter single variable::[12] Filter path with path length predicate on multi variables with one binding | passed |
| MatchWhere1 - Filter single variable::[13] Filter path with false path length predicate on multi variables with one binding | passed |
| MatchWhere2 - Filter multiple variables::[1] Filter nodes with conjunctive two-part property predicate on multi variables with multiple bindings | passed |
| MatchWhere2 - Filter multiple variables::[2] Filter node with conjunctive multi-part property predicates on multi variables with multiple bindings | passed |
| MatchWhere3 - Equi-Joins on variables::[1] Join between node identities | passed |
| MatchWhere3 - Equi-Joins on variables::[2] Join between node properties of disconnected nodes | passed |
| MatchWhere3 - Equi-Joins on variables::[3] Join between node properties of adjacent nodes | passed |
| MatchWhere4 - Non-Equi-Joins on variables::[1] Join nodes on inequality | passed |
| MatchWhere4 - Non-Equi-Joins on variables::[2] Join with disjunctive multi-part predicates including patterns | failed |
| MatchWhere5 - Filter on predicate resulting in null::[1] Filter out on null | passed |
| MatchWhere5 - Filter on predicate resulting in null::[2] Filter out on null if the AND'd predicate evaluates to false | passed |
| MatchWhere5 - Filter on predicate resulting in null::[3] Filter out on null if the AND'd predicate evaluates to true | passed |
| MatchWhere5 - Filter on predicate resulting in null::[4] Do not filter out on null if the OR'd predicate evaluates to true | passed |
| MatchWhere6 - Filter optional matches::[1] Filter node with node label predicate on multi variables with multiple bindings after MATCH and OPTIONAL MATCH | passed |
| MatchWhere6 - Filter optional matches::[2] Filter node with false node label predicate after OPTIONAL MATCH | passed |
| MatchWhere6 - Filter optional matches::[3] Filter node with property predicate on multi variables with multiple bindings after OPTIONAL MATCH | passed |
| MatchWhere6 - Filter optional matches::[4] Do not fail when predicates on optionally matched and missed nodes are invalid | passed |
| MatchWhere6 - Filter optional matches::[5] Matching and optionally matching with unbound nodes and equality predicate in reverse direction | passed |
| MatchWhere6 - Filter optional matches::[6] Join nodes on non-equality of properties – OPTIONAL MATCH and WHERE | passed |
| MatchWhere6 - Filter optional matches::[7] Join nodes on non-equality of properties – OPTIONAL MATCH on two relationships and WHERE | passed |
| MatchWhere6 - Filter optional matches::[8] Join nodes on non-equality of properties – Two OPTIONAL MATCH clauses and WHERE | passed |
| Merge1 - Merge node::[3] Merge node with label when it exists | passed |
| Merge1 - Merge node::[4] Merge node should create when it doesn't match, properties | passed |
| Merge1 - Merge node::[5] Merge node should create when it doesn't match, properties and label | passed |
| Merge1 - Merge node::[6] Merge node with prop and label | passed |
| Merge1 - Merge node::[8] Merge should handle argument properly | passed |
| Merge1 - Merge node::[9] Merge should support updates while merging | passed |
| Merge1 - Merge node::[10] Merge must properly handle multiple labels | passed |
| Merge1 - Merge node::[11] Merge should be able to merge using property of bound node | passed |
| Merge1 - Merge node::[14] Merges should not be able to match on deleted nodes | passed |
| Merge2 - Merge node - on create::[4] Merge node with label add property on update when it exists | passed |
| Merge2 - Merge node - on create::[5] Merge should be able to use properties of bound node in ON CREATE | passed |
| Merge3 - Merge node - on match::[1] Merge should be able to set labels on match | passed |
| Merge3 - Merge node - on match::[2] Merge node with label add label on match when it exists | passed |
| Merge3 - Merge node - on match::[3] Merge node and set property on match | passed |
| Merge3 - Merge node - on match::[4] Merge should be able to use properties of bound node in ON MATCH | passed |
| Merge4 - Merge node - on match and on create::[1] Merge should be able to set labels on match and on create | passed |
| Merge4 - Merge node - on match and on create::[2] Merge should be able to use properties of bound node in ON MATCH and ON CREATE | passed |
| Merge5 - Merge relationships::[1] Creating a relationship | passed |
| Merge5 - Merge relationships::[2] Matching a relationship | passed |
| Merge5 - Merge relationships::[3] Matching two relationships | passed |
| Merge5 - Merge relationships::[5] Filtering relationships | passed |
| Merge5 - Merge relationships::[6] Creating relationship when all matches filtered out | passed |
| Merge5 - Merge relationships::[7] Matching incoming relationship | passed |
| Merge5 - Merge relationships::[8] Creating relationship with property | passed |
| Merge5 - Merge relationships::[9] Creating relationship using merged nodes | passed |
| Merge5 - Merge relationships::[12] Match outgoing relationship when direction unspecified | passed |
| Merge5 - Merge relationships::[13] Match both incoming and outgoing relationships when direction unspecified | passed |
| Merge5 - Merge relationships::[15] Matching using list property | passed |
| Merge5 - Merge relationships::[16] Aliasing of existing nodes 1 | passed |
| Merge5 - Merge relationships::[17] Aliasing of existing nodes 2 | passed |
| Merge5 - Merge relationships::[18] Double aliasing of existing nodes 1 | passed |
| Merge5 - Merge relationships::[19] Double aliasing of existing nodes 2 | passed |
| Merge5 - Merge relationships::[20] Do not match on deleted entities | passed |
| Merge5 - Merge relationships::[21] Do not match on deleted relationships | passed |
| Merge6 - Merge relationships - on create::[1] Using ON CREATE on a node | passed |
| Merge6 - Merge relationships - on create::[2] Using ON CREATE on a relationship | passed |
| Merge6 - Merge relationships - on create::[3] Updating one property with ON CREATE | passed |
| Merge6 - Merge relationships - on create::[4] Null-setting one property with ON CREATE | passed |
| Merge6 - Merge relationships - on create::[6] Copying properties from node with ON CREATE | passed |
| Merge6 - Merge relationships - on create::[7] Copying properties from literal map with ON CREATE | passed |
| Merge7 - Merge relationships - on match::[1] Using ON MATCH on created node | passed |
| Merge7 - Merge relationships - on match::[2] Using ON MATCH on created relationship | passed |
| Merge7 - Merge relationships - on match::[3] Using ON MATCH on a relationship | passed |
| Merge7 - Merge relationships - on match::[4] Copying properties from node with ON MATCH | passed |
| Merge7 - Merge relationships - on match::[5] Copying properties from literal map with ON MATCH | passed |
| Merge8 - Merge relationships - on match and on create::[1] Using ON CREATE and ON MATCH | passed |
| Merge9 - Merge clause interoperation with other clauses::[4] MERGE after WITH with predicate and WITH with aggregation | passed |
| Remove1 - Remove a Property::[1] Remove a single node property | passed |
| Remove1 - Remove a Property::[2] Remove multiple node properties | passed |
| Remove1 - Remove a Property::[3] Remove a single relationship property | passed |
| Remove1 - Remove a Property::[4] Remove multiple relationship properties | passed |
| Remove1 - Remove a Property::[6] Ignore null when removing property from a relationship | passed |
| Remove1 - Remove a Property::[7] Remove a missing node property | passed |
| Remove2 - Remove a Label::[1] Remove a single label from a node with a single label | passed |
| Remove2 - Remove a Label::[2] Remove a single label from a node with two labels | passed |
| Remove2 - Remove a Label::[3] Remove two labels from a node with three labels | passed |
| Remove2 - Remove a Label::[4] Remove a non-existent node label | passed |
| Remove3 - Persistence of remove clause side effects::[1] Limiting to zero results after removing a property from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[2] Skipping all results after removing a property from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[3] Skipping and limiting to a few results after removing a property from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[4] Skipping zero results and limiting to all results after removing a property from nodes does not affect the result set nor the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[5] Filtering after removing a property from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[6] Aggregating in `RETURN` after removing a property from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[7] Aggregating in `WITH` after removing a property from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[8] Limiting to zero results after removing a label from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[9] Skipping all results after removing a label from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[10] Skipping and limiting to a few results after removing a label from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[11] Skipping zero result and limiting to all results after removing a label from nodes does not affect the result set nor the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[12] Filtering after removing a label from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[13] Aggregating in `RETURN` after removing a label from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[14] Aggregating in `WITH` after removing a label from nodes affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[15] Limiting to zero results after removing a property from relationships affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[16] Skipping all results after removing a property from relationships affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[17] Skipping and limiting to a few results after removing a property from relationships affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[18] Skipping zero result and limiting to all results after removing a property from relationships does not affect the result set nor the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[19] Filtering after removing a property from relationships affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[20] Aggregating in `RETURN` after removing a property from relationships affects the result set but not the side effects | passed |
| Remove3 - Persistence of remove clause side effects::[21] Aggregating in `WITH` after removing a property from relationships affects the result set but not the side effects | passed |
| Return1 - Return single variable (correct return of values according to their type)::[1] Returning a list property | passed |
| Return2 - Return single expression (correctly projecting an expression)::[2] Returning a node property value | passed |
| Return2 - Return single expression (correctly projecting an expression)::[3] Missing node property should become null | passed |
| Return2 - Return single expression (correctly projecting an expression)::[4] Returning a relationship property value | passed |
| Return2 - Return single expression (correctly projecting an expression)::[5] Missing relationship property should become null | passed |
| Return2 - Return single expression (correctly projecting an expression)::[6] Adding a property and a literal in projection | passed |
| Return2 - Return single expression (correctly projecting an expression)::[7] Adding list properties in projection | passed |
| Return2 - Return single expression (correctly projecting an expression)::[8] Returning label predicate expression | passed |
| Return2 - Return single expression (correctly projecting an expression)::[9] Returning a projected map | passed |
| Return2 - Return single expression (correctly projecting an expression)::[11] RETURN does not lose precision on large integers | passed |
| Return2 - Return single expression (correctly projecting an expression)::[12] Projecting a list of nodes and relationships | passed |
| Return2 - Return single expression (correctly projecting an expression)::[13] Projecting a map of nodes and relationships | passed |
| Return2 - Return single expression (correctly projecting an expression)::[14] Do not fail when returning type of deleted relationships | passed |
| Return2 - Return single expression (correctly projecting an expression)::[15] Fail when returning properties of deleted nodes | passed |
| Return2 - Return single expression (correctly projecting an expression)::[16] Fail when returning labels of deleted nodes | passed |
| Return2 - Return single expression (correctly projecting an expression)::[17] Fail when returning properties of deleted relationships | passed |
| Return3 - Return multiple expressions (if column order correct)::[1] Returning multiple expressions | passed |
| Return3 - Return multiple expressions (if column order correct)::[2] Returning multiple node property values | passed |
| Return3 - Return multiple expressions (if column order correct)::[3] Projecting nodes and relationships | passed |
| Return4 - Column renaming::[1] Honour the column name for RETURN items | passed |
| Return4 - Column renaming::[2] Support column renaming | passed |
| Return4 - Column renaming::[3] Aliasing expressions | passed |
| Return4 - Column renaming::[4] Keeping used expression 1 | passed |
| Return4 - Column renaming::[5] Keeping used expression 2 | passed |
| Return4 - Column renaming::[6] Keeping used expression 3 | passed |
| Return4 - Column renaming::[7] Keeping used expression 4 | passed |
| Return4 - Column renaming::[8] Support column renaming for aggregations | passed |
| Return4 - Column renaming::[9] Handle subexpression in aggregation also occurring as standalone expression with nested aggregation in a literal map | passed |
| Return4 - Column renaming::[11] Reusing variable names in RETURN | passed |
| Return5 - Implicit grouping with distinct::[1] DISTINCT inside aggregation should work with lists in maps | passed |
| Return5 - Implicit grouping with distinct::[2] DISTINCT on nullable values | passed |
| Return5 - Implicit grouping with distinct::[3] DISTINCT inside aggregation should work with nested lists in maps | passed |
| Return5 - Implicit grouping with distinct::[4] DISTINCT inside aggregation should work with nested lists of maps in maps | passed |
| Return5 - Implicit grouping with distinct::[5] Aggregate on list values | passed |
| Return6 - Implicit grouping with aggregates::[1] Return count aggregation over nodes | passed |
| Return6 - Implicit grouping with aggregates::[2] Projecting an arithmetic expression with aggregation | passed |
| Return6 - Implicit grouping with aggregates::[3] Aggregating by a list property has a correct definition of equality | passed |
| Return6 - Implicit grouping with aggregates::[4] Support multiple divisions in aggregate function | passed |
| Return6 - Implicit grouping with aggregates::[5] Aggregates inside normal functions | passed |
| Return6 - Implicit grouping with aggregates::[7] Aggregate on property | passed |
| Return6 - Implicit grouping with aggregates::[8] Handle aggregation on functions | failed |
| Return6 - Implicit grouping with aggregates::[9] Aggregates with arithmetics | passed |
| Return6 - Implicit grouping with aggregates::[10] Multiple aggregates on same variable | passed |
| Return6 - Implicit grouping with aggregates::[11] Counting matches | passed |
| Return6 - Implicit grouping with aggregates::[12] Counting matches per group | passed |
| Return6 - Implicit grouping with aggregates::[13] Returning the minimum length of paths | failed |
| Return6 - Implicit grouping with aggregates::[16] Aggregation on complex expressions | passed |
| Return7 - Return all variables::[1] Return all variables | passed |
| Return8 - Return clause interoperation with other clauses::[1] Return aggregation after With filtering | passed |
| ReturnOrderBy1 - Order by a single variable (correct order of values according to their type)::[11] ORDER BY should order distinct types in the expected order | passed |
| ReturnOrderBy1 - Order by a single variable (correct order of values according to their type)::[12] ORDER BY DESC should order distinct types in the expected order | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[1] ORDER BY should return results in ascending order | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[2] ORDER BY DESC should return results in descending order | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[3] Sort on aggregated function | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[4] Support sort and distinct | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[5] Support ordering by a property after being distinct-ified | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[6] Count star should count everything in scope | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[7] Ordering with aggregation | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[8] Returning all variables with ordering | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[9] Using aliased DISTINCT expression in ORDER BY | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[10] Returned columns do not change from using ORDER BY | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[11] Aggregates ordered by arithmetics | passed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[12] Aggregation of named paths | failed |
| ReturnOrderBy2 - Order by a single expression (order of projection)::[13] Fail when sorting on variable removed by DISTINCT | passed |
| ReturnOrderBy3 - Order by multiple expressions (order obey priority of expressions)::[1] Sort on aggregate function and normal property | passed |
| ReturnOrderBy4 - Order by in combination with projection::[2] Handle projections with ORDER BY | passed |
| ReturnOrderBy5 - Order by in combination with column renaming::[1] Renaming columns before ORDER BY should return results in ascending order | passed |
| ReturnSkipLimit1 - Skip::[1] Start the result from the second row | passed |
| ReturnSkipLimit1 - Skip::[2] Start the result from the second row by param | passed |
| ReturnSkipLimit1 - Skip::[3] SKIP with an expression that does not depend on variables | passed |
| ReturnSkipLimit1 - Skip::[6] Negative parameter for SKIP should fail | passed |
| ReturnSkipLimit1 - Skip::[7] Negative SKIP should fail | passed |
| ReturnSkipLimit1 - Skip::[8] Floating point parameter for SKIP should fail | passed |
| ReturnSkipLimit1 - Skip::[9] Floating point SKIP should fail | passed |
| ReturnSkipLimit2 - Limit::[2] Limit to two hits with explicit order | passed |
| ReturnSkipLimit2 - Limit::[3] LIMIT 0 should return an empty result | passed |
| ReturnSkipLimit2 - Limit::[4] Handle ORDER BY with LIMIT 1 | passed |
| ReturnSkipLimit2 - Limit::[6] LIMIT with an expression that does not depend on variables | passed |
| ReturnSkipLimit2 - Limit::[7] Limit to more rows than actual results 1 | passed |
| ReturnSkipLimit2 - Limit::[8] Limit to more rows than actual results 2 | passed |
| ReturnSkipLimit2 - Limit::[10] Negative parameter for LIMIT should fail | passed |
| ReturnSkipLimit2 - Limit::[11] Negative parameter for LIMIT with ORDER BY should fail | passed |
| ReturnSkipLimit2 - Limit::[13] Fail when using negative value in LIMIT 2 | passed |
| ReturnSkipLimit2 - Limit::[14] Floating point parameter for LIMIT should fail | passed |
| ReturnSkipLimit2 - Limit::[15] Floating point parameter for LIMIT with ORDER BY should fail | passed |
| ReturnSkipLimit2 - Limit::[17] Fail when using floating point in LIMIT 2 | passed |
| ReturnSkipLimit3 - Skip and limit::[1] Get rows in the middle | passed |
| ReturnSkipLimit3 - Skip and limit::[2] Get rows in the middle by param | passed |
| ReturnSkipLimit3 - Skip and limit::[3] Limiting amount of rows when there are fewer left than the LIMIT argument | passed |
| Set1 - Set a Property::[1] Set a property | passed |
| Set1 - Set a Property::[2] Set a property to an expression | passed |
| Set1 - Set a Property::[3] Set a property by selecting the node using a simple expression | passed |
| Set1 - Set a Property::[4] Set a property by selecting the relationship using a simple expression | passed |
| Set1 - Set a Property::[5] Adding a list property | passed |
| Set1 - Set a Property::[11] Set multiple node properties | passed |
| Set2 - Set a Property to Null::[1] Setting a node property to null removes the existing property | passed |
| Set2 - Set a Property to Null::[2] Setting a node property to null removes the existing property, but not before SET | passed |
| Set2 - Set a Property to Null::[3] Setting a relationship property to null removes the existing property | passed |
| Set3 - Set a Label::[1] Add a single label to a node with no label | passed |
| Set3 - Set a Label::[2] Adding multiple labels to a node with no label | passed |
| Set3 - Set a Label::[3] Add a single label to a node with an existing label | passed |
| Set3 - Set a Label::[4] Adding multiple labels to a node with an existing label | passed |
| Set3 - Set a Label::[5] Ignore whitespace before colon 1 | passed |
| Set3 - Set a Label::[6] Ignore whitespace before colon 2 | passed |
| Set3 - Set a Label::[7] Ignore whitespace before colon 3 | passed |
| Set4 - Set all properties with a map::[1] Set multiple properties with a property map | passed |
| Set4 - Set all properties with a map::[2] Non-existent values in a property map are removed with SET | passed |
| Set4 - Set all properties with a map::[3] Null values in a property map are removed with SET | passed |
| Set4 - Set all properties with a map::[4] All properties are removed if node is set to empty property map | passed |
| Set5 - Set multiple properties with a map::[2] Overwrite values when using += | passed |
| Set5 - Set multiple properties with a map::[3] Retain old values when using += | passed |
| Set5 - Set multiple properties with a map::[4] Explicit null values in a map remove old values | passed |
| Set5 - Set multiple properties with a map::[5] Set an empty map when using += has no effect | passed |
| Set6 - Persistence of set clause side effects::[1] Limiting to zero results after setting a property on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[2] Skipping all results after setting a property on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[3] Skipping and limiting to a few results after setting a property on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[4] Skipping zero results and limiting to all results after setting a property on nodes does not affect the result set nor the side effects | passed |
| Set6 - Persistence of set clause side effects::[5] Filtering after setting a property on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[6] Aggregating in `RETURN` after setting a property on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[7] Aggregating in `WITH` after setting a property on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[8] Limiting to zero results after adding a label on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[9] Skipping all results after adding a label on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[10] Skipping and limiting to a few results after adding a label on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[11] Skipping zero result and limiting to all results after adding a label on nodes does not affect the result set nor the side effects | passed |
| Set6 - Persistence of set clause side effects::[12] Filtering after adding a label on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[13] Aggregating in `RETURN` after adding a label on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[14] Aggregating in `WITH` after adding a label on nodes affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[15] Limiting to zero results after setting a property on relationships affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[16] Skipping all results after setting a property on relationships affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[17] Skipping and limiting to a few results after setting a property on relationships affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[18] Skipping zero result and limiting to all results after setting a property on relationships does not affect the result set nor the side effects | passed |
| Set6 - Persistence of set clause side effects::[19] Filtering after setting a property on relationships affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[20] Aggregating in `RETURN` after setting a property on relationships affects the result set but not the side effects | passed |
| Set6 - Persistence of set clause side effects::[21] Aggregating in `WITH` after setting a property on relationships affects the result set but not the side effects | passed |
| Union1 - Union::[4] Should be able to create text output from union queries | passed |
| Union2 - Union All::[4] Should be able to create text output from union all queries | passed |
| Unwind1::[5] Unwinding a collected expression | passed |
| Unwind1::[6] Creating nodes from an unwound parameter list | passed |
| Unwind1::[12] Unwind does not remove variables from scope | passed |
| With1 - Forward single variable::[1] Forwarind a node variable 1 | passed |
| With1 - Forward single variable::[2] Forwarind a node variable 2 | passed |
| With1 - Forward single variable::[3] Forwarding a relationship variable | passed |
| With1 - Forward single variable::[4] Forwarding a path variable | passed |
| With1 - Forward single variable::[6] Forwarding a node variable possibly null | passed |
| With2 - Forward single expression::[1] Forwarding a property to express a join | passed |
| With3 - Forward multiple expressions::[1] Forwarding multiple node and relationship variables | passed |
| With4 - Variable aliasing::[1] Aliasing relationship variable | passed |
| With4 - Variable aliasing::[2] Aliasing expression to new variable name | passed |
| With4 - Variable aliasing::[3] Aliasing expression to existing variable name | passed |
| With4 - Variable aliasing::[6] Reusing variable names in WITH | passed |
| With5 - Implicit grouping with DISTINCT::[1] DISTINCT on an expression | passed |
| With5 - Implicit grouping with DISTINCT::[2] Handling DISTINCT with lists in maps | passed |
| With6 - Implicit grouping with aggregates::[1] Implicit grouping with single expression as grouping key and single aggregation | passed |
| With6 - Implicit grouping with aggregates::[2] Implicit grouping with single relationship variable as grouping key and single aggregation | passed |
| With6 - Implicit grouping with aggregates::[3] Implicit grouping with multiple node and relationship variables as grouping key and single aggregation | passed |
| With6 - Implicit grouping with aggregates::[4] Implicit grouping with single path variable as grouping key and single aggregation | failed |
| With7 - WITH on WITH::[1] A simple pattern with one bound endpoint | passed |
| With7 - WITH on WITH::[2] Multiple WITHs using a predicate and aggregation | passed |
| WithOrderBy1 - Order by a single variable::[21] Sort distinct types in ascending order | passed |
| WithOrderBy1 - Order by a single variable::[22] Sort distinct types in descending order | passed |
| WithOrderBy1 - Order by a single variable::[23] Sort by a boolean variable projected from a node property in ascending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[23] Sort by a boolean variable projected from a node property in ascending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[23] Sort by a boolean variable projected from a node property in ascending order #3 | passed |
| WithOrderBy1 - Order by a single variable::[24] Sort by a boolean variable projected from a node property in descending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[24] Sort by a boolean variable projected from a node property in descending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[25] Sort by an integer variable projected from a node property in ascending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[25] Sort by an integer variable projected from a node property in ascending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[25] Sort by an integer variable projected from a node property in ascending order #3 | passed |
| WithOrderBy1 - Order by a single variable::[26] Sort by an integer variable projected from a node property in descending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[26] Sort by an integer variable projected from a node property in descending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[27] Sort by a float variable projected from a node property in ascending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[27] Sort by a float variable projected from a node property in ascending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[27] Sort by a float variable projected from a node property in ascending order #3 | passed |
| WithOrderBy1 - Order by a single variable::[28] Sort by a float variable projected from a node property in descending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[28] Sort by a float variable projected from a node property in descending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[29] Sort by a string variable projected from a node property in ascending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[29] Sort by a string variable projected from a node property in ascending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[29] Sort by a string variable projected from a node property in ascending order #3 | passed |
| WithOrderBy1 - Order by a single variable::[30] Sort by a string variable projected from a node property in descending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[30] Sort by a string variable projected from a node property in descending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[31] Sort by a list variable projected from a node property in ascending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[31] Sort by a list variable projected from a node property in ascending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[31] Sort by a list variable projected from a node property in ascending order #3 | passed |
| WithOrderBy1 - Order by a single variable::[32] Sort by a list variable projected from a node property in descending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[32] Sort by a list variable projected from a node property in descending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[33] Sort by a date variable projected from a node property in ascending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[33] Sort by a date variable projected from a node property in ascending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[33] Sort by a date variable projected from a node property in ascending order #3 | passed |
| WithOrderBy1 - Order by a single variable::[34] Sort by a date variable projected from a node property in descending order #1 | passed |
| WithOrderBy1 - Order by a single variable::[34] Sort by a date variable projected from a node property in descending order #2 | passed |
| WithOrderBy1 - Order by a single variable::[35] Sort by a local time variable projected from a node property in ascending order #1 | failed |
| WithOrderBy1 - Order by a single variable::[35] Sort by a local time variable projected from a node property in ascending order #2 | failed |
| WithOrderBy1 - Order by a single variable::[35] Sort by a local time variable projected from a node property in ascending order #3 | failed |
| WithOrderBy1 - Order by a single variable::[36] Sort by a local time variable projected from a node property in descending order #1 | failed |
| WithOrderBy1 - Order by a single variable::[36] Sort by a local time variable projected from a node property in descending order #2 | failed |
| WithOrderBy1 - Order by a single variable::[37] Sort by a time variable projected from a node property in ascending order #1 | failed |
| WithOrderBy1 - Order by a single variable::[37] Sort by a time variable projected from a node property in ascending order #2 | failed |
| WithOrderBy1 - Order by a single variable::[37] Sort by a time variable projected from a node property in ascending order #3 | failed |
| WithOrderBy1 - Order by a single variable::[38] Sort by a time variable projected from a node property in descending order #1 | failed |
| WithOrderBy1 - Order by a single variable::[38] Sort by a time variable projected from a node property in descending order #2 | failed |
| WithOrderBy1 - Order by a single variable::[39] Sort by a local date time variable projected from a node property in ascending order #1 | failed |
| WithOrderBy1 - Order by a single variable::[39] Sort by a local date time variable projected from a node property in ascending order #2 | failed |
| WithOrderBy1 - Order by a single variable::[39] Sort by a local date time variable projected from a node property in ascending order #3 | failed |
| WithOrderBy1 - Order by a single variable::[40] Sort by a local date time variable projected from a node property in descending order #1 | failed |
| WithOrderBy1 - Order by a single variable::[40] Sort by a local date time variable projected from a node property in descending order #2 | failed |
| WithOrderBy1 - Order by a single variable::[41] Sort by a date time variable projected from a node property in ascending order #1 | failed |
| WithOrderBy1 - Order by a single variable::[41] Sort by a date time variable projected from a node property in ascending order #2 | failed |
| WithOrderBy1 - Order by a single variable::[41] Sort by a date time variable projected from a node property in ascending order #3 | failed |
| WithOrderBy1 - Order by a single variable::[42] Sort by a date time variable projected from a node property in descending order #1 | failed |
| WithOrderBy1 - Order by a single variable::[42] Sort by a date time variable projected from a node property in descending order #2 | failed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #1 | passed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #2 | passed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #3 | passed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #4 | passed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #5 | passed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #6 | passed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #7 | passed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #8 | passed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #9 | passed |
| WithOrderBy1 - Order by a single variable::[46] Fail on sorting by an undefined variable #Example: <exampleName> #10 | passed |
| WithOrderBy2 - Order by a single expression::[1] Sort by a boolean expression in ascending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[1] Sort by a boolean expression in ascending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[1] Sort by a boolean expression in ascending order #3 | passed |
| WithOrderBy2 - Order by a single expression::[2] Sort by a boolean expression in descending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[2] Sort by a boolean expression in descending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[3] Sort by an integer expression in ascending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[3] Sort by an integer expression in ascending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[3] Sort by an integer expression in ascending order #3 | passed |
| WithOrderBy2 - Order by a single expression::[4] Sort by an integer expression in descending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[4] Sort by an integer expression in descending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[5] Sort by a float expression in ascending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[5] Sort by a float expression in ascending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[5] Sort by a float expression in ascending order #3 | passed |
| WithOrderBy2 - Order by a single expression::[6] Sort by a float expression in descending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[6] Sort by a float expression in descending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[7] Sort by a string expression in ascending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[7] Sort by a string expression in ascending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[7] Sort by a string expression in ascending order #3 | passed |
| WithOrderBy2 - Order by a single expression::[8] Sort by a string expression in descending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[8] Sort by a string expression in descending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[9] Sort by a list expression in ascending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[9] Sort by a list expression in ascending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[9] Sort by a list expression in ascending order #3 | passed |
| WithOrderBy2 - Order by a single expression::[10] Sort by a list expression in descending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[10] Sort by a list expression in descending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[11] Sort by a date expression in ascending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[11] Sort by a date expression in ascending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[11] Sort by a date expression in ascending order #3 | passed |
| WithOrderBy2 - Order by a single expression::[12] Sort by a date expression in descending order #1 | passed |
| WithOrderBy2 - Order by a single expression::[12] Sort by a date expression in descending order #2 | passed |
| WithOrderBy2 - Order by a single expression::[13] Sort by a local time expression in ascending order #1 | failed |
| WithOrderBy2 - Order by a single expression::[13] Sort by a local time expression in ascending order #2 | failed |
| WithOrderBy2 - Order by a single expression::[13] Sort by a local time expression in ascending order #3 | failed |
| WithOrderBy2 - Order by a single expression::[14] Sort by a local time expression in descending order #1 | failed |
| WithOrderBy2 - Order by a single expression::[14] Sort by a local time expression in descending order #2 | failed |
| WithOrderBy2 - Order by a single expression::[15] Sort by a time expression in ascending order #1 | failed |
| WithOrderBy2 - Order by a single expression::[15] Sort by a time expression in ascending order #2 | failed |
| WithOrderBy2 - Order by a single expression::[15] Sort by a time expression in ascending order #3 | failed |
| WithOrderBy2 - Order by a single expression::[16] Sort by a time expression in descending order #1 | failed |
| WithOrderBy2 - Order by a single expression::[16] Sort by a time expression in descending order #2 | failed |
| WithOrderBy2 - Order by a single expression::[17] Sort by a local date time expression in ascending order #1 | failed |
| WithOrderBy2 - Order by a single expression::[17] Sort by a local date time expression in ascending order #2 | failed |
| WithOrderBy2 - Order by a single expression::[17] Sort by a local date time expression in ascending order #3 | failed |
| WithOrderBy2 - Order by a single expression::[18] Sort by a local date time expression in descending order #1 | failed |
| WithOrderBy2 - Order by a single expression::[18] Sort by a local date time expression in descending order #2 | failed |
| WithOrderBy2 - Order by a single expression::[19] Sort by a date time expression in ascending order #1 | failed |
| WithOrderBy2 - Order by a single expression::[19] Sort by a date time expression in ascending order #2 | failed |
| WithOrderBy2 - Order by a single expression::[19] Sort by a date time expression in ascending order #3 | failed |
| WithOrderBy2 - Order by a single expression::[20] Sort by a date time expression in descending order #1 | failed |
| WithOrderBy2 - Order by a single expression::[20] Sort by a date time expression in descending order #2 | failed |
| WithOrderBy2 - Order by a single expression::[21] Sort by an expression that is only partially orderable on a non-distinct binding table #1 | passed |
| WithOrderBy2 - Order by a single expression::[21] Sort by an expression that is only partially orderable on a non-distinct binding table #2 | passed |
| WithOrderBy2 - Order by a single expression::[22] Sort by an expression that is only partially orderable on a non-distinct binding table, but used as a grouping key #1 | passed |
| WithOrderBy2 - Order by a single expression::[22] Sort by an expression that is only partially orderable on a non-distinct binding table, but used as a grouping key #2 | passed |
| WithOrderBy2 - Order by a single expression::[23] Sort by an expression that is only partially orderable on a non-distinct binding table, but used in parts as a grouping key #1 | passed |
| WithOrderBy2 - Order by a single expression::[23] Sort by an expression that is only partially orderable on a non-distinct binding table, but used in parts as a grouping key #2 | passed |
| WithOrderBy2 - Order by a single expression::[24] Sort by an expression that is only partially orderable on a non-distinct binding table, but made distinct #1 | passed |
| WithOrderBy2 - Order by a single expression::[24] Sort by an expression that is only partially orderable on a non-distinct binding table, but made distinct #2 | passed |
| WithOrderBy3 - Order by multiple expressions::[1] Sort by two expressions, both in ascending order #1 | passed |
| WithOrderBy3 - Order by multiple expressions::[1] Sort by two expressions, both in ascending order #2 | passed |
| WithOrderBy3 - Order by multiple expressions::[1] Sort by two expressions, both in ascending order #3 | passed |
| WithOrderBy3 - Order by multiple expressions::[1] Sort by two expressions, both in ascending order #4 | passed |
| WithOrderBy3 - Order by multiple expressions::[1] Sort by two expressions, both in ascending order #5 | passed |
| WithOrderBy3 - Order by multiple expressions::[1] Sort by two expressions, both in ascending order #6 | passed |
| WithOrderBy3 - Order by multiple expressions::[1] Sort by two expressions, both in ascending order #7 | passed |
| WithOrderBy3 - Order by multiple expressions::[1] Sort by two expressions, both in ascending order #8 | passed |
| WithOrderBy3 - Order by multiple expressions::[1] Sort by two expressions, both in ascending order #9 | passed |
| WithOrderBy3 - Order by multiple expressions::[2] Sort by two expressions, first in ascending order, second in descending order #1 | passed |
| WithOrderBy3 - Order by multiple expressions::[2] Sort by two expressions, first in ascending order, second in descending order #2 | passed |
| WithOrderBy3 - Order by multiple expressions::[2] Sort by two expressions, first in ascending order, second in descending order #3 | passed |
| WithOrderBy3 - Order by multiple expressions::[2] Sort by two expressions, first in ascending order, second in descending order #4 | passed |
| WithOrderBy3 - Order by multiple expressions::[2] Sort by two expressions, first in ascending order, second in descending order #5 | passed |
| WithOrderBy3 - Order by multiple expressions::[2] Sort by two expressions, first in ascending order, second in descending order #6 | passed |
| WithOrderBy3 - Order by multiple expressions::[3] Sort by two expressions, first in descending order, second in ascending order #1 | passed |
| WithOrderBy3 - Order by multiple expressions::[3] Sort by two expressions, first in descending order, second in ascending order #2 | passed |
| WithOrderBy3 - Order by multiple expressions::[3] Sort by two expressions, first in descending order, second in ascending order #3 | passed |
| WithOrderBy3 - Order by multiple expressions::[3] Sort by two expressions, first in descending order, second in ascending order #4 | passed |
| WithOrderBy3 - Order by multiple expressions::[3] Sort by two expressions, first in descending order, second in ascending order #5 | passed |
| WithOrderBy3 - Order by multiple expressions::[3] Sort by two expressions, first in descending order, second in ascending order #6 | passed |
| WithOrderBy3 - Order by multiple expressions::[4] Sort by two expressions, both in descending order #1 | passed |
| WithOrderBy3 - Order by multiple expressions::[4] Sort by two expressions, both in descending order #2 | passed |
| WithOrderBy3 - Order by multiple expressions::[4] Sort by two expressions, both in descending order #3 | passed |
| WithOrderBy3 - Order by multiple expressions::[4] Sort by two expressions, both in descending order #4 | passed |
| WithOrderBy3 - Order by multiple expressions::[5] An expression without explicit sort direction is sorted in ascending order #1 | passed |
| WithOrderBy3 - Order by multiple expressions::[5] An expression without explicit sort direction is sorted in ascending order #2 | passed |
| WithOrderBy3 - Order by multiple expressions::[5] An expression without explicit sort direction is sorted in ascending order #3 | passed |
| WithOrderBy3 - Order by multiple expressions::[5] An expression without explicit sort direction is sorted in ascending order #4 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #1 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #2 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #3 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #4 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #5 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #6 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #7 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #8 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #9 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #10 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #11 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #12 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #13 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #14 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #15 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #16 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #17 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #18 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #19 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #20 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #21 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #22 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #23 | passed |
| WithOrderBy3 - Order by multiple expressions::[6] An constant expression does not influence the order determined by other expression before and after the constant expression #24 | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[1] Sort by a projected expression | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[2] Sort by an alias of a projected expression | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[3] Sort by two projected expressions with order priority being different than projection order | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[4] Sort by one projected expression and one alias of a projected expression with order priority being different than projection order | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[5] Sort by one alias of a projected expression and one projected expression with order priority being different than projection order | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[6] Sort by aliases of two projected expressions with order priority being different than projection order | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[7] Sort by an alias of a projected expression where the alias shadows an existing variable | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[8] Sort by non-projected existing variable | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[9] Sort by an alias of a projected expression containing the variable shadowed by the alias | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[10] Sort by a non-projected expression containing an alias of a projected expression containing the variable shadowed by the alias | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[11] Sort by an aggregate projection | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[12] Sort by an aliased aggregate projection | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[13] Fail on sorting by a non-projected aggregation on a variable | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[14] Fail on sorting by a non-projected aggregation on an expression | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[15] Sort by an aliased aggregate projection does allow subsequent matching | passed |
| WithSkipLimit1 - Skip::[1] Handle dependencies across WITH with SKIP | passed |
| WithSkipLimit1 - Skip::[2] Ordering and skipping on aggregate | passed |
| WithSkipLimit2 - Limit::[1] ORDER BY and LIMIT can be used | passed |
| WithSkipLimit2 - Limit::[2] Handle dependencies across WITH with LIMIT | passed |
| WithSkipLimit2 - Limit::[3] Connected components succeeding WITH with LIMIT | passed |
| WithSkipLimit2 - Limit::[4] Ordering and limiting on aggregate | passed |
| WithSkipLimit3 - Skip and limit::[1] Get rows in the middle | passed |
| WithSkipLimit3 - Skip and limit::[2] Get rows in the middle by param | passed |
| WithSkipLimit3 - Skip and limit::[3] Limiting amount of rows when there are fewer left than the LIMIT argument | passed |
| WithWhere1 - Filter single variable::[1] Filter node with property predicate on a single variable with multiple bindings | passed |
| WithWhere1 - Filter single variable::[2] Filter node with property predicate on a single variable with multiple distinct bindings | passed |
| WithWhere1 - Filter single variable::[3] Filter for an unbound relationship variable | passed |
| WithWhere1 - Filter single variable::[4] Filter for an unbound node variable | passed |
| WithWhere2 - Filter multiple variables::[1] Filter nodes with conjunctive two-part property predicate on multi variables with multiple bindings | passed |
| WithWhere2 - Filter multiple variables::[2] Filter node with conjunctive multi-part property predicates on multi variables with multiple bindings | passed |
| WithWhere3 - Equi-Joins on variables::[1] Join between node identities | passed |
| WithWhere3 - Equi-Joins on variables::[2] Join between node properties of disconnected nodes | passed |
| WithWhere3 - Equi-Joins on variables::[3] Join between node properties of adjacent nodes | passed |
| WithWhere4 - Non-Equi-Joins on variables::[1] Join nodes on inequality | passed |
| WithWhere4 - Non-Equi-Joins on variables::[2] Join with disjunctive multi-part predicates including patterns | failed |
| WithWhere5 - Filter on predicate resulting in null::[1] Filter out on null | passed |
| WithWhere5 - Filter on predicate resulting in null::[2] Filter out on null if the AND'd predicate evaluates to false | passed |
| WithWhere5 - Filter on predicate resulting in null::[3] Filter out on null if the AND'd predicate evaluates to true | passed |
| WithWhere5 - Filter on predicate resulting in null::[4] Do not filter out on null if the OR'd predicate evaluates to true | passed |
| WithWhere6 - Filter on aggregates::[1] Filter a single aggregate | passed |
| WithWhere7 - Variable visibility under aliasing::[1] WHERE sees a variable bound before but not after WITH | passed |
| WithWhere7 - Variable visibility under aliasing::[2] WHERE sees a variable bound after but not before WITH | passed |
| WithWhere7 - Variable visibility under aliasing::[3] WHERE sees both, variable bound before but not after WITH and variable bound after but not before WITH | passed |
| Aggregation1 - Count::[1] Count only non-null values | passed |
| Aggregation1 - Count::[2] Counting loop relationships | passed |
| Aggregation3 - Sum::[1] Sum only non-null values | passed |
| Aggregation5 - Collect::[1] `collect()` filtering nulls | failed |
| Aggregation5 - Collect::[2] OPTIONAL MATCH and `collect()` on node property | failed |
| Aggregation6 - Percentiles::[1] `percentileDisc()` #1 | passed |
| Aggregation6 - Percentiles::[1] `percentileDisc()` #2 | passed |
| Aggregation6 - Percentiles::[1] `percentileDisc()` #3 | passed |
| Aggregation6 - Percentiles::[2] `percentileCont()` #1 | passed |
| Aggregation6 - Percentiles::[2] `percentileCont()` #2 | passed |
| Aggregation6 - Percentiles::[2] `percentileCont()` #3 | passed |
| Aggregation6 - Percentiles::[3] `percentileCont()` failing on bad arguments #1 | passed |
| Aggregation6 - Percentiles::[3] `percentileCont()` failing on bad arguments #2 | passed |
| Aggregation6 - Percentiles::[3] `percentileCont()` failing on bad arguments #3 | passed |
| Aggregation6 - Percentiles::[4] `percentileDisc()` failing on bad arguments #1 | passed |
| Aggregation6 - Percentiles::[4] `percentileDisc()` failing on bad arguments #2 | passed |
| Aggregation6 - Percentiles::[4] `percentileDisc()` failing on bad arguments #3 | passed |
| Aggregation6 - Percentiles::[5] `percentileDisc()` failing in more involved query | passed |
| Aggregation8 - DISTINCT::[2] Distinct on null | passed |
| Boolean4 - NOT logical operations::[3] NOT and false | passed |
| Comparison1 - Equality::[1] Number-typed integer comparison | passed |
| Comparison1 - Equality::[2] Number-typed float comparison | passed |
| Comparison1 - Equality::[3] Any-typed string comparison | passed |
| Comparison1 - Equality::[4] Comparing nodes to nodes | passed |
| Comparison1 - Equality::[5] Comparing relationships to relationships | passed |
| Comparison1 - Equality::[10] Handling inlined equality of large integer | passed |
| Comparison1 - Equality::[11] Handling explicit equality of large integer | passed |
| Comparison1 - Equality::[12] Handling inlined equality of large integer, non-equal values | passed |
| Comparison1 - Equality::[13] Handling explicit equality of large integer, non-equal values | passed |
| Comparison1 - Equality::[14] Direction of traversed relationship is not significant for path equality, simple | passed |
| Comparison2 - Half-bounded Range::[1] Comparing strings and integers using > in an AND'd predicate | passed |
| Comparison2 - Half-bounded Range::[2] Comparing strings and integers using > in a OR'd predicate | passed |
| Comparison2 - Half-bounded Range::[3] Comparing across types yields null, except numbers #1 | passed |
| Comparison2 - Half-bounded Range::[3] Comparing across types yields null, except numbers #2 | passed |
| Comparison2 - Half-bounded Range::[3] Comparing across types yields null, except numbers #3 | passed |
| Comparison2 - Half-bounded Range::[3] Comparing across types yields null, except numbers #4 | passed |
| Comparison3 - Full-Bound Range::[1] Handling numerical ranges 1 | passed |
| Comparison3 - Full-Bound Range::[2] Handling numerical ranges 2 | passed |
| Comparison3 - Full-Bound Range::[3] Handling numerical ranges 3 | passed |
| Comparison3 - Full-Bound Range::[4] Handling numerical ranges 4 | passed |
| Comparison3 - Full-Bound Range::[5] Handling string ranges 1 | passed |
| Comparison3 - Full-Bound Range::[6] Handling string ranges 2 | passed |
| Comparison3 - Full-Bound Range::[7] Handling string ranges 3 | passed |
| Comparison3 - Full-Bound Range::[8] Handling string ranges 4 | passed |
| Comparison3 - Full-Bound Range::[9] Handling empty range | passed |
| Comparison4 - Combination of Comparisons::[1] Handling long chains of operators | passed |
| Conditional1 - Coalesce expression::[1] Run coalesce | passed |
| ExistentialSubquery1 - Simple existential subquery::[1] Simple subquery without WHERE clause | passed |
| ExistentialSubquery1 - Simple existential subquery::[2] Simple subquery with WHERE clause | passed |
| ExistentialSubquery1 - Simple existential subquery::[3] Simple subquery without WHERE clause, not existing pattern | failed |
| ExistentialSubquery1 - Simple existential subquery::[4] Simple subquery with WHERE clause, not existing pattern | passed |
| ExistentialSubquery2 - Full existential subquery::[1] Full existential subquery | passed |
| ExistentialSubquery2 - Full existential subquery::[2] Full existential subquery with aggregation | passed |
| ExistentialSubquery3 - Nested existential subquery::[1] Nested simple existential subquery | passed |
| ExistentialSubquery3 - Nested existential subquery::[2] Nested full existential subquery | passed |
| ExistentialSubquery3 - Nested existential subquery::[3] Nested full existential subquery with pattern predicate | passed |
| Graph3 - Node labels::[5] Using `labels()` in return clauses | passed |
| Graph3 - Node labels::[6] `labels()` should accept type Any | passed |
| Graph3 - Node labels::[8] `labels()` failing on a path | passed |
| Graph3 - Node labels::[9] `labels()` failing on invalid arguments | passed |
| Graph4 - Edge relationship type::[1] `type()` | passed |
| Graph4 - Edge relationship type::[2] `type()` on two relationships | passed |
| Graph4 - Edge relationship type::[3] `type()` on null relationship | failed |
| Graph4 - Edge relationship type::[4] `type()` on mixed null and non-null relationships | passed |
| Graph4 - Edge relationship type::[5] `type()` handling Any type | passed |
| Graph4 - Edge relationship type::[6] `type()` failing on invalid arguments #1 | passed |
| Graph4 - Edge relationship type::[6] `type()` failing on invalid arguments #2 | passed |
| Graph4 - Edge relationship type::[6] `type()` failing on invalid arguments #3 | passed |
| Graph4 - Edge relationship type::[6] `type()` failing on invalid arguments #4 | passed |
| Graph4 - Edge relationship type::[6] `type()` failing on invalid arguments #5 | passed |
| Graph5 - Node and edge label expressions::[1] Single-labels expression on nodes | passed |
| Graph5 - Node and edge label expressions::[2] Single-labels expression on relationships | passed |
| Graph5 - Node and edge label expressions::[3] Conjunctive labels expression on nodes | passed |
| Graph5 - Node and edge label expressions::[4] Conjunctive labels expression on nodes with varying order and repeating labels #1 | passed |
| Graph5 - Node and edge label expressions::[4] Conjunctive labels expression on nodes with varying order and repeating labels #2 | passed |
| Graph5 - Node and edge label expressions::[4] Conjunctive labels expression on nodes with varying order and repeating labels #3 | passed |
| Graph5 - Node and edge label expressions::[4] Conjunctive labels expression on nodes with varying order and repeating labels #4 | passed |
| Graph5 - Node and edge label expressions::[4] Conjunctive labels expression on nodes with varying order and repeating labels #5 | passed |
| Graph5 - Node and edge label expressions::[5] Label expression on null | failed |
| Graph6 - Static property access::[1] Statically access a property of a non-null node | passed |
| Graph6 - Static property access::[2] Statically access a property of a optional non-null node | passed |
| Graph6 - Static property access::[4] Statically access a property of a node resulting from an expression | passed |
| Graph6 - Static property access::[5] Statically access a property of a non-null relationship | passed |
| Graph6 - Static property access::[6] Statically access a property of a optional non-null relationship | passed |
| Graph6 - Static property access::[8] Statically access a property of a relationship resulting from an expression | passed |
| Graph7 - Dynamic property access::[1] Execute n['name'] in read queries | passed |
| Graph8 - Property keys function::[1] Using `keys()` on a single node, non-empty result | passed |
| Graph8 - Property keys function::[2] Using `keys()` on multiple nodes, non-empty result | passed |
| Graph8 - Property keys function::[3] Using `keys()` on a single node, empty result | passed |
| Graph8 - Property keys function::[4] Using `keys()` on an optionally matched node | passed |
| Graph8 - Property keys function::[5] Using `keys()` on a relationship, non-empty result | passed |
| Graph8 - Property keys function::[6] Using `keys()` on a relationship, empty result | passed |
| Graph8 - Property keys function::[7] Using `keys()` on an optionally matched relationship | passed |
| Graph8 - Property keys function::[8] Using `keys()` and `IN` to check property existence | passed |
| Graph9 - Retrieve all properties as a property map::[1] `properties()` on a node | passed |
| Graph9 - Retrieve all properties as a property map::[2] `properties()` on a relationship | passed |
| List12 - List Comprehension::[1] Collect and extract using a list comprehension | passed |
| List12 - List Comprehension::[2] Collect and filter using a list comprehension | passed |
| List12 - List Comprehension::[4] Returning a list comprehension | passed |
| List12 - List Comprehension::[5] Using a list comprehension in a WITH | passed |
| List12 - List Comprehension::[6] Using a list comprehension in a WHERE | passed |
| List6 - List size::[2] Setting and returning the size of a list property | passed |
| List6 - List size::[7] Using size of pattern comprehension to test existence | passed |
| List6 - List size::[8] Get node degree via size of pattern comprehension | passed |
| List6 - List size::[9] Get node degree via size of pattern comprehension that specifies a relationship type | passed |
| List6 - List size::[10] Get node degree via size of pattern comprehension that specifies multiple relationship types | passed |
| List9 - List Tail::[1] Returning nested expressions based on list property | passed |
| Mathematical2 - Addition::[1] Allow addition | passed |
| Null1 - IS NULL validation::[1] Property null check on non-null node | passed |
| Null1 - IS NULL validation::[2] Property null check on optional non-null node | passed |
| Null1 - IS NULL validation::[6] IS NULL is case insensitive | passed |
| Null2 - IS NOT NULL validation::[1] Property not null check on non-null node | passed |
| Null2 - IS NOT NULL validation::[2] Property not null check on optional non-null node | passed |
| Null2 - IS NOT NULL validation::[6] IS NOT NULL is case insensitive | passed |
| Path2 - Relationships of a path::[1] Return relationships by fetching them from the path | passed |
| Path2 - Relationships of a path::[2] Return relationships by fetching them from the path - starting from the end | passed |
| Path3 - Length of a path::[1] Return a var length path of length zero | passed |
| Pattern1 - Pattern predicate::[1] Matching on any single outgoing directed connection | passed |
| Pattern1 - Pattern predicate::[2] Matching on a single undirected connection | passed |
| Pattern1 - Pattern predicate::[3] Matching on any single incoming directed connection | passed |
| Pattern1 - Pattern predicate::[4] Matching on a specific type of single outgoing directed connection | passed |
| Pattern1 - Pattern predicate::[5] Matching on a specific type of single undirected connection | passed |
| Pattern1 - Pattern predicate::[6] Matching on a specific type of single incoming directed connection | passed |
| Pattern1 - Pattern predicate::[7] Matching on a specific type of a variable length outgoing directed connection | failed |
| Pattern1 - Pattern predicate::[8] Matching on a specific type of variable length undirected connection | failed |
| Pattern1 - Pattern predicate::[9] Matching on a specific type of variable length incoming directed connection | failed |
| Pattern1 - Pattern predicate::[10] Matching on a specific type of undirected connection with length 2 | passed |
| Pattern1 - Pattern predicate::[12] Matching two nodes on a single directed connection between them | passed |
| Pattern1 - Pattern predicate::[13] Fail on matching two nodes on a single undirected connection between them | passed |
| Pattern1 - Pattern predicate::[14] Matching two nodes on a specific type of single outgoing directed connection | passed |
| Pattern1 - Pattern predicate::[15] Matching two nodes on a specific type of single undirected connection | passed |
| Pattern1 - Pattern predicate::[16] Matching two nodes on a specific type of a variable length outgoing directed connection | failed |
| Pattern1 - Pattern predicate::[17] Matching two nodes on a specific type of variable length undirected connection | failed |
| Pattern1 - Pattern predicate::[18] Matching two nodes on a specific type of undirected connection with length 2 | passed |
| Pattern1 - Pattern predicate::[19] Using a negated existential pattern predicate | passed |
| Pattern1 - Pattern predicate::[20] Using two existential pattern predicates in a conjunction | passed |
| Pattern1 - Pattern predicate::[21] Using two existential pattern predicates in a disjunction | passed |
| Pattern2 - Pattern Comprehension::[1] Return a pattern comprehension | passed |
| Pattern2 - Pattern Comprehension::[2] Return a pattern comprehension with label predicate | passed |
| Pattern2 - Pattern Comprehension::[3] Return a pattern comprehension with bound nodes | passed |
| Pattern2 - Pattern Comprehension::[4] Introduce a new node variable in pattern comprehension | passed |
| Pattern2 - Pattern Comprehension::[5] Introduce a new relationship variable in pattern comprehension | passed |
| Pattern2 - Pattern Comprehension::[6] Aggregate on a pattern comprehension | passed |
| Pattern2 - Pattern Comprehension::[7] Use a pattern comprehension inside a list comprehension | passed |
| Pattern2 - Pattern Comprehension::[8] Use a pattern comprehension in WITH | passed |
| Pattern2 - Pattern Comprehension::[9] Use a variable-length pattern comprehension in WITH | failed |
| Pattern2 - Pattern Comprehension::[10] Use a pattern comprehension in RETURN | passed |
| Pattern2 - Pattern Comprehension::[11] Use a pattern comprehension and ORDER BY | passed |
| Quantifier1 - None quantifier::[8] None quantifier on list containing nodes | passed |
| Quantifier1 - None quantifier::[9] None quantifier on list containing relationships | passed |
| Quantifier2 - Single quantifier::[8] Single quantifier on list containing nodes | passed |
| Quantifier2 - Single quantifier::[9] Single quantifier on list containing relationships | passed |
| Quantifier3 - Any quantifier::[8] Any quantifier on list containing nodes | passed |
| Quantifier3 - Any quantifier::[9] Any quantifier on list containing relationships | passed |
| Quantifier4 - All quantifier::[8] All quantifier on list containing nodes | passed |
| Quantifier4 - All quantifier::[9] All quantifier on list containing relationships | passed |
| String10 - Exact Substring Search::[1] Finding exact matches with non-proper substring | passed |
| String10 - Exact Substring Search::[2] Finding substring of string | passed |
| String10 - Exact Substring Search::[3] Finding the empty substring | passed |
| String10 - Exact Substring Search::[4] Finding strings containing whitespace | passed |
| String10 - Exact Substring Search::[5] Finding strings containing newline | passed |
| String10 - Exact Substring Search::[6] No string contains null | passed |
| String10 - Exact Substring Search::[7] No string does not contain null | passed |
| String10 - Exact Substring Search::[8] Handling non-string operands for CONTAINS | passed |
| String10 - Exact Substring Search::[9] NOT with CONTAINS | passed |
| String11 - Combining Exact String Search::[1] Combining prefix and suffix search | passed |
| String11 - Combining Exact String Search::[2] Combining prefix, suffix, and substring search | passed |
| String8 - Exact String Prefix Search::[1] Finding exact matches with non-proper prefix | passed |
| String8 - Exact String Prefix Search::[2] Finding beginning of string | passed |
| String8 - Exact String Prefix Search::[3] Finding the empty prefix | passed |
| String8 - Exact String Prefix Search::[4] Finding strings starting with whitespace | passed |
| String8 - Exact String Prefix Search::[5] Finding strings starting with newline | passed |
| String8 - Exact String Prefix Search::[6] No string starts with null | passed |
| String8 - Exact String Prefix Search::[7] No string does not start with null | passed |
| String8 - Exact String Prefix Search::[8] Handling non-string operands for STARTS WITH | passed |
| String8 - Exact String Prefix Search::[9] NOT with STARTS WITH | passed |
| String9 - Exact String Suffix Search::[1] Finding exact matches with non-proper suffix | passed |
| String9 - Exact String Suffix Search::[2] Finding end of string | passed |
| String9 - Exact String Suffix Search::[3] Finding the empty suffix | passed |
| String9 - Exact String Suffix Search::[4] Finding strings ending with whitespace | passed |
| String9 - Exact String Suffix Search::[5] Finding strings ending with newline | passed |
| String9 - Exact String Suffix Search::[6] No string ends with null | passed |
| String9 - Exact String Suffix Search::[7] No string does not end with null | passed |
| String9 - Exact String Suffix Search::[8] Handling non-string operands for ENDS WITH | passed |
| String9 - Exact String Suffix Search::[9] NOT with ENDS WITH | passed |
| Temporal5 - Access Components of Temporal Values::[1] Should provide accessors for date | passed |
| Temporal5 - Access Components of Temporal Values::[2] Should provide accessors for date in last weekYear | passed |
| Temporal5 - Access Components of Temporal Values::[3] Should provide accessors for local time | failed |
| Temporal5 - Access Components of Temporal Values::[4] Should provide accessors for time | failed |
| Temporal5 - Access Components of Temporal Values::[5] Should provide accessors for local date time | failed |
| Temporal5 - Access Components of Temporal Values::[6] Should provide accessors for date time | failed |
| Temporal5 - Access Components of Temporal Values::[7] Should provide accessors for duration | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[1] Should add or subtract duration to or from date #1 | passed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[1] Should add or subtract duration to or from date #2 | passed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[1] Should add or subtract duration to or from date #3 | passed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[2] Should add or subtract duration to or from local time #1 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[2] Should add or subtract duration to or from local time #2 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[2] Should add or subtract duration to or from local time #3 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[3] Should add or subtract duration to or from time #1 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[3] Should add or subtract duration to or from time #2 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[3] Should add or subtract duration to or from time #3 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[4] Should add or subtract duration to or from local date time #1 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[4] Should add or subtract duration to or from local date time #2 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[4] Should add or subtract duration to or from local date time #3 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[5] Should add or subtract duration to or from date time #1 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[5] Should add or subtract duration to or from date time #2 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[5] Should add or subtract duration to or from date time #3 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #1 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #2 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #3 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #4 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #5 | passed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #6 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #7 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #8 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[6] Should add or subtract durations #9 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[7] Should multiply or divide durations by numbers #1 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[7] Should multiply or divide durations by numbers #2 | failed |
| Temporal8 - Compute Arithmetic Operations on Temporal Values::[7] Should multiply or divide durations by numbers #3 | failed |
| TypeConversion1 - To Boolean::[5] Fail `toBoolean()` on invalid types #Example: <exampleName> #1 | passed |
| TypeConversion1 - To Boolean::[5] Fail `toBoolean()` on invalid types #Example: <exampleName> #2 | passed |
| TypeConversion1 - To Boolean::[5] Fail `toBoolean()` on invalid types #Example: <exampleName> #3 | passed |
| TypeConversion1 - To Boolean::[5] Fail `toBoolean()` on invalid types #Example: <exampleName> #4 | passed |
| TypeConversion1 - To Boolean::[5] Fail `toBoolean()` on invalid types #Example: <exampleName> #5 | passed |
| TypeConversion1 - To Boolean::[5] Fail `toBoolean()` on invalid types #Example: <exampleName> #6 | passed |
| TypeConversion2 - To Integer::[7] `toInteger()` on node property | passed |
| TypeConversion2 - To Integer::[8] Fail `toInteger()` on invalid types #Example: <exampleName> #1 | passed |
| TypeConversion2 - To Integer::[8] Fail `toInteger()` on invalid types #Example: <exampleName> #2 | passed |
| TypeConversion2 - To Integer::[8] Fail `toInteger()` on invalid types #Example: <exampleName> #3 | passed |
| TypeConversion2 - To Integer::[8] Fail `toInteger()` on invalid types #Example: <exampleName> #4 | passed |
| TypeConversion2 - To Integer::[8] Fail `toInteger()` on invalid types #Example: <exampleName> #5 | passed |
| TypeConversion3 - To Float::[5] `toFloat()` on node property | passed |
| TypeConversion3 - To Float::[6] Fail `toFloat()` on invalid types #Example: <exampleName> #1 | passed |
| TypeConversion3 - To Float::[6] Fail `toFloat()` on invalid types #Example: <exampleName> #2 | passed |
| TypeConversion3 - To Float::[6] Fail `toFloat()` on invalid types #Example: <exampleName> #3 | passed |
| TypeConversion3 - To Float::[6] Fail `toFloat()` on invalid types #Example: <exampleName> #4 | passed |
| TypeConversion3 - To Float::[6] Fail `toFloat()` on invalid types #Example: <exampleName> #5 | passed |
| TypeConversion3 - To Float::[6] Fail `toFloat()` on invalid types #Example: <exampleName> #6 | passed |
| TypeConversion4 - To String::[4] `toString()` handling boolean properties | passed |
| TypeConversion4 - To String::[7] `toString()` on node property | passed |
| TypeConversion4 - To String::[10] Fail `toString()` on invalid types #Example: <exampleName> #1 | passed |
| TypeConversion4 - To String::[10] Fail `toString()` on invalid types #Example: <exampleName> #2 | passed |
| TypeConversion4 - To String::[10] Fail `toString()` on invalid types #Example: <exampleName> #3 | passed |
| TypeConversion4 - To String::[10] Fail `toString()` on invalid types #Example: <exampleName> #4 | passed |
| TypeConversion4 - To String::[10] Fail `toString()` on invalid types #Example: <exampleName> #5 | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[1] Undirected match in self-relationship graph, count | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[2] Undirected match of self-relationship in self-relationship graph, count | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[3] Undirected match on simple relationship graph, count | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[4] Directed match on self-relationship graph, count | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[5] Directed match of self-relationship on self-relationship graph, count | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[6] Counting undirected self-relationships in self-relationship graph | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[7] Counting distinct undirected self-relationships in self-relationship graph | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[8] Directed match of a simple relationship, count | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[9] Counting directed self-relationships | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[10] Mixing directed and undirected pattern parts with self-relationship, count | passed |
| CountingSubgraphMatches1 - Matching subgraph patterns and count the number of matches::[11] Mixing directed and undirected pattern parts with self-relationship, undirected count | passed |

</details>

<details><summary>The empty graph (72 of 92 passed)</summary>

Setup became: the query runs in a fresh ephemeral session over Bolt, acting as a role that reaches no model domain: MATCH sees only the session's own nodes, of which there are none.

| Case | Outcome |
| --- | --- |
| Call1 - Basic procedure calling::[3] In-query call to procedure that takes no arguments and yields no results | passed |
| Call1 - Basic procedure calling::[16] In-query procedure call should fail if one of the argument expressions uses an aggregation function | passed |
| Create2 - Creating relationships::[4] Create two nodes and a single relationship in the reverse direction | passed |
| Create3 - Interoperation with other clauses::[11] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE node | passed |
| Create3 - Interoperation with other clauses::[12] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE pattern | passed |
| Create3 - Interoperation with other clauses::[13] Merge followed by multiple creates | passed |
| Create5 - Multiple hops create patterns::[1] Create a pattern with multiple hops | passed |
| Create5 - Multiple hops create patterns::[2] Create a pattern with multiple hops in the reverse direction | passed |
| Create5 - Multiple hops create patterns::[3] Create a pattern with multiple hops in varying directions | passed |
| Create5 - Multiple hops create patterns::[4] Create a pattern with multiple hops with multiple types and varying directions | passed |
| Create5 - Multiple hops create patterns::[5] Create a pattern with multiple hops and varying directions | passed |
| Delete1 - Deleting nodes::[4] Delete on null node | passed |
| Delete1 - Deleting nodes::[5] Ignore null when deleting node | failed |
| Delete1 - Deleting nodes::[6] Detach delete on null node | passed |
| Delete2 - Deleting relationships::[4] Ignore null when deleting relationship | failed |
| Delete3 - Deleting named paths::[2] Delete on null path | passed |
| Match1 - Match nodes::[1] Match non-existent nodes returns empty | passed |
| Match2 - Match relationships::[1] Match non-existent relationships returns empty | passed |
| Match3 - Match fixed length patterns::[27] Matching from null nodes should return no results owing to finding no matches | passed |
| Match7 - Optional match::[1] Simple OPTIONAL MATCH on empty graph | passed |
| Match7 - Optional match::[10] Optionally matching from null nodes should return null | passed |
| Merge1 - Merge node::[1] Merge node when no nodes exist | passed |
| Merge1 - Merge node::[2] Merge node with label | passed |
| Merge1 - Merge node::[7] Merge should work when finding multiple elements | passed |
| Merge1 - Merge node::[12] Merge should be able to merge using property of freshly created node | passed |
| Merge1 - Merge node::[13] Merge should bind a path | passed |
| Merge2 - Merge node - on create::[1] Merge node with label add label on create | passed |
| Merge2 - Merge node - on create::[2] ON CREATE on created nodes | passed |
| Merge2 - Merge node - on create::[3] Merge node with label add property on create | passed |
| Merge5 - Merge relationships::[4] Using bound variables from other updating clause | passed |
| Merge5 - Merge relationships::[10] Merge should bind a path | passed |
| Merge5 - Merge relationships::[11] Use outgoing direction when unspecified | passed |
| Merge5 - Merge relationships::[14] Using list properties via variable | passed |
| Merge9 - Merge clause interoperation with other clauses::[1] UNWIND with one MERGE | passed |
| Merge9 - Merge clause interoperation with other clauses::[2] UNWIND with multiple MERGE | passed |
| Merge9 - Merge clause interoperation with other clauses::[3] Mixing MERGE with CREATE | passed |
| Remove1 - Remove a Property::[5] Ignore null when removing property from a node | failed |
| Remove2 - Remove a Label::[5] Ignore null when removing a node label | failed |
| Return2 - Return single expression (correctly projecting an expression)::[10] Return count aggregation over an empty graph | passed |
| Return6 - Implicit grouping with aggregates::[6] Handle aggregates inside non-aggregate expressions | failed |
| Return6 - Implicit grouping with aggregates::[17] Handle constants and parameters inside an expression which contains an aggregation expression | passed |
| Return6 - Implicit grouping with aggregates::[18] Handle returned variables inside an expression which contains an aggregation expression | failed |
| Return6 - Implicit grouping with aggregates::[19] Handle returned property accesses inside an expression which contains an aggregation expression | failed |
| Return6 - Implicit grouping with aggregates::[20] Fail if not returned variables are used inside an expression which contains an aggregation expression | passed |
| Return6 - Implicit grouping with aggregates::[21] Fail if more complex expressions, even if returned, are used inside expression which contains an aggregation expression | passed |
| ReturnOrderBy6 - Aggregation expressions in order by::[1] Handle constants and parameters inside an order by item which contains an aggregation expression | passed |
| ReturnOrderBy6 - Aggregation expressions in order by::[2] Handle returned aliases inside an order by item which contains an aggregation expression | failed |
| ReturnOrderBy6 - Aggregation expressions in order by::[3] Handle returned property accesses inside an order by item which contains an aggregation expression | failed |
| ReturnOrderBy6 - Aggregation expressions in order by::[4] Fail if not returned variables are used inside an order by item which contains an aggregation expression | passed |
| ReturnOrderBy6 - Aggregation expressions in order by::[5] Fail if more complex expressions, even if returned, are used inside an order by item which contains an aggregation expression | passed |
| Set1 - Set a Property::[8] Ignore null when setting property | failed |
| Set3 - Set a Label::[8] Ignore null when setting label | failed |
| Set4 - Set all properties with a map::[5] Ignore null when setting properties using an overriding map | failed |
| Set5 - Set multiple properties with a map::[1] Ignore null when setting properties using an appending map | failed |
| Unwind1::[14] Unwind with merge | passed |
| With1 - Forward single variable::[5] Forwarding null | failed |
| With6 - Implicit grouping with aggregates::[5] Handle constants and parameters inside an expression which contains an aggregation expression | passed |
| With6 - Implicit grouping with aggregates::[6] Handle projected variables inside an expression which contains an aggregation expression | failed |
| With6 - Implicit grouping with aggregates::[7] Handle projected property accesses inside an expression which contains an aggregation expression | failed |
| With6 - Implicit grouping with aggregates::[8] Fail if not projected variables are used inside an expression which contains an aggregation expression | passed |
| With6 - Implicit grouping with aggregates::[9] Fail if more complex expression, even if projected, are used inside expression which contains an aggregation expression | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[16] Handle constants and parameters inside an order by item which contains an aggregation expression | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[17] Handle projected variables inside an order by item which contains an aggregation expression | failed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[18]  Handle projected property accesses inside an order by item which contains an aggregation expression | failed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[19] Fail if not projected variables are used inside an order by item which contains an aggregation expression | passed |
| WithOrderBy4 - Order by in combination with projection and aliasing::[20] Fail if more complex expressions, even if projected, are used inside an order by item which contains an aggregation expression | passed |
| Aggregation8 - DISTINCT::[1] Distinct on unbound node | passed |
| Graph3 - Node labels::[7] `labels()` on null node | failed |
| Graph6 - Static property access::[3] Statically access a property of a null node | passed |
| Graph6 - Static property access::[7] Statically access a property of a null relationship | passed |
| Graph9 - Retrieve all properties as a property map::[3] `properties()` on null | failed |
| List12 - List Comprehension::[3] Size of list comprehension | passed |
| Null1 - IS NULL validation::[3] Property null check on null node | passed |
| Null2 - IS NOT NULL validation::[3] Property not null check on null node | passed |
| Temporal4 - Store Temporal Values::[1] Should store date #1 | passed |
| Temporal4 - Store Temporal Values::[2] Should store date array #1 | passed |
| Temporal4 - Store Temporal Values::[2] Should store date array #2 | passed |
| Temporal4 - Store Temporal Values::[3] Should store local time #1 | passed |
| Temporal4 - Store Temporal Values::[4] Should store local time array #1 | passed |
| Temporal4 - Store Temporal Values::[4] Should store local time array #2 | passed |
| Temporal4 - Store Temporal Values::[5] Should store time #1 | passed |
| Temporal4 - Store Temporal Values::[6] Should store time array #1 | passed |
| Temporal4 - Store Temporal Values::[6] Should store time array #2 | passed |
| Temporal4 - Store Temporal Values::[7] Should store local date time #1 | passed |
| Temporal4 - Store Temporal Values::[8] Should store local date time array #1 | passed |
| Temporal4 - Store Temporal Values::[8] Should store local date time array #2 | passed |
| Temporal4 - Store Temporal Values::[9] Should store date time #1 | passed |
| Temporal4 - Store Temporal Values::[10] Should store date time array #1 | passed |
| Temporal4 - Store Temporal Values::[10] Should store date time array #2 | passed |
| Temporal4 - Store Temporal Values::[11] Should store duration #1 | passed |
| Temporal4 - Store Temporal Values::[12] Should store duration array #1 | passed |
| Temporal4 - Store Temporal Values::[12] Should store duration array #2 | passed |

</details>

<details><summary>Named TCK graphs (19 of 19 passed)</summary>

Setup became: the named graph's CREATE script, from the TCK's graphs directory, runs in one ephemeral session over Bolt, acting as a role that reaches no model domain, and the query reads what it made.

| Case | Outcome |
| --- | --- |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[1] Handling triadic friend of a friend | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[2] Handling triadic friend of a friend that is not a friend | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[3] Handling triadic friend of a friend that is not a friend with different relationship type | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[4] Handling triadic friend of a friend that is not a friend with superset of relationship type | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[5] Handling triadic friend of a friend that is not a friend with implicit subset of relationship type | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[6] Handling triadic friend of a friend that is not a friend with explicit subset of relationship type | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[7] Handling triadic friend of a friend that is not a friend with same labels | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[8] Handling triadic friend of a friend that is not a friend with different labels | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[9] Handling triadic friend of a friend that is not a friend with implicit subset of labels | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[10] Handling triadic friend of a friend that is not a friend with implicit superset of labels | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[11] Handling triadic friend of a friend that is a friend | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[12] Handling triadic friend of a friend that is a friend with different relationship type | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[13] Handling triadic friend of a friend that is a friend with superset of relationship type | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[14] Handling triadic friend of a friend that is a friend with implicit subset of relationship type | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[15] Handling triadic friend of a friend that is a friend with explicit subset of relationship type | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[16] Handling triadic friend of a friend that is a friend with same labels | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[17] Handling triadic friend of a friend that is a friend with different labels | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[18] Handling triadic friend of a friend that is a friend with implicit subset of labels | passed |
| TriadicSelection1 - Query three related nodes on binary-tree graphs::[19] Handling triadic friend of a friend that is a friend with implicit superset of labels | passed |

</details>

<details><summary>Test procedures (50 of 50 passed)</summary>

Setup became: the procedure is a statement command of the fixture model, bound in Cypher to the procedure's name, with the procedure's arguments and outputs, answering the scenario's rows for the arguments given; the query's CALL invokes it.

| Case | Outcome |
| --- | --- |
| Call1 - Basic procedure calling::[1] Standalone call to procedure that takes no arguments and yields no results | passed |
| Call1 - Basic procedure calling::[2] Standalone call to procedure that takes no arguments and yields no results, called with implicit arguments | passed |
| Call1 - Basic procedure calling::[3] In-query call to procedure that takes no arguments and yields no results | passed |
| Call1 - Basic procedure calling::[4] In-query call to procedure that takes no arguments and yields no results and consumes no rows | passed |
| Call1 - Basic procedure calling::[5] Standalone call to STRING procedure that takes no arguments | passed |
| Call1 - Basic procedure calling::[6] In-query call to STRING procedure that takes no arguments | passed |
| Call1 - Basic procedure calling::[7] Standalone call to procedure should fail if explicit argument is missing | passed |
| Call1 - Basic procedure calling::[8] In-query call to procedure should fail if explicit argument is missing | passed |
| Call1 - Basic procedure calling::[9] Standalone call to procedure should fail if too many explicit argument are given | passed |
| Call1 - Basic procedure calling::[10] In-query call to procedure should fail if too many explicit argument are given | passed |
| Call1 - Basic procedure calling::[11] Standalone call to procedure should fail if implicit argument is missing | passed |
| Call1 - Basic procedure calling::[12] In-query call to procedure that has outputs fails if no outputs are yielded | passed |
| Call1 - Basic procedure calling::[15] In-query procedure call should fail if shadowing an already bound variable | passed |
| Call1 - Basic procedure calling::[16] In-query procedure call should fail if one of the argument expressions uses an aggregation function | passed |
| Call2 - Procedure arguments::[1] In-query call to procedure with explicit arguments | passed |
| Call2 - Procedure arguments::[2] Standalone call to procedure with explicit arguments | passed |
| Call2 - Procedure arguments::[3] Standalone call to procedure with implicit arguments | passed |
| Call2 - Procedure arguments::[4] In-query call to procedure that takes arguments fails when trying to pass them implicitly | passed |
| Call2 - Procedure arguments::[5] Standalone call to procedure should fail if input type is wrong | passed |
| Call2 - Procedure arguments::[6] In-query call to procedure should fail if input type is wrong | passed |
| Call3 - Assignable-type arguments::[1] Standalone call to procedure with argument of type NUMBER accepts value of type INTEGER | passed |
| Call3 - Assignable-type arguments::[2] In-query call to procedure with argument of type NUMBER accepts value of type INTEGER | passed |
| Call3 - Assignable-type arguments::[3] Standalone call to procedure with argument of type NUMBER accepts value of type FLOAT | passed |
| Call3 - Assignable-type arguments::[4] In-query call to procedure with argument of type NUMBER accepts value of type FLOAT | passed |
| Call3 - Assignable-type arguments::[5] Standalone call to procedure with argument of type FLOAT accepts value of type INTEGER | passed |
| Call3 - Assignable-type arguments::[6] In-query call to procedure with argument of type FLOAT accepts value of type INTEGER | passed |
| Call4 - Null Arguments::[1] Standalone call to procedure with null argument | passed |
| Call4 - Null Arguments::[2] In-query call to procedure with null argument | passed |
| Call5 - Results projection::[1] Explicit procedure result projection | passed |
| Call5 - Results projection::[2] Explicit procedure result projection with RETURN * | passed |
| Call5 - Results projection::[3] The order of yield items is irrelevant #1 | passed |
| Call5 - Results projection::[3] The order of yield items is irrelevant #2 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #1 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #2 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #3 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #4 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #5 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #6 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #7 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #8 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #9 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #10 | passed |
| Call5 - Results projection::[4] Rename outputs to unbound variable names #11 | passed |
| Call5 - Results projection::[5] Fail on renaming to an already bound variable name | passed |
| Call5 - Results projection::[6] Fail on renaming all outputs to the same variable name | passed |
| Call5 - Results projection::[7] Fail on in-query call to procedure with YIELD * | passed |
| Call5 - Results projection::[8] Allow standalone call to procedure with YIELD * | passed |
| Call6 - Call clause interoperation with other clauses::[1] Calling the same STRING procedure twice using the same outputs in each call | passed |
| Call6 - Call clause interoperation with other clauses::[2] Project procedure results between query scopes with WITH clause | passed |
| Call6 - Call clause interoperation with other clauses::[3] Project procedure results between query scopes with WITH clause and rename the projection | passed |

</details>


## Failed cases

None.
<!-- conformance-results:end -->

## Deliberate differences

### Writes

`CREATE`, `MERGE`, `SET`, and `DELETE` are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name.

### Vocabulary fixed by the model

Dynamic labels and relationship types (e.g. `MATCH (n:$label)`) are not supported. Labels and relationship types are fixed by the model and must be declared there. This is a structural characteristic, not a limitation to be worked around.

### Add-ins

The APOC calls Cypher tools and agent frameworks make are offered as commands and functions: `apoc.meta.data`, `apoc.meta.schema` and `apoc.schema.nodes` answer from the role's graph, the common `apoc.text`, `apoc.coll`, `apoc.map`, `apoc.date` and `apoc.convert` functions push down with the query, and `apoc.path.expand` and `apoc.path.subgraphNodes` walk the graph as variable-length patterns. See [The APOC set Provisa ships](../commands.md#the-apoc-set-provisa-ships). Procedures that act outside the model (`apoc.create.*`, `apoc.periodic.iterate`, `apoc.load.*`) are not offered; for those, run them on your own Neo4j instance against data it reads from Provisa over Bolt. GDS algorithms are not available inline.

### Temporal values on the engine's types

Dates, times and durations are held in the engine's own types: a date as `DATE`, a time as `TIME` or `TIME WITH TIME ZONE`, a local date time as `TIMESTAMP`, a date time as `TIMESTAMP WITH TIME ZONE`, a duration as `INTERVAL` (months, days and microseconds).

- **Precision is the microsecond.** Every temporal value, read from a column or built in a query, has microsecond precision. Its `nanosecond` property is its microseconds times 1000, so the last three digits are always 000. A value written with digits below the microsecond keeps the microsecond and is not refused, and the answer writes that value as openCypher formats it. TCK scenarios that expect non-zero digits below the microsecond fail by this rule.
- **Zones.** When a query names a zone or an offset, in a literal (`'2015-07-21T21:40:32.142+01:00[Europe/Stockholm]'`) or with `timezone:`, the engine computes with it by its own time zone rules, daylight saving included, and the answer presents it as openCypher writes it: `2015-07-21T21:40:32.142+01:00[Europe/Stockholm]`. A date time read from a column that stores only an instant is presented with its offset and no zone name.
- **A duration built from computed text.** `duration('P1Y2M')` written in the statement is read as openCypher reads it. A duration built from text the query computes is read by the engine's interval parser. PostgreSQL reads ISO-8601; DuckDB does not.
- **Fractions of computed components.** `duration({months: 0.75})` written in the statement carries a fraction of a month down as openCypher's average month. When the fraction is computed, the engine carries it as 30 days.
- **The engine's range.** Years beyond the engine's (openCypher allows -999999999 to +999999999) and DuckDB offsets beyond ±15:59 are refused by the engine.
- **Engines with no interval of months and seconds.** Trino has no interval type that holds both a month part and a time part, so a statement with a duration is refused by Trino and answered as the engine's refusal. A source whose own SQL has no interval reads such a statement through the engine.

### Scenarios that build their own graph

TCK scenarios that create their graph (`CREATE` in their setup, a named TCK graph, a test procedure) are not runnable, because a Provisa graph's labels and relationship types are its model's. The pass rate counts the scenarios that run.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Neo4j Browser | — | — | Covered by driver (Neo4j JavaScript driver) |
| Neo4j Bloom | — | — | Covered by driver (Neo4j JavaScript driver) |
| neo4j-driver (Python) | — | — | Covered by driver |
| neo4j-driver (Java) | — | — | Covered by driver |
