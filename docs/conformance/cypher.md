# Cypher Conformance

**Suite:** openCypher TCK 2024.3.

**Pass rate:** 23.2% (675 of 2912); 968 cases not runnable by design.

<!-- conformance-results:begin -->
## Failing cases

<details><summary>Create1 - Creating nodes (12)</summary>

| Case | Reason |
| --- | --- |
| [1] Create a single node | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] Create two nodes | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [3] Create a single node with a label | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [4] Create two nodes with same label | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [5] Create a single node with multiple labels | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [6] Create three nodes with multiple labels | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [7] Create a single node with a property | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [8] Create a single node with a property and return it | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [9] Create a single node with two properties | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [10] Create a single node with two properties and return them | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [11] Create a single node with null properties should not return those properties | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [12] CREATE does not lose precision on large integers | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Create2 - Creating relationships (12)</summary>

| Case | Reason |
| --- | --- |
| [1] Create two nodes and a single relationship in a single pattern | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] Create two nodes and a single relationship in separate patterns | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [3] Create two nodes and a single relationship in separate clauses | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [4] Create two nodes and a single relationship in the reverse direction | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [7] Create a single node and a single self loop in a single pattern | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [8] Create a single node and a single self loop in separate patterns | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [9] Create a single node and a single self loop in separate clauses | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [13] Create a single relationship with a property | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [14] Create a single relationship with a property and return it | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [15] Create a single relationship with two properties | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [16] Create a single relationship with two properties and return them | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [17] Create a single relationship with null properties should not return those properties | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Create3 - Interoperation with other clauses (5)</summary>

| Case | Reason |
| --- | --- |
| [9] WITH-CREATE: A bound node should be recognized after projection with WITH + WITH | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [10] WITH-UNWIND-CREATE: A bound node should be recognized after projection with WITH + UNWIND | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [11] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE node | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [12] WITH-MERGE-CREATE: A bound node should be recognized after projection with WITH + MERGE pattern | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [13] Merge followed by multiple creates | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Create4 - Large Create Query (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Generate the movie graph | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] Many CREATE clauses | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Create5 - Multiple hops create patterns (5)</summary>

| Case | Reason |
| --- | --- |
| [1] Create a pattern with multiple hops | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] Create a pattern with multiple hops in the reverse direction | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [3] Create a pattern with multiple hops in varying directions | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [4] Create a pattern with multiple hops with multiple types and varying directions | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [5] Create a pattern with multiple hops and varying directions | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Create6 - Persistence of create clause side effects (14)</summary>

| Case | Reason |
| --- | --- |
| [1] Limiting to zero results after creating nodes affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] Skipping all results after creating nodes affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [3] Skipping and limiting to a few results after creating nodes does not affect the result set nor the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [4] Skipping zero result and limiting to all results after creating nodes does not affect the result set nor the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [5] Filtering after creating nodes affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [6] Aggregating in `RETURN` after creating nodes affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [7] Aggregating in `WITH` after creating nodes affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [8] Limiting to zero results after creating relationships affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [9] Skipping all results after creating relationships affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [10] Skipping and limiting to a few results after creating relationships does not affect the result set nor the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [11] Skipping zero result and limiting to all results after creating relationships does not affect the result set nor the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [12] Filtering after creating relationships affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [13] Aggregating in `RETURN` after creating relationships affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [14] Aggregating in `WITH` after creating relationships affects the result set but not the side effects | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Delete1 - Deleting nodes (3)</summary>

| Case | Reason |
| --- | --- |
| [4] Delete on null node | refused (400): Write clause 'DELETE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [5] Ignore null when deleting node | refused (400): Write clause 'DELETE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [6] Detach delete on null node | refused (400): Write clause 'DETACH' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Delete2 - Deleting relationships (1)</summary>

| Case | Reason |
| --- | --- |
| [4] Ignore null when deleting relationship | refused (400): Write clause 'DELETE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Delete3 - Deleting named paths (1)</summary>

| Case | Reason |
| --- | --- |
| [2] Delete on null path | refused (400): Write clause 'DETACH' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Match1 - Match nodes (7)</summary>

| Case | Reason |
| --- | --- |
| [1] Match non-existent nodes returns empty | 930 row(s), expected 0 |
| [9] Fail when a relationship has the same variable in the same pattern #1 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #2 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #3 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #4 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #11 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #12 | expected SyntaxError; the query ran |

</details>

<details><summary>Match2 - Match relationships (8)</summary>

| Case | Reason |
| --- | --- |
| [1] Match non-existent relationships returns empty | 1399 row(s), expected 0 |
| [11] Fail when a node has the same variable in the same pattern #1 | expected SyntaxError; the query ran |
| [11] Fail when a node has the same variable in the same pattern #2 | expected SyntaxError; the query ran |
| [11] Fail when a node has the same variable in the same pattern #3 | expected SyntaxError; the query ran |
| [11] Fail when a node has the same variable in the same pattern #7 | expected SyntaxError; the query ran |
| [11] Fail when a node has the same variable in the same pattern #8 | expected SyntaxError; the query ran |
| [11] Fail when a node has the same variable in the same pattern #14 | expected SyntaxError; the query ran |
| [11] Fail when a node has the same variable in the same pattern #15 | expected SyntaxError; the query ran |

</details>

<details><summary>Match3 - Match fixed length patterns (1)</summary>

| Case | Reason |
| --- | --- |
| [27] Matching from null nodes should return no results owing to finding no matches | refused (403): Invalid JOIN: traces and queries are combined with no condition relating them — cross joins are not permitted; Invalid JOIN: mv_refresh_lineage and queries are combined with no condition relating them |

</details>

<details><summary>Match5 - Match variable length patterns over given graphs scenarios (24)</summary>

| Case | Reason |
| --- | --- |
| [1] Handling unbounded variable length match | refused (400): Unbounded variable-length pattern [*] is not allowed. Specify a depth limit, e.g. [*..5]. |
| [2] Handling explicitly unbounded variable length match | refused (400): Expected 'NUMBER', got 'RBRACKET' (']') at pos 38 |
| [3] Handling single bounded variable length match 1 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [4] Handling single bounded variable length match 2 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [5] Handling single bounded variable length match 3 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [6] Handling upper and lower bounded variable length match 1 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [7] Handling upper and lower bounded variable length match 2 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [8] Handling symmetrically bounded variable length match, bounds are zero | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [9] Handling symmetrically bounded variable length match, bounds are one | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [10] Handling symmetrically bounded variable length match, bounds are two | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [11] Handling upper and lower bounded variable length match, empty interval 1 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [12] Handling upper and lower bounded variable length match, empty interval 2 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [13] Handling upper bounded variable length match, empty interval | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [14] Handling upper bounded variable length match 1 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [15] Handling upper bounded variable length match 2 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [16] Handling lower bounded variable length match 1 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [17] Handling lower bounded variable length match 2 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [18] Handling lower bounded variable length match 3 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [19] Handling a variable length relationship and a standard relationship in chain, zero length 1 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [20] Handling a variable length relationship and a standard relationship in chain, zero length 2 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [21] Handling a variable length relationship and a standard relationship in chain, single length 1 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [22] Handling a variable length relationship and a standard relationship in chain, single length 2 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [23] Handling a variable length relationship and a standard relationship in chain, longer 1 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |
| [24] Handling a variable length relationship and a standard relationship in chain, longer 2 | refused (400): There is no relationship type :LIKES. Registered types: :REPLACES, :DQ_RULES, :EDGES. |

</details>

<details><summary>Match6 - Match named paths scenarios (8)</summary>

| Case | Reason |
| --- | --- |
| [23] Fail when a node has the same variable in the same pattern #1 | expected SyntaxError; the query ran |
| [23] Fail when a node has the same variable in the same pattern #2 | expected SyntaxError; the query ran |
| [23] Fail when a node has the same variable in the same pattern #3 | expected SyntaxError; the query ran |
| [23] Fail when a node has the same variable in the same pattern #4 | expected SyntaxError; the query ran |
| [23] Fail when a node has the same variable in the same pattern #5 | expected SyntaxError; the query ran |
| [23] Fail when a node has the same variable in the same pattern #6 | expected SyntaxError; the query ran |
| [24] Fail when a relationship has the same variable in the same pattern #1 | expected SyntaxError; the query ran |
| [24] Fail when a relationship has the same variable in the same pattern #2 | expected SyntaxError; the query ran |

</details>

<details><summary>Match7 - Optional match (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Simple OPTIONAL MATCH on empty graph | 2061 row(s), expected 1 |
| [10] Optionally matching from null nodes should return null | refused (403): Invalid JOIN: traces and queries are combined with no condition relating them — cross joins are not permitted; Invalid JOIN: mv_refresh_lineage and queries are combined with no condition relating them |

</details>

<details><summary>Merge1 - Merge node (5)</summary>

| Case | Reason |
| --- | --- |
| [1] Merge node when no nodes exist | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] Merge node with label | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [7] Merge should work when finding multiple elements | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [12] Merge should be able to merge using property of freshly created node | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [13] Merge should bind a path | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Merge2 - Merge node - on create (3)</summary>

| Case | Reason |
| --- | --- |
| [1] Merge node with label add label on create | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] ON CREATE on created nodes | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [3] Merge node with label add property on create | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Merge5 - Merge relationships (4)</summary>

| Case | Reason |
| --- | --- |
| [4] Using bound variables from other updating clause | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [10] Merge should bind a path | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [11] Use outgoing direction when unspecified | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [14] Using list properties via variable | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Merge9 - Merge clause interoperation with other clauses (3)</summary>

| Case | Reason |
| --- | --- |
| [1] UNWIND with one MERGE | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] UNWIND with multiple MERGE | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [3] Mixing MERGE with CREATE | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Remove1 - Remove a Property (1)</summary>

| Case | Reason |
| --- | --- |
| [5] Ignore null when removing property from a node | refused (400): Write clause 'REMOVE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Remove2 - Remove a Label (1)</summary>

| Case | Reason |
| --- | --- |
| [5] Ignore null when removing a node label | refused (400): Write clause 'REMOVE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Return2 - Return single expression (correctly projecting an expression) (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Arithmetic expressions should propagate null values | refused (500): Internal server error |
| [10] Return count aggregation over an empty graph | columns ['(count(a) > 0)'] != ['count(a) > 0'] |

</details>

<details><summary>Return6 - Implicit grouping with aggregates (4)</summary>

| Case | Reason |
| --- | --- |
| [6] Handle aggregates inside non-aggregate expressions | refused (400): There is no relationship type :FATHER. Registered types: :CLASS_TERM, :VIA_TABLE, :TERM_TAGS. |
| [17] Handle constants and parameters inside an expression which contains an aggregation expression | refused (500): Execution failed: function avg(text) does not exist
LINE 1: SELECT $1 + AVG(person."age") - 1000 FROM (SELECT NULL AS __...
                    ^
HINT:  No function matches the given name and argument |
| [18] Handle returned variables inside an expression which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |
| [19] Handle returned property accesses inside an expression which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |

</details>

<details><summary>ReturnOrderBy1 - Order by a single variable (correct order of values according to their type) (2)</summary>

| Case | Reason |
| --- | --- |
| [9] ORDER BY should order lists in the expected order | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT _uw0."lists" FROM UNNEST(ARRAY[ARRAY[], ARRAY['a'], A...
                                              ^
HINT:  Explicitly cast to |
| [10] ORDER BY DESC should order lists in the expected order | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT _uw0."lists" FROM UNNEST(ARRAY[ARRAY[], ARRAY['a'], A...
                                              ^
HINT:  Explicitly cast to |

</details>

<details><summary>ReturnOrderBy4 - Order by in combination with projection (1)</summary>

| Case | Reason |
| --- | --- |
| [1] ORDER BY of a column introduced in RETURN should return salient results in ascending order | refused (400): Pipeline segment has no data source |

</details>

<details><summary>ReturnOrderBy6 - Aggregation expressions in order by (3)</summary>

| Case | Reason |
| --- | --- |
| [1] Handle constants and parameters inside an order by item which contains an aggregation expression | refused (500): Execution failed: function avg(text) does not exist
LINE 1: SELECT AVG(person."age") AS avgAge FROM (SELECT NULL AS __la...
               ^
HINT:  No function matches the given name and argument type |
| [2] Handle returned aliases inside an order by item which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |
| [3] Handle returned property accesses inside an order by item which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |

</details>

<details><summary>ReturnSkipLimit1 - Skip (1)</summary>

| Case | Reason |
| --- | --- |
| [4] Accept skip zero | columns [] != ['n'] |

</details>

<details><summary>ReturnSkipLimit2 - Limit (1)</summary>

| Case | Reason |
| --- | --- |
| [5] ORDER BY with LIMIT 0 should not generate errors | refused (400): There is no label :Person. Registered labels: :Queries, :PiiAccess, :MvRefreshLineage. |

</details>

<details><summary>Set1 - Set a Property (3)</summary>

| Case | Reason |
| --- | --- |
| [6] Concatenate elements onto a list property | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [7] Concatenate elements in reverse onto a list property | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [8] Ignore null when setting property | refused (400): Write clause 'SET' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Set3 - Set a Label (1)</summary>

| Case | Reason |
| --- | --- |
| [8] Ignore null when setting label | refused (400): Write clause 'SET' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Set4 - Set all properties with a map (1)</summary>

| Case | Reason |
| --- | --- |
| [5] Ignore null when setting properties using an overriding map | refused (400): Write clause 'SET' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Set5 - Set multiple properties with a map (1)</summary>

| Case | Reason |
| --- | --- |
| [1] Ignore null when setting properties using an appending map | refused (400): Write clause 'SET' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Union1 - Union (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Two elements, both unique, distinct | refused (400): Query has no data source |
| [2] Three elements, two unique, distinct | refused (400): Query has no data source |

</details>

<details><summary>Union2 - Union All (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Two elements, both unique, not distinct | refused (400): Query has no data source |
| [2] Three elements, two unique, not distinct | refused (400): Query has no data source |

</details>

<details><summary>Unwind1 (9)</summary>

| Case | Reason |
| --- | --- |
| [2] Unwinding a range | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT _uw0."x" FROM UNNEST(SEQUENCE(1, 3)) AS _uw0(x)
                                    ^
HINT:  No function matches the |
| [3] Unwinding a concatenation of lists | refused (400): Pipeline segment has no data source |
| [4] Unwinding a collected unwound expression | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: ...(SELECT ARRAY_AGG(_uw0."row") AS rows FROM UNNEST(SEQUENCE(1...
                                                         |
| [7] Double unwinding a list of lists | refused (400): Pipeline segment has no data source |
| [8] Unwinding the empty list | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT _uw0."empty" FROM UNNEST(ARRAY[]) AS _uw0(empty)
                                        ^
HINT:  Explicitly cast to the desired t |
| [9] Unwinding null | refused (500): Execution failed: function unnest(unknown) is not unique
LINE 1: SELECT _uw0."nil" FROM UNNEST(NULL) AS _uw0(nil)
                               ^
HINT:  Could not choose a best candidate function. Yo |
| [11] Unwind does not prune context | refused (500): Internal server error |
| [13] Multiple unwinds after each other | refused (500): Internal server error |
| [14] Unwind with merge | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>With1 - Forward single variable (1)</summary>

| Case | Reason |
| --- | --- |
| [5] Forwarding null | refused (500): Internal server error |

</details>

<details><summary>With2 - Forward single expression (1)</summary>

| Case | Reason |
| --- | --- |
| [2] Forwarding a nested map literal | refused (400): Pipeline segment has no data source |

</details>

<details><summary>With4 - Variable aliasing (1)</summary>

| Case | Reason |
| --- | --- |
| [7] Multiple aliasing and backreferencing | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>With6 - Implicit grouping with aggregates (3)</summary>

| Case | Reason |
| --- | --- |
| [5] Handle constants and parameters inside an expression which contains an aggregation expression | refused (500): Internal server error |
| [6] Handle projected variables inside an expression which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |
| [7] Handle projected property accesses inside an expression which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |

</details>

<details><summary>WithOrderBy1 - Order by a single variable (24)</summary>

| Case | Reason |
| --- | --- |
| [9] Sort lists in ascending order | refused (500): Execution failed: cannot determine type of empty array
LINE 1: ...(SELECT _uw0."lists" AS "lists" FROM UNNEST(ARRAY[ARRAY[], A...
                                                             ^
HINT:   |
| [10] Sort lists in descending order | refused (500): Execution failed: cannot determine type of empty array
LINE 1: ...(SELECT _uw0."lists" AS "lists" FROM UNNEST(ARRAY[ARRAY[], A...
                                                             ^
HINT:   |
| [11] Sort dates in ascending order | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT dates AS "dates" FROM UNNEST([CAST(MAP(['year', 'month', 'day'], [ |
| [12] Sort dates in descending order | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT dates AS "dates" FROM UNNEST([CAST(MAP(['year', 'month', 'day'], [ |
| [13] Sort local times in ascending order | refused (500): Internal server error |
| [14] Sort local times in descending order | refused (500): Internal server error |
| [15] Sort times in ascending order | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 175.
  'minute', 'timezone'], [TO_JSON(10), TO_JSON(35), TO_JSON('-08:00')]) AS TIME |
| [16] Sort times in descending order | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 175.
  'minute', 'timezone'], [TO_JSON(10), TO_JSON(35), TO_JSON('-08:00')]) AS TIME |
| [17] Sort local date times in ascending order | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: ... AS (SELECT localdatetimes AS "localdatetimes" FROM UNNEST([LOCAL |
| [18] Sort local date times in descending order | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: ... AS (SELECT localdatetimes AS "localdatetimes" FROM UNNEST([LOCAL |
| [19] Sort date times in ascending order | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [20] Sort date times in descending order | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [44] Sort by a variable that is only partially orderable on a non-distinct binding table, but made distinct #1 | refused (500): Internal server error |
| [44] Sort by a variable that is only partially orderable on a non-distinct binding table, but made distinct #2 | refused (500): Internal server error |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #1 | refused (400): Pipeline segment has no data source |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #2 | refused (400): Pipeline segment has no data source |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #3 | refused (400): Pipeline segment has no data source |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #4 | refused (400): Pipeline segment has no data source |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #5 | refused (400): Pipeline segment has no data source |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #6 | refused (400): Pipeline segment has no data source |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #7 | refused (400): Pipeline segment has no data source |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #8 | refused (400): Pipeline segment has no data source |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #9 | refused (400): Pipeline segment has no data source |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #10 | refused (400): Pipeline segment has no data source |

</details>

<details><summary>WithOrderBy4 - Order by in combination with projection and aliasing (3)</summary>

| Case | Reason |
| --- | --- |
| [16] Handle constants and parameters inside an order by item which contains an aggregation expression | refused (500): Execution failed: function avg(text) does not exist
LINE 1: WITH _w0 AS (SELECT AVG(person."age") AS avgAge FROM (SELECT...
                            ^
HINT:  No function matches the given name and  |
| [17] Handle projected variables inside an order by item which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |
| [18]  Handle projected property accesses inside an order by item which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |

</details>

<details><summary>Aggregation2 - Min and Max (12)</summary>

| Case | Reason |
| --- | --- |
| [1] `max()` over integers | columns ['max'] != ['max(x)'] |
| [2] `min()` over integers | columns ['min'] != ['min(x)'] |
| [3] `max()` over floats | columns ['max'] != ['max(x)'] |
| [4] `min()` over floats | columns ['min'] != ['min(x)'] |
| [5] `max()` over mixed numeric values | columns ['max'] != ['max(x)'] |
| [6] `min()` over mixed numeric values | columns ['min'] != ['min(x)'] |
| [7] `max()` over strings | columns ['max'] != ['max(i)'] |
| [8] `min()` over strings | columns ['min'] != ['min(i)'] |
| [9] `max()` over list values | refused (500): Execution failed: multidimensional arrays must have array expressions with matching dimensions |
| [10] `min()` over list values | refused (500): Execution failed: multidimensional arrays must have array expressions with matching dimensions |
| [11] `max()` over mixed values | refused (500): Execution failed: ARRAY types integer and integer[] cannot be matched
LINE 1: ...ECT MAX(_uw0."x") FROM UNNEST(ARRAY[1, 'a', NULL, ARRAY[1, 2...
                                                        |
| [12] `min()` over mixed values | refused (500): Execution failed: ARRAY types integer and integer[] cannot be matched
LINE 1: ...ECT MIN(_uw0."x") FROM UNNEST(ARRAY[1, 'a', NULL, ARRAY[1, 2...
                                                        |

</details>

<details><summary>Aggregation3 - Sum (1)</summary>

| Case | Reason |
| --- | --- |
| [2] No overflow during summation | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: WITH _w0 AS (SELECT _uw0."i" AS "i" FROM UNNEST(SEQUENCE(100...
                                                        ^
H |

</details>

<details><summary>Aggregation8 - DISTINCT (3)</summary>

| Case | Reason |
| --- | --- |
| [1] Distinct on unbound node | rows differ: got [[2243]] |
| [3] Collect distinct nulls | rows differ: got [[[None]]] |
| [4] Collect distinct values mixed with nulls | rows differ: got [[[1, None]]] |

</details>

<details><summary>Boolean1 - And logical operations (5)</summary>

| Case | Reason |
| --- | --- |
| [1] Conjunction of two truth values | refused (400): Query has no data source |
| [2] Conjunction of three truth values | refused (400): Query has no data source |
| [3] Conjunction of many truth values | refused (400): Query has no data source |
| [5] Conjunction is commutative on null | rows differ: got [[True, None, True], [False, None, False], [None, True, True]] |
| [7] Conjunction is associative on null | rows differ: got [[True, None, True, True], [False, None, True, False], [None, True, True, True]] |

</details>

<details><summary>Boolean2 - OR logical operations (5)</summary>

| Case | Reason |
| --- | --- |
| [1] Disjunction of two truth values | refused (400): Query has no data source |
| [2] Disjunction of three truth values | refused (400): Query has no data source |
| [3] Disjunction of many truth values | refused (400): Query has no data source |
| [5] Disjunction is commutative on null | rows differ: got [[True, None, False], [False, None, True], [None, True, False]] |
| [7] Disjunction is associative on null | rows differ: got [[True, None, True, False], [False, None, True, False], [None, True, True, False]] |

</details>

<details><summary>Boolean3 - XOR logical operations (3)</summary>

| Case | Reason |
| --- | --- |
| [1] Exclusive disjunction of two truth values | refused (400): Query has no data source |
| [2] Exclusive disjunction of three truth values | refused (400): Query has no data source |
| [3] Exclusive disjunction of many truth values | refused (400): Query has no data source |

</details>

<details><summary>Boolean4 - NOT logical operations (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Logical negation of truth values | refused (400): Query has no data source |
| [2] Double logical negation of truth values | refused (400): Query has no data source |

</details>

<details><summary>Boolean5 - Interop of logical operations (3)</summary>

| Case | Reason |
| --- | --- |
| [2] Disjunction is distributive over conjunction on null | rows differ: got [[True, None, True, False], [False, None, True, True], [None, True, True, False]] |
| [4] Conjunction is distributive over disjunction on null | rows differ: got [[True, None, True, False], [False, None, True, False], [None, True, True, True]] |
| [6] Conjunction is not distributive over exclusive disjunction on null | rows differ: got [[True, None, True, True], [False, None, True, False], [None, True, True, True]] |

</details>

<details><summary>Comparison1 - Equality (32)</summary>

| Case | Reason |
| --- | --- |
| [6] Comparing lists to lists #1 | refused (400): Query has no data source |
| [6] Comparing lists to lists #2 | refused (400): Query has no data source |
| [6] Comparing lists to lists #3 | refused (400): Query has no data source |
| [6] Comparing lists to lists #4 | refused (400): Query has no data source |
| [6] Comparing lists to lists #5 | refused (400): Query has no data source |
| [6] Comparing lists to lists #6 | refused (400): Query has no data source |
| [7] Comparing maps to maps #1 | refused (400): Query has no data source |
| [7] Comparing maps to maps #2 | refused (400): Query has no data source |
| [7] Comparing maps to maps #3 | refused (400): Query has no data source |
| [7] Comparing maps to maps #4 | refused (400): Query has no data source |
| [7] Comparing maps to maps #5 | refused (400): Query has no data source |
| [7] Comparing maps to maps #6 | refused (400): Query has no data source |
| [7] Comparing maps to maps #7 | refused (400): Query has no data source |
| [7] Comparing maps to maps #8 | refused (400): Query has no data source |
| [7] Comparing maps to maps #9 | refused (400): Query has no data source |
| [7] Comparing maps to maps #10 | refused (400): Query has no data source |
| [7] Comparing maps to maps #11 | refused (400): Query has no data source |
| [7] Comparing maps to maps #12 | refused (400): Query has no data source |
| [7] Comparing maps to maps #13 | refused (400): Query has no data source |
| [7] Comparing maps to maps #14 | refused (400): Query has no data source |
| [7] Comparing maps to maps #15 | refused (400): Query has no data source |
| [7] Comparing maps to maps #16 | refused (400): Query has no data source |
| [8] Equality and inequality of NaN #1 | refused (400): Query has no data source |
| [8] Equality and inequality of NaN #2 | refused (400): Query has no data source |
| [8] Equality and inequality of NaN #3 | refused (400): Query has no data source |
| [8] Equality and inequality of NaN #4 | refused (400): Query has no data source |
| [9] Equality between strings and numbers #1 | refused (400): Query has no data source |
| [9] Equality between strings and numbers #2 | refused (400): Query has no data source |
| [9] Equality between strings and numbers #3 | refused (400): Query has no data source |
| [9] Equality between strings and numbers #4 | refused (400): Query has no data source |
| [15] It is unknown - i.e. null - if a null is equal to a null | refused (400): Query has no data source |
| [16] It is unknown - i.e. null - if a null is not equal to a null | refused (400): Query has no data source |

</details>

<details><summary>Comparison2 - Half-bounded Range (13)</summary>

| Case | Reason |
| --- | --- |
| [4] Comparing lists #1 | refused (400): Query has no data source |
| [4] Comparing lists #2 | refused (400): Query has no data source |
| [4] Comparing lists #3 | refused (400): Query has no data source |
| [4] Comparing lists #4 | refused (400): Query has no data source |
| [4] Comparing lists #5 | refused (400): Query has no data source |
| [5] Comparing NaN #1 | refused (400): Query has no data source |
| [5] Comparing NaN #2 | refused (400): Query has no data source |
| [5] Comparing NaN #3 | refused (400): Query has no data source |
| [5] Comparing NaN #4 | refused (400): Query has no data source |
| [6] Comparability between numbers and strings #1 | refused (400): Query has no data source |
| [6] Comparability between numbers and strings #2 | refused (400): Query has no data source |
| [6] Comparability between numbers and strings #3 | refused (400): Query has no data source |
| [6] Comparability between numbers and strings #4 | refused (400): Query has no data source |

</details>

<details><summary>Conditional2 - Case Expression (12)</summary>

| Case | Reason |
| --- | --- |
| [1] Simple cases over integers #1 | refused (400): Query has no data source |
| [1] Simple cases over integers #2 | refused (400): Query has no data source |
| [1] Simple cases over integers #3 | refused (400): Query has no data source |
| [1] Simple cases over integers #4 | refused (400): Query has no data source |
| [1] Simple cases over integers #5 | refused (400): Query has no data source |
| [1] Simple cases over integers #6 | refused (400): Query has no data source |
| [1] Simple cases over integers #7 | refused (400): Query has no data source |
| [1] Simple cases over integers #8 | refused (400): Query has no data source |
| [1] Simple cases over integers #9 | refused (400): Query has no data source |
| [1] Simple cases over integers #10 | refused (400): Query has no data source |
| [1] Simple cases over integers #11 | refused (400): Query has no data source |
| [1] Simple cases over integers #12 | refused (400): Query has no data source |

</details>

<details><summary>Graph3 - Node labels (5)</summary>

| Case | Reason |
| --- | --- |
| [1] Creating node without label | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] Creating node with two labels | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [3] Ignore space when creating node with labels | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [4] Create node with label in pattern | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [7] `labels()` on null node | refused (400): There is no label :DoesNotExist. Registered labels: :Domains, :RolesParents, :JoinHotspots. |

</details>

<details><summary>Graph6 - Static property access (2)</summary>

| Case | Reason |
| --- | --- |
| [3] Statically access a property of a null node | columns [] != ['n.missing'] |
| [7] Statically access a property of a null relationship | columns ["(r__properties ->> '$.missing')"] != ['r.missing'] |

</details>

<details><summary>Graph7 - Dynamic property access (2)</summary>

| Case | Reason |
| --- | --- |
| [2] Execute n['name'] in update queries | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [3] Use dynamic property lookup based on parameters when there is lhs type information | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>Graph9 - Retrieve all properties as a property map (2)</summary>

| Case | Reason |
| --- | --- |
| [3] `properties()` on null | refused (400): There is no relationship type :NOT_THERE. Registered types: :IS_TRACE, :HAS_TRACES, :DOWNSTREAM. |
| [4] `properties()` on a map | refused (400): Query has no data source |

</details>

<details><summary>List1 - Dynamic Element Access (5)</summary>

| Case | Reason |
| --- | --- |
| [1] Indexing into literal list | refused (400): Query has no data source |
| [2] Indexing into nested literal lists | refused (400): Query has no data source |
| [3] Use list lookup based on parameters when there is no type information | refused (400): Pipeline segment has no data source |
| [4] Use list lookup based on parameters when there is lhs type information | refused (400): Pipeline segment has no data source |
| [5] Use list lookup based on parameters when there is rhs type information | refused (400): Pipeline segment has no data source |

</details>

<details><summary>List11 - Create a list from a range (41)</summary>

| Case | Reason |
| --- | --- |
| [1] Create list from `range()` with default step #1 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #2 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #3 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #4 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #5 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #6 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #7 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #8 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #9 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #10 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #11 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #12 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #13 | refused (400): Query has no data source |
| [1] Create list from `range()` with default step #14 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #1 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #2 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #3 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #4 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #5 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #6 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #7 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #8 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #9 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #10 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #11 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #12 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #13 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #14 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #15 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #16 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #17 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #18 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #19 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #20 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #21 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #22 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #23 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #24 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #25 | refused (400): Query has no data source |
| [2] Create list from `range()` with explicitly given step #26 | refused (400): Query has no data source |
| [3] Create an empty list if range direction and step direction are inconsistent | refused (400): Pipeline segment has no data source |

</details>

<details><summary>List12 - List Comprehension (2)</summary>

| Case | Reason |
| --- | --- |
| [3] Size of list comprehension | refused (400): The engine refused the query: Catalog Error: Scalar Function with name collect does not exist!
Did you mean "col_description"?

LINE 1: SELECT LENGTH(ARRAY(SELECT x FROM UNNEST(COLLECT(r)) AS _t0(x) W |
| [7] Fail when using aggregation in list comprehension | expected SyntaxError; the query ran |

</details>

<details><summary>List2 - List Slicing (15)</summary>

| Case | Reason |
| --- | --- |
| [1] List slice | refused (400): Pipeline segment has no data source |
| [2] List slice with implicit end | refused (400): Pipeline segment has no data source |
| [3] List slice with implicit start | refused (400): Pipeline segment has no data source |
| [4] List slice with singleton range | refused (400): Pipeline segment has no data source |
| [5] List slice with empty range | refused (400): Pipeline segment has no data source |
| [6] List slice with negative range | refused (400): Pipeline segment has no data source |
| [7] List slice with invalid range | refused (400): Pipeline segment has no data source |
| [8] List slice with exceeding range | refused (400): Pipeline segment has no data source |
| [9] List slice with null range #1 | refused (400): Pipeline segment has no data source |
| [9] List slice with null range #2 | refused (400): Pipeline segment has no data source |
| [9] List slice with null range #3 | refused (400): Pipeline segment has no data source |
| [9] List slice with null range #4 | refused (400): Pipeline segment has no data source |
| [9] List slice with null range #5 | refused (400): Pipeline segment has no data source |
| [10] List slice with parameterised range | refused (400): Pipeline segment has no data source |
| [11] List slice with parameterised invalid range | refused (400): Pipeline segment has no data source |

</details>

<details><summary>List3 - List Equality (7)</summary>

| Case | Reason |
| --- | --- |
| [1] Equality between list and literal should return false | refused (400): Query has no data source |
| [2] Equality of lists of different length should return false despite nulls | refused (400): Query has no data source |
| [3] Equality between different lists with null should return false | refused (400): Query has no data source |
| [4] Equality between almost equal lists with null should return null | refused (400): Query has no data source |
| [5] Equality of nested lists of different length should return false despite nulls | refused (400): Query has no data source |
| [6] Equality between different nested lists with null should return false | refused (400): Query has no data source |
| [7] Equality between almost equal nested lists with null should return null | refused (400): Query has no data source |

</details>

<details><summary>List4 - List Concatenation (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Concatenating lists of same type | refused (400): Query has no data source |
| [2] Concatenating a list with a scalar of same type | refused (400): Query has no data source |

</details>

<details><summary>List5 - List Membership Validation - IN Operator (41)</summary>

| Case | Reason |
| --- | --- |
| [1] IN should work with nested list subscripting | refused (400): Pipeline segment has no data source |
| [2] IN should work with nested literal list subscripting | refused (400): Query has no data source |
| [3] IN should work with list slices | refused (400): Pipeline segment has no data source |
| [4] IN should work with literal list slices | refused (400): Query has no data source |
| [5] IN should return false when matching a number with a string | refused (400): Query has no data source |
| [6] IN should return false when matching a number with a string - list version | refused (400): Query has no data source |
| [7] IN should return false when types of LHS and RHS don't match - singleton list | refused (400): Query has no data source |
| [8] IN should return false when types of LHS and RHS don't match - list | refused (400): Query has no data source |
| [9] IN should return true when types of LHS and RHS match - singleton list | refused (400): Query has no data source |
| [10] IN should return true when types of LHS and RHS match - list | refused (400): Query has no data source |
| [11] IN should return false when order of elements in LHS list and RHS list don't match | refused (400): Query has no data source |
| [12] IN with different length lists should return false | refused (400): Query has no data source |
| [13] IN should return false when matching a list with a nested list with same elements | refused (400): Query has no data source |
| [14] IN should return true when both LHS and RHS contain nested lists | refused (400): Query has no data source |
| [15] IN should return true when both LHS and RHS contain a nested list alongside a scalar element | refused (400): Query has no data source |
| [16] IN should return true when LHS and RHS contain a nested list - singleton version | refused (400): Query has no data source |
| [17] IN should return true when LHS and RHS contain a nested list | refused (400): Query has no data source |
| [18] IN should return false when LHS contains a nested list and type mismatch on RHS - singleton version | refused (400): Query has no data source |
| [19] IN should return false when LHS contains a nested list and type mismatch on RHS | refused (400): Query has no data source |
| [20] IN should return null if LHS and RHS are null | refused (400): Query has no data source |
| [21] IN should return null if LHS and RHS are null - list version | refused (400): Query has no data source |
| [22] IN should return null when LHS and RHS both ultimately contain null, even if LHS and RHS are of different types (nested list and flat list) | refused (400): Query has no data source |
| [23] IN with different length lists should return false despite nulls | refused (400): Query has no data source |
| [24] IN should return true if match despite nulls | refused (400): Query has no data source |
| [25] IN should return null if comparison with null is required | refused (400): Query has no data source |
| [26] IN should return true if correct list found despite other lists having nulls | refused (400): Query has no data source |
| [27] IN should return true if correct list found despite null being another element within containing list | refused (400): Query has no data source |
| [28] IN should return false if no match can be found, despite nulls | refused (400): Query has no data source |
| [29] IN should return null if comparison with null is required, list version | refused (400): Query has no data source |
| [30] IN should return false if different length lists compared, even if the extra element is null | refused (400): Query has no data source |
| [31] IN should return null when comparing two so-called identical lists where one element is null | refused (400): Query has no data source |
| [32] IN should return true with previous null match, list version | refused (400): Query has no data source |
| [33] IN should return false if different length lists with nested elements compared, even if the extra element is null | refused (400): Query has no data source |
| [34] IN should return null if comparison with null is required, list version 2 | refused (400): Query has no data source |
| [35] IN should work with an empty list | refused (400): Query has no data source |
| [36] IN should return false for the empty list if the LHS and RHS types differ | refused (400): Query has no data source |
| [37] IN should work with an empty list in the presence of other list elements: matching | refused (400): Query has no data source |
| [38] IN should work with an empty list in the presence of other list elements: not matching | refused (400): Query has no data source |
| [39] IN should work with an empty list when comparing nested lists | refused (400): Query has no data source |
| [40] IN should return null if comparison with null is required for empty list | refused (400): Query has no data source |
| [41] IN should return true when LHS and RHS contain nested list with multiple empty lists | refused (400): Query has no data source |

</details>

<details><summary>List6 - List size (3)</summary>

| Case | Reason |
| --- | --- |
| [1] Return list size | refused (400): Query has no data source |
| [3] Concatenating and returning the size of literal lists | refused (400): Query has no data source |
| [4] `size()` on null list | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Literals1 - Boolean and Null (6)</summary>

| Case | Reason |
| --- | --- |
| [1] Return a boolean true lower case | refused (400): Query has no data source |
| [2] Return a boolean true upper case | refused (400): Query has no data source |
| [3] Return a boolean false lower case | refused (400): Query has no data source |
| [4] Return a boolean false upper case | refused (400): Query has no data source |
| [5] Return null lower case | refused (400): Query has no data source |
| [6] Return null upper case | refused (400): Query has no data source |

</details>

<details><summary>Literals2 - Decimal integer (8)</summary>

| Case | Reason |
| --- | --- |
| [1] Return a short positive integer | refused (400): Query has no data source |
| [2] Return a long positive integer | refused (400): Query has no data source |
| [3] Return the largest integer | refused (400): Query has no data source |
| [4] Return a positive zero | refused (400): Query has no data source |
| [5] Return a negative zero | refused (400): Query has no data source |
| [6] Return a short negative integer | refused (400): Query has no data source |
| [7] Return a long negative integer | refused (400): Query has no data source |
| [8] Return the smallest integer | refused (400): Query has no data source |

</details>

<details><summary>Literals3 - Hexadecimal integer (11)</summary>

| Case | Reason |
| --- | --- |
| [1] Return a short positive hexadecimal integer | refused (500): Internal server error |
| [2] Return a long positive hexadecimal integer | refused (500): Internal server error |
| [3] Return the largest hexadecimal integer | refused (500): Internal server error |
| [4] Return a positive hexadecimal zero | refused (500): Internal server error |
| [5] Return a negative hexadecimal zero | refused (500): Internal server error |
| [6] Return a short negative hexadecimal integer | refused (500): Internal server error |
| [7] Return a long negative hexadecimal integer | refused (500): Internal server error |
| [8] Return the smallest hexadecimal integer | refused (500): Internal server error |
| [9] Return a lower case hexadecimal integer | refused (500): Internal server error |
| [10] Return a upper case hexadecimal integer | refused (500): Internal server error |
| [11] Return a mixed case hexadecimal integer | refused (500): Internal server error |

</details>

<details><summary>Literals4 - Octal integer (8)</summary>

| Case | Reason |
| --- | --- |
| [1] Return a short positive octal integer | refused (500): Internal server error |
| [2] Return a long positive octal integer | refused (500): Internal server error |
| [3] Return the largest octal integer | refused (500): Internal server error |
| [4] Return a positive octal zero | refused (500): Internal server error |
| [5] Return a negative octal zero | refused (500): Internal server error |
| [6] Return a short negative octal integer | refused (500): Internal server error |
| [7] Return a long negative octal integer | refused (500): Internal server error |
| [8] Return the smallest octal integer | refused (500): Internal server error |

</details>

<details><summary>Literals5 - Float (26)</summary>

| Case | Reason |
| --- | --- |
| [1] Return a short positive float | refused (400): Query has no data source |
| [2] Return a short positive float without integer digits | refused (500): Internal server error |
| [3] Return a long positive float | refused (400): Query has no data source |
| [4] Return a long positive float without integer digits | refused (500): Internal server error |
| [5] Return a very long positive float | refused (400): Query has no data source |
| [6] Return a very long positive float without integer digits | refused (500): Internal server error |
| [7] Return a positive zero float | refused (400): Query has no data source |
| [8] Return a positive zero float without integer digits | refused (500): Internal server error |
| [9] Return a negative zero float | refused (400): Query has no data source |
| [10] Return a negative zero float without integer digits | refused (500): Internal server error |
| [11] Return a very long negative float | refused (400): Query has no data source |
| [12] Return a very long negative float without integer digits | refused (500): Internal server error |
| [13] Return a positive float with positive lower case exponent | refused (500): Internal server error |
| [14] Return a positive float with positive upper case exponent | refused (500): Internal server error |
| [15] Return a positive float with positive lower case exponent without integer digits | refused (500): Internal server error |
| [16] Return a positive float with negative lower case exponent | refused (500): Internal server error |
| [17] Return a positive float with negative lower case exponent without integer digits | refused (500): Internal server error |
| [18] Return a positive float with negative upper case exponent without integer digits | refused (500): Internal server error |
| [19] Return a negative float in with positive lower case exponent | refused (500): Internal server error |
| [20] Return a negative float in with positive upper case exponent | refused (500): Internal server error |
| [21] Return a negative float with positive lower case exponent without integer digits | refused (500): Internal server error |
| [22] Return a negative float with negative lower case exponent | refused (500): Internal server error |
| [23] Return a negative float with negative lower case exponent without integer digits | refused (500): Internal server error |
| [24] Return a negative float with negative upper case exponent without integer digits | refused (500): Internal server error |
| [25] Return a positive float with one integer digit and maximum positive exponent | refused (500): Internal server error |
| [26] Return a positive float with nine integer digit and maximum positive exponent | refused (500): Internal server error |

</details>

<details><summary>Literals6 - String (12)</summary>

| Case | Reason |
| --- | --- |
| [1] Return a single-quoted empty string | refused (400): Query has no data source |
| [2] Return a single-quoted string with one character | refused (400): Query has no data source |
| [3] Return a single-quoted string with uft-8 characters | refused (400): Query has no data source |
| [4] Return a single-quoted string with escaped single-quoted | refused (400): Query has no data source |
| [5] Return a single-quoted string with escaped characters | refused (400): Query has no data source |
| [6] Return a single-quoted string with 100 characters | refused (400): Query has no data source |
| [7] Return a single-quoted string with 1000 characters | refused (400): Query has no data source |
| [8] Return a single-quoted string with 10000 characters | refused (400): Query has no data source |
| [9] Return a double-quoted empty string | refused (400): Query has no data source |
| [10] Accept valid Unicode literal | refused (400): Query has no data source |
| [11] Return a double-quoted string with one character | refused (400): Query has no data source |
| [12] Return a double-quoted string with uft-8 characters | refused (400): Query has no data source |

</details>

<details><summary>Literals7 - List (17)</summary>

| Case | Reason |
| --- | --- |
| [1] Return an empty list | refused (400): Query has no data source |
| [2] Return a list containing a boolean | refused (400): Query has no data source |
| [3] Return a list containing a null | refused (400): Query has no data source |
| [4] Return a list containing a integer | refused (400): Query has no data source |
| [5] Return a list containing a hexadecimal integer | refused (500): Internal server error |
| [6] Return a list containing a octal integer | refused (500): Internal server error |
| [7] Return a list containing a float | refused (500): Internal server error |
| [8] Return a list containing a string | refused (400): Query has no data source |
| [9] Return a list containing an empty lists | refused (400): Query has no data source |
| [10] Return seven-deep nested empty lists | refused (500): Internal server error |
| [11] Return 20-deep nested empty lists | refused (500): Internal server error |
| [12] Return 40-deep nested empty lists | refused (500): Internal server error |
| [13] Return a list containing an empty map | refused (400): Query has no data source |
| [14] Return a list containing multiple integer | refused (500): Internal server error |
| [16] Return a list containing multiple mixed values | refused (500): Internal server error |
| [17] Return a list containing real and fake nested lists | refused (400): Query has no data source |
| [18] Return a complex list containing multiple mixed and nested values | refused (500): Internal server error |

</details>

<details><summary>Literals8 - Maps (18)</summary>

| Case | Reason |
| --- | --- |
| [1] Return an empty map | refused (400): Query has no data source |
| [2] Return a map containing one value with alphabetic lower case key | refused (400): Query has no data source |
| [3] Return a map containing one value with alphabetic upper case key | refused (400): Query has no data source |
| [4] Return a map containing one value with alphabetic mixed case key | refused (400): Query has no data source |
| [5] Return a map containing one value with alphanumeric mixed case key | refused (400): Query has no data source |
| [6] Return a map containing a boolean | refused (400): Query has no data source |
| [7] Return a map containing a null | refused (400): Query has no data source |
| [8] Return a map containing a integer | refused (400): Query has no data source |
| [9] Return a map containing a hexadecimal integer | refused (500): Internal server error |
| [10] Return a map containing a octal integer | refused (500): Internal server error |
| [11] Return a map containing a float | refused (500): Internal server error |
| [12] Return a map containing a string | refused (400): Query has no data source |
| [13] Return a map containing an empty map | refused (400): Query has no data source |
| [14] Return seven-deep nested maps | refused (500): Internal server error |
| [15] Return 20-deep nested maps | refused (500): Internal server error |
| [16] Return 40-deep nested maps | refused (500): Internal server error |
| [17] Return a map containing real and fake nested maps | refused (400): Query has no data source |
| [18] Return a complex map containing multiple mixed and nested values | refused (500): Internal server error |

</details>

<details><summary>Map1 - Static value access (13)</summary>

| Case | Reason |
| --- | --- |
| [1] Statically access a field of a non-null map | refused (400): Pipeline segment has no data source |
| [2] Statically access a field of a null map | refused (400): Pipeline segment has no data source |
| [3] Statically access a field of a map resulting from an expression | refused (400): Pipeline segment has no data source |
| [4] Statically access a field is case-sensitive #1 | refused (400): Pipeline segment has no data source |
| [4] Statically access a field is case-sensitive #2 | refused (400): Pipeline segment has no data source |
| [4] Statically access a field is case-sensitive #3 | refused (400): Pipeline segment has no data source |
| [4] Statically access a field is case-sensitive #4 | refused (400): Pipeline segment has no data source |
| [5] Statically access a field with a delimited identifier #1 | refused (400): Pipeline segment has no data source |
| [5] Statically access a field with a delimited identifier #2 | refused (400): Pipeline segment has no data source |
| [5] Statically access a field with a delimited identifier #3 | refused (400): Pipeline segment has no data source |
| [5] Statically access a field with a delimited identifier #4 | refused (400): Pipeline segment has no data source |
| [5] Statically access a field with a delimited identifier #5 | refused (400): Pipeline segment has no data source |
| [5] Statically access a field with a delimited identifier #6 | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Map2 - Dynamic Value Access (11)</summary>

| Case | Reason |
| --- | --- |
| [1] Dynamically access a field based on parameters when there is no type information | refused (400): Pipeline segment has no data source |
| [2] Dynamically access a field based on parameters when there is rhs type information | refused (400): Pipeline segment has no data source |
| [3] Dynamically access a field on null results in null | refused (400): Pipeline segment has no data source |
| [4] Dynamically access a field with null results in null | refused (400): Pipeline segment has no data source |
| [5] Dynamically access a field is case-sensitive #1 | refused (400): Pipeline segment has no data source |
| [5] Dynamically access a field is case-sensitive #2 | refused (400): Pipeline segment has no data source |
| [5] Dynamically access a field is case-sensitive #3 | refused (400): Pipeline segment has no data source |
| [5] Dynamically access a field is case-sensitive #4 | refused (400): Pipeline segment has no data source |
| [5] Dynamically access a field is case-sensitive #5 | refused (400): Pipeline segment has no data source |
| [5] Dynamically access a field is case-sensitive #6 | refused (400): Pipeline segment has no data source |
| [5] Dynamically access a field is case-sensitive #7 | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Map3 - Keys function (11)</summary>

| Case | Reason |
| --- | --- |
| [1] Using `keys()` on a literal map | refused (400): Query has no data source |
| [2] Using `keys()` on a parameter map | refused (400): Query has no data source |
| [3] Using `keys()` on null map | refused (400): Pipeline segment has no data source |
| [4] Using `keys()` on map with null values #1 | refused (400): Query has no data source |
| [4] Using `keys()` on map with null values #2 | refused (400): Query has no data source |
| [4] Using `keys()` on map with null values #3 | refused (400): Query has no data source |
| [4] Using `keys()` on map with null values #4 | refused (400): Query has no data source |
| [4] Using `keys()` on map with null values #5 | refused (400): Query has no data source |
| [4] Using `keys()` on map with null values #6 | refused (400): Query has no data source |
| [4] Using `keys()` on map with null values #7 | refused (400): Query has no data source |
| [5] Using `keys()` and `IN` to check field existence | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Mathematical11 - Signed numbers functions (1)</summary>

| Case | Reason |
| --- | --- |
| [1] Absolute function | refused (400): Query has no data source |

</details>

<details><summary>Mathematical13 - Square root (1)</summary>

| Case | Reason |
| --- | --- |
| [1] `sqrt()` returning float values | refused (400): Query has no data source |

</details>

<details><summary>Mathematical8 - Arithmetic precedence (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Arithmetic precedence test | refused (400): Query has no data source |
| [2] Arithmetic precedence with parenthesis test | refused (400): Query has no data source |

</details>

<details><summary>Null1 - IS NULL validation (14)</summary>

| Case | Reason |
| --- | --- |
| [3] Property null check on null node | columns [] != ['n.missing IS NULL'] |
| [4] A literal null IS null | refused (400): Query has no data source |
| [5] IS NULL on a map #1 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #2 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #3 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #4 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #5 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #6 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #7 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #8 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #9 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #10 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #11 | refused (400): Pipeline segment has no data source |
| [5] IS NULL on a map #12 | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Null2 - IS NOT NULL validation (14)</summary>

| Case | Reason |
| --- | --- |
| [3] Property not null check on null node | columns [] != ['n.missing IS NOT NULL'] |
| [4] A literal null is not IS NOT null | refused (400): Query has no data source |
| [5] IS NOT NULL on a map #1 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #2 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #3 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #4 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #5 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #6 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #7 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #8 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #9 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #10 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #11 | refused (400): Pipeline segment has no data source |
| [5] IS NOT NULL on a map #12 | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Null3 - Null evaluation (10)</summary>

| Case | Reason |
| --- | --- |
| [1] The inverse of a null is a null | refused (400): Query has no data source |
| [2] It is unknown - i.e. null - if a null is equal to a null | refused (400): Query has no data source |
| [3] It is unknown - i.e. null - if a null is not equal to a null | refused (400): Query has no data source |
| [4] Using null in IN #1 | refused (400): Query has no data source |
| [4] Using null in IN #2 | refused (400): Query has no data source |
| [4] Using null in IN #3 | refused (400): Query has no data source |
| [4] Using null in IN #4 | refused (400): Query has no data source |
| [4] Using null in IN #5 | refused (400): Query has no data source |
| [4] Using null in IN #6 | refused (400): Query has no data source |
| [4] Using null in IN #7 | refused (400): Query has no data source |

</details>

<details><summary>Path1 - Nodes of a path (1)</summary>

| Case | Reason |
| --- | --- |
| [1] `nodes()` on null path | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Path2 - Relationships of a path (1)</summary>

| Case | Reason |
| --- | --- |
| [3] `relationships()` on null path | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Path3 - Length of a path (2)</summary>

| Case | Reason |
| --- | --- |
| [2] Failing when using `length()` on a node | expected SyntaxError; the query ran |
| [3] Failing when using `length()` on a relationship | expected SyntaxError; the query ran |

</details>

<details><summary>Precedence1 - On boolean values (21)</summary>

| Case | Reason |
| --- | --- |
| [1] Exclusive disjunction takes precedence over inclusive disjunction | refused (400): Query has no data source |
| [2] Conjunction disjunction takes precedence over exclusive disjunction | refused (400): Query has no data source |
| [3] Conjunction disjunction takes precedence over inclusive disjunction | refused (400): Query has no data source |
| [4] Negation takes precedence over conjunction | refused (400): Query has no data source |
| [5] Negation takes precedence over inclusive disjunction | refused (400): Query has no data source |
| [6] Comparison operator takes precedence over boolean negation | refused (400): Query has no data source |
| [7] Comparison operator takes precedence over binary boolean operator | refused (400): Query has no data source |
| [8] Null predicate takes precedence over comparison operator | refused (400): Query has no data source |
| [9] Null predicate takes precedence over negation | refused (400): Query has no data source |
| [10] Null predicate takes precedence over boolean operator | refused (400): Query has no data source |
| [11] List predicate takes precedence over comparison operator | refused (400): Query has no data source |
| [12] List predicate takes precedence over negation | refused (400): Query has no data source |
| [13] List predicate takes precedence over boolean operator | refused (400): Query has no data source |
| [14] Exclusive disjunction takes precedence over inclusive disjunction in every combination of truth values | refused (403): SQL parse error: Expecting ). Line 1, Col: 55.
  WITH _w0 AS (SELECT ARRAY_AGG((_uw0."a" OR _uw1."b" [4mXOR[0m _uw2."c") = (_uw0."a" OR ((_uw1."b" AND (NOT _uw2."c")) OR ((NOT _uw1."b") AND _uw2."c" |
| [15] Conjunction takes precedence over exclusive disjunction in every combination of truth values | rows differ: got [[False]] |
| [23] Null predicates take precedence over comparison operators in every combination of truth values #1 | rows differ: got [[False]] |
| [23] Null predicates take precedence over comparison operators in every combination of truth values #3 | rows differ: got [[False]] |
| [23] Null predicates take precedence over comparison operators in every combination of truth values #5 | rows differ: got [[False]] |
| [23] Null predicates take precedence over comparison operators in every combination of truth values #7 | rows differ: got [[False]] |
| [23] Null predicates take precedence over comparison operators in every combination of truth values #9 | rows differ: got [[False]] |
| [23] Null predicates take precedence over comparison operators in every combination of truth values #11 | rows differ: got [[False]] |

</details>

<details><summary>Precedence2 - On numeric values (26)</summary>

| Case | Reason |
| --- | --- |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #1 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #2 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #3 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #4 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #5 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #6 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #7 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #8 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #9 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #10 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #11 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #12 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #13 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #14 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #15 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #16 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #17 | refused (400): Query has no data source |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #18 | refused (400): Query has no data source |
| [2] Exponentiation takes precedence over numeric multiplicative operations #1 | refused (500): Internal server error |
| [2] Exponentiation takes precedence over numeric multiplicative operations #2 | refused (500): Internal server error |
| [2] Exponentiation takes precedence over numeric multiplicative operations #3 | refused (500): Internal server error |
| [3] Exponentiation takes precedence over numeric additive operations #1 | refused (500): Internal server error |
| [3] Exponentiation takes precedence over numeric additive operations #2 | refused (500): Internal server error |
| [4] Numeric unary negative takes precedence over exponentiation | refused (500): Internal server error |
| [5] Numeric unary negative takes precedence over numeric additive operations #1 | refused (400): Query has no data source |
| [5] Numeric unary negative takes precedence over numeric additive operations #2 | refused (400): Query has no data source |

</details>

<details><summary>Precedence3 - On list values (11)</summary>

| Case | Reason |
| --- | --- |
| [1] List element access takes precedence over list appending | refused (400): Query has no data source |
| [2] List element access takes precedence over list concatenation | refused (400): Query has no data source |
| [3] List slicing takes precedence over list concatenation | refused (400): Query has no data source |
| [4] List appending takes precedence over list element containment | refused (400): Query has no data source |
| [5] List concatenation takes precedence over list element containment | refused (400): Query has no data source |
| [6] List element containment takes precedence over comparison operator #1 | refused (400): Query has no data source |
| [6] List element containment takes precedence over comparison operator #2 | refused (400): Query has no data source |
| [6] List element containment takes precedence over comparison operator #3 | refused (400): Query has no data source |
| [6] List element containment takes precedence over comparison operator #4 | refused (400): Query has no data source |
| [6] List element containment takes precedence over comparison operator #5 | refused (400): Query has no data source |
| [6] List element containment takes precedence over comparison operator #6 | refused (400): Query has no data source |

</details>

<details><summary>Precedence4 - On null value (12)</summary>

| Case | Reason |
| --- | --- |
| [1] Null predicate takes precedence over comparison operator #1 | refused (400): Query has no data source |
| [1] Null predicate takes precedence over comparison operator #2 | refused (400): Query has no data source |
| [1] Null predicate takes precedence over comparison operator #3 | refused (400): Query has no data source |
| [2] Null predicate takes precedence over boolean negation | refused (400): Query has no data source |
| [3] Null predicate takes precedence over binary boolean operator #1 | refused (400): Query has no data source |
| [3] Null predicate takes precedence over binary boolean operator #2 | refused (400): Query has no data source |
| [3] Null predicate takes precedence over binary boolean operator #3 | refused (400): Query has no data source |
| [3] Null predicate takes precedence over binary boolean operator #4 | refused (400): Query has no data source |
| [3] Null predicate takes precedence over binary boolean operator #5 | refused (400): Query has no data source |
| [3] Null predicate takes precedence over binary boolean operator #6 | refused (400): Query has no data source |
| [3] Null predicate takes precedence over binary boolean operator #7 | refused (400): Query has no data source |
| [4] String predicate takes precedence over binary boolean operator | refused (400): Query has no data source |

</details>

<details><summary>Quantifier1 - None quantifier (100)</summary>

| Case | Reason |
| --- | --- |
| [1] None quantifier is always true on empty list | refused (400): Query has no data source |
| [2] None quantifier on list literal containing booleans #1 | refused (400): Query has no data source |
| [2] None quantifier on list literal containing booleans #2 | refused (400): Query has no data source |
| [2] None quantifier on list literal containing booleans #3 | refused (400): Query has no data source |
| [2] None quantifier on list literal containing booleans #4 | refused (400): Query has no data source |
| [2] None quantifier on list literal containing booleans #5 | refused (400): Query has no data source |
| [2] None quantifier on list literal containing booleans #6 | refused (400): Query has no data source |
| [2] None quantifier on list literal containing booleans #7 | refused (400): Query has no data source |
| [2] None quantifier on list literal containing booleans #8 | refused (400): Query has no data source |
| [2] None quantifier on list literal containing booleans #9 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #1 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #2 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #3 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #4 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #5 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #6 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #7 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #8 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #9 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #10 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #11 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #12 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #13 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #14 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #15 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #16 | refused (400): Query has no data source |
| [3] None quantifier on list literal containing integers #17 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #1 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #2 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #3 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #4 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #5 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #6 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #7 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #8 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #9 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #10 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #11 | refused (400): Query has no data source |
| [4] None quantifier on list literal containing floats #12 | refused (400): Query has no data source |
| [5] None quantifier on list literal containing strings #1 | refused (400): Query has no data source |
| [5] None quantifier on list literal containing strings #2 | refused (400): Query has no data source |
| [5] None quantifier on list literal containing strings #3 | refused (400): Query has no data source |
| [5] None quantifier on list literal containing strings #4 | refused (400): Query has no data source |
| [5] None quantifier on list literal containing strings #5 | refused (400): Query has no data source |
| [5] None quantifier on list literal containing strings #6 | refused (400): Query has no data source |
| [5] None quantifier on list literal containing strings #7 | refused (400): Query has no data source |
| [5] None quantifier on list literal containing strings #8 | refused (400): Query has no data source |
| [5] None quantifier on list literal containing strings #9 | refused (400): Query has no data source |
| [6] None quantifier on list literal containing lists #1 | refused (400): Query has no data source |
| [6] None quantifier on list literal containing lists #2 | refused (400): Query has no data source |
| [6] None quantifier on list literal containing lists #3 | refused (400): Query has no data source |
| [6] None quantifier on list literal containing lists #4 | refused (400): Query has no data source |
| [6] None quantifier on list literal containing lists #5 | refused (400): Query has no data source |
| [6] None quantifier on list literal containing lists #6 | refused (400): Query has no data source |
| [6] None quantifier on list literal containing lists #7 | refused (400): Query has no data source |
| [6] None quantifier on list literal containing lists #8 | refused (400): Query has no data source |
| [6] None quantifier on list literal containing lists #9 | refused (400): Query has no data source |
| [7] None quantifier on list literal containing maps #1 | refused (400): Query has no data source |
| [7] None quantifier on list literal containing maps #2 | refused (400): Query has no data source |
| [7] None quantifier on list literal containing maps #3 | refused (400): Query has no data source |
| [7] None quantifier on list literal containing maps #4 | refused (400): Query has no data source |
| [7] None quantifier on list literal containing maps #5 | refused (400): Query has no data source |
| [7] None quantifier on list literal containing maps #6 | refused (400): Query has no data source |
| [7] None quantifier on list literal containing maps #7 | refused (400): Query has no data source |
| [7] None quantifier on list literal containing maps #8 | refused (400): Query has no data source |
| [7] None quantifier on list literal containing maps #9 | refused (400): Query has no data source |
| [10] None quantifier on lists containing nulls #1 | refused (400): Query has no data source |
| [10] None quantifier on lists containing nulls #2 | refused (400): Query has no data source |
| [10] None quantifier on lists containing nulls #3 | refused (400): Query has no data source |
| [10] None quantifier on lists containing nulls #4 | refused (400): Query has no data source |
| [10] None quantifier on lists containing nulls #5 | refused (400): Query has no data source |
| [10] None quantifier on lists containing nulls #6 | refused (400): Query has no data source |
| [10] None quantifier on lists containing nulls #7 | refused (400): Query has no data source |
| [10] None quantifier on lists containing nulls #8 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #1 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #2 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #3 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #4 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #5 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #6 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #7 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #8 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #9 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #10 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #11 | refused (400): Query has no data source |
| [11] None quantifier with IS NULL predicate #12 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #1 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #2 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #3 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #4 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #5 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #6 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #7 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #8 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #9 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #10 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #11 | refused (400): Query has no data source |
| [12] None quantifier with IS NOT NULL predicate #12 | refused (400): Query has no data source |
| [13] None quantifier is true if the predicate is statically false and the list is not empty | refused (400): Query has no data source |
| [14] None quantifier is false if the predicate is statically true and the list is not empty | refused (400): Query has no data source |

</details>

<details><summary>Quantifier10 - Single quantifier invariants (8)</summary>

| Case | Reason |
| --- | --- |
| [1] Single quantifier is always false if the predicate is statically false and the list is not empty | refused (500): Internal server error |
| [2] Single quantifier is always false if the predicate is statically true and the list has more than one element | refused (500): Internal server error |
| [3] Single quantifier is always true if the predicate is statically true and the list has exactly one non-null element | refused (500): Internal server error |
| [4] Single quantifier is always equal whether the size of the list filtered with same the predicate is one #1 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [4] Single quantifier is always equal whether the size of the list filtered with same the predicate is one #2 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [4] Single quantifier is always equal whether the size of the list filtered with same the predicate is one #3 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [4] Single quantifier is always equal whether the size of the list filtered with same the predicate is one #4 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [4] Single quantifier is always equal whether the size of the list filtered with same the predicate is one #5 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |

</details>

<details><summary>Quantifier11 - Any quantifier invariants (22)</summary>

| Case | Reason |
| --- | --- |
| [1] Any quantifier is always false if the predicate is statically false and the list is not empty | refused (500): Internal server error |
| [2] Any quantifier is always true if the predicate is statically true and the list is not empty | refused (500): Internal server error |
| [3] Any quantifier is always true if the single or the all quantifier is true #1 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [3] Any quantifier is always true if the single or the all quantifier is true #2 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [3] Any quantifier is always true if the single or the all quantifier is true #3 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [3] Any quantifier is always true if the single or the all quantifier is true #4 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [3] Any quantifier is always true if the single or the all quantifier is true #5 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [4] Any quantifier is always equal the boolean negative of the none quantifier #1 | refused (400): Pipeline segment has no data source |
| [4] Any quantifier is always equal the boolean negative of the none quantifier #2 | refused (400): Pipeline segment has no data source |
| [4] Any quantifier is always equal the boolean negative of the none quantifier #3 | refused (400): Pipeline segment has no data source |
| [4] Any quantifier is always equal the boolean negative of the none quantifier #4 | refused (400): Pipeline segment has no data source |
| [4] Any quantifier is always equal the boolean negative of the none quantifier #5 | refused (400): Pipeline segment has no data source |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #1 | refused (400): Pipeline segment has no data source |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #2 | refused (400): Pipeline segment has no data source |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #3 | refused (400): Pipeline segment has no data source |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #4 | refused (400): Pipeline segment has no data source |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #5 | refused (400): Pipeline segment has no data source |
| [6] Any quantifier is always equal whether the size of the list filtered with same the predicate is grater zero #1 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [6] Any quantifier is always equal whether the size of the list filtered with same the predicate is grater zero #2 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [6] Any quantifier is always equal whether the size of the list filtered with same the predicate is grater zero #3 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [6] Any quantifier is always equal whether the size of the list filtered with same the predicate is grater zero #4 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [6] Any quantifier is always equal whether the size of the list filtered with same the predicate is grater zero #5 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |

</details>

<details><summary>Quantifier12 - All quantifier invariants (17)</summary>

| Case | Reason |
| --- | --- |
| [1] All quantifier is always false if the predicate is statically false and the list is not empty | refused (500): Internal server error |
| [2] All quantifier is always true if the predicate is statically true and the list is not empty | refused (500): Internal server error |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #1 | refused (400): Pipeline segment has no data source |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #2 | refused (400): Pipeline segment has no data source |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #3 | refused (400): Pipeline segment has no data source |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #4 | refused (400): Pipeline segment has no data source |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #5 | refused (400): Pipeline segment has no data source |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #1 | refused (400): Pipeline segment has no data source |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #2 | refused (400): Pipeline segment has no data source |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #3 | refused (400): Pipeline segment has no data source |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #4 | refused (400): Pipeline segment has no data source |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #5 | refused (400): Pipeline segment has no data source |
| [5] All quantifier is always equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #1 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [5] All quantifier is always equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #2 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [5] All quantifier is always equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #3 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [5] All quantifier is always equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #4 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [5] All quantifier is always equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #5 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |

</details>

<details><summary>Quantifier2 - Single quantifier (101)</summary>

| Case | Reason |
| --- | --- |
| [1] Single quantifier is always false on empty list | refused (400): Query has no data source |
| [2] Single quantifier on list literal #1 | refused (400): Query has no data source |
| [2] Single quantifier on list literal #2 | refused (400): Query has no data source |
| [2] Single quantifier on list literal #3 | refused (400): Query has no data source |
| [2] Single quantifier on list literal #4 | refused (400): Query has no data source |
| [2] Single quantifier on list literal #5 | refused (400): Query has no data source |
| [2] Single quantifier on list literal #6 | refused (400): Query has no data source |
| [2] Single quantifier on list literal #7 | refused (400): Query has no data source |
| [2] Single quantifier on list literal #8 | refused (400): Query has no data source |
| [2] Single quantifier on list literal #9 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #1 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #2 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #3 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #4 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #5 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #6 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #7 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #8 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #9 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #10 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #11 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #12 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #13 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #14 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #15 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #16 | refused (400): Query has no data source |
| [3] Single quantifier on list literal containing integers #17 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #1 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #2 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #3 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #4 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #5 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #6 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #7 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #8 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #9 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #10 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #11 | refused (400): Query has no data source |
| [4] Single quantifier on list literal containing floats #12 | refused (400): Query has no data source |
| [5] Single quantifier on list literal containing strings #1 | refused (400): Query has no data source |
| [5] Single quantifier on list literal containing strings #2 | refused (400): Query has no data source |
| [5] Single quantifier on list literal containing strings #3 | refused (400): Query has no data source |
| [5] Single quantifier on list literal containing strings #4 | refused (400): Query has no data source |
| [5] Single quantifier on list literal containing strings #5 | refused (400): Query has no data source |
| [5] Single quantifier on list literal containing strings #6 | refused (400): Query has no data source |
| [5] Single quantifier on list literal containing strings #7 | refused (400): Query has no data source |
| [5] Single quantifier on list literal containing strings #8 | refused (400): Query has no data source |
| [5] Single quantifier on list literal containing strings #9 | refused (400): Query has no data source |
| [6] Single quantifier on list literal containing lists #1 | refused (400): Query has no data source |
| [6] Single quantifier on list literal containing lists #2 | refused (400): Query has no data source |
| [6] Single quantifier on list literal containing lists #3 | refused (400): Query has no data source |
| [6] Single quantifier on list literal containing lists #4 | refused (400): Query has no data source |
| [6] Single quantifier on list literal containing lists #5 | refused (400): Query has no data source |
| [6] Single quantifier on list literal containing lists #6 | refused (400): Query has no data source |
| [6] Single quantifier on list literal containing lists #7 | refused (400): Query has no data source |
| [6] Single quantifier on list literal containing lists #8 | refused (400): Query has no data source |
| [6] Single quantifier on list literal containing lists #9 | refused (400): Query has no data source |
| [7] Single quantifier on list literal containing maps #1 | refused (400): Query has no data source |
| [7] Single quantifier on list literal containing maps #2 | refused (400): Query has no data source |
| [7] Single quantifier on list literal containing maps #3 | refused (400): Query has no data source |
| [7] Single quantifier on list literal containing maps #4 | refused (400): Query has no data source |
| [7] Single quantifier on list literal containing maps #5 | refused (400): Query has no data source |
| [7] Single quantifier on list literal containing maps #6 | refused (400): Query has no data source |
| [7] Single quantifier on list literal containing maps #7 | refused (400): Query has no data source |
| [7] Single quantifier on list literal containing maps #8 | refused (400): Query has no data source |
| [7] Single quantifier on list literal containing maps #9 | refused (400): Query has no data source |
| [10] Single quantifier on lists containing nulls #1 | refused (400): Query has no data source |
| [10] Single quantifier on lists containing nulls #2 | refused (400): Query has no data source |
| [10] Single quantifier on lists containing nulls #3 | refused (400): Query has no data source |
| [10] Single quantifier on lists containing nulls #4 | refused (400): Query has no data source |
| [10] Single quantifier on lists containing nulls #5 | refused (400): Query has no data source |
| [10] Single quantifier on lists containing nulls #6 | refused (400): Query has no data source |
| [10] Single quantifier on lists containing nulls #7 | refused (400): Query has no data source |
| [10] Single quantifier on lists containing nulls #8 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #1 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #2 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #3 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #4 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #5 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #6 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #7 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #8 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #9 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #10 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #11 | refused (400): Query has no data source |
| [11] Single quantifier with IS NULL predicate #12 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #1 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #2 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #3 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #4 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #5 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #6 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #7 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #8 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #9 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #10 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #11 | refused (400): Query has no data source |
| [12] Single quantifier with IS NOT NULL predicate #12 | refused (400): Query has no data source |
| [13] Single quantifier is false if the predicate is statically false and the list is not empty | refused (400): Query has no data source |
| [14] Single quantifier is false if the predicate is statically true and the list has more than one element | refused (400): Query has no data source |
| [15] Single quantifier is true if the predicate is statically true and the list has exactly one non-null element | refused (400): Query has no data source |

</details>

<details><summary>Quantifier3 - Any quantifier (100)</summary>

| Case | Reason |
| --- | --- |
| [1] Any quantifier is always false on empty list | refused (400): Query has no data source |
| [2] Any quantifier on list literal #1 | refused (400): Query has no data source |
| [2] Any quantifier on list literal #2 | refused (400): Query has no data source |
| [2] Any quantifier on list literal #3 | refused (400): Query has no data source |
| [2] Any quantifier on list literal #4 | refused (400): Query has no data source |
| [2] Any quantifier on list literal #5 | refused (400): Query has no data source |
| [2] Any quantifier on list literal #6 | refused (400): Query has no data source |
| [2] Any quantifier on list literal #7 | refused (400): Query has no data source |
| [2] Any quantifier on list literal #8 | refused (400): Query has no data source |
| [2] Any quantifier on list literal #9 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #1 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #2 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #3 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #4 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #5 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #6 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #7 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #8 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #9 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #10 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #11 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #12 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #13 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #14 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #15 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #16 | refused (400): Query has no data source |
| [3] Any quantifier on list literal containing integers #17 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #1 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #2 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #3 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #4 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #5 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #6 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #7 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #8 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #9 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #10 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #11 | refused (400): Query has no data source |
| [4] Any quantifier on list literal containing floats #12 | refused (400): Query has no data source |
| [5] Any quantifier on list literal containing strings #1 | refused (400): Query has no data source |
| [5] Any quantifier on list literal containing strings #2 | refused (400): Query has no data source |
| [5] Any quantifier on list literal containing strings #3 | refused (400): Query has no data source |
| [5] Any quantifier on list literal containing strings #4 | refused (400): Query has no data source |
| [5] Any quantifier on list literal containing strings #5 | refused (400): Query has no data source |
| [5] Any quantifier on list literal containing strings #6 | refused (400): Query has no data source |
| [5] Any quantifier on list literal containing strings #7 | refused (400): Query has no data source |
| [5] Any quantifier on list literal containing strings #8 | refused (400): Query has no data source |
| [5] Any quantifier on list literal containing strings #9 | refused (400): Query has no data source |
| [6] Any quantifier on list literal containing lists #1 | refused (400): Query has no data source |
| [6] Any quantifier on list literal containing lists #2 | refused (400): Query has no data source |
| [6] Any quantifier on list literal containing lists #3 | refused (400): Query has no data source |
| [6] Any quantifier on list literal containing lists #4 | refused (400): Query has no data source |
| [6] Any quantifier on list literal containing lists #5 | refused (400): Query has no data source |
| [6] Any quantifier on list literal containing lists #6 | refused (400): Query has no data source |
| [6] Any quantifier on list literal containing lists #7 | refused (400): Query has no data source |
| [6] Any quantifier on list literal containing lists #8 | refused (400): Query has no data source |
| [6] Any quantifier on list literal containing lists #9 | refused (400): Query has no data source |
| [7] Any quantifier on list literal containing maps #1 | refused (400): Query has no data source |
| [7] Any quantifier on list literal containing maps #2 | refused (400): Query has no data source |
| [7] Any quantifier on list literal containing maps #3 | refused (400): Query has no data source |
| [7] Any quantifier on list literal containing maps #4 | refused (400): Query has no data source |
| [7] Any quantifier on list literal containing maps #5 | refused (400): Query has no data source |
| [7] Any quantifier on list literal containing maps #6 | refused (400): Query has no data source |
| [7] Any quantifier on list literal containing maps #7 | refused (400): Query has no data source |
| [7] Any quantifier on list literal containing maps #8 | refused (400): Query has no data source |
| [7] Any quantifier on list literal containing maps #9 | refused (400): Query has no data source |
| [10] Any quantifier on lists containing nulls #1 | refused (400): Query has no data source |
| [10] Any quantifier on lists containing nulls #2 | refused (400): Query has no data source |
| [10] Any quantifier on lists containing nulls #3 | refused (400): Query has no data source |
| [10] Any quantifier on lists containing nulls #4 | refused (400): Query has no data source |
| [10] Any quantifier on lists containing nulls #5 | refused (400): Query has no data source |
| [10] Any quantifier on lists containing nulls #6 | refused (400): Query has no data source |
| [10] Any quantifier on lists containing nulls #7 | refused (400): Query has no data source |
| [10] Any quantifier on lists containing nulls #8 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #1 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #2 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #3 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #4 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #5 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #6 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #7 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #8 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #9 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #10 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #11 | refused (400): Query has no data source |
| [11] Any quantifier with IS NULL predicate #12 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #1 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #2 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #3 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #4 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #5 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #6 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #7 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #8 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #9 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #10 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #11 | refused (400): Query has no data source |
| [12] Any quantifier with IS NOT NULL predicate #12 | refused (400): Query has no data source |
| [13] Any quantifier is false if the predicate is statically false and the list is not empty | refused (400): Query has no data source |
| [14] Any quantifier is true if the predicate is statically true and the list is not empty | refused (400): Query has no data source |

</details>

<details><summary>Quantifier4 - All quantifier (100)</summary>

| Case | Reason |
| --- | --- |
| [1] All quantifier is always true on empty list | refused (400): Query has no data source |
| [2] All quantifier on list literal #1 | refused (400): Query has no data source |
| [2] All quantifier on list literal #2 | refused (400): Query has no data source |
| [2] All quantifier on list literal #3 | refused (400): Query has no data source |
| [2] All quantifier on list literal #4 | refused (400): Query has no data source |
| [2] All quantifier on list literal #5 | refused (400): Query has no data source |
| [2] All quantifier on list literal #6 | refused (400): Query has no data source |
| [2] All quantifier on list literal #7 | refused (400): Query has no data source |
| [2] All quantifier on list literal #8 | refused (400): Query has no data source |
| [2] All quantifier on list literal #9 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #1 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #2 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #3 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #4 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #5 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #6 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #7 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #8 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #9 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #10 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #11 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #12 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #13 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #14 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #15 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #16 | refused (400): Query has no data source |
| [3] All quantifier on list literal containing integers #17 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #1 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #2 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #3 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #4 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #5 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #6 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #7 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #8 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #9 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #10 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #11 | refused (400): Query has no data source |
| [4] All quantifier on list literal containing floats #12 | refused (400): Query has no data source |
| [5] All quantifier on list literal containing strings #1 | refused (400): Query has no data source |
| [5] All quantifier on list literal containing strings #2 | refused (400): Query has no data source |
| [5] All quantifier on list literal containing strings #3 | refused (400): Query has no data source |
| [5] All quantifier on list literal containing strings #4 | refused (400): Query has no data source |
| [5] All quantifier on list literal containing strings #5 | refused (400): Query has no data source |
| [5] All quantifier on list literal containing strings #6 | refused (400): Query has no data source |
| [5] All quantifier on list literal containing strings #7 | refused (400): Query has no data source |
| [5] All quantifier on list literal containing strings #8 | refused (400): Query has no data source |
| [5] All quantifier on list literal containing strings #9 | refused (400): Query has no data source |
| [6] All quantifier on list literal containing lists #1 | refused (400): Query has no data source |
| [6] All quantifier on list literal containing lists #2 | refused (400): Query has no data source |
| [6] All quantifier on list literal containing lists #3 | refused (400): Query has no data source |
| [6] All quantifier on list literal containing lists #4 | refused (400): Query has no data source |
| [6] All quantifier on list literal containing lists #5 | refused (400): Query has no data source |
| [6] All quantifier on list literal containing lists #6 | refused (400): Query has no data source |
| [6] All quantifier on list literal containing lists #7 | refused (400): Query has no data source |
| [6] All quantifier on list literal containing lists #8 | refused (400): Query has no data source |
| [6] All quantifier on list literal containing lists #9 | refused (400): Query has no data source |
| [7] All quantifier on list literal containing maps #1 | refused (400): Query has no data source |
| [7] All quantifier on list literal containing maps #2 | refused (400): Query has no data source |
| [7] All quantifier on list literal containing maps #3 | refused (400): Query has no data source |
| [7] All quantifier on list literal containing maps #4 | refused (400): Query has no data source |
| [7] All quantifier on list literal containing maps #5 | refused (400): Query has no data source |
| [7] All quantifier on list literal containing maps #6 | refused (400): Query has no data source |
| [7] All quantifier on list literal containing maps #7 | refused (400): Query has no data source |
| [7] All quantifier on list literal containing maps #8 | refused (400): Query has no data source |
| [7] All quantifier on list literal containing maps #9 | refused (400): Query has no data source |
| [10] All quantifier on lists containing nulls #1 | refused (400): Query has no data source |
| [10] All quantifier on lists containing nulls #2 | refused (400): Query has no data source |
| [10] All quantifier on lists containing nulls #3 | refused (400): Query has no data source |
| [10] All quantifier on lists containing nulls #4 | refused (400): Query has no data source |
| [10] All quantifier on lists containing nulls #5 | refused (400): Query has no data source |
| [10] All quantifier on lists containing nulls #6 | refused (400): Query has no data source |
| [10] All quantifier on lists containing nulls #7 | refused (400): Query has no data source |
| [10] All quantifier on lists containing nulls #8 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #1 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #2 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #3 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #4 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #5 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #6 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #7 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #8 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #9 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #10 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #11 | refused (400): Query has no data source |
| [11] All quantifier with IS NULL predicate #12 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #1 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #2 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #3 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #4 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #5 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #6 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #7 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #8 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #9 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #10 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #11 | refused (400): Query has no data source |
| [12] All quantifier with IS NOT NULL predicate #12 | refused (400): Query has no data source |
| [13] All quantifier is false if the predicate is statically false and the list is not empty | refused (400): Query has no data source |
| [14] All quantifier is true if the predicate is statically true and the list is not empty | refused (400): Query has no data source |

</details>

<details><summary>Quantifier5 - None quantifier interop (31)</summary>

| Case | Reason |
| --- | --- |
| [1] None quantifier can nest itself and other quantifiers on nested lists #1 | refused (400): Query has no data source |
| [1] None quantifier can nest itself and other quantifiers on nested lists #2 | refused (400): Query has no data source |
| [1] None quantifier can nest itself and other quantifiers on nested lists #3 | refused (400): Query has no data source |
| [1] None quantifier can nest itself and other quantifiers on nested lists #4 | refused (400): Query has no data source |
| [1] None quantifier can nest itself and other quantifiers on nested lists #5 | refused (400): Query has no data source |
| [1] None quantifier can nest itself and other quantifiers on nested lists #6 | refused (400): Query has no data source |
| [1] None quantifier can nest itself and other quantifiers on nested lists #7 | refused (400): Query has no data source |
| [1] None quantifier can nest itself and other quantifiers on nested lists #8 | refused (400): Query has no data source |
| [2] None quantifier can nest itself and other quantifiers on the same list #1 | refused (400): Pipeline segment has no data source |
| [2] None quantifier can nest itself and other quantifiers on the same list #2 | refused (400): Pipeline segment has no data source |
| [2] None quantifier can nest itself and other quantifiers on the same list #3 | refused (400): Pipeline segment has no data source |
| [2] None quantifier can nest itself and other quantifiers on the same list #4 | refused (400): Pipeline segment has no data source |
| [2] None quantifier can nest itself and other quantifiers on the same list #5 | refused (400): Pipeline segment has no data source |
| [2] None quantifier can nest itself and other quantifiers on the same list #6 | refused (400): Pipeline segment has no data source |
| [2] None quantifier can nest itself and other quantifiers on the same list #7 | refused (400): Pipeline segment has no data source |
| [2] None quantifier can nest itself and other quantifiers on the same list #8 | refused (400): Pipeline segment has no data source |
| [3] None quantifier is equal the boolean negative of the any quantifier #1 | refused (400): Query has no data source |
| [3] None quantifier is equal the boolean negative of the any quantifier #2 | refused (400): Query has no data source |
| [3] None quantifier is equal the boolean negative of the any quantifier #3 | refused (400): Query has no data source |
| [3] None quantifier is equal the boolean negative of the any quantifier #4 | refused (400): Query has no data source |
| [3] None quantifier is equal the boolean negative of the any quantifier #5 | refused (400): Query has no data source |
| [4] None quantifier is equal the all quantifier on the boolean negative of the predicate #1 | refused (400): Query has no data source |
| [4] None quantifier is equal the all quantifier on the boolean negative of the predicate #2 | refused (400): Query has no data source |
| [4] None quantifier is equal the all quantifier on the boolean negative of the predicate #3 | refused (400): Query has no data source |
| [4] None quantifier is equal the all quantifier on the boolean negative of the predicate #4 | refused (400): Query has no data source |
| [4] None quantifier is equal the all quantifier on the boolean negative of the predicate #5 | refused (400): Query has no data source |
| [5] None quantifier is equal whether the size of the list filtered with same the predicate is zero #1 | refused (400): Query has no data source |
| [5] None quantifier is equal whether the size of the list filtered with same the predicate is zero #2 | refused (400): Query has no data source |
| [5] None quantifier is equal whether the size of the list filtered with same the predicate is zero #3 | refused (400): Query has no data source |
| [5] None quantifier is equal whether the size of the list filtered with same the predicate is zero #4 | refused (400): Query has no data source |
| [5] None quantifier is equal whether the size of the list filtered with same the predicate is zero #5 | refused (400): Query has no data source |

</details>

<details><summary>Quantifier6 - Single quantifier interop (21)</summary>

| Case | Reason |
| --- | --- |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #1 | refused (400): Query has no data source |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #2 | refused (400): Query has no data source |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #3 | refused (400): Query has no data source |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #4 | refused (400): Query has no data source |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #5 | refused (400): Query has no data source |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #6 | refused (400): Query has no data source |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #7 | refused (400): Query has no data source |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #8 | refused (400): Query has no data source |
| [2] Single quantifier can nest itself and other quantifiers on the same list #1 | refused (400): Pipeline segment has no data source |
| [2] Single quantifier can nest itself and other quantifiers on the same list #2 | refused (400): Pipeline segment has no data source |
| [2] Single quantifier can nest itself and other quantifiers on the same list #3 | refused (400): Pipeline segment has no data source |
| [2] Single quantifier can nest itself and other quantifiers on the same list #4 | refused (400): Pipeline segment has no data source |
| [2] Single quantifier can nest itself and other quantifiers on the same list #5 | refused (400): Pipeline segment has no data source |
| [2] Single quantifier can nest itself and other quantifiers on the same list #6 | refused (400): Pipeline segment has no data source |
| [2] Single quantifier can nest itself and other quantifiers on the same list #7 | refused (400): Pipeline segment has no data source |
| [2] Single quantifier can nest itself and other quantifiers on the same list #8 | refused (400): Pipeline segment has no data source |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #1 | refused (400): Query has no data source |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #2 | refused (400): Query has no data source |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #3 | refused (400): Query has no data source |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #4 | refused (400): Query has no data source |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #5 | refused (400): Query has no data source |

</details>

<details><summary>Quantifier7 - Any quantifier interop (36)</summary>

| Case | Reason |
| --- | --- |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #1 | refused (400): Query has no data source |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #2 | refused (400): Query has no data source |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #3 | refused (400): Query has no data source |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #4 | refused (400): Query has no data source |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #5 | refused (400): Query has no data source |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #6 | refused (400): Query has no data source |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #7 | refused (400): Query has no data source |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #8 | refused (400): Query has no data source |
| [2] Any quantifier can nest itself and other quantifiers on the same list #1 | refused (400): Pipeline segment has no data source |
| [2] Any quantifier can nest itself and other quantifiers on the same list #2 | refused (400): Pipeline segment has no data source |
| [2] Any quantifier can nest itself and other quantifiers on the same list #3 | refused (400): Pipeline segment has no data source |
| [2] Any quantifier can nest itself and other quantifiers on the same list #4 | refused (400): Pipeline segment has no data source |
| [2] Any quantifier can nest itself and other quantifiers on the same list #5 | refused (400): Pipeline segment has no data source |
| [2] Any quantifier can nest itself and other quantifiers on the same list #6 | refused (400): Pipeline segment has no data source |
| [2] Any quantifier can nest itself and other quantifiers on the same list #7 | refused (400): Pipeline segment has no data source |
| [2] Any quantifier can nest itself and other quantifiers on the same list #8 | refused (400): Pipeline segment has no data source |
| [3] Any quantifier is true if the single or the all quantifier is true #1 | refused (400): Query has no data source |
| [3] Any quantifier is true if the single or the all quantifier is true #2 | refused (400): Query has no data source |
| [3] Any quantifier is true if the single or the all quantifier is true #3 | refused (400): Query has no data source |
| [3] Any quantifier is true if the single or the all quantifier is true #4 | refused (400): Query has no data source |
| [3] Any quantifier is true if the single or the all quantifier is true #5 | refused (400): Query has no data source |
| [4] Any quantifier is equal the boolean negative of the none quantifier #1 | refused (400): Query has no data source |
| [4] Any quantifier is equal the boolean negative of the none quantifier #2 | refused (400): Query has no data source |
| [4] Any quantifier is equal the boolean negative of the none quantifier #3 | refused (400): Query has no data source |
| [4] Any quantifier is equal the boolean negative of the none quantifier #4 | refused (400): Query has no data source |
| [4] Any quantifier is equal the boolean negative of the none quantifier #5 | refused (400): Query has no data source |
| [5] Any quantifier is equal the boolean negative of the all quantifier on the boolean negative of the predicate #1 | refused (400): Query has no data source |
| [5] Any quantifier is equal the boolean negative of the all quantifier on the boolean negative of the predicate #2 | refused (400): Query has no data source |
| [5] Any quantifier is equal the boolean negative of the all quantifier on the boolean negative of the predicate #3 | refused (400): Query has no data source |
| [5] Any quantifier is equal the boolean negative of the all quantifier on the boolean negative of the predicate #4 | refused (400): Query has no data source |
| [5] Any quantifier is equal the boolean negative of the all quantifier on the boolean negative of the predicate #5 | refused (400): Query has no data source |
| [6] Any quantifier is equal whether the size of the list filtered with same the predicate is grater zero #1 | refused (400): Query has no data source |
| [6] Any quantifier is equal whether the size of the list filtered with same the predicate is grater zero #2 | refused (400): Query has no data source |
| [6] Any quantifier is equal whether the size of the list filtered with same the predicate is grater zero #3 | refused (400): Query has no data source |
| [6] Any quantifier is equal whether the size of the list filtered with same the predicate is grater zero #4 | refused (400): Query has no data source |
| [6] Any quantifier is equal whether the size of the list filtered with same the predicate is grater zero #5 | refused (400): Query has no data source |

</details>

<details><summary>Quantifier8 - All quantifier interop (31)</summary>

| Case | Reason |
| --- | --- |
| [1] All quantifier can nest itself and other quantifiers on nested lists #1 | refused (400): Query has no data source |
| [1] All quantifier can nest itself and other quantifiers on nested lists #2 | refused (400): Query has no data source |
| [1] All quantifier can nest itself and other quantifiers on nested lists #3 | refused (400): Query has no data source |
| [1] All quantifier can nest itself and other quantifiers on nested lists #4 | refused (400): Query has no data source |
| [1] All quantifier can nest itself and other quantifiers on nested lists #5 | refused (400): Query has no data source |
| [1] All quantifier can nest itself and other quantifiers on nested lists #6 | refused (400): Query has no data source |
| [1] All quantifier can nest itself and other quantifiers on nested lists #7 | refused (400): Query has no data source |
| [1] All quantifier can nest itself and other quantifiers on nested lists #8 | refused (400): Query has no data source |
| [2] All quantifier can nest itself and other quantifiers on the same list #1 | refused (400): Pipeline segment has no data source |
| [2] All quantifier can nest itself and other quantifiers on the same list #2 | refused (400): Pipeline segment has no data source |
| [2] All quantifier can nest itself and other quantifiers on the same list #3 | refused (400): Pipeline segment has no data source |
| [2] All quantifier can nest itself and other quantifiers on the same list #4 | refused (400): Pipeline segment has no data source |
| [2] All quantifier can nest itself and other quantifiers on the same list #5 | refused (400): Pipeline segment has no data source |
| [2] All quantifier can nest itself and other quantifiers on the same list #6 | refused (400): Pipeline segment has no data source |
| [2] All quantifier can nest itself and other quantifiers on the same list #7 | refused (400): Pipeline segment has no data source |
| [2] All quantifier can nest itself and other quantifiers on the same list #8 | refused (400): Pipeline segment has no data source |
| [3] All quantifier is equal the none quantifier on the boolean negative of the predicate #1 | refused (400): Query has no data source |
| [3] All quantifier is equal the none quantifier on the boolean negative of the predicate #2 | refused (400): Query has no data source |
| [3] All quantifier is equal the none quantifier on the boolean negative of the predicate #3 | refused (400): Query has no data source |
| [3] All quantifier is equal the none quantifier on the boolean negative of the predicate #4 | refused (400): Query has no data source |
| [3] All quantifier is equal the none quantifier on the boolean negative of the predicate #5 | refused (400): Query has no data source |
| [4] All quantifier is equal the boolean negative of the any quantifier on the boolean negative of the predicate #1 | refused (400): Query has no data source |
| [4] All quantifier is equal the boolean negative of the any quantifier on the boolean negative of the predicate #2 | refused (400): Query has no data source |
| [4] All quantifier is equal the boolean negative of the any quantifier on the boolean negative of the predicate #3 | refused (400): Query has no data source |
| [4] All quantifier is equal the boolean negative of the any quantifier on the boolean negative of the predicate #4 | refused (400): Query has no data source |
| [4] All quantifier is equal the boolean negative of the any quantifier on the boolean negative of the predicate #5 | refused (400): Query has no data source |
| [5] All quantifier is equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #1 | refused (400): Query has no data source |
| [5] All quantifier is equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #2 | refused (400): Query has no data source |
| [5] All quantifier is equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #3 | refused (400): Query has no data source |
| [5] All quantifier is equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #4 | refused (400): Query has no data source |
| [5] All quantifier is equal whether the size of the list filtered with same the predicate is equal the size of the unfiltered list #5 | refused (400): Query has no data source |

</details>

<details><summary>Quantifier9 - None quantifier invariants (17)</summary>

| Case | Reason |
| --- | --- |
| [1] None quantifier is always true if the predicate is statically false and the list is not empty | refused (500): Internal server error |
| [2] None quantifier is always false if the predicate is statically true and the list is not empty | refused (500): Internal server error |
| [3] None quantifier is always equal the boolean negative of the any quantifier #1 | refused (400): Pipeline segment has no data source |
| [3] None quantifier is always equal the boolean negative of the any quantifier #2 | refused (400): Pipeline segment has no data source |
| [3] None quantifier is always equal the boolean negative of the any quantifier #3 | refused (400): Pipeline segment has no data source |
| [3] None quantifier is always equal the boolean negative of the any quantifier #4 | refused (400): Pipeline segment has no data source |
| [3] None quantifier is always equal the boolean negative of the any quantifier #5 | refused (400): Pipeline segment has no data source |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #1 | refused (400): Pipeline segment has no data source |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #2 | refused (400): Pipeline segment has no data source |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #3 | refused (400): Pipeline segment has no data source |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #4 | refused (400): Pipeline segment has no data source |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #5 | refused (400): Pipeline segment has no data source |
| [5] None quantifier is always equal whether the size of the list filtered with same the predicate is zero #1 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [5] None quantifier is always equal whether the size of the list filtered with same the predicate is zero #2 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [5] None quantifier is always equal whether the size of the list filtered with same the predicate is zero #3 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [5] None quantifier is always equal whether the size of the list filtered with same the predicate is zero #4 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |
| [5] None quantifier is always equal whether the size of the list filtered with same the predicate is zero #5 | refused (500): Governance failed: Expected END after CASE. Line 1, Col: 88.
  WITH _w0 AS (SELECT CASE WHEN CAST(ELEMENT_AT(input, 'fixed') AS JSON) ->> VARIADIC [4m'{}'[0m THEN CAST(ELEMENT_AT(input, 'list') AS J |

</details>

<details><summary>String1 - Substring extraction (1)</summary>

| Case | Reason |
| --- | --- |
| [1] `substring()` with default second argument | refused (400): Query has no data source |

</details>

<details><summary>String3 - String Reversal (1)</summary>

| Case | Reason |
| --- | --- |
| [1] `reverse()` | refused (400): Query has no data source |

</details>

<details><summary>String4 - String Splitting (1)</summary>

| Case | Reason |
| --- | --- |
| [1] `split()` | refused (500): Execution failed: function split(unknown, unknown) does not exist
LINE 1: SELECT COUNT(_uw0."item") AS item FROM UNNEST(SPLIT('one1two...
                                                      ^
HINT:  |

</details>

<details><summary>Temporal1 - Create Temporal Values from a Map (207)</summary>

| Case | Reason |
| --- | --- |
| [1] Should construct week date #1 | refused (400): Query has no data source |
| [1] Should construct week date #2 | refused (400): Query has no data source |
| [1] Should construct week date #3 | refused (400): Query has no data source |
| [1] Should construct week date #4 | refused (400): Query has no data source |
| [1] Should construct week date #5 | refused (400): Query has no data source |
| [1] Should construct week date #6 | refused (400): Query has no data source |
| [1] Should construct week date #7 | refused (400): Query has no data source |
| [1] Should construct week date #8 | refused (400): Query has no data source |
| [1] Should construct week date #9 | refused (400): Query has no data source |
| [1] Should construct week date #10 | refused (400): Query has no data source |
| [1] Should construct week date #11 | refused (400): Query has no data source |
| [1] Should construct week date #12 | refused (400): Query has no data source |
| [1] Should construct week date #13 | refused (400): Query has no data source |
| [1] Should construct week date #14 | refused (400): Query has no data source |
| [1] Should construct week date #15 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #1 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #2 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #3 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #4 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #5 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #6 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #7 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #8 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #9 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #10 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #11 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #12 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #13 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #14 | refused (400): Query has no data source |
| [2] Should construct week localdatetime #15 | refused (400): Query has no data source |
| [3] Should construct week datetime #1 | refused (400): Query has no data source |
| [3] Should construct week datetime #2 | refused (400): Query has no data source |
| [3] Should construct week datetime #3 | refused (400): Query has no data source |
| [3] Should construct week datetime #4 | refused (400): Query has no data source |
| [3] Should construct week datetime #5 | refused (400): Query has no data source |
| [3] Should construct week datetime #6 | refused (400): Query has no data source |
| [3] Should construct week datetime #7 | refused (400): Query has no data source |
| [3] Should construct week datetime #8 | refused (400): Query has no data source |
| [3] Should construct week datetime #9 | refused (400): Query has no data source |
| [3] Should construct week datetime #10 | refused (400): Query has no data source |
| [3] Should construct week datetime #11 | refused (400): Query has no data source |
| [3] Should construct week datetime #12 | refused (400): Query has no data source |
| [3] Should construct week datetime #13 | refused (400): Query has no data source |
| [3] Should construct week datetime #14 | refused (400): Query has no data source |
| [3] Should construct week datetime #15 | refused (400): Query has no data source |
| [4] Should construct date #1 | refused (400): Query has no data source |
| [4] Should construct date #2 | refused (400): Query has no data source |
| [4] Should construct date #3 | refused (400): Query has no data source |
| [4] Should construct date #4 | refused (400): Query has no data source |
| [4] Should construct date #5 | refused (400): Query has no data source |
| [4] Should construct date #6 | refused (400): Query has no data source |
| [4] Should construct date #7 | refused (400): Query has no data source |
| [4] Should construct date #8 | refused (400): Query has no data source |
| [5] Should construct local time #1 | refused (400): Query has no data source |
| [5] Should construct local time #2 | refused (400): Query has no data source |
| [5] Should construct local time #3 | refused (400): Query has no data source |
| [5] Should construct local time #4 | refused (400): Query has no data source |
| [5] Should construct local time #5 | refused (400): Query has no data source |
| [5] Should construct local time #6 | refused (400): Query has no data source |
| [5] Should construct local time #7 | refused (400): Query has no data source |
| [6] Should construct time #1 | refused (400): Query has no data source |
| [6] Should construct time #2 | refused (400): Query has no data source |
| [6] Should construct time #3 | refused (400): Query has no data source |
| [6] Should construct time #4 | refused (400): Query has no data source |
| [6] Should construct time #5 | refused (400): Query has no data source |
| [6] Should construct time #6 | refused (400): Query has no data source |
| [6] Should construct time #7 | refused (400): Query has no data source |
| [6] Should construct time #8 | refused (400): Query has no data source |
| [6] Should construct time #9 | refused (400): Query has no data source |
| [6] Should construct time #10 | refused (400): Query has no data source |
| [6] Should construct time #11 | refused (400): Query has no data source |
| [6] Should construct time #12 | refused (400): Query has no data source |
| [6] Should construct time #13 | refused (400): Query has no data source |
| [6] Should construct time #14 | refused (400): Query has no data source |
| [7] Should construct local date time #1 | refused (400): Query has no data source |
| [7] Should construct local date time #2 | refused (400): Query has no data source |
| [7] Should construct local date time #3 | refused (400): Query has no data source |
| [7] Should construct local date time #4 | refused (400): Query has no data source |
| [7] Should construct local date time #5 | refused (400): Query has no data source |
| [7] Should construct local date time #6 | refused (400): Query has no data source |
| [7] Should construct local date time #7 | refused (400): Query has no data source |
| [7] Should construct local date time #8 | refused (400): Query has no data source |
| [7] Should construct local date time #9 | refused (400): Query has no data source |
| [7] Should construct local date time #10 | refused (400): Query has no data source |
| [7] Should construct local date time #11 | refused (400): Query has no data source |
| [7] Should construct local date time #12 | refused (400): Query has no data source |
| [7] Should construct local date time #13 | refused (400): Query has no data source |
| [7] Should construct local date time #14 | refused (400): Query has no data source |
| [7] Should construct local date time #15 | refused (400): Query has no data source |
| [7] Should construct local date time #16 | refused (400): Query has no data source |
| [7] Should construct local date time #17 | refused (400): Query has no data source |
| [7] Should construct local date time #18 | refused (400): Query has no data source |
| [7] Should construct local date time #19 | refused (400): Query has no data source |
| [7] Should construct local date time #20 | refused (400): Query has no data source |
| [7] Should construct local date time #21 | refused (400): Query has no data source |
| [7] Should construct local date time #22 | refused (400): Query has no data source |
| [7] Should construct local date time #23 | refused (400): Query has no data source |
| [7] Should construct local date time #24 | refused (400): Query has no data source |
| [7] Should construct local date time #25 | refused (400): Query has no data source |
| [7] Should construct local date time #26 | refused (400): Query has no data source |
| [7] Should construct local date time #27 | refused (400): Query has no data source |
| [7] Should construct local date time #28 | refused (400): Query has no data source |
| [7] Should construct local date time #29 | refused (400): Query has no data source |
| [7] Should construct local date time #30 | refused (400): Query has no data source |
| [7] Should construct local date time #31 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #1 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #2 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #3 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #4 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #5 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #6 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #7 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #8 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #9 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #10 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #11 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #12 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #13 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #14 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #15 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #16 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #17 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #18 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #19 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #20 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #21 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #22 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #23 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #24 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #25 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #26 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #27 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #28 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #29 | refused (400): Query has no data source |
| [8] Should construct date time with default time zone #30 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #1 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #2 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #3 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #4 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #5 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #6 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #7 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #8 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #9 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #10 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #11 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #12 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #13 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #14 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #15 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #16 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #17 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #18 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #19 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #20 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #21 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #22 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #23 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #24 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #25 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #26 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #27 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #28 | refused (400): Query has no data source |
| [9] Should construct date time with offset time zone #29 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #1 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #2 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #3 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #4 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #5 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #6 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #7 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #8 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #9 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #10 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #11 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #12 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #13 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #14 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #15 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #16 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #17 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #18 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #19 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #20 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #21 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #22 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #23 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #24 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #25 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #26 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #27 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #28 | refused (400): Query has no data source |
| [10] Should construct date time with named time zone #29 | refused (400): Query has no data source |
| [11] Should construct date time from epoch | refused (400): Query has no data source |
| [12] Should construct duration #1 | refused (400): Query has no data source |
| [12] Should construct duration #2 | refused (400): Query has no data source |
| [12] Should construct duration #3 | refused (400): Query has no data source |
| [12] Should construct duration #4 | refused (400): Query has no data source |
| [12] Should construct duration #5 | refused (400): Query has no data source |
| [12] Should construct duration #6 | refused (400): Query has no data source |
| [12] Should construct duration #7 | refused (400): Query has no data source |
| [12] Should construct duration #8 | refused (400): Query has no data source |
| [12] Should construct duration #9 | refused (400): Query has no data source |
| [13] Should construct temporal with time offset with second precision #1 | refused (400): Query has no data source |
| [13] Should construct temporal with time offset with second precision #2 | refused (400): Query has no data source |
| [13] Should construct temporal with time offset with second precision #3 | refused (400): Query has no data source |
| [13] Should construct temporal with time offset with second precision #4 | refused (400): Query has no data source |

</details>

<details><summary>Temporal10 - Compute Durations Between two Temporal Values (131)</summary>

| Case | Reason |
| --- | --- |
| [1] Should split between boundaries correctly #1 | refused (400): Pipeline segment has no data source |
| [1] Should split between boundaries correctly #2 | refused (400): Pipeline segment has no data source |
| [1] Should split between boundaries correctly #3 | refused (400): Pipeline segment has no data source |
| [1] Should split between boundaries correctly #4 | refused (400): Pipeline segment has no data source |
| [1] Should split between boundaries correctly #5 | refused (400): Pipeline segment has no data source |
| [1] Should split between boundaries correctly #6 | refused (400): Pipeline segment has no data source |
| [2] Should compute duration between two temporals #1 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #2 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #3 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #4 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #5 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #6 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #7 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #8 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #9 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #10 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #11 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #12 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #13 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #14 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #15 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #16 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #17 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #18 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #19 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #20 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #21 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #22 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #23 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #24 | refused (400): Query has no data source |
| [2] Should compute duration between two temporals #25 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #1 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #2 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #3 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #4 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #5 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #6 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #7 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #8 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #9 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #10 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #11 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #12 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #13 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #14 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #15 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #16 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #17 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #18 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #19 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #20 | refused (400): Query has no data source |
| [3] Should compute duration between two temporals in months #21 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #1 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #2 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #3 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #4 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #5 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #6 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #7 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #8 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #9 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #10 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #11 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #12 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #13 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #14 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #15 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #16 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #17 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #18 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #19 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #20 | refused (400): Query has no data source |
| [4] Should compute duration between two temporals in days #21 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #1 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #2 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #3 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #4 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #5 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #6 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #7 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #8 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #9 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #10 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #11 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #12 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #13 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #14 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #15 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #16 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #17 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #18 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #19 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #20 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #21 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #22 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #23 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #24 | refused (400): Query has no data source |
| [5] Should compute duration between two temporals in seconds #25 | refused (400): Query has no data source |
| [6] Should compute duration between if they differ only by a fraction of a second and the first comes after the second. | refused (400): Query has no data source |
| [7] Should compute negative duration between in big units #1 | refused (400): Query has no data source |
| [7] Should compute negative duration between in big units #2 | refused (400): Query has no data source |
| [7] Should compute negative duration between in big units #3 | refused (400): Query has no data source |
| [7] Should compute negative duration between in big units #4 | refused (400): Query has no data source |
| [7] Should compute negative duration between in big units #5 | refused (400): Query has no data source |
| [8] Should handle durations at daylight saving time day #1 | refused (400): Query has no data source |
| [8] Should handle durations at daylight saving time day #2 | refused (400): Query has no data source |
| [8] Should handle durations at daylight saving time day #3 | refused (400): Query has no data source |
| [8] Should handle durations at daylight saving time day #4 | refused (400): Query has no data source |
| [8] Should handle durations at daylight saving time day #5 | refused (400): Query has no data source |
| [8] Should handle durations at daylight saving time day #6 | refused (400): Query has no data source |
| [9] Should handle large durations | refused (400): Query has no data source |
| [10] Should handle large durations in seconds | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #1 | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #2 | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #3 | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #4 | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #5 | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #6 | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #7 | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #8 | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #9 | refused (400): Query has no data source |
| [11] Should handle when seconds and subseconds have different signs #10 | refused (400): Query has no data source |
| [12] Should compute durations with no difference #1 | refused (400): Query has no data source |
| [12] Should compute durations with no difference #2 | refused (400): Query has no data source |
| [12] Should compute durations with no difference #3 | refused (400): Query has no data source |
| [12] Should compute durations with no difference #4 | refused (400): Query has no data source |
| [12] Should compute durations with no difference #5 | refused (400): Query has no data source |
| [13] Should propagate null #1 | refused (400): Query has no data source |
| [13] Should propagate null #2 | refused (400): Query has no data source |
| [13] Should propagate null #3 | refused (400): Query has no data source |
| [13] Should propagate null #4 | refused (400): Query has no data source |

</details>

<details><summary>Temporal2 - Create Temporal Values from a String (53)</summary>

| Case | Reason |
| --- | --- |
| [1] Should parse date from string #1 | refused (400): Query has no data source |
| [1] Should parse date from string #2 | refused (400): Query has no data source |
| [1] Should parse date from string #3 | refused (400): Query has no data source |
| [1] Should parse date from string #4 | refused (400): Query has no data source |
| [1] Should parse date from string #5 | refused (400): Query has no data source |
| [1] Should parse date from string #6 | refused (400): Query has no data source |
| [1] Should parse date from string #7 | refused (400): Query has no data source |
| [1] Should parse date from string #8 | refused (400): Query has no data source |
| [1] Should parse date from string #9 | refused (400): Query has no data source |
| [1] Should parse date from string #10 | refused (400): Query has no data source |
| [1] Should parse date from string #11 | refused (400): Query has no data source |
| [2] Should parse local time from string #1 | refused (400): Query has no data source |
| [2] Should parse local time from string #2 | refused (400): Query has no data source |
| [2] Should parse local time from string #3 | refused (400): Query has no data source |
| [2] Should parse local time from string #4 | refused (400): Query has no data source |
| [2] Should parse local time from string #5 | refused (400): Query has no data source |
| [2] Should parse local time from string #6 | refused (400): Query has no data source |
| [2] Should parse local time from string #7 | refused (400): Query has no data source |
| [3] Should parse time from string #1 | refused (400): Query has no data source |
| [3] Should parse time from string #2 | refused (400): Query has no data source |
| [3] Should parse time from string #3 | refused (400): Query has no data source |
| [3] Should parse time from string #4 | refused (400): Query has no data source |
| [3] Should parse time from string #5 | refused (400): Query has no data source |
| [3] Should parse time from string #6 | refused (400): Query has no data source |
| [3] Should parse time from string #7 | refused (400): Query has no data source |
| [3] Should parse time from string #8 | refused (400): Query has no data source |
| [4] Should parse local date time from string #1 | refused (400): Query has no data source |
| [4] Should parse local date time from string #2 | refused (400): Query has no data source |
| [4] Should parse local date time from string #3 | refused (400): Query has no data source |
| [4] Should parse local date time from string #4 | refused (400): Query has no data source |
| [4] Should parse local date time from string #5 | refused (400): Query has no data source |
| [4] Should parse local date time from string #6 | refused (400): Query has no data source |
| [4] Should parse local date time from string #7 | refused (400): Query has no data source |
| [5] Should parse date time from string #1 | refused (400): Query has no data source |
| [5] Should parse date time from string #2 | refused (400): Query has no data source |
| [5] Should parse date time from string #3 | refused (400): Query has no data source |
| [5] Should parse date time from string #4 | refused (400): Query has no data source |
| [5] Should parse date time from string #5 | refused (400): Query has no data source |
| [5] Should parse date time from string #6 | refused (400): Query has no data source |
| [5] Should parse date time from string #7 | refused (400): Query has no data source |
| [5] Should parse date time from string #8 | refused (400): Query has no data source |
| [6] Should parse date time with named time zone from string #1 | refused (400): Query has no data source |
| [6] Should parse date time with named time zone from string #2 | refused (400): Query has no data source |
| [6] Should parse date time with named time zone from string #3 | refused (400): Query has no data source |
| [6] Should parse date time with named time zone from string #4 | refused (400): Query has no data source |
| [6] Should parse date time with named time zone from string #5 | refused (400): Query has no data source |
| [7] Should parse duration from string #1 | refused (400): Query has no data source |
| [7] Should parse duration from string #2 | refused (400): Query has no data source |
| [7] Should parse duration from string #3 | refused (400): Query has no data source |
| [7] Should parse duration from string #4 | refused (400): Query has no data source |
| [7] Should parse duration from string #5 | refused (400): Query has no data source |
| [7] Should parse duration from string #6 | refused (400): Query has no data source |
| [7] Should parse duration from string #7 | refused (400): Query has no data source |

</details>

<details><summary>Temporal3 - Project Temporal Values from other Temporal Values (183)</summary>

| Case | Reason |
| --- | --- |
| [1] Should select date #1 | refused (400): Pipeline segment has no data source |
| [1] Should select date #2 | refused (400): Pipeline segment has no data source |
| [1] Should select date #3 | refused (400): Pipeline segment has no data source |
| [1] Should select date #4 | refused (400): Pipeline segment has no data source |
| [1] Should select date #5 | refused (400): Pipeline segment has no data source |
| [1] Should select date #6 | refused (400): Pipeline segment has no data source |
| [1] Should select date #7 | refused (400): Pipeline segment has no data source |
| [1] Should select date #8 | refused (400): Pipeline segment has no data source |
| [1] Should select date #9 | refused (400): Pipeline segment has no data source |
| [1] Should select date #10 | refused (400): Pipeline segment has no data source |
| [1] Should select date #11 | refused (400): Pipeline segment has no data source |
| [1] Should select date #12 | refused (400): Pipeline segment has no data source |
| [1] Should select date #13 | refused (400): Pipeline segment has no data source |
| [1] Should select date #14 | refused (400): Pipeline segment has no data source |
| [1] Should select date #15 | refused (400): Pipeline segment has no data source |
| [1] Should select date #16 | refused (400): Pipeline segment has no data source |
| [1] Should select date #17 | refused (400): Pipeline segment has no data source |
| [1] Should select date #18 | refused (400): Pipeline segment has no data source |
| [1] Should select date #19 | refused (400): Pipeline segment has no data source |
| [1] Should select date #20 | refused (400): Pipeline segment has no data source |
| [1] Should select date #21 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #1 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #2 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #3 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #4 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #5 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #6 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #7 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #8 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #9 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #10 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #11 | refused (400): Pipeline segment has no data source |
| [2] Should select local time #12 | refused (400): Pipeline segment has no data source |
| [3] Should select time #1 | refused (400): Pipeline segment has no data source |
| [3] Should select time #2 | refused (400): Pipeline segment has no data source |
| [3] Should select time #3 | refused (400): Pipeline segment has no data source |
| [3] Should select time #4 | refused (400): Pipeline segment has no data source |
| [3] Should select time #5 | refused (400): Pipeline segment has no data source |
| [3] Should select time #6 | refused (400): Pipeline segment has no data source |
| [3] Should select time #7 | refused (400): Pipeline segment has no data source |
| [3] Should select time #8 | refused (400): Pipeline segment has no data source |
| [3] Should select time #9 | refused (400): Pipeline segment has no data source |
| [3] Should select time #10 | refused (400): Pipeline segment has no data source |
| [3] Should select time #11 | refused (400): Pipeline segment has no data source |
| [3] Should select time #12 | refused (400): Pipeline segment has no data source |
| [3] Should select time #13 | refused (400): Pipeline segment has no data source |
| [3] Should select time #14 | refused (400): Pipeline segment has no data source |
| [3] Should select time #15 | refused (400): Pipeline segment has no data source |
| [3] Should select time #16 | refused (400): Pipeline segment has no data source |
| [3] Should select time #17 | refused (400): Pipeline segment has no data source |
| [3] Should select time #18 | refused (400): Pipeline segment has no data source |
| [3] Should select time #19 | refused (400): Pipeline segment has no data source |
| [3] Should select time #20 | refused (400): Pipeline segment has no data source |
| [4] Should select date into local date time #1 | refused (400): Pipeline segment has no data source |
| [4] Should select date into local date time #2 | refused (400): Pipeline segment has no data source |
| [4] Should select date into local date time #3 | refused (400): Pipeline segment has no data source |
| [4] Should select date into local date time #4 | refused (400): Pipeline segment has no data source |
| [4] Should select date into local date time #5 | refused (400): Pipeline segment has no data source |
| [4] Should select date into local date time #6 | refused (400): Pipeline segment has no data source |
| [5] Should select time into local date time #1 | refused (400): Pipeline segment has no data source |
| [5] Should select time into local date time #2 | refused (400): Pipeline segment has no data source |
| [5] Should select time into local date time #3 | refused (400): Pipeline segment has no data source |
| [5] Should select time into local date time #4 | refused (400): Pipeline segment has no data source |
| [5] Should select time into local date time #5 | refused (400): Pipeline segment has no data source |
| [5] Should select time into local date time #6 | refused (400): Pipeline segment has no data source |
| [5] Should select time into local date time #7 | refused (400): Pipeline segment has no data source |
| [5] Should select time into local date time #8 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #1 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #2 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #3 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #4 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #5 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #6 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #7 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #8 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #9 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #10 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #11 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #12 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #13 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #14 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #15 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #16 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #17 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #18 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #19 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #20 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #21 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #22 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #23 | refused (400): Pipeline segment has no data source |
| [6] Should select date and time into local date time #24 | refused (400): Pipeline segment has no data source |
| [7] Should select datetime into local date time #1 | refused (400): Pipeline segment has no data source |
| [7] Should select datetime into local date time #2 | refused (400): Pipeline segment has no data source |
| [7] Should select datetime into local date time #3 | refused (400): Pipeline segment has no data source |
| [7] Should select datetime into local date time #4 | refused (400): Pipeline segment has no data source |
| [7] Should select datetime into local date time #5 | refused (400): Pipeline segment has no data source |
| [7] Should select datetime into local date time #6 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #1 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #2 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #3 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #4 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #5 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #6 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #7 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #8 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #9 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #10 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #11 | refused (400): Pipeline segment has no data source |
| [8] Should select date into date time #12 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #1 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #2 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #3 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #4 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #5 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #6 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #7 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #8 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #9 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #10 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #11 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #12 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #13 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #14 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #15 | refused (400): Pipeline segment has no data source |
| [9] Should select time into date time #16 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #1 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #2 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #3 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #4 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #5 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #6 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #7 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #8 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #9 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #10 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #11 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #12 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #13 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #14 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #15 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #16 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #17 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #18 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #19 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #20 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #21 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #22 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #23 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #24 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #25 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #26 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #27 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #28 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #29 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #30 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #31 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #32 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #33 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #34 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #35 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #36 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #37 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #38 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #39 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #40 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #41 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #42 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #43 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #44 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #45 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #46 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #47 | refused (400): Pipeline segment has no data source |
| [10] Should select date and time into date time #48 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #1 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #2 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #3 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #4 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #5 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #6 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #7 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #8 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #9 | refused (400): Pipeline segment has no data source |
| [11] Should datetime into date time #10 | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Temporal4 - Store Temporal Values (39)</summary>

| Case | Reason |
| --- | --- |
| [1] Should store date #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] Should store date array #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [2] Should store date array #2 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [3] Should store local time #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [4] Should store local time array #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [4] Should store local time array #2 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [5] Should store time #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [6] Should store time array #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [6] Should store time array #2 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [7] Should store local date time #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [8] Should store local date time array #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [8] Should store local date time array #2 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [9] Should store date time #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [10] Should store date time array #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [10] Should store date time array #2 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [11] Should store duration #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [12] Should store duration array #1 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [12] Should store duration array #2 | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |
| [13] Should propagate null #1 | refused (400): Query has no data source |
| [13] Should propagate null #2 | refused (400): Query has no data source |
| [13] Should propagate null #3 | refused (400): Query has no data source |
| [13] Should propagate null #4 | refused (400): Query has no data source |
| [13] Should propagate null #5 | refused (400): Query has no data source |
| [13] Should propagate null #6 | refused (400): Query has no data source |
| [13] Should propagate null #7 | refused (400): Query has no data source |
| [13] Should propagate null #8 | refused (400): Query has no data source |
| [13] Should propagate null #9 | refused (400): Query has no data source |
| [13] Should propagate null #10 | refused (400): Query has no data source |
| [13] Should propagate null #11 | refused (400): Query has no data source |
| [13] Should propagate null #12 | refused (400): Query has no data source |
| [13] Should propagate null #13 | refused (400): Query has no data source |
| [13] Should propagate null #14 | refused (400): Query has no data source |
| [13] Should propagate null #15 | refused (400): Query has no data source |
| [13] Should propagate null #16 | refused (400): Query has no data source |
| [13] Should propagate null #17 | refused (400): Query has no data source |
| [13] Should propagate null #18 | refused (400): Query has no data source |
| [13] Should propagate null #19 | refused (400): Query has no data source |
| [13] Should propagate null #20 | refused (400): Query has no data source |
| [13] Should propagate null #21 | refused (400): Query has no data source |

</details>

<details><summary>Temporal6 - Render Temporal Values as a String (17)</summary>

| Case | Reason |
| --- | --- |
| [1] Should serialize date | refused (400): Pipeline segment has no data source |
| [2] Should serialize local time | refused (400): Pipeline segment has no data source |
| [3] Should serialize time | refused (400): Pipeline segment has no data source |
| [4] Should serialize local date time | refused (400): Pipeline segment has no data source |
| [5] Should serialize date time | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #1 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #2 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #3 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #4 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #5 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #6 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #7 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #8 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #9 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #10 | refused (400): Pipeline segment has no data source |
| [6] Should serialize duration #11 | refused (400): Pipeline segment has no data source |
| [7] Should serialize timezones correctly | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Temporal7 - Compare Temporal Values (18)</summary>

| Case | Reason |
| --- | --- |
| [1] Should compare dates #1 | refused (400): Pipeline segment has no data source |
| [1] Should compare dates #2 | refused (400): Pipeline segment has no data source |
| [2] Should compare local times #1 | refused (400): Pipeline segment has no data source |
| [2] Should compare local times #2 | refused (400): Pipeline segment has no data source |
| [3] Should compare times #1 | refused (400): Pipeline segment has no data source |
| [3] Should compare times #2 | refused (400): Pipeline segment has no data source |
| [4] Should compare local date times #1 | refused (400): Pipeline segment has no data source |
| [4] Should compare local date times #2 | refused (400): Pipeline segment has no data source |
| [5] Should compare date times #1 | refused (400): Pipeline segment has no data source |
| [5] Should compare date times #2 | refused (400): Pipeline segment has no data source |
| [6] Should compare durations for equality #1 | refused (400): Pipeline segment has no data source |
| [6] Should compare durations for equality #2 | refused (400): Pipeline segment has no data source |
| [6] Should compare durations for equality #3 | refused (400): Pipeline segment has no data source |
| [6] Should compare durations for equality #4 | refused (400): Pipeline segment has no data source |
| [6] Should compare durations for equality #5 | refused (400): Pipeline segment has no data source |
| [6] Should compare durations for equality #6 | refused (400): Pipeline segment has no data source |
| [6] Should compare durations for equality #7 | refused (400): Pipeline segment has no data source |
| [6] Should compare durations for equality #8 | refused (400): Pipeline segment has no data source |

</details>

<details><summary>Temporal9 - Truncate Temporal Values (322)</summary>

| Case | Reason |
| --- | --- |
| [1] Should truncate date #1 | refused (400): Query has no data source |
| [1] Should truncate date #2 | refused (400): Query has no data source |
| [1] Should truncate date #3 | refused (400): Query has no data source |
| [1] Should truncate date #4 | refused (400): Query has no data source |
| [1] Should truncate date #5 | refused (400): Query has no data source |
| [1] Should truncate date #6 | refused (400): Query has no data source |
| [1] Should truncate date #7 | refused (400): Query has no data source |
| [1] Should truncate date #8 | refused (400): Query has no data source |
| [1] Should truncate date #9 | refused (400): Query has no data source |
| [1] Should truncate date #10 | refused (400): Query has no data source |
| [1] Should truncate date #11 | refused (400): Query has no data source |
| [1] Should truncate date #12 | refused (400): Query has no data source |
| [1] Should truncate date #13 | refused (400): Query has no data source |
| [1] Should truncate date #14 | refused (400): Query has no data source |
| [1] Should truncate date #15 | refused (400): Query has no data source |
| [1] Should truncate date #16 | refused (400): Query has no data source |
| [1] Should truncate date #17 | refused (400): Query has no data source |
| [1] Should truncate date #18 | refused (400): Query has no data source |
| [1] Should truncate date #19 | refused (400): Query has no data source |
| [1] Should truncate date #20 | refused (400): Query has no data source |
| [1] Should truncate date #21 | refused (400): Query has no data source |
| [1] Should truncate date #22 | refused (400): Query has no data source |
| [1] Should truncate date #23 | refused (400): Query has no data source |
| [1] Should truncate date #24 | refused (400): Query has no data source |
| [1] Should truncate date #25 | refused (400): Query has no data source |
| [1] Should truncate date #26 | refused (400): Query has no data source |
| [1] Should truncate date #27 | refused (400): Query has no data source |
| [1] Should truncate date #28 | refused (400): Query has no data source |
| [1] Should truncate date #29 | refused (400): Query has no data source |
| [1] Should truncate date #30 | refused (400): Query has no data source |
| [1] Should truncate date #31 | refused (400): Query has no data source |
| [1] Should truncate date #32 | refused (400): Query has no data source |
| [1] Should truncate date #33 | refused (400): Query has no data source |
| [1] Should truncate date #34 | refused (400): Query has no data source |
| [1] Should truncate date #35 | refused (400): Query has no data source |
| [1] Should truncate date #36 | refused (400): Query has no data source |
| [1] Should truncate date #37 | refused (400): Query has no data source |
| [1] Should truncate date #38 | refused (400): Query has no data source |
| [1] Should truncate date #39 | refused (400): Query has no data source |
| [1] Should truncate date #40 | refused (400): Query has no data source |
| [1] Should truncate date #41 | refused (400): Query has no data source |
| [1] Should truncate date #42 | refused (400): Query has no data source |
| [1] Should truncate date #43 | refused (400): Query has no data source |
| [1] Should truncate date #44 | refused (400): Query has no data source |
| [1] Should truncate date #45 | refused (400): Query has no data source |
| [1] Should truncate date #46 | refused (400): Query has no data source |
| [1] Should truncate date #47 | refused (400): Query has no data source |
| [1] Should truncate date #48 | refused (400): Query has no data source |
| [1] Should truncate date #49 | refused (400): Query has no data source |
| [1] Should truncate date #50 | refused (400): Query has no data source |
| [1] Should truncate date #51 | refused (400): Query has no data source |
| [2] Should truncate datetime #1 | refused (400): Query has no data source |
| [2] Should truncate datetime #2 | refused (400): Query has no data source |
| [2] Should truncate datetime #3 | refused (400): Query has no data source |
| [2] Should truncate datetime #4 | refused (400): Query has no data source |
| [2] Should truncate datetime #5 | refused (400): Query has no data source |
| [2] Should truncate datetime #6 | refused (400): Query has no data source |
| [2] Should truncate datetime #7 | refused (400): Query has no data source |
| [2] Should truncate datetime #8 | refused (400): Query has no data source |
| [2] Should truncate datetime #9 | refused (400): Query has no data source |
| [2] Should truncate datetime #10 | refused (400): Query has no data source |
| [2] Should truncate datetime #11 | refused (400): Query has no data source |
| [2] Should truncate datetime #12 | refused (400): Query has no data source |
| [2] Should truncate datetime #13 | refused (400): Query has no data source |
| [2] Should truncate datetime #14 | refused (400): Query has no data source |
| [2] Should truncate datetime #15 | refused (400): Query has no data source |
| [2] Should truncate datetime #16 | refused (400): Query has no data source |
| [2] Should truncate datetime #17 | refused (400): Query has no data source |
| [2] Should truncate datetime #18 | refused (400): Query has no data source |
| [2] Should truncate datetime #19 | refused (400): Query has no data source |
| [2] Should truncate datetime #20 | refused (400): Query has no data source |
| [2] Should truncate datetime #21 | refused (400): Query has no data source |
| [2] Should truncate datetime #22 | refused (400): Query has no data source |
| [2] Should truncate datetime #23 | refused (400): Query has no data source |
| [2] Should truncate datetime #24 | refused (400): Query has no data source |
| [2] Should truncate datetime #25 | refused (400): Query has no data source |
| [2] Should truncate datetime #26 | refused (400): Query has no data source |
| [2] Should truncate datetime #27 | refused (400): Query has no data source |
| [2] Should truncate datetime #28 | refused (400): Query has no data source |
| [2] Should truncate datetime #29 | refused (400): Query has no data source |
| [2] Should truncate datetime #30 | refused (400): Query has no data source |
| [2] Should truncate datetime #31 | refused (400): Query has no data source |
| [2] Should truncate datetime #32 | refused (400): Query has no data source |
| [2] Should truncate datetime #33 | refused (400): Query has no data source |
| [2] Should truncate datetime #34 | refused (400): Query has no data source |
| [2] Should truncate datetime #35 | refused (400): Query has no data source |
| [2] Should truncate datetime #36 | refused (400): Query has no data source |
| [2] Should truncate datetime #37 | refused (400): Query has no data source |
| [2] Should truncate datetime #38 | refused (400): Query has no data source |
| [2] Should truncate datetime #39 | refused (400): Query has no data source |
| [2] Should truncate datetime #40 | refused (400): Query has no data source |
| [2] Should truncate datetime #41 | refused (400): Query has no data source |
| [2] Should truncate datetime #42 | refused (400): Query has no data source |
| [2] Should truncate datetime #43 | refused (400): Query has no data source |
| [2] Should truncate datetime #44 | refused (400): Query has no data source |
| [2] Should truncate datetime #45 | refused (400): Query has no data source |
| [2] Should truncate datetime #46 | refused (400): Query has no data source |
| [2] Should truncate datetime #47 | refused (400): Query has no data source |
| [2] Should truncate datetime #48 | refused (400): Query has no data source |
| [2] Should truncate datetime #49 | refused (400): Query has no data source |
| [2] Should truncate datetime #50 | refused (400): Query has no data source |
| [2] Should truncate datetime #51 | refused (400): Query has no data source |
| [2] Should truncate datetime #52 | refused (400): Query has no data source |
| [2] Should truncate datetime #53 | refused (400): Query has no data source |
| [2] Should truncate datetime #54 | refused (400): Query has no data source |
| [2] Should truncate datetime #55 | refused (400): Query has no data source |
| [2] Should truncate datetime #56 | refused (400): Query has no data source |
| [2] Should truncate datetime #57 | refused (400): Query has no data source |
| [2] Should truncate datetime #58 | refused (400): Query has no data source |
| [2] Should truncate datetime #59 | refused (400): Query has no data source |
| [2] Should truncate datetime #60 | refused (400): Query has no data source |
| [2] Should truncate datetime #61 | refused (400): Query has no data source |
| [2] Should truncate datetime #62 | refused (400): Query has no data source |
| [2] Should truncate datetime #63 | refused (400): Query has no data source |
| [2] Should truncate datetime #64 | refused (400): Query has no data source |
| [2] Should truncate datetime #65 | refused (400): Query has no data source |
| [2] Should truncate datetime #66 | refused (400): Query has no data source |
| [2] Should truncate datetime #67 | refused (400): Query has no data source |
| [2] Should truncate datetime #68 | refused (400): Query has no data source |
| [2] Should truncate datetime #69 | refused (400): Query has no data source |
| [2] Should truncate datetime #70 | refused (400): Query has no data source |
| [2] Should truncate datetime #71 | refused (400): Query has no data source |
| [2] Should truncate datetime #72 | refused (400): Query has no data source |
| [2] Should truncate datetime #73 | refused (400): Query has no data source |
| [2] Should truncate datetime #74 | refused (400): Query has no data source |
| [2] Should truncate datetime #75 | refused (400): Query has no data source |
| [2] Should truncate datetime #76 | refused (400): Query has no data source |
| [2] Should truncate datetime #77 | refused (400): Query has no data source |
| [2] Should truncate datetime #78 | refused (400): Query has no data source |
| [2] Should truncate datetime #79 | refused (400): Query has no data source |
| [2] Should truncate datetime #80 | refused (400): Query has no data source |
| [2] Should truncate datetime #81 | refused (400): Query has no data source |
| [2] Should truncate datetime #82 | refused (400): Query has no data source |
| [2] Should truncate datetime #83 | refused (400): Query has no data source |
| [2] Should truncate datetime #84 | refused (400): Query has no data source |
| [2] Should truncate datetime #85 | refused (400): Query has no data source |
| [2] Should truncate datetime #86 | refused (400): Query has no data source |
| [2] Should truncate datetime #87 | refused (400): Query has no data source |
| [2] Should truncate datetime #88 | refused (400): Query has no data source |
| [2] Should truncate datetime #89 | refused (400): Query has no data source |
| [2] Should truncate datetime #90 | refused (400): Query has no data source |
| [2] Should truncate datetime #91 | refused (400): Query has no data source |
| [2] Should truncate datetime #92 | refused (400): Query has no data source |
| [2] Should truncate datetime #93 | refused (400): Query has no data source |
| [2] Should truncate datetime #94 | refused (400): Query has no data source |
| [2] Should truncate datetime #95 | refused (400): Query has no data source |
| [2] Should truncate datetime #96 | refused (400): Query has no data source |
| [2] Should truncate datetime #97 | refused (400): Query has no data source |
| [2] Should truncate datetime #98 | refused (400): Query has no data source |
| [2] Should truncate datetime #99 | refused (400): Query has no data source |
| [2] Should truncate datetime #100 | refused (400): Query has no data source |
| [2] Should truncate datetime #101 | refused (400): Query has no data source |
| [2] Should truncate datetime #102 | refused (400): Query has no data source |
| [2] Should truncate datetime #103 | refused (400): Query has no data source |
| [2] Should truncate datetime #104 | refused (400): Query has no data source |
| [2] Should truncate datetime #105 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #1 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #2 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #3 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #4 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #5 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #6 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #7 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #8 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #9 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #10 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #11 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #12 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #13 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #14 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #15 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #16 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #17 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #18 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #19 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #20 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #21 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #22 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #23 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #24 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #25 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #26 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #27 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #28 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #29 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #30 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #31 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #32 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #33 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #34 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #35 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #36 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #37 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #38 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #39 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #40 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #41 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #42 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #43 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #44 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #45 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #46 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #47 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #48 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #49 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #50 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #51 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #52 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #53 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #54 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #55 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #56 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #57 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #58 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #59 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #60 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #61 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #62 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #63 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #64 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #65 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #66 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #67 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #68 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #69 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #70 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #71 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #72 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #73 | refused (400): Query has no data source |
| [3] Should truncate localdatetime #74 | refused (400): Query has no data source |
| [4] Should truncate localtime #1 | refused (400): Query has no data source |
| [4] Should truncate localtime #2 | refused (400): Query has no data source |
| [4] Should truncate localtime #3 | refused (400): Query has no data source |
| [4] Should truncate localtime #4 | refused (400): Query has no data source |
| [4] Should truncate localtime #5 | refused (400): Query has no data source |
| [4] Should truncate localtime #6 | refused (400): Query has no data source |
| [4] Should truncate localtime #7 | refused (400): Query has no data source |
| [4] Should truncate localtime #8 | refused (400): Query has no data source |
| [4] Should truncate localtime #9 | refused (400): Query has no data source |
| [4] Should truncate localtime #10 | refused (400): Query has no data source |
| [4] Should truncate localtime #11 | refused (400): Query has no data source |
| [4] Should truncate localtime #12 | refused (400): Query has no data source |
| [4] Should truncate localtime #13 | refused (400): Query has no data source |
| [4] Should truncate localtime #14 | refused (400): Query has no data source |
| [4] Should truncate localtime #15 | refused (400): Query has no data source |
| [4] Should truncate localtime #16 | refused (400): Query has no data source |
| [4] Should truncate localtime #17 | refused (400): Query has no data source |
| [4] Should truncate localtime #18 | refused (400): Query has no data source |
| [4] Should truncate localtime #19 | refused (400): Query has no data source |
| [4] Should truncate localtime #20 | refused (400): Query has no data source |
| [4] Should truncate localtime #21 | refused (400): Query has no data source |
| [4] Should truncate localtime #22 | refused (400): Query has no data source |
| [4] Should truncate localtime #23 | refused (400): Query has no data source |
| [4] Should truncate localtime #24 | refused (400): Query has no data source |
| [4] Should truncate localtime #25 | refused (400): Query has no data source |
| [4] Should truncate localtime #26 | refused (400): Query has no data source |
| [4] Should truncate localtime #27 | refused (400): Query has no data source |
| [4] Should truncate localtime #28 | refused (400): Query has no data source |
| [4] Should truncate localtime #29 | refused (400): Query has no data source |
| [4] Should truncate localtime #30 | refused (400): Query has no data source |
| [4] Should truncate localtime #31 | refused (400): Query has no data source |
| [4] Should truncate localtime #32 | refused (400): Query has no data source |
| [4] Should truncate localtime #33 | refused (400): Query has no data source |
| [4] Should truncate localtime #34 | refused (400): Query has no data source |
| [4] Should truncate localtime #35 | refused (400): Query has no data source |
| [4] Should truncate localtime #36 | refused (400): Query has no data source |
| [4] Should truncate localtime #37 | refused (400): Query has no data source |
| [4] Should truncate localtime #38 | refused (400): Query has no data source |
| [4] Should truncate localtime #39 | refused (400): Query has no data source |
| [4] Should truncate localtime #40 | refused (400): Query has no data source |
| [4] Should truncate localtime #41 | refused (400): Query has no data source |
| [4] Should truncate localtime #42 | refused (400): Query has no data source |
| [4] Should truncate localtime #43 | refused (400): Query has no data source |
| [4] Should truncate localtime #44 | refused (400): Query has no data source |
| [5] Should truncate time #1 | refused (400): Query has no data source |
| [5] Should truncate time #2 | refused (400): Query has no data source |
| [5] Should truncate time #3 | refused (400): Query has no data source |
| [5] Should truncate time #4 | refused (400): Query has no data source |
| [5] Should truncate time #5 | refused (400): Query has no data source |
| [5] Should truncate time #6 | refused (400): Query has no data source |
| [5] Should truncate time #7 | refused (400): Query has no data source |
| [5] Should truncate time #8 | refused (400): Query has no data source |
| [5] Should truncate time #9 | refused (400): Query has no data source |
| [5] Should truncate time #10 | refused (400): Query has no data source |
| [5] Should truncate time #11 | refused (400): Query has no data source |
| [5] Should truncate time #12 | refused (400): Query has no data source |
| [5] Should truncate time #13 | refused (400): Query has no data source |
| [5] Should truncate time #14 | refused (400): Query has no data source |
| [5] Should truncate time #15 | refused (400): Query has no data source |
| [5] Should truncate time #16 | refused (400): Query has no data source |
| [5] Should truncate time #17 | refused (400): Query has no data source |
| [5] Should truncate time #18 | refused (400): Query has no data source |
| [5] Should truncate time #19 | refused (400): Query has no data source |
| [5] Should truncate time #20 | refused (400): Query has no data source |
| [5] Should truncate time #21 | refused (400): Query has no data source |
| [5] Should truncate time #22 | refused (400): Query has no data source |
| [5] Should truncate time #23 | refused (400): Query has no data source |
| [5] Should truncate time #24 | refused (400): Query has no data source |
| [5] Should truncate time #25 | refused (400): Query has no data source |
| [5] Should truncate time #26 | refused (400): Query has no data source |
| [5] Should truncate time #27 | refused (400): Query has no data source |
| [5] Should truncate time #28 | refused (400): Query has no data source |
| [5] Should truncate time #29 | refused (400): Query has no data source |
| [5] Should truncate time #30 | refused (400): Query has no data source |
| [5] Should truncate time #31 | refused (400): Query has no data source |
| [5] Should truncate time #32 | refused (400): Query has no data source |
| [5] Should truncate time #33 | refused (400): Query has no data source |
| [5] Should truncate time #34 | refused (400): Query has no data source |
| [5] Should truncate time #35 | refused (400): Query has no data source |
| [5] Should truncate time #36 | refused (400): Query has no data source |
| [5] Should truncate time #37 | refused (400): Query has no data source |
| [5] Should truncate time #38 | refused (400): Query has no data source |
| [5] Should truncate time #39 | refused (400): Query has no data source |
| [5] Should truncate time #40 | refused (400): Query has no data source |
| [5] Should truncate time #41 | refused (400): Query has no data source |
| [5] Should truncate time #42 | refused (400): Query has no data source |
| [5] Should truncate time #43 | refused (400): Query has no data source |
| [5] Should truncate time #44 | refused (400): Query has no data source |
| [5] Should truncate time #45 | refused (400): Query has no data source |
| [5] Should truncate time #46 | refused (400): Query has no data source |
| [5] Should truncate time #47 | refused (400): Query has no data source |
| [5] Should truncate time #48 | refused (400): Query has no data source |

</details>

<details><summary>TypeConversion1 - To Boolean (2)</summary>

| Case | Reason |
| --- | --- |
| [2] `toBoolean()` on valid literal string | refused (400): Query has no data source |
| [4] `toBoolean()` on invalid strings | rows differ: got [[None], [None], [True]] |

</details>

<details><summary>TypeConversion2 - To Integer (6)</summary>

| Case | Reason |
| --- | --- |
| [1] `toInteger()` on float | refused (400): Pipeline segment has no data source |
| [2] `toInteger()` returning null on non-numerical string | refused (400): Pipeline segment has no data source |
| [3] `toInteger()` handling mixed number types | refused (400): Pipeline segment has no data source |
| [4] `toInteger()` handling Any type | refused (400): Pipeline segment has no data source |
| [5] `toInteger()` on a list of strings | refused (400): Pipeline segment has no data source |
| [6] `toInteger()` on a complex-typed expression | refused (400): Query has no data source |

</details>

<details><summary>TypeConversion3 - To Float (4)</summary>

| Case | Reason |
| --- | --- |
| [1] `toFloat()` on mixed number types | refused (400): Pipeline segment has no data source |
| [2] `toFloat()` returning null on non-numerical string | refused (400): Pipeline segment has no data source |
| [3] `toFloat()` handling Any type | refused (400): Pipeline segment has no data source |
| [4] `toFloat()` on a list of strings | refused (400): Pipeline segment has no data source |

</details>

<details><summary>TypeConversion4 - To String (5)</summary>

| Case | Reason |
| --- | --- |
| [1] `toString()` handling integer literal | refused (400): Query has no data source |
| [2] `toString()` handling boolean literal | refused (400): Query has no data source |
| [3] `toString()` handling inlined boolean | refused (400): Query has no data source |
| [5] `toString()` should work on Any type | refused (400): Query has no data source |
| [6] `toString()` on a list of integers | refused (400): Pipeline segment has no data source |

</details>

<!-- conformance-results:end -->

## Deliberate differences

### Writes

`CREATE`, `MERGE`, `SET`, and `DELETE` are supported where the write maps onto the model through governed table writes. A write needing structure the model lacks is refused by name.

### Vocabulary fixed by the model

Dynamic labels and relationship types (e.g. `MATCH (n:$label)`) are not supported. Labels and relationship types are fixed by the model and must be declared there. This is a structural characteristic, not a limitation to be worked around.

### Add-ins

The APOC calls Cypher tools and agent frameworks make are offered as commands and functions: `apoc.meta.data`, `apoc.meta.schema` and `apoc.schema.nodes` answer from the role's graph, the common `apoc.text`, `apoc.coll`, `apoc.map`, `apoc.date` and `apoc.convert` functions push down with the query, and `apoc.path.expand` and `apoc.path.subgraphNodes` walk the graph as variable-length patterns. See [The APOC set Provisa ships](../commands.md#the-apoc-set-provisa-ships). Procedures that act outside the model (`apoc.create.*`, `apoc.periodic.iterate`, `apoc.load.*`) are not offered; for those, run them on your own Neo4j instance against data it reads from Provisa over Bolt. GDS algorithms are not available inline.

### Scenarios that build their own graph

TCK scenarios that create their graph (`CREATE` in their setup, a named TCK graph, a test procedure) are not runnable: a Provisa graph's labels and relationship types are its model's. The pass rate counts the scenarios that run.

## Tools

| Tool | Version | Date | Method |
| --- | --- | --- | --- |
| Neo4j Browser | TBD | TBD | Covered by driver (Neo4j JavaScript driver) |
| Neo4j Bloom | TBD | TBD | Covered by driver (Neo4j JavaScript driver) |
| neo4j-driver (Python) | TBD | TBD | Covered by driver |
| neo4j-driver (Java) | TBD | TBD | Covered by driver |
