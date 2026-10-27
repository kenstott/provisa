# Cypher Conformance

**Suite:** openCypher TCK 2024.3.

**Pass rate:** 45.2% (1316 of 2912); 968 cases not runnable by design.

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

<details><summary>Match1 - Match nodes (14)</summary>

| Case | Reason |
| --- | --- |
| [1] Match non-existent nodes returns empty | 930 row(s), expected 0 |
| [9] Fail when a relationship has the same variable in the same pattern #1 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #2 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #3 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #4 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #11 | expected SyntaxError; the query ran |
| [9] Fail when a relationship has the same variable in the same pattern #12 | expected SyntaxError; the query ran |
| [11] Fail when matching a node variable bound to a value #1 | expected SyntaxError; the query ran |
| [11] Fail when matching a node variable bound to a value #2 | expected SyntaxError; the query ran |
| [11] Fail when matching a node variable bound to a value #3 | expected SyntaxError; the query ran |
| [11] Fail when matching a node variable bound to a value #4 | expected SyntaxError; the query ran |
| [11] Fail when matching a node variable bound to a value #6 | expected SyntaxError; the query ran |
| [11] Fail when matching a node variable bound to a value #7 | expected SyntaxError; the query ran |
| [11] Fail when matching a node variable bound to a value #8 | expected SyntaxError; the query ran |

</details>

<details><summary>Match2 - Match relationships (16)</summary>

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
| [13] Fail when matching a relationship variable bound to a value #1 | expected SyntaxError; the query ran |
| [13] Fail when matching a relationship variable bound to a value #2 | expected SyntaxError; the query ran |
| [13] Fail when matching a relationship variable bound to a value #3 | expected SyntaxError; the query ran |
| [13] Fail when matching a relationship variable bound to a value #4 | expected SyntaxError; the query ran |
| [13] Fail when matching a relationship variable bound to a value #5 | expected SyntaxError; the query ran |
| [13] Fail when matching a relationship variable bound to a value #6 | expected SyntaxError; the query ran |
| [13] Fail when matching a relationship variable bound to a value #7 | expected SyntaxError; the query ran |
| [13] Fail when matching a relationship variable bound to a value #8 | expected SyntaxError; the query ran |

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

<details><summary>Match6 - Match named paths scenarios (17)</summary>

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
| [24] Fail when a relationship has the same variable in the same pattern #3 | expected SyntaxError; the query ran |
| [25] Fail when matching a path variable bound to a value #1 | expected SyntaxError; the query ran |
| [25] Fail when matching a path variable bound to a value #2 | expected SyntaxError; the query ran |
| [25] Fail when matching a path variable bound to a value #3 | expected SyntaxError; the query ran |
| [25] Fail when matching a path variable bound to a value #4 | expected SyntaxError; the query ran |
| [25] Fail when matching a path variable bound to a value #5 | expected SyntaxError; the query ran |
| [25] Fail when matching a path variable bound to a value #6 | expected SyntaxError; the query ran |
| [25] Fail when matching a path variable bound to a value #7 | expected SyntaxError; the query ran |
| [25] Fail when matching a path variable bound to a value #8 | expected SyntaxError; the query ran |

</details>

<details><summary>Match7 - Optional match (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Simple OPTIONAL MATCH on empty graph | 2111 row(s), expected 1 |
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

<details><summary>Return2 - Return single expression (correctly projecting an expression) (1)</summary>

| Case | Reason |
| --- | --- |
| [10] Return count aggregation over an empty graph | columns ['(count(a) > 0)'] != ['count(a) > 0'] |

</details>

<details><summary>Return4 - Column renaming (1)</summary>

| Case | Reason |
| --- | --- |
| [10] Fail when returning multiple columns with same name | expected SyntaxError; the query ran |

</details>

<details><summary>Return6 - Implicit grouping with aggregates (5)</summary>

| Case | Reason |
| --- | --- |
| [6] Handle aggregates inside non-aggregate expressions | refused (400): There is no relationship type :FATHER. Registered types: :CLASS_TERM, :VIA_TABLE, :TERM_TAGS. |
| [15] Using `rand()` in aggregations | expected SyntaxError; the query ran |
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
| [1] ORDER BY of a column introduced in RETURN should return salient results in ascending order | 1 row(s), expected 2 |

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

<details><summary>Union1 - Union (1)</summary>

| Case | Reason |
| --- | --- |
| [5] Failing when UNION has different columns | expected SyntaxError; the query ran |

</details>

<details><summary>Union2 - Union All (1)</summary>

| Case | Reason |
| --- | --- |
| [5] Failing when UNION ALL has different columns | expected SyntaxError; the query ran |

</details>

<details><summary>Union3 - Union in combination with Union All (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Failing when mixing UNION and UNION ALL | expected SyntaxError; the query ran |
| [2] Failing when mixing UNION ALL and UNION | expected SyntaxError; the query ran |

</details>

<details><summary>Unwind1 (8)</summary>

| Case | Reason |
| --- | --- |
| [2] Unwinding a range | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT _uw0."x" FROM UNNEST(SEQUENCE(1, 3)) AS _uw0(x)
                                    ^
HINT:  No function matches the |
| [3] Unwinding a concatenation of lists | refused (500): Execution failed: operator does not exist: integer[] + integer[]
LINE 1: ... _uw0."x" FROM _w0 CROSS JOIN UNNEST((_w0."first" + _w0."sec...
                                                             |
| [4] Unwinding a collected unwound expression | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: ...(SELECT ARRAY_AGG(_uw0."row") AS rows FROM UNNEST(SEQUENCE(1...
                                                         |
| [7] Double unwinding a list of lists | refused (500): Execution failed: function unnest(integer) does not exist
LINE 1: ... _w0 CROSS JOIN UNNEST(lol) AS _uw0(x) CROSS JOIN UNNEST(x) ...
                                                             ^
HINT |
| [8] Unwinding the empty list | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT _uw0."empty" FROM UNNEST(ARRAY[]) AS _uw0(empty)
                                        ^
HINT:  Explicitly cast to the desired t |
| [9] Unwinding null | refused (500): Execution failed: function unnest(unknown) is not unique
LINE 1: SELECT _uw0."nil" FROM UNNEST(NULL) AS _uw0(nil)
                               ^
HINT:  Could not choose a best candidate function. Yo |
| [13] Multiple unwinds after each other | columns ['xs', 'ys', 'zs', 'x', 'y', 'z'] != ['x', 'xs', 'y', 'ys', 'z', 'zs'] |
| [14] Unwind with merge | refused (400): Write clause 'MERGE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>With1 - Forward single variable (1)</summary>

| Case | Reason |
| --- | --- |
| [5] Forwarding null | refused (400): There is no label :Start. Registered labels: :DataProducts, :StaleMetadata, :Meta. |

</details>

<details><summary>With2 - Forward single expression (1)</summary>

| Case | Reason |
| --- | --- |
| [2] Forwarding a nested map literal | columns ['name2'] != ['nestedMap.name.name2'] |

</details>

<details><summary>With4 - Variable aliasing (1)</summary>

| Case | Reason |
| --- | --- |
| [7] Multiple aliasing and backreferencing | refused (400): Write clause 'CREATE' is not supported. Provisa Cypher executes CREATE, DELETE, and SET as direct table writes (governed like any other write); MERGE, DETACH DELETE, and REMOVE are not supported. |

</details>

<details><summary>With6 - Implicit grouping with aggregates (3)</summary>

| Case | Reason |
| --- | --- |
| [5] Handle constants and parameters inside an expression which contains an aggregation expression | refused (500): Execution failed: function avg(text) does not exist
LINE 1: WITH _w0 AS (SELECT $1 + AVG(person."age") - 1000 AS agg FRO...
                                 ^
HINT:  No function matches the given name |
| [6] Handle projected variables inside an expression which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |
| [7] Handle projected property accesses inside an expression which contains an aggregation expression | refused (400): Expected 'LPAREN', got 'MINUS' ('-') at pos 19 |

</details>

<details><summary>WithOrderBy1 - Order by a single variable (22)</summary>

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
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #1 | refused (500): Execution failed: column _w1.numOfValues does not exist
LINE 1: ...x < _uw0."value")) AS x, _uw0."value" AS "value", "_w1"."num...
                                                             ^
HINT:  |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #2 | refused (500): Execution failed: column _w1.numOfValues does not exist
LINE 1: ...x < _uw0."value")) AS x, _uw0."value" AS "value", "_w1"."num...
                                                             ^
HINT:  |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #3 | refused (500): Execution failed: column _w1.numOfValues does not exist
LINE 1: ...x < _uw0."value")) AS x, _uw0."value" AS "value", "_w1"."num...
                                                             ^
HINT:  |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #4 | refused (500): Execution failed: column _w1.numOfValues does not exist
LINE 1: ...x < _uw0."value")) AS x, _uw0."value" AS "value", "_w1"."num...
                                                             ^
HINT:  |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #5 | refused (500): Execution failed: cannot determine type of empty array
LINE 1: ...ECT ARRAY[ARRAY[2, 2], ARRAY[2, -2], ARRAY[1, 2], ARRAY[], A...
                                                             ^
HINT:   |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #6 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT [CAST(MAP(['year', 'month', 'day'], [TO_JSON(1910), TO_JSON...
    |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #7 | rows differ: got [[False]] |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #8 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 146.
  'minute', 'timezone'], [TO_JSON(10), TO_JSON(35), TO_JSON('-08:00')]) AS TIME |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #9 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT [LOCALDATETIME(MAP(['year', 'month', 'day', 'hou |
| [45] Sort order should be consistent with comparisons where comparisons are defined #Example: <exampleName> #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |

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
| [1] Distinct on unbound node | rows differ: got [[2375]] |
| [3] Collect distinct nulls | rows differ: got [[[None]]] |
| [4] Collect distinct values mixed with nulls | rows differ: got [[[1, None]]] |

</details>

<details><summary>Boolean1 - And logical operations (4)</summary>

| Case | Reason |
| --- | --- |
| [5] Conjunction is commutative on null | rows differ: got [[True, None, True], [False, None, False], [None, True, True]] |
| [7] Conjunction is associative on null | rows differ: got [[True, None, True, True], [False, None, True, False], [None, True, True, True]] |
| [8] Fail on conjunction of at least one non-booleans #17 | expected SyntaxError; the query ran |
| [8] Fail on conjunction of at least one non-booleans #18 | expected SyntaxError; the query ran |

</details>

<details><summary>Boolean2 - OR logical operations (2)</summary>

| Case | Reason |
| --- | --- |
| [5] Disjunction is commutative on null | rows differ: got [[True, None, False], [False, None, True], [None, True, False]] |
| [7] Disjunction is associative on null | rows differ: got [[True, None, True, False], [False, None, True, False], [None, True, True, False]] |

</details>

<details><summary>Boolean3 - XOR logical operations (2)</summary>

| Case | Reason |
| --- | --- |
| [2] Exclusive disjunction of three truth values | rows differ: got [[True, False, None, True, True, True, None, None, None, True, True, None, True, False, None, True, None, None, True, None, None, None, None, None, None, None, None]] |
| [3] Exclusive disjunction of many truth values | runner: timed out |

</details>

<details><summary>Boolean4 - NOT logical operations (2)</summary>

| Case | Reason |
| --- | --- |
| [4] Fail when using NOT on a non-boolean literal #6 | expected SyntaxError; the query ran |
| [4] Fail when using NOT on a non-boolean literal #7 | expected SyntaxError; the query ran |

</details>

<details><summary>Boolean5 - Interop of logical operations (3)</summary>

| Case | Reason |
| --- | --- |
| [2] Disjunction is distributive over conjunction on null | rows differ: got [[True, None, True, False], [False, None, True, True], [None, True, True, False]] |
| [4] Conjunction is distributive over disjunction on null | rows differ: got [[True, None, True, False], [False, None, True, False], [None, True, True, True]] |
| [6] Conjunction is not distributive over exclusive disjunction on null | rows differ: got [[True, None, True, True], [False, None, True, False], [None, True, True, True]] |

</details>

<details><summary>Comparison1 - Equality (16)</summary>

| Case | Reason |
| --- | --- |
| [6] Comparing lists to lists #2 | refused (500): Execution failed: operator does not exist: text[] = integer[]
LINE 1: SELECT ARRAY[NULL] = ARRAY[1] AS result
                           ^
HINT:  No operator matches the given name and argument types. |
| [6] Comparing lists to lists #3 | refused (500): Execution failed: operator does not exist: text[] = integer[]
LINE 1: SELECT ARRAY['a'] = ARRAY[1] AS result
                          ^
HINT:  No operator matches the given name and argument types. Y |
| [6] Comparing lists to lists #4 | refused (500): Execution failed: ARRAY could not convert type text[] to integer[]
LINE 1: SELECT ARRAY[ARRAY[1]] = ARRAY[ARRAY[1], ARRAY[NULL]] AS res...
                                                 ^ |
| [6] Comparing lists to lists #5 | refused (500): Execution failed: ARRAY could not convert type text[] to integer[]
LINE 1: ...ELECT ARRAY[ARRAY[1], ARRAY[2]] = ARRAY[ARRAY[1], ARRAY[NULL...
                                                           |
| [6] Comparing lists to lists #6 | refused (500): Execution failed: ARRAY could not convert type text[] to integer[]
LINE 1: ...CT ARRAY[ARRAY[1], ARRAY[2, 3]] = ARRAY[ARRAY[1], ARRAY[NULL...
                                                           |
| [7] Comparing maps to maps #12 | rows differ: got [[True]] |
| [7] Comparing maps to maps #13 | rows differ: got [[False]] |
| [7] Comparing maps to maps #14 | rows differ: got [[False]] |
| [7] Comparing maps to maps #15 | rows differ: got [[False]] |
| [7] Comparing maps to maps #16 | rows differ: got [[False]] |
| [8] Equality and inequality of NaN #1 | refused (500): Execution failed: division by zero |
| [8] Equality and inequality of NaN #2 | refused (500): Execution failed: division by zero |
| [8] Equality and inequality of NaN #3 | refused (500): Execution failed: division by zero |
| [8] Equality and inequality of NaN #4 | refused (500): Execution failed: invalid input syntax for type double precision: "a"
LINE 1: SELECT CAST(0.0 AS DOUBLE PRECISION) / 0.0 = 'a' AS isEqual,...
                                                     ^ |
| [9] Equality between strings and numbers #3 | rows differ: got [[True]] |
| [9] Equality between strings and numbers #4 | rows differ: got [[True]] |

</details>

<details><summary>Comparison2 - Half-bounded Range (8)</summary>

| Case | Reason |
| --- | --- |
| [4] Comparing lists #3 | rows differ: got [[False]] |
| [4] Comparing lists #4 | refused (500): Execution failed: invalid input syntax for type integer: "a"
LINE 1: SELECT ARRAY[1, 'a'] >= ARRAY[1, NULL] AS result
                        ^ |
| [5] Comparing NaN #1 | refused (500): Execution failed: division by zero |
| [5] Comparing NaN #2 | refused (500): Execution failed: division by zero |
| [5] Comparing NaN #3 | refused (500): Execution failed: division by zero |
| [5] Comparing NaN #4 | refused (500): Execution failed: invalid input syntax for type double precision: "a"
LINE 1: SELECT CAST(0.0 AS DOUBLE PRECISION) / 0.0 > 'a' AS gt, CAST...
                                                     ^ |
| [6] Comparability between numbers and strings #3 | rows differ: got [[False]] |
| [6] Comparability between numbers and strings #4 | rows differ: got [[False]] |

</details>

<details><summary>Conditional2 - Case Expression (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Simple cases over integers #10 | refused (500): Execution failed: operator does not exist: text = integer
LINE 1: SELECT CASE '0' WHEN -10 THEN 'minus ten' WHEN 0 THEN 'zero'...
                        ^
HINT:  No operator matches the given name an |
| [1] Simple cases over integers #11 | refused (500): Execution failed: operator does not exist: boolean = integer
LINE 1: SELECT CASE TRUE WHEN -10 THEN 'minus ten' WHEN 0 THEN 'zero...
                         ^
HINT:  No operator matches the given nam |

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
| [4] `properties()` on a map | refused (500): Internal server error |

</details>

<details><summary>List1 - Dynamic Element Access (6)</summary>

| Case | Reason |
| --- | --- |
| [2] Indexing into nested literal lists | columns ['list_extract(list_extract(main.list_value(main.list_value(1)), 1), 1)'] != ['[[1]][0][0]'] |
| [3] Use list lookup based on parameters when there is no type information | rows differ: got [[None]] |
| [4] Use list lookup based on parameters when there is lhs type information | rows differ: got [[None]] |
| [5] Use list lookup based on parameters when there is rhs type information | rows differ: got [[None]] |
| [6] Fail when indexing a non-list #Example: <exampleName> #4 | expected TypeError; the query ran |
| [7] Fail when indexing a non-list given by a parameter #Example: <exampleName> #4 | expected TypeError; the query ran |

</details>

<details><summary>List11 - Create a list from a range (41)</summary>

| Case | Reason |
| --- | --- |
| [1] Create list from `range()` with default step #1 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-1236, -1234) AS list
               ^
HINT:  No function matches the given name and argument types. You mi |
| [1] Create list from `range()` with default step #2 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-1234, -1234) AS list
               ^
HINT:  No function matches the given name and argument types. You mi |
| [1] Create list from `range()` with default step #3 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-10, -3) AS list
               ^
HINT:  No function matches the given name and argument types. You might n |
| [1] Create list from `range()` with default step #4 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-10, 0) AS list
               ^
HINT:  No function matches the given name and argument types. You might ne |
| [1] Create list from `range()` with default step #5 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-1, 0) AS list
               ^
HINT:  No function matches the given name and argument types. You might nee |
| [1] Create list from `range()` with default step #6 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, -123) AS list
               ^
HINT:  No function matches the given name and argument types. You might n |
| [1] Create list from `range()` with default step #7 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, -1) AS list
               ^
HINT:  No function matches the given name and argument types. You might nee |
| [1] Create list from `range()` with default step #8 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-1, 1) AS list
               ^
HINT:  No function matches the given name and argument types. You might nee |
| [1] Create list from `range()` with default step #9 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 0) AS list
               ^
HINT:  No function matches the given name and argument types. You might need |
| [1] Create list from `range()` with default step #10 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 1) AS list
               ^
HINT:  No function matches the given name and argument types. You might need |
| [1] Create list from `range()` with default step #11 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 10) AS list
               ^
HINT:  No function matches the given name and argument types. You might nee |
| [1] Create list from `range()` with default step #12 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(6, 10) AS list
               ^
HINT:  No function matches the given name and argument types. You might nee |
| [1] Create list from `range()` with default step #13 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(1234, 1234) AS list
               ^
HINT:  No function matches the given name and argument types. You migh |
| [1] Create list from `range()` with default step #14 | refused (500): Execution failed: function sequence(integer, integer) does not exist
LINE 1: SELECT SEQUENCE(1234, 1236) AS list
               ^
HINT:  No function matches the given name and argument types. You migh |
| [2] Create list from `range()` with explicitly given step #1 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(1381, -3412, -1298) AS list
               ^
HINT:  No function matches the given name and argumen |
| [2] Create list from `range()` with explicitly given step #2 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, -2000, -1298) AS list
               ^
HINT:  No function matches the given name and argument t |
| [2] Create list from `range()` with explicitly given step #3 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(10, -10, -3) AS list
               ^
HINT:  No function matches the given name and argument types |
| [2] Create list from `range()` with explicitly given step #4 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, -10, -3) AS list
               ^
HINT:  No function matches the given name and argument types. |
| [2] Create list from `range()` with explicitly given step #5 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, -20, -2) AS list
               ^
HINT:  No function matches the given name and argument types. |
| [2] Create list from `range()` with explicitly given step #6 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, -10, -1) AS list
               ^
HINT:  No function matches the given name and argument types. |
| [2] Create list from `range()` with explicitly given step #7 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, -1, -1) AS list
               ^
HINT:  No function matches the given name and argument types.  |
| [2] Create list from `range()` with explicitly given step #8 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-1236, -1234, 1) AS list
               ^
HINT:  No function matches the given name and argument t |
| [2] Create list from `range()` with explicitly given step #9 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-10, 0, 1) AS list
               ^
HINT:  No function matches the given name and argument types.  |
| [2] Create list from `range()` with explicitly given step #10 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-1, 0, 1) AS list
               ^
HINT:  No function matches the given name and argument types. Y |
| [2] Create list from `range()` with explicitly given step #11 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 1, -123) AS list
               ^
HINT:  No function matches the given name and argument types. |
| [2] Create list from `range()` with explicitly given step #12 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 1, -1) AS list
               ^
HINT:  No function matches the given name and argument types. Y |
| [2] Create list from `range()` with explicitly given step #13 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, -123, 1) AS list
               ^
HINT:  No function matches the given name and argument types. |
| [2] Create list from `range()` with explicitly given step #14 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, -1, 1) AS list
               ^
HINT:  No function matches the given name and argument types. Y |
| [2] Create list from `range()` with explicitly given step #15 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 0, -1) AS list
               ^
HINT:  No function matches the given name and argument types. Y |
| [2] Create list from `range()` with explicitly given step #16 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 0, 1) AS list
               ^
HINT:  No function matches the given name and argument types. Yo |
| [2] Create list from `range()` with explicitly given step #17 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 1, 2) AS list
               ^
HINT:  No function matches the given name and argument types. Yo |
| [2] Create list from `range()` with explicitly given step #18 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 1, 1) AS list
               ^
HINT:  No function matches the given name and argument types. Yo |
| [2] Create list from `range()` with explicitly given step #19 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(0, 10, 1) AS list
               ^
HINT:  No function matches the given name and argument types. Y |
| [2] Create list from `range()` with explicitly given step #20 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(6, 10, 1) AS list
               ^
HINT:  No function matches the given name and argument types. Y |
| [2] Create list from `range()` with explicitly given step #21 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(1234, 1234, 1) AS list
               ^
HINT:  No function matches the given name and argument typ |
| [2] Create list from `range()` with explicitly given step #22 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(1234, 1236, 1) AS list
               ^
HINT:  No function matches the given name and argument typ |
| [2] Create list from `range()` with explicitly given step #23 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-10, 0, 3) AS list
               ^
HINT:  No function matches the given name and argument types.  |
| [2] Create list from `range()` with explicitly given step #24 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-10, 10, 3) AS list
               ^
HINT:  No function matches the given name and argument types. |
| [2] Create list from `range()` with explicitly given step #25 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-2000, 0, 1298) AS list
               ^
HINT:  No function matches the given name and argument ty |
| [2] Create list from `range()` with explicitly given step #26 | refused (500): Execution failed: function sequence(integer, integer, integer) does not exist
LINE 1: SELECT SEQUENCE(-3412, 1381, 1298) AS list
               ^
HINT:  No function matches the given name and argument |
| [3] Create an empty list if range direction and step direction are inconsistent | refused (400): The engine refused the query: Catalog Error: Scalar Function with name size does not exist!
Did you mean "sin"?

LINE 1: ... _w1) SELECT COALESCE(LIST_BOOL_AND(LIST_TRANSFORM(ARRAY_AGG((SIZE(_w2."list |

</details>

<details><summary>List12 - List Comprehension (2)</summary>

| Case | Reason |
| --- | --- |
| [3] Size of list comprehension | refused (400): The engine refused the query: Catalog Error: Scalar Function with name collect does not exist!
Did you mean "col_description"?

LINE 1: SELECT LENGTH(ARRAY(SELECT x FROM UNNEST(COLLECT(r)) AS _t0(x) W |
| [7] Fail when using aggregation in list comprehension | expected SyntaxError; the query ran |

</details>

<details><summary>List2 - List Slicing (1)</summary>

| Case | Reason |
| --- | --- |
| [6] List slice with negative range | rows differ: got [[[2, 3]]] |

</details>

<details><summary>List3 - List Equality (6)</summary>

| Case | Reason |
| --- | --- |
| [1] Equality between list and literal should return false | refused (500): Execution failed: malformed array literal: "foo"
LINE 1: SELECT ARRAY[1, 2] = 'foo' AS res
                             ^
DETAIL:  Array value must start with "{" or dimension information. |
| [3] Equality between different lists with null should return false | refused (500): Execution failed: operator does not exist: integer[] = text[]
LINE 1: SELECT ARRAY[1, 2] = ARRAY[NULL, 'foo'] AS res
                           ^
HINT:  No operator matches the given name and argument |
| [4] Equality between almost equal lists with null should return null | rows differ: got [[False]] |
| [5] Equality of nested lists of different length should return false despite nulls | refused (500): Execution failed: ARRAY could not convert type text[] to integer[]
LINE 1: SELECT ARRAY[ARRAY[1]] = ARRAY[ARRAY[1], ARRAY[NULL]] AS res
                                                 ^ |
| [6] Equality between different nested lists with null should return false | refused (500): Execution failed: ARRAY could not convert type text[] to integer[]
LINE 1: ...AY[ARRAY[1, 2], ARRAY[1, 3]] = ARRAY[ARRAY[1, 2], ARRAY[NULL...
                                                           |
| [7] Equality between almost equal nested lists with null should return null | refused (500): Execution failed: ARRAY could not convert type text[] to integer[]
LINE 1: SELECT ARRAY[ARRAY[1, 2], ARRAY['foo', 'bar']] = ARRAY[ARRAY...
                                  ^ |

</details>

<details><summary>List4 - List Concatenation (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Concatenating lists of same type | refused (500): Execution failed: operator does not exist: integer[] + integer[]
LINE 1: SELECT ARRAY[1, 10, 100] + ARRAY[4, 5] AS foo
                                 ^
HINT:  No operator matches the given name and  |
| [2] Concatenating a list with a scalar of same type | refused (500): Execution failed: operator does not exist: boolean[] + boolean
LINE 1: SELECT ARRAY[FALSE, TRUE] + FALSE AS foo
                                  ^
HINT:  No operator matches the given name and argume |

</details>

<details><summary>List5 - List Membership Validation - IN Operator (31)</summary>

| Case | Reason |
| --- | --- |
| [5] IN should return false when matching a number with a string | rows differ: got [[True]] |
| [6] IN should return false when matching a number with a string - list version | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1, 2] IN (1, ARRAY[1, '2']) AS res
                           ^
HINT:  No operator matches the given name and argume |
| [7] IN should return false when types of LHS and RHS don't match - singleton list | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1] IN (1, 2) AS res
                        ^
HINT:  No operator matches the given name and argument types. You migh |
| [8] IN should return false when types of LHS and RHS don't match - list | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1, 2] IN (1, 2) AS res
                           ^
HINT:  No operator matches the given name and argument types. Yo |
| [9] IN should return true when types of LHS and RHS match - singleton list | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1] IN (1, 2, ARRAY[1]) AS res
                        ^
HINT:  No operator matches the given name and argument types |
| [10] IN should return true when types of LHS and RHS match - list | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1, 2] IN (1, ARRAY[1, 2]) AS res
                           ^
HINT:  No operator matches the given name and argument |
| [11] IN should return false when order of elements in LHS list and RHS list don't match | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1, 2] IN (1, ARRAY[2, 1]) AS res
                           ^
HINT:  No operator matches the given name and argument |
| [12] IN with different length lists should return false | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1, 2] IN (1, ARRAY[1, 2, 3]) AS res
                           ^
HINT:  No operator matches the given name and argum |
| [13] IN should return false when matching a list with a nested list with same elements | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1, 2] IN (1, ARRAY[ARRAY[1, 2]]) AS res
                           ^
HINT:  No operator matches the given name and a |
| [14] IN should return true when both LHS and RHS contain nested lists | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]] IN (5, ARRAY[ARRAY[1,...
                                               ^
HINT:  No operat |
| [15] IN should return true when both LHS and RHS contain a nested list alongside a scalar element | refused (500): Execution failed: ARRAY types integer[] and integer cannot be matched
LINE 1: SELECT ARRAY[ARRAY[1, 2], 3] IN (1, ARRAY[ARRAY[1, 2], 3]) A...
                                  ^ |
| [16] IN should return true when LHS and RHS contain a nested list - singleton version | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[ARRAY[1]] IN (2, ARRAY[ARRAY[1]]) AS res
                               ^
HINT:  No operator matches the given name  |
| [17] IN should return true when LHS and RHS contain a nested list | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[ARRAY[1, 3]] IN (2, ARRAY[ARRAY[1, 3]]) AS res
                                  ^
HINT:  No operator matches the gi |
| [18] IN should return false when LHS contains a nested list and type mismatch on RHS - singleton version | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[ARRAY[1]] IN (2, ARRAY[1]) AS res
                               ^
HINT:  No operator matches the given name and arg |
| [19] IN should return false when LHS contains a nested list and type mismatch on RHS | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[ARRAY[1, 3]] IN (2, ARRAY[1, 3]) AS res
                                  ^
HINT:  No operator matches the given nam |
| [21] IN should return null if LHS and RHS are null - list version | rows differ: got [[True]] |
| [26] IN should return true if correct list found despite other lists having nulls | refused (500): Execution failed: operator does not exist: integer[] = text[]
LINE 1: SELECT ARRAY[1, 2] IN (ARRAY[NULL, 'foo'], ARRAY[1, 2]) AS r...
                           ^
HINT:  No operator matches the given  |
| [27] IN should return true if correct list found despite null being another element within containing list | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1, 2] IN (1, ARRAY[1, 2], NULL) AS res
                           ^
HINT:  No operator matches the given name and ar |
| [28] IN should return false if no match can be found, despite nulls | refused (500): Execution failed: operator does not exist: integer[] = text[]
LINE 1: SELECT ARRAY[1, 2] IN (ARRAY[NULL, 'foo']) AS res
                           ^
HINT:  No operator matches the given name and argum |
| [29] IN should return null if comparison with null is required, list version | rows differ: got [[False]] |
| [30] IN should return false if different length lists compared, even if the extra element is null | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1, 2] IN (1, ARRAY[1, 2, NULL]) AS res
                           ^
HINT:  No operator matches the given name and ar |
| [31] IN should return null when comparing two so-called identical lists where one element is null | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[1, 2, NULL] IN (1, ARRAY[1, 2, NULL]) AS res
                                 ^
HINT:  No operator matches the given |
| [33] IN should return false if different length lists with nested elements compared, even if the extra element is null | refused (500): Execution failed: operator does not exist: integer[] = integer
LINE 1: SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]] IN (5, ARRAY[ARRAY[1,...
                                               ^
HINT:  No operat |
| [34] IN should return null if comparison with null is required, list version 2 | rows differ: got [[False]] |
| [35] IN should work with an empty list | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT ARRAY[] IN (ARRAY[]) AS res
               ^
HINT:  Explicitly cast to the desired type, for example ARRAY[]::integer[]. |
| [36] IN should return false for the empty list if the LHS and RHS types differ | refused (500): Execution failed: syntax error at or near ")"
LINE 1: SELECT ARRAY[] IN () AS res
                           ^ |
| [37] IN should work with an empty list in the presence of other list elements: matching | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT ARRAY[] IN (1, ARRAY[]) AS res
               ^
HINT:  Explicitly cast to the desired type, for example ARRAY[]::integer[]. |
| [38] IN should work with an empty list in the presence of other list elements: not matching | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT ARRAY[] IN (1, 2) AS res
               ^
HINT:  Explicitly cast to the desired type, for example ARRAY[]::integer[]. |
| [39] IN should work with an empty list when comparing nested lists | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT ARRAY[ARRAY[]] IN (1, ARRAY[ARRAY[]]) AS res
                     ^
HINT:  Explicitly cast to the desired type, for example ARRAY[ |
| [40] IN should return null if comparison with null is required for empty list | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT ARRAY[] IN (1, 2, NULL) AS res
               ^
HINT:  Explicitly cast to the desired type, for example ARRAY[]::integer[]. |
| [41] IN should return true when LHS and RHS contain nested list with multiple empty lists | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT ARRAY[ARRAY[], ARRAY[]] IN (1, ARRAY[ARRAY[], ARRAY[]...
                     ^
HINT:  Explicitly cast to the desired type, for ex |

</details>

<details><summary>List6 - List size (2)</summary>

| Case | Reason |
| --- | --- |
| [3] Concatenating and returning the size of literal lists | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY[ARRAY[], ARRAY[]] + ARRAY[ARRAY[]])...
                                 ^
HINT:  Explicitly cast to the desired  |
| [4] `size()` on null list | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: WITH _w0 AS (SELECT NULL AS l) SELECT CARDINALITY(_w0."l"), ...
                                              ^
HINT:  No function m |

</details>

<details><summary>Literals2 - Decimal integer (2)</summary>

| Case | Reason |
| --- | --- |
| [9] Fail on a too large integer | expected SyntaxError; the query ran |
| [10] Fail on a too small integer | expected SyntaxError; the query ran |

</details>

<details><summary>Literals3 - Hexadecimal integer (2)</summary>

| Case | Reason |
| --- | --- |
| [16] Fail on a too large hexadecimal integer | expected SyntaxError; the query ran |
| [17] Fail on a too small hexadecimal integer | expected SyntaxError; the query ran |

</details>

<details><summary>Literals4 - Octal integer (2)</summary>

| Case | Reason |
| --- | --- |
| [9] Fail on a too large octal integer | expected SyntaxError; the query ran |
| [10] Fail on a too small octal integer | expected SyntaxError; the query ran |

</details>

<details><summary>Literals5 - Float (2)</summary>

| Case | Reason |
| --- | --- |
| [6] Return a very long positive float without integer digits | expected value unreadable: could not convert string to float: '1e' |
| [12] Return a very long negative float without integer digits | expected value unreadable: could not convert string to float: '-1e' |

</details>

<details><summary>Literals6 - String (3)</summary>

| Case | Reason |
| --- | --- |
| [5] Return a single-quoted string with escaped characters | rows differ: got [['a\\bcn5t\'"\\//\\"\'']] |
| [10] Accept valid Unicode literal | rows differ: got [['u01FF']] |
| [13] Failing on incorrect unicode literal | expected SyntaxError; the query ran |

</details>

<details><summary>Literals7 - List (7)</summary>

| Case | Reason |
| --- | --- |
| [1] Return an empty list | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT ARRAY[] AS literal
               ^
HINT:  Explicitly cast to the desired type, for example ARRAY[]::integer[]. |
| [9] Return a list containing an empty lists | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT ARRAY[ARRAY[]] AS literal
                     ^
HINT:  Explicitly cast to the desired type, for example ARRAY[]::integer[]. |
| [10] Return seven-deep nested empty lists | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT ARRAY[ARRAY[ARRAY[ARRAY[ARRAY[ARRAY[ARRAY[]]]]]]] AS ...
                                                   ^
HINT:  Explicitly ca |
| [11] Return 20-deep nested empty lists | runner: timed out |
| [12] Return 40-deep nested empty lists | runner: timed out |
| [16] Return a list containing multiple mixed values | refused (500): Execution failed: ARRAY types numeric and boolean cannot be matched
LINE 1: SELECT ARRAY[0.2, ', as#?lßdj ', NULL, 71034856, FALSE] AS l...
                                                         ^ |
| [17] Return a list containing real and fake nested lists | refused (500): Execution failed: malformed array literal: " [ a "
LINE 1: SELECT ARRAY[NULL, ARRAY[' a ', ' '], ' [ a ', ' [ ], ] ', '...
                                              ^
DETAIL:  "[" must introduce e |

</details>

<details><summary>Literals8 - Maps (2)</summary>

| Case | Reason |
| --- | --- |
| [15] Return 20-deep nested maps | runner: timed out |
| [16] Return 40-deep nested maps | runner: timed out |

</details>

<details><summary>Map1 - Static value access (5)</summary>

| Case | Reason |
| --- | --- |
| [1] Statically access a field of a non-null map | columns ['NULL', 'm.notMissing', 'm.existing'] != ['m.missing', 'm.notMissing', 'm.existing'] |
| [2] Statically access a field of a null map | refused (500): Execution failed: missing FROM-clause entry for table "m"
LINE 1: WITH _w0 AS (SELECT NULL AS m) SELECT "m"."missing" FROM _w0
                                              ^ |
| [3] Statically access a field of a map resulting from an expression | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'MAP |
| [4] Statically access a field is case-sensitive #3 | rows differ: got [['Mats']] |
| [5] Statically access a field with a delimited identifier #6 | rows differ: got [['Mats']] |

</details>

<details><summary>Map2 - Dynamic Value Access (9)</summary>

| Case | Reason |
| --- | --- |
| [1] Dynamically access a field based on parameters when there is no type information | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'list_extract(STRUCT("name" VARCHAR), VARCHAR)'. You might need to add explicit type casts.
	Candidate |
| [2] Dynamically access a field based on parameters when there is rhs type information | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'list_extract(STRUCT("name" VARCHAR), VARCHAR)'. You might need to add explicit type casts.
	Candidate |
| [3] Dynamically access a field on null results in null | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'list_extract("NULL", VARCHAR)'. You might need to add explicit type casts.
	Candidate functions:
	lis |
| [4] Dynamically access a field with null results in null | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'list_extract(MAP(VARCHAR, JSON), "NULL")'. You might need to add explicit type casts.
	Candidate func |
| [5] Dynamically access a field is case-sensitive #1 | rows differ: got [['"Mats"']] |
| [5] Dynamically access a field is case-sensitive #2 | rows differ: got [['"Mats"']] |
| [5] Dynamically access a field is case-sensitive #3 | rows differ: got [['"Pontus"']] |
| [5] Dynamically access a field is case-sensitive #6 | rows differ: got [['"Mats"']] |
| [5] Dynamically access a field is case-sensitive #7 | rows differ: got [['"Pontus"']] |

</details>

<details><summary>Map3 - Keys function (11)</summary>

| Case | Reason |
| --- | --- |
| [1] Using `keys()` on a literal map | refused (500): Internal server error |
| [2] Using `keys()` on a parameter map | refused (500): Internal server error |
| [3] Using `keys()` on null map | refused (500): Internal server error |
| [4] Using `keys()` on map with null values #1 | refused (500): Internal server error |
| [4] Using `keys()` on map with null values #2 | refused (500): Internal server error |
| [4] Using `keys()` on map with null values #3 | refused (500): Internal server error |
| [4] Using `keys()` on map with null values #4 | refused (500): Internal server error |
| [4] Using `keys()` on map with null values #5 | refused (500): Internal server error |
| [4] Using `keys()` on map with null values #6 | refused (500): Internal server error |
| [4] Using `keys()` on map with null values #7 | refused (500): Internal server error |
| [5] Using `keys()` and `IN` to check field existence | refused (500): Internal server error |

</details>

<details><summary>Mathematical11 - Signed numbers functions (1)</summary>

| Case | Reason |
| --- | --- |
| [1] Absolute function | columns ['abs'] != ['abs(-1)'] |

</details>

<details><summary>Mathematical13 - Square root (1)</summary>

| Case | Reason |
| --- | --- |
| [1] `sqrt()` returning float values | columns ['sqrt'] != ['sqrt(12.96)'] |

</details>

<details><summary>Mathematical8 - Arithmetic precedence (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Arithmetic precedence test | columns ['?column?'] != ['12 / 4 * 3 - 2 * 4'] |
| [2] Arithmetic precedence with parenthesis test | columns ['?column?'] != ['12 / 4 * (3 - 2 * 4)'] |

</details>

<details><summary>Null1 - IS NULL validation (2)</summary>

| Case | Reason |
| --- | --- |
| [3] Property null check on null node | columns [] != ['n.missing IS NULL'] |
| [5] IS NULL on a map #12 | refused (500): Execution failed: missing FROM-clause entry for table "map"
LINE 1: WITH _w0 AS (SELECT NULL AS map) SELECT "map"."name" IS NULL...
                                                ^ |

</details>

<details><summary>Null2 - IS NOT NULL validation (2)</summary>

| Case | Reason |
| --- | --- |
| [3] Property not null check on null node | columns [] != ['n.missing IS NOT NULL'] |
| [5] IS NOT NULL on a map #12 | refused (500): Execution failed: missing FROM-clause entry for table "map"
LINE 1: WITH _w0 AS (SELECT NULL AS map) SELECT NOT "map"."name" IS ...
                                                    ^ |

</details>

<details><summary>Null3 - Null evaluation (7)</summary>

| Case | Reason |
| --- | --- |
| [4] Using null in IN #1 | refused (500): Execution failed: syntax error at or near "$2"
LINE 1: SELECT $1 IN $2 AS result
                     ^ |
| [4] Using null in IN #2 | refused (500): Execution failed: syntax error at or near "$2"
LINE 1: SELECT $1 IN $2 AS result
                     ^ |
| [4] Using null in IN #3 | refused (500): Execution failed: syntax error at or near "$2"
LINE 1: SELECT $1 IN $2 AS result
                     ^ |
| [4] Using null in IN #4 | refused (500): Execution failed: syntax error at or near "$2"
LINE 1: SELECT $1 IN $2 AS result
                     ^ |
| [4] Using null in IN #5 | refused (500): Execution failed: syntax error at or near "$2"
LINE 1: SELECT $1 IN $2 AS result
                     ^ |
| [4] Using null in IN #6 | refused (500): Execution failed: syntax error at or near "$2"
LINE 1: SELECT $1 IN $2 AS result
                     ^ |
| [4] Using null in IN #7 | refused (500): Execution failed: syntax error at or near "$2"
LINE 1: SELECT $1 IN $2 AS result
                     ^ |

</details>

<details><summary>Path1 - Nodes of a path (1)</summary>

| Case | Reason |
| --- | --- |
| [1] `nodes()` on null path | refused (500): Internal server error |

</details>

<details><summary>Path2 - Relationships of a path (1)</summary>

| Case | Reason |
| --- | --- |
| [3] `relationships()` on null path | refused (500): Internal server error |

</details>

<details><summary>Path3 - Length of a path (2)</summary>

| Case | Reason |
| --- | --- |
| [2] Failing when using `length()` on a node | expected SyntaxError; the query ran |
| [3] Failing when using `length()` on a relationship | expected SyntaxError; the query ran |

</details>

<details><summary>Precedence1 - On boolean values (11)</summary>

| Case | Reason |
| --- | --- |
| [1] Exclusive disjunction takes precedence over inclusive disjunction | refused (403): SQL parse error: Invalid expression / Unexpected token. Line 1, Col: 23.
  SELECT TRUE OR TRUE [4mXOR[0m TRUE AS a, TRUE OR ((TRUE AND (NOT TRUE)) OR ((NOT TRUE) AND TRUE)) AS b, ((TRUE OR TRUE) AND |
| [2] Conjunction disjunction takes precedence over exclusive disjunction | rows differ: got [[False, True, False]] |
| [8] Null predicate takes precedence over comparison operator | rows differ: got [[False, True, False]] |
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

<details><summary>Precedence2 - On numeric values (12)</summary>

| Case | Reason |
| --- | --- |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #2 | rows differ: got [[9.5, 9.5, 10.0]] |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #5 | rows differ: got [[6.5, 6.5, -2.0]] |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #7 | rows differ: got [[8.0, 8.0, 1.6]] |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #8 | rows differ: got [[3.5, 3.5, 0.4]] |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #9 | refused (500): Execution failed: operator does not exist: double precision % integer
LINE 1: ...(3 % 2) AS b, CAST(4 AS DOUBLE PRECISION) / (2 + 3) % 2 AS c
                                                           |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #10 | rows differ: got [[-4.0, -4.0, -8.0]] |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #11 | rows differ: got [[0.5, 0.5, -2.0]] |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #12 | refused (500): Execution failed: operator does not exist: double precision % integer
LINE 1: ...(3 % 2) AS b, CAST(4 AS DOUBLE PRECISION) / (2 - 3) % 2 AS c
                                                           |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #14 | rows differ: got [[1.5, 1.5, 2.0]] |
| [1] Numeric multiplicative operations takes precedence over numeric additive operations #17 | rows differ: got [[-1.5, -1.5, 0.0]] |
| [2] Exponentiation takes precedence over numeric multiplicative operations #2 | rows differ: got [[8.0, 8.0, 512.0]] |
| [2] Exponentiation takes precedence over numeric multiplicative operations #3 | refused (500): Execution failed: operator does not exist: double precision % double precision
LINE 1: SELECT POWER(4, 3) % POWER(2, 3) AS a, (POWER(4, 3)) % (POWE...
                           ^
HINT:  No operator m |

</details>

<details><summary>Precedence3 - On list values (11)</summary>

| Case | Reason |
| --- | --- |
| [1] List element access takes precedence over list appending | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [2] List element access takes precedence over list concatenation | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [3] List slicing takes precedence over list concatenation | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [4] List appending takes precedence over list element containment | refused (403): SQL parse error: Invalid expression / Unexpected token. Line 1, Col: 33.
  SELECT ARRAY[1] + 2 IN ARRAY[3] [4m+[0m 4 AS a, (ARRAY[1] + 2) IN (ARRAY[3] + 4) AS b, ARRAY[1] + (2 IN (3)) + 4 AS c |
| [5] List concatenation takes precedence over list element containment | refused (403): SQL parse error: Invalid expression / Unexpected token. Line 1, Col: 40.
  SELECT ARRAY[1] + ARRAY[2] IN ARRAY[3] [4m+[0m ARRAY[4] AS a, (ARRAY[1] + ARRAY[2]) IN (ARRAY[3] + ARRAY[4]) AS b, ((ARRAY[ |
| [6] List element containment takes precedence over comparison operator #1 | refused (500): Execution failed: operator does not exist: integer[] = boolean
LINE 1: SELECT ARRAY[1, 2] = ARRAY[3, 4] IN (ARRAY[3, 4], FALSE) AS ...
                                         ^
HINT:  No operator mat |
| [6] List element containment takes precedence over comparison operator #2 | refused (500): Execution failed: operator does not exist: integer[] = boolean
LINE 1: SELECT ARRAY[1, 2] <> ARRAY[3, 4] IN (ARRAY[3, 4], FALSE) AS...
                                          ^
HINT:  No operator ma |
| [6] List element containment takes precedence over comparison operator #3 | refused (500): Execution failed: operator does not exist: integer[] = boolean
LINE 1: SELECT ARRAY[1, 2] < ARRAY[3, 4] IN (ARRAY[3, 4], FALSE) AS ...
                                         ^
HINT:  No operator mat |
| [6] List element containment takes precedence over comparison operator #4 | refused (500): Execution failed: operator does not exist: integer[] = boolean
LINE 1: SELECT ARRAY[1, 2] > ARRAY[3, 4] IN (ARRAY[3, 4], FALSE) AS ...
                                         ^
HINT:  No operator mat |
| [6] List element containment takes precedence over comparison operator #5 | refused (500): Execution failed: operator does not exist: integer[] = boolean
LINE 1: SELECT ARRAY[1, 2] <= ARRAY[3, 4] IN (ARRAY[3, 4], FALSE) AS...
                                          ^
HINT:  No operator ma |
| [6] List element containment takes precedence over comparison operator #6 | refused (500): Execution failed: operator does not exist: integer[] = boolean
LINE 1: SELECT ARRAY[1, 2] >= ARRAY[3, 4] IN (ARRAY[3, 4], FALSE) AS...
                                          ^
HINT:  No operator ma |

</details>

<details><summary>Precedence4 - On null value (2)</summary>

| Case | Reason |
| --- | --- |
| [1] Null predicate takes precedence over comparison operator #2 | rows differ: got [[True, False, True]] |
| [4] String predicate takes precedence over binary boolean operator | refused (500): Execution failed: function starts_with(unknown, boolean) does not exist
LINE 1: ...RUE) AS a, (STARTS_WITH('abc', NULL) OR TRUE) <> (STARTS_WIT...
                                                      |

</details>

<details><summary>Quantifier1 - None quantifier (10)</summary>

| Case | Reason |
| --- | --- |
| [6] None quantifier on list literal containing lists #4 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER[]', from first occurrence
 - 'VARCHAR[] |
| [6] None quantifier on list literal containing lists #5 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'VARCHAR[]', from first occurrence
 - 'INTEGER[] |
| [6] None quantifier on list literal containing lists #6 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER[]', from first occurrence
 - 'VARCHAR[] |
| [6] None quantifier on list literal containing lists #7 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'VARCHAR[]', from first occurrence
 - 'INTEGER[] |
| [10] None quantifier on lists containing nulls #1 | rows differ: got [[True]] |
| [10] None quantifier on lists containing nulls #2 | rows differ: got [[True]] |
| [10] None quantifier on lists containing nulls #3 | rows differ: got [[True]] |
| [10] None quantifier on lists containing nulls #7 | rows differ: got [[True]] |
| [13] None quantifier is true if the predicate is statically false and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [14] None quantifier is false if the predicate is statically true and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |

</details>

<details><summary>Quantifier10 - Single quantifier invariants (8)</summary>

| Case | Reason |
| --- | --- |
| [1] Single quantifier is always false if the predicate is statically false and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [2] Single quantifier is always false if the predicate is statically true and the list has more than one element | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [3] Single quantifier is always true if the predicate is statically true and the list has exactly one non-null element | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
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
| [1] Any quantifier is always false if the predicate is statically false and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [2] Any quantifier is always true if the predicate is statically true and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
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
| [4] Any quantifier is always equal the boolean negative of the none quantifier #1 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] Any quantifier is always equal the boolean negative of the none quantifier #2 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] Any quantifier is always equal the boolean negative of the none quantifier #3 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] Any quantifier is always equal the boolean negative of the none quantifier #4 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] Any quantifier is always equal the boolean negative of the none quantifier #5 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #1 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #2 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #3 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #4 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [5] Any quantifier is always equal the boolean negative of the all quantifier on the boolean negative of the predicate #5 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
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
| [1] All quantifier is always false if the predicate is statically false and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [2] All quantifier is always true if the predicate is statically true and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #1 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #2 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #3 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #4 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [3] All quantifier is always equal the none quantifier on the boolean negative of the predicate #5 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #1 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #2 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #3 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #4 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] All quantifier is always equal the boolean negative of the any quantifier on the boolean negative of the predicate #5 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
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

<details><summary>Quantifier2 - Single quantifier (33)</summary>

| Case | Reason |
| --- | --- |
| [1] Single quantifier is always false on empty list | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[]) AS _t...
                                                      ^
HINT:  Explicitly |
| [2] Single quantifier on list literal #1 | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[]) AS _t...
                                                      ^
HINT:  Explicitly |
| [3] Single quantifier on list literal containing integers #1 | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[]) AS _t...
                                                      ^
HINT:  Explicitly |
| [4] Single quantifier on list literal containing floats #1 | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[]) AS _t...
                                                      ^
HINT:  Explicitly |
| [5] Single quantifier on list literal containing strings #1 | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[]) AS _t...
                                                      ^
HINT:  Explicitly |
| [5] Single quantifier on list literal containing strings #2 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ...ELECT x FROM UNNEST(ARRAY['abc']) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [5] Single quantifier on list literal containing strings #3 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ...SELECT x FROM UNNEST(ARRAY['ef']) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [5] Single quantifier on list literal containing strings #4 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ...x FROM UNNEST(ARRAY['abc', 'ef']) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [5] Single quantifier on list literal containing strings #5 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ...x FROM UNNEST(ARRAY['ef', 'abc']) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [5] Single quantifier on list literal containing strings #6 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ...UNNEST(ARRAY['abc', 'ef', 'abc']) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [5] Single quantifier on list literal containing strings #7 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ... UNNEST(ARRAY['ef', 'abc', 'ef']) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [5] Single quantifier on list literal containing strings #8 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ...NNEST(ARRAY['abc', 'abc', 'abc']) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [5] Single quantifier on list literal containing strings #9 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ...M UNNEST(ARRAY['ef', 'ef', 'ef']) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [6] Single quantifier on list literal containing lists #1 | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[]) AS _t...
                                                      ^
HINT:  Explicitly |
| [6] Single quantifier on list literal containing lists #2 | refused (500): Execution failed: function cardinality(integer) does not exist
LINE 1: ...ROM UNNEST(ARRAY[ARRAY[1, 2, 3]]) AS _t0(x) WHERE CARDINALIT...
                                                             ^ |
| [6] Single quantifier on list literal containing lists #3 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ... x FROM UNNEST(ARRAY[ARRAY['a']]) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [6] Single quantifier on list literal containing lists #4 | refused (500): Execution failed: ARRAY could not convert type text[] to integer[]
LINE 1: ...(ARRAY(SELECT x FROM UNNEST(ARRAY[ARRAY[1, 2, 3], ARRAY['a']...
                                                           |
| [6] Single quantifier on list literal containing lists #5 | refused (500): Execution failed: ARRAY could not convert type integer[] to text[]
LINE 1: ...LITY(ARRAY(SELECT x FROM UNNEST(ARRAY[ARRAY['a'], ARRAY[1, 2...
                                                           |
| [6] Single quantifier on list literal containing lists #6 | refused (500): Execution failed: ARRAY could not convert type text[] to integer[]
LINE 1: ...(ARRAY(SELECT x FROM UNNEST(ARRAY[ARRAY[1, 2, 3], ARRAY['a']...
                                                           |
| [6] Single quantifier on list literal containing lists #7 | refused (500): Execution failed: ARRAY could not convert type integer[] to text[]
LINE 1: ...LITY(ARRAY(SELECT x FROM UNNEST(ARRAY[ARRAY['a'], ARRAY[1, 2...
                                                           |
| [6] Single quantifier on list literal containing lists #8 | refused (500): Execution failed: function cardinality(integer) does not exist
LINE 1: ... ARRAY[1, 2, 3], ARRAY[1, 2, 3]]) AS _t0(x) WHERE CARDINALIT...
                                                             ^ |
| [6] Single quantifier on list literal containing lists #9 | refused (500): Execution failed: function cardinality(text) does not exist
LINE 1: ...AY['a'], ARRAY['a'], ARRAY['a']]) AS _t0(x) WHERE CARDINALIT...
                                                             ^
HI |
| [7] Single quantifier on list literal containing maps #1 | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[]) AS _t...
                                                      ^
HINT:  Explicitly |
| [10] Single quantifier on lists containing nulls #1 | refused (500): Execution failed: operator does not exist: text = integer
LINE 1: ...LECT x FROM UNNEST(ARRAY[NULL]) AS _t0(x) WHERE x = 2)) = 1 ...
                                                             ^
HINT |
| [10] Single quantifier on lists containing nulls #2 | refused (500): Execution failed: operator does not exist: text = integer
LINE 1: ... FROM UNNEST(ARRAY[NULL, NULL]) AS _t0(x) WHERE x = 2)) = 1 ...
                                                             ^
HINT |
| [10] Single quantifier on lists containing nulls #3 | rows differ: got [[False]] |
| [10] Single quantifier on lists containing nulls #4 | rows differ: got [[True]] |
| [10] Single quantifier on lists containing nulls #5 | rows differ: got [[True]] |
| [10] Single quantifier on lists containing nulls #7 | rows differ: got [[False]] |
| [11] Single quantifier with IS NULL predicate #1 | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[]) AS _t...
                                                      ^
HINT:  Explicitly |
| [12] Single quantifier with IS NOT NULL predicate #1 | refused (500): Execution failed: cannot determine type of empty array
LINE 1: SELECT CARDINALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[]) AS _t...
                                                      ^
HINT:  Explicitly |
| [13] Single quantifier is false if the predicate is statically false and the list is not empty | refused (500): Execution failed: ARRAY types integer and boolean cannot be matched
LINE 1: ...INALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[1, NULL, TRUE, 4.5,...
                                                          |
| [14] Single quantifier is false if the predicate is statically true and the list has more than one element | refused (500): Execution failed: ARRAY types integer and boolean cannot be matched
LINE 1: ...INALITY(ARRAY(SELECT x FROM UNNEST(ARRAY[1, NULL, TRUE, 4.5,...
                                                          |

</details>

<details><summary>Quantifier3 - Any quantifier (10)</summary>

| Case | Reason |
| --- | --- |
| [6] Any quantifier on list literal containing lists #4 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER[]', from first occurrence
 - 'VARCHAR[] |
| [6] Any quantifier on list literal containing lists #5 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'VARCHAR[]', from first occurrence
 - 'INTEGER[] |
| [6] Any quantifier on list literal containing lists #6 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER[]', from first occurrence
 - 'VARCHAR[] |
| [6] Any quantifier on list literal containing lists #7 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'VARCHAR[]', from first occurrence
 - 'INTEGER[] |
| [10] Any quantifier on lists containing nulls #1 | rows differ: got [[False]] |
| [10] Any quantifier on lists containing nulls #2 | rows differ: got [[False]] |
| [10] Any quantifier on lists containing nulls #3 | rows differ: got [[False]] |
| [10] Any quantifier on lists containing nulls #7 | rows differ: got [[False]] |
| [13] Any quantifier is false if the predicate is statically false and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [14] Any quantifier is true if the predicate is statically true and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |

</details>

<details><summary>Quantifier4 - All quantifier (11)</summary>

| Case | Reason |
| --- | --- |
| [6] All quantifier on list literal containing lists #4 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER[]', from first occurrence
 - 'VARCHAR[] |
| [6] All quantifier on list literal containing lists #5 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'VARCHAR[]', from first occurrence
 - 'INTEGER[] |
| [6] All quantifier on list literal containing lists #6 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER[]', from first occurrence
 - 'VARCHAR[] |
| [6] All quantifier on list literal containing lists #7 | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'VARCHAR[]', from first occurrence
 - 'INTEGER[] |
| [10] All quantifier on lists containing nulls #1 | rows differ: got [[True]] |
| [10] All quantifier on lists containing nulls #2 | rows differ: got [[True]] |
| [10] All quantifier on lists containing nulls #4 | rows differ: got [[True]] |
| [10] All quantifier on lists containing nulls #5 | rows differ: got [[True]] |
| [10] All quantifier on lists containing nulls #8 | rows differ: got [[True]] |
| [13] All quantifier is false if the predicate is statically false and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [14] All quantifier is true if the predicate is statically true and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |

</details>

<details><summary>Quantifier5 - None quantifier interop (4)</summary>

| Case | Reason |
| --- | --- |
| [1] None quantifier can nest itself and other quantifiers on nested lists #3 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |
| [1] None quantifier can nest itself and other quantifiers on nested lists #4 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |
| [2] None quantifier can nest itself and other quantifiers on the same list #3 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |
| [2] None quantifier can nest itself and other quantifiers on the same list #4 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |

</details>

<details><summary>Quantifier6 - Single quantifier interop (13)</summary>

| Case | Reason |
| --- | --- |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #3 | refused (500): Execution failed: function unnest(text) does not exist
LINE 1: ...) AS _t0(x) WHERE CARDINALITY(ARRAY(SELECT y FROM UNNEST(x) ...
                                                             ^
HINT:   |
| [1] Single quantifier can nest itself and other quantifiers on nested lists #4 | refused (500): Execution failed: function unnest(text) does not exist
LINE 1: ...) AS _t0(x) WHERE CARDINALITY(ARRAY(SELECT y FROM UNNEST(x) ...
                                                             ^
HINT:   |
| [2] Single quantifier can nest itself and other quantifiers on the same list #1 | refused (400): The engine refused the query: Binder Error: Referenced column "x" not found in FROM clause!
Candidate bindings: "list"

LINE 1: ... NOT COALESCE(LIST_BOOL_OR(LIST_TRANSFORM(_w0."list", y -> x < y)), F |
| [2] Single quantifier can nest itself and other quantifiers on the same list #2 | refused (400): The engine refused the query: Binder Error: Referenced column "x" not found in FROM clause!
Candidate bindings: "list"

LINE 1: ... NOT COALESCE(LIST_BOOL_OR(LIST_TRANSFORM(_w0."list", y -> x % y = 0) |
| [2] Single quantifier can nest itself and other quantifiers on the same list #5 | refused (400): The engine refused the query: Binder Error: Referenced column "x" not found in FROM clause!
Candidate bindings: "list"

LINE 1: ... COALESCE(LIST_BOOL_OR(LIST_TRANSFORM(_w0."list", y -> 2 * x + y > 25 |
| [2] Single quantifier can nest itself and other quantifiers on the same list #6 | refused (400): The engine refused the query: Binder Error: Referenced column "x" not found in FROM clause!
Candidate bindings: "list"

LINE 1: ...) WHERE COALESCE(LIST_BOOL_OR(LIST_TRANSFORM(_w0."list", y -> x < y)) |
| [2] Single quantifier can nest itself and other quantifiers on the same list #7 | refused (400): The engine refused the query: Binder Error: Referenced column "x" not found in FROM clause!
Candidate bindings: "list"

LINE 1: ...) WHERE COALESCE(LIST_BOOL_AND(LIST_TRANSFORM(_w0."list", y -> x <= y |
| [2] Single quantifier can nest itself and other quantifiers on the same list #8 | refused (400): The engine refused the query: Binder Error: Referenced column "x" not found in FROM clause!
Candidate bindings: "list"

LINE 1: ...) WHERE COALESCE(LIST_BOOL_AND(LIST_TRANSFORM(_w0."list", y -> x <= y |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #1 | refused (400): The engine refused the query: Parser Error: syntax error at or near "="

LINE 1: ...([1, 2, 3, 4, 5, 6, 7, 8, 9]) AS _t0(x) WHERE x = 2)) = 1 = (LENGTH(LIST_TRANSFORM(ARRAY(SELECT x FROM UNNEST([1,... |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #2 | refused (400): The engine refused the query: Parser Error: syntax error at or near "="

LINE 1: ...([1, 2, 3, 4, 5, 6, 7, 8, 9]) AS _t0(x) WHERE x % 2 = 0)) = 1 = (LENGTH(LIST_TRANSFORM(ARRAY(SELECT x FROM UNNEST([1 |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #3 | refused (400): The engine refused the query: Parser Error: syntax error at or near "="

LINE 1: ...([1, 2, 3, 4, 5, 6, 7, 8, 9]) AS _t0(x) WHERE x % 3 = 0)) = 1 = (LENGTH(LIST_TRANSFORM(ARRAY(SELECT x FROM UNNEST([1 |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #4 | refused (400): The engine refused the query: Parser Error: syntax error at or near "="

LINE 1: ...([1, 2, 3, 4, 5, 6, 7, 8, 9]) AS _t0(x) WHERE x < 7)) = 1 = (LENGTH(LIST_TRANSFORM(ARRAY(SELECT x FROM UNNEST([1,... |
| [3] Single quantifier is equal whether the size of the list filtered with same the predicate is one #5 | refused (400): The engine refused the query: Parser Error: syntax error at or near "="

LINE 1: ...([1, 2, 3, 4, 5, 6, 7, 8, 9]) AS _t0(x) WHERE x >= 3)) = 1 = (LENGTH(LIST_TRANSFORM(ARRAY(SELECT x FROM UNNEST([1,.. |

</details>

<details><summary>Quantifier7 - Any quantifier interop (4)</summary>

| Case | Reason |
| --- | --- |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #3 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |
| [1] Any quantifier can nest itself and other quantifiers on nested lists #4 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |
| [2] Any quantifier can nest itself and other quantifiers on the same list #3 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |
| [2] Any quantifier can nest itself and other quantifiers on the same list #4 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |

</details>

<details><summary>Quantifier8 - All quantifier interop (4)</summary>

| Case | Reason |
| --- | --- |
| [1] All quantifier can nest itself and other quantifiers on nested lists #3 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |
| [1] All quantifier can nest itself and other quantifiers on nested lists #4 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |
| [2] All quantifier can nest itself and other quantifiers on the same list #3 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |
| [2] All quantifier can nest itself and other quantifiers on the same list #4 | refused (400): The engine refused the query: Binder Error: subqueries in lambda expressions are not supported |

</details>

<details><summary>Quantifier9 - None quantifier invariants (17)</summary>

| Case | Reason |
| --- | --- |
| [1] None quantifier is always true if the predicate is statically false and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [2] None quantifier is always false if the predicate is statically true and the list is not empty | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'INT |
| [3] None quantifier is always equal the boolean negative of the any quantifier #1 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [3] None quantifier is always equal the boolean negative of the any quantifier #2 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [3] None quantifier is always equal the boolean negative of the any quantifier #3 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [3] None quantifier is always equal the boolean negative of the any quantifier #4 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [3] None quantifier is always equal the boolean negative of the any quantifier #5 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #1 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #2 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #3 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #4 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
| [4] None quantifier is always equal the all quantifier on the boolean negative of the predicate #5 | refused (400): The engine refused the query: Binder Error: No function matches the given name and argument types 'reverse(INTEGER[])'. You might need to add explicit type casts.
	Candidate functions:
	reverse(VARCHA |
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

<details><summary>String3 - String Reversal (1)</summary>

| Case | Reason |
| --- | --- |
| [1] `reverse()` | columns ['reverse'] != ["reverse('raksO')"] |

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
| [1] Should construct week date #1 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1816), TO_JSON(1)])...
               ^ |
| [1] Should construct week date #2 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1816), TO_JSON(52)]...
               ^ |
| [1] Should construct week date #3 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1817), TO_JSON(1)])...
               ^ |
| [1] Should construct week date #4 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1817), TO_JSON(10)]...
               ^ |
| [1] Should construct week date #5 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1817), TO_JSON(30)]...
               ^ |
| [1] Should construct week date #6 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1817), TO_JSON(52)]...
               ^ |
| [1] Should construct week date #7 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1818), TO_JSON(1)])...
               ^ |
| [1] Should construct week date #8 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1818), TO_JSON(52)]...
               ^ |
| [1] Should construct week date #9 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1818), TO_JSON(53)]...
               ^ |
| [1] Should construct week date #10 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1819), TO_JSON(1)])...
               ^ |
| [1] Should construct week date #11 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1819), TO_JSON(52)]...
               ^ |
| [1] Should construct week date #12 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['dayOfWeek', 'year', 'week'], [TO_JSON(2), TO_JSON...
                |
| [1] Should construct week date #13 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['date', 'week', 'dayOfWeek'], [TO_JSON(CAST('1816...
               ^ |
| [1] Should construct week date #14 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['date', 'week'], [TO_JSON(CAST('1816-12-31' AS...
               ^ |
| [1] Should construct week date #15 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['date', 'year', 'week'], [TO_JSON(CAST('1816-12...
               ^ |
| [2] Should construct week localdatetime #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1816), TO_JSON.. |
| [2] Should construct week localdatetime #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1816), TO_JSON.. |
| [2] Should construct week localdatetime #3 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1817), TO_JSON.. |
| [2] Should construct week localdatetime #4 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1817), TO_JSON.. |
| [2] Should construct week localdatetime #5 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1817), TO_JSON.. |
| [2] Should construct week localdatetime #6 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1817), TO_JSON.. |
| [2] Should construct week localdatetime #7 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1818), TO_JSON.. |
| [2] Should construct week localdatetime #8 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1818), TO_JSON.. |
| [2] Should construct week localdatetime #9 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1818), TO_JSON.. |
| [2] Should construct week localdatetime #10 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1819), TO_JSON.. |
| [2] Should construct week localdatetime #11 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week'], [TO_JSON(1819), TO_JSON.. |
| [2] Should construct week localdatetime #12 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['dayOfWeek', 'year', 'week'], [TO_JSON...
 |
| [2] Should construct week localdatetime #13 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['date', 'week', 'dayOfWeek'], [TO_JSON...
 |
| [2] Should construct week localdatetime #14 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['date', 'week'], [TO_JSON(CAST('1816...
   |
| [2] Should construct week localdatetime #15 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['date', 'year', 'week'], [TO_JSON(CAST...
 |
| [3] Should construct week datetime #1 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #2 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #3 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #9 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #11 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #12 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #13 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #14 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should construct week datetime #15 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [4] Should construct date #1 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
               ^ |
| [4] Should construct date #2 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'month'], [TO_JSON(1984), TO_JSON(10)...
               ^ |
| [4] Should construct date #3 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week', 'dayOfWeek'], [TO_JSON(1984),...
               ^ |
| [4] Should construct date #4 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'week'], [TO_JSON(1984), TO_JSON(10)]...
               ^ |
| [4] Should construct date #5 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year'], [TO_JSON(1984)]) AS DATE) AS result
               ^ |
| [4] Should construct date #6 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'ordinalDay'], [TO_JSON(1984), TO_JSON...
               ^ |
| [4] Should construct date #7 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'quarter', 'dayOfQuarter'], [TO_JSON(1984...
               ^ |
| [4] Should construct date #8 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: SELECT CAST(MAP(['year', 'quarter'], [TO_JSON(1984), TO_JSON(3...
               ^ |
| [5] Should construct local time #1 | refused (500): Internal server error |
| [5] Should construct local time #2 | refused (500): Internal server error |
| [5] Should construct local time #3 | refused (500): Internal server error |
| [5] Should construct local time #4 | refused (500): Internal server error |
| [5] Should construct local time #5 | refused (500): Internal server error |
| [5] Should construct local time #6 | refused (500): Internal server error |
| [5] Should construct local time #7 | refused (500): Internal server error |
| [6] Should construct time #1 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 210.
   TO_JSON(31), TO_JSON(14), TO_JSON(789), TO_JSON(123), TO_JSON(456)]) AS TIME |
| [6] Should construct time #2 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 158.
  econd'], [TO_JSON(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876123)]) AS TIME |
| [6] Should construct time #3 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 150.
  , 'nanosecond'], [TO_JSON(12), TO_JSON(31), TO_JSON(14), TO_JSON(3)]) AS TIME |
| [6] Should construct time #4 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 156.
  rosecond'], [TO_JSON(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876)]) AS TIME |
| [6] Should construct time #5 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 153.
  millisecond'], [TO_JSON(12), TO_JSON(31), TO_JSON(14), TO_JSON(645)]) AS TIME |
| [6] Should construct time #6 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 124.
  'hour', 'minute', 'second'], [TO_JSON(12), TO_JSON(31), TO_JSON(14)]) AS TIME |
| [6] Should construct time #7 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 101.
  SELECT CAST(CAST(MAP(['hour', 'minute'], [TO_JSON(12), TO_JSON(31)]) AS TIMES |
| [6] Should construct time #8 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 78.
  SELECT CAST(CAST(MAP(['hour'], [TO_JSON(12)]) AS TIMESTAMPTZ) AT TIME ZONE  [ |
| [6] Should construct time #9 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 189.
  2), TO_JSON(31), TO_JSON(14), TO_JSON(645876123), TO_JSON('+01:00')]) AS TIME |
| [6] Should construct time #10 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 187.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [6] Should construct time #11 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 184.
  JSON(12), TO_JSON(31), TO_JSON(14), TO_JSON(645), TO_JSON('+01:00')]) AS TIME |
| [6] Should construct time #12 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 155.
  mezone'], [TO_JSON(12), TO_JSON(31), TO_JSON(14), TO_JSON('+01:00')]) AS TIME |
| [6] Should construct time #13 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 132.
  'minute', 'timezone'], [TO_JSON(12), TO_JSON(31), TO_JSON('+01:00')]) AS TIME |
| [6] Should construct time #14 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 109.
  CAST(CAST(MAP(['hour', 'timezone'], [TO_JSON(12), TO_JSON('+01:00')]) AS TIME |
| [7] Should construct local date time #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour', 'minute... |
| [7] Should construct local date time #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour', 'minute... |
| [7] Should construct local date time #3 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour', 'minute... |
| [7] Should construct local date time #4 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour', 'minute... |
| [7] Should construct local date time #5 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour', 'minute... |
| [7] Should construct local date time #6 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour', 'minute... |
| [7] Should construct local date time #7 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour', 'minute... |
| [7] Should construct local date time #8 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour'], [TO_JSON. |
| [7] Should construct local date time #9 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'month', 'day'], [TO_JSON(1984...
 |
| [7] Should construct local date time #10 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek', 'hour',...
   |
| [7] Should construct local date time #11 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek', 'hour',...
   |
| [7] Should construct local date time #12 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek', 'hour',...
   |
| [7] Should construct local date time #13 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek', 'hour',...
   |
| [7] Should construct local date time #14 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek', 'hour',...
   |
| [7] Should construct local date time #15 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek', 'hour']...
   |
| [7] Should construct local date time #16 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek'], [TO_JSON...
 |
| [7] Should construct local date time #17 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'ordinalDay', 'hour', 'minute...
  |
| [7] Should construct local date time #18 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'ordinalDay', 'hour', 'minute...
  |
| [7] Should construct local date time #19 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'ordinalDay', 'hour', 'minute...
  |
| [7] Should construct local date time #20 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'ordinalDay', 'hour', 'minute...
  |
| [7] Should construct local date time #21 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'ordinalDay', 'hour', 'minute...
  |
| [7] Should construct local date time #22 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'ordinalDay', 'hour'], [TO_JSON... |
| [7] Should construct local date time #23 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'ordinalDay'], [TO_JSON(1984...
   |
| [7] Should construct local date time #24 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'quarter', 'dayOfQuarter', 'hour.. |
| [7] Should construct local date time #25 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'quarter', 'dayOfQuarter', 'hour.. |
| [7] Should construct local date time #26 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'quarter', 'dayOfQuarter', 'hour.. |
| [7] Should construct local date time #27 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'quarter', 'dayOfQuarter', 'hour.. |
| [7] Should construct local date time #28 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'quarter', 'dayOfQuarter', 'hour.. |
| [7] Should construct local date time #29 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'quarter', 'dayOfQuarter', 'hour.. |
| [7] Should construct local date time #30 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year', 'quarter', 'dayOfQuarter'], ...
   |
| [7] Should construct local date time #31 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: SELECT LOCALDATETIME(MAP(['year'], [TO_JSON(1984)])) AS result
      |
| [8] Should construct date time with default time zone #1 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #2 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #3 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #9 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #11 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #12 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #13 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #14 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #15 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #16 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #17 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #18 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #19 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #20 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #21 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #22 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #23 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #24 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #25 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #26 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #27 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #28 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #29 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should construct date time with default time zone #30 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #1 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #2 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #3 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #9 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #11 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #12 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #13 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #14 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #15 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #16 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #17 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #18 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #19 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #20 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #21 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #22 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #23 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #24 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #25 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #26 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #27 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #28 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should construct date time with offset time zone #29 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #1 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #2 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #3 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #9 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #11 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #12 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #13 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #14 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #15 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #16 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #17 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #18 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #19 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #20 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #21 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #22 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #23 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #24 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #25 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #26 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #27 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #28 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should construct date time with named time zone #29 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should construct date time from epoch | refused (400): Unknown function: 'datetime.fromepoch' |
| [12] Should construct duration #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: SELECT DURATION(MAP(['days', 'hours', 'minutes'], [TO_JSON(14)...
      |
| [12] Should construct duration #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: SELECT DURATION(MAP(['months', 'days'], [TO_JSON(5), TO_JSON(1...
      |
| [12] Should construct duration #3 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: SELECT DURATION(MAP(['months'], [TO_JSON(0.75)])) AS result
            |
| [12] Should construct duration #4 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: SELECT DURATION(MAP(['weeks'], [TO_JSON(2.5)])) AS result
              |
| [12] Should construct duration #5 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: SELECT DURATION(MAP(['years', 'months', 'days', 'hours', 'minutes...
   |
| [12] Should construct duration #6 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: SELECT DURATION(MAP(['days', 'seconds', 'milliseconds'], [TO_JSON...
   |
| [12] Should construct duration #7 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: SELECT DURATION(MAP(['days', 'seconds', 'microseconds'], [TO_JSON...
   |
| [12] Should construct duration #8 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: SELECT DURATION(MAP(['days', 'seconds', 'nanoseconds'], [TO_JSON...
    |
| [12] Should construct duration #9 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: SELECT DURATION(MAP(['minutes', 'seconds'], [TO_JSON(1.5), TO_JSON...
  |
| [13] Should construct temporal with time offset with second precision #1 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 158.
  one'], [TO_JSON(12), TO_JSON(34), TO_JSON(56), TO_JSON('+02:05:00')]) AS TIME |
| [13] Should construct temporal with time offset with second precision #2 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 158.
  one'], [TO_JSON(12), TO_JSON(34), TO_JSON(56), TO_JSON('+02:05:59')]) AS TIME |
| [13] Should construct temporal with time offset with second precision #3 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 158.
  one'], [TO_JSON(12), TO_JSON(34), TO_JSON(56), TO_JSON('-02:05:07')]) AS TIME |
| [13] Should construct temporal with time offset with second precision #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |

</details>

<details><summary>Temporal10 - Compute Durations Between two Temporal Values (131)</summary>

| Case | Reason |
| --- | --- |
| [1] Should split between boundaries correctly #1 | refused (400): Unknown function: 'duration.between' |
| [1] Should split between boundaries correctly #2 | refused (400): Unknown function: 'duration.between' |
| [1] Should split between boundaries correctly #3 | refused (400): Unknown function: 'duration.between' |
| [1] Should split between boundaries correctly #4 | refused (400): Unknown function: 'duration.between' |
| [1] Should split between boundaries correctly #5 | refused (400): Unknown function: 'duration.between' |
| [1] Should split between boundaries correctly #6 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #1 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #2 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #3 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #4 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #5 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #6 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #7 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #8 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #9 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #10 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #11 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #12 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #13 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #14 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #15 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #16 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #17 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #18 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #19 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #20 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #21 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #22 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #23 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #24 | refused (400): Unknown function: 'duration.between' |
| [2] Should compute duration between two temporals #25 | refused (400): Unknown function: 'duration.between' |
| [3] Should compute duration between two temporals in months #1 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #2 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #3 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #4 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #5 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #6 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #7 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #8 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #9 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #10 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #11 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #12 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #13 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #14 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #15 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #16 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #17 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #18 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #19 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #20 | refused (400): Unknown function: 'duration.inMonths' |
| [3] Should compute duration between two temporals in months #21 | refused (400): Unknown function: 'duration.inMonths' |
| [4] Should compute duration between two temporals in days #1 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #2 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #3 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #4 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #5 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #6 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #7 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #8 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #9 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #10 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #11 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #12 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #13 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #14 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #15 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #16 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #17 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #18 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #19 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #20 | refused (400): Unknown function: 'duration.inDays' |
| [4] Should compute duration between two temporals in days #21 | refused (400): Unknown function: 'duration.inDays' |
| [5] Should compute duration between two temporals in seconds #1 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #2 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #3 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #4 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #5 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #6 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #7 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #8 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #9 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #10 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #11 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #12 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #13 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #14 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #15 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #16 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #17 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #18 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #19 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #20 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #21 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #22 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #23 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #24 | refused (400): Unknown function: 'duration.inSeconds' |
| [5] Should compute duration between two temporals in seconds #25 | refused (400): Unknown function: 'duration.inSeconds' |
| [6] Should compute duration between if they differ only by a fraction of a second and the first comes after the second. | refused (400): Unknown function: 'duration.inSeconds' |
| [7] Should compute negative duration between in big units #1 | refused (400): Unknown function: 'duration.inMonths' |
| [7] Should compute negative duration between in big units #2 | refused (400): Unknown function: 'duration.inMonths' |
| [7] Should compute negative duration between in big units #3 | refused (400): Unknown function: 'duration.inMonths' |
| [7] Should compute negative duration between in big units #4 | refused (400): Unknown function: 'duration.inMonths' |
| [7] Should compute negative duration between in big units #5 | refused (400): Unknown function: 'duration.inMonths' |
| [8] Should handle durations at daylight saving time day #1 | refused (400): Unknown function: 'duration.inSeconds' |
| [8] Should handle durations at daylight saving time day #2 | refused (400): Unknown function: 'duration.inSeconds' |
| [8] Should handle durations at daylight saving time day #3 | refused (400): Unknown function: 'duration.inSeconds' |
| [8] Should handle durations at daylight saving time day #4 | refused (400): Unknown function: 'duration.inSeconds' |
| [8] Should handle durations at daylight saving time day #5 | refused (400): Unknown function: 'duration.inSeconds' |
| [8] Should handle durations at daylight saving time day #6 | refused (400): Unknown function: 'duration.inSeconds' |
| [9] Should handle large durations | refused (400): Unknown function: 'duration.between' |
| [10] Should handle large durations in seconds | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #1 | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #2 | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #3 | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #4 | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #5 | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #6 | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #7 | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #8 | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #9 | refused (400): Unknown function: 'duration.inSeconds' |
| [11] Should handle when seconds and subseconds have different signs #10 | refused (400): Unknown function: 'duration.inSeconds' |
| [12] Should compute durations with no difference #1 | refused (400): Unknown function: 'duration.inSeconds' |
| [12] Should compute durations with no difference #2 | refused (400): Unknown function: 'duration.inSeconds' |
| [12] Should compute durations with no difference #3 | refused (400): Unknown function: 'duration.inSeconds' |
| [12] Should compute durations with no difference #4 | refused (400): Unknown function: 'duration.inSeconds' |
| [12] Should compute durations with no difference #5 | refused (400): Unknown function: 'duration.inSeconds' |
| [13] Should propagate null #1 | refused (400): Unknown function: 'duration.between' |
| [13] Should propagate null #2 | refused (400): Unknown function: 'duration.inMonths' |
| [13] Should propagate null #3 | refused (400): Unknown function: 'duration.inDays' |
| [13] Should propagate null #4 | refused (400): Unknown function: 'duration.inSeconds' |

</details>

<details><summary>Temporal2 - Create Temporal Values from a String (50)</summary>

| Case | Reason |
| --- | --- |
| [1] Should parse date from string #3 | refused (500): Execution failed: invalid input syntax for type date: "2015-07"
LINE 1: SELECT DATE('2015-07') AS result
                    ^ |
| [1] Should parse date from string #4 | refused (500): Execution failed: date/time field value out of range: "201507"
LINE 1: SELECT DATE('201507') AS result
                    ^
HINT:  Perhaps you need a different "datestyle" setting. |
| [1] Should parse date from string #5 | refused (500): Execution failed: invalid input syntax for type date: "2015-W30-2"
LINE 1: SELECT DATE('2015-W30-2') AS result
                    ^ |
| [1] Should parse date from string #6 | refused (500): Execution failed: invalid input syntax for type date: "2015W302"
LINE 1: SELECT DATE('2015W302') AS result
                    ^ |
| [1] Should parse date from string #7 | refused (500): Execution failed: invalid input syntax for type date: "2015-W30"
LINE 1: SELECT DATE('2015-W30') AS result
                    ^ |
| [1] Should parse date from string #8 | refused (500): Execution failed: invalid input syntax for type date: "2015W30"
LINE 1: SELECT DATE('2015W30') AS result
                    ^ |
| [1] Should parse date from string #10 | refused (500): Execution failed: date/time field value out of range: "2015202"
LINE 1: SELECT DATE('2015202') AS result
                    ^
HINT:  Perhaps you need a different "datestyle" setting. |
| [1] Should parse date from string #11 | refused (500): Execution failed: invalid input syntax for type date: "2015"
LINE 1: SELECT DATE('2015') AS result
                    ^ |
| [2] Should parse local time from string #1 | refused (500): Execution failed: syntax error at or near "'21:40:32.142'"
LINE 1: SELECT LOCALTIME('21:40:32.142') AS result
                         ^ |
| [2] Should parse local time from string #2 | refused (500): Execution failed: syntax error at or near "'214032.142'"
LINE 1: SELECT LOCALTIME('214032.142') AS result
                         ^ |
| [2] Should parse local time from string #3 | refused (500): Execution failed: syntax error at or near "'21:40:32'"
LINE 1: SELECT LOCALTIME('21:40:32') AS result
                         ^ |
| [2] Should parse local time from string #4 | refused (500): Execution failed: syntax error at or near "'214032'"
LINE 1: SELECT LOCALTIME('214032') AS result
                         ^ |
| [2] Should parse local time from string #5 | refused (500): Execution failed: syntax error at or near "'21:40'"
LINE 1: SELECT LOCALTIME('21:40') AS result
                         ^ |
| [2] Should parse local time from string #6 | refused (500): Execution failed: syntax error at or near "'2140'"
LINE 1: SELECT LOCALTIME('2140') AS result
                         ^ |
| [2] Should parse local time from string #7 | refused (500): Execution failed: syntax error at or near "'21'"
LINE 1: SELECT LOCALTIME('21') AS result
                         ^ |
| [3] Should parse time from string #1 | refused (500): Execution failed: syntax error at or near "'21:40:32.142+0100'"
LINE 1: SELECT TIME('21:40:32.142+0100') AS result
                    ^ |
| [3] Should parse time from string #2 | refused (500): Execution failed: syntax error at or near "'214032.142Z'"
LINE 1: SELECT TIME('214032.142Z') AS result
                    ^ |
| [3] Should parse time from string #3 | refused (500): Execution failed: syntax error at or near "'21:40:32+01:00'"
LINE 1: SELECT TIME('21:40:32+01:00') AS result
                    ^ |
| [3] Should parse time from string #4 | refused (500): Execution failed: syntax error at or near "'214032-0100'"
LINE 1: SELECT TIME('214032-0100') AS result
                    ^ |
| [3] Should parse time from string #5 | refused (500): Execution failed: syntax error at or near "'21:40-01:30'"
LINE 1: SELECT TIME('21:40-01:30') AS result
                    ^ |
| [3] Should parse time from string #6 | refused (500): Execution failed: syntax error at or near "'2140-00:00'"
LINE 1: SELECT TIME('2140-00:00') AS result
                    ^ |
| [3] Should parse time from string #7 | refused (500): Execution failed: syntax error at or near "'2140-02'"
LINE 1: SELECT TIME('2140-02') AS result
                    ^ |
| [3] Should parse time from string #8 | refused (500): Execution failed: syntax error at or near "'22+18:00'"
LINE 1: SELECT TIME('22+18:00') AS result
                    ^ |
| [4] Should parse local date time from string #1 | refused (500): Execution failed: function localdatetime(unknown) does not exist
LINE 1: SELECT LOCALDATETIME('2015-07-21T21:40:32.142') AS result
               ^
HINT:  No function matches the given name and argume |
| [4] Should parse local date time from string #2 | refused (500): Execution failed: function localdatetime(unknown) does not exist
LINE 1: SELECT LOCALDATETIME('2015-W30-2T214032.142') AS result
               ^
HINT:  No function matches the given name and argument |
| [4] Should parse local date time from string #3 | refused (500): Execution failed: function localdatetime(unknown) does not exist
LINE 1: SELECT LOCALDATETIME('2015-202T21:40:32') AS result
               ^
HINT:  No function matches the given name and argument typ |
| [4] Should parse local date time from string #4 | refused (500): Execution failed: function localdatetime(unknown) does not exist
LINE 1: SELECT LOCALDATETIME('2015T214032') AS result
               ^
HINT:  No function matches the given name and argument types. Yo |
| [4] Should parse local date time from string #5 | refused (500): Execution failed: function localdatetime(unknown) does not exist
LINE 1: SELECT LOCALDATETIME('20150721T21:40') AS result
               ^
HINT:  No function matches the given name and argument types. |
| [4] Should parse local date time from string #6 | refused (500): Execution failed: function localdatetime(unknown) does not exist
LINE 1: SELECT LOCALDATETIME('2015-W30T2140') AS result
               ^
HINT:  No function matches the given name and argument types.  |
| [4] Should parse local date time from string #7 | refused (500): Execution failed: function localdatetime(unknown) does not exist
LINE 1: SELECT LOCALDATETIME('2015202T21') AS result
               ^
HINT:  No function matches the given name and argument types. You |
| [5] Should parse date time from string #1 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015-07-21T21:40:32.142+0100') AS result
               ^
HINT:  No function matches the given name and argument ty |
| [5] Should parse date time from string #2 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015-W30-2T214032.142Z') AS result
               ^
HINT:  No function matches the given name and argument types. Y |
| [5] Should parse date time from string #3 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015-202T21:40:32+01:00') AS result
               ^
HINT:  No function matches the given name and argument types.  |
| [5] Should parse date time from string #4 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015T214032-0100') AS result
               ^
HINT:  No function matches the given name and argument types. You mig |
| [5] Should parse date time from string #5 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('20150721T21:40-01:30') AS result
               ^
HINT:  No function matches the given name and argument types. You |
| [5] Should parse date time from string #6 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015-W30T2140-00:00') AS result
               ^
HINT:  No function matches the given name and argument types. You  |
| [5] Should parse date time from string #7 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015-W30T2140-02') AS result
               ^
HINT:  No function matches the given name and argument types. You mig |
| [5] Should parse date time from string #8 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015202T21+18:00') AS result
               ^
HINT:  No function matches the given name and argument types. You mig |
| [6] Should parse date time with named time zone from string #1 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015-07-21T21:40:32.142+02:00[Europe/Stockh...
               ^
HINT:  No function matches the given name and argum |
| [6] Should parse date time with named time zone from string #2 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015-07-21T21:40:32.142+0845[Australia/Eucl...
               ^
HINT:  No function matches the given name and argum |
| [6] Should parse date time with named time zone from string #3 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015-07-21T21:40:32.142-04[America/New_York...
               ^
HINT:  No function matches the given name and argum |
| [6] Should parse date time with named time zone from string #4 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('2015-07-21T21:40:32.142[Europe/London]') AS...
               ^
HINT:  No function matches the given name and argum |
| [6] Should parse date time with named time zone from string #5 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME('1818-07-21T21:40:32.142[Europe/Stockholm]')...
               ^
HINT:  No function matches the given name and argum |
| [7] Should parse duration from string #1 | refused (500): Execution failed: function duration(unknown) does not exist
LINE 1: SELECT DURATION('P14DT16H12M') AS result
               ^
HINT:  No function matches the given name and argument types. You might ne |
| [7] Should parse duration from string #2 | refused (500): Execution failed: function duration(unknown) does not exist
LINE 1: SELECT DURATION('P5M1.5D') AS result
               ^
HINT:  No function matches the given name and argument types. You might need t |
| [7] Should parse duration from string #3 | refused (500): Execution failed: function duration(unknown) does not exist
LINE 1: SELECT DURATION('P0.75M') AS result
               ^
HINT:  No function matches the given name and argument types. You might need to |
| [7] Should parse duration from string #4 | refused (500): Execution failed: function duration(unknown) does not exist
LINE 1: SELECT DURATION('PT0.75M') AS result
               ^
HINT:  No function matches the given name and argument types. You might need t |
| [7] Should parse duration from string #5 | refused (500): Execution failed: function duration(unknown) does not exist
LINE 1: SELECT DURATION('P2.5W') AS result
               ^
HINT:  No function matches the given name and argument types. You might need to  |
| [7] Should parse duration from string #6 | refused (500): Execution failed: function duration(unknown) does not exist
LINE 1: SELECT DURATION('P12Y5M14DT16H12M70S') AS result
               ^
HINT:  No function matches the given name and argument types. You  |
| [7] Should parse duration from string #7 | refused (500): Execution failed: function duration(unknown) does not exist
LINE 1: SELECT DURATION('P2012-02-02T14:37:21.545') AS result
               ^
HINT:  No function matches the given name and argument types. |

</details>

<details><summary>Temporal3 - Project Temporal Values from other Temporal Values (183)</summary>

| Case | Reason |
| --- | --- |
| [1] Should select date #1 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
     |
| [1] Should select date #2 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
     |
| [1] Should select date #3 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
     |
| [1] Should select date #4 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
     |
| [1] Should select date #5 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
     |
| [1] Should select date #6 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
     |
| [1] Should select date #7 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
     |
| [1] Should select date #8 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [1] Should select date #9 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [1] Should select date #10 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [1] Should select date #11 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [1] Should select date #12 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [1] Should select date #13 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [1] Should select date #14 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [1] Should select date #15 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [1] Should select date #16 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [1] Should select date #17 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [1] Should select date #18 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [1] Should select date #19 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [1] Should select date #20 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [1] Should select date #21 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [2] Should select local time #1 | refused (500): Internal server error |
| [2] Should select local time #2 | refused (500): Internal server error |
| [2] Should select local time #3 | refused (500): Internal server error |
| [2] Should select local time #4 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [2] Should select local time #5 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [2] Should select local time #6 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [2] Should select local time #7 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [2] Should select local time #8 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [2] Should select local time #9 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [2] Should select local time #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [2] Should select local time #11 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [2] Should select local time #12 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should select time #1 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 101.
  WITH _w0 AS (SELECT LOCALTIME AS other) SELECT CAST(CAST(_w0."other" AS TIMES |
| [3] Should select time #2 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 127.
  TIME AS other) SELECT CAST(CAST(MAP(['time'], [TO_JSON(_w0."other")]) AS TIME |
| [3] Should select time #3 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 158.
  (MAP(['time', 'timezone'], [TO_JSON(_w0."other"), TO_JSON('+05:00')]) AS TIME |
| [3] Should select time #4 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 150.
  AST(CAST(MAP(['time', 'second'], [TO_JSON(_w0."other"), TO_JSON(42)]) AS TIME |
| [3] Should select time #5 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 181.
   'timezone'], [TO_JSON(_w0."other"), TO_JSON(42), TO_JSON('+05:00')]) AS TIME |
| [3] Should select time #6 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [3] Should select time #7 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [3] Should select time #8 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [3] Should select time #9 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [3] Should select time #10 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [3] Should select time #11 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 279.
  , TO_JSON(14), TO_JSON(645)])) AS other) SELECT CAST(CAST(_w0."other" AS TIME |
| [3] Should select time #12 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 305.
  )])) AS other) SELECT CAST(CAST(MAP(['time'], [TO_JSON(_w0."other")]) AS TIME |
| [3] Should select time #13 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 336.
  (MAP(['time', 'timezone'], [TO_JSON(_w0."other"), TO_JSON('+05:00')]) AS TIME |
| [3] Should select time #14 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 328.
  AST(CAST(MAP(['time', 'second'], [TO_JSON(_w0."other"), TO_JSON(42)]) AS TIME |
| [3] Should select time #15 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 359.
   'timezone'], [TO_JSON(_w0."other"), TO_JSON(42), TO_JSON('+05:00')]) AS TIME |
| [3] Should select time #16 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should select time #17 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should select time #18 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should select time #19 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [3] Should select time #20 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [4] Should select date into local date time #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: ...(1984), TO_JSON(10), TO_JSON(11)]) AS DATE) AS other) SELECT LOCA |
| [4] Should select date into local date time #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: ...(1984), TO_JSON(10), TO_JSON(11)]) AS DATE) AS other) SELECT LOCA |
| [4] Should select date into local date time #3 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [4] Should select date into local date time #4 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [4] Should select date into local date time #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [4] Should select date into local date time #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [5] Should select time into local date time #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALTIME AS other) SELECT LOCALDATETIME(MAP(['y |
| [5] Should select time into local date time #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALTIME AS other) SELECT LOCALDATETIME(MAP(['y |
| [5] Should select time into local date time #3 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [5] Should select time into local date time #4 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 200.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [5] Should select time into local date time #5 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [5] Should select time into local date time #6 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [5] Should select time into local date time #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [5] Should select time into local date time #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: ...(11)]) AS DATE) AS otherDate, LOCALTIME AS otherTime) SELECT LOCA |
| [6] Should select date and time into local date time #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: ...(11)]) AS DATE) AS otherDate, LOCALTIME AS otherTime) SELECT LOCA |
| [6] Should select date and time into local date time #3 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 301.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [6] Should select date and time into local date time #4 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 301.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [6] Should select date and time into local date time #5 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: ...(1984), TO_JSON(10), TO_JSON(11)]) AS DATE) AS otherDate, LOCALDA |
| [6] Should select date and time into local date time #6 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: ...(1984), TO_JSON(10), TO_JSON(11)]) AS DATE) AS otherDate, LOCALDA |
| [6] Should select date and time into local date time #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #9 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [6] Should select date and time into local date time #10 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [6] Should select date and time into local date time #11 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 402.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [6] Should select date and time into local date time #12 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 402.
  N(12), TO_JSON(31), TO_JSON(14), TO_JSON(645876), TO_JSON('+01:00')]) AS TIME |
| [6] Should select date and time into local date time #13 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [6] Should select date and time into local date time #14 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [6] Should select date and time into local date time #15 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #16 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #17 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #18 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #19 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #20 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #21 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #22 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #23 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should select date and time into local date time #24 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [7] Should select datetime into local date time #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [7] Should select datetime into local date time #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [7] Should select datetime into local date time #3 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'week', 'dayOfWeek',  |
| [7] Should select datetime into local date time #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [7] Should select datetime into local date time #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [7] Should select datetime into local date time #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #1 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #2 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #3 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #9 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #11 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [8] Should select date into date time #12 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #1 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #2 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #3 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #9 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #11 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #12 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #13 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #14 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #15 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [9] Should select time into date time #16 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #1 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #2 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #3 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #9 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #11 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #12 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #13 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #14 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #15 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #16 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #17 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #18 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #19 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #20 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #21 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #22 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #23 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #24 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #25 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #26 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #27 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #28 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #29 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #30 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #31 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #32 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #33 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #34 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #35 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #36 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #37 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #38 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #39 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #40 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #41 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #42 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #43 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #44 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #45 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #46 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #47 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [10] Should select date and time into date time #48 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #1 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #2 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #3 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #4 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #6 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #7 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #8 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #9 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [11] Should datetime into date time #10 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |

</details>

<details><summary>Temporal4 - Store Temporal Values (38)</summary>

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
| [13] Should propagate null #2 | refused (400): Unknown function: 'date.transaction' |
| [13] Should propagate null #3 | refused (400): Unknown function: 'date.statement' |
| [13] Should propagate null #4 | refused (400): Unknown function: 'date.realtime' |
| [13] Should propagate null #5 | refused (500): Execution failed: syntax error at or near "NULL"
LINE 1: SELECT LOCALTIME(NULL) AS t
                         ^ |
| [13] Should propagate null #6 | refused (400): Unknown function: 'localtime.transaction' |
| [13] Should propagate null #7 | refused (400): Unknown function: 'localtime.statement' |
| [13] Should propagate null #8 | refused (400): Unknown function: 'localtime.realtime' |
| [13] Should propagate null #9 | refused (500): Execution failed: syntax error at or near "NULL"
LINE 1: SELECT TIME(NULL) AS t
                    ^ |
| [13] Should propagate null #10 | refused (400): Unknown function: 'time.transaction' |
| [13] Should propagate null #11 | refused (400): Unknown function: 'time.statement' |
| [13] Should propagate null #12 | refused (400): Unknown function: 'time.realtime' |
| [13] Should propagate null #13 | refused (500): Execution failed: function localdatetime(unknown) does not exist
LINE 1: SELECT LOCALDATETIME(NULL) AS t
               ^
HINT:  No function matches the given name and argument types. You might need t |
| [13] Should propagate null #14 | refused (400): Unknown function: 'localdatetime.transaction' |
| [13] Should propagate null #15 | refused (400): Unknown function: 'localdatetime.statement' |
| [13] Should propagate null #16 | refused (400): Unknown function: 'localdatetime.realtime' |
| [13] Should propagate null #17 | refused (500): Execution failed: function datetime(unknown) does not exist
LINE 1: SELECT DATETIME(NULL) AS t
               ^
HINT:  No function matches the given name and argument types. You might need to add expl |
| [13] Should propagate null #18 | refused (400): Unknown function: 'datetime.transaction' |
| [13] Should propagate null #19 | refused (400): Unknown function: 'datetime.statement' |
| [13] Should propagate null #20 | refused (400): Unknown function: 'datetime.realtime' |
| [13] Should propagate null #21 | refused (500): Execution failed: function duration(unknown) does not exist
LINE 1: SELECT DURATION(NULL) AS t
               ^
HINT:  No function matches the given name and argument types. You might need to add expl |

</details>

<details><summary>Temporal6 - Render Temporal Values as a String (17)</summary>

| Case | Reason |
| --- | --- |
| [1] Should serialize date | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
     |
| [2] Should serialize local time | rows differ: got [['04:29:21.150734', True]] |
| [3] Should serialize time | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 202.
  2), TO_JSON(31), TO_JSON(14), TO_JSON(645876123), TO_JSON('+01:00')]) AS TIME |
| [4] Should serialize local date time | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [5] Should serialize date time | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should serialize duration #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['years', 'months', 'days', 'hours', ' |
| [6] Should serialize duration #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['years', 'months', 'days', 'hours'],  |
| [6] Should serialize duration #3 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['minutes', 'seconds'], [TO_JSON(12),  |
| [6] Should serialize duration #4 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['seconds', 'milliseconds'], [TO_JSON( |
| [6] Should serialize duration #5 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['seconds', 'milliseconds'], [TO_JSON( |
| [6] Should serialize duration #6 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['seconds', 'milliseconds'], [TO_JSON( |
| [6] Should serialize duration #7 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['days', 'milliseconds'], [TO_JSON(1), |
| [6] Should serialize duration #8 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['days', 'milliseconds'], [TO_JSON(1), |
| [6] Should serialize duration #9 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['seconds', 'milliseconds'], [TO_JSON( |
| [6] Should serialize duration #10 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['seconds', 'milliseconds'], [TO_JSON( |
| [6] Should serialize duration #11 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['seconds', 'milliseconds'], [TO_JSON( |
| [7] Should serialize timezones correctly | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |

</details>

<details><summary>Temporal7 - Compare Temporal Values (18)</summary>

| Case | Reason |
| --- | --- |
| [1] Should compare dates #1 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1980), TO_JSON...
     |
| [1] Should compare dates #2 | refused (400): The engine refused the query: Conversion Error: Unimplemented type for cast (MAP(VARCHAR, JSON) -> DATE)

LINE 1: WITH _w0 AS (SELECT CAST(MAP(['year', 'month', 'day'], [TO_JSON(1984), TO_JSON...
     |
| [2] Should compare local times #1 | columns ['(_w0.x > _w0.d)', '(_w0.x < _w0.d)', '(_w0.x >= _w0.d)', '(_w0.x <= _w0.d)', '(_w0.x = _w0.d)'] != ['x > d', 'x < d', 'x >= d', 'x <= d', 'x = d'] |
| [2] Should compare local times #2 | columns ['(_w0.x > _w0.d)', '(_w0.x < _w0.d)', '(_w0.x >= _w0.d)', '(_w0.x <= _w0.d)', '(_w0.x = _w0.d)'] != ['x > d', 'x < d', 'x >= d', 'x <= d', 'x = d'] |
| [3] Should compare times #1 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 144.
   'minute', 'timezone'], [TO_JSON(10), TO_JSON(0), TO_JSON('+01:00')]) AS TIME |
| [3] Should compare times #2 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 201.
  9), TO_JSON(35), TO_JSON(14), TO_JSON(645876123), TO_JSON('+00:00')]) AS TIME |
| [4] Should compare local date times #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [4] Should compare local date times #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name localdatetime does not exist!
Did you mean "lcase"?

LINE 1: WITH _w0 AS (SELECT LOCALDATETIME(MAP(['year', 'month', 'day', 'hour |
| [5] Should compare date times #1 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [5] Should compare date times #2 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should compare durations for equality #1 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['years', 'months', 'days', 'hours', ' |
| [6] Should compare durations for equality #2 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['years', 'months', 'days', 'hours', ' |
| [6] Should compare durations for equality #3 | refused (500): Governance failed: Required keyword: 'zone' missing for <class 'sqlglot.expressions.core.AtTimeZone'>. Line 1, Col: 360.
  9), TO_JSON(35), TO_JSON(14), TO_JSON(645876123), TO_JSON('+00:00')]) AS TIME |
| [6] Should compare durations for equality #4 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['years', 'months', 'days', 'hours', ' |
| [6] Should compare durations for equality #5 | refused (500): Governance failed: 'NoneType' object has no attribute 'name' |
| [6] Should compare durations for equality #6 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['years', 'months', 'days', 'hours', ' |
| [6] Should compare durations for equality #7 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['years', 'months', 'days', 'hours', ' |
| [6] Should compare durations for equality #8 | refused (400): The engine refused the query: Catalog Error: Scalar Function with name duration does not exist!
Did you mean "radians"?

LINE 1: WITH _w0 AS (SELECT DURATION(MAP(['years', 'months', 'days', 'hours', ' |

</details>

<details><summary>Temporal9 - Truncate Temporal Values (322)</summary>

| Case | Reason |
| --- | --- |
| [1] Should truncate date #1 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #2 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #3 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #4 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #5 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #6 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #7 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #8 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #9 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #10 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #11 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #12 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #13 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #14 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #15 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #16 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #17 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #18 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #19 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #20 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #21 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #22 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #23 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #24 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #25 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #26 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #27 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #28 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #29 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #30 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #31 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #32 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #33 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #34 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #35 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #36 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #37 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #38 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #39 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #40 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #41 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #42 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #43 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #44 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #45 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #46 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #47 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #48 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #49 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #50 | refused (400): Unknown function: 'date.truncate' |
| [1] Should truncate date #51 | refused (400): Unknown function: 'date.truncate' |
| [2] Should truncate datetime #1 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #2 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #3 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #4 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #5 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #6 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #7 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #8 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #9 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #10 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #11 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #12 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #13 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #14 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #15 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #16 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #17 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #18 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #19 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #20 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #21 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #22 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #23 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #24 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #25 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #26 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #27 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #28 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #29 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #30 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #31 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #32 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #33 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #34 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #35 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #36 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #37 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #38 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #39 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #40 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #41 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #42 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #43 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #44 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #45 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #46 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #47 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #48 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #49 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #50 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #51 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #52 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #53 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #54 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #55 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #56 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #57 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #58 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #59 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #60 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #61 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #62 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #63 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #64 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #65 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #66 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #67 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #68 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #69 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #70 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #71 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #72 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #73 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #74 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #75 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #76 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #77 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #78 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #79 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #80 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #81 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #82 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #83 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #84 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #85 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #86 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #87 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #88 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #89 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #90 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #91 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #92 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #93 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #94 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #95 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #96 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #97 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #98 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #99 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #100 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #101 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #102 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #103 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #104 | refused (400): Unknown function: 'datetime.truncate' |
| [2] Should truncate datetime #105 | refused (400): Unknown function: 'datetime.truncate' |
| [3] Should truncate localdatetime #1 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #2 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #3 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #4 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #5 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #6 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #7 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #8 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #9 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #10 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #11 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #12 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #13 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #14 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #15 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #16 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #17 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #18 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #19 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #20 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #21 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #22 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #23 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #24 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #25 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #26 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #27 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #28 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #29 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #30 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #31 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #32 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #33 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #34 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #35 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #36 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #37 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #38 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #39 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #40 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #41 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #42 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #43 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #44 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #45 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #46 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #47 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #48 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #49 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #50 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #51 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #52 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #53 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #54 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #55 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #56 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #57 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #58 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #59 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #60 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #61 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #62 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #63 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #64 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #65 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #66 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #67 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #68 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #69 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #70 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #71 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #72 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #73 | refused (400): Unknown function: 'localdatetime.truncate' |
| [3] Should truncate localdatetime #74 | refused (400): Unknown function: 'localdatetime.truncate' |
| [4] Should truncate localtime #1 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #2 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #3 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #4 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #5 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #6 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #7 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #8 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #9 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #10 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #11 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #12 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #13 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #14 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #15 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #16 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #17 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #18 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #19 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #20 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #21 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #22 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #23 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #24 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #25 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #26 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #27 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #28 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #29 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #30 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #31 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #32 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #33 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #34 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #35 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #36 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #37 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #38 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #39 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #40 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #41 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #42 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #43 | refused (400): Unknown function: 'localtime.truncate' |
| [4] Should truncate localtime #44 | refused (400): Unknown function: 'localtime.truncate' |
| [5] Should truncate time #1 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #2 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #3 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #4 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #5 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #6 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #7 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #8 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #9 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #10 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #11 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #12 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #13 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #14 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #15 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #16 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #17 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #18 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #19 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #20 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #21 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #22 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #23 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #24 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #25 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #26 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #27 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #28 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #29 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #30 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #31 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #32 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #33 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #34 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #35 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #36 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #37 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #38 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #39 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #40 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #41 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #42 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #43 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #44 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #45 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #46 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #47 | refused (400): Unknown function: 'time.truncate' |
| [5] Should truncate time #48 | refused (400): Unknown function: 'time.truncate' |

</details>

<details><summary>TypeConversion1 - To Boolean (1)</summary>

| Case | Reason |
| --- | --- |
| [4] `toBoolean()` on invalid strings | rows differ: got [[None], [None], [True]] |

</details>

<details><summary>TypeConversion2 - To Integer (5)</summary>

| Case | Reason |
| --- | --- |
| [1] `toInteger()` on float | columns ['case'] != ['toInteger(weight)'] |
| [2] `toInteger()` returning null on non-numerical string | refused (500): Execution failed: invalid input syntax for type bigint: "foo" |
| [3] `toInteger()` handling mixed number types | rows differ: got [[[2, 3]]] |
| [4] `toInteger()` handling Any type | rows differ: got [[[2, 3, 2]]] |
| [5] `toInteger()` on a list of strings | rows differ: got [[[2, 3, None]]] |

</details>

<details><summary>TypeConversion3 - To Float (1)</summary>

| Case | Reason |
| --- | --- |
| [2] `toFloat()` returning null on non-numerical string | refused (500): Execution failed: invalid input syntax for type double precision: "foo" |

</details>

<details><summary>TypeConversion4 - To String (1)</summary>

| Case | Reason |
| --- | --- |
| [5] `toString()` should work on Any type | refused (400): The engine refused the query: Binder Error: Cannot deduce template type 'T' in function: 'list_value(T, [T...]) -> T[]'
Type 'T' was inferred to be:
 - 'INTEGER_LITERAL', from first occurrence
 - 'DEC |

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
