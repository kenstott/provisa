# Cypher 查詢支援

Provisa 透過 `provisa/cypher/` 模組將 openCypher 的子集轉譯為 SQL。(REQ-345、REQ-347) 查詢由一個自訂的遞迴下降剖析器剖析（不使用外部 Cypher 程式庫）(REQ-571)，對照語意層進行結構描述解析 (REQ-351)，轉譯為 SQL 後，再路由至目標執行引擎。(REQ-066、REQ-067、REQ-347)

## 已實作功能

### 子句

| 子句 | 狀態 | 備註 |
| -------- | -------- | ------- |
| `MATCH (n:Label)` | ✓ | 具標籤、變數、內嵌屬性的節點模式 |
| `OPTIONAL MATCH` | ✓ | 產出 LEFT JOIN |
| `WHERE` | ✓ | 完整運算式支援；於 MATCH 之後套用 |
| `RETURN` | ✓ | 星號、屬性存取、運算式、別名 |
| `RETURN DISTINCT` | ✓ | 產出 SELECT DISTINCT |
| `WITH` | ✓ | 產出一個具名 CTE（`_w0`、`_w1`、……）；支援 `WITH … WHERE` |
| `ORDER BY` | ✓ | ASC / DESC |
| `SKIP` / `LIMIT` | ✓ | 對應至 SQL 的 OFFSET / LIMIT |
| `UNION` / `UNION ALL` | ✓ | 跨子 AST 的遞迴聯集 |
| `CALL { … }` | ✓ | 經 `cypher_calls_to_sql_list` 進行頂層 call 子查詢分解 |
| `CALL { WITH x … }` | ✓ | 相關子查詢 → `CROSS JOIN LATERAL`；見「§相關 CALL」小節 |
| `CALL db.labels()` | ✓ | 自語意層回傳節點標籤；無 SQL 轉譯 (REQ-572) |
| `CALL db.relationshipTypes()` | ✓ | 自語意層回傳關係型別 (REQ-572) |
| `CALL db.propertyKeys()` | ✓ | 回傳所有節點型別中的所有屬性鍵名稱 (REQ-572) |
| `UNWIND` | ✓ | 陣列展開為資料列；第一個項目成為 FROM，後續項目成為 CROSS JOIN UNNEST |

### 比對模式

| 模式 | 狀態 | 備註 |
| --------- | -------- | ------- |
| `(n)` — 無標籤節點 | ✓ | 對所有已知型別執行 UNION ALL |
| `(n:Label)` | ✓ | 對應至該 GraphQL 型別所註冊的資料表 |
| `(n:Label {prop: val})` | ✓ | 內嵌屬性篩選條件成為 WHERE |
| `(a)-[:TYPE]->(b)` | ✓ | 有向、單跳 |
| `(a)<-[:TYPE]-(b)` | ✓ | 反向走訪；JOIN 欄位反轉 |
| `(a)-[]->(b)` | ✓ | 任意方向的 a→b 關係；若符合多個型別則 UNION ALL |
| `(a)-[]-(b)` | ✓ | 雙向；展開為所有正向及反向關係的 UNION ALL |
| `(a)-[:TYPE*..N]->(b)` | ✓ | 具上界的可變長度；自我參照時使用遞迴 CTE，否則使用扁平 JOIN |
| `(a)-[]->(b)-[]->(c)` | ✓ | 多跳鏈式 JOIN |
| `(n:DomainLabel)` | ✓ | 領域標籤 → 對該領域內所有型別執行 UNION ALL 子查詢 |
| `(n:A\|B)` | ✓ | 標籤替代 → 將特設領域注入標籤對應表；對符合的型別執行 UNION ALL |
| `shortestPath(…)` | ✓ | 異質端點使用扁平 JOIN；同型別／自我參照使用 WITH RECURSIVE CTE |
| `allShortestPaths(…)` | ✓ | 與 shortestPath 相同，惟不含 LIMIT 1 |

### 運算式與述詞

| 功能 | 狀態 | SQL 對應 |
| --------- | -------- | ------------ |
| 屬性存取 `n.prop` | ✓ | `n."prop"` |
| 參數 `$name` | ✓ | 位置式 `$N` |
| 舊式參數 `{name}` | ✓ | 於剖析時正規化為 `$name` |
| 比較 `=`、`<>`、`<`、`>`、`<=`、`>=` | ✓ | 直接對應 |
| `AND`、`OR`、`NOT` | ✓ | 直接對應 |
| `IS NULL` / `IS NOT NULL` | ✓ | 直接對應 |
| `IN [list]` | ✓ | SQL IN；Cypher 的 `[...]` 中括號語法改寫為 `(...)` |
| `STARTS WITH` | ✓ | `starts_with(col, val)` |
| `ENDS WITH` | ✓ | `col LIKE CONCAT('%', val)` |
| `CONTAINS` | ✓ | `strpos(col, val) > 0` |
| `=~` 正規表達式 | ✓ | `regexp_like(col, pattern)` |
| `exists(n.prop)` | ✓ | `(n.prop) IS NOT NULL` |
| `EXISTS { MATCH … }` | ✓ | 相關的 `EXISTS (SELECT 1 FROM …)` 子查詢 |
| `COUNT { MATCH … }` | ✓ | 相關的 `(SELECT count(*) FROM …)` 子查詢 |
| `COLLECT { MATCH … RETURN x }` | ✓ | 相關的 `ARRAY(SELECT x FROM …)` 子查詢 |
| `id(n)` | ✓ | 解析為該節點所設定的 ID 欄位 |
| `labels(n)` | ✓ | `ARRAY['Label']` |
| `keys(n)` | ✓ | `ARRAY['prop1', 'prop2', …]` |
| `type(r)` | ✓ | 於編譯時解析為 `'REL_TYPE'` 字串常值；無執行時欄位 |
| `length(p)` | ✓ | 遞迴 CTE 路徑用 `_t.hops`；扁平 JOIN 路徑用 `1` |
| `CASE WHEN … THEN … ELSE … END` | ✓ | 直接對應（搜尋式及簡式兩種形式） |
| 隱含 GROUP BY | ✓ | 當任一項目含有聚合函式時，未聚合的 RETURN 項目成為 GROUP BY 鍵值 |

### Map 投影

| 語法 | SQL 對應 |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])`——自結構描述展開 |
| `n { .*, extra: expr }` | 所有結構描述屬性加上具名鍵值；合併的 MAP |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

### 聚合函式

| Cypher | SQL |
| -------- | ----- |
| `count(*)`、`count(x)` | 直接對應 |
| `count(DISTINCT x)` | `count(DISTINCT x)` |
| `collect(x)` | `array_agg(x)` |
| `avg`、`sum`、`min`、`max` | 直接對應 |
| `stDev(x)` | `stddev_samp(x)` |
| `stDevP(x)` | `stddev_pop(x)` |
| `percentileCont(x, p)` | `approx_percentile(x, p)` |
| `percentileDisc(x, p)` | `approx_percentile(x, p)` |

### 字串函式

| Cypher | SQL |
| -------- | ----- |
| `toLower(x)` | `lower(x)` |
| `toUpper(x)` | `upper(x)` |
| `ltrim(x)`、`rtrim(x)`、`trim(x)` | 直接對應 |
| `replace(x, a, b)` | 直接對應 |
| `reverse(x)` | 直接對應 |
| `split(x, d)` | 直接對應 |
| `left(x, n)` | `left(x, n)` |
| `right(x, n)` | `right(x, n)` |
| `substring(x, start, len)` | `substr(x, start+1, len)`（0→1 索引） |
| `size(string)` | `char_length(string)` |
| `size(list)` | `cardinality(list)` |

### 型別轉換函式

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`、`toIntegerOrNull`、`toFloatOrNull`、`toBooleanOrNull` | `TRY_CAST` 各變體 |

### 數學函式

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)`（自然對數） |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`、`sqrt`、`ceil`、`floor`、`round`、`sign` | 直接傳遞 |

### 清單函式

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### 清單推導式

| 語法 | SQL 對應 |
| -------- | ------------ |
| `[x IN list \| f(x)]` | `transform(list, x -> f(x))` |
| `[x IN list WHERE p(x)]` | `filter(list, x -> p(x))` |
| `[x IN list WHERE p(x) \| f(x)]` | `transform(filter(list, x -> p(x)), x -> f(x))` |
| `any(x IN list WHERE p(x))` | `any_match(list, x -> p(x))` |
| `all(x IN list WHERE p(x))` | `all_match(list, x -> p(x))` |
| `none(x IN list WHERE p(x))` | `none_match(list, x -> p(x))` |
| `single(x IN list WHERE p(x))` | `cardinality(filter(list, x -> p(x))) = 1` |
| `reduce(acc = init, x IN list \| expr)` | `reduce(list, init, (acc, x) -> expr, acc -> acc)` |

### 模式推導式

| 語法 | SQL 對應 |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | 型別自語意層推斷；相同的 ARRAY 子查詢型態 |

### 相關 CALL 子查詢

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` 轉譯為 `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`。(REQ-573) 規則：

- 外層作用域變數（`x`）必須出現於 `WITH` 中
- 支援多個匯入變數（`WITH a, b`）
- 內層 MATCH 中，來源為 lateral-bound 變數的第一個關係，決定了內層的 `FROM` 及 JOIN 條件
- 非相關的頂層 `CALL { ... }` 區塊（無 `WITH`）由 `cypher_calls_to_sql_list` 處理

---

## 寫入

Cypher 透過 `/data/cypher` 端點支援三種寫入模式，由 `provisa/cypher/write_translator.py` 執行。(REQ-818) [tool-verified: `provisa/api/rest/cypher_router.py:415-545`]

| Cypher | SQL | 需求 |
| -------- | ----- | ----- |
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` | REQ-666 |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` | REQ-667 |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` | REQ-668 |

屬性名稱經由領域前綴剝除及別名解析對應至欄位；Cypher 純量值會被強制轉換為目標欄位型別。(REQ-666、REQ-668) 回應本文帶有一個 `affected_rows` 計數。(REQ-670)

規則：

- 該標籤必須恰好解析至一個已註冊的資料表。含糊或未知的標籤即為硬性錯誤；不進行模糊比對。(REQ-661) 無法透過 Cypher 建立新標籤或型別。(REQ-662)
- 每次寫入均受目標資料表的 `writable_by` ACL 閘控；不具寫入權限的角色會於編譯時被拒絕。(REQ-663)
- 該後端數據來源連接器必須支援 DML。唯讀數據來源（Trino 聯邦、無 Delta 連接器的 Iceberg）會於轉譯時拒絕寫入。(REQ-664)
- 關係無法被寫入——它們衍生自語意層中宣告的 JOIN，而非儲存的邊。以關係為目標即為硬性錯誤。(REQ-665) 由聯結資料表支撐的邊也不例外：其背後的關聯資料表本身就是一張已註冊的資料表，寫入的資料列進入那張資料表，而不是進入邊。(REQ-1586)
- 寫入會經過完整的寫入管線：RLS 注入及變異後掛鉤（回應快取失效、具體化檢視標記過期、Kafka 變更事件、熱資料表重新載入）。(REQ-798)
- `MERGE`、`DETACH DELETE` 及 `REMOVE` 不受支援，並於剖析時遭拒絕。(REQ-671)

---

## 協定存取

Cypher 經由兩種傳輸方式抵達相同的受治理管線：

- **HTTP**——`POST /data/cypher`，帶一個 JSON 本文（`{"query": "...", "params": {...}}`）。回傳具型別的資料列，或於寫入時回傳 `affected_rows`。`RETURN` 子句中的圖形變數序列化為 JSON：節點帶有 `id`、`label`、`tableLabel` 及 `properties`；邊帶有 `identity`、`start`、`end`、`type`、`properties`、`startNode` 及 `endNode`；路徑帶有 `nodes`、`edges` 及 `length`/`hops`。(REQ-750) 已註冊的命令亦可於此處經 `CALL fn(args) YIELD col1, col2` 呼叫——位置式引數依序對應至該命令所宣告的引數名稱。(REQ-1156) [tool-verified: `provisa/api/rest/registered_call.py:113-143`]
- **Bolt**——一個相容於 Neo4j 的二進位協定伺服器（PackStream 編解碼器、分塊 framing），讓 Neo4j Browser、Bloom 及 Bolt 驅動程式可於聯邦圖形上執行 Cypher。(REQ-802) 當 `PROVISA_BOLT_PORT` 設為非零值時啟動，預設為停用；TLS 請設定 `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY`。[tool-verified: `provisa/api/app_startup.py:317-338`] Bolt 驗證將主體對應至使用者，將資料庫對應至角色：`SHOW DATABASES` 會為每個（檢視 × 角色）配對列出一筆項目，命名為 `provisa_<role>`（業務領域）或 `provisa_ops_<role>`（含系統/中繼/ops 領域）；`:use` 選取使用中的角色及檢視。(REQ-807) 關係經由一個 `rel_ids` 資料表取得持久性整數 ID，其設計仿照 `node_ids`。(REQ-806) 已註冊的命令可以 `CALL command(args)` 呼叫——位置式引數依序對應至已宣告的引數名稱；`CALL dbms.*` / `CALL db.*` 程序具優先權。(REQ-1156) [tool-verified: `provisa/bolt/session.py:722-749`]

### 圖形分析

`POST /data/graph-analytics` 會執行一個 Cypher 查詢，自所得的節點及邊建立一個記憶體內的 NetworkX 圖形，執行一個具名演算法，並在以 JSON 形式回傳每個節點及邊之前，將一個 `_analytics` 字典合併入其中，並附帶一個 `elapsed_ms` 欄位。(REQ-642) `_analytics` 鍵值依演算法而異：中心性得出 `score`；社群偵測得出 `cluster`；k-core 得出 `core_number`；度中心性加入 `in_degree` 及 `out_degree`。(REQ-643) 該端點對超過可設定大小上限（預設 10,000 個節點／50,000 條邊）的圖形以 HTTP 413 拒絕；Girvan-Newman 上限為 500 個節點，除非呼叫端傳入 `force=true`。(REQ-650、REQ-651)

---

## 限制

### 設計限制

1. **寫入僅限於 `CREATE`、`SET` 及 `DELETE`。**這些會經由與 GraphQL 及 SQL 變異相同的管線，作為直接資料表寫入執行。(REQ-818、REQ-666、REQ-667、REQ-668) 見下方「§寫入」小節。`MERGE`、`DETACH DELETE` 及 `REMOVE` 於剖析時遭拒絕。(REQ-671、REQ-818) APOC 程序亦遭拒絕。

2. **關係屬性只存在於由聯結資料表支撐的邊上。**在一對外部索引鍵欄位上宣告的邊，於語意層中僅作為 JOIN 中繼資料存在（REQ-574），不帶有任何儲存屬性，因此 `WHERE r.since > 2020` 或 `RETURN r.weight` 對它並無意義。而在聯結資料表上宣告的邊確實帶有屬性：關聯資料表其餘的欄位就是該關係的屬性，`RETURN r` 會回傳它們，對其中之一的 `WHERE` 會編譯成作用於聯結資料表別名的述詞——因此它約束的是走訪本身，而不是過濾已組裝好的資料列。(REQ-1586) 聯結資料表本身會從圖綱要的節點一側移除；它在這裡是一條邊，在其他任何地方都是一張資料表。

3. **雙向走訪** `(a)-[]-(b)` 改寫為語意層中所有符合的有向關係之正向+反向 UNION ALL。(REQ-575) 語意層中的每個關係均具方向性；雙向語法是展開為兩個方向的語法糖。額外分支會於最外層查詢層級發出——同一查詢中後續的 MATCH 模式不會於各分支間重複（此為多重 MATCH 雙向情境的限制）。

4. **遞迴路徑須具上界。**可變長度模式（`[*]`）必須含有上界（例如 `[*..10]`）。(REQ-348) 無界走訪會於剖析時遭拒絕，以防止失控的遞迴 CTE。

### 行為說明

5. **非自我參照路徑上的 `shortestPath` 使用扁平 JOIN，而非跳數排序。**當起訖型別不同，且結構描述中無自我參照關係時，轉譯器會產出一個扁平 JOIN 鏈（結構描述最短路徑）。(REQ-576) 它不會發出 `ORDER BY hops`，因為該程式碼路徑並未追蹤跳數。所得結果是結構上最短的結構描述路徑，而非跨多列資料的數據最短路徑。

6. **多條結構描述路徑會產出 `UNION ALL`。**當兩條跳數相同的結構描述路徑連接相同的起訖型別時（例如 `Person -[WORKS_AT]-> Company` 及 `Person -[MANAGES]-> Company`），兩者均以 `UNION ALL` 分支發出。(REQ-577) 不會對兩分支中皆出現的資料列進行去重複。

7. **每對來源→目標及 rel_type 組合僅有一個 `RelationshipMapping`。**若同一來源型別上的兩個 GraphQL 欄位，對同一目標型別產出相同的 `rel_type` 字串（轉大寫後），則第二次註冊會覆寫 `CypherLabelMap.relationships` 中的第一次註冊。該關係鍵值包含來源及目標型別名稱，因此具相同型別名稱的相異來源/目標配對，各自擁有獨立項目，不受影響。

8. **`WITH` 子句 CTE 命名為 `_w0`、`_w1`、……**(REQ-578) 名稱於單次轉譯呼叫內依位置指派。若將多個已轉譯查詢組合（例如批次處理）而以樸素方式串接，可能產生 CTE 名稱衝突。

### 運算式與模式涵蓋範圍 (REQ-913)

Cypher 運算式會被剖析為一個 AST，並逐節點降階為 SQL（`provisa/cypher/expr_parser.py`、`provisa/cypher/expr_visitor.py`）。該文法遵循 openCypher 的 `oC_Expression` 優先權層級。支援項目：常值、參數、屬性存取、`n.prop`、索引與切片、算術運算（`+ - * / % ^`）、比較、`IN`、`STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`、`IS [NOT] NULL`、布林 `AND` / `OR` / `XOR` / `NOT`、`CASE`、清單及 map 常值、清單及模式推導式（包含 `p = (…)` 路徑繫結）、map 投影、`reduce`、`all` / `any` / `none` / `single` 量詞、存在性子查詢，以及函式呼叫。

9. **標籤為固定值；您無法透過 Cypher 建立物件型別。**一個標籤會解析至一個已知領域、一個已知物件型別，或一個限定的 `domain:object_type`——即已註冊結構描述所定義的封閉集合。Cypher 絕不會引入新標籤或型別。僅能對已在可寫入數據來源中定義的型別建立實例；`CREATE` 會將資料列寫入此類資料表（見「§寫入」小節），但無法定義新標籤或型別。(REQ-662) 兩種標籤形式皆獲接受，且意義相同：後綴式 `n:Label` 及冗長式 `n IS :Label`（及其否定形式 `n IS NOT :Label`）。限定標籤寫作 `n:domain:object_type`。

10. **`shortestPath` 及 `allShortestPaths` 僅於 `MATCH` 內受支援，不可作為運算式使用。**於模式中（`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`）它們會轉譯為一個 `WITH RECURSIVE` CTE，並須具有已加標籤的來源及目標節點。若用於運算式位置——例如 `RETURN shortestPath((a)-[*]->(b))` 或 `WHERE length(shortestPath((a)-[*]->(b))) < 5`——則不受支援，因為該遞迴改寫是由 `MATCH` 子句驅動，而非相關子查詢。

11. **清單推導式、`REDUCE` 及量詞均作用於清單值；模式推導式則進行走訪。**`reduce(...)`、`all/any/none/single(...)` 及清單推導式 `[x IN list | …]` 均作用於一個清單運算式，並降階為引擎的高階清單函式——它們本身並不走訪圖形。**模式**推導式 `[(a)-[:R]->(b) WHERE p | e]` 則會進行走訪：其圖形模式會被轉為一個相關子查詢處理，因此它是一個以走訪作為來源的推導式。若要將走訪結果餵入清單形式，請使用 `nodes(p)` / `relationships(p)` / `collect(...)`，或直接使用模式推導式。
