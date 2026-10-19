# Cypher 查詢支援 {: #cypher-query-support }

Provisa 透過 `provisa/cypher/` 模組將 openCypher 的子集轉譯為 SQL。 查詢由一個自訂的遞迴下降剖析器剖析（不使用外部 Cypher 程式庫），對照模型進行結構描述解析，轉譯為 SQL 後，再路由至目標執行引擎。

## 已實作功能 {: #implemented-features }

### 子句 {: #clauses }

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
| `CALL db.labels()` | ✓ | 自模型回傳節點標籤；無 SQL 轉譯 |
| `CALL db.relationshipTypes()` | ✓ | 自模型回傳關係型別 |
| `CALL db.propertyKeys()` | ✓ | 回傳所有節點型別中的所有屬性鍵名稱 |
| `UNWIND` | ✓ | 陣列展開為資料列；第一個項目成為 FROM，後續項目成為 CROSS JOIN UNNEST |

### 比對模式 {: #match-patterns }

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

### 運算式與述詞 {: #expressions-and-predicates }

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

### Map 投影 {: #map-projections }

| 語法 | SQL 對應 |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])`——自結構描述展開 |
| `n { .*, extra: expr }` | 所有結構描述屬性加上具名鍵值；合併的 MAP |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

**物件屬性。** 持有已宣告物件的欄（遠端 GraphQL 類型的嵌入物件，或任何帶物件欄位登記的欄）是映射屬性：`RETURN n.address` 以映射返回它，無論引擎以何種形式返回，節點 `n` 也在其屬性中攜帶它。指向已登記類型的遠端 GraphQL 欄位（一個物件或一個列表）不是屬性；透過其關聯到達。

### 聚合函式 {: #aggregation-functions }

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

### 字串函式 {: #string-functions }

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

### 型別轉換函式 {: #type-conversion-functions }

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`、`toIntegerOrNull`、`toFloatOrNull`、`toBooleanOrNull` | `TRY_CAST` 各變體 |

### 數學函式 {: #math-functions }

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)`（自然對數） |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`、`sqrt`、`ceil`、`floor`、`round`、`sign` | 直接傳遞 |

### 清單函式 {: #list-functions }

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### 清單推導式 {: #list-comprehensions }

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

### 模式推導式 {: #pattern-comprehensions }

| 語法 | SQL 對應 |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | 型別自模型推斷；相同的 ARRAY 子查詢型態 |

### 相關 CALL 子查詢 {: #correlated-call-subqueries }

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` 轉譯為 `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`。 規則：

- 外層作用域變數（`x`）必須出現於 `WITH` 中
- 支援多個匯入變數（`WITH a, b`）
- 內層 MATCH 中，來源為 lateral-bound 變數的第一個關係，決定了內層的 `FROM` 及 JOIN 條件
- 非相關的頂層 `CALL { ... }` 區塊（無 `WITH`）由 `cypher_calls_to_sql_list` 處理

---

## 寫入 {: #writes }

Cypher 透過 `/data/cypher` 端點支援三種寫入模式，由 `provisa/cypher/write_translator.py` 執行。

| Cypher | SQL |
|---|---|
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` |

屬性名稱經由領域前綴剝除及別名解析對應至欄位；Cypher 純量值會被強制轉換為目標欄位型別。 回應本文帶有一個 `affected_rows` 計數。

節點的屬性是其欄位的發佈名稱：欄位有別名時用別名，否則用命名慣例下的名稱——即 GraphQL 和 SQL 讀取它所用的名稱——在 `MATCH`、`RETURN`、`CREATE`、`SET` 和 `MERGE` 中一致。

規則：

- 該標籤必須恰好解析至一個已註冊的資料表。含糊或未知的標籤即為硬性錯誤；不進行模糊比對。 無法透過 Cypher 建立新標籤或型別。
- 每次寫入均受目標資料表寫入授權的閘控：角色需要 `write` 權利和該操作的授權，且寫入的欄位必須在授權中。不符合的角色會於編譯時被拒絕。
- 該後端數據來源連接器必須支援 DML。唯讀數據來源（Trino 聯邦、無 Delta 連接器的 Iceberg）會於轉譯時拒絕寫入。
- 關係無法被寫入：它們衍生自模型中宣告的 JOIN，而非儲存的邊。以關係為目標即為硬性錯誤。 由聯結資料表支撐的邊也不例外：其背後的關聯資料表本身就是一張已註冊的資料表，寫入的資料列進入那張資料表，而不是進入邊。
- 寫入會經過完整的寫入管線：RLS 注入及變異後掛鉤（回應快取失效、具體化檢視標記過期、Kafka 變更事件、熱資料表重新載入）。
- `MERGE`、`DETACH DELETE` 及 `REMOVE` 不受支援，並於剖析時遭拒絕。

---

## 協定存取 {: #protocol-access }

Cypher 經由兩種傳輸方式抵達相同的受治理管線：

- **HTTP**——`POST /data/cypher`，帶一個 JSON 本文（`{"query": "...", "params": {...}}`）。回傳具型別的資料列，或於寫入時回傳 `affected_rows`。`RETURN` 子句中的圖形變數序列化為 JSON：節點帶有 `id`、`label`、`tableLabel` 及 `properties`；邊帶有 `identity`、`start`、`end`、`type`、`properties`、`startNode` 及 `endNode`；路徑帶有 `nodes`、`edges` 及 `length`/`hops`。 已註冊的命令亦可於此處經 `CALL fn(args) YIELD col1, col2` 呼叫，位置式引數依序對應至該命令所宣告的引數名稱。
- **Bolt**——一個相容於 Neo4j 的二進位協定伺服器（PackStream 編解碼器、分塊 framing），讓 Neo4j Browser、Bloom 及 Bolt 驅動程式可於聯邦圖形上執行 Cypher。 當 `PROVISA_BOLT_PORT` 設為非零值時啟動，預設為停用；TLS 請設定 `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY`。 Bolt 驗證將主體對應至使用者，將資料庫對應至角色：`SHOW DATABASES` 會為每個（檢視 × 角色）配對列出一筆項目，命名為 `provisa_<role>`（業務領域）或 `provisa_ops_<role>`（含系統/中繼/ops 領域）；`:use` 選取使用中的角色及檢視。 關係經由一個 `rel_ids` 資料表取得持久性整數 ID，其設計仿照 `node_ids`。 已註冊的命令可以 `CALL command(args)` 呼叫，位置式引數依序對應至已宣告的引數名稱；`CALL dbms.*` / `CALL db.*` 程序具優先權。

### 圖形分析 {: #graph-analytics }

`POST /data/graph-analytics` 會執行一個 Cypher 查詢，自所得的節點及邊建立一個記憶體內的 NetworkX 圖形，執行一個具名演算法，並在以 JSON 形式回傳每個節點及邊之前，將一個 `_analytics` 字典合併入其中，並附帶一個 `elapsed_ms` 欄位。 `_analytics` 鍵值依演算法而異：中心性得出 `score`；社群偵測得出 `cluster`；k-core 得出 `core_number`；度中心性加入 `in_degree` 及 `out_degree`。 該端點對超過可設定大小上限（預設 10,000 個節點／50,000 條邊）的圖形以 HTTP 413 拒絕；Girvan-Newman 上限為 500 個節點，除非呼叫端傳入 `force=true`。

---

## 限制 {: #limitations }

### 設計限制 {: #design-constraints }

1. **寫入僅限於 `CREATE`、`SET` 及 `DELETE`。**這些會經由與 GraphQL 及 SQL 變異相同的管線，作為直接資料表寫入執行。 見上方「§寫入」小節。`MERGE`、`DETACH DELETE` 及 `REMOVE` 於剖析時遭拒絕。 APOC 程序亦遭拒絕。

2. **關係屬性只存在於由聯結資料表支撐的邊上。** 基於一對外鍵欄宣告的邊，在模型中只作為連接中繼資料存在，不儲存任何屬性，所以 `WHERE r.since > 2020` 或 `RETURN r.weight` 對它沒有意義。基於聯結資料表宣告的邊則帶有屬性。關聯表的其餘欄就是該關係的屬性，`RETURN r` 會傳回它們。對其中某個屬性的 `WHERE` 會編譯為聯結資料表別名上的謂詞，因此它約束的是走訪本身，而不是在組裝後篩選列。聯結資料表會從圖結構描述的節點一側去掉：它在這裏是邊，在其他地方都是資料表。

3. **雙向走訪** `(a)-[]-(b)` 會改寫為模型中所有符合的有向關係的正向與反向 UNION ALL。模型中的每個關係都有方向，雙向語法只是展開為兩個方向的語法糖。額外的分支在查詢的最外層產生。同一查詢中後續的 MATCH 模式不會在各分支中複製，這限制了含多個 MATCH 子句的查詢中的雙向模式。

4. **遞迴路徑須具上界。**可變長度模式（`[*]`）必須含有上界（例如 `[*..10]`）。 無界走訪會於剖析時遭拒絕，以防止失控的遞迴 CTE。

### 行為說明 {: #behaviour-notes }

5. **非自我參照路徑上的 `shortestPath` 使用扁平 JOIN，不按跳數排序。** 當起點與終點類型不同、且結構描述中不存在自我參照關係時，轉換器會沿最短的結構描述路徑產生一條扁平 JOIN 鏈。該程式碼路徑不追蹤跳數，所以不會產生 `ORDER BY hops`。結果是結構上最短的結構描述路徑，它可能不同於跨多列資料的最短路徑。

6. **多條結構描述路徑會產出 `UNION ALL`。**當兩條跳數相同的結構描述路徑連接相同的起訖型別時（例如 `Person -[WORKS_AT]-> Company` 及 `Person -[MANAGES]-> Company`），兩者均以 `UNION ALL` 分支發出。 不會對兩分支中皆出現的資料列進行去重複。

7. **每對來源→目標及 rel_type 組合僅有一個 `RelationshipMapping`。**若同一來源型別上的兩個 GraphQL 欄位，對同一目標型別產出相同的 `rel_type` 字串（轉大寫後），則第二次註冊會覆寫 `CypherLabelMap.relationships` 中的第一次註冊。該關係鍵值包含來源及目標型別名稱，因此具相同型別名稱的相異來源/目標配對，各自擁有獨立項目，不受影響。

8. **`WITH` 子句 CTE 命名為 `_w0`、`_w1`、……** 名稱於單次轉譯呼叫內依位置指派。若將多個已轉譯查詢組合（例如批次處理）而以樸素方式串接，可能產生 CTE 名稱衝突。

### 運算式與模式涵蓋範圍 {: #expression-and-pattern-coverage }

Cypher 運算式會被剖析為一個 AST，並逐節點降階為 SQL（`provisa/cypher/expr_parser.py`、`provisa/cypher/expr_visitor.py`）。該文法遵循 openCypher 的 `oC_Expression` 優先權層級。支援項目：常值、參數、屬性存取、`n.prop`、索引與切片、算術運算（`+ - * / % ^`）、比較、`IN`、`STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`、`IS [NOT] NULL`、布林 `AND` / `OR` / `XOR` / `NOT`、`CASE`、清單及 map 常值、清單及模式推導式（包含 `p = (…)` 路徑繫結）、map 投影、`reduce`、`all` / `any` / `none` / `single` 量詞、存在性子查詢，以及函式呼叫。

9. **標籤為固定值；您無法透過 Cypher 建立物件型別。** 標籤會解析為已知領域、已知物件型別或限定形式的 `domain:object_type`，也就是已登錄結構描述定義的封閉集合。Cypher 從不引入新的標籤或型別。您只能為可寫入資料來源中已定義的型別建立實例：`CREATE` 會向這類資料表寫入列（見 §Writes），但不能定義新的標籤或型別。兩種標籤形式都可接受，表示同一個判斷：後綴形式 `n:Label` 及完整形式 `n IS :Label`（及其否定 `n IS NOT :Label`）。限定標籤寫作 `n:domain:object_type`。

10. **`shortestPath` 及 `allShortestPaths` 只能在 `MATCH` 內部使用。** 在模式中（`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`），它們會轉換為 `WITH RECURSIVE` CTE，並要求起點及終點節點帶標籤。它們不支援出現在運算式位置，例如 `RETURN shortestPath((a)-[*]->(b))` 或 `WHERE length(shortestPath((a)-[*]->(b))) < 5`。遞迴改寫由 `MATCH` 子句驅動，沒有相關子查詢可以承載它。

11. **清單推導式、`REDUCE` 及量詞均作用於清單值；模式推導式則進行走訪。** `reduce(...)`、`all/any/none/single(...)` 以及清單推導式 `[x IN list | …]` 作用於清單運算式，並下降為引擎的高階清單函數。它們本身不走訪圖。**模式**推導式 `[(a)-[:R]->(b) WHERE p | e]` 則會走訪：它的圖模式按相關子查詢處理，所以它的來源就是一次走訪。要把走訪結果送入清單形式，請使用 `nodes(p)` / `relationships(p)` / `collect(...)`，或直接使用模式推導式。
