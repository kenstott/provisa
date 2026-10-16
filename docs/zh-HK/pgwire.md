# Provisa pgwire 伺服器 {: #provisa-pgwire-server }

Provisa 公開一個 PostgreSQL 網絡協定（pgwire）端點。任何支援 PostgreSQL 客戶端協定的工具——psycopg2、asyncpg、DBeaver、Tableau、JDBC——都可以連接並透過與 HTTP API 相同的治理管線查詢 Provisa 數據。（REQ-266）

查詢會經過完整的治理堆疊：行級安全執行、遮罩規則、關係防護、域訪問檢查。（REQ-001、REQ-002、REQ-263）pgwire 介面並非繞過機制。（REQ-002、REQ-266）

---

## 連接詳情 {: #connection-details }

當 `PROVISA_PGWIRE_PORT` 設定為非零整數時，伺服器便會啟動。預設為停用狀態。（REQ-527）[tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS。** 將 `PROVISA_PGWIRE_CERT` 和 `PROVISA_PGWIRE_KEY` 設定為 PEM 證書及金鑰的路徑。兩者皆存在時，伺服器會將傳入連接以 TLS 包裝。兩者皆缺席時，TLS 為關閉狀態，伺服器會對 SSL 協商請求回覆 `N`。（REQ-530）[tool-verified: `server.py:1746-1750`]

**回報的伺服器版本。** 客戶端看到的版本為 `14.0.provisa`。根據版本號啟用功能的工具，其行為可能如同連接至 PostgreSQL 14。（REQ-579）[tool-verified: `server.py:208`]

---

## 身份驗證 {: #authentication }

啟動封包會攜帶一個用戶名和一個密文欄位，但沒有任何方案說明該密文是什麼。Provisa 會根據密文本身判斷，因此客戶端除了 `user` 和 `password` 之外無需任何額外設定：

| 密文是 | 辨識依據 | 解析為 |
| --------------- | --------------- | ------------- |
| 個人存取權杖 | 其 `provisa_pat_` 前綴 | 該權杖的擁有者及其角色（REQ-1263） |
| OIDC／提供者簽發的 bearer 權杖 | 所設定的提供者是權杖提供者 | 該權杖所聲明的身分（REQ-890） |
| 密碼 | 其他任何情況 | 所設定提供者（`basic` 或 `simple`）中的帳戶 |

判斷只做一次。被選定驗證器拒絕的憑證不會再交給另一個驗證器重試，因此一次拒絕不會變成第二次猜測。

trust 模式（`provider: none`，或驗證中介軟件未啟用）是例外：用戶名會直接用作 `role_id`，密文會被忽略。請勿在未加密的連接上使用它。

**SCRAM-SHA-256。** 在 `provider: basic` 且 `auth.scram: true` 時，伺服器會公告帶 `SCRAM-SHA-256` 的 SASL（驗證代碼 10），密碼以證明方式驗證而非傳送。（REQ-1394）不提供 `SCRAM-SHA-256-PLUS`。對於尚未寫入驗證子的用戶——驗證子無法從 bcrypt 雜湊推導——伺服器會以模擬交換作答，使連線上看不出誰已遷移；該用戶會透過 TLS 上的明文密碼進行驗證，直到下次輸入密碼時寫入驗證子為止。關閉 `auth.scram` 時，伺服器使用 PG 驗證類型 3（明文密碼）。兩種情況下都不支援 MD5。

**客戶端憑證。** 設定 `PROVISA_MTLS_CLIENT_CA` 後，伺服器會在交握期間、檢查任何憑證之前驗證客戶端憑證。（REQ-1228）啟用 `PROVISA_MTLS_BIND_PRINCIPAL` 時，憑證的 common name 必須與該連接隨後用於驗證的 `user` 相同。參見[設定](configuration.md#mutual-tls)。

**失敗嘗試會被計數。** 五分鐘內五次失敗會將帳戶鎖定十五分鐘，且該計數器與 HTTP 和 Bolt 共用——在任一介面上招致的鎖定，在所有介面上都生效。（REQ-1393）

**選擇組織。** 在多組織部署中，連接至 `<org>.<your-domain>`，pgwire 會從 TLS ClientHello 的主機名稱中讀取組織，方式與 HTTP 從 `Host` 標頭中讀取相同。（REQ-1234）主機名稱是在請求某個組織，而不是授予它；在該組織中沒有成員資格的主體會被拒絕。透過 IP 位址連接則不請求任何組織。

---

## 支援的功能 {: #what-works }

### SELECT

所有 SELECT 陳述式都會經過治理管線（`_pipeline.py`）。（REQ-001、REQ-262、REQ-266）此管線會：

1. 將語意 SQL 重寫為實體 SQL（`rewrite_semantic_to_physical`）
2. 套用治理（行級安全、遮罩、域訪問）（REQ-263）
3. 對照已註冊的結構描述 (Schema) 進行驗證（REQ-011）
4. 路由至 Trino 或直接路由至來源池（REQ-027、REQ-028）

支援多陳述式的簡單查詢。以分號分隔的陳述式會被拆分並按順序執行。（REQ-580）[tool-verified: `server.py:318-381`]

在簡單查詢模式及擴展查詢（Bind/Execute）模式下均支援參數化查詢（`$1`、`$2`……）。參數會在執行前以字面值替換。（REQ-581）[tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` 及 `SELECT fn(args)`——其中 `fn` 指定一個已註冊且受追蹤的函式——會在治理管線之前被攔截，並透過唯一受治理的執行器（`invoke_tracked_function`）路由。結果為一個類型化的行集，與該指令在其他任何介面所返回的結果一致。`writable_by` 及治理規則會在執行器內強制執行。（REQ-1156）[tool-verified: `provisa/pgwire/function_call.py:74-88`]

### 游標（DECLARE / FETCH / MOVE / CLOSE） {: #cursors-declare-fetch-move-close }

Provisa 在簡單查詢協定上處理 SQL 游標的文字語法。（REQ-1862） 把 `DECLARE`/`FETCH`/`MOVE`/`CLOSE` 作為一般 SQL 陳述式傳送的用戶端，會得到真正的伺服器端串流，而不是協定錯誤或在用戶端記憶體中緩衝整個結果。

**這與以 portal 為基礎的串流不同。** 延伸查詢協定——帶有資料列上限欄位的 Bind/Execute——是另一種線路協定機制。asyncpg 及大多數現代 PostgreSQL 驅動程式會自動使用它，不需要游標 SQL。如果你的驅動程式已經使用 portal，你便不需要這種語法。`DECLARE CURSOR` 是為按字面傳送游標 SQL 的用戶端而設：psycopg2 的具名/伺服器端游標（`conn.cursor(name="...")`）、部分 BI 工具及一些較舊的 JDBC 驅動程式。

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```
內部的 SELECT 會經過 Provisa 一般的受治理管線——與任何獨立 SELECT 相同的行層級安全執行、相同的遮罩，以及相同的網域存取檢查。[tool-verified: `server.py:1367` — `ctx.execute_sql(inner_sql)`, the same call path as a plain SELECT] 宣告游標不會繞過治理。

`SCROLL` 關鍵字同時控制允許的導覽及游標的記憶體佔用：

| 宣告 | 向後導覽 | 記憶體成本 |
|---|---|---|
| `DECLARE c CURSOR FOR ...`（預設） | 不允許 | 限於一批 FETCH |
| `DECLARE c NO SCROLL CURSOR FOR ...` | 不允許 | 限於一批 FETCH |
| `DECLARE c SCROLL CURSOR FOR ...` | 允許 | 整個結果緩衝於 Python 記憶體 |

既沒有指定 `SCROLL` 也沒有指定 `NO SCROLL` 時，預設為 `NO SCROLL`——與 PostgreSQL 本身一致。[tool-verified: `server.py:1361-1362`] `NO SCROLL` 游標在每次向前推進後，都會從其內部緩衝區捨棄已消耗的資料列，因此對大型結果的 `FETCH FORWARD` 走訪，在任何時候都只在 Python 記憶體中保留大約一批數據。[tool-verified: `server.py:443-447`] `SCROLL` 游標會保留它曾經拉取的每一個資料列——這是支援 `FETCH BACKWARD`、`ABSOLUTE`、`FIRST` 及 `LAST` 的代價。在對大型結果宣告 `SCROLL` 游標之前，請為整個結果的大小預留 Python 堆積空間。[tool-verified: `server.py:383-389`]

重新宣告已開啟的游標名稱會先關閉先前的游標。[tool-verified: `server.py:1369-1371`] `WITH HOLD` 及 `WITHOUT HOLD` 會被語法接受；請參閱下文的游標生命週期。

**FETCH 及 MOVE**

```sql
FETCH 100 FROM c1            -- next 100 rows
FETCH FORWARD 100 FROM c1    -- same
FETCH ALL FROM c1            -- all remaining rows
FETCH BACKWARD 10 FROM c1    -- previous 10 rows (SCROLL cursors only)
FETCH ABSOLUTE 50 FROM c1    -- the row at absolute position 50 (SCROLL cursors only)
FETCH RELATIVE -5 FROM c1    -- 5 positions back (SCROLL cursors only)
FETCH FIRST FROM c1          -- row 1 (SCROLL cursors only)
FETCH LAST FROM c1           -- the final row (SCROLL cursors only)
MOVE 100 FROM c1             -- advance position without returning rows
```
`FETCH` 傳回資料列並傳送命令標籤 `FETCH <n>`，其中 `n` 是實際傳回的數目，當游標到達結尾時可能少於所要求的數目。`MOVE` 移動位置而不傳回資料列，並傳送 `MOVE <n>`。[tool-verified: `server.py:1417`, `server.py:1419`]

`FROM` 及 `IN` 在游標名稱之前可以互換。沒有加引號的游標名稱會轉為小寫；加引號的名稱（`"CaseSensitive"`）保留大小寫。[tool-verified: `test_cursor.py:74-79`]

對 `NO SCROLL` 游標進行向後導覽（`BACKWARD`、`ABSOLUTE`、`FIRST`、`LAST`、負數的 `RELATIVE`）會傳回 SQLSTATE 55000（`object_not_in_prerequisite_state`），與 PostgreSQL 本身的拒絕一致。[tool-verified: `server.py:1402-1406`] 對從未宣告的游標名稱執行 FETCH 或 MOVE 會傳回 SQLSTATE 34000（`invalid_cursor_name`）。[tool-verified: `server.py:1398-1400`]

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```
`CLOSE` 釋放底層的結果串流，並從工作階段中移除游標。[tool-verified: `server.py:1421-1436`] 不執行 `CLOSE` 便中斷連線是安全的：`ProvisaSession.close()` 會在中斷連線時自動釋放所有仍然開啟的游標。[tool-verified: `test_cursor.py:335-346`]

**游標生命週期。** Provisa 的 pgwire 伺服器沒有交易狀態機（`ProvisaSession.in_transaction()` 固定傳回 `False`）。因此每個游標的行為都如同宣告了 `WITH HOLD`：它在整個連線期間存續，而不是直至 `COMMIT` 為止。[tool-verified: `server.py:200` — `in_transaction()` always returns `False`] 關鍵字 `WITH HOLD` 及 `WITHOUT HOLD` 會被接受並靜默忽略——沒有需要執行的按交易劃分的游標生命週期。

### 定義（CREATE、ALTER、DROP、TRUNCATE） {: #definitions-create-alter-drop-truncate }

不會透過 pgwire 定義任何內容。建立、修改或移除物件的陳述式（`CREATE TABLE`、`CREATE TABLE … AS SELECT`、`CREATE VIEW`、`ALTER`、`DROP`、索引、序列或結構描述）不論角色持有甚麼，都會以 SQLSTATE 0A000 被拒絕；例外是工作階段自己的暫存資料表（請參閱[暫存資料表](#temporary-tables)）。資料表或檢視是模型物件：在模型中建立它們（管理頁面、管理 API 或設定），或在數據來源中建立後納入模型。`TRUNCATE` 以同樣方式被拒絕；`DELETE` 是移除資料列的受治理方式。[tool-verified: `provisa/compiler/definitions.py`]

拒絕只發生一次，位於每個 SQL 介面都會經過的管線中，因此 HTTP 上的 SQL、Flight 及 MCP 會以相同的訊息拒絕相同的陳述式。

### 暫存資料表 {: #temporary-tables }

工作階段可以建立、寫入、讀取及移除自己的暫存資料表：

```sql
CREATE TEMP TABLE busy AS SELECT customer_id, COUNT(*) AS n FROM sales.orders GROUP BY customer_id;
INSERT INTO busy VALUES (0, 0);
UPDATE busy SET n = n + 1 WHERE customer_id = 0;
DELETE FROM busy WHERE n < 5;
SELECT c.name, b.n FROM sales.customers c JOIN busy b ON b.customer_id = c.id;
DROP TABLE busy;
```
- **建立**——`CREATE TEMP TABLE name (column type, ...)` 或 `CREATE TEMP TABLE name AS SELECT ...`。名稱不接受結構描述。它所根據的 `SELECT` 與任何其他讀取一樣：角色的資料列規則、遮罩、偽造數據及資料列上限都適用於它所取得的資料列。
- **欄位類型**：`smallint`、`integer`、`bigint`、`real`、`double precision`、`boolean`、`date`、`timestamp`、`text`、`varchar` 及 `DECIMAL(precision, scale)`。小數保留其精度及小數位數。任何其他類型都會被拒絕，並指出欄位及類型：值絕不會以並非其本身的類型儲存。由 `SELECT` 建立的欄位採用其值的類型；沒有可推斷類型之值的欄位會被拒絕，因此請在 `SELECT` 中使用 `CAST`。
- **資料列上限。** `CREATE ... AS SELECT` 或 `INSERT ... SELECT` 所取得的資料列，按角色讀取任何內容的方式讀取，因此角色的資料列上限適用。由被上限截斷的讀取填入的暫存資料表，會帶有與任何被截斷讀取相同的陳述式警告（`statement.rows_cut`），並指出上限。
- **寫入**——`INSERT ... VALUES`、`INSERT ... SELECT`、`UPDATE` 及 `DELETE`，不帶 `RETURNING`，也不帶 `FROM`。寫入暫存資料表並不是變更：它既不觸及數據來源，也不觸及環境的數據，因此在環境任何的變更處理方式下都可使用，亦不需要寫入權。
- **讀取**——按其名稱，單獨讀取或與模型資料表聯結。讀取由引擎計算。
- **生命週期。** 暫存資料表不是模型物件，其他工作階段看不到它，它隨工作階段結束。在 pgwire 中，工作階段就是連線。在沒有自己連線的介面上（HTTP 上的 SQL、Flight、MCP），工作階段就是請求，因此暫存資料表只在該請求中一併傳送的陳述式之間存續。
- **儲存。** 資料列存放於引擎儲存中工作階段自己的結構描述，並在工作階段結束時移除。`INSERT`、`UPDATE` 或 `DELETE` 會經由引擎重寫整張資料表，因此暫存資料表適合工作數據，而不適合逐列寫入的大型資料表。沒有結束其工作階段便停止的程序會在儲存中留下這些工作階段的結構描述；它們會在環境停用時被移除。

同樣的陳述式在每個 SQL 介面上都可使用：它們在所有介面共用的管線中處理。[inferred: verified by `tests/integration/test_session_temp_tables.py` over HTTP; the pgwire connection-long session is not yet covered by a test]

### 數據寫入（INSERT、UPDATE、DELETE、MERGE、COPY FROM STDIN） {: #data-writes-insert-update-delete-merge-copy-from-stdin }

現有已註冊資料表的資料列經由 pgwire 寫入，方式與任何其他 SQL 介面相同，經由同一個寫入准入（`provisa/compiler/write_admission.py`）：

1. **權利。** 角色持有 `write`。（REQ-868）
2. **欄位。** 陳述式寫入的每個欄位都在其 `writable_by` 中指名了該角色；沒有宣告任何內容的欄位，任何人都不能寫入。（REQ-663）
3. **可以觸及的資料列。** 角色的資料列過濾（與其讀取所攜帶的謂詞相同）會加入 `UPDATE` 或 `DELETE` 的 `WHERE`。
4. **保留下來的資料列。** 受過濾的角色只能寫入之後它能讀取的資料列：`INSERT` 的資料列，以及設定了過濾所讀取欄位的 `UPDATE` 的新值，會在向數據來源傳送任何內容之前對照過濾檢查。

無法判定的內容會被點名拒絕，而不是在有疑問時放行（`INSERT … SELECT`、既非字面值也非綁定參數的值、讀取陳述式沒有提供的欄位的過濾）。[tool-verified: `provisa/compiler/write_admission.py`, `provisa/pgwire/copy_handler.py`]

### COPY

`COPY ... TO STDOUT` 及 `COPY ... FROM STDIN` 均受支援。（REQ-585）

**COPY TO STDOUT**——以 PG COPY 線路格式匯出查詢結果。兩種形式都可使用：

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```
支援的格式：`text`（以定位字元分隔，預設）及 `csv`。COPY 輸出不支援二進位格式。[tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** 是向現有已註冊資料表寫入數據：它由與相同資料列的 `INSERT` 相同的寫入准入放行（請參閱[數據寫入](#data-writes-insert-update-delete-merge-copy-from-stdin)）。

### 交易與工作階段命令 {: #transactions-and-session-commands }

SET、BEGIN、COMMIT、ROLLBACK、SAVEPOINT、RELEASE、DISCARD、RESET 及 DEALLOCATE 會被攔截並返回空的成功回應。（REQ-587）伺服器就交易而言屬於無狀態——不存在交易隔離或回滾支援。（REQ-587）[tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## 目錄攔截 {: #catalog-intercept }

對 `information_schema` 及 `pg_catalog` 的查詢會在本地解答，無需往返 Trino。（REQ-532）攔截層會按每個請求建構一個記憶體內的 DuckDB 數據庫，並以該角色的編譯上下文填充數據。（REQ-532）[tool-verified: `catalog.py:210-213`]

被攔截的資料表：

**information_schema：** `schemata`、`tables`、`columns`、`views`、`table_constraints`、`key_column_usage`、`referential_constraints`

**pg_catalog：** `pg_namespace`、`pg_class`、`pg_attribute`、`pg_type`、`pg_attrdef`、`pg_description`、`pg_index`、`pg_constraint`、`pg_proc`、`pg_roles`、`pg_auth_members`、`pg_database`、`pg_settings`、`pg_tables`、`pg_stat_user_tables`、`pg_statio_user_tables`、`pg_am`、`pg_extension`、`pg_enum`、`pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` 會以從域模型的 `pk_columns` 及 `joins` 欄位衍生的真實主索引鍵及外部索引鍵數據填充。（REQ-392、REQ-399）檢查外部索引鍵關係的 BI 工具（Tableau、DBeaver 等）將看到 Provisa 所知的連接圖。[tool-verified: `catalog.py:551-632`] 同一來源/目標配對之間的單欄位連接，若其目標欄位共同組成該目標的複合主索引鍵，會被合併為單一 FK 行，並以多元素的 `conkey`/`confkey` 陣列表示。（REQ-1094）[tool-verified: `catalog_constraints.py`]

由聯結資料表支撐的關係（REQ-1586）不會產生 FK 行。它是穿過關聯資料表的一條邊，而不是一對欄位，而 `pg_constraint` 沒有可容納兩跳的形態——因此網域模型不會把它放進 `joins`，聯結資料表則表現為一張普通資料表，各自持有指向兩端的外部索引鍵。SQL 客戶端透過聯結該資料表來存取它；Cypher 客戶端則把它當作單條關係走訪。[tool-verified: `provisa/compiler/schema_gen.py:302-306`]

`pg_index` 會就每個主索引鍵及 UNIQUE 約束填充一行（`indrelid` = 資料表 oid，`indkey` = 已排序的鍵值 attnum，`indisprimary`/`indisunique` 已設定）。透過 `pg_index.indkey` 而非 `pg_constraint` 解析鍵值欄位的客戶端——例如 DataGrip——會透過標準的 `pg_index` → `pg_attribute` 連接找出正確的欄位。（REQ-1095）[tool-verified: `catalog_constraints.py:340-384`]

以下純量表達式亦會被攔截：（REQ-588）

- `current_user`、`session_user` → 已驗證的 `role_id`
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → 從固定的設定表返回數值
- `SHOW <setting>` → 從同一設定表返回數值

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## 二進制參數編碼 {: #binary-parameter-encoding }

擴展查詢協定（Bind/Execute）支援以二進制編碼的參數。（REQ-589）以下類型 OID 會從二進制解碼：[tool-verified: `postgres.py:69-97`]

| OID | PG 類型 | Python 類型 |
| ----- | --------- | ------------- |
| 16 | bool | bool |
| 17 | bytea | bytes |
| 20 | int8 | int |
| 21 | int2 | int |
| 23 | int4 | int |
| 25 | text | str |
| 700 | float4 | float |
| 701 | float8 | float |
| 1043 | varchar | str |
| 1082 | date | datetime.date |
| 1114 | timestamp | datetime.datetime |
| 1184 | timestamptz | datetime.datetime (UTC) |
| 1700 | numeric | decimal.Decimal |
| 2950 | uuid | str |

不在此表中的任何 OID 都會引發 `"Unsupported binary parameter type: <oid>"`。（REQ-589）[tool-verified: `postgres.py:579`]

當客戶端要求時，結果欄位亦會以二進制傳送，適用於相同類型集加上 ARRAY、JSON、INTERVAL 及 BIGINT。（REQ-589）[tool-verified: `postgres.py:191-244`]

---

## 驅動程式建議 {: #driver-recommendations }

**原生 Python 驅動程式（psycopg2、asyncpg）。** 這些驅動程式預設會協商擴展查詢協定，並對大多數類型使用二進制編碼。類型保真度在此最高——`NUMERIC` 欄位以 `Decimal` 形式送達，`TIMESTAMP` 則以 `datetime` 形式送達，如此類推。適用於以 Python 為基礎的 ETL、腳本或直接整合。

**JDBC（PostgreSQL JDBC 驅動程式）。** 適用於 Java 生態系統工具：DBeaver、Tableau、Power BI、Metabase、Airflow 的 JDBC 運算子。JDBC 預設使用簡單查詢協定，可避免二進制編碼帶來的複雜情況。連接字串：

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

部分基於 JDBC 的 BI 工具在連接時，會傳送一連串針對 `information_schema` 及 `pg_catalog` 的查詢，以填充其結構描述瀏覽器。這些查詢全部由目錄攔截層解答——在結構描述檢查期間不會產生任何 Trino 流量。（REQ-532）

**應何時選用哪一種。** 若客戶端為 Python，請使用 psycopg2 或 asyncpg 以獲得更佳的類型處理。若客戶端為 BI 工具或任何 JVM 應用程式，請使用 JDBC。若觀察到類型轉換方面的異常情況，應避免在同一連接中混用二進制及文字協定的預期行為——JDBC 的文字模式行為更易於推斷。

---

## 注意事項與限制 {: #caveats-and-constraints }

**僅限 SQL。** pgwire 監聽器僅解析並執行 SQL——不接受 GraphQL 及 Cypher 字串。（REQ-614） 對現有資料表的數據寫入，由角色的權利、其欄位授權及其資料列過濾放行；請參閱[數據寫入](#data-writes-insert-update-delete-merge-copy-from-stdin)。

**不支援定義。** 不論角色持有甚麼，`CREATE`、`ALTER`、`DROP` 及 `TRUNCATE` 都會以 SQLSTATE 0A000 被拒絕，工作階段自己的暫存資料表除外；請參閱[暫存資料表](#temporary-tables)。（REQ-615、REQ-616）

**不支援真正的交易功能。** BEGIN/COMMIT/ROLLBACK 會被接受並靜默忽略。每個陳述式均獨立執行。（REQ-587）[tool-verified: `server.py:146-158`——`in_transaction()` 一律返回 `False`]

**查詢逾時。** 陳述式可以使用 pgwire 本身的請求逾時 `limits.request_timeouts.pgwire`，未設定時使用共用的 `limits.request_timeout`；兩者都是營運設定。（REQ-590、REQ-1905） [tool-verified: `provisa/core/limits.py`, `server.py`]

**COPY 輸出格式為 text 或 csv。** 尚未實作 PG 二進制 COPY 格式（`FORMAT binary`）。[inferred：`_rows_to_copy_text` / `_rows_to_copy_csv` 中僅存在 `text` 及 `csv` 分支]

**參數替換屬字面值形式。** `$1`、`$2`……等參數會在執行前以 SQL 字面值替換，而非以綁定參數形式傳送至底層引擎。這意味著底層引擎永遠不會看到已準備的陳述式。對 Trino 而言此舉沒有實際影響；對於直接連接池的來源，則會繞過已準備陳述式的快取機制。（REQ-581）[tool-verified: `server.py:78-85`]

**DECLARE SCROLL 會緩衝整個結果。** `SCROLL` 游標會把它從引擎拉取的每一個資料列累積到 Python 堆積中，以便按需提供 `FETCH BACKWARD`、`ABSOLUTE`、`FIRST` 及 `LAST`。面對大型結果，這表示只要游標仍然開啟，整個結果便會留在記憶體中。只需向前的串流請使用預設的 `NO SCROLL`。（REQ-1862） [tool-verified: `server.py:383-389`]

**`pg_stat_activity`、`pg_stat_user_tables`、`pg_extension`、`pg_enum`、`pg_attrdef`、`pg_proc`。** 這些資料表存在於目錄層中，但屬於空的存根 (stub)。查詢它們的監控工具將收到零行結果，而非錯誤。（REQ-532）[tool-verified: `catalog.py:519-535`, `catalog.py:639-934`]（`pg_index` 已有數據填充——請參閱「目錄攔截」一節。）
