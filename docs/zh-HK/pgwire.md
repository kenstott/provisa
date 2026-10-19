# Provisa pgwire 伺服器 {: #provisa-pgwire-server }

Provisa 提供一個 PostgreSQL 線路協定（pgwire）端點。任何支援 PostgreSQL 用戶端協定的工具，例如 psycopg2、asyncpg、DBeaver、Tableau 或 JDBC，都可以連接並經與 HTTP API 相同的治理管道查詢 Provisa 數據。

查詢會經過完整的治理堆疊：行級安全執行、遮罩規則、關係防護、域訪問檢查。pgwire 介面並非繞過機制。

---

## 連接詳情 {: #connection-details }

當 `PROVISA_PGWIRE_PORT` 設定為非零整數時，伺服器便會啟動。預設為停用狀態。

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS。** 將 `PROVISA_PGWIRE_CERT` 和 `PROVISA_PGWIRE_KEY` 設定為 PEM 證書及金鑰的路徑。兩者皆存在時，伺服器會將傳入連接以 TLS 包裝。兩者皆缺席時，TLS 為關閉狀態，伺服器會對 SSL 協商請求回覆 `N`。

**回報的伺服器版本。** 客戶端看到的版本為 `14.0.provisa`。根據版本號啟用功能的工具，其行為可能如同連接至 PostgreSQL 14。

---

## 身份驗證 {: #authentication }

啟動封包會攜帶一個用戶名和一個密文欄位，但沒有任何方案說明該密文是什麼。Provisa 會根據密文本身判斷，因此客戶端除了 `user` 和 `password` 之外無需任何額外設定：

| 密文是 | 辨識依據 | 解析為 |
| --------------- | --------------- | ------------- |
| 個人存取權杖 | 其 `provisa_pat_` 前綴 | 該權杖的擁有者及其角色 |
| OIDC／提供者簽發的 bearer 權杖 | 所設定的提供者是權杖提供者 | 該權杖所聲明的身分 |
| 密碼 | 其他任何情況 | 所設定提供者（`basic` 或 `simple`）中的帳戶 |

判斷只做一次。被選定驗證器拒絕的憑證不會再交給另一個驗證器重試，因此一次拒絕不會變成第二次猜測。

trust 模式（`provider: none`，或驗證中介軟件未啟用）是例外：用戶名會直接用作 `role_id`，密文會被忽略。請勿在未加密的連接上使用它。

**SCRAM-SHA-256。** 在 `provider: basic` 且 `auth.scram: true` 時，伺服器會公告帶 `SCRAM-SHA-256` 的 SASL（驗證代碼 10），密碼以證明方式驗證而非傳送。不提供 `SCRAM-SHA-256-PLUS`。對於尚未寫入驗證子的用戶——驗證子無法從 bcrypt 雜湊推導——伺服器會以模擬交換作答，使連線上看不出誰已遷移；該用戶會透過 TLS 上的明文密碼進行驗證，直到下次輸入密碼時寫入驗證子為止。關閉 `auth.scram` 時，伺服器使用 PG 驗證類型 3（明文密碼）。兩種情況下都不支援 MD5。

**客戶端憑證。** 設定 `PROVISA_MTLS_CLIENT_CA` 後，伺服器會在交握期間、檢查任何憑證之前驗證客戶端憑證。啟用 `PROVISA_MTLS_BIND_PRINCIPAL` 時，憑證的 common name 必須與該連接隨後用於驗證的 `user` 相同。參見[設定](configuration.md#mutual-tls)。

**失敗嘗試會被計數。** 五分鐘內失敗五次，帳戶會被鎖定十五分鐘。計數器與 HTTP 及 Bolt 共用，所以在任何一個入口觸發的鎖定，在所有入口都生效。

**選擇組織。** 在多組織部署中，連接至 `<org>.<your-domain>`，pgwire 會從 TLS ClientHello 的主機名稱中讀取組織，方式與 HTTP 從 `Host` 標頭中讀取相同。主機名稱是在請求某個組織，而不是授予它；在該組織中沒有成員資格的主體會被拒絕。透過 IP 位址連接則不請求任何組織。

---

## 支援的功能 {: #what-works }

### SELECT

所有 SELECT 陳述式都會經過治理管線（`_pipeline.py`）。此管線會：

1. 將語意 SQL 重寫為實體 SQL（`rewrite_semantic_to_physical`）
2. 套用治理（行級安全、遮罩、域訪問）
3. 對照已註冊的結構描述 (Schema) 進行驗證
4. 路由至 Trino 或直接路由至來源池

支援多陳述式的簡單查詢。以分號分隔的陳述式會被拆分並按順序執行。

在簡單查詢模式及擴展查詢（Bind/Execute）模式下均支援參數化查詢（`$1`、`$2`……）。參數會在執行前以字面值替換。

`SELECT * FROM fn(args)` 及 `SELECT fn(args)`——其中 `fn` 指定一個已註冊且受追蹤的函式——會在治理管線之前被攔截，並透過唯一受治理的執行器（`invoke_tracked_function`）路由。結果為一個類型化的行集，與該指令在其他任何介面所返回的結果一致。指令的分配（`visible_to`）、變更所需的 `write` 權利 及治理規則會在執行器內強制執行。

### 游標（DECLARE / FETCH / MOVE / CLOSE） {: #cursors-declare-fetch-move-close }

Provisa 在簡單查詢協定上支援文字形式的 SQL 遊標語法。以一般 SQL 語句傳送 `DECLARE`/`FETCH`/`MOVE`/`CLOSE` 的用戶端可以獲得真正的伺服器端串流，不會出現協定錯誤，亦不會把整個結果載入用戶端記憶體。

**這與以 portal 為基礎的串流不同。** 延伸查詢協定——帶有資料列上限欄位的 Bind/Execute——是另一種線路協定機制。asyncpg 及大多數現代 PostgreSQL 驅動程式會自動使用它，不需要游標 SQL。如果你的驅動程式已經使用 portal，你便不需要這種語法。`DECLARE CURSOR` 是為按字面傳送游標 SQL 的用戶端而設：psycopg2 的具名/伺服器端游標（`conn.cursor(name="...")`）、部分 BI 工具及一些較舊的 JDBC 驅動程式。

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```
內部的 SELECT 會經過 Provisa 正常的受治理管道，與任何單獨的 SELECT 一樣執行 RLS、遮罩及領域存取檢查。宣告遊標並不能繞過治理。

`SCROLL` 關鍵字同時控制允許的導覽及游標的記憶體佔用：

| 宣告 | 向後導覽 | 記憶體成本 |
|---|---|---|
| `DECLARE c CURSOR FOR ...`（預設） | 不允許 | 限於一批 FETCH |
| `DECLARE c NO SCROLL CURSOR FOR ...` | 不允許 | 限於一批 FETCH |
| `DECLARE c SCROLL CURSOR FOR ...` | 允許 | 整個結果緩衝於 Python 記憶體 |

既沒有指定 `SCROLL` 也沒有指定 `NO SCROLL` 時，預設為 `NO SCROLL`——與 PostgreSQL 本身一致。 `NO SCROLL` 游標在每次向前推進後，都會從其內部緩衝區捨棄已消耗的資料列，因此對大型結果的 `FETCH FORWARD` 走訪，在任何時候都只在 Python 記憶體中保留大約一批數據。 `SCROLL` 游標會保留它曾經拉取的每一個資料列——這是支援 `FETCH BACKWARD`、`ABSOLUTE`、`FIRST` 及 `LAST` 的代價。在對大型結果宣告 `SCROLL` 游標之前，請為整個結果的大小預留 Python 堆積空間。

重新宣告已開啟的游標名稱會先關閉先前的游標。 `WITH HOLD` 及 `WITHOUT HOLD` 會被語法接受；請參閱下文的游標生命週期。

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
`FETCH` 傳回資料列並傳送命令標籤 `FETCH <n>`，其中 `n` 是實際傳回的數目，當游標到達結尾時可能少於所要求的數目。`MOVE` 移動位置而不傳回資料列，並傳送 `MOVE <n>`。

`FROM` 及 `IN` 在游標名稱之前可以互換。沒有加引號的游標名稱會轉為小寫；加引號的名稱（`"CaseSensitive"`）保留大小寫。

對 `NO SCROLL` 游標進行向後導覽（`BACKWARD`、`ABSOLUTE`、`FIRST`、`LAST`、負數的 `RELATIVE`）會傳回 SQLSTATE 55000（`object_not_in_prerequisite_state`），與 PostgreSQL 本身的拒絕一致。 對從未宣告的游標名稱執行 FETCH 或 MOVE 會傳回 SQLSTATE 34000（`invalid_cursor_name`）。

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```
`CLOSE` 釋放底層的結果串流，並從工作階段中移除游標。 不執行 `CLOSE` 便中斷連線是安全的：`ProvisaSession.close()` 會在中斷連線時自動釋放所有仍然開啟的游標。

**遊標生命週期。** Provisa 的 pgwire 伺服器沒有交易狀態機（`ProvisaSession.in_transaction()` 固定傳回 `False`）。因此每個遊標的行為都像宣告了 `WITH HOLD`：它在連線存續期間一直存在，`COMMIT` 不會結束它。`WITH HOLD` 及 `WITHOUT HOLD` 關鍵字會被接受並略過，因為不存在按交易管理的遊標生命週期。

### 定義（CREATE、ALTER、DROP） {: #definitions-create-alter-drop }

不會透過 pgwire 定義任何內容。建立、修改或移除物件的陳述式（`CREATE TABLE`、`CREATE TABLE … AS SELECT`、`CREATE VIEW`、`ALTER`、`DROP`、索引、序列或結構描述）不論角色持有甚麼，都會以 SQLSTATE 0A000 被拒絕；例外是工作階段自己的暫存資料表（請參閱[暫存資料表](#temporary-tables)）。資料表或檢視是模型物件：在模型中建立它們（管理頁面、管理 API 或設定），或在數據來源中建立後納入模型。`TRUNCATE` 不定義任何內容：它是數據寫入（請參閱[數據寫入](#data-writes-insert-update-delete-merge-copy-from-stdin)）。

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

同樣的陳述式在每個 SQL 介面上都可使用：它們在所有介面共用的管線中處理。

### 數據寫入（INSERT、UPDATE、DELETE、MERGE、COPY FROM STDIN） {: #data-writes-insert-update-delete-merge-copy-from-stdin }

現有已註冊資料表的資料列經由 pgwire 寫入，方式與任何其他 SQL 介面相同，經由同一個寫入准入（`provisa/compiler/write_admission.py`）：

1. **權利。** 角色持有 `write`。
2. **授權。** 角色持有資料表上該操作的寫入授權：`INSERT` 需要 `insert` 授權，`UPDATE` 需要 `update` 授權，`DELETE` 需要 `delete` 授權。`MERGE` 需要涵蓋所有行的 `insert` 和 `update` 授權，`INSERT ... ON CONFLICT DO UPDATE` 需要涵蓋所有行的 `update` 授權。
3. **欄位。** 陳述式寫入的每個欄位都是授權指定的欄位。授權預設的欄位歸授權所有：陳述式不得提供它，授權的值（工作階段變數、字面值或寫入時間）會被加入。
4. **可觸及的行。** 授權的 `rows` 謂詞會加入 `UPDATE` 或 `DELETE` 的 `WHERE` 中。未聲明 `rows` 的授權採用角色的行篩選器，即其讀取所攜帶的同一謂詞。
5. **留下的行。** `INSERT` 寫入的行，以及設定了授權 `check` 所讀取欄位的 `UPDATE` 的新值，會在向數據來源發送任何內容之前對照 `check` 進行檢查。未聲明 `check` 的授權採用角色的行篩選器。

`TRUNCATE` 同樣是數據寫入，按其自身運行，絕不會被改寫為 `DELETE`。它需要 `write` 權利、涵蓋該資料表所有行的 `delete` 授權，以及支援刪除的數據來源。它一次清空一張資料表，授權限制了行的角色（`rows` 謂詞，或授權未聲明時採用的角色行篩選器）會被拒絕，因為 `TRUNCATE` 無法套用該謂詞：請改用能套用它的 `DELETE`。

無法判定的內容會被點名拒絕，而不是在有疑問時放行（`INSERT … SELECT`、既非字面值也非綁定參數的值、讀取陳述式沒有提供的欄位的過濾）。

### COPY

`COPY ... TO STDOUT` 及 `COPY ... FROM STDIN` 均受支援。

**COPY TO STDOUT**——以 PG COPY 線路格式匯出查詢結果。兩種形式都可使用：

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```
支援的格式：`text`（以定位字元分隔，預設）、`csv` 及 `binary`，即 PostgreSQL 的二進位 COPY 格式，DuckDB 的 postgres 擴充以它讀取。

**COPY FROM STDIN** 是對現有已註冊資料表的數據寫入：它與相同行的 `INSERT` 經由同一個寫入准入，另加一個條件：`insert` 授權不得帶有預設（參見[數據寫入](#data-writes-insert-update-delete-merge-copy-from-stdin)）。它讀取 `text`（預設）和 `csv`。任何其他格式都會在發送任何數據之前以 SQLSTATE 0A000 被拒絕，並指明該格式。

### 交易與工作階段命令 {: #transactions-and-session-commands }

SET、BEGIN、COMMIT、ROLLBACK、SAVEPOINT、RELEASE、DISCARD、RESET 及 DEALLOCATE 會被攔截，並傳回空的成功回應。伺服器不保存交易狀態，所以沒有交易隔離，亦不支援回復。

---

## 目錄攔截 {: #catalog-intercept }

對 `information_schema` 及 `pg_catalog` 的查詢會在本地解答，無需往返 Trino。攔截層會按每個請求建構一個記憶體內的 DuckDB 數據庫，並以該角色的編譯上下文填充數據。

被攔截的資料表：

**information_schema：** `schemata`、`tables`、`columns`、`views`、`table_constraints`、`key_column_usage`、`referential_constraints`

**pg_catalog：** `pg_namespace`、`pg_class`、`pg_attribute`、`pg_type`、`pg_attrdef`、`pg_description`、`pg_index`、`pg_constraint`、`pg_proc`、`pg_roles`、`pg_auth_members`、`pg_database`、`pg_settings`、`pg_tables`、`pg_stat_user_tables`、`pg_statio_user_tables`、`pg_am`、`pg_extension`、`pg_enum`、`pg_stat_activity`

`pg_constraint` 會以從域模型的 `pk_columns` 及 `joins` 欄位衍生的真實主索引鍵及外部索引鍵數據填充。檢查外部索引鍵關係的 BI 工具（Tableau、DBeaver 等）將看到 Provisa 所知的連接圖。 同一來源/目標配對之間的單欄位連接，若其目標欄位共同組成該目標的複合主索引鍵，會被合併為單一 FK 行，並以多元素的 `conkey`/`confkey` 陣列表示。

經由關聯表的關係不會產生外鍵列。它是一條穿過關聯表的邊，而 `pg_constraint` 無法表達兩跳，所以領域模型不會把它放進 `joins`，關聯表以一般資料表的形式出現，帶有指向兩端的外鍵。SQL 用戶端經聯結這個資料表來存取它；Cypher 用戶端把它作為一個關係來遍歷。

`pg_index` 會就每個主索引鍵及 UNIQUE 約束填充一行（`indrelid` = 資料表 oid，`indkey` = 已排序的鍵值 attnum，`indisprimary`/`indisunique` 已設定）。透過 `pg_index.indkey` 而非 `pg_constraint` 解析鍵值欄位的客戶端——例如 DataGrip——會透過標準的 `pg_index` → `pg_attribute` 連接找出正確的欄位。

以下純量表達式亦會被攔截：

- `current_user`、`session_user` → 已驗證的 `role_id`
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → 從固定的設定表返回數值
- `SHOW <setting>` → 從同一設定表返回數值

---

## 二進制參數編碼 {: #binary-parameter-encoding }

擴展查詢協定（Bind/Execute）支援以二進制編碼的參數。以下類型 OID 會從二進制解碼：

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

不在此表中的任何 OID 都會引發 `"Unsupported binary parameter type: <oid>"`。

當客戶端要求時，結果欄位亦會以二進制傳送，適用於相同類型集加上 ARRAY、JSON、INTERVAL 及 BIGINT。

---

## 驅動程式建議 {: #driver-recommendations }

**原生 Python 驅動程式（psycopg2、asyncpg）。** 這些驅動程式預設會協商擴展查詢協定，並對大多數類型使用二進制編碼。類型保真度在此最高——`NUMERIC` 欄位以 `Decimal` 形式送達，`TIMESTAMP` 則以 `datetime` 形式送達，如此類推。適用於以 Python 為基礎的 ETL、腳本或直接整合。

**JDBC（PostgreSQL JDBC 驅動程式）。** 適用於 Java 生態系統工具：DBeaver、Tableau、Power BI、Metabase、Airflow 的 JDBC 運算子。JDBC 預設使用簡單查詢協定，可避免二進制編碼帶來的複雜情況。連接字串：

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

部分基於 JDBC 的 BI 工具在連接時，會傳送一連串針對 `information_schema` 及 `pg_catalog` 的查詢，以填充其結構描述瀏覽器。這些查詢全部由目錄攔截層解答——在結構描述檢查期間不會產生任何 Trino 流量。

**應何時選用哪一種。** 若客戶端為 Python，請使用 psycopg2 或 asyncpg 以獲得更佳的類型處理。若客戶端為 BI 工具或任何 JVM 應用程式，請使用 JDBC。若觀察到類型轉換方面的異常情況，應避免在同一連接中混用二進制及文字協定的預期行為——JDBC 的文字模式行為更易於推斷。

---

## 注意事項與限制 {: #caveats-and-constraints }

**只限 SQL。** pgwire 監聽器只剖析及執行 SQL，不接受 GraphQL 或 Cypher 字串。向現有資料表寫入數據時，由角色的權限、資料行授權及資料列篩選器決定是否准入；參見[數據寫入](#data-writes-insert-update-delete-merge-copy-from-stdin)。

**不支援定義。** 不論角色持有甚麼，`CREATE`、`ALTER` 及 `DROP` 都會以 SQLSTATE 0A000 被拒絕，工作階段自己的暫存資料表除外；請參閱[暫存資料表](#temporary-tables)。`TRUNCATE` 是數據寫入。

**不支援真正的交易功能。** BEGIN/COMMIT/ROLLBACK 會被接受並靜默忽略。每個陳述式均獨立執行。

**查詢逾時。** 陳述式可以使用 pgwire 本身的請求逾時 `limits.request_timeouts.pgwire`，未設定時使用共用的 `limits.request_timeout`；兩者都是營運設定。

**COPY 輸入為 text 或 csv。** `COPY ... TO STDOUT` 寫出 `text`、`csv` 及 `binary`；`COPY ... FROM STDIN` 讀取 `text` 及 `csv`。

**參數保持繫結。** `$1`、`$2`……的值永遠不會拼進語句文字：它們以繫結方式一路傳到引擎或數據來源，所以所有取值共用一份 SQL 文字（伺服器端預處理及按 SQL 文字快取照常運作），值亦永遠無法改變受治理的語句結構。只有自行回答 `information_schema` 及 `pg_catalog` 查詢的目錄攔截會內嵌讀取這些值。

**DECLARE SCROLL 會緩衝整個結果。** `SCROLL` 游標會把它從引擎拉取的每一個資料列累積到 Python 堆積中，以便按需提供 `FETCH BACKWARD`、`ABSOLUTE`、`FIRST` 及 `LAST`。面對大型結果，這表示只要游標仍然開啟，整個結果便會留在記憶體中。只需向前的串流請使用預設的 `NO SCROLL`。

**`pg_stat_activity`、`pg_stat_user_tables`、`pg_extension`、`pg_enum`、`pg_attrdef`、`pg_proc`。** 這些資料表存在於目錄層中，但屬於空的存根 (stub)。查詢它們的監控工具將收到零行結果，而非錯誤。（`pg_index` 已有數據填充——請參閱「目錄攔截」一節。）
