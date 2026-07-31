# Provisa pgwire 伺服器

Provisa 公開一個 PostgreSQL 網絡協定（pgwire）端點。任何支援 PostgreSQL 客戶端協定的工具——psycopg2、asyncpg、DBeaver、Tableau、JDBC——都可以連接並透過與 HTTP API 相同的治理管線查詢 Provisa 數據。（REQ-266）

查詢會經過完整的治理堆疊：行級安全執行、遮罩規則、關係防護、域訪問檢查。（REQ-001、REQ-002、REQ-263）pgwire 介面並非繞過機制。（REQ-002、REQ-266）

---

## 連接詳情

當 `PROVISA_PGWIRE_PORT` 設定為非零整數時，伺服器便會啟動。預設為停用狀態。（REQ-527）[tool-verified: `app.py:1739`]

```
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS。** 將 `PROVISA_PGWIRE_CERT` 和 `PROVISA_PGWIRE_KEY` 設定為 PEM 證書及金鑰的路徑。兩者皆存在時，伺服器會將傳入連接以 TLS 包裝。兩者皆缺席時，TLS 為關閉狀態，伺服器會對 SSL 協商請求回覆 `N`。（REQ-530）[tool-verified: `server.py:1746-1750`]

**回報的伺服器版本。** 客戶端看到的版本為 `14.0.provisa`。根據版本號啟用功能的工具，其行為可能如同連接至 PostgreSQL 14。（REQ-579）[tool-verified: `server.py:208`]

---

## 身份驗證

兩種模式，由 `auth_config` 中的 `provider` 鍵控制：

| 模式 | `provider` 值 | 行為 |
|------|-----------------|-----------|
| Trust | `none`（或驗證中介軟件未啟用） | 客戶端傳送的用戶名會直接用作 `role_id`。密碼會被忽略。 |
| Simple | `simple` | 密碼會對照 `simple` 驗證提供者（bcrypt）進行驗證。成功後用戶名會成為 `role_id`。（REQ-124） |

任何其他 `provider` 值在登入時都會返回 FATAL 錯誤。（REQ-529）此協定一律使用 PG 驗證類型 3（明文密碼）。（REQ-529）請勿在未加密的連接上使用 trust 模式。[tool-verified: `server.py:282-311`]

---

## 支援的功能

### SELECT

所有 SELECT 陳述式都會經過治理管線（`_pipeline.py`）。（REQ-001、REQ-262、REQ-266）此管線會：

1. 將語意 SQL 重寫為實體 SQL（`rewrite_semantic_to_physical`）
2. 套用治理（行級安全、遮罩、域訪問）（REQ-263）
3. 對照已註冊的結構描述 (Schema) 進行驗證（REQ-011）
4. 路由至 Trino 或直接路由至來源池（REQ-027、REQ-028）

支援多陳述式的簡單查詢。以分號分隔的陳述式會被拆分並按順序執行。（REQ-580）[tool-verified: `server.py:318-381`]

在簡單查詢模式及擴展查詢（Bind/Execute）模式下均支援參數化查詢（`$1`、`$2`……）。參數會在執行前以字面值替換。（REQ-581）[tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` 及 `SELECT fn(args)`——其中 `fn` 指定一個已註冊且受追蹤的函式——會在治理管線之前被攔截，並透過唯一受治理的執行器（`invoke_tracked_function`）路由。結果為一個類型化的行集，與該指令在其他任何介面所返回的結果一致。`writable_by` 及治理規則會在執行器內強制執行。（REQ-1156）[tool-verified: `provisa/pgwire/function_call.py:74-88`]

### DDL

DDL 陳述式由 `server.py` 中的正則表達式偵測，並派送至 `DdlHandler`。角色必須具備 `"ddl"` 權限。（REQ-042）若無此權限，該陳述式會以 SQLSTATE 42501 被拒絕。[tool-verified: `ddl_handler.py:82-83`]

可識別的 DDL 形式為：

```
CREATE TABLE / VIEW / INDEX / UNIQUE INDEX / SEQUENCE / SCHEMA
ALTER TABLE / INDEX / SEQUENCE / VIEW
DROP TABLE / VIEW / INDEX / SEQUENCE / SCHEMA
```

[tool-verified: `server.py:56-61`]

根據 `ddl_catalog` 存在兩條執行路徑：（REQ-582）

**Trino 路徑**——當 `ddl_catalog` 為 Iceberg、Hive 或其他未註冊的 Trino 目錄（例如 `iceberg`、`hive`、`otel`、`results`）時使用。此路徑僅支援 `CREATE TABLE` 及 `CREATE VIEW`。嘗試執行 `ALTER`、`DROP` 或 `CREATE INDEX` 會引發錯誤。資料表名稱會完整限定為 `catalog.schema.table`。[tool-verified: `ddl_handler.py:92-100`]

**直接路徑**——當 `ddl_catalog` 對應至已註冊的來源 ID 時使用。支援完整 DDL：CREATE、ALTER、DROP、索引、序列。`CREATE TABLE` 及 `CREATE VIEW` 會以結構描述限定為 `schema.table`。其餘 DDL（ALTER、DROP、CREATE INDEX）在設定結構描述上下文後會原樣傳遞。對於 PostgreSQL 及 SQLite 來源，上下文以 `SET search_path TO schema` 設定。對於 MySQL 及 MariaDB，則以 `USE schema` 設定。[tool-verified: `ddl_handler.py:139-170`, `ddl_handler.py:207-213`]

在任一路徑執行 DDL 後，新資料表會註冊至該角色的編譯上下文中，以便立即可供查詢。（REQ-583）[tool-verified: `ddl_handler.py:216-250`]

**寫入目標的解析。** DDL 目錄及結構描述來自該域的 `ddl_catalog` 及 `ddl_schema` 欄位。若未設定 `ddl_catalog`，系統預設使用 Iceberg 目錄。若未設定 `ddl_schema`，則預設使用域 ID。域會透過角色的 `domain_access` 清單解析。（REQ-584）[tool-verified: `app.py:804-811`, `ddl_handler.py:104-115`]

### COPY

`COPY ... TO STDOUT` 及 `COPY ... FROM STDIN` 均獲支援。（REQ-585）[tool-verified: `copy_handler.py:231-257`]

**COPY TO STDOUT**——以 PG COPY 網絡格式匯出查詢結果。有兩種形式可用：

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

支援的格式：`text`（以定位字符分隔，預設）及 `csv`。COPY 輸出不支援二進制格式。[tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN**——將行插入目標資料表。僅限類型為 `postgresql`、`mysql`、`sqlite` 或 `mariadb` 的來源使用。（REQ-586）嘗試對僅限 Trino 的來源（例如 Iceberg）執行 COPY FROM 會引發權限錯誤。[tool-verified: `copy_handler.py:65`, `copy_handler.py:351-356`]

```sql
COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)
```

若未提供欄位清單，欄位會從已註冊的結構描述推斷得出。[tool-verified: `copy_handler.py:357`]

### 交易與工作階段命令

SET、BEGIN、COMMIT、ROLLBACK、SAVEPOINT、RELEASE、DISCARD、RESET 及 DEALLOCATE 會被攔截並返回空的成功回應。（REQ-587）伺服器就交易而言屬於無狀態——不存在交易隔離或回滾支援。（REQ-587）[tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## 目錄攔截

對 `information_schema` 及 `pg_catalog` 的查詢會在本地解答，無需往返 Trino。（REQ-532）攔截層會按每個請求建構一個記憶體內的 DuckDB 數據庫，並以該角色的編譯上下文填充數據。（REQ-532）[tool-verified: `catalog.py:210-213`]

被攔截的資料表：

**information_schema：** `schemata`、`tables`、`columns`、`views`、`table_constraints`、`key_column_usage`、`referential_constraints`

**pg_catalog：** `pg_namespace`、`pg_class`、`pg_attribute`、`pg_type`、`pg_attrdef`、`pg_description`、`pg_index`、`pg_constraint`、`pg_proc`、`pg_roles`、`pg_auth_members`、`pg_database`、`pg_settings`、`pg_tables`、`pg_stat_user_tables`、`pg_statio_user_tables`、`pg_am`、`pg_extension`、`pg_enum`、`pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` 會以從域模型的 `pk_columns` 及 `joins` 欄位衍生的真實主索引鍵及外部索引鍵數據填充。（REQ-392、REQ-399）檢查外部索引鍵關係的 BI 工具（Tableau、DBeaver 等）將看到 Provisa 所知的連接圖。[tool-verified: `catalog.py:551-632`] 同一來源/目標配對之間的單欄位連接，若其目標欄位共同組成該目標的複合主索引鍵，會被合併為單一 FK 行，並以多元素的 `conkey`/`confkey` 陣列表示。（REQ-1094）[tool-verified: `catalog_constraints.py`]

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

## 二進制參數編碼

擴展查詢協定（Bind/Execute）支援以二進制編碼的參數。（REQ-589）以下類型 OID 會從二進制解碼：[tool-verified: `postgres.py:69-97`]

| OID | PG 類型 | Python 類型 |
|-----|---------|-------------|
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

## 驅動程式建議

**原生 Python 驅動程式（psycopg2、asyncpg）。** 這些驅動程式預設會協商擴展查詢協定，並對大多數類型使用二進制編碼。類型保真度在此最高——`NUMERIC` 欄位以 `Decimal` 形式送達，`TIMESTAMP` 則以 `datetime` 形式送達，如此類推。適用於以 Python 為基礎的 ETL、腳本或直接整合。

**JDBC（PostgreSQL JDBC 驅動程式）。** 適用於 Java 生態系統工具：DBeaver、Tableau、Power BI、Metabase、Airflow 的 JDBC 運算子。JDBC 預設使用簡單查詢協定，可避免二進制編碼帶來的複雜情況。連接字串：

```
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

部分基於 JDBC 的 BI 工具在連接時，會傳送一連串針對 `information_schema` 及 `pg_catalog` 的查詢，以填充其結構描述瀏覽器。這些查詢全部由目錄攔截層解答——在結構描述檢查期間不會產生任何 Trino 流量。（REQ-532）

**應何時選用哪一種。** 若客戶端為 Python，請使用 psycopg2 或 asyncpg 以獲得更佳的類型處理。若客戶端為 BI 工具或任何 JVM 應用程式，請使用 JDBC。若觀察到類型轉換方面的異常情況，應避免在同一連接中混用二進制及文字協定的預期行為——JDBC 的文字模式行為更易於推斷。

---

## 注意事項與限制

**僅限 SQL；不支援 DML 變更操作。** pgwire 監聽器僅解析並執行 SQL——不接受 GraphQL 及 Cypher 字串。（REQ-614）純粹的 `INSERT`、`UPDATE` 及 `DELETE` 不會路由至寫入路徑。（REQ-615）請透過 `COPY FROM STDIN`（可寫入的來源）或 `CREATE TABLE AS` 寫入數據；行級變更則應改為透過 GraphQL、Cypher 或 Trino 的寫入路徑處理。

**COPY 及 DDL 需要 `ddl` 權限。** `COPY`（不論方向）及 DDL 均受角色的 `ddl` 權限所限制；未具備此權限的角色會收到 SQLSTATE 42501。（REQ-616）

**不支援真正的交易功能。** BEGIN/COMMIT/ROLLBACK 會被接受並靜默忽略。每個陳述式均獨立執行。（REQ-587）[tool-verified: `server.py:146-158`——`in_transaction()` 一律返回 `False`]

**DDL 逾時 60 秒，查詢逾時 120 秒。** 這些數值於處理程序執行緒中硬編碼。（REQ-590）針對遠端來源的長時間執行 DDL（大型資料表的結構描述變更）可能會逾時。[tool-verified: `ddl_handler.py:136`, `server.py:186`]

**COPY FROM 僅適用於可寫入的來源。** Iceberg、Hive、僅限 Trino 的來源，以及唯讀的來源類型均不接受 COPY FROM。錯誤代碼為 SQLSTATE 42501。（REQ-586）[tool-verified: `copy_handler.py:65`]

**COPY 輸出格式為 text 或 csv。** 尚未實作 PG 二進制 COPY 格式（`FORMAT binary`）。[inferred：`_rows_to_copy_text` / `_rows_to_copy_csv` 中僅存在 `text` 及 `csv` 分支]

**Trino 路徑上的 DDL 僅限 CREATE。** 不支援針對 Iceberg 或 Hive 目錄執行 ALTER、DROP 及 CREATE INDEX。如需完整 DDL，請以已註冊的 SQL 來源作為 `ddl_catalog`。（REQ-582）[tool-verified: `ddl_handler.py:92-100`]

**參數替換屬字面值形式。** `$1`、`$2`……等參數會在執行前以 SQL 字面值替換，而非以綁定參數形式傳送至底層引擎。這意味著底層引擎永遠不會看到已準備的陳述式。對 Trino 而言此舉沒有實際影響；對於直接連接池的來源，則會繞過已準備陳述式的快取機制。（REQ-581）[tool-verified: `server.py:78-85`]

**`pg_stat_activity`、`pg_stat_user_tables`、`pg_extension`、`pg_enum`、`pg_attrdef`、`pg_proc`。** 這些資料表存在於目錄層中，但屬於空的存根 (stub)。查詢它們的監控工具將收到零行結果，而非錯誤。（REQ-532）[tool-verified: `catalog.py:519-535`, `catalog.py:639-934`]（`pg_index` 已有數據填充——請參閱「目錄攔截」一節。）
