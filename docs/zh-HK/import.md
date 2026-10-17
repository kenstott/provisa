# 從 Hasura 匯入 {: #importing-from-hasura }

Provisa 可以將現有的 Hasura 中繼資料轉換為 Provisa 的 `config.yaml`，並保留已追蹤的資料表、關聯、權限和遠端結構描述 (schema)。

## 互動式匯入（管理介面 → Import Hasura Config） {: #interactive-import-admin-import-hasura-config }

管理介面運行相同的轉換器，因此匯入不需要 shell 存取權限，也不需要往返設定檔。此操作需要 `org_settings` 權限；匯入結果會套用至目前工作階段所處理的組織。

1. **上載。** 選擇一個 zip 壓縮的 Hasura v2 中繼資料目錄、一個 zip 壓縮的 DDN 專案、一份整合的中繼資料匯出檔（`.yaml`/`.json`，包括中繼資料 API 傳回的 `{resource_version, metadata}` 封裝格式），或單一 `.hml` 檔案。除非上載內容含糊不清，否則請保留 *Detect automatically*（自動偵測）設定。
2. **對應網域**（可選）。每一組對應會將一個 v2 結構描述或一個 DDN 子圖 (subgraph) 對應到一個 Provisa 網域；未對應的項目會保留其原有名稱。
3. **轉換並預覽。** 伺服器進行轉換，並返回計數、轉換報告（參見[報告](#the-report)）和產生的設定。此步驟不會寫入任何內容。
4. **檢閱及編輯。** 設定內容可以就地編輯——包括連線詳細資料、網域名稱、角色名稱。你套用的內容即為畫面上顯示的內容。
5. **套用。** *套用至本組織*會載入該設定並重建組織的結構描述。匯入一律是與組織現有的設定合併。

端點：`POST /admin/import/hasura/preview` 及 `POST /admin/import/hasura/apply`。

---

## Hasura v2

### 匯出中繼資料 {: #export-metadata }

兩種匯出佈局都可以轉換。

**CLI 的中繼資料目錄。** `hasura metadata export` 會寫出一個目錄。`databases/databases.yaml` 列出每個數據來源的類型和連線，並透過 `!include` 索引指向其資料表和函式：

```bash
hasura metadata export
```

```yaml
# metadata/databases/databases.yaml
- name: default
  kind: postgres
  configuration:
    connection_info:
      database_url:
        from_env: HASURA_GRAPHQL_DATABASE_URL
  tables: "!include default/tables/tables.yaml"
  functions: "!include default/functions/functions.yaml"
```

扁平佈局（根目錄下的 `tables.yaml` 和 `actions.yaml`）同樣可以轉換。

**中繼資料 API 的單一文件。** 中繼資料 API 以單一物件返回整個匯出，其中包含目錄佈局無處存放的各數據來源 `configuration`：

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

裸物件和 `{resource_version, metadata}` 信封都可接受。管理端匯入使用此形式（參見[互動式匯入](#interactive-import-admin-import-hasura-config)）；命令列轉換器只讀取目錄。

### 轉換 {: #convert }

命令列轉換器讀取 Hasura 中繼資料**目錄**（`hasura metadata export` 產生的佈局，或扁平的 `tables.yaml` / `actions.yaml` 佈局），並寫出 Provisa 設定：

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

省略 `-o` 則將設定寫到 stdout。如果還有需要手動重做的內容，報告會輸出到 stderr。

旗標：

| 旗標 | 用途 |
| ------ | --------- |
| `-o`, `--output` | 輸出 YAML 路徑（預設：stdout） |
| `--source-overrides` | 按數據來源覆寫連線的 YAML 檔案（host、port、憑證），以數據來源名稱為鍵 |
| `--domain-map` | 以 `SCHEMA=DOMAIN` 對表示的 schema 到網域的對應 |
| `--auth-env-file` | 含身分驗證設定的 `.env` 檔案；轉換 JWT/JWK、管理員密鑰和 claims 對應 |
| `--dry-run` | 僅解析並驗證，不寫出輸出 |

### 報告 {: #the-report }

每次轉換都會把發現的內容分為三部分。請按此順序閱讀。

1. **原樣帶過來。** 物件在 Provisa 中有直接對應物，轉換後含義不變：數據來源、資料表、已批准的關係、select 權限、cron 觸發器。
2. **對應到更好的 Provisa 功能。** Provisa 有比 Hasura 功能做得更好的功能，於是物件變成該功能。每一項都會說明原因。event 觸發器、action、remote schema、額外的根欄位名，以及繼承自多個角色的角色都歸入此類。
3. **需手動重做。** 物件在 Provisa 中沒有對應物，或匯出缺少轉換所需的內容。每一項都會說明該怎麼做。這些就是警告。

預覽以清單形式返回這三部分。命令列轉換器把它們輸出到 stderr，標題為 `Carried as`、`Mapped to` 和 `Redo by hand`，各帶一個計數。第三部分為空的轉換即告完成。

### 轉換內容對照 {: #what-gets-converted }

| Hasura 概念 | 部分 | Provisa 結果 |
| --------------- | ---- | -------------- |
| 數據來源（`kind`、`connection_info`） | 原樣帶過來 | `sources[]`。匯出中明文寫出的連線會轉換為 host、port、資料庫和憑證。從環境變數讀取的連線不會被猜測（參見[連線與網域](#connections-and-domains-on-the-import-tab)）。 |
| 已追蹤的資料表 | 原樣帶過來 | 一張已註冊的資料表。其自訂名稱（或其 `select` 根欄位）成為資料表別名。 |
| 自訂欄位名（`column_config`，或較舊的 `custom_column_names`） | 原樣帶過來 | 欄位的 `alias`。 |
| `select` 之外的根欄位名（`select_by_pk`、`select_aggregate`、`insert` 等） | 對應 | 按名稱報告。Provisa 把一張資料表作為一個欄位提供：按鍵讀取是對它的篩選，聚合是它的 `_aggregate` 欄位，寫入是它的變更。請確認應用程式的查詢採用這種形態。 |
| Object 和 array relationship | 原樣帶過來 | 一個已批准的關係（object 對應 `many-to-one`，array 對應 `one-to-many`），其 GraphQL 欄位是 Hasura relationship 的名稱。不同的情形見下文。（REQ-1680） |
| select 權限 | 原樣帶過來 | 欄位權限（`visible_to`）和一條行規則。帶工作階段變數的項（`X-Hasura-User-Id`）變為 `current_setting('provisa.hasura_user_id')`，請求在查詢時根據身分的 claims 或成員的屬性綁定它。Hasura 工作階段變數保留自己的名稱；`provisa.user_id` 仍是已登入的身分。（REQ-1682） |
| 穿過 relationship 的行規則 | 原樣帶過來 | 對關聯資料表的 `EXISTS` 子查詢。 |
| insert、update 和 delete 權限 | 原樣帶過來 | 資料表上的寫入授權：每個角色和操作一條。`columns` 成為授權的欄位，`filter` 成為其 `rows`，`check` 成為其 `check`，`set` 成為其預設。 |
| 繼承自一個角色的角色 | 原樣帶過來 | 該角色作為 `parent_role_id`。 |
| 繼承自多個角色的角色 | 對應 | 持有它們授權聯集的角色，聯集被明確寫出：Provisa 角色只有一個父角色。 |
| Computed field | 重做 | 基於該資料表的檢視中的一個欄位。 |
| 已追蹤的函式 | 原樣帶過來 | 一個指令；見下文。否則重做。 |
| Action | 對應 | 呼叫 action 處理程式的指令。 |
| Cron 觸發器 | 原樣帶過來 | 一個排程觸發器。 |
| Event 觸發器 | 對應 | 一個針對資料表變更的觸發器。 |
| Remote schema | 對應 | 一個 `graphql_remote` 數據來源，其根欄位落地為受治理的資料表。（REQ-1681） |

relationship 的情形：

- **衝突。** 資料表上已有與 relationship 同名的欄位時，relationship 採用 Provisa 給它的名稱，報告會發出警告。如果應用程式需要另一個名稱，請在 Relationships 頁面重新命名。
- **同一個 join 宣告兩次。** 第二個宣告了已宣告過的 join 的 relationship 會併入第一個。報告在「原樣帶過來」一類中說明。
- **多個欄位對。** Provisa 關係只在一個欄位對上連接。在多個欄位對上連接的 relationship 會被警告且不會建立，但穿過它的行規則會被帶過來。
- **僅外鍵欄位。** 以 `foreign_key_constraint_on: artist_id` 宣告的 object relationship 在匯出中不指明目標。轉換器透過反向的 array relationship 解析它，沒有時發出警告。

寫入授權詳情：

- 空的 `filter` 或 `check` 表示所有行，寫作 `TRUE`。
- 屬於工作階段變數的 `set` 值（`X-Hasura-User-Id`）成為對應工作階段變數的 `session` 預設（`X-Hasura-User-Id` 成為 `hasura_user_id`）。其他任何值成為 `literal` 預設。預設欄位不會同時出現在授權的欄位中。
- Hasura 隱含的 admin 持有每一項權限。它成為角色 `org_admin`，持有涵蓋所有欄位和所有行的 insert、update 和 delete 授權，並在每個匯入的欄位上被指名。（REQ-1684）
- 每個具有 insert、update 或 delete 權限的角色還會獲得 `write` 權利。

指令詳情：

- **已追蹤的函式。** 匯出指明函式及呼叫它的角色，但不包含它的參數和返回內容。預覽從資料庫的目錄讀取這兩者，並註冊一個帶有所發現簽章的指令。數據來源無法到達、不是 PostgreSQL，或函式缺失、被多載，或返回匯入未註冊的資料表的行時，該函式轉入重做。命令列轉換器從不連接資料庫，因此它發現的每個已追蹤函式都轉入重做。
- **Action。** 指令以 `POST` 呼叫 action 的處理程式。處理程式 URL 中的 `{{VAR}}` 寫作 `${env:VAR}`，在指令被呼叫時解析。指令對 action 權限所指名的角色可見；未指名任何角色時只對 `org_admin` 可見。非同步 action 會被警告：其指令等待處理程式的回應。匯出不含其欄位的返回類型會被警告。
- **Event 觸發器。** 觸發器在資料表的每次變更時觸發，每批變更一次，並發送資料表和已變更的行。它不會按行帶著舊值和新值觸發。監聽少於全部三種操作的 Hasura 觸發器會被警告，因為 Provisa 觸發器對所有操作觸發。重試不會被帶過來：帶 `num_retries` 的 Hasura 觸發器會被警告，因為 Provisa 觸發器只呼叫一次。如果漏掉一次呼叫會有影響，請在處理程式中重試。
- **Cron 觸發器。** cron 表達式、URL、payload 和 enabled 旗標會被帶過來。重試不會被帶過來，並以同樣方式警告。

合併後的文件還包含轉換器沒有目標的部分：`query_collections`、`allowlist`、`rest_endpoints`、`network` 和 `opentelemetry`。出現其中任何一項時，報告會把它列在需重做的內容中。目錄讀取器從不開啟它們的檔案。

### 匯入分頁上的連線與網域 {: #connections-and-domains-on-the-import-tab }

匯出明文寫出的連線按原樣轉換。當匯出從環境變數讀取資料庫 URL（`database_url: {from_env: HASURA_GRAPHQL_DATABASE_URL}`）時，變數的值不在匯出中，因此轉換不會猜測任何值。該數據來源會以空的連線欄位註冊，報告會要求重做：在匯入的數據來源設定中輸入該數據來源的 host、port、資料庫、使用者和密碼，然後再次轉換。只有你修改過的欄位會隨之傳遞，作為數據來源覆寫。網域行涵蓋上傳內容攜帶的每個 schema、subgraph 和 remote schema；每一行是對組織現有網域的選擇器，也接受鍵入的名稱，不匹配任何現有網域時標記為「新網域」。套用會合併到組織已有的內容中。（REQ-1687）

### 類型在預覽時來自數據來源 {: #types-come-from-the-source-at-preview }

Hasura 匯出為欄位命名但不附類型，而沒有權限的已追蹤資料表不會指名任何欄位。預覽會以你提供的數據來源連線執行，因此它會讀取每個可連上的 SQL 數據來源的 `information_schema.columns`：每個沒有類型的欄位都會取得數據來源對映至 IR 詞彙的類型，而沒有欄位的資料表會取得數據來源的每一個欄位，僅對 `org_admin` 可見，因為 Hasura 沒有把它公開給任何其他角色。預覽無法連上的數據來源會以 `[sources]` 警告報告，其欄位維持沒有類型，留待你在套用之前完成。(REQ-1691, REQ-1684)

### 限制 {: #limitations }

- **Computed field** 需手動重做：匯出指明了資料庫函式但沒有定義它。把它新增為基於該資料表的檢視中的一個欄位，或在應用程式中計算。
- **Remote schema** 根據角色權限 SDL 落地為資料表。沒有權限的 remote schema 不會落地任何內容，因為匯出沒有其他關於其形態的陳述。（REQ-1681）
- **已追蹤的函式** 需要在預覽時存取資料庫來讀取其簽章；離線時，在 Commands 頁面重做。
- **event 和 cron 觸發器的重試** 不會被帶過來。

---

## Hasura DDN（v3） {: #hasura-ddn-v3 }

### 找出 HML 專案 {: #locate-the-hml-project }

DDN 轉換器會直接讀取 DDN 專案中 `.hml` 檔案的**目錄**——不需要建置 supergraph 的步驟。專案根目錄下的第一層目錄名稱會被視為子圖名稱；`globals/` 目錄下的檔案會被指派至 `globals` 子圖。

### 轉換 {: #convert_1 }

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

省略 `-o` 會將設定內容寫至標準輸出 (stdout)。

旗標：

| 旗標 | 用途 |
| ------ | --------- |
| `-o`, `--output` | 輸出 YAML 路徑（預設：標準輸出） |
| `--source-overrides` | 包含各來源連線覆寫設定的 YAML 檔案 |
| `--domain-map` | 以 `SUBGRAPH=DOMAIN` 配對表示的子圖至網域對應 |
| `--aggregates-output` | 聚合運算式附屬檔案的輸出路徑（預設：`<output>-aggregates.yaml`） |
| `--dry-run` | 僅解析並驗證，不寫出輸出內容 |

`AggregateExpression` 中繼資料會保留在附屬的 `*-aggregates.yaml` 檔案中。

### 轉換內容對照 {: #what-gets-converted_1 }

| DDN 概念 | Provisa 對應項目 |
| ------------ | ------------------- |
| 子圖模型 | 來源下的 `tables[]` |
| 關聯 | `relationships[]` |
| 權限規則 | RLS 篩選條件 |
| Command | Webhook mutation 或檢視 |
| Connector | 帶有連線詳細資料的來源項目 |

### 限制 {: #limitations_1 }

- **Lambda connector**（TypeScript/Python 函式）需要人手設定 webhook
- **生命週期外掛**沒有直接對應項目
- **DDN 身分驗證模式**會對應至 Provisa 身分驗證提供者，但 JWT 聲明路徑可能需要調整

---

## 匯入後 {: #after-import }

1. 審閱產生的 `config.yaml` 和報告 —— 從 `Redo by hand` 的條目開始
2. 核實連線憑證。匯出原本從環境變數讀取的連線會以空值到達；報告會指明它
3. 啟動 Provisa，確認資料表出現在 Explorer 中
4. 執行你現有的 GraphQL 查詢——結構描述對常見模式相容
5. 在啟用正式環境治理之前，透過管理 API 或使用者介面提交查詢以待批核
