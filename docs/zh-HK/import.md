# 從 Hasura 匯入

Provisa 可以將現有的 Hasura 中繼資料轉換為 Provisa 的 `config.yaml`，並保留已追蹤的資料表、關聯、權限和遠端結構描述 (schema)。

## 互動式匯入（管理介面 → Import Hasura Config）

管理介面運行相同的轉換器，因此匯入不需要 shell 存取權限，也不需要往返設定檔。此操作需要 `org_settings` 權限；匯入結果會套用至目前工作階段所處理的組織。

1. **上載。** 選擇一個 zip 壓縮的 Hasura v2 中繼資料目錄、一個 zip 壓縮的 DDN 專案、一份整合的中繼資料匯出檔（`.yaml`/`.json`，包括中繼資料 API 傳回的 `{resource_version, metadata}` 封裝格式），或單一 `.hml` 檔案。除非上載內容含糊不清，否則請保留 *Detect automatically*（自動偵測）設定。
2. **對應網域**（可選）。每一組對應會將一個 v2 結構描述或一個 DDN 子圖 (subgraph) 對應到一個 Provisa 網域；未對應的項目會保留其原有名稱。
3. **轉換及預覽。** 伺服器會執行轉換，並傳回計數、轉換器警告及產生的設定內容。此步驟不會寫入任何內容。
4. **檢閱及編輯。** 設定內容可以就地編輯——包括連線詳細資料、網域名稱、角色名稱。你套用的內容即為畫面上顯示的內容。
5. **套用。** *Replace the existing semantic layer*（取代現有的語義層）會刪除設定內容中未包含的所有來源、資料表、角色及規則；若不勾選此選項，匯入的內容會與組織現有的設定合併。套用後會載入該設定並重建組織的結構描述。

端點：`POST /admin/import/hasura/preview` 及 `POST /admin/import/hasura/apply`。

---

## Hasura v2

### 匯出中繼資料

在你的 Hasura 主控台或 CLI 中執行：

```bash
hasura metadata export --output metadata.yaml
```

或使用 Hasura API：

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### 轉換

v2 轉換器會讀取一個 Hasura 中繼資料**目錄**（由 `hasura metadata export` 產生的目錄結構，或扁平的 `tables.yaml` / `actions.yaml` 結構），並寫出一份 Provisa 設定檔：

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

省略 `-o` 會將設定內容寫至標準輸出 (stdout)。

旗標：

| 旗標 | 用途 |
| ------ | --------- |
| `-o`, `--output` | 輸出 YAML 路徑（預設：標準輸出） |
| `--source-overrides` | 包含各來源連線覆寫設定（主機、連接埠、憑證）的 YAML 檔案 |
| `--domain-map` | 以 `SCHEMA=DOMAIN` 配對表示的結構描述至網域對應 |
| `--auth-env-file` | 包含身分驗證設定的 `.env` 檔案；會轉換 JWT/JWK、管理員密鑰及聲明對應 |
| `--dry-run` | 僅解析並驗證，不寫出輸出內容 |

### 轉換內容對照

| Hasura 概念 | Provisa 對應項目 |
| --------------- | ------------------- |
| 已追蹤資料表 | 帶有 `publish: true` 的 `tables[]` |
| 物件關聯 | 帶有 `cardinality: many-to-one` 的 `relationships[]` |
| 陣列關聯 | 帶有 `cardinality: one-to-many` 的 `relationships[]` |
| Select 權限 | 角色可視性 + RLS 篩選條件 |
| 欄權限 | `visible_to` / `writable_by` |
| Insert/update/delete 權限 | Mutation 的 `writable_by` + RLS |
| 遠端結構描述 | `graphql_remote` 來源註冊 |
| 運算欄位 | 帶有 `kind: query` 的 `functions[]` 項目 |

### 限制

- **Actions** 會自動轉換：HTTP 處理常式的 action 會轉為 `webhooks[]` mutation；使用非 HTTP（資料庫）處理常式的 action 會轉為 `functions[]` 佔位項目，並發出警告以提示需要檢閱該處理常式
- **事件觸發器**會轉換為各資料表的 `event_triggers` 設定（操作、webhook 網址、重試原則），並發出警告，說明轉換的精確度有限
- **遠端結構描述**會轉換為 `graphql_remote` 來源項目
- **自訂 SQL 函式**需要人手檢閱——簡單的情況會轉換為 `functions[]` 項目，複雜的情況則需要人手處理
- **Cron 觸發器**會轉換為 `scheduler` 設定項目，並保留 cron 運算式及啟用旗標

---

## Hasura DDN（v3）

### 找出 HML 專案

DDN 轉換器會直接讀取 DDN 專案中 `.hml` 檔案的**目錄**——不需要建置 supergraph 的步驟。專案根目錄下的第一層目錄名稱會被視為子圖名稱；`globals/` 目錄下的檔案會被指派至 `globals` 子圖。

### 轉換

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

### 轉換內容對照

| DDN 概念 | Provisa 對應項目 |
| ------------ | ------------------- |
| 子圖模型 | 來源下的 `tables[]` |
| 關聯 | `relationships[]` |
| 權限規則 | RLS 篩選條件 |
| Command | Webhook mutation 或檢視 |
| Connector | 帶有連線詳細資料的來源項目 |

### 限制

- **Lambda connector**（TypeScript/Python 函式）需要人手設定 webhook
- **生命週期外掛**沒有直接對應項目
- **DDN 身分驗證模式**會對應至 Provisa 身分驗證提供者，但 JWT 聲明路徑可能需要調整

---

## 匯入後

1. 檢閱產生的 `config.yaml`——留意轉換器發出的 `warnings`
2. 驗證連線憑證（轉換器使用的是預留位置值）
3. 啟動 Provisa，確認資料表出現在 Explorer 中
4. 執行你現有的 GraphQL 查詢——結構描述對常見模式相容
5. 在啟用正式環境治理之前，透過管理 API 或使用者介面提交查詢以待批核
