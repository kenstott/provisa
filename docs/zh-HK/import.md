# 自 Hasura 匯入

Provisa 可將現有的 Hasura 中繼資料轉換為 Provisa 的 `config.yaml`，並保留已追蹤資料表、關係、權限及遠端結構描述。

## Hasura v2

### 匯出中繼資料

自您的 Hasura 主控台或 CLI：
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

v2 轉換器會讀取一個 Hasura 中繼資料**目錄**（由 `hasura metadata export` 產出的版面配置，或扁平的 `tables.yaml` / `actions.yaml` 版面配置），並寫出一個 Provisa 設定：

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

省略 `-o` 則會將設定寫至 stdout。

旗標：

| 旗標 | 用途 |
|------|---------|
| `-o`, `--output` | 輸出 YAML 路徑（預設：stdout） |
| `--source-overrides` | 含每個數據來源連線覆寫（主機、連接埠、憑證）的 YAML 檔 |
| `--domain-map` | 以 `SCHEMA=DOMAIN` 配對表示的結構描述至領域對應 |
| `--auth-env-file` | 含驗證設定的 `.env` 檔；轉換 JWT/JWK、管理員密鑰及 claims 對應 |
| `--dry-run` | 剖析並驗證，但不寫出輸出 |

### 轉換內容

| Hasura 概念 | Provisa 對應項目 |
|---------------|-------------------|
| 已追蹤資料表 | 具 `publish: true` 的 `tables[]` |
| 物件關係 | 具 `cardinality: many-to-one` 的 `relationships[]` |
| 陣列關係 | 具 `cardinality: one-to-many` 的 `relationships[]` |
| Select 權限 | 角色可見性 + RLS 篩選條件 |
| 欄位權限 | `visible_to` / `writable_by` |
| Insert/update/delete 權限 | 變異 `writable_by` + RLS |
| 遠端結構描述 | `graphql_remote` 數據來源註冊 |
| 計算欄位 | 具 `kind: query` 的 `functions[]` 項目 |

### 限制

- **Actions** 會自動轉換：HTTP 處理器的 action 會轉為 `webhooks[]` 變異；具有非 HTTP（資料庫）處理器的 action 會轉為 `functions[]` 佔位項目，並發出警告以提示審查該處理器
- **Event triggers** 會轉為逐資料表的 `event_triggers` 設定（作業、webhook URL、重試政策），並發出警告指出保真度有限
- **遠端結構描述**會轉為 `graphql_remote` 數據來源項目
- **自訂 SQL 函式**需要審查——簡單案例會轉為 `functions[]` 項目，複雜案例則需要手動處理
- **Cron triggers** 會轉為 `scheduler` 設定項目，並保留 cron 運算式及啟用旗標

---

## Hasura DDN（v3）

### 找出 HML 專案

DDN 轉換器會直接讀取由 `.hml` 檔案組成的 DDN 專案**目錄**——無須進行 supergraph 建置步驟。專案根目錄下的第一層目錄名稱會被視為子圖名稱；`globals/` 底下的檔案會被指定為 `globals` 子圖。

### 轉換

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

省略 `-o` 則會將設定寫至 stdout。

旗標：

| 旗標 | 用途 |
|------|---------|
| `-o`, `--output` | 輸出 YAML 路徑（預設：stdout） |
| `--source-overrides` | 含每個數據來源連線覆寫的 YAML 檔 |
| `--domain-map` | 以 `SUBGRAPH=DOMAIN` 配對表示的子圖至領域對應 |
| `--aggregates-output` | 彙總運算式附屬檔案的輸出路徑（預設：`<output>-aggregates.yaml`） |
| `--dry-run` | 剖析並驗證，但不寫出輸出 |

`AggregateExpression` 中繼資料會保留於一個附屬的 `*-aggregates.yaml` 檔案中。

### 轉換內容

| DDN 概念 | Provisa 對應項目 |
|------------|-------------------|
| 子圖模型 | 某數據來源下的 `tables[]` |
| 關係 | `relationships[]` |
| 權限規則 | RLS 篩選條件 |
| Command | Webhook 變異或檢視 |
| Connector | 具連線詳情的數據來源項目 |

### 限制

- **Lambda connector**（TypeScript/Python 函式）需要手動設定 webhook
- **Lifecycle plugin** 無直接對應項目
- **DDN 驗證模式**會對應至 Provisa 驗證提供者，惟 JWT claim 路徑可能需要調整

---

## 匯入之後

1. 檢視所產生的 `config.yaml`——留意轉換器發出的 `warnings`
2. 驗證連線憑證（轉換器使用的是佔位值）
3. 啟動 Provisa，並確認資料表出現於 Explorer 中
4. 執行您現有的 GraphQL 查詢——該結構描述相容於常見模式
5. 於啟用生產治理之前，經 Admin API 或 UI 提交查詢以供核准
