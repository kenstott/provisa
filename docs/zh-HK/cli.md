# CLI 參考 {: #cli-reference }

`provisa` 指令是透過 pip 安裝的嵌入式層級的唯一入口。它啟動執行環境、管理授權、觸發中繼資料發佈、部署模型並控制維護橫幅。你不需要 Docker、Node 或任何外部服務。

使用 `embedded` extra 安裝它，該 extra 還會引入離線 DuckDB 擴充
和內嵌 PostgreSQL 控制平面：

```bash
pip install 'provisa[embedded]'
```

**平台要求。** `provisa run` 需要 Python 3.12 以及具有 pgserver wheel 的平台：
linux x86_64、macOS 或 Windows x86_64。Linux aarch64 既沒有 pgserver wheel 也沒有原始碼分發包，
因此內嵌層無法在該平台上執行。在 aarch64 上請使用容器層級。

---

## 共享選項 {: #shared-options }

多個子命令會呼叫 Provisa HTTP API。它們共享三個旗標和兩個環境
變數。

| 旗標 | 預設值 | 環境變數退回 |
| --- | --- | --- |
| `--api <url>` | `http://127.0.0.1:8000` | `PROVISA_API_URL` |
| `--token <token>` | _（無）_ | `PROVISA_API_TOKEN` |
| `--timeout <seconds>` | 300（`maintenance` 為 30） | _（無）_ |

`--api` 是正在執行的 Provisa 實例的基礎 URL。在多租戶模式下，主機名稱指定組織，所以 `https://acme.provisa.org` 會路由到 acme 的租戶。`--token` 是 Bearer 權杖。它為空時不傳送 `Authorization` 標頭，這正是未啟用認證的部署所需要的。

在你的 CI 環境中同時設定這兩個變數，以避免每次呼叫都重複輸入：

```bash
export PROVISA_API_URL=https://acme.provisa.org
export PROVISA_API_TOKEN=<token>
```

接受這些旗標的子命令：`metadata export`、每個 `env` 子命令（`list`、`show`、
`create`、`delete`、`diff`、`merge`、`deploy`、`revert`、`redo`、`fetch`、`push`、`pull`）、
`maintenance on`、`maintenance off`、`maintenance status`。

---

## provisa run

在單一程序中啟動嵌入式 Provisa 系統：API 伺服器，以及提供介面靜態檔案及代理的伺服器。不涉及 Docker、Node 或外部服務。

```
provisa run [--demo] [--host HOST] [--api-port PORT] [--ui-port PORT]
            [--no-browser] [--reset] [--data-dir DIR]
```

### 旗標 {: #flags }

| 旗標 | 預設值 | 說明 |
| --- | --- | --- |
| `--demo` | 關閉 | 載入內建示範——透過內嵌 SQLite 提供的 pet-store 和 shelter 範例網域 |
| `--host` | `127.0.0.1` | 兩個伺服器的綁定位址 |
| `--api-port` | `8000` | API 伺服器連接埠 |
| `--ui-port` | `3000` | UI 靜態/代理伺服器連接埠 |
| `--no-browser` | 關閉 | UI 就緒時跳過開啟瀏覽器；仍會印出 URL |
| `--reset` | 關閉 | 在啟動前刪除並重建內嵌控制平面儲存；在 Provisa 升級後如果啟動報告結構描述不匹配，請使用此選項 |
| `--data-dir` | `~/.provisa/native` | 儲存內嵌 PostgreSQL 叢集和 DuckDB 擴充快取的目錄 |

### 環境變數 {: #environment-variables }

`provisa run` 在啟動 HTTP 伺服器之前會讀取若干附加變數。
設定它們可覆蓋 `load_profile("native", ...)` 原本會應用的預設值。

| 變數 | 作用 |
| --- | --- |
| `TRINO_HOST` / `TRINO_PORT` | 用客戶提供的 Trino 協調器替換內嵌 DuckDB 引擎 |
| `PROVISA_ENGINE_URL` | 指向外部聯邦引擎的另一種方式 |
| `PROVISA_CONFIG` | 要載入的設定檔；`--demo` 會將其設定為內建示範設定 |
| `PROVISA_DEMO` | 由 `--demo` 設定為 `1`；將工作階段標記為示範執行 |
| `PROVISA_DEMO_DIR` | 示範範例數據目錄的路徑；由 `--demo` 設定 |
| `PROVISA_DUCKDB_EXT_DIR` | 預先準備好的 DuckDB 擴充目錄；如果存在 `provisa-duckdb-ext` 包，會自動從中設定；不存在則 DuckDB 會在首次使用時從網絡下載 |

### 啟動順序 {: #startup-sequence }

1. 平台檢查——如果 Python 不受支援或缺少 pgserver，則以明確的訊息中止。
2. `--reset`（如果請求）——刪除內嵌 PostgreSQL 叢集；它會在下一步重建。
3. 示範設定（如果指定 `--demo`）——設定 `PROVISA_CONFIG` 和 `PROVISA_DEMO_DIR`。
4. 內嵌環境——啟動 PostgreSQL 控制平面，解析其socket URL，並在安裝了
   `provisa-duckdb-ext` 時預先準備離線 DuckDB 擴充。
5. 結構描述漂移檢查——掃描即時控制平面以查找缺失的欄位。如果發現任何缺失，
   會印出 `--reset` 提示並以結束代碼 1 結束。V1 沒有遷移；較新版本中新增的欄位
   需要重設。
6. 兩個伺服器同時啟動。就緒通知器輪詢 `GET /ready`（而不是 `/health`，因為 `/ready` 端點確認儲存已連線且引擎已預熱），並在其傳回 200 時開啟瀏覽器。

### 結束代碼 {: #exit-codes }

| 代碼 | 含義 |
| --- | --- |
| 0 | 正常關閉（Ctrl-C） |
| 1 | 啟動錯誤（平台檢查失敗、示範設定缺失、偵測到結構描述漂移） |

### 範例 {: #example }

```bash
# Start with the demo data
provisa run --demo

# Start on non-default ports, no browser
provisa run --api-port 8080 --ui-port 4000 --no-browser

# Upgrade: reset the control plane first, then start
provisa run --reset

# Point at an external Trino cluster instead of the embedded DuckDB engine
TRINO_HOST=trino.internal TRINO_PORT=8080 provisa run
```

---

## provisa license apply

離線驗證並安裝授權檔案。該檔案是由
provisa.dev 頒發的 `license.json`。

```
provisa license apply <file>
```

| 參數 | 說明 |
| --- | --- |
| `file` | 授權檔案的路徑；會應用 `~` 展開 |

結束代碼 0 表示授權有效且已安裝。結束代碼 1 表示授權被拒絕；
原因會印出到 stderr。

```bash
provisa license apply ~/Downloads/license.json
```

---

## provisa license status

顯示機器 ID、試用狀態、經過天數以及授權有效性。

```
provisa license status
```

沒有旗標。列印五行（機器 ID、首次記錄日期、已過天數、試用狀態及授權狀態），並以 0 結束。

```
Machine ID:   a1b2c3d4e5f6...
First seen:   2026-07-01
Elapsed:      83.0 days
Trial:        active
Licensed:     no (no license installed)
```

---

## provisa metadata export

觸發正在執行的伺服器按需發佈中繼資料。該指令向 `POST /admin/metadata-export/publish` 傳送請求，也就是 Admin 分頁中 **立即發佈** 按鈕呼叫的端點，所以兩條路徑傳送的是同一份完整快照。

```
provisa metadata export [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| 旗標 | 預設值 | 說明 |
| --- | --- | --- |
| `--api` | `$PROVISA_API_URL`，其次是 `http://127.0.0.1:8000` | 在多租戶模式下，主機名稱識別組織 |
| `--token` | `$PROVISA_API_TOKEN` | 持有 `org_settings` 的身份的 Bearer 權杖；在未啟用身分驗證的部署中可省略 |
| `--timeout` | `300` | 放棄 HTTP 呼叫前等待的秒數 |

| 結束代碼 | 含義 |
| --- | --- |
| 0 | 每項資產都已發佈 |
| 1 | 部分發佈或連線失敗；逐項資產的錯誤會印出到 stderr |

```bash
provisa metadata export \
  --api  https://acme.provisa.org \
  --token "$PROVISA_API_TOKEN"

# Cron example — daily at 06:00
# 0 6 * * *  provisa metadata export --api https://acme.provisa.org >> /var/log/provisa-export.log 2>&1
```

完整的設定參考見 [中繼資料匯出](metadata-export.md#from-the-command-line)，涵蓋提供者、憑證、`reconcile_cron` 以及快照內容。

---

## provisa env

針對正在執行的 Provisa 執行環境生命週期。每個子命令都是介面所發起的同一個管理 API
呼叫；CLI 不做任何決定。副本攜帶內容的清單、合併與移除規則、綁定、保護以及呼叫方的權利
都屬於伺服器，因此無論以何種方式呼叫該操作，它們都同樣適用。

```
provisa env {list,show,create,delete,diff,merge,deploy,revert,redo,fetch,push,pull} --org ORG ...
```

### 每個子命令都接受的旗標 {: #flags-every-subcommand-takes }

| 旗標 | 必填 | 說明 |
| --- | --- | --- |
| `--org` | 是 | 環境所屬的組織 |
| `--json` | 否 | 以 JSON 印出 API 回應，供指令碼使用 |
| `--api` | 否 | 參見[共享選項](#shared-options) |
| `--token` | 否 | 獲准執行該操作的身份的 Bearer 權杖 |
| `--timeout` | 否 | 秒；預設 300 |

作用於單一環境的子命令還接受 `--env ENV`（必填）。`list`、`diff`、
`merge` 和 `fetch` 不接受；`diff` 和 `merge` 用 `--from` 和 `--into` 指定它們的環境。

### 結束代碼 {: #exit-codes_1 }

| 代碼 | 含義 |
| --- | --- |
| 0 | 完成，或未應用任何內容的試執行 |
| 1 | 被拒絕。拒絕規則的代碼以 `refused (<code>): <detail>` 印出到 stderr；使用 `--json` 時為 stdout 上的 `{"refused": {...}}` |
| 2 | 已提議，未應用：目標環境受保護，變更正在等待批准 |

結束代碼 2 是有意為之。把等待中的批准當作已發出的部署的管線
會出錯。

### provisa env list

列出組織的環境以及每個環境所在的建構。除共享旗標外沒有其他旗標。

```
provisa env list --org ORG [--json]
```

### provisa env show

顯示一個環境及其目前建構。如果
環境不存在，以 1 和 `environments.unknown` 結束。

```
provisa env show --org ORG --env ENV [--json]
```

### provisa env create

建立一個持有另一個環境模型的環境，其數據在建立時選定。由 API
決定呼叫方可以做出哪些選擇。

```
provisa env create --org ORG --env NAME --data-mode MODE
                   [--from SOURCE] [--mutation-handling HANDLING] [--expires-at ISO8601]
```

| 旗標 | 必填 | 說明 |
| --- | --- | --- |
| `--env` | 是 | 新環境的名稱 |
| `--from` | 否 | 作為起點的環境；預設 `prod` |
| `--data-mode` | 是 | `inherit`、`unbound`、`test_fake` 或 `test_synthetic`：按引用使用數據來源連線、在綁定自己的連線之前沒有連線，或測試數據 |
| `--mutation-handling` | 否 | `refused`、`reversible` 或 `direct`；預設 `refused` |
| `--expires-at` | 否 | ISO-8601 時刻，此後環境被移除 |

```bash
provisa env create --org acme --env feature-x --from prod --data-mode test_fake
```

### provisa env delete

停用一個環境。只有點名時才刪除其分支。

```
provisa env delete --org ORG --env ENV [--delete-branch] [--delete-remote-branch]
```

| 旗標 | 說明 |
| --- | --- |
| `--delete-branch` | 同時刪除其分支 |
| `--delete-remote-branch` | 同時刪除其在遠端儲存庫上的分支 |

### provisa env diff

報告將一個環境合併到另一個環境會做什麼。它是 `merge-preview` 的 GET，因此
它不能寫入；把它交給不應更改任何內容的執行器。

```
provisa env diff --org ORG --from SOURCE --into TARGET [--removals] [--select PATH ...]
```

| 旗標 | 必填 | 說明 |
| --- | --- | --- |
| `--from` | 是 | 合併來源環境 |
| `--into` | 是 | 合併目標環境 |
| `--removals` | 否 | 同時從目標中移除來源已不再持有的內容 |
| `--select PATH` | 否 | 將合併限制為該物件，按其在模型中的路徑（`metrics/revenue.yaml`）；重複以添加其他物件。它所需而目標缺少的內容隨之帶入並列出 |

### provisa env merge

按身份將一個環境合併到另一個環境。

```
provisa env merge --org ORG --from SOURCE --into TARGET
                  [--removals] [--select PATH ...] [--dry-run] [--message MSG]
                  [--retire-source] [--retire-remote]
```

| 旗標 | 必填 | 說明 |
| --- | --- | --- |
| `--from` | 是 | 合併來源環境 |
| `--into` | 是 | 合併目標環境 |
| `--removals` | 否 | 同時從目標中移除來源已不再持有的內容 |
| `--select PATH` | 否 | 將合併限制為該物件，按其在模型中的路徑（`metrics/revenue.yaml`）；重複以添加其他物件。它所需而目標缺少的內容隨之帶入並列出 |
| `--dry-run` | 否 | 印出合併報告，不應用任何內容 |
| `--message` | 否 | 合併做了什麼；應用時必填 |
| `--retire-source` | 否 | 當來源的工作已進入目標時移除來源 |
| `--retire-remote` | 否 | 配合 `--retire-source`，同時刪除來源在遠端儲存庫上的分支 |

### provisa env deploy

將某個 git 引用處的模型部署到環境中，使該樹成為環境的目前模型
。這是部署管線執行的命令；部署始終是
針對指定控制平面、帶身份的呼叫。

```
provisa env deploy --org ORG --env ENV --ref REF [--dry-run] [--message MSG]
```

| 旗標 | 必填 | 說明 |
| --- | --- | --- |
| `--env` | 是 | 將持有所部署模型的環境 |
| `--ref` | 是 | 組織儲存庫中的分支或提交 |
| `--dry-run` | 否 | 報告部署會做什麼；不應用任何內容 |
| `--message` | 否 | 目標受保護時隨批准請求攜帶的說明 |

載入到已有環境中時絕不應用角色。沒有 `--seed`：API 以
`environments.seed_only_at_creation` 拒絕 seed。

```bash
# Fetch remote branches, then deploy
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"

provisa env deploy \
  --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

### provisa env revert

將環境退回到之前的建構。不帶 `--to` 時，它在環境自己的歷史中
後退一個提交。帶 `--to` 時，它就是對該建構的普通部署。

```
provisa env revert --org ORG --env ENV [--to BUILD] [--message MSG]
```

| 旗標 | 必填 | 說明 |
| --- | --- | --- |
| `--env` | 是 | 要退回的環境 |
| `--to` | 否 | 要部署的建構；預設為目前建構之前的那個 |
| `--message` | 否 | 隨批准請求攜帶的說明 |

### provisa env redo

再次前進到退回所離開的建構。

```
provisa env redo --org ORG --env ENV
```

### provisa env fetch

將組織的遠端分支拉取到其 Provisa 儲存庫中。當你想指定 `origin/<branch>` 時，
在部署之前執行它。Provisa 從不按計時器執行它。

```
provisa env fetch --org ORG
```

每拉取一個分支印出一行，`origin/<name>  <sha12>`。

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# origin/main  a1b2c3d4e5f6
# origin/dev   9f8e7d6c5b4a
```

### provisa env push

將環境的分支發送到組織的遠端儲存庫。沒有可發送的內容時
印出 `nothing to send`。

```
provisa env push --org ORG --env ENV
```

### provisa env pull

應用遠端儲存庫為某個環境持有的內容。沒有可應用的內容時印出
`<env> already holds what the remote does`。結束代碼 2 的含義與
deploy 相同。

```
provisa env pull --org ORG --env ENV
```

關於環境類別、保護規則、合併報告以及
批准生命週期的完整說明，請參閱[環境](environments.md#the-env-cli-commands)。

---

## provisa maintenance on

在部署上升起計劃維護橫幅。在會令資料平面停機的計劃工作之前執行它。切換 `var.engine_cluster_mode` 就是一例，因為它會取代引擎叢集及其上的每個分片。

```
provisa maintenance on [--message MSG] [--ends-at ISO8601]
                       [--api URL] [--token TOKEN] [--timeout SECONDS]
```

| 旗標 | 說明 |
| --- | --- |
| `--message` | 覆蓋部署環境的標準措辭；預設使用伺服器的標準資訊 |
| `--ends-at` | 預計工作結束的 ISO-8601 時間點，例如 `2026-08-14T22:30:00Z`；預設無預估值 |
| `--api` | 參見[共享選項](#shared-options) |
| `--token` | 持有 `platform_settings` 的身份的 Bearer 權杖 |
| `--timeout` | 預設 30 秒 |

印出結果橫幅狀態並以結束代碼 0 結束。在 HTTP 或連線失敗時擲回 `SystemExit`。

```bash
provisa maintenance on \
  --message "Engine cluster rolling upgrade" \
  --ends-at "2026-09-22T03:00:00Z" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## provisa maintenance off

在工作完成後清除維護橫幅。

```
provisa maintenance off [--api URL] [--token TOKEN] [--timeout SECONDS]
```

印出結果橫幅狀態（active: false）並以結束代碼 0 結束。

```bash
provisa maintenance off --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
# Maintenance notice: OFF
```

---

## provisa maintenance status

在不改變維護橫幅的情況下顯示其目前狀態。

```
provisa maintenance status [--api URL] [--token TOKEN] [--timeout SECONDS]
```

印出橫幅狀態並以結束代碼 0 結束。

```
Maintenance notice: ON
  Message:  Engine cluster rolling upgrade
  Since:    2026-09-22T01:15:00Z
  Ends at:  2026-09-22T03:00:00Z
```

---

## 快速參考 {: #quick-reference }

| 命令 | 作用 |
|---|---|
| `provisa run` | 啟動內嵌 API + UI |
| `provisa run --demo` | 使用 pet-store / shelter 範例數據啟動 |
| `provisa run --reset` | 在啟動前重建控制平面 |
| `provisa license apply <file>` | 離線安裝授權檔案 |
| `provisa license status` | 顯示機器 ID 及試用/授權狀態 |
| `provisa metadata export` | 按需發佈中繼資料快照 |
| `provisa env list --org ORG` | 列出組織的環境及其建構 |
| `provisa env show --org ORG --env ENV` | 顯示一個環境 |
| `provisa env create --org ORG --env ENV --data-mode MODE` | 從另一個環境建立環境 |
| `provisa env delete --org ORG --env ENV` | 停用一個環境 |
| `provisa env diff --org ORG --from A --into B` | 預覽合併（唯讀） |
| `provisa env merge --org ORG --from A --into B` | 將一個環境合併到另一個環境 |
| `provisa env deploy --org ORG --env ENV --ref REF` | 將引用部署到環境中 |
| `provisa env revert --org ORG --env ENV [--to BUILD]` | 後退一個提交，或部署某個建構 |
| `provisa env redo --org ORG --env ENV` | 再次前進 |
| `provisa env fetch --org ORG` | 將遠端分支拉取到 Provisa 儲存庫 |
| `provisa env push --org ORG --env ENV` | 將環境的分支發送到遠端儲存庫 |
| `provisa env pull --org ORG --env ENV` | 應用遠端儲存庫持有的內容 |
| `provisa maintenance on` | 升起維護橫幅 |
| `provisa maintenance off` | 清除維護橫幅 |
| `provisa maintenance status` | 顯示目前橫幅狀態 |

## 另請參閱 {: #see-also }

- [環境](environments.md) —— 環境模型、受保護環境、部署審批生命週期
- [中繼資料匯出](metadata-export.md) —— 目錄提供者、設定，以及快照包含的內容
- [部署](deployment.md) —— 容器層級和雲端部署
