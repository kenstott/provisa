# 環境

環境是一個組織受治理模型的一份具名副本。這份副本在實體上是一個獨立的 PostgreSQL 結構描述 (Schema)——不是一個判別欄位，不是一個前綴，而是一個貨真價實的結構描述——因此每一條既有的存放庫查詢，在環境之內毋須改寫任何東西便是正確的；而一個環境的資料行，也不會因為某個被遺忘的謂詞而落入另一個環境的讀取結果（REQ-1487、REQ-1488）。
[tool-verified: `environments.py` module docstring; `org_schema()` at environments.py lines 86-96]

每個組織一開始都有一個名為 `prod` 的環境。它不能刪除，也不能改名。未指名環境的請求由 `prod` 服務；指名了不存在環境的請求則遭拒。[tool-verified: `PROD = "prod"` at environments.py line 44; `select_environment()`
at env_routing.py lines 93-129]

環境功能提供給採用付費方案的組織。[inferred: REQ-1507]

## 環境名稱

名稱必須符合 `[a-z][a-z0-9_]{1,31}`——二至三十二個字元的小寫字母、數字與底線，且以字母開頭。`prod` 以及以 `pg_` 開頭的名稱會被拒絕。任一組織可用的最大長度取決於該組織自己的 id：PostgreSQL 會靜默截斷超過 63 位元組的識別碼，而環境所推導出的最長結構描述名稱，正是這道上限要防的東西。[tool-verified: `ENV_NAME_PATTERN` at environments.py line 59; `validate_env_name()` at
environments.py lines 119-142; `max_env_name_length()` at environments.py lines 108-116]

## 副本帶走什麼

組織結構描述中的每一張表，都恰好落在一個類別裡（REQ-1489）。這項分類是一份允許清單，而非排除清單：日後新增的表在有人於此為它指定類別之前不會隨行，因此遺漏一張表的失敗表現是一個紅色測試。[tool-verified: `CLASSIFIED`
constant and module docstring, env_classes.py lines 19-22]

| 類別 | 表 | 複製時會發生什麼 |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | 整份複製 |
| IDENTITY_ONLY | sources, api_sources, kafka_sources, kafka_sinks | 身分與治理欄位隨行；連線值留在原處（見「繫結」） |
| SEEDED_AT_CREATION | roles, user_role_assignments | 只在環境首次建立時複製；日後的合併不動它們 |
| PARTIAL | org_settings | 逐索引鍵複製：治理設定隨行，而指名外部目標或逐環境執行階段的索引鍵留在原處 |
| NEVER_SENSITIVE | org_secrets, user_directory | 永不複製 |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log 及其他 | 永不複製 |

[tool-verified: `CARRIED`, `IDENTITY_ONLY`, `SEEDED_AT_CREATION`, `PARTIAL`, `NEVER_SENSITIVE`,
`NEVER_RUNTIME` frozensets, env_classes.py lines 29-113]

`SEEDED_AT_CREATION` 的存在是為了解一個特定的問題。新環境需要角色與指派，否則它一開張就沒有人能動手。但日後某次合併若把 `prod` 的 `developer` 那一行帶過來，就會覆寫掉某個受限分支可能正需要的受限版本，讓審查路徑反倒成了提權途徑。所以角色與指派只在建立時隨行一次，此後便是各環境自己的答案。[tool-verified: env_classes.py lines 65-71; env_copy.py lines 41-44]

## 繫結

繫結就是那些說明數據來源實際指向何處的欄位——`host`、`port`、`database`、`username` 等等。它們在任何複製中都不隨行。尚未繫結的環境會被標記為 `unbound`，而不是留白：空白的主機不等於沒有主機，連線建構器會把它讀成 `localhost:5432`。[tool-verified: `BOUND_COLUMN = "bound"` at
env_classes.py line 143; `BINDING_COLUMNS` dict at env_classes.py lines 155-172]

環境的數據來源以兩種方式之一解析。

**基底**——環境自帶憑證。org_admin 先建立一個基底，然後逐一明確繫結每個數據來源。[tool-verified: `CreateEnvBody.inherit_connections = False` (default) at
environments_router.py line 227; "binding a base is an org_admin's act" comment at line 358]

**分支**——環境以引用方式繼承基底的憑證，什麼都不複製。當查詢需要連線時，解析會沿 `branched_from` 鏈往上走，停在第一個其資料行已繫結的環境。在基底上輪替憑證會自動傳播到它的每一個分支，毋須任何動作；撤銷憑證則一次為全部撤銷。密鑰絕不會在分支、匯出或存放庫可能把它帶走的任何地方被具體化。
[tool-verified: `resolve()` at env_bindings.py lines 114-151; `lineage()` at env_bindings.py
lines 74-102; env_bindings.py module docstring lines 11-33]

要建立分支，請在環境面板中設定**繼承連線**。預設為關閉。
[tool-verified: `environmentsTab.json` key `inheritConnections`; `inheritHelp2` string]

## git 投影

對模型的每一次寫入，都會把結果提交到該環境的 git 分支。存放庫是模型的一份投影，絕非其權威所在：Provisa 讀寫的是控制平面；存放庫是紀錄，不是來源。部署一棵樹需要明確的呼叫——git 主機上已合併的拉取請求並不會自行部署（REQ-1524、REQ-1526）。[tool-verified:
deploy endpoint docstring at environments_router.py lines 777-791]

每個實體對應一個檔案。路徑是去掉配置方式與組織之後的 REQ-1385 URI：`provisa://acme/sales/tables/Order` 變成 `sales/tables/Order.yaml`。數據來源落在 `sources/`，命令落在 `commands/`，指標落在 `metrics/`。由父項串聯而下的子資料行——欄位、關係、RLS 規則——寫在父項的檔案之內，而不是各自成檔。
[tool-verified: `table_path()` at env_files.py line 109-115; `kind_path()` at env_files.py
lines 118-120; `COMMANDS_DIR = "commands"` at env_project.py line 71; env_files.py module
docstring lines 17-24]

命令及其標籤指派能撐過整趟來回。命令上的標籤會被導向該命令自己的檔案（`commands/<name>.yaml`）；不屬於任何檔案的標籤會從投影中消失，並在該樹的下一次部署時被刪除。[tool-verified:
env_project.py lines 346-364; `owner_command_name` routing in `_assignments_for()` at
env_project.py lines 137-164]

沒有任何代理索引鍵會進到檔案裡。`registered_tables.id` 是自動遞增整數——同一個模型在兩個環境中會拿到不同的整數，因此一份天真的傾印會跟自己比出差異。所有代理索引鍵都被丟棄，而每一處對它的引用都寫成目標的路徑。
[tool-verified: `STORAGE_COLUMNS` and `_model_columns()` at env_files.py lines 62-128;
env_project.py docstring lines 26-27]

序列化是決定性的。索引鍵按字母序輸出，子集合按其位址排序，YAML 樣式固定。兩個持有相同模型的環境，產出的樹在位元組層面完全一致。[tool-verified: `dump()` at env_files.py lines 131-143]

## 合併

把一個環境的模型合併進另一個，是按身分更新：來源有的每個物件，都會在目標中被建立或更新。來源已不再持有的物件，只在呼叫方明確要求移除時才會被移除。中途失敗的合併會讓目標維持原狀——一個交易。[tool-verified: `copy_model()` at env_copy.py lines 216-234; REQ-1490 description]

套用之前，請呼叫預覽端點（`GET /{name}/merge-preview`）或傳入 `dry_run: true`。預覽走的是合併所用的同一條程式碼路徑；它是一個 `GET` 端點，因此把旗標寫錯的 CI 指令碼不會誤把它本想查看的合併給套用下去。[tool-verified:
`preview_merge()` docstring at environments_router.py lines 1086-1095]

合併會讓目標的繫結、角色與密鑰維持原封不動。開發環境不會因為從 prod 取得較新的模型而失去自己的資料庫連線；prod 也不會因此取得開發環境的授權。[tool-verified: env_copy.py lines 269-287; REQ-1490 scenario]

### 報告點名了什麼

合併報告會按路徑列出新增、變更、移除與未變動的項目。它也會點名任何**衝突**——即雙方自上次共用提交以來都改動過的物件。衝突只會被回報，不會被解決：來源勝出，這正是合併進目標的含義。Provisa 不提供衝突解決、不提供合併標記、不提供逐物件挑選。衝突清單的價值在於它傳達的訊號——兩個人在互不知情下編輯了同一個物件（REQ-1555）。[tool-verified: `CopyReport.conflicts` at env_copy.py lines 151-165; `detect_conflicts()` called
at env_copy.py lines 261-263; REQ-1555 description]

雙方都改成同一個值的物件是共識，不是衝突。當兩個環境根本沒有共同祖先時，報告中的基底為 `None`，此時空的衝突清單意味著什麼都沒比對過，而不是什麼都沒相撞。[tool-verified: `CopyReport.compared`
property at env_copy.py lines 164-166; env_copy.py lines 255-264]

合併會以一個壓縮提交落在目標的分支上。提交訊息為必填且不得留空——它是這次壓縮所代表的那一段工作的唯一交代。來源的各次提交留在原處，此後仍可按 SHA 部署。
[tool-verified: `_squash()` docstring at environments_router.py lines 663-680;
`MergeBody.message` comment at environments_router.py lines 258-260]

## 拉取

拉取會取來遠端為某環境所持有的內容，並讓它成為模型。它不會直接快轉本機分支；它把取回的樹經由一般的部署路徑套用，因此管束手動部署的那一套驗證與審計，同樣管束拉取。
[tool-verified: `pull_environment()` docstring at environments_router.py lines 1450-1462]

一如合併，拉取也會回報它覆寫了什麼——即傳入的樹所改動、而本機環境自兩條線上次共用提交以來也改動過的物件。未提交的本機變更代表環境已偏移（見下方「歷程」）；拉取會在報告中把它當成一般變更點名。
[tool-verified: REQ-1556 description; `pull_environment()` at environments_router.py
lines 1485-1519]

當兩條線已**分歧**時——雙方各自持有對方沒有的提交——拉取會遭拒。拒絕訊息帶著雙方都碰過的物件清單，好讓那個現在得決定誰的工作留下的人，知道該去看哪些物件。[tool-verified: `state["diverged"]` check at
environments_router.py lines 1491-1503; `_collisions()` at environments_router.py
lines 1581-1602]

## 歷程

每次部署都會把環境的游標在它自己的提交線上往前推。復原退回一個提交；重做則朝復原離開的位置再往前一步。兩項操作都不會移除提交——往回退是新增一個位置，而不是改寫歷程。
[tool-verified: `_move()` docstring at environments_router.py lines 854-868]

分支的種子是它所由建立的那個環境的頂端，因此復原會停在那個播種點，不會走進母環境的提交裡。[tool-verified:
`origin_sha` comment at environments_router.py lines 428-448; `_move()` at
environments_router.py lines 907-916]

`can_undo` 與 `can_redo` 兩個旗標隨環境清單回應一同送出。當投影並未持有控制平面所指名的那個提交時，兩者都回報 `false`——這是設計所承認的一種狀態，稱為**已偏移**。存放庫儲存區從未收到某個特定提交的節點，仍然會列出它的各個環境；只有歷程相關的答案會不同（REQ-1561）。[tool-verified: `_with_history()`
at environments_router.py lines 316-344; REQ-1561 description]

## 授權

環境受兩項權限管束。預設情況下，分析師兩項都不持有（REQ-1573）。
[tool-verified: REQ-1573 description; `MANAGE_CAPABILITY = "environment_management"` and
`SWITCH_CAPABILITY = "environment_switch"` at environments_router.py line 110 and
env_routing.py line 53]

| 權限 | 誰持有（播種時） | 它管束什麼 |
| --- | --- | --- |
| `environment_management` | org_admin, developer | 建立與刪除環境 |
| `environment_switch` | org_admin, developer | 由 prod 以外的任何環境提供服務 |

`prod` 不需要任何權限——未指名任何環境的請求就是由它服務，拒絕它等同拒絕所有請求。

強制是在選取點進行的，早於抵達任何路由之前。缺少 `environment_switch` 的成員，會在所有介面上同時遭拒——HTTP、GraphQL、SQL 與各傳輸協定——因為環境是在中介軟體中繫結的，而不是在個別處理常式裡。
[tool-verified: `select_environment()` at env_routing.py lines 93-129; env_routing.py
module docstring lines 28-34]

不持有任何環境權限的分析師可以查詢 `prod`，並且看不到環境切換器。獲授分析師角色的外包人員看不到環境介面，也無法建立或切換進生產環境以外的任何環境。[tool-verified: REQ-1573 use_case and scenario]

### 環境擁有者的權柄

建立環境是唯讀成員取得模型編輯權的唯一途徑（REQ-1528）。在自己所建立的環境之內，建立者持有 `developer` 角色的各項能力——但扣除數據權限（`write`、`full_results`、`usage`）。是建模型的權限，不是動數據的權限。[tool-verified: `ENVIRONMENT_OWNER_CAPABILITIES` at env_authority.py lines 75-77;
`_DATA_RIGHTS` at env_authority.py lines 74-77; env_authority.py module docstring lines 14-38]

這項授予是在授權當下由 `environments.created_by` 推導而來，絕不寫進任何授權表。刪除該環境，在同一個動作中就把它一併移除。
[tool-verified: env_authority.py module docstring lines 39-42; `environment_owner()` at
env_authority.py lines 84-98]

網域成員資格仍然限制擁有者能改動什麼。開分支改變的是成員可以做什麼；它從不改變他們可以對哪些網域去做（REQ-1530）。
[tool-verified: `domains_within()` at env_authority.py lines 121-145]

## 受保護的環境（REQ-1504）

環境可以設為受保護。合併或部署進受保護的環境時，不會在提出當下就套用；它會被提為一項提案，並且必須由請求者以外的人批准。

一旦組織的成員多於一人，`prod` 便自動受保護。單一成員的組織滿足不了「請求者以外的人」，因此該規則在那裡不予套用——否則會讓 `prod` 無法合併。org_admin 可以把任何環境標記為受保護。
[tool-verified: `is_protected()` at env_approvals.py lines 79-96; `protectedHelp2` UI string
in environmentsTab.json line 28]

合併請求是一筆資料行，不是一個確認對話方塊。批准者按定義就是與請求者不同的人，且在請求發生的那一刻並不在場；短暫的確認框會迫使批准在請求者的工作階段內完成，而那正是這項需求所禁止的唯一安排。[tool-verified: env_approvals.py module docstring lines 11-17]

請求資料行帶著合併報告，連同請求者的訊息。是否過時是在讀取時推導出來的，絕不儲存：在讀取時重新規劃並與儲存的報告比對，是唯一不會出錯的做法。過時的請求必須重新提出。請求者不能批准自己的請求。[tool-verified: `STALE` constant and `effective_state()` at
env_approvals.py lines 53, 215-243; `decide()` lines 265-268]

請求的生命週期狀態：`requested` → `approved`／`rejected` → `applied`。`stale` 是推導而來的。
[tool-verified: `REQUESTED`, `APPROVED`, `REJECTED`, `APPLIED`, `STALE` at env_approvals.py
lines 47-53]

同一道門也處理由存放庫 ref 而來的部署：請求在提案當下就把 SHA 釘住。若該 ref 在提案與裁決之間移動了，批准者讀到的仍是被釘住那個提交的報告，而不是新的那個。[tool-verified: `request_deploy()` at env_approvals.py lines
150-189; env_approvals.py docstring lines 26-27]

!!! note
    合併請求的 UI 在環境面板的**合併請求**分頁下。**報告**欄以計數顯示會有什麼變動；展開該資料行可看到逐物件的細節。[tool-verified: `environmentsTab.json` keys `requestsTitle`, `colReport`,
    `approve`, `reject`]

## `env` CLI 命令

`provisa env deploy` 把某個 ref 上的模型送進一個環境。部署已套用或屬試跑時結束碼為 0；環境受保護而部署只是被提案時，結束碼為 2——把待批准當成已發佈部署的管線是錯的，而結束碼就是這麼說的。[tool-verified: `_cmd_env_deploy()` at cli.py lines 389-411]

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` 把組織的遠端分支帶進本機存放庫。之後部署便可指名 `origin/<branch>`。[tool-verified: `_cmd_env_fetch()` at cli.py lines 414-426]

```
provisa env fetch --org acme --api <url> --token <token>
```

兩個命令都接受 `--api`（Provisa API URL）與 `--token`（bearer 權杖）。在環境中設定 `PROVISA_API_URL` 與 `PROVISA_API_TOKEN`，即可免去每次呼叫都要傳。
[inferred: shared `_api_call()` helper]

以存放庫為底的工作流程，典型的 CI 管線如下：

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## 另見

- [部署](deployment.md)——如何架起環境所連接的控制平面
- [命令](commands.md)——出現在每個環境樹中的追蹤函式與 webhook
