# 遠端結構描述

遠端結構描述來源將外部 API——GraphQL（含 GitHub）、gRPC 或 REST（OpenAPI）——連接至 Provisa 模型。新增來源不會註冊任何資料表。來源會提供資料表，由 data steward 透過「Register Table」選擇器註冊所需的每一張表；該註冊即為策展（curation）步驟。（REQ-308、REQ-316、REQ-322）已註冊的資料表是一級的 Provisa 資料表。（REQ-308、REQ-316、REQ-325）所有治理規則、查詢介面及安全層均會自動套用。（REQ-310、REQ-319、REQ-328）遠端服務永遠不會看到 Provisa 的治理規則。（REQ-310、REQ-319、REQ-328）

---

## 三種來源類型

### GraphQL 遠端結構描述（REQ-307–313）

**如何新增來源。** 向 `/admin/sources/graphql-remote` 發送 POST 請求，附上端點 URL、命名空間及可選的驗證資訊。Provisa 會對遠端端點發起標準的 `__schema` 內省查詢，以確認端點及憑證。（REQ-307）[tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

新增來源不會註冊任何資料表或 command。每種遠端來源類型在新增和刷新時都回傳相同的計數：`tables`（已註冊或已更新至最新的資料表；新增時為 0）、`available_tables`（可提供的資料表）、`mutations`（始終為 0）及 `available_mutations`（可提供的 command）。[tool-verified: `provisa/api/admin/schema_common.py` `remote_source_counts`; `provisa/api/admin/graphql_remote_router.py` `register_graphql_remote_source`]

**註冊資料表。** 開啟 Tables，再開啟 Register Table，選擇來源及結構描述 `graphql`，然後選擇所需的資料表和欄位。透過管理端 GraphQL API：`availableTables(sourceId, schemaName)` 列出可提供的資料表，`availableColumns` 列出某張資料表的欄位，`registerTable(input: TableInput)` 依所選的欄位註冊一張資料表。已註冊的資料表隨即受到治理。（REQ-308）[tool-verified: `provisa/api/admin/introspect.py` `_native_tables_graphql` (`if schema_name != "graphql": return []`), `provisa/api/admin/_graphql_table_registration.py` `offered_tables`, `offered_columns`, `columns_to_register`][tool-verified: `provisa/api/admin/schema_query.py` `available_tables`, `available_columns`; `registerTable` is from the task brief, not read]

已註冊資料表的讀取方式（根欄位、列路徑、必要引數、分頁引數）儲存於 `sources.mapping["tables"]`，因此重新啟動後的程序無需向遠端服務查詢其結構描述即可讀取。[tool-verified: `_graphql_table_registration.py` `TABLE_SPECS_KEY = "tables"`, `remember_table`]

```json
{
  "source_id": "petstore-gql",
  "url": "https://api.example.com/graphql",
  "namespace": "petstore",
  "domain_id": "veterinary",
  "auth": { "type": "bearer", "token": "..." },
  "cache_ttl": 300,
  "field_overrides": { "createPet": "query" },
  "relationships": [
    { "source_table": "petstore__pets", "source_column": "owner_id",
      "target_table": "owners__users", "target_column": "id" }
  ]
}
```

驗證選項：`none`、`bearer`（Authorization 標頭）、`basic`（Base64 編碼的使用者名稱:密碼）。（REQ-307）[tool-verified: `provisa/graphql_remote/introspect.py:36–45`]

**欄位覆寫。** `field_overrides` 是一個 `{fieldName: "query" | "mutation"}` 對應表，於內省後套用，其優先於結構性分類。只有 query 類型的欄位可重新分類為 mutation；mutation 類型的欄位在 GraphQL 中沒有覆寫路徑。（REQ-531）[tool-verified: `provisa/graphql_remote/mapper.py`]

**註冊時的關聯。** `relationships` 於註冊時宣告資料表之間的外部索引鍵/主索引鍵連接路徑，並儲存為手動宣告的關聯（沒有 `remote_managed` 旗標）。刷新時，自動偵測的關聯（帶有 `remote_managed: True` 者）會重新執行並可能改變；手動宣告的關聯則不受影響。（REQ-554）[tool-verified: `provisa/api/admin/graphql_remote_router.py`]

**來源提供的內容。** 遠端 `Query` 類型上每個傳回物件或物件清單的欄位都會作為資料表提供，單一物件欄位下的每個 Relay 連線亦然（見下文）。註冊某個已提供的資料表即使其成為資料表。遠端 `Mutation` 類型上的每個欄位都是一個可提供的命令，計入 `available_mutations`；新增來源不會註冊其中任何一個。將所需的註冊為命令；參見[遠端來源的寫入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-308、REQ-1924） [tool-verified: `provisa/graphql_remote/mapper.py:243–278`, `graphql_remote_router.py` `register_graphql_remote_source` (`"functions": 0`)]

**資料表命名。** 資料表命名為 `{namespace}__{field_name}`。以命名空間 `petstore` 及查詢欄位 `pets` 為例：資料表名稱為 `petstore__pets`。（REQ-312）[tool-verified: `provisa/graphql_remote/mapper.py:250`]

**Relay 連線。** 許多 API 以 Relay 連線回傳清單：一個物件，帶有 `nodes`（或 `edges { node }`）以及 `pageInfo`。Provisa 將連線對應為其節點的資料表，並逐頁讀取。（REQ-308、REQ-309）[tool-verified: `provisa/graphql_remote/mapper.py` `_is_connection`, `_map_connection_table`]

- 回傳連線的根欄位（`securityAdvisories`）會成為其節點的一張資料表。
- 根欄位所回傳的單一物件上的連線會成為獨立的資料表。該資料表沿用根欄位的必要引數。對於 `repository(owner, name)` 及 `Repository` 上的連線 `issues`，資料表為 `repositoryIssues`，在命名空間 `gh` 下的 SQL 名稱為 `gh__repository_issues`。可透過 `_nf_owner` 和 `_nf_name` 欄位對其篩選：`WHERE _nf_owner = 'acme' AND _nf_name = 'widgets'`。
- 連線絕不會成為欄位。否則每一列都要為其類型所擁有的每個連線攜帶一次由遠端逐列計算的讀取。
- 僅當連線欄位接受 `first` 和 `after`、從而可逐頁讀取時，它才是資料表。需要自身引數的連線不是資料表。聯集類型（union）的連線，以及回傳清單的根欄位之下的任何連線，也都不是。

[tool-verified: `provisa/graphql_remote/mapper.py` `_map_connection_table`, `_map_child_connection_tables`; `tests/unit/test_graphql_remote_relay.py` `test_child_connection_table_takes_the_root_fields_arguments`]

**類型對應（REQ-308）。** 純量欄位會直接對應至 Provisa 類型。OBJECT 欄位則按目標類型是否受治理而分為兩種情況（見下方「受治理資料表」）。[tool-verified: `provisa/graphql_remote/mapper.py:14–36`, `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]

| GraphQL 類型 | Provisa 類型 |
| --- | --- |
| `String` | `text` |
| `ID` | `text` |
| `Int` | `integer` |
| `Float` | `numeric` |
| `Boolean` | `boolean` |
| OBJECT（未受治理的內嵌類型，例如 `ContactInfo`） | `jsonb` blob 欄 |
| OBJECT（受治理的目標類型） | 完全從 SDL 及擷取中排除 |
| 任何 ENUM | `jsonb` |
| 自訂純量 | `text`（後備值） |

**受治理資料表。** 若 GQL 類型在遠端結構描述中以 `Query` 的根欄位形式出現，即屬受治理類型。`_collect_queryable_types` 會於註冊期間收集這些類型，並優先選取沒有必要引數的欄位，使其可作為聯結目標進行批量擷取。[tool-verified: `provisa/graphql_remote/mapper.py:395–413`]

當受治理資料表上的 OBJECT 類型欄指向另一個受治理類型時，該欄位會同時受三項規則規範 [tool-verified: `provisa/api/data/endpoint.py:655–671`, `provisa/compiler/schema_gen.py:481–485`]：

1. **從 GQL 擷取中排除**——擷取父資料表的資料列時，不會請求該欄位。
2. **從 SDL 中排除**——該欄位不會出現在生成結構描述中的父類型上。
3. **僅可透過已宣告的關聯存取**——data steward 必須在兩個已具體化的受治理資料表之間註冊 JOIN。若無此關聯，該欄位純粹缺席；並無 blob 後備方案。

無法作為根 Query 欄位存取的 OBJECT 類型（例如 `ContactInfo` 或 `Address` 等內嵌類型）遵循不同的規則：它們會以 `jsonb` blob 欄的形式擷取，並於 SDL 中呈現為巢狀物件欄位。子欄位可透過 SQL 中的 `-->>` 擷取存取。

**需要引數的欄位不是欄位。** 帶有必要引數的欄位無法直接選取，因此會被排除在資料表的欄位及巢狀選擇之外。[tool-verified: `provisa/graphql_remote/mapper.py` `_build_columns`, `_build_gql_field_selection`]

**必要引數。** 當根查詢欄位帶有非空值、無預設值的引數時，這些引數會成為資料表上的 `native_filter_type: query_param` 欄位（於注入時加上 `_nf_` 前綴）。執行器會將其作為 GraphQL 變數傳遞。（REQ-555）[tool-verified: `provisa/graphql_remote/mapper.py:110–120`, `provisa/api/app.py:1280–1303`]

**自動偵測的關聯。** Provisa 會掃描每張已註冊資料表中 OBJECT 類型的欄位。當所參照的 GQL 類型同樣是同一來源中已註冊的資料表，且該關聯所依附的欄位屬於已註冊的欄位時，該關聯即被儲存。尚未註冊的資料表則不會獲得關聯。[tool-verified: `_graphql_table_registration.py` `sync_detected_relationships`]多對一關聯會依據命名慣例推斷來源欄位與目標欄位（來源類型上的 `breedName` → 目標類型 `Breed` 上的 `name`）。一對多（LIST）欄位會產生欄位參照為空的關聯——外鍵位於目標一側。（REQ-554）[tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutation。** mutation 欄位會被逐一註冊為 `source_operation` 種類的命令。其引數的類型均為 `json`，並作為類型化變數傳給遠端服務；回應即遠端服務傳回的 JSON，沒有 `return_schema`。參見[遠端來源的寫入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-1924） [tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation` (`body.returns = ""`, arguments typed `json`); `provisa/executor/source_operation.py` `_call_graphql`]

**刷新。** 向 `/admin/sources/graphql-remote/{id}/refresh` 發送 POST 請求。此操作會重新對遠端結構描述進行內省，並使已註冊的資料表與之保持一致。它不會新增任何資料表或欄位：結構描述新增的資料表或欄位仍處於可提供狀態，結構描述已刪去的欄位則會被移除。既有的治理規則（RLS、遮罩）將予以保留。（REQ-311）[tool-verified: `provisa/api/admin/graphql_remote_router.py` `refresh_graphql_remote_source`; `_graphql_table_registration.py` `refreshed_registered_tables`: "a column the schema has lost is gone; one it has gained is on offer and is not added"]

**限制。**

- 純量及 ENUM 類型的根查詢欄位（回傳類型非 OBJECT）會成為受追蹤的函式，而非虛擬資料表。其 `return_schema` 為單一欄位 `value`，類型為對應的純量類型。[tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- 物件巢狀結構於註冊時會解析至 `graphql_remote.max_object_depth`（預設值：5）。遠端擷取的選擇集與子欄位中繼資料均建構至此深度；超出限制的欄位不會被擷取，也無法用於 SQL 擷取。沿任一路徑，同一類型只會進入一次：類型已在向下路徑上的欄位會被排除，因此類型相互參照的結構描述每個類型只走訪一次，而不是每個深度層級走訪一次。（REQ-556）[tool-verified: `provisa/graphql_remote/mapper.py` `_build_gql_field_selection`, `tests/unit/test_graphql_remote_relay.py` `test_a_type_is_entered_once_along_a_path`]
- LIST 類型的巢狀 OBJECT 欄位（例如 `breed.awards: [Award]`）會被納入擷取選擇集，最多巢狀 `graphql_remote.max_list_depth` 層（預設值：2）。在此限制內，清單會作為父欄位上的 `jsonb` 陣列擷取。當清單欄位宣告了 `first` 引數（Relay、PostGraphile、pg_graphql）或 `limit` 引數（Hasura）時，選擇集會將其作為 `first: N` 或 `limit: N` 傳入，其中 N 為 `graphql_remote.max_list_items`（預設值：100）。兩者都未宣告的清單欄位不會獲得引數，因為遠端服務會拒絕欄位未宣告的引數。超出 `max_list_depth` 後，該 LIST 欄位將被完全排除，以防止資料無限膨脹。在 SQL 中，陣列可透過 `json_array_elements(column_name)` 或使用 `->>` 的索引擷取來存取。若清單的元素類型自帶根查詢，應改為將其註冊為獨立資料表並建立關聯——聯結路徑更有效率，且可繞過 blob。（REQ-556）[tool-verified: `provisa/graphql_remote/mapper.py` `_list_limit_arg`, `_build_gql_field_selection`; `tests/unit/test_graphql_remote_relay.py` `test_a_plain_list_takes_no_first_and_a_list_that_declares_first_gets_it`]
- 對於 SQL 查詢，未受治理的 OBJECT 類型欄位會從遠端來源完整擷取（所有子欄位至設定深度為止），並以 `jsonb` 形式快取。SQL 中對子欄位的存取是透過對 blob 進行 `->>` 擷取來處理；遠端請求不會限縮為 SQL 查詢所選取的欄位。當清單的元素類型沒有根查詢，且 blob 表示法不敷使用時，應直接以 GraphQL SDL 撰寫查詢——Provisa 會忠實地重現 GQL 欄位選擇，令遠端來源僅接收到確切請求的欄位。[tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- 若遠端伺服器因需要子欄位選擇而拒絕某個 OBJECT 類型欄位（在 `gql_selection` 可用時理應不會發生此情況），執行器會移除該等欄位後重試一次，以確保純量欄位仍可正常回傳。此規則適用於從根欄位讀取的資料表。連線資料表不走此路徑。[tool-verified: `provisa/graphql_remote/executor.py` `execute_remote` (`for attempt in range(2)`), `_execute_connection`]

**分頁讀取。** 連線資料表透過游標讀取。每一頁請求 `first: N, after: $pageCursor` 及 `pageInfo { hasNextPage endCursor }`，讀取會沿 `endCursor` 繼續，直至遠端服務回報沒有下一頁。（REQ-309）[tool-verified: `provisa/graphql_remote/executor.py` `_connection_query`, `_execute_connection`]

| 設定 | 預設值 | 作用 |
| --- | --- | --- |
| `graphql_remote.max_list_items` | `100` | 每頁列數。[tool-verified: `provisa/api/data/materialization.py` passes `limit=max_items` to `execute_remote`] |
| `graphql_remote.max_rows` | `10000` | 對一張連線資料表的一次讀取最多取得的列數。達到該值的讀取會停止並記錄一則警告。[tool-verified: `provisa/core/models.py` `GraphQLRemoteConfig`] |

```yaml
graphql_remote:
  max_list_items: 100
  max_rows: 10000
```

有兩種回應會使執行器重試：

- **頁面過重。** 當遠端服務回應 502 或 504 時，會以一半大小重新請求同一頁，最小降至一列。[tool-verified: `_PAGE_TOO_HEAVY = (502, 504)`, `page_size = max(1, page_size // 2)`]
- **附等待時間的速率限制。** 當遠端服務回應 403 或 429，且 `Retry-After` 不超過 120 秒時，執行器會等待相應時間後重新發送請求，最多三次。沒有 `Retry-After`，或要求更長等待時間的拒絕，會作為錯誤擲出。此規則適用於每一次讀取，無論是否為連線。[tool-verified: `_post`, `_RETRY_AFTER_STATUSES`, `_RETRY_AFTER_ATTEMPTS`, `_RETRY_AFTER_MAX_SECONDS`]

回應中的任何其他錯誤都會使讀取失敗，除非來源類型另有規定（見下文 GitHub）。父層回傳 null 的連線沒有任何列。[tool-verified: `_accept_row_field_errors`, `_execute_connection`]

---

### GitHub（REQ-1923）

GitHub 是一種普通的來源類型。其 API 為 GraphQL，因此其資料表的行為如上所述，包括 `gh__repository_issues` 這類連線資料表。[tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["github"]`]

**新增來源。**

1. 開啟 Sources，新增類型為 **GitHub** 的來源。
2. 輸入 GitHub 存取權杖。可選擇輸入命名空間，即資料表名稱的前綴；預設值為 `gh`。
3. 儲存。Provisa 會向 GitHub 驗證該權杖。被 GitHub 拒絕的權杖會使新增失敗，並附上 GitHub 的訊息。

新增來源不會註冊任何資料表。[tool-verified: `provisa/api/admin/graphql_remote_router.py` `_register_branded_source` (`"tables": 0`, `verify_query="query { viewer { login } }"`)]

**註冊資料表。** 開啟 Tables，再開啟 Register Table。選擇 GitHub 來源，選擇結構描述 `graphql`，然後選擇所需的資料表。GitHub 提供的每張資料表都會列出；註冊即由您決定公開哪些。[tool-verified: `provisa/api/admin/_graphql_table_registration.py` `offered_tables`][inferred: picker labels and the `graphql` schema name from the task brief; the UI strings were not read]

**權杖範圍（scope）。** 註冊資料表時，Provisa 會用您的權杖向 GitHub 驗證一次。

- 權杖範圍未涵蓋的欄位會被排除在資料表之外。結果會列出每個被排除的欄位：`Left out, because the source's credential may not read them: projectsV2`。[tool-verified: `provisa/api/admin/schema_mutation_ops.py`]
- 權杖完全無法讀取的資料表會被拒絕，並附上 GitHub 給出的原因：`GitHub does not let this source's credential read gh__repository_issues: ...`。[tool-verified: `provisa/api/admin/_table_ops.py` `_branded_columns_for_input`]

**權杖無權檢視的列。** 對於權杖無權在某一特定列中檢視的欄位（例如無推送權限的儲存庫的協作者），GitHub 回應 `FORBIDDEN`；對於僅存在於組織所有的儲存庫的欄位，則回應 `NOT_ORG_OWNED_REPO`。該欄位在該列中為 null，其餘讀取照常進行，Provisa 會記錄一則警告。針對資料表本身的錯誤會使讀取失敗。[tool-verified: `provisa/graphql_remote/brands.py` `error_policy`, `provisa/graphql_remote/executor.py` `_accept_row_field_errors`]

**過重的頁面。** 當 GitHub 因某一頁的計算成本過高而回應 `RESOURCE_LIMITS_EXCEEDED` 時，會以一半大小重新請求該頁。[tool-verified: `brands.py` `overload`, `executor.py` `_execute_connection`]

**巢狀物件。** GitHub 資料表使用自身的巢狀深度 0（此來源類型的 `max_object_depth=0`），而不是 `graphql_remote.max_object_depth`。巢狀物件欄位僅以其自身的純量欄位選取；其內部的物件顯示為 `__typename`。[tool-verified: `brands.py`]

**權杖儲存。** 權杖存入密鑰庫，來源列保留一個參照，因此重新啟動後會重新讀取該來源，無需重新輸入權杖。[tool-verified: `provisa/api/admin/graphql_remote_router.py` `_persist_source` docstring: "The credential goes to the org's vault and the row carries the reference"]

**運作方式（面向維運人員）。** GitHub 的結構描述隨 Provisa 一同發布，因此新增來源不會發起內省呼叫，大型結構描述在註冊時也不產生任何成本。資料表在註冊時才從中逐個對應。刷新端點會拒絕此來源類型；新的 GitHub 結構描述會隨 Provisa 版本發布而到來。[tool-verified: `brands.py` module docstring, `brand_schema`; REQ-1923 "there is no refresh" in `docs/arch/requirements.yaml` REQ-1875 supersession note][tool-verified: refresh handler returns code `graphql_remote.branded_source_not_refreshed`]

---

### GitLab（REQ-1923）

GitLab 是一種普通的來源類型，其新增與註冊方式與 GitHub 相同：新增類型為 **GitLab** 的來源並提供存取權杖，然後從結構描述 `graphql` 中註冊所需的資料表。資料表名稱的預設前綴為 `gl`。該來源連線 `gitlab.com`。[tool-verified: `provisa/graphql_remote/brands.py` `BRANDS["gitlab"]`]

**選擇欄位。** GitLab 會為每個查詢定價，並拒絕成本過高的查詢：匿名呼叫端為 200 點，帶權杖為 250 點。選取全部欄位的寬資料表會超出此價格，因此請註冊僅含所需欄位的 GitLab 資料表。[tool-verified: live against gitlab.com 2026-10-02, `project.issues` with all 63 columns answered "Query has complexity of 1733, which exceeds max complexity of 200"; with 14 chosen columns it registered and read]

- 註冊資料表時，Provisa 會向 GitLab 詢問一次，看其是否會在讀取所用的頁面大小下處理該選擇。如果 GitLab 回應查詢過於複雜或過大，該資料表不會被註冊，結果會附上 GitLab 的訊息：`Table 'gl__project_issues' was not registered with the columns selected: Query has complexity of 1733, which exceeds max complexity of 200. Choose fewer columns.`[tool-verified: `provisa/graphql_remote/probe.py` `QueryTooComplex`; `provisa/api/admin/_table_ops.py` code `schema.table_too_complex`]
- 一個欄位的成本取決於其種類。一般值約為一點；巢狀物件欄位的成本是其許多倍。捨棄巢狀物件欄位最能節省成本。[tool-verified: live, five scalar columns scored 26 at 100 rows a page; two small object columns added 18]
- 頁面大小是價格的一部分。它即 `graphql_remote.max_list_items`。[tool-verified: live, the same five columns scored 15 at 5 rows a page and 26 at 100]

**權杖驗證。** GitLab 對無法辨識的權杖回應空結果，而不是錯誤。Provisa 將其視為被拒絕的權杖，不會新增該來源。[tool-verified: `provisa/api/admin/graphql_remote_router.py` `_verify_live_auth`]

---

### gRPC 遠端結構描述（REQ-322–329）

**如何新增來源。** 向 `/admin/grpc-remote/register` 發送 POST 請求，附上伺服器位址、`.proto` 檔案的路徑或 URL，以及可選的 TLS 設定。新增來源不會註冊任何資料表。

```json
{
  "source_id": "orders-grpc",
  "proto_path": "https://api.example.com/orders.proto",
  "server_address": "grpc.example.com:443",
  "namespace": "orders",
  "domain_id": "commerce",
  "tls": true,
  "cache_ttl": 300,
  "method_overrides": { "CreateOrder": "query" },
  "relationships": [
    { "source_table": "orders__OrderService__ListOrders", "source_column": "customer_id",
      "target_table": "customers__CustomerService__GetCustomer", "target_column": "id" }
  ]
}
```

Provisa 會擷取 proto 檔案，以純文字解析器（解析時不依賴任何外部 proto 依賴項）進行解析，透過 `grpc_tools.protoc` 編譯 Python stub，並開啟一個持續存在的 `grpc.aio.Channel`。（REQ-322）[tool-verified: `provisa/grpc_remote/loader.py:99–128`, `provisa/grpc_remote/loader.py:166–214`, `provisa/api/admin/grpc_remote_router.py:80–104`]

Proto 檔案亦可為本機路徑。常見類型（`google/protobuf/timestamp.proto`）的匯入路徑會於註冊時儲存，並於刷新時重複使用。（REQ-329）[tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**來源提供的內容。** Proto 中的每個 `rpc` 方法均會依優先順序使用三項訊號分類為 query 或 mutation：（REQ-323）[tool-verified: `provisa/grpc_remote/mapper.py`]

1. **註冊酬載中的 `method_overrides`**——`{"MethodName": "query"}` 或 `{"MethodName": "mutation"}` 優先於其他一切。
2. **`server_streaming: true`**——伺服器發送訊息串流；恆為虛擬資料表（除非輸出為純量）。
3. **輸出訊息帶有重複的訊息類型欄位**——例如 `ListOrdersResponse { repeated Order items; }` 會被視為清單包裝並成為虛擬資料表。重複的純量欄位（例如 `repeated string tags`）不會觸發此規則——它們是單一實體的陣列屬性，並非資料列來源。

不符合以上任何訊號的方法（回傳單一實體訊息的一元 RPC，或任何純量輸出）會成為受追蹤的函式。

**註冊資料表。** 每個 query 方法都會作為一張資料表提供，名稱為 `{namespace}__{Service}__{Method}`，位於選擇器結構描述 `grpc_remote` 之下。透過 Register Table 選擇器註冊所需的資料表（`availableTables`、`availableColumns`、`registerTable`，與 GraphQL 來源相同），並選擇回應欄位。請求欄位會成為 `_nf_*` 原生篩選欄位，且始終包含在內。（REQ-322）[tool-verified: `provisa/api/admin/introspect.py` `_native_tables_grpc` (`if schema_name != "grpc_remote": return []`), `provisa/api/admin/grpc_remote_router.py` `query_table_name`, `query_columns`, `_register_schema` (`if table_name not in registered: continue`), `provisa/api/admin/_table_ops.py` `_grpc_columns_for_input`]

Mutation 方法是可提供的命令，計入 `available_mutations`；新增來源不會記錄其中任何一個。gRPC mutation 在「命令」頁面註冊：先選擇來源，再選擇方法，方法名為 `Service.Method`；該命令的種類為 `source_operation`。參見[遠端來源的寫入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-1924） [tool-verified: `provisa/executor/source_operation.py` `grpc_operation_name`, `_grpc_operations`; `provisa/api/admin/actions_router.py` `_as_source_operation`]

**資料表命名。** 預設名稱為 `{namespace}__{ServiceName}__{MethodName}`。若無命名空間，服務名稱與方法名稱會直接連接。任何已註冊的資料表均可指定 `alias`；一旦設定，該別名將於各處使用（查詢、SDL、關聯）。自動生成的名稱為註冊索引鍵，永遠不會改變。（REQ-322）[tool-verified: `provisa/core/repositories/table.py:129–134`]

**類型對應（REQ-324）。** Proto 純量類型與 SQL 類型的對應如下。[tool-verified: `provisa/grpc_remote/mapper.py:31–47`]

| Proto 類型 | SQL 類型 |
| --- | --- |
| `string`、`bytes` | `text` |
| `int32` / `uint32` / `sint32` / `fixed32` / `sfixed32` | `integer` |
| `int64` / `uint64` / `sint64` / `fixed64` / `sfixed64` | `bigint` |
| `float` | `real` |
| `double` | `numeric` |
| `bool` | `boolean` |
| `repeated <T>` | `jsonb` |
| 巢狀訊息 | `jsonb` |
| Enum | `text` |

**註冊時的關聯。** `relationships` 的運作方式與 GQL 轉接器相同——宣告外部索引鍵/主索引鍵連接路徑，並儲存為手動宣告的關聯（沒有 `remote_managed` 旗標）。刷新時，這些關聯會維持不變。（REQ-554）[tool-verified: `provisa/api/admin/grpc_remote_router.py:93–109`]

**Query 方法（REQ-325）。** 輸出訊息的欄位會成為資料表欄位。輸入訊息的欄位既會成為傳遞至遠端呼叫的 GraphQL 引數，*同時*亦會註冊為以 `_nf_` 為前綴、`native_filter_type: "grpc_input"` 的欄位——此機制與 GQL 及 OpenAPI 用於原生篩選器注入的機制相同。（REQ-555）[tool-verified: `provisa/api/admin/grpc_remote_router.py:207–213`]

**巢狀訊息的子欄位。** 對於 query 方法，深度 0（直接輸出欄）的非重複訊息類型欄位，其子欄位會解析多一層並儲存為 `ColumnDef` 上的 `object_fields`。此中繼資料用於 SQL 中的 `jsonb` 子欄位擷取及結構描述文件。超出深度 1 的巢狀欄位不會遞迴展開。（REQ-556）[tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

伺服器串流方法會先將所有串流訊息收集成清單，再回傳資料列。（REQ-325）[tool-verified: `provisa/grpc_remote/executor.py:86–119`]

**Mutation 方法（REQ-326）。** 已註冊的 mutation 方法是一個命令，其引數即輸入訊息的欄位，類型均為 `json`，並原樣傳遞。遠端服務的回應以列的形式傳回；被拒絕的呼叫為 422，`functions.remote_refused`。參見[遠端來源的寫入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-1924） [tool-verified: `provisa/executor/source_operation.py` `_grpc_operations`, `_call_grpc`, `_refused`]

**頻道管理。** 每個已註冊來源會有一個 `grpc.aio.Channel`，儲存於應用程式狀態中並於後續請求重複使用。刷新時，舊頻道會在新頻道開啟前關閉。（REQ-327）[tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**刷新。** 向 `/admin/grpc-remote/refresh/{source_id}` 發送 POST 請求。此操作會從已儲存的路徑重新載入 proto，重新編譯存根（stub），並使已註冊的資料表與 proto 保持一致，沿用各資料表註冊時所用的欄位。它不會註冊任何新資料表；新增到 proto 的 query 方法仍處於可提供狀態。也可向 `/admin/grpc-remote/{source_id}/proto` 發送 PUT 請求並附上新的 `proto_text`，以內嵌方式更新 proto。（REQ-329）[tool-verified: `provisa/api/admin/grpc_remote_router.py` `refresh_grpc_remote_source`, `_load_and_register` and `put_grpc_proto` (both pass `registered=await registered_query_tables(conn, source_id)`)]

**限制。**

- 物件子欄位擷取僅支援一層深度。超出深度 1 的巢狀訊息欄位不會遞迴展開。（REQ-556）[tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST（REQ-314–321）

**如何新增來源。** 向 `/admin/openapi/register` 發送 POST 請求，附上來源 ID 及規格（從本機檔案或 URL 載入）。規格會被解析並隨來源保存；不會註冊任何資料表或 command。回應回傳 `tables: 0` 與 `mutations: 0`，可提供的數量則在 `available_tables` 與 `available_mutations` 中。（REQ-314）[tool-verified: `provisa/openapi/loader.py:30–55`, `provisa/api/admin/openapi_router.py` `_load_and_register` docstring: "Tables and functions are NOT auto-registered here. Users register them individually via the Register Table / Register Action UI."]

**註冊資料表。** 透過 Register Table 選擇器註冊所需的每個 GET 操作（`availableTables`、`availableColumns`、`registerTable`），並選擇欄位。每個非 GET 操作則在「命令」頁面逐一註冊為命令，由 `availableFunctions` 列出；參見[遠端來源的寫入操作](commands.md#a-remote-sources-write-operation-req-1924)。`PUT /admin/openapi/spec/{source_id}` 會儲存手動編輯的規格，不註冊任何內容，並回傳 `available_tables` 與 `available_mutations`。（REQ-316） [tool-verified: `provisa/api/admin/openapi_router.py` `put_openapi_spec`; `provisa/api/admin/schema_query.py` `available_functions` ("returns non-GET operations")] [tool-verified: `provisa/api/admin/_table_ops.py` `_build_columns_for_input`; a registered OpenAPI table is read through the operation in the stored spec, `provisa/api/data/materialization.py` (`state.openapi_specs`)]

**註冊酬載。** `/admin/openapi/register` 端點除了 `source_id`、`spec_path` 等欄位外，還接受兩個額外欄位：

```json
{
  "operation_overrides": { "createPet": "query", "listOrders": "mutation" },
  "relationships": [
    { "source_table": "pets__listPets", "source_column": "owner_id",
      "target_table": "owners__listOwners", "target_column": "id" }
  ]
}
```

**來源提供的內容。** 規格中的每個 GET 操作都會作為資料表提供，除非其回應結構描述是純量類型（`string`、`number`、`boolean`、`integer`）——傳回純量的 GET 操作則是只有單一 `value` 欄的函式。每個非 GET 操作（POST、PUT、PATCH、DELETE）都會作為命令提供，以其 `operationId` 命名。註冊後，它接受該操作的路徑參數，以及用於請求本文的 `body` 引數，類型均為 `json`；其餘任何引數都放在查詢字串中。參見[遠端來源的寫入操作](commands.md#a-remote-sources-write-operation-req-1924)。（REQ-316、REQ-317、REQ-1924） [tool-verified: `provisa/executor/source_operation.py` `_openapi_operations`, `_call_openapi`]

分類優先順序：`operation_overrides`（酬載）優先於 `x-provisa-kind`（規格擴充），而 `x-provisa-kind` 又優先於 GET 啟發式規則。`operation_overrides` 為建議的覆寫途徑；`x-provisa-kind` 則適用於須由規格本身承載分類資訊的情況。（REQ-408）[tool-verified: `provisa/openapi/mapper.py:192–203`]

**註冊時的關聯。** `relationships` 的運作方式與其他轉接器相同——儲存為手動宣告的關聯，並於刷新時予以保留。（REQ-554）[tool-verified: `provisa/api/admin/openapi_router.py:103–108`]

**資料表命名。** 資料表使用操作的 `operationId`。若未定義 `operationId`，Provisa 會將 `{method}_{path}` 轉為 slug。別名的推導方式為移除開頭的動詞片段並將名詞轉為單數（`findPetsByStatus` → `pet_by_status`）。（REQ-557）[tool-verified: `provisa/openapi/register.py:39–56`]

**類型對應。** JSON Schema 類型與 Provisa 類型的對應如下。[tool-verified: `provisa/openapi/register.py:59–70`]

| JSON Schema 類型 | Provisa 類型 |
| --- | --- |
| `string` | `string` |
| `integer` | `integer` |
| `number` | `number` |
| `boolean` | `boolean` |
| `array` | `jsonb` |
| `object` | `jsonb` |

**作為原生篩選器欄位的參數。** 尚未屬於回應欄位的路徑及查詢參數，會成為 `native_filter_type` 設為 `path_param` 或 `query_param`、並以 `_nf_` 為前綴的欄位。當參數名稱與回應欄位名稱相符時，該參數的中繼資料會併入既有的欄位項目，而非另建重複項目。（REQ-555）[tool-verified: `provisa/openapi/register.py:116–122`, `provisa/openapi/register.py:172–196`]

**回應結構描述的解析。** 映射器會依序檢查 `responses.200`、`responses.2xx`，再檢查 `responses.default`。陣列類型的回應會展開至其元素結構描述。`$ref` 參照會解析至一層深度。（REQ-316）[tool-verified: `provisa/openapi/mapper.py:83–101`]

**物件子欄位。** 帶有 `type: object` 且自身具有 `properties` 的回應屬性，會儲存為該欄位上的 `object_fields`。這些子欄位於 SDL 中可見，並用於查詢中的 `jsonb` 擷取。（REQ-556）[tool-verified: `provisa/openapi/register.py:87–96`]

**回應快取（REQ-318）。** GET 操作的結果會由 `pg_cache.py` 快取於 PostgreSQL 中。每種請求參數組合均擁有其專屬的 `_params_hash` 群組。當 TTL 到期時，特定雜湊值的資料列會被取代。帶路徑參數的端點（`/pets/{id}`）會略過初始批量擷取——快取資料表會先建立為空以供結構描述內省之用，再依主索引鍵於請求到達時逐步填入。[tool-verified: `provisa/openapi/pg_cache.py:181–234`, `provisa/openapi/pg_cache.py:307–360`]

**刷新（REQ-321）。** 向 `/admin/openapi/refresh/{source_id}` 發送 POST 請求。透過 `_load_and_register` 重新解析規格，該函式不註冊任何內容：它不會新增任何資料表或欄位。既有的治理規則將予以保留。[tool-verified: `provisa/api/admin/openapi_router.py` `refresh_openapi_source`, `_load_and_register`]已註冊的表保留其欄位；它透過刷新後 spec 中的操作讀取。 [tool-verified: `provisa/api/admin/openapi_router.py` `_load_and_register` (replaces `state.openapi_specs[source_id]`), `provisa/api/data/materialization.py`]

**限制。**

- 物件子欄位擷取僅支援一層深度。`object_fields` 中巢狀的屬性不會遞迴展開。（REQ-556）[tool-verified: `provisa/openapi/register.py:87–96`]
- 標頭及 Cookie 參數會被忽略；只有 `path` 及 `query` 參數會被註冊。（REQ-555）[tool-verified: `provisa/openapi/mapper.py:144–158`]
- 規格層級的 `$ref` 解析對於屬性結構描述僅支援一層深度；深層巢狀的元件參照可能無法解析。[tool-verified: `provisa/openapi/mapper.py:51–60`]

---

## 註冊遠端資料表的影響

從任何遠端結構描述來源註冊的資料表，均為一級的 Provisa 資料表。在執行階段，它與本機連接的關聯式資料表在待遇上並無任何分別。（REQ-308、REQ-313）

**查詢介面。** 該資料表可立即透過 GraphQL、SQL（pgwire 或直接連線）、Cypher（GQL）、JSON:API 及 Arrow Flight 進行查詢。（REQ-001、REQ-267、REQ-345、REQ-257、REQ-051）由於遠端資料表沒有目錄，結構描述生成過程會為其合成 `ColumnMetadata`——類型對應是於結構描述建構時套用的。（REQ-602）[tool-verified: `provisa/api/app.py:1367–1386`]

**安全模型。** 所有五層治理規則均適用：

1. 網域存取控制——資料表的 `domain_id` 決定哪些角色可以查看它。（REQ-039）[tool-verified: `provisa/compiler/schema_gen.py:1064–1076`]
2. 行級安全（RLS）——不論介面為何，資料表上設定的列篩選器均會注入每項查詢中。（REQ-040、REQ-041）
3. 欄位可見性——每個欄位的 `visible_to` 清單控制按角色而定的欄位曝露。（REQ-039）
4. 欄位遮罩——遮罩規則於治理流程的第二階段套用。（REQ-040、REQ-263）
5. 述詞防護——已遮罩的欄位會於 WHERE 及 HAVING 子句中被拒絕。（REQ-603）

針對遠端資料表的即席查詢僅依使用者本身的權限予以允許——存取方式統一以權限為基礎（資料表/欄位權限加上已核准的關聯），並無按資料表而異的治理模式。（REQ-001、REQ-003）

**關聯治理（V002）。** 針對遠端資料表的 JOIN 條件——當透過 SQL 或 Cypher 查詢時——必須符合一項已註冊並已核准的關聯。（REQ-604）由於 SDL 定義的關聯依設計已預先核准，GraphQL 查詢會略過 V002 檢查。詳見 [docs/security.md](security.md#relationship-governance-v002)。

**OBJECT 類型欄位。** 當欄位對應至未受治理的內嵌 GQL OBJECT 或 OpenAPI 物件類型時，其 Provisa 類型為 `jsonb`。該欄位會儲存完整的巢狀 JSON blob。當宣告了子欄位（`gql_object_fields` 或 `object_fields`）時，`gql_object_columns` 對應表會於結構描述建構時填入。當查詢選取這些子欄位時，SQL 生成器會使用此對應表發出 `->>` 擷取運算式。[tool-verified: `provisa/api/app.py:1305–1315`, `provisa/compiler/schema_gen.py:80–82`]

**作為原生篩選器參數的必要引數。** 帶有非空值、無預設值引數的根查詢欄位，會為已註冊資料表注入額外欄位。這些欄位帶有 `native_filter_type: query_param`。Cypher 轉譯器會將 `WHERE n.id = $val` 重寫為 `WHERE n._nf_id = $val`，而 GraphQL 執行器則會將其識別為要傳遞至遠端端點的變數。（REQ-555）[tool-verified: `provisa/api/app.py:1280–1303`]

---

## 建立覆蓋性關聯的影響

當 data steward 於兩個遠端資料表之間（或於一個遠端資料表與一個本機資料表之間）註冊一項關聯時，該關聯即成為查詢時所使用的聯結路徑。

**聯結如何取得優先。** 於查詢編譯階段，Provisa 會透過已註冊的關聯解析聯結路徑。該關聯的 `source_column` 及 `target_column` 會成為生成 SQL 中的聯結條件。聯結會取代原本針對已連接類型所需的、按資料表逐一發出的遠端呼叫。

**原始 blob 永遠不會於 SQL 中曝露。** `petstore__pets` 上的 `breed` 欄位無法於 SQL 查詢中作為原始 jsonb 值選取。當 `petstore__pets` 與 `petstore__breeds` 之間已註冊一項關聯時，SQL 查詢會經由聯結解析——`SELECT breed.name FROM petstore__pets` 是透過外部索引鍵聯結解析，而非透過 blob。若未註冊任何關聯，但該欄位帶有已宣告的子欄位（`gql_object_fields`），則 SQL 中對子欄位的參照會被重寫為對已儲存 blob 的 `->>` 擷取。此路徑僅適用於未受治理的內嵌類型——受治理目標類型的欄位完全從 SDL 中排除，並無 blob 可供擷取。原始 blob 本身永遠不會以裸欄位值的形式輸出。[tool-verified: `provisa/compiler/sql_gen.py:1156`, `tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

於 GraphQL SDL 中，未受治理的內嵌 OBJECT 欄位會被定型為該巢狀物件類型。至於它究竟是於執行階段透過聯結或透過 blob 擷取來提供服務，屬於實作細節——兩種情況下的 SDL 形狀均相同。當子類型被註冊為其獨立資料表（因而成為受治理類型）時，五層治理規則會獨立套用於其上：其自身的 RLS 規則、欄位可見性、遮罩規則、述詞防護及網域存取控制。（REQ-039、REQ-040、REQ-041、REQ-263）Blob 擷取則會繞過此機制——子項資料會以預先內嵌的形式隨父資料列一併到達，並僅受父資料表規則的治理。將子項註冊為資料表並建立關聯,是對子類型實現精細治理的途徑。

**關聯上的 `graphql_alias`。** `graphql_alias` 欄位會為關聯於父類型上曝露的 SDL 欄位命名。若缺省，其名稱會依目標資料表的 `field_name` 及該關聯的基數,透過 `rel_field_name(target.field_name, cardinality)` 推導而來。（REQ-605）[tool-verified: `provisa/compiler/schema_gen.py:1050`]

**聯結路徑上的 V002。** 凡經由 SQL 及 Cypher 遍歷該關聯的查詢,均須受 V002 關聯治理規範。該關聯必須已註冊並獲核准,方可允許進行聯結。（REQ-604）透過 SDL 關聯欄位進行的 GraphQL 遍歷則恆為預先核准。[tool-verified: `docs/security.md:41–54`]

**remote-managed 旗標。** 於 GraphQL 遠端結構描述註冊期間自動偵測的關聯,會以 `remote_managed: True` 儲存。（REQ-554）[tool-verified: `provisa/graphql_remote/mapper.py:199`] 這是一個中繼資料標記,並不會改變治理行為。

---

## 僅供類型定義的行為

並非遠端結構描述中的每種類型都必須成為可查詢的資料表。

當 `SchemaInput` 上設定了 `root_table_ids` 時,ID 不在該集合中的資料表會從生成 SDL 的根查詢欄位中排除。它們仍會以 GraphQL 類型的形式存在,並可透過具有根項目的資料表上的關聯欄位加以存取。（REQ-601）[tool-verified: `provisa/compiler/schema_gen.py:1062–1069`]

相同機制亦適用於按網域篩選的結構描述建構:位於角色無法存取的網域中的資料表,僅屬類型定義——其類型定義存在於 SDL 中以供關聯遍歷之用,但不會為其生成任何根查詢欄位。（REQ-039）[tool-verified: `provisa/compiler/schema_gen.py:1068–1076`]

僅供類型定義的資料表具備以下特性:

- 沒有根查詢欄位——用戶端無法直接按名稱查詢它。
- 可透過具有根項目的資料表上的關聯欄位加以存取。
- 仍會於結構描述內省中以具名類型的形式出現。
- 當透過關聯存取資料時,仍會套用所有治理規則。（REQ-039、REQ-040）

只有在資料表的註冊被完全刪除時,才會從結構描述中完全移除——包括其類型定義。將資料表標記為僅供類型定義（透過從 `root_table_ids` 中移除其 ID,或按網域存取權進行篩選）並不會移除該類型。

此設計讓 data steward 能夠公開可導覽的物件圖,其中部分類型僅可透過遍歷存取,而非獨立查詢。
