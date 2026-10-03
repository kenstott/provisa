# 命令

命令是一個已註冊、受治理的函式，把外部運算納入 Provisa 的治理、審計與血緣體系。聯邦引擎原生處理 SQL，而命令則是它表達不了的那些運算的接縫：一個資料增益微服務、一個 Python 模型、一段 shell 指令碼、一個資料庫原生預存程序。註冊一次，每個用戶端介面——GraphQL、pgwire SQL、REST、Arrow Flight、gRPC、Bolt/Cypher——都能以完全相同的治理去呼叫它（REQ-885、REQ-1156）。[tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

關鍵分野在於：命令是**受治理的 RPC**，不是臨時拼湊的 ETL。它的輸入與輸出都經過宣告、定型、驗證、追蹤，並接進血緣。未受治理的 curl 呼叫或子行程，一樣都不是。

## 實作種類

支援六個 `impl_kind` 值 [tool-verified: `_EXECUTORS` dict in `provisa/executor/function_dispatch.py`]：

| `impl_kind` | 傳輸方式 |
| --- | --- |
| `source_procedure` | 已註冊數據來源上的原生預存程序 |
| `source_operation` | OpenAPI、遠端 GraphQL 或 gRPC 來源的寫入操作，原樣傳遞（見 [遠端來源的寫入操作](#a-remote-sources-write-operation-req-1924)） |
| `script` | 本機子行程，由 stdin 餵入 JSON，自 stdout 讀取 JSON |
| `http` | HTTP/S 端點；JSON 請求主體，JSON 回應 |
| `grpc` | gRPC 一元呼叫；免 proto 的 JSON 橋接 |
| `python` | 行程內的 Python 可呼叫物件（`module:attr`） |

定址（目錄中的 `name` 與 `function_name`）與 `binding`（傳輸方式與位置）是解耦的。換掉 binding，命令的治理、血緣與呼叫方合約都維持不變。[tool-verified: Function model in models.py:710-750]

## 引數種類

每個引數都要宣告一個 `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]：

| `arg_kind` | 行為 |
| --- | --- |
| `column_value` | 純量；直接放進請求負載傳遞 |
| `table_ref` | 延遲式；Provisa 原樣傳遞關聯引用，由服務自行取數 |
| `result_set` | 積極式；Provisa 具體化被引用的關聯並送出其資料行 |

`http` 與 `grpc` 命令**必須**至少宣告一個 `table_ref` 或 `result_set` 引數。只收到純量引數的外部命令會被逐行呼叫一次，那就破壞了批次處理。派送器會在呼叫時拒絕這種設定（422）。[tool-verified: `_reject_rowwise_external` in function_dispatch.py:322-344]

會傳回集合的命令（經 `output_columns` 與 `return_schema` 宣告）是一個表值函式。可用於 `FROM` 子句或 `JOIN`。[inferred from models.py:744-748 and command_localize.py:52-63]

## 數據集合約（REQ-1159）

每個 `table_ref` 或 `result_set` 引數都可宣告一份**輸入欄位合約**：`FunctionArgument.columns` 中一份有序、以 IR 定型的欄位清單。命令本身則在 `Function.output_columns` 中宣告一份**輸出欄位合約**。[tool-verified: DatasetColumn model in models.py:675-683, Function.output_columns in models.py:748]

兩份合約在每次呼叫時都以失敗即報錯的方式驗證：

- **輸入（僅限 result_set）：** 具體化之後，Provisa 會依所宣告的欄位驗證資料行。多出的欄位、缺少的欄位與類型不符，一律引發 HTTP 422。
  [tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **輸出：** 命令傳回的資料行在抵達呼叫方之前，會先依 `output_columns` 驗證。[tool-verified: function_dispatch.py:488-490]
- **窄投影：** 宣告了輸入合約之後，具體化查詢只會投影**那些欄位**（`SELECT "id", "region" FROM ...`），而不是 `SELECT *`。
  [tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed
  to projection at line 171]

### IR 類型詞彙

合約欄位的類型使用標準的 IR 類型系統（REQ-846），而非 GraphQL 純量或數據來源原生的寫法。有效的名稱為 [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]：

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

常見別名會自動解析（`varchar` → `text`、`int4` → `integer`、`jsonb` → `json` 等）。[tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` 是 `output_columns` 的 **GraphQL 投影**，而非事實來源。請為驗證與血緣宣告 `output_columns`；再加上 `return_schema` 供 GraphQL 類型生成之用。[tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

## 撰寫一個命令

### 設定檔

```yaml
functions:
  - name: enrich_orders
    description: Enrich orders inline — deterministic score + region label
    domain_id: sales-analytics
    kind: query
    impl_kind: python
    source_id: ""
    function_name: enrich_orders
    returns: ""
    binding:
      callable: demo.py_functions:enrich_orders
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}   # narrow input contract
          - {name: region, type: text}
    visible_to: [admin]
    output_columns:
      - {name: id, type: integer}
      - {name: score, type: double}
      - {name: region_label, type: text}
    return_schema:
      type: array
      items:
        type: object
        properties:
          id: {type: integer}
          score: {type: number}
          region_label: {type: string}
```

[tool-verified: sample_config.yaml enrich_orders block]

gRPC 變體（`enrich_grpc_set`）沿用同一套寫法，只是指定 `impl_kind: grpc`，並在 `binding` 中以 `target` 與 `method` 索引鍵取代 `callable`：

```yaml
  - name: enrich_grpc_set
    impl_kind: grpc
    binding:
      target: ${env:DEMO_GRPC_TARGET:-localhost:50071}
      method: /provisa.demo.Enrich/EnrichRows
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}
          - {name: region, type: text}
    output_columns:
      - {name: id, type: integer}
      - {name: embedding, type: text}
      - {name: geo, type: text}
```

[tool-verified: config/provisa.yaml enrich_grpc_set block]

### 管理 UI

**設定 → 命令** 中的命令表單包含一個逐數據集的輸入欄位編輯器（每個已宣告欄位一行，附 IR 類型選擇器）與一個輸出欄位編輯器。儲存表單即可註冊或更新命令，毋須重新載入設定。[inferred from CommandFormFields.tsx]

## 內嵌組合（REQ-1159）

命令可以出現在較大的 SQL 陳述式**之內**——被聯結、被用作子查詢，或被投影。你並不限於 `SELECT * FROM fn(args)`。例外是遠端來源的寫入操作，它只能單獨呼叫（見 [為何不能被組合](#why-it-cannot-be-composed)）。

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

在治理、驗證或路由執行之前，管線會偵測出已註冊的命令呼叫，讓每一個都經由共用的受治理執行器執行（因此 I/O 合約與身分模型的套用方式，與直接呼叫時完全一致），再把呼叫點改寫為一個已定型的本機關聯。
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in
command_localize.py:178-222]

替換方式會依大小自適應：一千行以內，結果會以已定型的 `VALUES` 清單內嵌；超過該門檻，則在引擎中註冊為一個具名的本機關聯。
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

在地化之後的陳述式照常路由。單一數據來源的查詢留在該數據來源上；只有真正跨數據來源的查詢才進聯邦引擎。[tool-verified: _pipeline.py:304 comment
"REQ-1159: a localized statement carries an inline local relation..."]

## 遠端來源的寫入操作（REQ-1924） {: #a-remote-sources-write-operation-req-1924 }

OpenAPI、遠端 GraphQL 或 gRPC 來源會提供寫入操作。將其中一個註冊為命令後，便可從所有介面呼叫，並受治理、被稽核。新增來源不會註冊其中任何一個；你要逐一註冊所需的操作，方式與註冊資料表相同。註冊即為策展。 [tool-verified: `provisa/executor/source_operation.py` module docstring; `provisa/api/admin/schema_common.py` `remote_source_counts`, `"mutations": 0`]

來源提供的內容 [tool-verified: `provisa/executor/source_operation.py` `offered_operations`]：

| 來源類型 | 提供的操作 | 操作名稱 |
| --- | --- | --- |
| `openapi` | 規格中的每個非 GET 操作 | `operationId` |
| `graphql_remote` | 遠端 `Mutation` 類型的每個欄位 | 欄位名稱 |
| `grpc_remote` | 每個被歸類為 mutation 的方法 | `Service.Method` |

### 註冊一個 {: #register-one }

1. 開啟 **模型 → 命令** 並新增一個命令。
2. 選擇遠端來源。表單會切換為操作選擇器，列出該來源所提供的操作。
3. 選擇操作、一個網域，以及可以呼叫它的角色。
4. 可選：開啟 **需要審批**，並填寫 **寫入表** 欄位。

[tool-verified: `provisa-ui/src/components/navGroups.ts` (`/commands` in the Model group); `provisa-ui/src/pages/commands/CommandFormFields.tsx` `isSourceOperation`, `command-requires-approval-switch`, `writesTable`; `provisa/api/admin/schema_query.py` `available_functions` ("Listing them registers none")]

透過管理 GraphQL API，`availableFunctions(sourceId, schemaName)` 會列出這些操作。結構描述名稱依來源類型而定，為 `openapi`、`graphql` 或 `grpc_remote`。 [tool-verified: `OPERATION_SCHEMA` in `source_operation.py`; `available_functions` returns `[]` when the schema name does not match the source type]

其餘一切均由操作決定，而非由表單所傳送的內容決定。`_as_source_operation` 會覆寫以下欄位：

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

[tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation`]

每個引數的類型均為 `json`。操作的引數即其 OpenAPI 路徑參數（操作帶請求本文時另加 `body`）、其 GraphQL mutation 引數，或其 gRPC 請求欄位。 [tool-verified: `_openapi_operations`, `_graphql_operations`, `_grpc_operations` in `source_operation.py`] 來源未提供的操作會被拒絕，傳回 422，`functions.operation_not_offered`。 [tool-verified: `offered_operation`]

### 呼叫 {: #calling-it }

Provisa 不會對輸入進行整形、類型化或檢查。每個引數都原樣連同來源的憑證送往遠端服務，遠端服務的回應也原樣傳回。Provisa 負責治理誰可以呼叫、在哪個網域中呼叫、是否需要批准，並記錄該呼叫。 [tool-verified: `source_operation.py` module docstring]

遠端服務如何接收引數：

- **OpenAPI.** 路徑參數填入路徑範本。`body` 是 JSON 請求本文。其餘每個引數都放在查詢字串中。 [tool-verified: `_call_openapi`]
- **GraphQL.** 引數以類型化變數傳送，每個變數均按遠端結構描述給定的類型宣告。mutation 會回取回應中的純量欄位和列舉欄位，以及其內部物件的這類欄位，最多深入兩層。 [tool-verified: `mutation_document`, `_selection`, `_ANSWER_DEPTH = 2`]
- **gRPC.** 這些引數成為 `Service.Method` 的請求訊息。 [tool-verified: `_call_grpc`]

回應即命令的列：一個物件是一列，物件清單即其各列，其他任何內容都是一列 `{"result": ...}`。 [tool-verified: `_rows`]

在 GraphQL 中，命令是一個 mutation 欄位，其回應是 JSON 純量。 [tool-verified: `provisa/compiler/actions_schema.py` (`gql_return = JSONScalar` for `source_operation`; `kind` defaults to `"mutation"`)]

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

[inferred: argument names are those of the remote operation; the example call was not run]

在 SQL 介面（pgwire 及其他傳遞 SQL 的介面）上，將每個引數寫成字串中的 JSON 字面值。`'{"title": "x"}'` 是物件，`'"text"'` 是字串，`'3'` 是數字。不是有效 JSON 的字面值會失敗，傳回 422，`functions.json_argument_invalid`。 [tool-verified: `_json_arguments_from_sql` in `function_dispatch.py`]

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

[inferred: the first-argument shape follows `_json_arguments_from_sql`; the statement was not run, and the command's argument list is the operation's]

在 REST 上，向 `/data/rest/{domain}/commands/{command}` 以 POST 傳送由引數組成的 JSON 物件。`json` 引數在產生的規格中被記載為任意值。 [tool-verified: `provisa/api/rest/openapi_spec.py` `cmd_path = f"/{cmd_domain}/commands/{cmd_name}"`, `_arg_type_to_openapi` (`"json"` returns `{}`)]

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

[inferred: host, domain and command name are placeholders; not run]

### 拒絕 {: #refusals }

遠端服務的拒絕按其原樣傳回。 [tool-verified: `_refused` in `source_operation.py`]

| 遠端服務 | Provisa 回應 |
| --- | --- |
| 拒絕該呼叫（HTTP 4xx、GraphQL `errors`、被拒絕的 gRPC 呼叫） | 422，`functions.remote_refused`，附帶 `remote_status` 和 `answer` |
| 失敗（HTTP 5xx） | 502, `functions.remote_refused` |

來源的憑證能否執行該操作，由遠端服務在呼叫該操作時決定。Provisa 無法在註冊時試寫而不真正執行它。 [tool-verified: REQ-1924 CREDENTIAL AT CALL amendment in `docs/arch/requirements.yaml`; no credential check in `_as_source_operation`]

### 批准 {: #approval }

開啟 **需要審批** 後，每次呼叫在執行前都會提交給部署的批准掛鉤。僅當掛鉤批准時才會執行。 [tool-verified: `provisa/api/data/action_exec.py` `_require_approval`]

- 未設定掛鉤：403，`functions.approval_unavailable`。
- 掛鉤拒絕：403，`functions.approval_denied`，附帶掛鉤給出的原因。

掛鉤會收到呼叫者、角色、命令名稱及其引數。參見 [ABAC 批准掛鉤](security.md#abac)。該旗標儲存為 `Function.requires_approval`，該檢查適用於任何設定了它的命令。 [tool-verified: `action_exec.py` `if fn.get("requires_approval")`]

### 寫入表 {: #writes-table }

在 **寫入表** 欄位中填寫該操作所寫入的資料表，格式為 `schema.table`。它必須是該命令自身來源下的已註冊資料表，否則儲存會被拒絕，傳回 422，`actions.written_table_not_registered`。該設定為選用。 [tool-verified: `_check_written_table` in `actions_router.py`; `written_table` in `source_operation.py`]

遠端服務每接受一次呼叫，Provisa 就將其視為對該資料表的一次寫入。它會捨棄該表的快取回應，將基於它的具體化檢視標記為過期，發出變更事件，執行該表的 sink，並在該表被保持為熱狀態時重新載入它。 [tool-verified: `provisa/api/data/table_written.py` `after_table_written`]

當該表從其整表複本讀取時（營運方的設定將其置於複本上，或引擎無法就地讀取其資料來源），呼叫之後會請求建置該複本，原因為 `write`。讀取方在新複本取代之前繼續使用舊複本。逐行複製的表或帶有參數欄的表沒有可重建的整表複本。 [tool-verified: `provisa/api/data/table_written.py` `_request_replica_build`; `provisa/federation/replica_state.py` `REASON_WRITE`]

### 為何不能被組合 {: #why-it-cannot-be-composed }

寫入操作是一個動作，而不是對資料的轉換。包含它的檢視或具體化檢視會在每次被讀取或重新整理時執行該寫入。因此該呼叫必須單獨存在：單獨的 `SELECT * FROM create_issue(...)` 會執行它，而同一呼叫出現在更大的陳述式內則會被拒絕，無論位於何處——聯結、子查詢或投影中。 [tool-verified: `provisa/pgwire/_pipeline.py` `_refuse_composed_mutators`; `provisa/executor/source_operation.py` `writes_called_in`]

若檢視或具體化檢視的定義呼叫了它，儲存時會被拒絕。只要註冊了任何寫入操作，無法剖析的定義也會被拒絕，因為無法證明它沒有呼叫任何一個。 [tool-verified: `refuse_writes_in_definition` in `source_operation.py`, called from `provisa/api/admin/_table_ops.py` `_build_columns_for_input` (views) and `provisa/api/admin/schema_common.py` (materialized views)]

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view (REQ-1924)
```

來源操作也不是血緣節點：血緣讀取自檢視和查詢的 SQL，命令在其中表現為節點，而任何已儲存的定義都無法呼叫來源操作。 [tool-verified: `provisa/lineage/graph.py` (`kind="command"` for a call in the SQL); `refuse_writes_in_definition`]

## 命令與血緣

由於每個命令都宣告了自己的輸入與輸出欄位，欄位層級血緣得以**跨越不透明的命令邊界而閉合**。血緣引擎會套用一次污染閉合：每個已宣告的輸出欄位，都推導自每個已宣告的輸入欄位。[tool-verified: `_splice_commands` in graph.py:223-242]

**由此而來的實際後果：** 你的輸入合約有多寬，那次閉合就有多精確。窄輸入——只放命令真正需要的欄位——產出的是一個緊湊、易讀的血緣錐形。把來源關聯裡的每個欄位都宣告進去，則會在每個輸出上扇入一大片；這仍然是健全的（沒有任何血緣遺失），但可追溯性會變得模糊。

**經驗法則：** 只傳命令所需的最小投影，且只傳回衍生欄位（不要把輸入原封不動地回傳）。這樣能讓污染錐形保持準確。[inferred from
_splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

命令節點在 DAG 中如何呈現、又該怎麼閱讀，見 [血緣](lineage.md)。

## 外連允許清單

`http` 與 `grpc` 命令會呼叫外部端點。每個目標主機都必須出現在該部署的 `udf_egress_allowlist` 上。回送位址（`localhost`、`127.0.0.1`、`::1`）一律放行。允許清單不存在時，所有外部外連皆以 HTTP 403 拒絕——沒有靜默的預設值。[tool-verified: `_check_egress` in function_dispatch.py:292-311]

## 呼叫追蹤（REQ-886）

不論結果如何，每次呼叫都會發出一筆追蹤。追蹤內容包含命令名稱、傳輸種類、身分模型（DEFINER 或 INVOKER）、輸入關聯引用、角色 id，以及輸出基數。追蹤由派送器發出——沒有任何 `impl_kind` 繞得過去。
[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]

## CLI：provisa metadata export

`provisa metadata export` 是 shell 層級的工作，不是受治理的 RPC。它會向 `/admin/metadata-export/publish` 發出 POST，觸發執行中伺服器的隨選中繼資料發佈（REQ-1072／REQ-1074）——與管理分頁上**立即發佈**按鈕所呼叫的是同一個端點。[tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

當設定的 `reconcile_cron` 排程粒度不夠細時，可用它從 cron 或 CI 驅動定時匯出：

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

結束碼 0 = 完整發佈。結束碼 1 = 部分發佈或連線失敗。

完整的旗標參考、驗證選項、多租用戶主機命名以及 cron 範例，見
[中繼資料匯出——從命令列](metadata-export.md#from-the-command-line)。


命令會出現在每個環境的 git 投影中。命令及其標籤指派如何在合併與拉取中存續，見 [環境](environments.md)。
