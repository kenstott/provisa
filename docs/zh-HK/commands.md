# 命令

命令是一個已註冊、受治理的函式，把外部運算納入 Provisa 的治理、審計與血緣體系。聯邦引擎原生處理 SQL，而命令則是它表達不了的那些運算的接縫：一個資料增益微服務、一個 Python 模型、一段 shell 指令碼、一個資料庫原生預存程序。註冊一次，每個用戶端介面——GraphQL、pgwire SQL、REST、Arrow Flight、gRPC、Bolt/Cypher——都能以完全相同的治理去呼叫它（REQ-885、REQ-1156）。[tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

關鍵分野在於：命令是**受治理的 RPC**，不是臨時拼湊的 ETL。它的輸入與輸出都經過宣告、定型、驗證、追蹤，並接進血緣。未受治理的 curl 呼叫或子行程，一樣都不是。

## 實作種類

支援五個 `impl_kind` 值 [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]：

| `impl_kind` | 傳輸方式 |
| --- | --- |
| `source_procedure` | 已註冊數據來源上的原生預存程序 |
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

命令可以出現在較大的 SQL 陳述式**之內**——被聯結、被用作子查詢，或被投影。你並不限於 `SELECT * FROM fn(args)`。

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
