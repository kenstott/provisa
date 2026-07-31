# 命令（Command）

命令是一個已註冊、受治理的函式，將外部運算納入 Provisa 的治理、審核及血緣系統。聯邦引擎原生處理 SQL 之處，命令則是其無法表達之運算的接縫：一個增益微服務、一個 Python 模型、一個 shell script、一個原生資料庫預存程序。註冊一次；每個用戶端介面——GraphQL、pgwire SQL、REST、Arrow Flight、gRPC、Bolt/Cypher——均可以相同的治理呼叫它 (REQ-885、REQ-1156)。[tool-verified: function_dispatch.py module docstring + REQ-885 in requirements.md]

關鍵區別在於：命令是**受治理的 RPC**，而非隨意的 ETL。其輸入與輸出均經宣告、具型別、經驗證、經追蹤，並納入血緣系統。一個未受治理的 curl 呼叫或子行程都不具備這些特性。

## 實作種類

支援五種 `impl_kind` 值 [tool-verified: `_EXECUTORS` dict in function_dispatch.py:420-426]：

| `impl_kind` | 傳輸方式 |
|---|---|
| `source_procedure` | 已註冊數據來源上的原生預存程序 |
| `script` | 本機子行程，以 JSON 餵入 stdin，自 stdout 讀取 JSON |
| `http` | HTTP/S 端點；JSON 要求本文，JSON 回應 |
| `grpc` | gRPC 一元呼叫；無 proto 的 JSON 橋接 |
| `python` | 行程內 Python 可呼叫物件（`module:attr`） |

定址（目錄中的 `name` 及 `function_name`）與 `binding`（傳輸方式及位置）互相解耦。更換 binding 時，該命令的治理、血緣及呼叫端契約保持不變。[tool-verified: Function model in models.py:710-750]

## 引數種類

每個引數均宣告一個 `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]：

| `arg_kind` | 行為 |
|---|---|
| `column_value` | 純量；直接於要求承載中傳遞 |
| `table_ref` | 惰性；Provisa 原樣傳遞該關係參照；由服務端擷取數據 |
| `result_set` | 積極；Provisa 具體化所參照的關係，並傳送其資料列 |

`http` 及 `grpc` 命令**必須**宣告至少一個 `table_ref` 或 `result_set` 引數。一個僅接收純量引數的外部命令會被逐列呼叫一次，這會破壞批次處理。派送器會於呼叫時拒絕此設定（422）。[tool-verified: `_reject_rowwise_external` in function_dispatch.py:322-344]

一個回傳集合（經由 `output_columns` 及 `return_schema` 宣告）的命令，即為一個資料表值函式。可於 `FROM` 子句或 `JOIN` 中使用它。[inferred from models.py:744-748 and command_localize.py:52-63]

## 數據集契約 (REQ-1159)

每個 `table_ref` 或 `result_set` 引數均可宣告一個**輸入欄位契約**：於 `FunctionArgument.columns` 中一份有序、具 IR 型別的欄位清單。命令本身則於 `Function.output_columns` 中宣告一個**輸出欄位契約**。[tool-verified: DatasetColumn model in models.py:675-683, Function.output_columns in models.py:748]

兩份契約均於每次呼叫時進行「失敗即顯」驗證：

- **輸入（僅 result_set）：**具體化之後，Provisa 會依所宣告的欄位驗證資料列。多餘欄位、缺漏欄位及型別錯誤均會擲出 HTTP 422。[tool-verified: `_validate_against` called in `_prepare_args` at function_dispatch.py:243-248]
- **輸出：**該命令所回傳的資料列，會於送達呼叫端之前依 `output_columns` 進行驗證。[tool-verified: function_dispatch.py:488-490]
- **窄投影：**當宣告了輸入契約時，具體化查詢僅投影**該等欄位**（`SELECT "id", "region" FROM ...`），而非 `SELECT *`。[tool-verified: `_materialize_relation` at function_dispatch.py:155-177, col_names passed to projection at line 171]

### IR 型別詞彙

契約欄位型別採用標準 IR 型別系統（REQ-846），而非 GraphQL 純量或數據來源原生拼寫。有效名稱為 [tool-verified: `_IR_TO_SA` keys in ir_types.py:45-63]：

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

常見別名會自動解析（`varchar` → `text`、`int4` → `integer`、`jsonb` → `json` 等）。[tool-verified: `_ALIASES` dict in ir_types.py:67-90]

`return_schema` 是 `output_columns` 的 **GraphQL 投影**，而非事實來源。請宣告 `output_columns` 以供驗證及血緣使用；再加入 `return_schema` 以供 GraphQL 型別產生使用。[tool-verified: models.py:744-748, comment "return_schema is its GraphQL projection"]

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

gRPC 版本（`enrich_grpc_set`）遵循相同型態，惟指定 `impl_kind: grpc`，並以帶有 `target` 及 `method` 鍵值的 `binding` 取代 `callable`：

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

### Admin UI

**Settings → Commands** 中的命令表單，包含一個逐數據集的輸入欄位編輯器（每個已宣告欄位各一列，具 IR 型別選擇器）及一個輸出欄位編輯器。儲存該表單即可註冊或更新命令，無須重新載入設定。[inferred from CommandFormFields.tsx]

## 內嵌組合 (REQ-1159)

命令可出現於一個更大的 SQL 陳述式**之內**——經 JOIN、子查詢或投影。您並不受限於 `SELECT * FROM fn(args)`。

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

於治理、驗證或路由執行之前，管線會偵測已註冊的命令呼叫，透過共用的受治理執行器逐一執行（因此 I/O 契約及身分模型的套用方式與直接呼叫完全相同），並將呼叫位置改寫為一個具型別的本機關係。[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 and localize_commands in command_localize.py:178-222]

替換方式會依大小調整：1,000 列以內的結果，會以具型別的 `VALUES` 清單內嵌；超過此門檻，則會於引擎中註冊為一個具名的本機關係。[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, path at lines 211-216]

已本機化的陳述式會照常路由。單一數據來源查詢會留在該數據來源上；僅真正跨數據來源的查詢才會送往聯邦引擎。[tool-verified: _pipeline.py:304 comment "REQ-1159: a localized statement carries an inline local relation..."]

## 命令與血緣

由於每個命令均宣告其輸入及輸出欄位，欄位層級血緣**得以跨越該不透明的命令邊界而閉合**。血緣引擎套用一種污染閉合：每個已宣告的輸出欄位均衍生自每個已宣告的輸入欄位。[tool-verified: `_splice_commands` in graph.py:223-242]

**可據以行動的後果：**您輸入契約的寬度，決定了該閉合的精確度。窄輸入——僅包含該命令實際需要的欄位——會產生一個緊湊、易於閱讀的血緣錐形。若宣告來源關係中的每一個欄位，則會廣泛扇入至每一個輸出，雖仍屬健全（不會遺失任何血緣），但會使可追溯性變得模糊。

**經驗法則：**僅傳遞該命令所需的最小投影，並僅回傳衍生欄位（而非原樣回傳、未經改動的輸入）。這能使污染錐形保持準確。[inferred from _splice_commands behavior in graph.py and _materialize_relation narrow-projection in function_dispatch.py:161]

關於命令節點如何呈現於 DAG 中，以及如何解讀它們，請參閱[血緣](lineage.md)。

## 外連允許清單

`http` 及 `grpc` 命令會呼叫外部端點。每個目標主機均須列於該部署的 `udf_egress_allowlist` 中。Loopback（`localhost`、`127.0.0.1`、`::1`）恆獲允許。若未設定允許清單，則會以 HTTP 403 拒絕所有外部連出——不存在無聲的預設值。[tool-verified: `_check_egress` in function_dispatch.py:292-311]

## 呼叫追蹤 (REQ-886)

無論結果為何，每次呼叫均會發出一筆追蹤紀錄。該追蹤紀錄包含命令名稱、傳輸種類、身分模型（DEFINER 或 INVOKER）、輸入關係參照、角色 ID 及輸出基數。派送器會發出該追蹤紀錄——沒有任何 `impl_kind` 可繞過它。[tool-verified: `udf_invocation_trace` context in dispatch_function:475-492]
