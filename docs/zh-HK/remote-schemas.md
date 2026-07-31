# 遠端結構描述

遠端結構描述來源將外部 API——GraphQL、gRPC 或 REST（OpenAPI）——連接至 Provisa 語意層。註冊後，外部 API 的操作即成為一級的 Provisa 資料表及函式。（REQ-308、REQ-316、REQ-325）所有治理規則、查詢介面及安全層均會自動套用。（REQ-310、REQ-319、REQ-328）遠端服務永遠不會看到 Provisa 的治理規則。（REQ-310、REQ-319、REQ-328）

---

## 三種來源類型

### GraphQL 遠端結構描述（REQ-307–313）

**如何註冊。** 向 `/admin/sources/graphql-remote` 發送 POST 請求，帶上端點 URL、命名空間及可選的驗證設定。Provisa 會向遠端端點發出標準的 `__schema` 內省查詢。（REQ-307）[tool-verified: `provisa/graphql_remote/introspect.py:47–59`]

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

**自動發現的內容。** 遠端 `Query` 類型上每個回傳 OBJECT 的欄位都會成為虛擬資料表。遠端 `Mutation` 類型上每個欄位都會成為受追蹤的函式。（REQ-308）[tool-verified: `provisa/graphql_remote/mapper.py:243–278`]

**資料表命名。** 資料表命名為 `{namespace}__{field_name}`。以命名空間 `petstore` 及查詢欄位 `pets` 為例：資料表名稱為 `petstore__pets`。（REQ-312）[tool-verified: `provisa/graphql_remote/mapper.py:250`]

**類型對應（REQ-308）。** 純量欄位會直接對應至 Provisa 類型。OBJECT 欄位則按目標類型是否受治理而分為兩種情況（見下方「受治理資料表」）。[tool-verified: `provisa/graphql_remote/mapper.py:14–36`、`provisa/api/data/endpoint.py:655–671`、`provisa/compiler/schema_gen.py:481–485`]

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

當受治理資料表上的 OBJECT 類型欄指向另一個受治理類型時，該欄位會同時受三項規則規範 [tool-verified: `provisa/api/data/endpoint.py:655–671`、`provisa/compiler/schema_gen.py:481–485`]：

1. **從 GQL 擷取中排除**——擷取父資料表的資料列時，不會請求該欄位。
2. **從 SDL 中排除**——該欄位不會出現在生成結構描述中的父類型上。
3. **僅可透過已宣告的關聯存取**——data steward 必須在兩個已具體化的受治理資料表之間註冊 JOIN。若無此關聯，該欄位純粹缺席；並無 blob 後備方案。

無法作為根 Query 欄位存取的 OBJECT 類型（例如 `ContactInfo` 或 `Address` 等內嵌類型）遵循不同的規則：它們會以 `jsonb` blob 欄的形式擷取，並於 SDL 中呈現為巢狀物件欄位。子欄位可透過 SQL 中的 `-->>` 擷取存取。

**必要引數。** 當根查詢欄位帶有非空值、無預設值的引數時，這些引數會成為資料表上的 `native_filter_type: query_param` 欄位（於注入時加上 `_nf_` 前綴）。執行器會將其作為 GraphQL 變數傳遞。（REQ-555）[tool-verified: `provisa/graphql_remote/mapper.py:110–120`、`provisa/api/app.py:1280–1303`]

**自動偵測的關聯。** Provisa 會掃描每個資料表中 OBJECT 類型的欄位。當被參照的 GQL 類型亦已於同一來源中註冊為資料表時，即會產生一項關聯。多對一關聯會依命名慣例推斷來源及目標欄位（來源類型上的 `breedName` → 目標類型 `Breed` 上的 `name`）。一對多（LIST）欄位所產生的關聯，其欄位參照為空——外部索引鍵位於目標一方。（REQ-554）[tool-verified: `provisa/graphql_remote/mapper.py:162–202`]

**Mutation。** Mutation 欄位會產生受追蹤的函式，其引數類型依 mutation 的引數對應而來，`return_schema` 則依 mutation 的回傳類型推導。（REQ-308）[tool-verified: `provisa/graphql_remote/mapper.py:261–278`]

**刷新。** 向 `/admin/sources/graphql-remote/{id}/refresh` 發送 POST 請求。此操作會重新對遠端結構描述進行內省，並更新資料表及函式的註冊資訊。既有的治理規則（RLS、遮罩）將予以保留。（REQ-311）[tool-verified: `provisa/api/admin/graphql_remote_router.py:217–257`]

**限制。**

- 純量及 ENUM 類型的根查詢欄位（回傳類型非 OBJECT）會成為受追蹤的函式，而非虛擬資料表。其 `return_schema` 為單一欄位 `value`，類型為對應的純量類型。[tool-verified: `provisa/graphql_remote/mapper.py:254–279`]
- 物件巢狀結構於註冊時會解析至 `graphql_remote.max_object_depth`（預設值：5）的深度。遠端擷取的欄位選擇及子欄位中繼資料均會建構至此深度；超出限制的欄位不會被擷取，亦無法用於 SQL 擷取。（REQ-556）[tool-verified: `provisa/graphql_remote/mapper.py:38–52`]
- LIST 類型的巢狀 OBJECT 欄位（例如 `breed.awards: [Award]`）會於擷取選擇中納入，直至 `graphql_remote.max_list_depth` 個巢狀層級（預設值：2）。於此限制內，清單會以 `jsonb` 陣列的形式擷取至父欄位，而 GQL 選擇會注入 `first: N`（N 為 `graphql_remote.max_list_items`，預設值：100），以限制陣列大小。超出 `max_list_depth` 時，該 LIST 欄位會完全被排除，以防止資料無限膨脹。在 SQL 中，可透過 `json_array_elements(column_name)` 或以 `->>` 進行索引擷取來存取該陣列。若清單的元素類型本身具有根查詢，建議將其另行註冊為獨立資料表並建立關聯——聯結路徑效率更高，亦可繞過 blob。（REQ-556）[tool-verified: `provisa/graphql_remote/mapper.py:43–70`]
- 對於 SQL 查詢，未受治理的 OBJECT 類型欄位會從遠端來源完整擷取（所有子欄位至設定深度為止），並以 `jsonb` 形式快取。SQL 中對子欄位的存取是透過對 blob 進行 `->>` 擷取來處理；遠端請求不會限縮為 SQL 查詢所選取的欄位。當清單的元素類型沒有根查詢，且 blob 表示法不敷使用時，應直接以 GraphQL SDL 撰寫查詢——Provisa 會忠實地重現 GQL 欄位選擇，令遠端來源僅接收到確切請求的欄位。[tool-verified: `provisa/compiler/sql_gen.py:1332–1368`]
- 若遠端伺服器因需要子欄位選擇而拒絕某個 OBJECT 類型欄位（在 `gql_selection` 可用時理應不會發生此情況），執行器會移除該等欄位後重試一次，以確保純量欄位仍可正常回傳。[tool-verified: `provisa/graphql_remote/executor.py:76–80`]

---

### gRPC 遠端結構描述（REQ-322–329）

**如何註冊。** 向 `/admin/grpc-remote/register` 發送 POST 請求，帶上伺服器位址、`.proto` 檔案的路徑或 URL，以及可選的 TLS 設定。

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

Provisa 會擷取 proto 檔案，以純文字解析器（解析時不依賴任何外部 proto 依賴項）進行解析，透過 `grpc_tools.protoc` 編譯 Python stub，並開啟一個持續存在的 `grpc.aio.Channel`。（REQ-322）[tool-verified: `provisa/grpc_remote/loader.py:99–128`、`provisa/grpc_remote/loader.py:166–214`、`provisa/api/admin/grpc_remote_router.py:80–104`]

Proto 檔案亦可為本機路徑。常見類型（`google/protobuf/timestamp.proto`）的匯入路徑會於註冊時儲存，並於刷新時重複使用。（REQ-329）[tool-verified: `provisa/grpc_remote/loader.py:135–159`]

**自動發現的內容。** Proto 中的每個 `rpc` 方法均會依優先順序使用三項訊號分類為 query 或 mutation：（REQ-323）[tool-verified: `provisa/grpc_remote/mapper.py`]

1. **註冊酬載中的 `method_overrides`**——`{"MethodName": "query"}` 或 `{"MethodName": "mutation"}` 優先於其他一切。
2. **`server_streaming: true`**——伺服器發送訊息串流；恆為虛擬資料表（除非輸出為純量）。
3. **輸出訊息帶有重複的訊息類型欄位**——例如 `ListOrdersResponse { repeated Order items; }` 會被視為清單包裝並成為虛擬資料表。重複的純量欄位（例如 `repeated string tags`）不會觸發此規則——它們是單一實體的陣列屬性，並非資料列來源。

不符合以上任何訊號的方法（回傳單一實體訊息的一元 RPC，或任何純量輸出）會成為受追蹤的函式。

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

**Mutation 方法（REQ-326）。** 輸入訊息的欄位會成為 mutation 輸入引數。輸出訊息的結構描述則會成為 `return_schema`。[tool-verified: `provisa/grpc_remote/executor.py:122–143`]

**頻道管理。** 每個已註冊來源會有一個 `grpc.aio.Channel`，儲存於應用程式狀態中並於後續請求重複使用。刷新時，舊頻道會在新頻道開啟前關閉。（REQ-327）[tool-verified: `provisa/api/admin/grpc_remote_router.py:107–117`]

**刷新。** 向 `/admin/grpc-remote/refresh/{source_id}` 發送 POST 請求。此操作會從已儲存的路徑重新載入 proto、重新編譯 stub，並重新註冊資料表及函式。另外，亦可向 `/admin/grpc-remote/{source_id}/proto` 發送 PUT 請求，並附上新的 `proto_text` 以內嵌方式更新 proto。（REQ-329）[tool-verified: `provisa/api/admin/grpc_remote_router.py:241–268`、`provisa/api/admin/grpc_remote_router.py:300–358`]

**限制。**

- 物件子欄位擷取僅支援一層深度。超出深度 1 的巢狀訊息欄位不會遞迴展開。（REQ-556）[tool-verified: `provisa/grpc_remote/mapper.py:111–128`]

---

### OpenAPI / REST（REQ-314–321）

**如何註冊。** 呼叫 `auto_register_openapi_source`，並帶上來源識別碼、已解析的規格及連接中繼資料。此規格可從本機檔案或 URL 載入。（REQ-314）[tool-verified: `provisa/openapi/loader.py:30–55`、`provisa/openapi/register.py:249–264`]

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

**自動發現的內容。** 規格中每個 GET 操作都會成為虛擬資料表，除非其回應結構描述屬純量類型（`string`、`number`、`boolean`、`integer`）——回傳純量的 GET 操作則會成為帶有單一 `value` 欄的受追蹤函式。每個非 GET 操作（POST、PUT、PATCH、DELETE）都會成為受追蹤的函式。（REQ-316、REQ-317）

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

**作為原生篩選器欄位的參數。** 尚未屬於回應欄位的路徑及查詢參數，會成為 `native_filter_type` 設為 `path_param` 或 `query_param`、並以 `_nf_` 為前綴的欄位。當參數名稱與回應欄位名稱相符時，該參數的中繼資料會併入既有的欄位項目，而非另建重複項目。（REQ-555）[tool-verified: `provisa/openapi/register.py:116–122`、`provisa/openapi/register.py:172–196`]

**回應結構描述的解析。** 映射器會依序檢查 `responses.200`、`responses.2xx`，再檢查 `responses.default`。陣列類型的回應會展開至其元素結構描述。`$ref` 參照會解析至一層深度。（REQ-316）[tool-verified: `provisa/openapi/mapper.py:83–101`]

**物件子欄位。** 帶有 `type: object` 且自身具有 `properties` 的回應屬性，會儲存為該欄位上的 `object_fields`。這些子欄位於 SDL 中可見，並用於查詢中的 `jsonb` 擷取。（REQ-556）[tool-verified: `provisa/openapi/register.py:87–96`]

**回應快取（REQ-318）。** GET 操作的結果會由 `pg_cache.py` 快取於 PostgreSQL 中。每種請求參數組合均擁有其專屬的 `_params_hash` 群組。當 TTL 到期時，特定雜湊值的資料列會被取代。帶路徑參數的端點（`/pets/{id}`）會略過初始批量擷取——快取資料表會先建立為空以供結構描述內省之用，再依主索引鍵於請求到達時逐步填入。[tool-verified: `provisa/openapi/pg_cache.py:181–234`、`provisa/openapi/pg_cache.py:307–360`]

**刷新（REQ-321）。** 重新解析規格並再次呼叫 `auto_register_openapi_source`。既有的治理規則會予以保留；註冊資訊會以 ON CONFLICT upsert 方式更新。[tool-verified: `provisa/openapi/register.py:249–264`]

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

**關聯治理（V002）。** 針對遠端資料表的 JOIN 條件——當透過 SQL 或 Cypher 查詢時——必須符合一項已註冊並已核准的關聯。（REQ-604）由於 SDL 定義的關聯依設計已預先核准，GraphQL 查詢會略過 V002 檢查。詳見 [docs/security.md](security.md#v002)。

**OBJECT 類型欄位。** 當欄位對應至未受治理的內嵌 GQL OBJECT 或 OpenAPI 物件類型時，其 Provisa 類型為 `jsonb`。該欄位會儲存完整的巢狀 JSON blob。當宣告了子欄位（`gql_object_fields` 或 `object_fields`）時，`gql_object_columns` 對應表會於結構描述建構時填入。當查詢選取這些子欄位時，SQL 生成器會使用此對應表發出 `->>` 擷取運算式。[tool-verified: `provisa/api/app.py:1305–1315`、`provisa/compiler/schema_gen.py:80–82`]

**作為原生篩選器參數的必要引數。** 帶有非空值、無預設值引數的根查詢欄位，會為已註冊資料表注入額外欄位。這些欄位帶有 `native_filter_type: query_param`。Cypher 轉譯器會將 `WHERE n.id = $val` 重寫為 `WHERE n._nf_id = $val`，而 GraphQL 執行器則會將其識別為要傳遞至遠端端點的變數。（REQ-555）[tool-verified: `provisa/api/app.py:1280–1303`]

---

## 建立覆蓋性關聯的影響

當 data steward 於兩個遠端資料表之間（或於一個遠端資料表與一個本機資料表之間）註冊一項關聯時，該關聯即成為查詢時所使用的聯結路徑。

**聯結如何取得優先。** 於查詢編譯階段，Provisa 會透過已註冊的關聯解析聯結路徑。該關聯的 `source_column` 及 `target_column` 會成為生成 SQL 中的聯結條件。聯結會取代原本針對已連接類型所需的、按資料表逐一發出的遠端呼叫。

**原始 blob 永遠不會於 SQL 中曝露。** `petstore__pets` 上的 `breed` 欄位無法於 SQL 查詢中作為原始 jsonb 值選取。當 `petstore__pets` 與 `petstore__breeds` 之間已註冊一項關聯時，SQL 查詢會經由聯結解析——`SELECT breed.name FROM petstore__pets` 是透過外部索引鍵聯結解析，而非透過 blob。若未註冊任何關聯，但該欄位帶有已宣告的子欄位（`gql_object_fields`），則 SQL 中對子欄位的參照會被重寫為對已儲存 blob 的 `->>` 擷取。此路徑僅適用於未受治理的內嵌類型——受治理目標類型的欄位完全從 SDL 中排除，並無 blob 可供擷取。原始 blob 本身永遠不會以裸欄位值的形式輸出。[tool-verified: `provisa/compiler/sql_gen.py:1156`、`tests/unit/test_sql_gen.py:TestGqlJsonBlobExtraction`]

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
