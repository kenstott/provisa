# 安全模型 {: #security-model }

Provisa 在所有查詢語言（GraphQL、SQL、Cypher）及所有傳輸（REST、gRPC、Arrow Flight、JDBC、WebSocket）上執行分層安全模型。（REQ-001、REQ-266） 治理一律適用——不存在繞過它的查詢路徑。（REQ-002、REQ-266）

各層按次序套用。請求必須通過每一層，下一層才會被評估。

## 分層模型 {: #layered-model }

### 第 0 層——內省過濾 {: #layer-0-introspection-filtering }

呈現給角色的結構描述及目錄，只包含其 `domain_access` 清單中的資料表，以及通過每個欄位 `visible_to` 規則的欄位。（REQ-039） 角色存取範圍以外的物件在探索時不可見——無法查詢、自動完成或推斷其存在。（REQ-039） 這適用於 GraphQL 結構描述、SQL 目錄，以及查詢編輯器中的結構描述瀏覽器。（REQ-039、REQ-363）

請參閱[結構描述可見性](#schema-visibility)。

### 第 1 層——公開存取 {: #layer-1-public-access }

以 `public` 範圍發佈的欄位，會提供給組織中的每個角色，包括其 `domain_access` 並未涵蓋該資料表網域的角色；這樣的角色只會看到帶有公開欄位的該資料表。在註冊資料表的人明確說明之前，沒有任何內容是公開的。（REQ-1959） 請參閱[公開欄位](#public-columns)。

### 第 2 層——網域存取 {: #layer-2-domain-access }

每個角色攜帶一份由網域 id 組成的 `domain_access` 清單。觸及這些網域以外資料表的查詢，會在執行前被拒絕。（REQ-038、REQ-039） 這是粗粒度的擁有權邊界——不論 SQL 如何撰寫，HR 角色都無法觸及財務資料表。（REQ-002）

請參閱[權限模型](#rights-model)。

### 第 3 層——行層級安全 {: #layer-3-row-level-security }

確認網域存取之後，按資料表、按角色的 `WHERE` 謂詞會在執行時注入每一個 `SELECT`。（REQ-041、REQ-263） 謂詞針對數據來源的數據求值。查詢共用訂單資料表的區域經理即使使用 `SELECT *`，也只會看到其所在區域的資料列。（REQ-264）

請參閱[行層級安全（RLS）](#row-level-security-rls)。

### 第 4 層——欄位可見性與遮罩 {: #layer-4-column-visibility-and-masking }

`visible_to` 清單不包括提出請求角色的欄位，會從查詢輸出中移除。（REQ-040、REQ-263） 帶有遮罩規則的欄位，其值會在結果離開伺服器之前被取代——透過正規表示式編輯、常數取代或截斷。（REQ-263） 遮罩適用於所有查詢語言及輸出格式。（REQ-263）

請參閱[欄位權限模型](#column-permission-model)及[欄位層級遮罩](#column-level-masking)。

### 第 5 層——謂詞防護 {: #layer-5-predicate-guard }

被遮罩的欄位在 `WHERE` 及 `HAVING` 子句中會被拒絕。（REQ-263） 否則，即使輸出已遮罩，呼叫者亦可在過濾條件中以二分搜尋推斷出未遮罩的值。拒絕在剖析查詢時、執行之前發生。（REQ-531）

### 關係治理（V002） {: #relationship-governance-v002 }

SQL 的 JOIN 條件必須與資料表之間一個已註冊且已批准的關係相符。（REQ-001） 未經批准的聯結會被拒絕。每個關係都帶有人類可讀的原因及描述——為使用者及自主代理說明該走訪路徑為何存在。這是一項治理政策，而不是硬性的安全邊界：第 2–5 層的執行與聯結結構無關，因此蓄意的繞過不會揭露該角色以兩條獨立查詢無法取得的數據。繞過嘗試會被記錄並可審計。

**繞過機制**——V002 可以兩種方式繞過。第一種是能力：持有 `ignore_relationships` 的角色可以聯結目錄未涵蓋的關係。預設的系統角色中只有 `modeler` 擁有它——這是一個探索角色，其職責是定義模型，而不是執行模型。（REQ-1297） `analyst` 沒有它。

第二種是雙條件例外，兩個條件必須同時成立：

1. **角色旗標**——角色定義中的 `relationship_guard: false`（預設：`true`）。
2. **查詢內例外**——SQL 包含註解 `--relationship-guard=false`。

只有角色旗標不會繞過 V002；只有註解也不會繞過 V002。

**高安全模式鎖定防護。** 在 `security.mode: high` 之下，任何繞過都不起作用：`ignore_relationships` 會被忽略，`relationship_guard: false` 會被忽略，每個聯結都必須存在於已批准關係目錄中。（REQ-693） 這是刻意的冗餘——被誤授予該能力的正式環境角色，仍然無法跳出模型。

**GraphQL 路徑**——GraphQL 查詢一律略過 V002。SDL 中定義的關係本質上已預先批准；該檢查屬多餘，不會套用。

**SQL 及 Cypher 路徑**——V002 預設啟用。`endpoint_dev.py` 及 `cypher_router.py` 都會在呼叫 `validate_sql` 之前套用雙條件檢查。

**pgwire 路徑**——與 SQL 相同的雙條件檢查。註解 `--relationship-guard=false` 會在執行前從查詢中移除；它不會到達資料庫。

---

這些層互相疊加。對於同時具有網域存取、行層級安全及遮罩欄位的角色，所有五項限制會同時生效。新增數據來源、欄位或關係不需要更新每條規則——每一層獨立設定，並自動套用於任何觸及受治理物件的查詢。

---

## 權限模型 {: #rights-model }

可獨立指派的能力，可透過 `parent_role_id` 構成可選的角色階層。`admin` 授予一切。（REQ-042）

| 能力 | 說明 |
| ----------- | ------------- |
| `source_registration` | 註冊數據來源 |
| `table_registration` | 註冊資料表及欄位 |
| `create_relationship` | 定義外鍵關係 |
| `access_config` | 設定行層級安全及遮罩 |
| `query_development` | 執行查詢 |
| `write` | 呼叫已註冊的變更（粗粒度控制；請參閱變更操作授權） |
| `full_results` | 繞過取樣限制 |
| `ignore_relationships` | 繞過關係治理（V002）。系統角色中只有 `modeler` 擁有它，在高安全模式下會被完全忽略 |
| `admin` | 超級使用者——授予一切 |

### 角色繼承 {: #role-inheritance }

角色可以透過 `parent_role_id`（在設定中設定，或在安全頁面上設為「繼承自」）從一個父角色繼承。（REQ-215） 繼承鏈在啟動時攤平：子角色取得其祖先的能力與網域存取的聯集、每一項指名某個祖先的欄位、指標、函數及 webhook 授權，以及祖先按資料表的行層級安全規則，子角色優先——鏈中最近一個對某資料表有規則的角色提供該資料表的謂詞，為子角色儲存的規則會取代父角色對該資料表的規則。（REQ-1677）

```yaml
roles:
  - id: basic_user
    description: Queries the public domain.
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    description: Analyses sales; reads full results.
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```
## 角色：授予角色、派生角色及元角色 {: #roles-granted-derived-and-meta }

三種角色在 Provisa 的存取模型中協同運作。角色頁面把前兩種一併列出；元角色從不列出。

### 授予角色 {: #granted-roles }

授予角色直接指派給使用者。它攜帶權利：能力、網域存取、遮罩項目、資料列過濾。繼承（`parent_role_id`）在啟動時把祖先的權利摺疊進子角色。權利只會增加，沒有減去。要限制讀取者，請授予一個同時指名角色及條件的複合項目，或把一條遮罩項目放在寬鬆項目之前。（REQ-215、REQ-1677）

### 到期與未使用的授予 {: #expiring-and-unused-grants }

Provisa 持有的授予可以結束。設定了到期時間的授予由該時刻起，在每個請求中都不再被持有。角色可以聲明 `inactivity_days`：若某個該角色的授予超過這一天數未被任何請求依賴（由其最後一次使用與建立時間中較晚者起計），該授予即被移除。來自身份提供者聲明或角色對應的角色，其時限由提供者決定，請在那裡設定到期。衍生角色和元角色永不到期。(REQ-2077)

當某個角色單獨服務一個請求時，或者在請求以多個角色身份行事、且該角色獲授予語句所讀取的表時，該請求依賴該角色。以「全部」身份行事並不會把每個持有的角色都計為已使用。授予的最後一次使用每天最多記錄一次。

每 15 分鐘，每個組織的清理任務會刪除已到期和未使用的授予，並在管理審計記錄中將每一項記為 `role.expired` 或 `role.removed_inactive`。凡設定了外寄郵件的部署（包括只有一個組織的部署），持有人和組織管理員會在授予結束前 `auth.role_grant_notice_days` 天（預設 7 天）收到郵件，授予被移除時管理員亦會收到郵件；未設定郵件時，審計記錄即為憑證。無論用戶的郵件偏好如何，這些通知都會發送。

### 派生角色 {: #derived-roles }

派生角色來自使用者的屬性。組織宣告哪些屬性會成為角色（`derived_roles`），每個都附有描述。對每個已宣告的屬性 `a`，若使用者的屬性為其提供了值 `v`，使用者便擁有父角色 `a` 及其值角色 `a__v`：`region = eu` 得出 `region` 和 `region__eu`。沒有值，便兩者皆無。

屬性的解析很重要。組織本身關於成員的記錄，優先於身分識別提供者所提供的內容：兩者指名同一個屬性時，使用組織的值，提供者的值會被標記為已覆寫。

派生角色不攜帶權利，也從不手動指派。授權清單、遮罩或資料列規則指名它們的方式與指名授予角色完全相同；請求在 `X-Provisa-Role` 中指名它們並以其身份執行。（REQ-2057）

在角色頁面上，每個已宣告的屬性都是一個標示為*派生*的父角色，其值角色會隨着其值出現而列在它下面。**+ 角色 → 由屬性派生**用來宣告一個。它的面板顯示該屬性、其描述、至今見到的值，以及指名它或其某個值角色的每個遮罩、資料列規則及授權。它沒有授權控制項。值角色的描述是自動產生的：「當用戶的 region 為 eu 時擁有」。（REQ-2078、REQ-2079）

在設定中以清單形式宣告它們：

```yaml
derived_roles:
- attribute: region
  description: The subsidiary a member works for; region__eu marks EU staff.
```

想想 JWT：它的 claim 是關於這個人的事實。派生角色把選定的事實變成角色，讓授權可以針對它們，而不必為每種組合建立一個角色。

### 元角色 {: #meta-roles }

當請求指名多於一個角色時，Provisa 會建構一個臨時元角色，其識別碼為 `meta:<ids sorted, joined by +>`。它是所指名集合中每個角色的唯一子角色。整條管線隨後如同對待任何單一角色一樣以它執行。

權利是每個成員權利的聯集。資料列過濾是每個成員所允許過濾的 OR；若讀取某資料表的任何成員沒有過濾，元角色亦沒有——沒有過濾表示所有資料列。元角色從不儲存、列出或匯出。模型變更時，它會根據其成員重新建構。（REQ-1620）

每一行的欄按照其行過濾允許該行的角色進行遮蔽和顯示：在這些角色中，採用對該欄限制最少的讀取方式，未允許該行的角色對其讀取方式沒有任何影響。遮蔽在讀取表的地方生效，先於任何連接或聚合，因此聚合讀取的是遮蔽後的值。寫入以同樣方式檢查：只有當授予該欄的角色允許某行時，該欄才會在該行上被寫入。（REQ-1620）

審計記錄會指名每個執行中的角色，而不是元角色 id。由於元角色從不儲存，它沒有自己的描述：凡是顯示描述的地方，它的描述就是其成員的描述，每個 id 各附自己的描述。（REQ-2080）

### 描述 {: #descriptions }

每個角色及每條資料列規則都說明自己的用途。存取審查會讀取誰可以看到甚麼以及原因。沒有人能解釋的角色或規則，也就沒有人能審查，因此描述是必需的：描述為空的角色、派生父角色或資料列規則，會被設定載入、管理 API 及 UI 拒絕，並附上原因。角色清單及資料列規則清單顯示每條描述，其詳細資料面板顯示並編輯描述。（REQ-2080、REQ-2081）

一個角色只有一個描述，而不是每個網域一個。如果一個通用角色沒有描述它在某個特定網域中的職責，就表示該網域需要一個自己的角色，通常是通用角色的子角色（`parent_role_id`），它繼承通用角色的權利，並描述它在那裏做甚麼。

### 請求如何指名角色 {: #how-a-request-names-roles }

用戶端在 `X-Provisa-Role` 中傳送一個角色或逗號分隔的集合。所指名的每個角色都必須由使用者持有。未指派給該身份的角色會被點名拒絕。

權利及數據只跟隨請求所執行的角色：持有一個角色不等於以它執行。一個角色就是該角色；兩個或以上的角色以它們的元角色執行。如果執行角色會帶上所持有每個角色的權利，它便沒有意義，因此請求取得最小權限，審計記錄準確指名實際執行的內容。

被指名的派生角色以限定詞身份進入。限定詞沒有自己的權利、網域存取、遮罩或資料列過濾，因此它不是元角色的權利成員：作為沒有遮罩的成員，它會解除每個欄位的遮罩；作為不讀取任何資料表的成員，它會終結任何資料列過濾的 OR。它只進入授權：指名 `region__eu` 的授權或遮罩項目，會觸及同時指名該限定詞及一個授予角色的請求。（REQ-2057）

MCP 代理從不超出賦予它的角色行事。遠端代理只能指名其權杖主體所持有的角色。本地 stdio 代理只能指名 `mcp.role`（`PROVISA_MCP_ROLE`）中固定的集合內的角色，未指名任何角色時即以該集合行事；任何其他角色都會被點名拒絕。（REQ-2075）

代理透過 MCP 工具 `describe_role` 詢問自己以哪個角色執行。答案是該角色及其用途（元角色則附上其每個成員），以及代理可以切換到的角色，每個都附有其描述。答案從不包含資料列規則、遮罩、授權或條件。Provisa 從不告訴代理結果為何被收窄，結果亦不帶任何「已過濾」標記：角色的數據就是它的全部檢視，而說明缺少了甚麼的理由會誘使人去探查它。（REQ-2082）

### `+` 的含義 {: #what-means }

`+` 出現在兩個地方，各有不同含義。

**在授權項目中。** `visible_to` 或 `to` 清單中的 `analyst+region__eu` 表示讀取者同時持有全部——`analyst` 與 `region__eu` 同時持有。並非複合的項目就是一個角色。清單項目之間的規則是任一：讀取者持有任一項目即擁有該授權。內部為 AND，之間為 OR。

**在元角色 id 中。** `meta:analyst+engineer` 是集合 {analyst, engineer} 的元角色在伺服器端的識別碼。它的權利是兩者的聯集。用戶端從不直接指名元角色 id；它指名自己持有的角色，由伺服器建構元角色。

### 把 ABAC 欄位規則化為簡單的 RBAC {: #abac-column-rules-as-plain-rbac }

以屬性為基礎的政策問：「具有這些屬性的這個使用者能看到這個欄位嗎？」RBAC 問：「哪些角色能看到這個欄位？」複合授權及派生角色把它變成同一個問題。

「只對歐盟的分析師解除這個電郵的遮罩」就是遮罩項目 `{to: [analyst+region__eu], mask: null}`。任何同時持有 `analyst` 角色及 `region__eu` 派生角色的讀取者，都能讀取未遮罩的該欄位。不需要政策語言。不需要為每種組合建立角色。

其代價是以計算取代列舉。小型角色目錄降低維護成本，但「誰能看到這個歐盟電郵？」變成一個計算出來的答案：複合項目會對照每個使用者的指派及 claim 求值。Provisa 可以雙向回答，因為模型是一份定義。欄位編輯器中的存取檢查（`POST /admin/masks/check`）會為選定的角色集合執行這項求值。

### 好處 {: #benefits }

- ABAC 欄位規則成為 RBAC 授權項目。
- 小型角色目錄，而不是每種組合一個角色。
- 一份定義協調每個區域的規則。
- 每個請求都是最小權限及準確的審計記錄。

### 指引 {: #best-practices }

- 在組織許可的範圍內盡量保持角色目錄細小。
- 在同一維度內繼承（年資、密級）。跨獨立維度組合（角色 + 區域、角色 + 部門）。
- 把最具體的遮罩項目放在最前——即使讀取者持有後面的項目，前面的項目亦會優先。
- 在組合投入正式環境之前，以 `POST /admin/masks/check` 檢查實際存取。

## 欄位權限模型 {: #column-permission-model }

每個欄位都有一個權限模型，按角色控制讀取、寫入及遮罩。（REQ-042、REQ-249）

### 三層可見性 {: #three-tier-visibility }

| 層級 | 條件 | 結果 |
| ------ | ----------- | -------- |
| **隱藏** | 角色不在 `visible_to` 之中 | 該欄位不在結構描述及目錄中 |
| **遮罩** | 角色在 `visible_to` 之中；它所持有的第一條遮罩項目有 `mask` | 欄位可見，輸出中的值被取代 |
| **未遮罩** | 角色在 `visible_to` 之中；它所持有的第一條遮罩項目沒有 `mask`，或它沒有任何項目 | 完整讀取權限 |

### 公開欄位 {: #public-columns }

每個欄位都有一個 `scope`，在模型檔案、透過管理 API 或在資料表頁面上設定。三種範圍按其服務對象排序：`restricted`，然後 `domain`，然後 `public`。資料表的編輯者（資料表網域中的 `table_registration`）可以收窄欄位的範圍。放寬它——`restricted` 至 `domain` 或 `public`，`domain` 至 `public`——會把欄位提供給更多角色，並需要其授權所需的權利：資料表網域中的 `column_grant` 或 `access_config`。正在註冊的欄位以 `domain` 為基準衡量，因此把它註冊為 `public` 屬於放寬。對帶有敏感標籤的欄位，任何範圍變更都需要 `sensitive_data`。被拒絕的變更會指名欄位及權利。（REQ-1959、REQ-1943、REQ-1944）

| 範圍 | 提供給 |
| ------- | ----------- |
| `domain`（預設） | 涵蓋資料表網域的角色，按 `visible_to` 所述。空白的 `visible_to` 即每個這樣的角色。 |
| `public` | 同樣這些角色，以及並未涵蓋資料表網域的角色。 |
| `restricted` | 只限 `visible_to` 所指名的角色。空白的 `visible_to` 即沒有人。 |

發佈一個欄位只會為該欄位解除網域條件，僅此而已：

- 對網域以外的角色，資料表只會帶著其公開欄位提供。資料表的其他欄位，以及不發佈任何內容的資料表，都不在它的結構描述、目錄及 SQL 之中。
- 欄位本身的 `visible_to` 仍然決定提供給誰：只授予一個角色的公開欄位，只對該角色公開。
- 遮罩、資料列過濾、資料列上限及關係防護的套用方式與網域之內相同。角色沒有資料列過濾時，已發佈的欄位不經過濾讀取；資料表或其網域上的資料列過濾仍然適用。
- 關係在其兩端的欄位都被提供時才會提供。
- API 資料表的參數欄位（路徑或查詢引數）並非數據，也不會發佈任何內容。
- `ops` 網域要求逐個欄位明確授權（REQ-1133）；不論範圍如何，其中任何內容都不會跨網域提供。

該規則在每個介面上都相同，因為每個介面都讀取角色編譯後的結構描述：GraphQL、SQL（HTTP、pgwire、Flight、JDBC）、Cypher 及 Bolt、gRPC、REST 及 JSON:API、Flight 及 MCP 目錄，以及管理清單。並非這三個值之一的範圍，會在儲存欄位時被拒絕。

### 寫入授權 {: #write-grants }

讀取欄位由 `visible_to` 控制（為空表示所有角色都可以讀取）。寫入按資料表由 `write_grants` 控制，而不是按欄位。資料表預設沒有任何授權，因此任何角色都不能寫入它。（REQ-2074）

寫入授權聲明某個角色透過一種操作（`insert`、`update` 或 `delete`）可以向某張資料表寫入什麼：

| 欄位 | 用途 |
| ------- | --------- |
| `columns` | `INSERT` 可以列出、`UPDATE` 可以設定的欄位 |
| `rows` | `UPDATE` 或 `DELETE` 可以觸及的行（SQL 謂詞） |
| `check` | `INSERT` 寫入的每一行、或 `UPDATE` 留下的每一行必須滿足的條件（SQL 謂詞） |
| `presets` | 授權自己設定的欄位，來自工作階段變數、字面值或寫入時間。陳述式不得提供這些欄位。 |

省略 `rows` 或 `check` 時，授權採用該角色在這張資料表上的讀取行篩選器，因此角色寫入的就是它能讀到的行。`"TRUE"` 表示所有行。

同一個准入機制決定每個介面上的每次寫入，順序如下：角色持有 `write` 權利；持有資料表上該操作的授權；陳述式寫入的每個欄位都在授權中；加入預設；`rows` 以 AND 併入 `WHERE`；在向數據來源發送任何內容之前，先對新行評估 `check`。拒絕訊息會指明資料表、角色和操作。（REQ-2074）

更寬的陳述式需要更寬的授權。`TRUNCATE` 需要涵蓋所有行的 `delete` 授權。`MERGE` 需要涵蓋所有行的 `insert` 和 `update` 授權。`INSERT ... ON CONFLICT DO UPDATE` 需要涵蓋所有行且沒有預設的 `update` 授權。`COPY` 需要沒有預設的 `insert` 授權。

GraphQL 只為角色持有授權的操作提供變更欄位，欄位帶有授權的欄位。子角色繼承父角色的授權，自己的授權優先。元角色持有其成員授權的摺疊結果：欄位的聯集、`rows` 和 `check` 的 OR，以及只保留所有成員共有的預設。在管理介面中，資料表的**寫入**部分會列出並編輯它的授權。

### 範例 {: #example }

```yaml
tables:
  - source_id: hr-pg
    domain_id: people
    schema: public
    table: employees
    columns:
      - name: id
        visible_to: []                # all can read
      - name: email
        visible_to: [admin, analyst, viewer]
        masks:
          - to: [admin]               # admin sees raw value (no mask)
          - to: ["*"]                 # everyone else sees it masked
            mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}
      - name: salary
        visible_to: [admin, hr]
        masks:
          - to: [admin, hr]           # admin and hr see real value
          - to: ["*"]                 # all other visible roles see 0
            mask: {type: constant, value: 0}
      - name: created_at
        visible_to: []                # all can read
    write_grants:
      - role: admin
        operation: update
        columns: [email]
        rows: "TRUE"
      - role: hr
        operation: update
        columns: [salary]
      - role: hr
        operation: insert
        columns: [email, salary]
        presets:
          - {column: created_at, source: now}
```

在此範例中：

- `email`：admin 看到 `alice@example.com`，並可在任何行更新；analyst 和 viewer 看到 `a***@example.com`，不能寫入
- `salary`：admin 和 hr 看到真實值；hr 可以更新它在 hr 能讀到的行上的值，因為授權沒有聲明 `rows`；其他可見角色看到 `0`
- `created_at`：所有人都可讀取，沒有角色提供它，hr 的插入會把它設定為寫入時間

## 變更操作授權 {: #mutation-authorization }

已註冊的變更（遠端 GraphQL、OpenAPI、gRPC、Hasura）由兩項相互獨立的檢查把關。(REQ-868) 角色只有同時持有全域 `write` 能力，並且該指令透過 `visible_to` 分配給它（清單為空表示分配給所有角色），才可以呼叫變更。(REQ-868) 指令沒有 `writable_by` 清單；設定會按名稱拒絕它。[tool-verified: `provisa/security/mutation_authz.py` `command_assigned`、`admit_command`；`provisa/core/models.py` `_one_role_list`]

變更按契約而非呼叫者的聲明被歸類為寫入。（REQ-869） 參照變更類函數的 `SELECT` 會被提升為寫入，並接受同樣的雙重閘門檢查，因此呼叫者無法把變更偽裝成讀取來呼叫。（REQ-869） 把變更重新歸類為讀取安全需要 `access_config` 能力，並作為治理決定記錄；沒有按請求的例外。（REQ-870）

## 結構描述可見性 {: #schema-visibility }

每個角色的 GraphQL 結構描述會隱藏未獲授權的內容：（REQ-039）

- **網域存取**：角色只會看到其 `domain_access` 網域中的資料表（`"*"` = 全部）（REQ-039）
- **欄位可見性**：不在角色 `visible_to` 之中的欄位會從 SDL 中排除（REQ-039）
- 未獲授權的資料表/欄位不會出現在結構描述中（REQ-039）

## 行層級安全（RLS） {: #row-level-security-rls }

按資料表、按角色注入 SQL WHERE 子句。在編譯之後、執行之前套用。（REQ-041、REQ-263）

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```
過濾條件以 AND 附加至查詢的 WHERE 子句。適用於查詢及變更（UPDATE/DELETE）。（REQ-035、REQ-041）

`current_setting('provisa.<name>')` 項會從請求所綁定的工作階段變數解析：來自已驗證身份的 `user_id`、來自執行角色的 `role`，以及權杖中每個純量 claim，以其小寫名稱綁定，連字號換為底線。Hasura 工作階段變數保留自己的名稱：`X-Hasura-User-Id` 是 `hasura_user_id`，讀自權杖的 `x-hasura-user-id` claim（在頂層，或在 Hasura claims 物件內，如 `https://hasura.io/jwt/claims`），或作為屬性 `hasura_user_id` 記錄在成員名下。因此匯入的、以 `X-Hasura-User-Id` 為基礎的 Hasura 過濾會讀取 `provisa.hasura_user_id`。`provisa.user_id` 一律是已登入的身份，任何 claim 都不會取代它。角色所設定的 `session_vars` 是基礎常數；請求的綁定疊加於其上。從另一個角色繼承的角色，會把祖先的常數放在自己的常數之下，每個名稱取最近角色的值。`user_id`、`role`、`region` 和 `org` 由請求或節點繫結，不能設定為常數。在不安全模式下，標頭 `x-provisa-session-<name>` 綁定 `<name>`。任何地方都沒有綁定的變數解析為 NULL，不會符合任何資料列。（REQ-1682）

角色常數可以是 `${env:NAME}` 或 `${secret:NAME}` 參照，在查詢使用它時透過與其他所有參照相同的解析器解析；模型保留參照本身，從不保留解析後的值，無法解析為任何內容的參照會使查詢失敗，並指明該常數。解析後的常數會參與查詢：Provisa 的語句記錄（查詢審計日誌、追蹤、EXPLAIN）以及聯邦資料來源自身的日誌都可能顯示它，因此不要使用不能在那裏被看到的值。建議使用 `${env:...}` 形式。基於這個原因，透過管理 API 或角色編輯器儲存角色時，會按名稱拒絕 `${secret:...}` 常數。按原樣載入的設定檔不會被檢查：在那裏把機密參照寫入角色常數的人自行承擔這一責任。（REQ-2086）

## 欄位層級遮罩 {: #column-level-masking }

遮罩以每個欄位一份有序清單的形式設定。讀取者所持有的第一條項目決定其看到的內容。（REQ-2059、REQ-263）

| 遮罩類型 | 支援的欄位類型 | SQL 運算式 |
| ----------- | ----------------------- | --------------- |
| `regex` | 字串（varchar、char、text） | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | 任何 | 字面值（`NULL`、`0`、自訂字串） |
| `truncate` | 日期/時間戳記 | `DATE_TRUNC(precision, col)` |

每條項目都有一份 `to` 清單，指名其所針對的讀取者，以及一個可選的 `mask`。沒有 `mask` 的項目對這些讀取者表示「不遮罩」。`to` 中的 `*` 符合任何讀取者。

遮罩運算式會被下推至 SQL SELECT 投影。資料庫傳回遮罩後的值；對於遮罩角色，未遮罩的數據從不經過網絡傳輸。（REQ-263） 遮罩欄位在 `WHERE` 及 `HAVING` 子句中亦會被阻擋（第 5 層謂詞防護），以防止透過過濾推斷值。（REQ-263、REQ-531）

**遮罩涵蓋欄位的每一個名稱，但不涵蓋關聯欄位。** 以別名或實體名稱讀取的欄位會以相同方式遮罩。遮罩不會隱藏已核准關係與角色可見欄位連接的鍵：連接比較的是真實值，因此關聯欄位會顯示被遮罩鍵的內容。請同時遮罩關聯欄位，或不要為該角色核准此關係。（REQ-263、REQ-019）

**沒有項目，便不遮罩。** 不持有任何項目的讀取者會讀取未遮罩的欄位。當每個讀取者都應受到某種遮罩時，請以一條 `*` 項目結束清單。清單沒有 `*` 項目時，欄位編輯器會發出警告（`unlisted_unmasked`）。

**按次序拒絕。** 即使讀取者亦持有後面一條較寬鬆的項目，前面的項目仍會優先。要令承包商不論還有哪些其他角色都一律看到遮罩的薪酬，請把承包商項目放在不遮罩項目之前。授予角色從不失去父角色透過繼承給予的內容，但在清單中位置較前的遮罩項目會先套用——這就是這裏限制的方式。

**元角色。** 元角色的鏈包括每個成員的角色、它們的祖先，以及所有被指名的派生角色。授予任何成員的項目都會觸及元角色。另一個成員所持有、排在其前面的項目仍會先套用。

**存取檢查。** `POST /admin/masks/check` 接受一份遮罩清單及可選的角色集合，傳回對這些角色套用的項目，以及任何讀取者都無法觸及的項目。受 `masking_config` 保護。

```yaml
# email column: analyst+region__eu sees raw; everyone else sees masked
masks:
  - to: [analyst+region__eu]          # composite: analyst AND in EU region
  - to: ["*"]                         # all others
    mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}

# hire_date column: hr sees raw; all other visible roles see year only
masks:
  - to: [hr]
  - to: ["*"]
    mask: {type: truncate, precision: year}
```
## 治理操作回應 {: #governing-action-responses }

被追蹤函數或 webhook 的回應，會如同資料表的資料列一樣受治理。（REQ-1679） 操作傳回的資料列會被綁定為一個關係，其欄位是該操作所宣告的輸出契約（函數的 `output_columns`、webhook 的 `inline_return_type`，或函數陣列型 `return_schema` 的物件屬性），而該關係會透過帶有這些資料列的 VALUES CTE，經過與每次資料表讀取相同的治理階段。（REQ-1679） 行層級安全、遮罩或可見性都不會為操作重新實作。

有三件事適用，次序與資料表相同。（REQ-1679）

- 資料列過濾：為該操作按名稱儲存的角色行層級安全規則（帶 `actionName` 的 `upsertRlsRule`），或在操作沒有自己的規則時，操作所在網域的網域規則。儲存時會如同資料表規則一樣對照契約檢查謂詞。（REQ-1676）
- 欄位可見性：宣告了 `visible_to` 的契約欄位，會對清單沒有指名的角色移除；沒有宣告任何內容的欄位屬於操作的公開形態，並會保留。
- 遮罩：契約欄位攜帶 `masks`，即與資料表欄位相同的有序遮罩清單。（REQ-2059）

角色繼承為三者解析繼承鏈。（REQ-1677） 傳回宣告契約以外欄位的回應會以 502 被拒絕，而不是不受治理地通過。沒有宣告欄位契約的操作傳回純量，沒有可綁定的內容；它會按原樣傳回。管理端的測試呼叫會報告所套用的過濾、排除及遮罩，受治理的陳述式會記錄於查詢審計記錄中。

## 取樣 {: #sampling }

除非擁有 `full_results` 能力，所有角色看到的都是取樣結果（預設：100 個資料列）。（REQ-554） 由環境變數 `PROVISA_SAMPLE_SIZE` 控制。（REQ-554）

## 審計記錄 {: #audit-logging }

每個觸及網域資產的查詢，都會記錄於 `query_audit_log`。（REQ-596、REQ-613） 審計記錄屬於組織，而不屬於平台：平台本身的組織、使用者及邀請註冊表不保存審計條目。（REQ-1922） 每個組織的 `query_audit_log` 是該組織自己的 schema 中的一張表，位於該組織為其在所選每個區域中的記錄所指定的儲存中。未宣告任何區域的組織，會將該 schema 連同其模型一併保存在部署的控制平面資料庫中。

每一列記錄 `tenant_id`（組織）、`user_id`、`role_id`、查詢文字的 SHA-256 雜湊、加密後的查詢文字本身（REQ-689）、`table_ids` 及 `table_names`、`source`、`status_code`、`duration_ms`、路由及列數、對該陳述式實施了哪些強制措施，以及 `logged_at`。組織設定了金鑰時，查詢文字以組織的金鑰加密，否則以部署的金鑰加密（REQ-1574）。未設定加密提供者的部署，以未加密形式儲存。該文字從不暴露於可查詢的營運表中，只會在獲授權的管理讀取中解密。

該記錄在資料庫層面只可附加：PostgreSQL 規則將針對 `query_audit_log` 的 `DELETE` 及 `UPDATE` 變為空操作。（REQ-596、REQ-613） 兩個索引——`(tenant_id, logged_at)` 及 `(user_id, logged_at)`——支援按組織及按使用者在時間範圍內的合規查詢。（REQ-596、REQ-613）

保留期：列會被無限期保留。Provisa 沒有保留期設定，亦從不清除審計記錄；由於 `DELETE` 是空操作，刪除舊列意味着由營運人員在 Provisa 之外刪除或封存該表的資料。

## 速率限制 {: #rate-limiting }

每個角色的速率限制在 `provisa.yaml` 中設定：每秒最多查詢數、最多同時 SSE 訂閱數，以及最多同時 Arrow Flight 串流數。（REQ-369） 限制在 API 層、編譯或執行之前執行；超出限制的請求會以 HTTP 429 及 `Retry-After` 標頭被拒絕。（REQ-369）

自然語言查詢服務（`POST /query/nl`）透過 `nl.rate_limit`（每個角色每分鐘請求數）有獨立的限制。超出限制的請求會在呼叫任何 LLM 之前被拒絕。（REQ-370）

速率限制狀態以滑動視窗計數器保存於 Redis（`cache.redis_url`）——不在單一執行個體內保存狀態——因此限制會在所有水平擴展的 Provisa 執行個體上執行。（REQ-371）

## 查詢複雜度上限 {: #query-complexity-limit }

速率限制約束角色傳送多少請求。複雜度上限約束單一陳述式可以要求甚麼。（REQ-1174）

每條陳述式都有一個複雜度分數：它讀取的關係、它的聯結、它選取的欄位（`*` 按其所代表的欄位計算）以及巢狀於其中的查詢。從遠端 API 數據來源（OpenAPI、遠端 GraphQL、遠端 gRPC）讀取的關係計為 10，因為每次讀取都會耗用遠端系統為該數據來源所有使用者所設定的預算。

上限是角色 `rate_limit` 中的 `max_query_complexity`。組織範圍的設定 `limits.max_query_complexity` 是每個角色的上限：角色可以設定較低的上限，較高的上限不會生效。預設兩者都沒有設定。

```yaml
roles:
  - id: analyst
    rate_limit:
      max_query_complexity: 200
```
分數在查詢剖析之後、治理或執行之前量度，因此同一個上限適用於陳述式可能到達的每個介面：GraphQL、HTTP 上的 SQL、pgwire、Arrow Flight、gRPC、Cypher、JSON:API 及 MCP。超出上限的陳述式會被拒絕，並附上其分數、上限及它所要求的內容。HTTP 介面回應 413。該拒絕會作為政策拒絕記錄。

## 身份驗證 {: #authentication }

可插拔的身分驗證提供者：（REQ-120）

| 提供者 | 憑據 | 情境 |
| ---------- | ----------- | ---------- |
| `none` | X-Provisa-Role 標頭 | 只限開發 |
| `basic` | 本機帳戶（bcrypt）+ 工作階段 JWT | 自行託管的部署 |
| `firebase` | Firebase ID 權杖 | 正式環境 |
| `keycloak` | Keycloak JWT | 企業 |
| `oauth` / `oidc` | OIDC JWT | PingFed、Okta、Azure AD、Auth0 |
| `ldap` | 目錄使用者名稱及密碼（LDAP 或 Active Directory） | 企業目錄（REQ-1265） |
| `saml` | SAML 2.0 簽署斷言，然後是工作階段 JWT | 企業單一登入（REQ-1265） |
| `simple` | bcrypt + JWT | 測試 |

`ldap` 採用先搜尋後繫結的方式讓使用者登入：Provisa 以服務帳戶繫結，找到使用者名稱所指向的那一個條目，再以該條目及使用者提交的密碼進行繫結，由目錄來驗證密碼。使用者所屬的群組成為其角色。每次工作階段權杖返回時，都會重新讀取帳戶及其群組，因此從目錄或從某個群組中移除的使用者，會在權杖過期之前失去相應存取權。`ldaps://` 及 StartTLS 均受支援。

`saml` 是由 SP 發起的 Web Browser SSO。回應的 XML 簽章依據身分識別提供者的中繼資料驗證，claim 只從已簽署的元素中讀取。Issuer、Audience、ACS URL、有效期及 `InResponseTo` 均會檢查。未經請求（由 IdP 發起）的回應、加密斷言、帶有 DOCTYPE 的文件、多個斷言及 SHA-1 簽章均會被拒絕。SAML 工作階段不會再向身分識別提供者覆核：在該處被移除的使用者，在工作階段權杖過期之前仍保有存取權。

角色對映：身份 claim → 透過可設定規則對映為 Provisa 角色。（REQ-120） `assignments_source` 欄位控制角色指派的來源：`claims` 從 JWT 權杖 claim 讀取（預設），`provisa` 從 Provisa 內部的指派儲存讀取。（REQ-551） 多租戶部署（`multitenancy: true`）按組織自行指派角色，因此必須使用 `assignments_source: provisa`：使用 `claims` 時它不會啟動，會導致這種組合的設定變更亦會被拒絕。單一租戶部署可以使用任一選項。

在 `provisa.yaml` 中設定的超級使用者（使用者名稱加上來自環境密鑰的密碼），不論設定了哪個提供者，都一律取得 admin 角色及所有能力——這是初始設定的引導路徑。（REQ-125）

### 各介面與憑證 {: #surfaces-and-credentials }

每個介面都透過同一個提供者契約進行身分驗證，因此在一個介面上可用的憑證，在協定能攜帶它的所有介面上都可用。（REQ-124、REQ-1263） 此表是唯一的參考；各介面的文件不會重複。

| 介面 | 密碼 | 提供者權杖 | 個人存取權杖 | 用戶端憑證（mTLS） |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP（REST、JSON:API、GraphQL） | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | 透過終止代理 |
| pgwire | 密碼欄位（明文或 SCRAM） | 密碼欄位，OIDC 部署 | 密碼欄位 | 是 |
| Bolt | `basic` 方案 | `bearer` 方案 | `bearer` 方案 | 是 |
| Arrow Flight | — | 交握或 ticket 負載中的 `token`；`list_flights`、`get_flight_info` 及 `get_schema` 使用呼叫標頭 `authorization: Bearer` | 同上 | 是 |
| gRPC | — | `authorization` 中繼資料 | `authorization` 中繼資料 | 是 |
| Airport（DuckDB `airport` 擴充，可選） | — | `authorization: Bearer`（密鑰的 `auth_token`） | 同上 | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | 透過終止代理 |

DuckDB `airport` 擴充唯一的身分驗證選項是其密鑰的 bearer `auth_token`，因此 DuckDB 用戶端在那裏出示提供者權杖或個人存取權杖，並以其身份所解析到的角色執行。能夠設定呼叫標頭的用戶端，可以在 `x-provisa-role` 中要求一個已持有的角色或逗號分隔的集合。（REQ-1120、REQ-1263）

在這些介面上，拒絕都會報告其類別。缺少、被拒絕或無法使用的憑證是身分驗證失敗：HTTP 401、gRPC `UNAUTHENTICATED`、Arrow Flight 及 Airport `Unauthenticated`。有效的憑證要求以身份並不持有的角色執行，是授權失敗，並會指名該角色：HTTP 403、gRPC `PERMISSION_DENIED`、Arrow Flight 及 Airport `Unauthorized`。（REQ-273、REQ-1263） 有一個例外位於 Arrow 用戶端程式庫而非伺服器：Flight `get_schema` 呼叫的拒絕，會以帶有伺服器原因、但不帶其類別的一般 Arrow 錯誤到達 Python 用戶端；`list_flights`、`get_flight_info` 及 `do_get` 會傳遞類別。

儲存格為 `—` 之處，協定不攜帶可以附加密碼的使用者名稱欄位；權杖形式涵蓋了這種情況。pgwire 則相反：啟動封包中只有一個密鑰欄位且沒有方案，因此方法按密鑰*是甚麼*來選擇：PAT 按前綴識別；當設定的提供者是權杖提供者時，密鑰會作為 bearer 權杖讀取；其他一切都是密碼。選擇只做一次——被所選驗證器拒絕的憑證，不會再由另一個驗證器檢查。

該矩陣由 `tests/unit/test_auth_surface_conformance.py` 執行，它呼叫每個介面真正的身分驗證進入點，若新增介面而沒有對應的列便會失敗。

### 個人存取權杖 {: #personal-access-tokens }

PAT 是使用者為無法完成互動式登入的用戶端（指令碼、BI 工具、驅動程式）建立的長效 bearer 密鑰。（REQ-1263） 它攜帶自己的組織及角色，每個介面都透過同一個驗證器解析它，因此任何介面都不需要知道 PAT 是甚麼。

線路形式為 `provisa_pat_` 後接 43 個 URL 安全的 base64 字元。該前綴會把出示的密鑰導向權杖儲存而不是身分識別提供者，並令外洩的權杖可以在記錄及程式碼儲存庫中被找到。

- **儲存**——只保存密鑰的 SHA-256。密鑰本身只在建立時顯示一次，且無法復原。清單攜帶顯示前綴及生命週期時間戳記，但從不攜帶有效憑證。
- **簽發及撤銷**——`POST /auth/tokens`、`GET /auth/tokens`、`DELETE /auth/tokens/{token_hash}`，以及管理介面中使用者本身個人檔案內的自助區域。建立及撤銷憑證是權杖擁有者的操作。
- **歸屬**——經驗證的 PAT 會解析為其擁有者的帳戶：使用者 id、電郵及顯示名稱。因此在 PAT 之下寫入的審計列或用量報告，指名的是人，而不是憑證。該人的哪個權杖在執行，會另外放在 `raw_claims["token_name"]` 中傳遞。
- **到期**——權杖可以設定到期時間；已到期的權杖在驗證時會被拒絕。移除使用者的成員資格會一併撤銷其權杖。

### pgwire 上的 SCRAM-SHA-256 {: #scram-sha-256-on-pgwire }

使用 `basic` 提供者時，設定 `auth.scram: true` 會令 pgwire 以 `SCRAM-SHA-256` 機制提供 SASL（身分驗證代碼 10），從而證明密碼而不是傳送密碼。（REQ-1394） 不提供通道綁定（`SCRAM-SHA-256-PLUS`）。

SCRAM 需要 RFC 5802 驗證器，而它無法從 bcrypt 雜湊推導。每當密碼以明文經過時——註冊、登入、更改密碼、管理員重設——都會寫入驗證器，因此啟用 SCRAM 的部署會在使用者下一次進行身分驗證時收集驗證器，每個使用者的第一次 SCRAM 連線，會在其下一次輸入密碼之後出現。尚未有驗證器的使用者，會得到一次與真實交換無法區分的模擬交換，因此網絡不會揭露誰已經遷移。

### 相互 TLS {: #mutual-tls }

用戶端憑證驗證把第一道檢查移至 TLS 交握：沒有由部署的 CA 簽發之憑證的呼叫者，永遠到達不了憑證層。（REQ-1228） 它在 pgwire、Bolt、gRPC 及 Arrow Flight 上可用——這四種傳輸自行終止其 TLS。

| 變數 | 含義 |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | 獲准簽發用戶端憑證的 CA 的 PEM 套件 |
| `PROVISA_MTLS_MODE` | `required`（一旦設定 CA 便為預設）或 `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | 為 true 時，憑證的通用名稱必須與連線其後進行身分驗證所用的使用者名稱相符 |

按協定的覆寫遵循與 TLS 設定相同的命名。不會作任何推斷：沒有 CA 卻設定了模式會拒絕啟動，無法辨識的模式亦會拒絕啟動，而不是被讀作最安全的相鄰值——一個以為自己要求用戶端憑證但實際並沒有要求的部署，比一個不啟動的部署處境更差。

### TLS 版本 {: #tls-versions }

Provisa 終結 TLS 的每個監聽器（HTTP API 及 UI、pgwire、Bolt、MCP、gRPC 及 Arrow Flight）都接受 TLS 1.2 及 TLS 1.3，並拒絕 TLS 1.0 及 1.1。Provisa 本身不設定協定版本或加密套件清單。HTTP、pgwire 及 Bolt 監聽器使用 Python `ssl` 模組的伺服器預設值（最低 TLS 1.2）。gRPC 及 Arrow Flight 使用 gRPC 的預設值（最低 TLS 1.2）。這些版本不可設定。

### 登入節流 {: #login-throttling }

猜測密碼與協定無關：同一個帳戶可以經由 HTTP、pgwire 及 Bolt 受到攻擊。因此計數器位於憑證驗證層，而不是任何單一介面上，從而在任何地方觸發的鎖定，在所有地方都有效。（REQ-1393）

它預設啟用——五分鐘內失敗五次會把主體鎖定十五分鐘——並可在 `auth.login_throttle` 中設定。被鎖定的主體會在憑證被檢查之前便被拒絕，成功的身分驗證會清除該主體的歷史。

鍵是協定所攜帶的主體。只有 bearer 的介面不攜帶主體，因此鍵是憑證本身的摘要；這可防止無限制地重播錯誤的權杖。儲存位於程序之內，因此有多個 API worker 的部署，容許每個 worker 最多 `max_attempts` 次——該節流是減慢猜測，而不是分散式配額。

### 角色探測 {: #role-probing }

指名呼叫方並不持有的角色，會在每個介面上被視為已偵測到的高嚴重性安全事件：HTTP API、MCP、pgwire、Bolt、Arrow Flight、airport Flight 服務和 gRPC。合法的用戶端只指名分配給它的角色；指名其他角色的用戶端不是出了故障，就是在探測。（REQ-2076）

請求被拒絕。呼叫方只會得到 "not permitted"，不會得知該角色是否存在；有連線或工作階段的地方，連線或工作階段隨之結束：pgwire 連線、Bolt 工作階段、gRPC 呼叫、MCP 工作階段。唯一的例外是 HTTP API 上已登入的工作階段，它保留指名該角色的拒絕訊息。

其餘處理取決於憑證和傳輸方式：

| 憑證 | 傳輸方式 | 暫停對象 |
|---|---|---|
| 個人存取權杖 | 任意 | 該權杖 |
| 由外部身分提供者簽發的權杖 | MCP、pgwire、Bolt、Flight、gRPC | 該主體對組織的權杖存取 |
| 由外部身分提供者簽發的權杖 | HTTP API | 無 |
| 由本部署自行驗證的工作階段或密碼 | 任意 | 無 |
| 本機 MCP 工作階段（無 bearer） | MCP | 無；事件寫入營運方日誌 |

HTTP API 之所以例外，是因為瀏覽器出示的 bearer 權杖與 API 用戶端相同，伺服器無法區分兩者；而分頁仍指名使用者已失去的角色（在管理員更改其角色後，或切換組織後）是常見情況。MCP 及其他傳輸方式從來不是瀏覽器工作階段。在那裏，事件會像其他事件一樣被拒絕、記錄並通知；只是不執行暫停。

被暫停的權杖會像已撤銷的權杖一樣被拒絕。權杖存取被暫停的主體，在該組織中透過 MCP、pgwire、Bolt、Flight 和 gRPC 的請求都會被拒絕，但仍可登入介面。只有管理員能解除暫停；暫停不會因時間屆滿而解除。團隊 → 已暫停的存取 會列出每項暫停及其起因事件，並可將其解除（`GET /admin/orgs/{org}/suspensions`、`POST /admin/orgs/{org}/suspensions/lift`）。

每個事件都會寫入組織的管理日誌（操作 `security.role_probe`），包括主體、憑證 ID、用戶端位址、傳輸方式、端點或工具、所指名的角色、所持有的角色、時間，以及該主體最近經過審計的語句的 ID。語句文字保留在查詢審計日誌中，需具備 `view_governance` 權限才能讀取。

在部署會發送電郵的情況下，組織管理員會收到電郵。設定了 `security.webhook_url` 時，事件亦會以 JSON 傳送到營運方的安全 webhook。離開組織的內容包括主體、憑證 ID、傳輸方式、端點、所指名的角色、時間和嚴重性，並附有指向管理檢視的連結；不包含任何語句，亦不包含所持有的角色。無法送達的通知會被記入日誌，並且絕不會撤銷拒絕或暫停。

### 指名組織 {: #naming-the-org }

在多租戶模式下，每個請求都會指名其目標組織。只屬於一個組織並不等於指名了它：沒有指名任何組織的請求會被拒絕，拒絕會說明應傳送甚麼。（REQ-1235） 各介面指名組織的方式如下：

| 介面 | 用戶端如何指名組織 |
| ------- | -------------------------- |
| HTTP（介面、REST、GraphQL） | 組織本身的主機名稱 `acme.provisa.dev`，或在控制平面主機上使用 `X-Org-Provisa` 標頭 |
| pgwire | 資料庫名稱（`psql -d acme`、BI 工具的資料庫欄位），或透過 TLS 撥號的組織本身的主機名稱 |
| Bolt | 資料庫名稱 `<org>.provisa_<role>`（或 `<org>.provisa_ops_<role>`），或透過 TLS 撥號的組織本身的主機名稱 |
| Arrow Flight | ticket 中的 `"org"` |
| gRPC | `x-provisa-org` 中繼資料標頭 |
| MCP（HTTP 傳輸） | 組織本身的主機名稱或 `X-Org-Provisa` 標頭 |
| Python 用戶端 | `ProvisaClient`、`connect()` 及 `adbc_connect()` 中的 `org=`；SQLAlchemy URL 中的 `?org=` |

透過 TLS 撥號主機名稱的 pgwire 用戶端，會在 TLS ClientHello 中傳送該主機名稱，Provisa 從中讀取組織（REQ-1234）。當主機名稱及資料庫名稱都指名了組織而兩者不同時，連線會被拒絕；兩者都不優先。預設使用的資料庫名稱 `provisa` 並不指名組織，連接至某組織的 BI 工具會把
該組織顯示為其資料庫。在 Bolt 上，`SHOW DATABASES` 會為使用者所屬的每個組織
列出一個 `<org>.provisa_<role>`；在多租戶模式下，單獨的 `provisa_<role>` 會被拒絕，工作階段會保持在
它最先指名的組織之中。

名稱是請求，而不是授權。在任何地方它都會到達同一個解析器，該解析器會拒絕已驗證主體並非其成員、且對其沒有跨組織權利的組織。

個人存取權杖為一個組織簽發，並自行指名該組織。它只開放該組織：指名另一個組織的請求會被拒絕，即使權杖擁有者同時屬於兩者。

單一租戶部署沒有需要指名的組織。在那裏，沒有任何介面會為此讀取主機名稱、資料庫名稱或標頭。

## 跨組織的密鑰 {: #secrets-across-organizations }

組織的密鑰只由該組織讀取。在 Provisa 本身的儲存中，每個密鑰都存放於其組織之下。在承載多個組織的部署的中央服務中（Vault、AWS Secrets Manager、GCP Secret Manager、Azure Key Vault），每次讀取都在讀取方組織的前綴之內進行，會離開該前綴的參照會被拒絕。（REQ-1557） 前綴，以及把現有密鑰遷移至前綴之下的步驟，請參閱[密鑰](secrets.md#one-organizations-secrets-in-a-central-service)。

## 跨組織的遙測 {: #telemetry-across-organizations }

在許多組織共用的部署上，維運訊號（`"ops"."traces"`，在 Trino 引擎上還有旁邊的指標和日誌）是所有組織的工作都寫入的同一個儲存。每個 span 記錄啟動它所服務的組織，儲存為 `tenant_id`；對這些訊號的每次讀取只傳回讀取方組織的列，與角色無關。在任何組織的工作之外啟動的 span（例如伺服器啟動時）不屬於任何組織，也沒有組織會讀到它。平台管理員在任何組織中都沒有數據存取權，也不透過 Provisa 讀取其中任何一個。(REQ-1349, REQ-1327)

維運訊號在兩種引擎上都會無限期保留，與稽核日誌一樣：壓縮會重寫存放它們的檔案，但不刪除任何列。在 Trino 上，它們是 `otel` 目錄的 Iceberg 資料表。在原生引擎（DuckDB）上，伺服器把自身的追蹤記錄以 Parquet 檔案寫入其遙測資料夾，引擎以同一名稱 `"ops"."traces"` 讀取它們，並在其上提供重新整理血緣檢視 `"ops"."mv_refresh_lineage"`；兩者都按同一條按組織劃分的規則讀取。(REQ-862)

訊號在其儲存中以明文保存。需要在一個部署內部比按組織劃分更強邊界的工作負載，在專用部署上執行；高度敏感或受監管的工作負載則自行託管。

## 部署加固 {: #deployment-hardening }

### 儲存區憑據與儲存區的監聽位置 {: #store-credentials-and-where-the-stores-listen }

內建儲存區沒有預設憑據。PostgreSQL 控制平面、Redis、MinIO、Grafana 管理員帳戶，以及 Trino 叢集的共用密鑰，各自使用為該部署產生的值：

- 啟動器（`provisa start`、桌面版與伺服器安裝程式）產生一次，並保存於 `~/.provisa/store-credentials.env`（權限 600）。環境中已有的值按原樣使用；使用 primary 儲存區的 secondary 節點，在沒有 primary 的值時拒絕啟動。
- Terraform 模組以 `random_password` 產生它們，並把相同的值交給每個節點。它們保存於 Terraform 狀態中，因此應如保護密碼本身一般保護該狀態。
- Helm chart 不設定任何值。它需要你建立一個 Secret，由 `credentials.existingSecret` 指定，包含 `pg-password`、`redis-password`、`minio-access-key`、`minio-secret-key`、`grafana-admin-password` 與 `trino-shared-secret`。

Redis 要求提供密碼，PostgreSQL 角色的密碼在每次啟動時重新設定，使先前建立的磁碟區跟隨部署目前的密碼。儲存區、聯邦引擎與遙測服務在同一個位址 `PROVISA_STORE_BIND_IP` 上發佈連接埠：單獨節點上為 `127.0.0.1`，secondary 連線至 primary 時則為 primary 的私有位址。Grafana 管理員帳戶使用產生的密碼；compose 堆疊允許匿名訪客檢視儀表板，而 Helm chart 不接納匿名訪客。

### 傳輸加密 {: #transport-encryption }

Provisa 終結的每個監聽器都提供 TLS（參見 [TLS 版本](#tls-versions)）。伺服器節點使用 `PROVISA_TLS_CERT`/`PROVISA_TLS_KEY` 中的憑證，或產生自簽憑證。Helm chart 預設提供 TLS（`tls.enabled: true`），憑證來自 `tls.secretName` 所指定的 `kubernetes.io/tls` Secret。

### 內嵌 PostgreSQL {: #the-embedded-postgresql }

桌面與原生層以內嵌方式執行 PostgreSQL。在 macOS 與 Linux 上，它監聽其資料目錄內的 unix socket（權限 700），只有擁有該目錄的帳戶才能存取。透過 TCP（Windows 上，監聽 127.0.0.1）時，每個連線都必須提供為該資料目錄產生並保存於其中的密碼（`provisa.password`，權限 600）。它從不以 root 身分啟動。

### 聯邦引擎（Trino） {: #the-federation-engine-trino }

- **節點之間。** 協調器與其工作節點使用為部署產生的共用密鑰（`internal-communication.shared-secret`）互相驗證。加密它們之間的流量是營運者的選項，預設關閉：關閉時，流量保持在部署的私有網絡或叢集之內。當合規或零信任要求部署內部也須加密時再開啟。Trino 文件指出，它會使 shuffle 密集的查詢（分散式連接、聚合、視窗函數）變慢 10% 至 100% 以上。在 compose 部署上，它是設定 `engine.trino_internal_tls`（Deployment settings → Engine），旁邊是引擎網絡與協調器的固定位址，並於下次啟動時生效。在 Helm chart 下，它是值 `trino.internalTls`；chart 產生節點使用的 CA 和證書，在升級之間保留它們，並按每個節點的 pod DNS 名稱定址。託管平台的引擎是單節點協調器，沒有節點間流量，並拒絕該選項。（REQ-2072）
- **暫存資料。** 容錯執行以每個查詢產生、查詢結束即丟棄的金鑰，加密它暫存的交換資料（`fault-tolerant-execution.exchange-encryption-enabled=true`，明確設定）。不使用舊式的溢寫至磁碟。
- **同一引擎上的多個組織。** 每個組織的 catalog 屬於它自己，以該組織命名，每個組織的陳述式在它自己的資源群組中執行。
- **目錄及其中的憑證只存在於引擎記憶體中。** Provisa 在執行時建立每個 Trino 目錄，每個目錄保存其數據來源的連線屬性，包括密碼。Provisa 執行的每個引擎都把它們保存在記憶體中（`catalog.store=memory`）：不會有目錄寫入引擎磁碟，也不會從檔案掛載目錄。重新啟動的協調器回來時沒有任何目錄；每個 Provisa 程序會在下一次查詢時發現，引擎監看器則在一分鐘內發現，並重新建立它所服務的目錄。帶 SASL 憑證的 Kafka 數據來源需要一個檔案：Trino 的 Kafka 連接器只從檔案讀取用戶端設定，因此 Provisa 把它們（包括密碼）寫入 `kafka-client/<catalog>.properties`。該檔案只有 Trino 的使用者可讀（0700 資料夾中的 0600 模式）。在容器堆疊中，該資料夾是 API 與引擎共用的記憶體磁碟區。在託管平台的引擎上，它是引擎自己的 Kubernetes Secret，保存在節點記憶體中，只有 Trino 的使用者群組可讀（0440 模式）。無論哪種方式，該檔案都不會落到磁碟上。在託管引擎上，Kafka 數據來源必須指定結構描述登錄：主題欄位只由模型宣告的數據來源在儲存或套用時會被按名稱拒絕，因為它的資料表描述需要一個無法交給執行中引擎的資料夾。啟動堆疊時會刪除早期版本留在磁碟上的目錄檔案和 Kafka 用戶端檔案。

### 共用部署上的數據來源 {: #sources-on-a-shared-deployment }

在承載多個組織的部署上，組織的數據來源只能存取公開位址：絕不能存取平台的私有網絡，那裏有它的控制平面、快取與物件儲存。該檢查在儲存數據來源時進行（無論經由 UI、管理 API 或設定套用），並在每次連線與每次 HTTP 重新導向時再次進行。示範沙箱的數據來源還被限定在檢查時的位址。部署本身的組織（其數據來源由營運者設定）不受限制。

## 靜態加密與組織自有金鑰 {: #encryption-at-rest-and-the-organizations-own-keys }

使用組織自有金鑰的加密是可用的，在組織開啟之前處於關閉狀態。除非組織管理員建立了金鑰，否則不會有任何內容在組織的金鑰下加密。

**組織加密金鑰**（REQ-1574）包裹組織的密鑰、其加密欄位及其稽核查詢文字。組織管理員在 **Admin → Org Secrets → Org Key** 下建立它，或使用 `PUT /admin/org-encryption`：不帶主體時 Provisa 產生該金鑰且從不顯示，帶 `key_b64` 主體則提供你自己的 32 位元組。`GET /admin/org-encryption` 只回報其指紋。沒有組織金鑰時，組織的數據以部署的金鑰（`provisa.yaml` 中的 `encryption.provider`）包裹。未設定加密提供者的部署，會以未加密形式保存加密欄位與稽核查詢文字；密鑰絕不以明文保存（參見[密鑰](secrets.md#provisas-built-in-store)）。

**組織的資料夾**（REQ-1992）存放其上傳與落地的檔案、其引擎的溢寫檔案及其暫存檔案。組織管理員在 **Admin → Org Secrets → File folder encryption** 下開啟其加密，或使用 `PUT /admin/org-files`（`{"encrypted": true, "key_reference": "${secret:NAME}"}`）。`POST /admin/org-files/generate-key` 一次性回傳新的 256 位元金鑰，供存入保管庫。金鑰保留在組織自己的保管庫中（AWS Secrets Manager、GCP Secret Manager、Azure Key Vault 或 HashiCorp Vault）：Provisa 只保存參照，gocryptfs 以該金鑰使資料夾中的檔案在磁碟上保持加密。這需要 FUSE，即帶有 `/dev/fuse` 的 Linux 主機或容器；在 macOS 上該設定會被拒絕。

在通常以自有引擎執行一個組織的自行託管部署上，同時開啟兩者即以該組織的金鑰加密其密鑰、檔案、溢寫檔案與暫存檔案：

- 組織執行的 DuckDB 引擎溢寫至組織的資料夾。
- 列篩選器擷取的列與上傳的程式碼暫存於組織的資料夾中。
- Trino 的容錯執行以每個查詢產生、查詢結束即丟棄的金鑰，加密暫存的交換資料（`fault-tolerant-execution.exchange-encryption-enabled=true`）。Provisa 不開啟 Trino 舊式的溢寫至磁碟。
- Trino 把組織的目錄連同其憑證保存在記憶體中，不向磁碟寫入任何目錄（`catalog.store=memory`）；Kafka 數據來源的 SASL 用戶端設定位於只有 Trino 使用者可讀的檔案中，在容器堆疊中位於記憶體（參見[聯邦引擎](#the-federation-engine-trino)）。
- PostgreSQL 聯邦引擎把暫存檔案寫在該 PostgreSQL 伺服器保存它們的位置，處於伺服器本身的儲存加密之下，而不是組織的資料夾中。

### 共用與專用託管平台 {: #shared-and-dedicated-hosted-platforms }

共用託管平台在邏輯上隔離各組織。一個部署服務所有組織，每個組織的數據透過治理、引擎上自己的 schema、catalog 與資源群組、自己的金鑰、暫存引擎數據的每查詢金鑰，以及拒絕其數據來源存取平台私有位址，而始終歸其自己所有。建議用於試驗與小型企業。專用託管平台在物理上隔離：擁有自己的機器、網絡與儲存區，服務一個客戶。對於高度敏感或受監管的工作負載，請在你自己的邊界內自行託管 Provisa，由你自己的維運人員掌握每一把金鑰和每一台主機。託管平台目前沒有 SOC 2 報告。我們正在評估 SOC 2，並會提供進展。如果你需要 SOC 2，我們建議自行託管。

### 瀏覽器中的 Python {: #python-in-the-browser }

Explore 的 Python 和 Notebook 分頁在訪客的瀏覽器中執行 Python。它發出的每次讀取都以已登入使用者的身分、帶著其目前角色、組織和環境傳送到伺服器，並與來自任何其他用戶端的同一陳述式完全一樣地受到治理。

## 高安全模式 {: #high-security-mode }

`provisa.yaml` 中的 `security.mode: high` 聲明一項保證：Provisa 後端從不處理明文數據。（REQ-693） 每個有意義的欄位都在數據來源處加密，只有持有解密金鑰的用戶端才能讀取它。這項保證有一些部署必須為之規劃的後果。

**該模式做甚麼：**

- **數據端點要求用戶端解密的證明。** `/data/` 之下的一切都傳回 403，除非呼叫者出示 `X-Provisa-KMS-Key` 標頭——這是設定為本機解密的 JDBC 或 Python 用戶端的標誌。明文的瀏覽器或 REST 使用者不攜帶這樣的金鑰，因此會被拒絕。該控制是對整棵樹的預設拒絕：明天新增的路由在發佈當天便受到保護，例外需要合理理由。
- **結構描述中繼資料端點保持開放。** `/data/sdl`、`/data/introspection`、`/data/schema-version`、`/data/domains`、`/data/proto` 及 `/data/compile` 不傳回資料列數據，而用戶端必須先讀取結構描述——包括哪些欄位是 `@encrypted`——才能連線。
- **gRPC 及 Arrow Flight 繼續提供服務，要求相同的證明。** 它們是加密用戶端實際使用的傳輸；關閉它們會令高安全部署沒有線路協定可用。兩者上的數據呼叫都必須以呼叫中繼資料攜帶同一個 KMS 金鑰。
- **pgwire、Bolt 及 MCP 不會啟動。** 三者都沒有能攜帶解密上下文的逐連線交握：pgwire 的資料列集及 Cypher 結果以明文傳輸，MCP 的工具呼叫把結果作為文字交給模型。為其中任何一個設定的連接埠，都會在啟動時被拒絕，而不是提供服務。
- **關係防護無法繞過。** `ignore_relationships` 及 `relationship_guard: false` 都會被忽略；請參閱[關係治理](#relationship-governance-v002)。

**如何驗證部署處於該模式：** 啟動記錄會指名它，不帶 KMS 金鑰的 `/data/sql` 查詢會以 403 回應，並附上指名 REQ-693 的訊息，而 pgwire、Bolt 及 MCP 連接埠沒有在監聽。

## ABAC 批准掛鉤 {: #abac-approval-hook }

在查詢執行之前觸發的可選外部政策掛鉤。（REQ-203） 設定之後，Provisa 會帶著使用者身份、角色、資料表、欄位及操作呼叫你的政策引擎。回應決定查詢是否繼續。（REQ-203） 以**需要批准**註冊的命令，在每次呼叫之前都會交給同一個掛鉤；請參閱[命令呼叫](#command-calls)。（REQ-1924）

### 範圍界定 {: #scoping }

掛鉤只會在查詢觸及啟用了它的資料表或數據來源時觸發——其他一切都沒有額外開銷。（REQ-204）

| 設定 | 效果 |
| -------- | -------- |
| `auth.approval_hook.scope: all` | 每個查詢都呼叫掛鉤 |
| `sources[].approval_hook: true` | 該數據來源的所有資料表都呼叫掛鉤 |
| `tables[].approval_hook: true` | 該資料表呼叫掛鉤 |

### 各項協定 {: #protocols }

支援三種傳輸：（REQ-246）

| 類型 | 情境 | 設定欄位 |
| ------ | ---------- | ------------- |
| `webhook` | 任何 HTTP 政策服務（OPA、自訂） | `url` |
| `unix_socket` | 同一主機上的 OPA 或政策 sidecar | `socket_path` + `url` |
| `grpc` | 就近的高效能政策服務 | `url` (host:port) |

gRPC 傳輸使用 `provisa/auth/approval.proto` 中定義的 `provisa.auth.ApprovalService` 契約。在你的政策引擎中實作此服務：（REQ-246）

```proto
service ApprovalService {
  rpc Evaluate (ApprovalRequest) returns (ApprovalResponse);
}

message ApprovalRequest {
  string user = 1;
  repeated string roles = 2;
  repeated string tables = 3;
  repeated string columns = 4;
  string operation = 5;
  map<string, string> session_vars = 6;
  string command = 7;         // a command call's name; empty for a query
  string arguments_json = 8;  // a command call's arguments as a JSON object
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```
gRPC 通道是持久的——每個 Provisa 執行個體一個通道，在對該掛鉤端點的所有呼叫中重用。（REQ-555）

### 請求／回應 {: #request-response }

三種傳輸都攜帶相同的負載：（REQ-246）

| 欄位 | 類型 | 說明 |
| ------- | ------ | ------------- |
| `user` | string | 已驗證使用者的身份 |
| `roles` | string[] | 使用者的 Provisa 角色 |
| `tables` | string[] | 查詢參照的資料表 id |
| `columns` | string[] | 查詢中選取的欄位 |
| `operation` | string | `"query"` 或 `"mutation"`；命令呼叫為 `"command"` |
| `command` | string | 命令呼叫時的命令名稱；查詢時為空 |
| `arguments` | object | 呼叫命令所用的引數；查詢時為空 |

webhook 及 Unix socket 傳輸交換 JSON，以 `command` 及 `arguments` 為鍵。在 gRPC 上，引數以 JSON 文字 `arguments_json` 傳遞。回應必須包含 `approved`（bool），並可選包含 `reason`（string）。（REQ-246）

### 命令呼叫 {: #command-calls }

設定了 `requires_approval`（命令表單上的**需要批准**開關）的命令，在任何介面上每次呼叫之前都會交給掛鉤。只有掛鉤批准時它才會執行。請求攜帶 `operation: "command"`、命令名稱及其引數；`tables` 及 `columns` 為空。（REQ-1924） [tool-verified: `provisa/api/data/action_exec.py` `_require_approval` (`operation="command"`, `tables=[]`, `columns=[]`, `command=fn["name"]`, `arguments=args`)]

```json
{
  "user": "analyst",
  "roles": ["analyst"],
  "tables": [],
  "columns": [],
  "operation": "command",
  "command": "create_issue",
  "arguments": {"input": {"title": "Crash on save"}}
}
```
[inferred: the payload also carries `session_vars`; values shown are placeholders]

兩種拒絕，均為 403：

- `functions.approval_unavailable`：沒有設定掛鉤。與查詢不同，需要批准的命令在沒有掛鉤時從不放行。
- `functions.approval_denied`：掛鉤回應了 `approved: false`。原因在訊息中。

### 逾時與退路 {: #timeout-and-fallback }

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```
逾時或傳輸錯誤時，會套用 `fallback` 政策。（REQ-247） 斷路器（預設：連續 5 次錯誤後斷開，30 秒後半開）可防止緩慢的掛鉤端點引發連鎖故障。（REQ-556）

### 設定範例 {: #configuration-example }

```yaml
auth:
  approval_hook:
    type: webhook
    url: "http://opa.internal:8181/v1/data/provisa/allow"
    timeout_ms: 300
    fallback: deny

sources:
  - id: analytics_pg
    approval_hook: true   # all tables on this source require hook approval

tables:
  - id: salary_data
    approval_hook: true   # this table always requires hook approval
```
## 密鑰 {: #secrets }

憑證使用 `${env:VAR_NAME}` 語法，在執行時解析。（REQ-557） 密碼從不保存於設定資料庫中。（REQ-557）

關於密鑰服務的完整說明——儲存、參照語法及提供者——請參閱[密鑰](secrets.md)。
