# 區域 {: #regions }

Provisa 為放置數據提供兩個基本構件：**區域**，以及**區域歸屬**，即建立一個區域並把資料表分配給它。兩者本身都不是策略。你用它們來做什麼，分為三類：

- **效能。** 到讀取者的延遲、他們副本的新鮮程度，以及引擎是否合適：它的數據中心在哪裡、讀取什麼類型的儲存、有多少 CPU 和記憶體。
- **成本。** 工作在哪裡執行、在什麼上執行：自有硬件還是雲、大引擎還是小引擎，以及在不同地點之間移動數據的花費（egress）。
- **主權。** 誰控制數據、數據可以去哪裡：法律的駐留規則（個人數據可以在哪裡處理、在哪裡儲存），以及組織自身的控制線，例如一個業務部門或子公司把自己的數據和基礎設施掌握在自己手中。

三者的機制相同。不同的只是你在其上編寫的規則。

## 區域是某個地點的一個引擎及其儲存 {: #a-region-is-an-engine-and-its-stores-in-a-place }

區域是 Provisa 執行工作並儲存工作產物的一個地點。每個區域都有自己的引擎和自己的儲存。(REQ-1922) [tool-verified: `provisa/core/regions.py` `OrgRegion`, `STORE_ROLES`]

| 儲存 | 儲存的內容 |
| --- | --- |
| `engine` | 在該區域執行語句的聯邦引擎 |
| `replicas` | 歸屬於此的資料表的副本，以及該區域複製的無區域資料表的副本 |
| `views` | 該區域的具體化檢視副本 |
| `cache` | 快取的回應 |
| `state` | 建構與重新整理的狀態 |
| `record` | 在此處理的每個請求的記錄 |

一個模型治理每個區域。數據來源、資料表、關係、角色、資料列和欄位規則、檢視以及新鮮度設定都只在一個模型儲存中宣告一次，每個區域都提供同一個模型，因此定義不會在區域之間產生分歧。一次變更以同一個戳記到達每個區域，隨後每個區域各自收斂到它。(REQ-1914, REQ-1922)

平台宣告其節點執行所在的物理區域，以及每個區域回應的位址。每個組織的模型選擇它使用其中哪些區域，並為每個區域中的每個角色指定自己的儲存。節點用 `provisa run --region <id>` 在某個區域啟動，並為每個選擇該區域的組織提供服務。宣告了區域時，未帶 `--region` 啟動的節點會拒絕啟動，並列出它可以服務的區域。(REQ-1922) [tool-verified: `provisa/cli.py` `--region`; `provisa/core/regions.py` `validate_regions`]

```yaml
platform:
  regions:
    - { id: eu, address: "https://eu.provisa.example" }
    - { id: us, address: "https://us.provisa.example" }

stores:
  - { id: eu_engine, url: "${secret:eu-trino-url}", kind: trino }
  - { id: us_engine, url: "${secret:us-trino-url}", kind: trino }
  - { id: eu_data,   url: "${secret:eu-pg-url}" }
  - { id: us_data,   url: "${secret:us-pg-url}" }
  - { id: eu_cache,  url: "${secret:eu-redis-url}" }
  - { id: us_cache,  url: "${secret:us-redis-url}" }

regions:
  - { id: eu, engine: eu_engine, replicas: eu_data, views: eu_data, cache: eu_cache, state: eu_data, record: eu_data }
  - { id: us, engine: us_engine, replicas: us_data, views: us_data, cache: us_cache, state: us_data, record: us_data }
```
[inferred: the URLs and secret names are placeholders; the shape follows `PlatformRegion`, `StoreConfig` and `OrgRegion` in `provisa/core/regions.py`]

載入時，如果區域之間不一致，設定會被拒絕並指明原因。區域必須為每個角色指定一個已宣告的儲存。它的 `replicas` 和 `views` 必須是同一個儲存，它的引擎儲存必須指定引擎 `kind`。平台有多個區域時，內嵌引擎檔案會被拒絕，因為其他區域無法存取它。[tool-verified: `require_one_materialize_store`, `require_engine_kind`, `require_reachable_engine`]

多個區域可以共用一個資料庫執行個體、一個 Redis 或一個物件儲存桶。區域儲存的一切都在名稱中帶有該區域，因此它們的儲存不會衝突。(REQ-1922)

不宣告區域的部署只有一個隱式區域，任何地方都不顯示區域。沒有區域欄位，也沒有 `--region` 旗標，指定區域的設定會被拒絕。(REQ-1922)

## 區域歸屬：資料表指定它的歸屬區域 {: #region-homing-a-table-names-its-home }

資料表可以把其組織的某個區域指定為歸屬區域。歸屬區域是唯一建構並重新整理其副本的區域，也是唯一可以把它保持為熱資料表的區域。其他所有區域都從歸屬區域的副本就地讀取該資料表，位址由模型給出。讀取者的資料列和欄位規則會進入這條語句，因此過濾在歸屬區域完成，只有讀取者被允許看到的內容會越過邊界。(REQ-1921) [tool-verified: `provisa/core/regions.py` module docstring]

```yaml
tables:
  - source_id: eu-shop
    schema_name: public
    table_name: customers
    region: eu
```
由此帶來的一些結果：

- 其他區域都不讀取該資料表的數據來源，只讀取歸屬區域的副本。你為防止負載而複製的數據來源仍然受到保護。
- 如果副本尚未建構，或歸屬區域無法存取，請求會被拒絕並指明歸屬區域。不會改為即時讀取數據來源。
- 未被複製的歸屬資料表（`replicate` 為 Never，或設定尚未要求副本）由每個區域從其數據來源即時讀取，與任何未複製的資料表相同。
- 資料表的熱度計數只是其歸屬區域的流量。(REQ-239)
- 具體化檢視也可以指定區域。這樣它只在那裡建構，其他所有區域都讀取該副本，與歸屬資料表完全相同。

讀取另一個區域副本的區域，需要一個能讀取它的引擎：`duckdb`、`motherduck`、`pg`、`trino` 或 `trino-byo`。歸屬區域的 `replicas` 儲存必須是 PostgreSQL 伺服器。兩者不匹配時，設定會指明它們。[tool-verified: `require_readable_elsewhere`; `REGION_READERS` in `provisa/core/engine_kinds.py`]

數據來源也可以指定區域，但該區域只在你註冊該數據來源的新資料表時預填表單。它不決定數據存放在哪裡，之後修改它也不會移動任何資料表。(REQ-1921)

## 無區域的資料表在被讀取的地方複製 {: #unhomed-tables-are-copied-where-they-are-read }

不指定區域的資料表可以被複製到每個區域。當它的 `replicate` 設定需要副本時，讀取者連線的區域會在自己的儲存中建構一個副本。不指定區域的具體化檢視會在每個區域建構，每個副本都在該區域的檢視儲存中。(REQ-1921)

因此，增加一個區域意味著更多副本和更多重新整理工作，但絕不意味著更多治理。一個模型治理每個副本，每個區域都適用相同的資料列和欄位規則。檢視在某個區域的建構按組織管理員在該區域執行來治理，因此每個區域的副本只包含可以儲存在那裡的內容。(REQ-1921, REQ-1922)

## 使用區域的三個理由 {: #three-reasons-to-use-regions }

### 效能 {: #performance }

只為效能而使用的區域不宣告跨區域規則。資料表、角色和規則與你為單一區域編寫的相同，區域歸屬決定工作在哪裡進行：

- **延遲。** 讀取者連線到離他們近的區域，該區域的引擎、快取和副本對他們而言都是本機的。
- **新鮮度。** 歸屬於其變化之處的資料表在那裡重新整理，靠近其數據來源。
- **引擎適配。** 每個區域執行自己的引擎，因此你可以讓引擎與地點相匹配：它所在的數據中心、它讀取的儲存，以及它擁有的 CPU 和記憶體。

**範例：兩個相同的區域。** 在兩個數據中心宣告 `east` 和 `west`，二者使用相同類型的引擎和相同類型的儲存，並且不為任何資料表指定歸屬。每個區域服務離它最近的讀取者：為他們讀取的內容建構自己的副本，並快取他們的回應。兩者可以位於同一個負載平衡器之後分擔負載。重新整理排程只有一個，即模型的排程，兩個區域都按它開始重新整理各自的副本，但它們的引擎可能無法在相同時間內完成。因此在某一時刻，無法保證兩者處於相同狀態，從一個區域被轉到另一個區域的讀取者可能看到新鮮度不同的數據。某個區域停止時，Provisa 不會把用戶端轉到另一個區域；用戶端去往哪裡由負載平衡器決定。[inferred: no failover or cross-region redirect exists in `provisa/`; clients connect to a region by its address, and each region builds and refreshes only its own copies, on the model's one schedule (REQ-1922)]

### 成本 {: #cost }

為成本而設的區域同樣不宣告跨區域規則。它們把工作放在執行成本最低的地方：

- **自有硬件與雲。** 在自家機房自有的硬件上保留一個區域，服務那裡的資料表和讀取者，並在雲中執行另一個區域處理其餘部分。
- **引擎規模。** 在一個區域執行大引擎處理偶爾的繁重工作，在另一個區域執行小引擎處理日常讀取。讀取者透過位址選擇區域。
- **Egress。** 其他區域的讀取者只獲得歸屬資料表副本中經過過濾的資料列，歸屬於某個區域的大資料表不會被複製到每個區域。

### 主權 {: #sovereignty }

為主權而使用的區域確實宣告跨區域規則，無論原因是法律還是組織：把自己的資料表歸屬於自己區域的子公司，會在自己的基礎設施上處理和儲存這些資料表，並透過這些規則決定集團其他部分能讀取什麼。這些規則就是普通的資料列和欄位規則，寫在表明讀取者（或執行工作的節點）所在位置的屬性上：

- 當平台宣告了區域時，`current_setting('provisa.region')` 是回應請求的節點所在的區域。寫在它上面的資料列過濾會把某個區域的資料列留在該區域。(REQ-1922) [tool-verified: `provisa/core/request_context.py` `session_vars_for`]
- 成員的 `region` 屬性使其持有派生角色 `region` 和 `region__<value>`，因此遮罩或授權條目可以指定 `analyst+region__eu`。節點自身的後台工作也以同樣方式攜帶其區域。(REQ-2057) 參見[角色：授予角色、派生角色及元角色](security.md#roles-granted-derived-and-meta)。

這些規則與區域歸屬一起，決定什麼會越過邊界。歸屬資料表的個人數據在其歸屬區域儲存和處理，其他地方的讀取者只獲得其規則允許的內容，並在歸屬區域完成過濾。在每個區域建構的無區域檢視包含該區域管理員可以看到的內容，因此一個區域的副本可能包含另一個區域的副本所排除的資料列。(REQ-1921)

## 誰可以為資料表指定歸屬 {: #who-may-home-a-table }

設定、更改或移除資料表、數據來源或具體化檢視的區域，就是讓數據跨越邊界。這需要 `data_residency` 權利，同時還要透過其網域擁有該物件。授權指明它覆蓋的值：組織的各個區域以及「無區域」。在兩個區域之間移動物件需要同時覆蓋兩者的授權。超出授權的儲存會被拒絕，並指明未覆蓋的值。(REQ-1921)

該權利只在平台宣告區域時存在。物件的其他一切由誰編輯，仍由網域所有權決定。區域本身不賦予任何權利；希望某區域的資料表由該區域人員管理的組織，會給他們屬於自己的網域。(REQ-1921)

資料表或檢視也可以設為**草稿**：停止服務，不向任何讀者提供。透過管理端註冊的資料表以草稿狀態開始。其負責人只在它所指定區域的節點上預覽它（未指定區域的，在負責人所在之處預覽），該預覽落地的副本保留在那個區域，等待該資料表發佈。其他區域持有的、在其區域設定或更改之前留下的副本，會像已發佈的資料表一樣被停用並刪除。區域之間的遷移可以經過草稿，使該資料表在此期間不在任何地方提供服務：第一個區域一方將其設為草稿，第二個區域的持有者透過設定自己的區域來接管該物件。(REQ-1921)

另請參閱[雙區域示範](regions-demo.md)。
