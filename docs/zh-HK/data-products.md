# 數據產品 (REQ-1634)

數據產品是一組具名、有擁有者、共同發佈以供使用的資料表集合。它是目錄向使用者公開的單位——並非個別資料表，而是某個網域明確宣告已就緒的整理後介面。欄位遵循ODPS（開放數據產品標準）詞彙，適用於Provisa已擁有真實來源之處。[tool-verified: `provisa/core/models.py:318-342`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json`]

## 網域擁有權規則

每個數據產品恰好由一個網域擁有（`domain_id` 為必填欄位）。資料表只有在與數據產品共享相同 `domain_id` 時，才可加入該數據產品。UI會將資料表選擇器限定於該產品所屬的網域；後端會在儲存時拒絕網域不相符的 `product_id` 指派。[tool-verified: `provisa/core/models.py:320`, `docs/arch/requirements.yaml:54573-54574`]

若某產品需要另一網域的數據，必須先將該數據以網域檢視形式引入，再將該檢視納入為成員。

## 輸出埠

指派予數據產品的資料表及命令，即為其**輸出埠**——使用者所見的可查詢介面。指派資料表會設定 `Table.product_id`；清除則移除其成員資格。一個資料表最多只能屬於一個產品。同一網域內的命令亦可指派為成員。[tool-verified: `provisa/core/models.py:941`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json:tablesLabel,commandsLabel`]

## 詳情面板分節

在管理員UI中開啟數據產品，會顯示以下面板：

| 面板 | 顯示內容 |
| --- | --- |
| 輸出埠 | 成員資料表及其欄位；成員命令；範例查詢（GraphQL、SQL、Cypher、gRPC、JSON:API、REST） |
| 相關詞彙 | 連結至該產品成員資料表的詞彙表術語 |
| 相關資料表 | 可透過已批准關係從成員資料表到達、但尚未成為該產品一部分的資料表 |
| 關係 | 該產品成員資料表之間已批准的關係 |
| 血緣 | 欄位血緣圖，顯示成員資料表作為已發佈端點，以及所有上游資料表。需要 `view_governance` 能力 |
| 輸入埠 | 由血緣衍生出的單跳輸入 → 轉換 → 輸出。需要 `view_governance` |
| 數據品質 | 其合約掃描此產品輸出埠的檢查器資料表；每次執行、每項檢查各一行。包含規則模態視窗及PII標籤顯示 |

[tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:detail`]

## 中繼資料匯出 {: #metadata-export }

預設情況下，只有指派至某產品的資料表會發佈至外部目錄。`build_snapshot` 會套用 `data_products_only` 篩選器：未指派的資料表會被保留不發佈，連同其關係邊、血緣邊及治理標籤一併保留。數據來源及網域則一律發佈，不受此限。[tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

沒有任何已匯出成員的產品不會發佈——空的列表會令人誤以為該產品存在卻毫無內容。[tool-verified: `provisa/api/metadata_export/model.py:106-113`]

只有具備原生數據產品概念的目錄，才會將其發佈為一級實體；其餘目錄則會發佈（已篩選的）成員資料表，而不作產品分組：

| 目錄 | 發佈形式 |
| --- | --- |
| Snowflake Horizon | SHARE + organization listing（原生數據產品）；`publish=false` 使其保持DRAFT狀態，`publish=true` 則使其上線 |
| BigQuery Analytics Hub | Analytics Hub listing（原生） |
| OpenMetadata | `DataProduct` 實體（原生） |
| DataHub | 原生 `dataProduct` URN實體，具備自身的屬性／擁有權切面 |
| Collibra | 屬於 `Data Product` 社群類型的資產，與成員資料表相關聯 |
| Apache Atlas | 盡力而為的自訂 `provisa_data_product` typedef——Atlas並無原生數據產品類型 |
| Atlan | 盡力而為的自訂 `DataProduct` typedef猜測——Atlan並無此項的文件化穩定類型 |
| OpenLineage | 並非listing——成員資料表會攜帶一個命名該產品的自訂 `provisa_data_product` facet |

[tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:389-418`, `provisa/api/metadata_export/bigquery_dataplex.py:112-136`, `provisa/api/metadata_export/openmetadata.py:326-344`, `provisa/api/metadata_export/datahub.py:133-136,443-483`, `provisa/api/metadata_export/collibra.py:129-133,371-388`, `provisa/api/metadata_export/atlas.py:134-147`, `provisa/api/metadata_export/atlan.py:60`, `provisa/api/metadata_export/openlineage.py:243,348`]

## 欄位

| 欄位 | 必填 | 備註 |
| --- | --- | --- |
| `id` | 是 | 機器可讀的穩定識別碼，例如 `customer_360` |
| `domain_id` | 是 | 擁有網域；成員資格規則依此強制執行 |
| `name` | 是 | 顯示名稱 |
| `owner_role` | 否 | 對此產品負責的角色；與網域數據管家不同 |
| `team_role` | 否 | 擔任此角色者組成日常工作團隊；可解析至個別使用者 |
| `purpose` | 否 | 此產品發佈的內容及原因 |
| `limitations` | 否 | 已知限制、注意事項或例外情況 |
| `usage` | 否 | 如何使用此產品 |
| `version` | 否 | 例如 `1.2.0` |
| `status` | 否 | 例如 `proposed`、`active`、`deprecated`、`retired` |
| `sla` | 否 | 服務水平承諾；僅限散文形式——一個產品橫跨多個成員資料表，結構化SLA無法明確指出所描述的是哪個成員 |
| `support` | 否 | 自由文字支援指引 |
| `support_contact` | 否 | 電郵或URL；Snowflake Horizon Catalog organization listing manifest要求必填（REQ-1635） |
| `publish` | 否 | `true` 表示立即發佈Horizon Catalog listing；新listing預設為DRAFT（REQ-1635） |
| `custom_properties` | 否 | 標準欄位未涵蓋的任意鍵值中繼資料 |

[tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/types.py:104-118,538-551`]
