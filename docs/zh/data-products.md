# 数据产品(REQ-1634)

数据产品是一个有名称、有归属的表捆绑包，各表一起发布供消费。它是目录向消费者暴露的单位——不是单张表，而是某个领域明确声明已就绪的经过整理的界面。字段沿用 ODPS（开放数据产品标准）词汇——Provisa 已经是相关信息的权威来源之处。[tool-verified: `provisa/core/models.py:318-342`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json`]

## 领域所有权规则

每个数据产品都恰好归属于一个领域（`domain_id` 是必填字段）。只有当表与产品共享相同的 `domain_id` 时，该表才能加入该数据产品。界面会将表选择器限定在该产品所属的领域范围内；后端会在保存时拒绝领域不匹配产品所属领域的 `product_id` 赋值。[tool-verified: `provisa/core/models.py:320`, `docs/arch/requirements.yaml:54573-54574`]

需要来自其他领域数据的产品，必须先以领域视图的形式引入该数据，然后再将该视图纳入为成员。

## 输出端口

指定给某个数据产品的表和命令是其**输出端口**——消费者所见的可查询界面。指定某张表会设置 `Table.product_id`；清除该字段则会移除其成员关系。一张表至多属于一个产品。同一领域中的命令也可以被指定为成员。[tool-verified: `provisa/core/models.py:941`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json:tablesLabel,commandsLabel`]

## 详情面板各部分

在管理界面中打开某个数据产品会显示以下面板：

| 面板 | 显示内容 |
| --- | --- |
| 输出端口 | 成员表及其列；成员命令；示例查询（GraphQL、SQL、Cypher、gRPC、JSON:API、REST） |
| 相关术语 | 与该产品成员表关联的术语表术语 |
| 相关表 | 可通过已批准关系从成员表到达、但尚未成为该产品一部分的表 |
| 关系 | 该产品成员表之间已批准的关系 |
| 血缘 | 列血缘图，将成员表显示为已发布端点，并列出每一张上游表。需要 `view_governance` 能力 |
| 输入端口 | 由血缘派生的一跳输入 → 转换 → 输出。需要 `view_governance` |
| 数据质量 | 其契约扫描该产品输出端口的检查器表；每次运行的每项检查各占一行。包含规则弹窗和 PII 标签显示 |

[tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:detail`]

## 元数据导出 {: #metadata-export }

默认情况下，只有指定给某个产品的表才会发布到外部目录。`build_snapshot` 应用 `data_products_only` 过滤器：未指定的表会连同其关系边、血缘边和治理标签一起被扣留。数据源和领域始终照常发布。[tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

没有已导出成员的产品不会发布——一个空清单会宣称某产品存在，但其背后却空无一物。[tool-verified: `provisa/api/metadata_export/model.py:106-113`]

只有具备原生数据产品概念的目录才会将其作为一等实体发布；其余目录会发布（已过滤的）成员表，但不带产品分组：

| 目录 | 发布形式 |
| --- | --- |
| Snowflake Horizon | SHARE + 组织清单（原生数据产品）；`publish=false` 使其保持 DRAFT（草稿）状态，`publish=true` 使其上线 |
| BigQuery Analytics Hub | Analytics Hub 清单（原生） |
| OpenMetadata | `DataProduct` 实体（原生） |
| DataHub | 原生 `dataProduct` URN 实体，拥有自己的属性/所有权切面 |
| Collibra | `Data Product` 社区类型的资产，与成员表相关联 |
| Apache Atlas | 尽力而为的自定义 `provisa_data_product` 类型定义——Atlas 没有原生的数据产品类型 |
| Atlan | 尽力而为的自定义 `DataProduct` 类型定义推测——Atlan 对此没有文档化的稳定类型 |
| OpenLineage | 不是清单——成员表携带一个命名该产品的自定义 `provisa_data_product` 切面 |

[tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:389-418`, `provisa/api/metadata_export/bigquery_dataplex.py:112-136`, `provisa/api/metadata_export/openmetadata.py:326-344`, `provisa/api/metadata_export/datahub.py:133-136,443-483`, `provisa/api/metadata_export/collibra.py:129-133,371-388`, `provisa/api/metadata_export/atlas.py:134-147`, `provisa/api/metadata_export/atlan.py:60`, `provisa/api/metadata_export/openlineage.py:243,348`]

## 字段

| 字段 | 必需 | 说明 |
| --- | --- | --- |
| `id` | 是 | 机器可读的稳定标识符，例如 `customer_360` |
| `domain_id` | 是 | 所属领域；成员关系规则据此强制执行 |
| `name` | 是 | 显示名称 |
| `owner_role` | 否 | 对该产品负责的角色；与领域数据管家不同 |
| `team_role` | 否 | 组成日常工作团队的角色所持有的身份；解析为具体个人 |
| `purpose` | 否 | 该产品发布的内容及原因 |
| `limitations` | 否 | 已知的限制、注意事项或排除项 |
| `usage` | 否 | 如何使用该产品 |
| `version` | 否 | 例如 `1.2.0` |
| `status` | 否 | 例如 `proposed`、`active`、`deprecated`、`retired` |
| `sla` | 否 | 服务级别承诺；仅为自由文本——一个产品跨越多张成员表，结构化的 SLA 无法明确指出它描述的是哪个成员 |
| `support` | 否 | 自由文本支持说明 |
| `support_contact` | 否 | 电子邮件或 URL；Snowflake Horizon Catalog 组织清单所需(REQ-1635) |
| `publish` | 否 | 设为 `true` 会立即发布 Horizon Catalog 清单；新清单默认是 DRAFT（草稿）状态(REQ-1635) |
| `custom_properties` | 否 | 标准字段未涵盖的任意键值元数据 |

[tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/types.py:104-118,538-551`]
