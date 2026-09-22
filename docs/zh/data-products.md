# 数据产品（REQ-1634）

数据产品是一组具名的、有归属方的表束，被一起发布以供消费。这是目录向消费者暴露的单位 —— 不是单独的表，而是某个域明确声明已就绪的一个经过整理的界面。字段遵循 ODPS（Open Data Product Standard）词汇体系，前提是 Provisa 已经拥有相应的权威来源。[tool-verified: `provisa/core/models.py:318-342`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json`]

## 域归属规则

每个数据产品都恰好归属于一个域（`domain_id` 是必填字段）。只有当一张表与某个数据产品共享相同的 `domain_id` 时，该表才能加入该数据产品。UI 会将表选择器限定在该产品所属的域内；后端会在保存时拒绝任何域不匹配该产品所属域的 `product_id` 赋值。[tool-verified: `provisa/core/models.py:320`, `docs/arch/requirements.yaml:54573-54574`]

需要来自另一个域的数据的产品，必须先将该数据以域视图的形式引入，然后再将该视图作为成员纳入。

## 输出端口

被分配给某个数据产品的表和命令是它的**输出端口** —— 也就是消费者所看到的可查询界面。为某张表分配产品会设置 `Table.product_id`；清除该分配则会移除其成员关系。一张表最多只能属于一个产品。同一个域中的命令也可以被分配为成员。[tool-verified: `provisa/core/models.py:941`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json:tablesLabel,commandsLabel`]

## 详情面板各部分

在管理 UI 中打开一个数据产品会显示以下面板：

| 面板 | 显示内容 |
| --- | --- |
| 输出端口 | 成员表及其列；成员命令；示例查询（GraphQL、SQL、Cypher、gRPC、JSON:API、REST） |
| 相关术语 | 与该产品成员表关联的术语表条目 |
| 相关表 | 通过已批准关系可从成员表触达、但尚未成为该产品一部分的表 |
| 关系 | 该产品成员表之间已批准的关系 |
| 血缘 | 列血缘图，将成员表显示为已发布的端点，外加所有上游表。需要 `view_governance` 能力 |
| 输入端口 | 从血缘派生出的“单跳输入 → 转换 → 输出”。需要 `view_governance` |
| 数据质量 | 其合约会扫描该产品输出端口的检查器表；每次运行的每项检查各占一行。包含规则弹窗和 PII 标签展示 |

[tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:detail`]

## 元数据导出 {: #metadata-export }

默认情况下，只有被分配给某个产品的表才会发布到外部目录。`build_snapshot` 会应用一个 `data_products_only` 过滤器：未分配的表会被扣留，连同它们的关系边、血缘边和治理标签一起。数据源和域始终会发布，不受此过滤器影响。[tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

一个没有任何已导出成员的产品不会被发布 —— 一份空清单会误导性地宣称某个产品存在，却背后空无一物。[tool-verified: `provisa/api/metadata_export/model.py:106-113`]

只有具备原生数据产品概念的目录会将其作为一等实体发布；其余目录则发布（已经过滤的）成员表，不带产品分组：

| 目录 | 发布方式 |
| --- | --- |
| Snowflake Horizon | SHARE + 组织清单（原生 Data Product）；`publish=false` 使其保持 DRAFT 状态，`publish=true` 使其上线。该清单的数据字典覆盖每个成员及其列；前五个成员会被精选展示，脱敏列在预览中被隐藏（REQ-1656） |
| BigQuery Analytics Hub | Analytics Hub 清单（原生） |
| OpenMetadata | `DataProduct` 实体（原生） |
| DataHub | 原生 `dataProduct` URN 实体，具有自己的属性/所有权切面 |
| Collibra | `Data Product` 社区类型的资产，与成员表相关联 |
| Apache Atlas | 尽力而为的自定义 `provisa_data_product` typedef —— Atlas 没有原生的数据产品类型 |
| Atlan | 尽力而为的自定义 `DataProduct` typedef 推测 —— Atlan 对此没有文档记录的稳定类型 |
| OpenLineage | 不是一份清单 —— 成员表携带一个命名该产品的 `provisa_data_product` 自定义 facet |

[tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:389-418`, `provisa/api/metadata_export/bigquery_dataplex.py:112-136`, `provisa/api/metadata_export/openmetadata.py:326-344`, `provisa/api/metadata_export/datahub.py:133-136,443-483`, `provisa/api/metadata_export/collibra.py:129-133,371-388`, `provisa/api/metadata_export/atlas.py:134-147`, `provisa/api/metadata_export/atlan.py:60`, `provisa/api/metadata_export/openlineage.py:243,348`]

## 字段

| 字段 | 是否必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | 机器可读的稳定标识符，例如 `customer_360` |
| `domain_id` | 是 | 所属域；成员关系规则依据此字段强制执行 |
| `name` | 是 | 显示名称 |
| `owner_role` | 否 | 对该产品负责的角色；不同于域管家 |
| `team_role` | 否 | 其持有者构成日常工作团队的角色；解析为具体个人 |
| `purpose` | 否 | 该产品发布的内容及原因 |
| `limitations` | 否 | 已知的约束、注意事项或排除项 |
| `usage` | 否 | 如何消费该产品 |
| `version` | 否 | 例如 `1.2.0` |
| `status` | 否 | 例如 `proposed`、`active`、`deprecated`、`retired` |
| `sla` | 否 | 服务级别承诺；仅为文字说明 —— 一个产品跨越多个成员表，结构化的 SLA 无法明确指出它描述的是哪个成员 |
| `support` | 否 | 自由文本形式的支持说明 |
| `support_contact` | 否 | 电子邮件或 URL；Snowflake Horizon Catalog 组织清单文件要求提供此字段（REQ-1635） |
| `publish` | 否 | `true` 表示立即发布 Horizon Catalog 清单；新清单默认是 DRAFT 状态（REQ-1635） |
| `custom_properties` | 否 | 标准字段未覆盖的任意键值元数据 |

[tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/types.py:104-118,538-551`]
