# 区域 {: #regions }

Provisa 为放置数据提供两个基本构件：**区域**，以及**区域归属**，即创建一个区域并把表分配给它。两者本身都不是策略。你用它们来做什么，分为三类：

- **性能。** 到读取者的延迟、他们副本的新鲜程度，以及引擎是否合适：它的数据中心在哪里、读取什么类型的存储、有多少 CPU 和内存。
- **成本。** 工作在哪里运行、在什么上运行：自有硬件还是云、大引擎还是小引擎，以及在不同地点之间移动数据的花费（egress）。
- **主权。** 谁控制数据、数据可以去哪里：法律的驻留规则（个人数据可以在哪里处理、在哪里存储），以及组织自身的控制线，例如一个业务部门或子公司把自己的数据和基础设施掌握在自己手中。

三者的机制相同。不同的只是你在其上编写的规则。

## 区域是某个地点的一个引擎及其存储 {: #a-region-is-an-engine-and-its-stores-in-a-place }

区域是 Provisa 执行工作并保存工作产物的一个地点。每个区域都有自己的引擎和自己的存储。(REQ-1922) [tool-verified: `provisa/core/regions.py` `OrgRegion`, `STORE_ROLES`]

| 存储 | 保存的内容 |
| --- | --- |
| `engine` | 在该区域执行语句的联邦引擎 |
| `replicas` | 归属于此的表的副本，以及该区域复制的无区域表的副本 |
| `views` | 该区域的物化视图副本 |
| `cache` | 缓存的应答 |
| `state` | 构建与刷新的状态 |
| `record` | 在此处理的每个请求的记录 |

一个模型治理每个区域。数据源、表、关系、角色、行和列规则、视图以及新鲜度设置都只在一个模型存储中声明一次，每个区域都提供同一个模型，因此定义不会在区域之间产生分歧。一次变更以同一个戳记到达每个区域，随后每个区域各自收敛到它。(REQ-1914, REQ-1922)

平台声明其节点运行所在的物理区域，以及每个区域应答的地址。每个组织的模型选择它使用其中哪些区域，并为每个区域中的每个角色指定自己的存储。节点用 `provisa run --region <id>` 在某个区域启动，并为每个选择该区域的组织提供服务。声明了区域时，未带 `--region` 启动的节点会拒绝启动，并列出它可以服务的区域。(REQ-1922) [tool-verified: `provisa/cli.py` `--region`; `provisa/core/regions.py` `validate_regions`]

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

加载时，如果区域之间不一致，配置会被拒绝并指明原因。区域必须为每个角色指定一个已声明的存储。它的 `replicas` 和 `views` 必须是同一个存储，它的引擎存储必须指定引擎 `kind`。平台有多个区域时，嵌入式引擎文件会被拒绝，因为其他区域无法访问它。[tool-verified: `require_one_materialize_store`, `require_engine_kind`, `require_reachable_engine`]

多个区域可以共用一个数据库实例、一个 Redis 或一个对象存储桶。区域保存的一切都在名称中带有该区域，因此它们的存储不会冲突。(REQ-1922)

不声明区域的部署只有一个隐式区域，任何地方都不显示区域。没有区域字段，也没有 `--region` 标志，指定区域的配置会被拒绝。(REQ-1922)

## 区域归属：表指定它的归属区域 {: #region-homing-a-table-names-its-home }

表可以把其组织的某个区域指定为归属区域。归属区域是唯一构建并刷新其副本的区域，也是唯一可以把它保持为热表的区域。其他所有区域都从归属区域的副本就地读取该表，地址由模型给出。读取者的行和列规则会进入这条语句，因此过滤在归属区域完成，只有读取者被允许看到的内容会越过边界。(REQ-1921) [tool-verified: `provisa/core/regions.py` module docstring]

```yaml
tables:
  - source_id: eu-shop
    schema_name: public
    table_name: customers
    region: eu
```
由此带来的一些结果：

- 其他区域都不读取该表的数据源，只读取归属区域的副本。你为防止负载而复制的数据源仍然受到保护。
- 如果副本尚未构建，或归属区域无法访问，请求会被拒绝并指明归属区域。不会改为实时读取数据源。
- 未被复制的归属表（`replicate` 为 Never，或设置尚未要求副本）由每个区域从其数据源实时读取，与任何未复制的表相同。
- 表的热度计数只是其归属区域的流量。(REQ-239)
- 物化视图也可以指定区域。这样它只在那里构建，其他所有区域都读取该副本，与归属表完全相同。

读取另一个区域副本的区域，需要一个能读取它的引擎：`duckdb`、`motherduck`、`pg`、`trino` 或 `trino-byo`。归属区域的 `replicas` 存储必须是 PostgreSQL 服务器。两者不匹配时，配置会指明它们。[tool-verified: `require_readable_elsewhere`; `REGION_READERS` in `provisa/core/engine_kinds.py`]

数据源也可以指定区域，但该区域只在你注册该数据源的新表时预填表单。它不决定数据存放在哪里，之后修改它也不会移动任何表。(REQ-1921)

## 无区域的表在被读取的地方复制 {: #unhomed-tables-are-copied-where-they-are-read }

不指定区域的表可以被复制到每个区域。当它的 `replicate` 设置需要副本时，读取者连接的区域会在自己的存储中构建一个副本。不指定区域的物化视图会在每个区域构建，每个副本都在该区域的视图存储中。(REQ-1921)

因此，增加一个区域意味着更多副本和更多刷新工作，但绝不意味着更多治理。一个模型治理每个副本，每个区域都适用相同的行和列规则。视图在某个区域的构建按组织管理员在该区域执行来治理，因此每个区域的副本只包含可以保存在那里的内容。(REQ-1921, REQ-1922)

## 使用区域的三个理由 {: #three-reasons-to-use-regions }

### 性能 {: #performance }

只为性能而使用的区域不声明跨区域规则。表、角色和规则与你为单个区域编写的相同，区域归属决定工作在哪里进行：

- **延迟。** 读取者连接到离他们近的区域，该区域的引擎、缓存和副本对他们而言都是本地的。
- **新鲜度。** 归属于其变化之处的表在那里刷新，靠近其数据源。
- **引擎适配。** 每个区域运行自己的引擎，因此你可以让引擎与地点相匹配：它所在的数据中心、它读取的存储，以及它拥有的 CPU 和内存。

**示例：两个相同的区域。** 在两个数据中心声明 `east` 和 `west`，二者使用相同类型的引擎和相同类型的存储，并且不为任何表指定归属。每个区域服务离它最近的读取者：为他们读取的内容构建自己的副本，并缓存他们的应答。两者可以位于同一个负载均衡器之后分担负载。刷新计划只有一个，即模型的计划，两个区域都按它开始刷新各自的副本，但它们的引擎可能无法在相同时间内完成。因此在某一时刻，无法保证两者处于相同状态，从一个区域被转到另一个区域的读取者可能看到新鲜度不同的数据。某个区域停止时，Provisa 不会把客户端转到另一个区域；客户端去往哪里由负载均衡器决定。[inferred: no failover or cross-region redirect exists in `provisa/`; clients connect to a region by its address, and each region builds and refreshes only its own copies, on the model's one schedule (REQ-1922)]

### 成本 {: #cost }

为成本而设的区域同样不声明跨区域规则。它们把工作放在运行成本最低的地方：

- **自有硬件与云。** 在本地自有的硬件上保留一个区域，服务那里的表和读取者，并在云中运行另一个区域处理其余部分。
- **引擎规模。** 在一个区域运行大引擎处理偶尔的繁重工作，在另一个区域运行小引擎处理日常读取。读取者通过地址选择区域。
- **Egress。** 其他区域的读取者只获得归属表副本中经过过滤的行，归属于某个区域的大表不会被复制到每个区域。

### 主权 {: #sovereignty }

为主权而使用的区域确实声明跨区域规则，无论原因是法律还是组织：把自己的表归属于自己区域的子公司，会在自己的基础设施上处理和存储这些表，并通过这些规则决定集团其他部分能读取什么。这些规则就是普通的行和列规则，写在表明读取者（或执行工作的节点）所在位置的属性上：

- 当平台声明了区域时，`current_setting('provisa.region')` 是应答请求的节点所在的区域。写在它上面的行过滤会把某个区域的行留在该区域。(REQ-1922) [tool-verified: `provisa/core/request_context.py` `session_vars_for`]
- 成员的 `region` 属性使其持有派生角色 `region` 和 `region__<value>`，因此脱敏或授权条目可以指定 `analyst+region__eu`。节点自身的后台工作也以同样方式携带其区域。(REQ-2057) 参见[角色：授予角色、派生角色和元角色](security.md#roles-granted-derived-and-meta)。

这些规则与区域归属一起，决定什么会越过边界。归属表的个人数据在其归属区域存储和处理，其他地方的读取者只获得其规则允许的内容，并在归属区域完成过滤。在每个区域构建的无区域视图包含该区域管理员可以看到的内容，因此一个区域的副本可能包含另一个区域的副本所排除的行。(REQ-1921)

## 谁可以为表指定归属 {: #who-may-home-a-table }

设置、更改或移除表、数据源或物化视图的区域，就是让数据跨越边界。这需要 `data_residency` 权利，同时还要通过其域拥有该对象。授权指明它覆盖的值：组织的各个区域以及“无区域”。在两个区域之间移动对象需要同时覆盖两者的授权。超出授权的保存会被拒绝，并指明未覆盖的值。(REQ-1921)

该权利只在平台声明区域时存在。对象的其他一切由谁编辑，仍由域所有权决定。区域本身不赋予任何权利；希望某区域的表由该区域人员管理的组织，会给他们属于自己的域。(REQ-1921)

表或视图也可以设为**草稿**：停止服务，不向任何读者提供。通过管理端注册的表以草稿状态开始。其负责人只在它所指定区域的节点上预览它（未指定区域的，在负责人所在之处预览），该预览落地的副本保留在那个区域，等待该表发布。其他区域持有的、在其区域设置或更改之前留下的副本，会像已发布的表一样被停用并删除。区域之间的迁移可以经过草稿，使该表在此期间不在任何地方提供服务：第一个区域一方将其设为草稿，第二个区域的持有者通过设置自己的区域来接管该对象。(REQ-1921)

另请参阅[双区域演示](regions-demo.md)。
