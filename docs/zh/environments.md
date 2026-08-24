# 环境

环境是一个组织受治理模型的具名副本。这份副本在物理上是一个独立的 PostgreSQL 架构——不是一个判别列，不是前缀，而是一个真正的架构——因此每一条既有的仓库查询在环境内部都无需改写便是正确的，而且一个环境的行不会因为漏写谓词而进入另一个环境的读取结果（REQ-1487、REQ-1488）。
[tool-verified: `environments.py` module docstring; `org_schema()` at environments.py lines 86-96]

每个组织起步时都有一个名为 `prod` 的环境。它不能被删除或重命名。未指名环境的请求由 `prod` 服务；指名了不存在环境的请求会被拒绝。[tool-verified: `PROD = "prod"` at environments.py line 44; `select_environment()`
at env_routing.py lines 93-129]

环境功能面向付费套餐的组织提供。[inferred: REQ-1507]

## 环境名称

名称必须匹配 `[a-z][a-z0-9_]{1,31}`——两到三十二个字符的小写字母、数字和下划线，以字母开头。`prod` 以及以 `pg_` 开头的名称会被拒绝。任一组织可用的最大长度取决于该组织自身的 id：PostgreSQL 会静默截断超过 63 字节的标识符，而这个上限保护的正是环境所派生的最长架构名。[tool-verified: `ENV_NAME_PATTERN` at environments.py line 59; `validate_env_name()` at
environments.py lines 119-142; `max_env_name_length()` at environments.py lines 108-116]

## 副本带走什么

组织架构中的每一张表都恰好落在一个类别中（REQ-1489）。该分类是一份允许列表，而不是排除列表：后来新增的表在有人把它的类别写到这里之前不会随行，因此漏登记一张表的失败表现是一个变红的测试。[tool-verified: `CLASSIFIED`
constant and module docstring, env_classes.py lines 19-22]

| 类别 | 表 | 复制时会发生什么 |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | 整体复制 |
| IDENTITY_ONLY | sources, api_sources, kafka_sources, kafka_sinks | 身份与治理字段随行；连接值留在原处（见"绑定"） |
| SEEDED_AT_CREATION | roles, user_role_assignments | 仅在环境首次创建时复制；之后的合并不会碰它们 |
| PARTIAL | org_settings | 按键复制：治理设置随行，指向外部目标或按环境运行时的键留在原处 |
| NEVER_SENSITIVE | org_secrets, user_directory | 从不复制 |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log 等 | 从不复制 |

[tool-verified: `CARRIED`, `IDENTITY_ONLY`, `SEEDED_AT_CREATION`, `PARTIAL`, `NEVER_SENSITIVE`,
`NEVER_RUNTIME` frozensets, env_classes.py lines 29-113]

`SEEDED_AT_CREATION` 的存在是为了解决一个具体问题。新环境需要角色和分配，否则它一开张就没人能做任何事。但如果后来的合并把 `prod` 的 `developer` 行也带过来，就会覆盖某个受限分支可能需要的受限版本，从而把评审路径变成提权通道。因此角色和分配只在创建时随行一次，此后就是各环境自己的答案。[tool-verified: env_classes.py lines 65-71; env_copy.py lines 41-44]

## 绑定

绑定是说明数据源实际指向何处的那些列——`host`、`port`、`database`、`username` 等等。它们在任何复制中都不随行。尚未绑定的环境会被标记为 `unbound`，而不是留白：空主机不等于没有主机，连接构建器会把它读作 `localhost:5432`。[tool-verified: `BOUND_COLUMN = "bound"` at
env_classes.py line 143; `BINDING_COLUMNS` dict at env_classes.py lines 155-172]

一个环境的数据源有两种解析方式。

**基座**——环境自带凭据。org_admin 创建一个基座，然后逐一显式绑定每个数据源。[tool-verified: `CreateEnvBody.inherit_connections = False` (default) at
environments_router.py line 227; "binding a base is an org_admin's act" comment at line 358]

**分支**——环境按引用继承基座的凭据。什么都不复制。当查询需要连接时，解析会沿 `branched_from` 链向上走，在第一个其行已绑定的环境处停下。在基座上轮换凭据会传播到它的每一个分支，无需任何操作。吊销它则一次性对所有分支生效。任何密钥都不会在分支、导出或仓库可能带走它的地方被物化出来。
[tool-verified: `resolve()` at env_bindings.py lines 114-151; `lineage()` at env_bindings.py
lines 74-102; env_bindings.py module docstring lines 11-33]

要创建分支，请在环境面板中勾选**继承连接**。默认为关闭。
[tool-verified: `environmentsTab.json` key `inheritConnections`; `inheritHelp2` string]

## git 投影

对模型的每一次写入都会把结果提交到该环境的 git 分支。仓库是模型的投影，绝不是它的权威：Provisa 读写控制平面；仓库是记录，而不是来源。部署一棵树需要一次显式调用——在 git 托管方合并一个拉取请求并不会自行部署（REQ-1524、REQ-1526）。[tool-verified:
deploy endpoint docstring at environments_router.py lines 777-791]

每个实体对应一个文件。路径是去掉协议和组织后的 REQ-1385 URI：`provisa://acme/sales/tables/Order` 变成 `sales/tables/Order.yaml`。数据源落到 `sources/`，命令落到 `commands/`，指标落到 `metrics/`。由父级级联而来的子行——列、关系、RLS 规则——写在父级的文件内部，而不是各自成文件。
[tool-verified: `table_path()` at env_files.py line 109-115; `kind_path()` at env_files.py
lines 118-120; `COMMANDS_DIR = "commands"` at env_project.py line 71; env_files.py module
docstring lines 17-24]

命令及其标签分配能完整往返。命令上的标签会被路由到该命令自己的文件（`commands/<name>.yaml`）；不属于任何文件的标签会从投影中消失，并会在该树的下一次部署时被删除。[tool-verified:
env_project.py lines 346-364; `owner_command_name` routing in `_assignments_for()` at
env_project.py lines 137-164]

没有代理键会进入文件。`registered_tables.id` 是自增整数——同一个模型在两个环境中会拿到不同的整数，因此一份天真的转储会与自身产生差异。所有代理键都被丢弃，所有对它们的引用都写成目标的路径。
[tool-verified: `STORAGE_COLUMNS` and `_model_columns()` at env_files.py lines 62-128;
env_project.py docstring lines 26-27]

序列化是确定性的。键按字母序输出，子集合按各自的地址排序，YAML 样式固定。持有同一模型的两个环境会产出逐字节相同的树。[tool-verified: `dump()` at env_files.py lines 131-143]

## 合并

把一个环境的模型合并到另一个环境，是按身份更新：源方拥有的每个对象都会在目标方被创建或更新。源方不再拥有的对象，只有在调用方显式请求删除时才会被移除。中途失败的合并会让目标保持原样——一个事务。[tool-verified: `copy_model()` at env_copy.py lines 216-234; REQ-1490 description]

在应用之前，先调用预览终结点（`GET /{name}/merge-preview`）或传入 `dry_run: true`。预览走的是合并所用的同一条代码路径；它是一个 `GET` 终结点，因此把标志写错的 CI 脚本不会误把它本想查看的合并给应用了。[tool-verified:
`preview_merge()` docstring at environments_router.py lines 1086-1095]

合并会让目标的绑定、角色和密钥保持原封不动。开发环境不会因为从 prod 取来一份更新的模型而丢掉自己的数据库连接。prod 也不会获得 dev 的授权。[tool-verified: env_copy.py lines 269-287; REQ-1490 scenario]

### 报告点名了什么

合并报告按路径列出新增、变更、移除以及保持不变的内容。它还会点名任何**冲突**——自双方上次共享提交以来两边都改动过的对象。冲突只报告不解决：源方胜出，这正是"合并进目标"的含义。Provisa 不提供冲突解决、不提供合并标记、不提供逐对象抉择。冲突清单的价值在于信号——两个人在互不知情的情况下编辑了同一个对象（REQ-1555）。[tool-verified: `CopyReport.conflicts` at env_copy.py lines 151-165; `detect_conflicts()` called
at env_copy.py lines 261-263; REQ-1555 description]

双方都改成同一个值的对象是一致，不是冲突。当两个环境完全没有共同祖先时，报告中的 base 为 `None`，此时空的冲突列表意味着什么都没有比较过，而不是什么都没有相撞。[tool-verified: `CopyReport.compared`
property at env_copy.py lines 164-166; env_copy.py lines 255-264]

合并会作为一个压缩提交落在目标的分支上。提交信息是必填的，且不得为空——它是那次压缩所代表的整段工作的唯一交代。源方的提交留在原处，之后仍可按 SHA 部署。
[tool-verified: `_squash()` docstring at environments_router.py lines 663-680;
`MergeBody.message` comment at environments_router.py lines 258-260]

## 拉取

拉取会取来远端为某个环境所持有的内容，并使其成为模型。它不会直接快进本地分支；它把取来的树经由普通的部署路径应用，因此管辖手动部署的那套校验和审计也同样管辖拉取。
[tool-verified: `pull_environment()` docstring at environments_router.py lines 1450-1462]

与合并一样，拉取会报告它覆盖了什么——即自两条线上次共享提交以来，本地环境也改动过、而传入的树同样改动了的那些对象。未提交的本地改动意味着环境已漂移（见下文"历史"）；拉取会在报告中把它当作一处普通变更点名。[tool-verified: REQ-1556 description; `pull_environment()` at environments_router.py
lines 1485-1519]

当两条线已经**分岔**时——双方各自持有对方没有的提交——拉取会被拒绝。拒绝信息附带双方都触碰过的对象清单，好让现在必须决定谁的工作留下的那个人知道该看哪些对象。[tool-verified: `state["diverged"]` check at
environments_router.py lines 1491-1503; `_collisions()` at environments_router.py
lines 1581-1602]

## 历史

每次部署都会让环境的游标在它自己的提交线上向前移动。撤销回退一个提交；重做则朝着撤销出发时的位置再向前一步。两种操作都不会移除提交——回退是新增一个位置，而不是改写历史。
[tool-verified: `_move()` docstring at environments_router.py lines 854-868]

分支是在其创建来源环境的顶端播种的，因此撤销会停在那个播种点，不会走到父环境的提交上。[tool-verified:
`origin_sha` comment at environments_router.py lines 428-448; `_move()` at
environments_router.py lines 907-916]

`can_undo` 和 `can_redo` 标志随环境列表响应一同返回。当投影中没有控制平面所指名的那个提交时，两者都报告 `false`——这是设计所承认的一种状态，称为**已漂移**。仓库存储从未收到过某个特定提交的节点，仍会列出它的环境；只是历史相关的答案会变（REQ-1561）。[tool-verified: `_with_history()`
at environments_router.py lines 316-344; REQ-1561 description]

## 授权

环境由两项权限管辖。默认情况下，分析师两项都不持有（REQ-1573）。
[tool-verified: REQ-1573 description; `MANAGE_CAPABILITY = "environment_management"` and
`SWITCH_CAPABILITY = "environment_switch"` at environments_router.py line 110 and
env_routing.py line 53]

| 权限 | 谁持有它（初始播种） | 它管辖什么 |
| --- | --- | --- |
| `environment_management` | org_admin、developer | 创建和删除环境 |
| `environment_switch` | org_admin、developer | 由 prod 之外的任何环境提供服务 |

`prod` 不需要任何权限——它就是未指名任何环境的请求所对应的服务方，对它设限等于拒绝每一个请求。

强制执行发生在选择点，早于到达任何路由。缺少 `environment_switch` 的成员会在所有界面上被同时拒绝——HTTP、GraphQL、SQL 以及各种线协议——因为环境是在中间件中绑定的，而不是在各个处理器里。
[tool-verified: `select_environment()` at env_routing.py lines 93-129; env_routing.py
module docstring lines 28-34]

不持有任何环境权限的分析师可以查询 `prod`，且看不到环境切换器。被授予分析师角色的外包人员看不到环境相关界面，也无法创建或切换到生产以外的任何环境。[tool-verified: REQ-1573 use_case and scenario]

### 环境所有者的权限

创建环境是只读成员获得模型编辑权的唯一途径（REQ-1528）。在自己创建的环境内部，创建者持有 `developer` 角色的各项能力——减去数据权限（`write`、`full_results`、`usage`）。是建模权，不是数据权。[tool-verified: `ENVIRONMENT_OWNER_CAPABILITIES` at env_authority.py lines 75-77;
`_DATA_RIGHTS` at env_authority.py lines 74-77; env_authority.py module docstring lines 14-38]

该授予是在授权时从 `environments.created_by` 推导出来的，绝不写入任何授权表。删除该环境的同一个动作就把它一并撤除。
[tool-verified: env_authority.py module docstring lines 39-42; `environment_owner()` at
env_authority.py lines 84-98]

域成员资格仍然限制所有者可以改动什么。开分支改变的是成员可以做什么；它绝不改变他们可以对哪些域去做（REQ-1530）。
[tool-verified: `domains_within()` at env_authority.py lines 121-145]

## 受保护的环境（REQ-1504）

环境可以被设为受保护。向受保护环境的合并或部署不会在请求时被应用；它会被提出为一项提案，且必须由请求者之外的人批准。

一旦组织成员超过一人，`prod` 会自动受保护。单成员组织无法满足"请求者之外的人"，因此该规则在那里不适用——否则会让 `prod` 无法合并。org_admin 可以把任何环境标记为受保护。
[tool-verified: `is_protected()` at env_approvals.py lines 79-96; `protectedHelp2` UI string
in environmentsTab.json line 28]

合并请求是一行记录，而不是一个确认对话框。批准者按定义就是与请求者不同的人，且在请求发出的那一刻并不在场；临时性的确认框会迫使批准发生在请求者的会话内部，而那恰恰是该需求所禁止的安排。[tool-verified: env_approvals.py module docstring lines 11-17]

请求行携带合并报告以及请求者的留言。是否过期是在读取时推导的，从不存储：在读取时重新规划并与已存报告比对，是唯一不会出错的做法。过期的请求必须重新发起。请求者不能批准自己的请求。[tool-verified: `STALE` constant and `effective_state()` at
env_approvals.py lines 53, 215-243; `decide()` lines 265-268]

请求的生命周期状态：`requested` → `approved`/`rejected` → `applied`。`stale` 是推导出来的。
[tool-verified: `REQUESTED`, `APPROVED`, `REJECTED`, `APPLIED`, `STALE` at env_approvals.py
lines 47-53]

同一道门也处理来自仓库 ref 的部署：请求在提案时就把 SHA 钉住。如果该 ref 在提案与决定之间移动了，批准者读到的是被钉住那个提交的报告，而不是新的。[tool-verified: `request_deploy()` at env_approvals.py lines
150-189; env_approvals.py docstring lines 26-27]

!!! note
    合并请求的 UI 位于环境面板的**合并请求**选项卡下。
    **报告**列按数量展示会有什么变化；展开该行可查看逐对象的详情。[tool-verified: `environmentsTab.json` keys `requestsTitle`, `colReport`,
    `approve`, `reject`]

## `env` CLI 命令

`provisa env deploy` 把某个 ref 上的模型送入一个环境。当部署已应用或是一次演练时退出码为 0；当环境受保护、部署只是被提出为提案时退出码为 2——把待批准当作已发布部署的流水线会是错的，退出码把这一点说清楚。[tool-verified: `_cmd_env_deploy()` at cli.py lines 389-411]

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` 把组织的远端分支取到本地仓库。之后部署就可以指名 `origin/<branch>`。[tool-verified: `_cmd_env_fetch()` at cli.py lines 414-426]

```
provisa env fetch --org acme --api <url> --token <token>
```

两个命令都接受 `--api`（Provisa API 的 URL）和 `--token`（bearer 令牌）。在环境中设置 `PROVISA_API_URL` 和 `PROVISA_API_TOKEN`，即可免去每次调用都传它们。
[inferred: shared `_api_call()` helper]

仓库驱动工作流的典型 CI 流水线：

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## 另请参阅

- [部署](deployment.md) —— 如何搭起环境所连接的控制平面
- [命令](commands.md) —— 出现在每个环境树中的受跟踪函数与 webhook
