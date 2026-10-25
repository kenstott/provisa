# 代码解析器

用 Python 或 Node 编写的业务逻辑。

如果你写过 GraphQL 服务器，就熟悉这种模式。代码解析器是为一种对象类型作答的函数：在 GraphQL 中，标记了 `@table` 的函数*就是*其表类型的 resolver。Provisa 用查询的参数调用它，并对返回内容进行治理。

与手写 resolver 的不同之处：

- **受治理。** 可见性、掩码和行规则作用于函数返回的行，与任何表一样。
- **批量。** 接收 `loader` 参数的函数在一次调用中拿到一个 join 的全部键，即 DataLoader 模式。
- **下推。** 在你要求时，列上的过滤条件会作为参数传给你的函数。
- **处处可用。** 同一个函数回答 GraphQL、SQL（通过 HTTP、pgwire 和 Arrow Flight）、Cypher（通过 HTTP 和 Bolt）、gRPC、REST、JSON:API 和 MCP。

代码解析器是在 Provisa 中编写业务逻辑的三种方式之一。见 [Provisa 中的业务逻辑](business-logic.md)。

## 第一个 resolver {: #an-example }

```python
from dataclasses import dataclass


@dataclass
class Region:
    code: str
    name: str


def regions() -> list[Region]:
    """Sales regions.

    @table
    """
    return [
        Region(code="emea", name="Europe, Middle East and Africa"),
        Region(code="amer", name="Americas"),
        Region(code="apac", name="Asia Pacific"),
    ]
```

提供服务的步骤：

1. 添加一个 `python` 数据源并指向代码：PyPI 包、Git 提交、上传文件或文件夹（见[代码从哪里来](#where-the-code-comes-from)）。
2. 保存。Provisa 安装并读取代码，把 `regions` 作为一张表提供出来，包含 `code` 和 `name` 两列。
3. 数据管家在一个域（这里是 `sales`）中注册该表，并像其他表一样授予访问权限。

然后在 GraphQL 中读取它：

```graphql
{
  regions {
    code
    name
  }
}
```

或者在 SQL 中：

```sql
SELECT code, name
FROM sales.regions
```

Node 的用法相同，使用 JSDoc 标签和 TypeScript 类型：见 [Node](#node)。

## 构建 {: #build }

### 函数会变成什么 {: #what-a-function-becomes }

Provisa 读取代码自身的声明：Python 中的 docstring 和类型提示，Node 中的 JSDoc 标签和 TypeScript 类型。

| 标签 | 变成 |
|---|---|
| `@table` | 一张表。函数返回列表或可迭代对象，每个元素是一行。 |
| `@command` | 一个命令，按请求运行。 |
| `@stream` | 一张实时表。函数是生成器，每产出一行就发送一行。 |
| `@columns` | 一个列转换：输入一批行，每行输出一个结果。见[列转换](#column-transforms)。 |

- 没有标签的函数会被忽略。
- Provisa 不认识的标签会被拒绝，无法映射的类型也会被拒绝。拒绝信息会一次列出所有问题，方便你一次改完。

行类型决定列：

- 文本、数字、布尔值、日期和时间戳映射为对应的列类型。列表保持为列表，嵌套对象变为具名类型。
- 在 Python 中，行类型可以是 dataclass、`TypedDict`、`NamedTuple`、Pydantic 模型或任何带注解字段的类。Pydantic 模型的行按 `model_dump(by_alias=True)` 的结果发送，因此带别名的字段会成为以别名命名的列。
- 在 TypeScript 中，行类型可以是接口、类型别名或类。
- 注册表时，字段自身的文档会成为其列的描述：Pydantic 的 `Field(description=...)`，或 TypeScript 属性上的文档注释。
- 宽松的行类型（Python 中的 `Any` 或 `dict`，TypeScript 中的 `any`、`unknown` 或 `Record`）没有可读取的列。Provisa 会不带参数调用函数一次，并根据返回的行建议列。如果函数有必填参数，就无法这样调用，你需要在注册表时自己填写列类型。

### 参数 {: #parameters }

函数的参数成为其表的参数。下面这个函数为一个账户打分：

```python
@dataclass
class Risk:
    score: float
    reasons: list[str]


def churn_risk(account_id: str) -> list[Risk]:
    """Churn risk of one account.

    @table
    """
    return [Risk(score=0.82, reasons=["no logins in 30 days"])]
```

在 GraphQL 中，参数是字段参数：

```graphql
{
  churnRisk(account_id: "A-1042") {
    score
    reasons
  }
}
```

在 SQL 中，它是一个 `_nf_` 条件：

```sql
SELECT score, reasons
FROM accounts.churn_risk
WHERE _nf_account_id = 'A-1042'
```

- 没有默认值的参数是必填的。省略它的读取会在代码运行前被拒绝。
- 有默认值的参数是可选的。省略时使用你的默认值。
- 数据管家仍可以把可选参数设为必填。

### 函数能收到什么 {: #what-reaches-your-function }

一次调用只收到读取的参数，请求中的其他内容一概收不到。每个参数的值来自字段参数、`_nf_` 条件、下推给它的过滤条件或 join 的键。你的函数永远看不到是谁在提问：调用者的身份和角色留在 Provisa，由它治理函数返回的行。

下推正是建立在这一点之上。查询中的过滤条件成为某个参数的值，因此函数可以只取所请求的行。

### 过滤下推 {: #pushdown }

对某个返回列的过滤可以作为参数传给你的函数，让它只获取所需的行。用以下三种方式之一声明由哪个参数接收：

- **按名称。** 与某个返回列同名的参数接收该列上的相等条件。名为 `<op>_<column>` 的参数接收该运算符，例如 `gte_score` 或 `in_region`。
- **按标签。** 在 docstring 或 JSDoc 中写一行 `@<op>_<column> <parameter>`（或 `@<column> <parameter>`），或在 TypeScript 参数前写内联的 `/* @<op>_<column> */`。
- **按 `Annotated`。** 在 Python 中，在参数的类型提示上写字符串 `"<op>_<column>"`（或 `"<column>"`）。

运算符：

| `op` | 接收 |
|---|---|
| `ne`、`gt`、`gte`、`lt`、`lte` | 与列同类型的值 |
| `in` | 与列同类型的列表 |
| `like` | 文本，作用于文本列 |

- 列上的范围会填满它的两个参数：`score BETWEEN 0.5 AND 0.9` 得到 `gte_score` 和 `lte_score`。
- 列名完全按你在行类型中声明的样子匹配。只有在 Provisa 为读者重命名列之后才匹配的名称不算匹配。
- 函数类型无法接受的声明会被拒绝。
- 过滤条件仍会作用于函数返回的行，因此忽略或放宽所给值的函数不会返回错误的行。

### 批量键：`loader` 参数 {: #batched-keys }

join 可以连到代码表，由另一张表的行提供其参数：比如把客户与其流失评分关联起来。默认情况下，Provisa 会为 join 行给出的每组不同参数调用一次函数，并把所有这些调用放在一个请求里发给 worker。

接收名为 `loader` 参数的函数则改为每批调用一次，这是 GraphQL 的 DataLoader 推广开的模式。

- **接收什么。** `loader` 是一个哈希：一个 dict（TypeScript 中是对象），从每个键的稳定哈希映射到键本身。键是一个 JSON 对象，包含 join 行为函数其他参数给出的值。
- **去重。** 每种不同的取值组合只出现一次。
- **稀疏。** 没有值的字段不会出现在键中，因此会使用函数自己的默认值，与单次调用一样。
- **返回什么。** 一个从收到的每个键映射到该键对应行的哈希：表的普通行，可以没有也可以很多。Provisa 把每个键的行交给共享该键的每个 join 行。
- **按哈希匹配。** 函数会用默认值填补稀疏键缺失的字段，因此它看到的值与 join 行的值不同。哈希由 Provisa 计算，函数只需原样返回收到的键。
- **类型照常。** 函数仍只声明其行类型，即行的列表。
- **需要默认值。** `loader` 之外的参数需要默认值，因为 loader 调用只传 `loader`。
- **批大小。** 像 DataLoader 的 `maxBatchSize` 一样设上限，在以下三处之一写 `loader N`：在 `@table` 旁写一行 `@loader 100`，在 TypeScript 的 `loader` 参数前写注释 `/* @loader 100 */`，或在 Python 中用 `Annotated` 字符串，`loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`。Provisa 随后把键按该大小分批，并按哈希合并结果。不设大小时，一批包含所有键。
- **拒绝。** 在没有 `loader` 参数的函数上设大小、大小不是正整数、一个函数上有两个大小。

当一次处理多个键的调用比多次处理一个键的调用更好时，使用 `loader`：

- 在一次向量化计算中对整批数据运行模型评分。
- 把请求批量发给一次能接收多个输入的 API 或语言模型。
- 每批只做一次准备工作，例如加载模型或打开连接。

```python
from dataclasses import dataclass
from typing import TypedDict


class CustomerKey(TypedDict, total=False):
    customer_id: str
    plan: str
    region: str


@dataclass
class Score:
    customer_id: str
    score: float


def churn_score(
    customer_id: str | None = None,
    plan: str = "basic",
    region: str = "US",
    loader: dict[str, CustomerKey] | None = None,
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or {"": {"customer_id": customer_id, "plan": plan, "region": region}}
    rows = {h: {"plan": "basic", "region": "US", **key} for h, key in keys.items()}
    scores = MODEL.predict(features(list(rows.values())))  # one vectorized call for the batch
    out = {h: [Score(r["customer_id"], s)] for (h, r), s in zip(rows.items(), scores)}
    return out if loader else out[""]  # a loader call returns the hash of key to rows
```

与客户表 join 时，它会以类似 `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}` 的 `loader` 被调用。第二个键只带 `customer_id`，因此其套餐和区域取默认值。函数返回 `{"9f2c...": [...], "41ab...": [...], ...}`。

### 列转换：`@columns` {: #column-transforms }

标记为 `@columns` 的函数会根据你已有行的列计算出新列：把地址拆分成各个部分、对描述进行分类，或丰富一个名称。它接收一批行，并按相同顺序为每行返回一个结果。

每个参数是一个输入列，以该批次按行顺序排列的值列表形式传入。函数返回一个列表，每个输入行对应一个对象，对象的字段就是输出列。用 Python 写：

```python
@dataclass
class Address:
    street: str
    city: str
    country: str


def clean_address(raw_address: list[str]) -> list[Address]:
    """Each raw address of the batch, as its parts.

    @columns
    """
    ...
```

在 TypeScript 中，把它声明为 `clean_address(raw_address: string[]): Promise<Address[]>`，并在其 JSDoc 中加上 `@columns`。

你把它注册为命令，像其他命令一样一次注册一个：在 **Commands** 页面上选择数据源，它的 `@columns` 函数就会出现在可选列表中。函数的声明提供输入列和输出列。把它注册到一个网域，并指定可以使用它的角色。

在 select 列表中应用它，它的输出列会取代这次调用：

```sql
SELECT id, clean_address(raw_address) FROM suppliers
```

这会返回 `id, street, city, country`。如果需要其他名称，请在外层查询中重命名输出。调用必须是语句自身 select 列表中的一项：Provisa 在行读取之后才对其运行函数，因此出现在 `WHERE`、`JOIN`、`GROUP BY` 中或嵌在其他表达式里的调用都会被拒绝，给调用加别名也会被拒绝。每个输出列都是投影血缘中一个具名的步骤。

行以每批 1,000 行的方式交给函数。在两次调用之间不保留状态的函数，可以让各批次分布在数据源的 worker 上并行运行：在数据源表单上设置 **Workers**（其 mapping 中的 `workers`，见 [dev 模式与 worker](#workers)）即可运行多个，每增加一个 worker 就增加一份吞吐量。如果某批返回的结果数与它收到的行数不同，就会被拒绝，并指明是哪个函数。

函数可以调用 Provisa 的服务，其中包括 LLM 服务，因此一次模型调用就能清洗、分类或丰富整批数据。每次调用都会像数据源的其他调用一样被计量并受数据源的上限约束（[调用模型](#calling-a-model)）。

应用了列转换的视图会被落地，无论你是否把它声明为物化视图：Provisa 会像构建物化视图那样构建它，按批对视图的行调用函数并存储结果，视图随后从这张存储表中读取。之后语句就可以像对待其他列一样，对输出列进行过滤、连接或分组。每次视图刷新时，输出都会按其新鲜度声明重新计算。它们没有自己的调度。

### 代码从哪里来 {: #where-the-code-comes-from }

- **一个包。** Node 用 npm 包，Python 用 PyPI 包，固定到确切版本。
- **一个 Git 仓库。** 完整的 40 字符提交，如果代码不在顶层则指定子目录。
- **一次上传。** 代码的 `.zip`、`.tgz` 或 `.tar.gz`。
- **你电脑上的文件夹。** **选择文件夹**会在浏览器中把文件夹打包成 zip（排除 `node_modules`、`.venv`、`__pycache__` 和 `.git`），并作为数据源的归档上传。在能保留文件夹的浏览器（Chrome、Edge）中，**保存**会重新打包并再次发送；其他浏览器会再次让你选择文件夹。
- **服务器上的目录。** 在单组织安装中，可以是运维人员允许的目录之下的某个目录。它在原处被读取，因此修改会被下一次查询读到。多组织的服务器会拒绝它。

除服务器目录外，每种来源都是固定的，因此数据源运行的正是保存时的代码。

- **入口模块。** 当 Python 代码有多个顶层模块时，指明从哪个进入。
- **在 VS Code 中打开。** 服务器目录会在你安装的 VS Code 中打开；Git 来源会在 vscode.dev 上按其提交打开。包或上传没有此链接。

### 安装依赖 {: #installing-dependencies }

有依赖的代码需要锁文件，每次安装都严格按它进行。没有锁文件时，声明了依赖的包会被拒绝，拒绝信息会提示你添加一个。

| 运行时 | 锁文件 |
|---|---|
| Node.js | `package-lock.json` 或 `npm-shrinkwrap.json` |
| Bun | `bun.lock` 或 `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock`（及其 `pyproject.toml`）、`pylock.toml`，或每行都带 `--hash` 的 `requirements.txt` |

- **自带或另给。** 自带锁文件的代码从它安装：带锁文件的 Git 仓库、上传或文件夹，或作者发布了 `npm-shrinkwrap.json` 的 npm 包（npm 从不发布 `package-lock.json`）。大多数 npm 包和 PyPI 上的 Python 包都不自带，因此请在旁边另给一个。
- **为什么重要。** 没有锁文件时，同一份代码的两次安装可能不同，今天通过的测试明天可能因新的依赖版本而失败。
- **构建好的镜像本身就是固定的。** 一旦你[构建数据源的镜像](#running-on-a-remote-host)，依赖就在镜像里。锁文件对 dev 模式和每次新构建都有意义。
- **不运行安装脚本。** npm 使用 `--ignore-scripts` 安装，Python 只安装 wheel，从不从源码分发构建。
- **你的仓库。** 包来自你所在组织自己的 npm 仓库、PyPI 索引和 Git 主机，只需在 **Admin → Package registries** 下填写一次。公共 npm 仓库和 PyPI 无需填写。见 [Package Registries](package-registries.md)。
- **缓存。** 安装按代码的内容哈希缓存，同一份代码只安装一次。

### Node {: #node }

- Node 代码可运行在 Node.js、Bun 或 Deno 上，由你选择。
- 标签写在 JSDoc 中，类型来自 TypeScript。

TypeScript 版的第一个 resolver：

```typescript
export interface Risk {
  score: number;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(account_id: string): Promise<Risk[]> {
  return [{ score: 0.82, reasons: ["no logins in 30 days"] }];
}
```

TypeScript 版的批量 `churnScore`：

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id?: string,
  plan = "basic",
  region = "US",
  loader?: Record<string, CustomerKey>,
): Promise<Score[]> {
  const keys = loader ?? { "": { customer_id, plan, region } };
  const rows = Object.entries(keys).map(([h, key]) => [h, { plan: "basic", region: "US", ...key }] as const);
  const scores = await model.predict(rows.map(([, row]) => row)); // one call for the whole batch
  const out = Object.fromEntries(
    rows.map(([h, row], i) => [h, [{ customer_id: row.customer_id, score: scores[i] }]]),
  );
  return loader ? out : out[""]; // a loader call returns the hash of key to rows
}
```

## 部署 {: #deployment }

### dev 模式与 worker {: #workers }

数据源默认运行在 Provisa 服务器上，这就是 dev 模式。

- 你的代码运行在自己的 worker 进程中，每个 Provisa 进程里每个数据源及其代码版本各一个，在首次调用时启动。
- 崩溃的 worker 会让正在进行的调用失败，并指明数据源和函数。下一次调用会启动新的 worker。
- 请求的截止时间一到，调用就会被取消：worker 会被告知停止该调用，等待中的请求会立即释放。

### 读取表 {: #reading-a-table }

读取如何到达你的函数，取决于它还读取什么：

- **只读取一张代码表**时，直接交给你的函数。不会落地任何数据。Provisa 用读取的参数和下推的过滤条件调用它，并把语句的其余部分应用到返回的行上。
- **与其他表 join 的读取**交给联邦引擎。dev 模式数据源的表会为此落地：这些行按对应参数保留，时长为表的缓存 TTL。
- **已构建数据源的表**在这种 join 中，会从其远程主机实时读取，引擎把该主机作为 Postgres 目录挂接。以下情况则改为落地：其数据源被复制或设了下限、该表被永久保留、它声明了大于 0 的落地 TTL，或引擎无法挂接主机。

无论哪种情况，可见性、掩码和行规则都作用于这些行，与任何表一样。传参方式与任何表参数相同：SQL 中用 `_nf_` 条件，GraphQL 中用字段参数。

### 在远程主机上运行 {: #running-on-a-remote-host }

当你的代码需要服务器以外的机器，例如 GPU 主机或另一网络中的主机时，构建它的镜像并在那里运行。数据源随即以构建模式运行，其表、授权和读者保持不变。

**构建镜像。**

- 先在 dev 模式下保存数据源，让其代码被读取并固定。
- 在表单的 **Remote host** 下，选择架构（Linux x86-64 或 ARM64）以及镜像的去处：你所在组织的某个容器仓库（**Admin → Container registries**），或下载。
- Provisa 自己在最小基础镜像上组装镜像，不需要 Docker 守护进程：固定哈希的代码、通过你的包仓库按其锁文件为该平台安装的依赖、固定的运行时，以及远程主机程序。构建期间镜像中的任何东西都不会运行。
- 镜像标签包含代码哈希和 Provisa 版本，其标签说明了运行时、代码哈希和导出项。

**运行它。** 表单会显示该镜像的 compose 服务。下载的镜像需先用 `docker load -i <file>` 加载。容器在 7443 监听，并把身份保存在其卷上。主机端口用 `PROVISA_HOST_PORT` 选择；没有它服务不会启动。

```yaml
services:
  provisa-code-rates:
    image: ghcr.io/example/provisa-code-rates:3f9a1c2e7b40-0.1.0
    restart: unless-stopped
    ports:
      - "${PROVISA_HOST_PORT:?set PROVISA_HOST_PORT to a free port on this host}:7443"
    volumes:
      - provisa-code-rates-state:/var/lib/provisa
volumes:
  provisa-code-rates-state: {}
```

首次启动时，容器会在日志中打印一行以 `pairing code:` 开头的内容。

**连接它。**

- 在表单中填写 Provisa 访问容器所用的主机、你发布的端口和配对码。在共享的 Docker 网络或 Kubernetes 中，填写容器名或 Service 名以及 7443，无需发布端口。
- Provisa 通过双向 TLS 与容器配对。配对码只用一次；之后双方只接受对方。
- Provisa 检查容器运行的是该数据源的代码并提供其导出项，然后把数据源切换到构建模式。
- 每个 Provisa 进程都保持与主机的独立连接，主机不再响应时会重新连接。在它不响应期间，对该数据源表的读取会失败并指明主机。

**如果代码变了。**

- 回来时运行的是其他代码的容器会被拒绝，直到数据管家审核代码并在表单中重新固定。
- 要修改代码本身，把数据源切回 dev，保存新代码，再重新构建。

### Provisa 的服务 {: #provisas-services }

你的代码可以导入一个 Provisa 在服务器上始终提供的模块：Python 中是 `provisa_services`，Node.js、Bun 和 Deno 中是 `@provisa/services`。无需安装，它也不在任何包仓库中。在 Provisa 之外它不存在，因此你的测试需要模拟它。

```python
import provisa_services as ps

region = ps.settings.get("region")
rates = ps.cache.get("rates") or fetch_rates()
ps.cache.set("rates", rates, ttl=3600)
```

```typescript
import { settings, cache } from "@provisa/services";

const region = await settings.get("region");
const rates = (await cache.get("rates")) ?? (await fetchRates());
await cache.set("rates", rates, 3600);
```

| 调用 | 作用 |
|---|---|
| `secrets.get(name)` | 数据源声明的某个密钥，当下从保管库读取。见[密钥](#secrets)。 |
| `settings.get(name, default)`、`settings.all()` | 数据源的设置，来自表单上的 **Settings**，数据管家无需改代码即可修改。缺失且无默认值时会抛出错误。 |
| `cache.get(key, default)`、`cache.set(key, value, ttl)`、`cache.delete(key)` | 数据源自己的缓存，由它的所有 worker 共享。值可以是 JSON 能容纳的任何内容，最大 1 MB，保留 `ttl` 秒或直到被替换。 |
| `vectors.upsert(items, model)`、`vectors.search(query, k, model)`、`vectors.embed(texts, model)`、`vectors.delete(ids)` | 数据源自己的向量集合，用于它自己的参考数据。一个条目有 `id` 以及 `text` 或 `vector`；文本由你所在组织的某个向量模型嵌入。它无法搜索你受治理的表，那些数据只以参数形式到达代码。 |
| `http.get(url)`、`http.post(url, ...)` | Provisa 代表代码发起的请求，经过部署的出站代理和证书颁发机构。主机必须在出站允许列表中，每次请求都会记入审计记录。 |
| `llm.chat(model, messages)`、`llm.embeddings(model, input)`、`llm.base_url()`（JavaScript 中为 `llm.baseUrl()`） | Provisa 的 LLM 服务。见[调用模型](#calling-a-model)。 |
| `log(message, level, fields)`、`metric(name, value, kind, attributes)` | 一行日志，以及名为 `code_source.<name>` 的指标点，附加到你的代码正在处理的请求上。 |

- 服务拒绝的调用会抛出 `ServiceError`，指明服务并说明原因。
- 这些服务只响应运行在 Provisa 服务器上的代码。在远程主机上，每次调用都会抛出错误。

#### 密钥 {: #secrets }

付费 API 的密钥应放在你所在组织的保管库里，而不是代码中。

- 在数据源表单的 **Secrets** 下，选择代码可以读取的组织密钥。在配置文件中，把它们列在数据源的 mapping 里：

    ```yaml
    mapping:
      secrets: [openrouter]
    ```

- 单独的名称指同名的组织密钥。若要改为读取部署的环境变量，请给出引用：`secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`。
- 该列表引用保管库，从不复制值，因此即使配置中命名的密钥尚无人存储，配置仍能加载。

你的代码用 `secrets.get(name)` 读取密钥，或直接从 `$PROVISA_SERVICES/secrets/<name>` 读取：

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

- `PROVISA_SERVICES` 是你的代码得到的唯一环境变量。该请求与代码的其他流量走同一个出站代理，由代理自己应答；Provisa 的其他部分都无法这样访问。
- 该地址带有只有该数据源的 worker 持有的令牌。它只能读取数据源列出的名称，worker 停止后即失效。
- 每次读取都在当时从保管库解析密钥，因此轮换后的密钥在下一次读取时生效。
- 数据源未列出的名称会被拒绝，无法解析的密钥也会被拒绝。拒绝信息会指明密钥，但绝不包含其值。
- 每次读取都会连同数据源、名称和时间记入组织的审计记录。任何值都不会写入日志。
- API 所在的主机仍须在出站允许列表中。

#### 调用模型 {: #calling-a-model }

你的代码可以通过 Provisa 调用语言模型，而无需持有供应商密钥。`$PROVISA_SERVICES/llm/v1` 是兼容 OpenAI 的端点，因此任何 OpenAI SDK 只要把基础 URL 指向这里即可使用。该地址就是凭证，所以 SDK 要求的密钥可以是任意文本。

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

- **按角色。** 把模型写成 `provisa:<role>`，你会得到组织在 **Admin → AI Models** 下为该角色指定的模型；管理员更改选择后它会随之改变。当某个模型被指定了角色时，你也可以直接写出该模型。
- **嵌入。** `$PROVISA_SERVICES/llm/v1/embeddings` 接受你所在组织的任何向量模型。
- 其他任何模型都会被拒绝。
- 调用通过你所在组织自己的供应商、端点和密钥发出，发送的内容会先经过 Provisa 的脱敏规则。
- 不提供流式输出。请发送 `stream: false`，这也是默认值。

## 安全保障 {: #safeguards }

### 沙箱 {: #the-sandbox }

- 你的代码在自己的 worker 进程中、在沙箱里运行。它能读取自己已安装的代码，磁盘上的其他内容一概不能读取。
- 它只能通过 Provisa 运行的代理访问部署出站允许列表中的主机，且永远无法访问 Provisa 自己的端口。
- 无法提供沙箱的主机会拒绝启动 worker 并指明缺少什么；代码绝不会在无约束状态下运行。在 Windows 上，代码数据源会被拒绝。
- 在远程主机上，容器的网络策略就是代码的出站边界。服务器的出站允许列表适用于 dev 模式。

### 拒绝 {: #refusals }

以下每种情况都会被拒绝，能在代码运行前拒绝的都会在运行前拒绝：

- 未知标签或 Provisa 无法映射的类型，一次列出一个函数的所有问题。
- 省略必填参数的读取。
- 函数类型无法接受的下推或 `loader` 声明。
- 有依赖但没有锁文件的包。
- 不在允许范围内的服务器目录，或在多组织服务器上的服务器目录。
- 运行的代码与数据源固定的代码不同的远程主机。
- 数据源未列出的密钥、不允许的模型、服务不予响应的服务调用。

### 计量与上限 {: #metering }

- 每次模型调用都会为数据源记录模型、token 数，以及按 **Polly model prices** 中该模型条目计算的费用，并写入组织的审计记录。
- 要限制数据源的花费，在其表单上设置 **LLM monthly cap**，单位为美元。在配置文件中，它写在 mapping 里：

    ```yaml
    mapping:
      llm: {cap_usd: 20}
    ```

- 设了上限的数据源只能调用在那里有价格的模型。
- 一旦当月（UTC）花费达到上限，其调用会被拒绝，直到进入下个月。达到上限时已在进行的调用仍会完成，因此当月可能略超上限。

### 结果治理 {: #governance }

- 函数返回的行会按表的列进行校验。
- 随后，可见性、掩码和行规则会针对每个读者生效，与任何表一样。
- 你的代码只能以读取所给参数的形式看到受治理表的数据。

### 追踪 {: #tracing }

- 每次对你代码的调用都作为触发它的请求的一部分被追踪：函数、类型、收到的参数个数（从不记录参数值）、返回的行数和批数、耗时以及 worker 的 CPU。
- 每次服务调用也以同样方式追踪。
- 在普通追踪级别下，这些信息记录在请求的 span 上；在调试级别下，每次调用是一个独立的 span。
- 如果你的代码使用 OpenTelemetry，它的 span 会加入同一个 trace。
