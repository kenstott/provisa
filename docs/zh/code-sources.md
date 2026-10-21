# Node 和 Python 数据源

有时您需要的数据藏在团队已经写好的代码后面：一个评分模型、一个内部服务的客户端、一个整理导出文件的脚本。Node 或 Python 数据源让您把这些代码作为表来提供。您给想要共享的函数加上标签，告诉 Provisa 代码在哪里，每个加了标签的函数就变成一张表，每个角色都按与其他表相同的规则读取它。

## 函数会变成什么 {: #what-a-function-becomes }

Provisa 读取代码自身的声明：Node 用 JSDoc 标签和 TypeScript 类型，Python 用 docstring 和类型注解。有三个标签起作用。

| 标签 | 变成 |
|---|---|
| `@table` | 一张表。函数返回列表或可迭代对象，每个元素是一行 |
| `@command` | 一个命令，按请求运行 |
| `@stream` | 一张实时表。函数是生成器，每产出一行就发送一行 |

没有标签的函数会被忽略。Provisa 不认识的标签会被拒绝，无法映射的类型也一样。拒绝信息会指明函数，并一次列出所有问题，让您一次改完。

行类型决定列。文本、数字、布尔值、日期和时间戳映射为对应的列类型，列表仍是列表，嵌套对象成为命名类型。当行类型很宽松时（TypeScript 中的 `any`、`unknown` 或 `Record`，Python 中的 `Any` 或 `dict`），Provisa 会不带参数调用函数一次，并根据返回的行建议列。如果函数有必填参数，就无法这样调用，您需要在注册表时自己指定列类型。

函数的参数成为表的参数。没有默认值的参数是必填的，所以缺少它的读取会在您的代码运行之前就被拒绝。有默认值的参数是可选的；省略它，就采用您的默认值。数据管家仍然可以把可选参数设为必填。

## 代码从哪里来 {: #where-the-code-comes-from }

代码是固定的，所以数据源运行的始终正是注册时的那份代码：

- **软件包**：Node 用 npm 包，Python 用 PyPI 包，指定确切版本。
- **Git 仓库**：完整的 40 位提交，如果代码不在顶层，再加一个子目录。
- **上传**：代码的 `.zip`、`.tgz` 或 `.tar.gz` 压缩包。

Node 代码在 Node.js、Bun 或 Deno 上运行，由您选择。Python 代码在 Provisa 自带的 Python 上运行。如果代码有多个顶层 Python 模块，请在**入口模块**中指明从哪一个进入。

## 安装依赖 {: #installing-dependencies }

有依赖的代码需要锁文件，每次安装都严格按它进行。没有锁文件时，声明了依赖的包会被拒绝，拒绝信息会提示您添加一个。

| 运行时 | 锁文件 |
|---|---|
| Node.js | `package-lock.json` 或 `npm-shrinkwrap.json` |
| Bun | `bun.lock` 或 `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock`（连同其 `pyproject.toml`）、`pylock.toml`，或每行都带 `--hash` 的 `requirements.txt` |

锁文件是可选的，但没有它，同一份代码的两次安装可能不同，今天通过的测试明天可能因某个依赖的新版本而失败。要让每次安装都完全相同，代码必须自带锁文件，或者由您创建一个并在这里提供。构建好的镜像已经固定。一旦您构建了数据源的镜像（[构建镜像](#running-on-a-remote-host)），它的依赖就在镜像里；如果经过您的测试和 CI/CD 的正是这个镜像，那么运行的就是您测试过的镜像。锁文件对 dev 模式和镜像的每次新构建都很重要。自带锁文件的代码按它安装：带锁文件的 Git 仓库或上传，或作者发布了 `npm-shrinkwrap.json` 的 npm 包（npm 从不发布 `package-lock.json`）。没有锁文件的代码安装时依赖版本不固定：没有锁文件的 Git 仓库或上传、大多数 npm 包，以及来自 PyPI 的 Python 包（wheel 中没有放锁文件的位置）。请在这类代码旁边提供一个锁文件，让每次安装都完全相同。有些代码不需要锁文件，因为它的分发方式已经固定了每个依赖。

任何包都不能运行自己的安装脚本：npm 以 `--ignore-scripts` 安装，Python 只安装 wheel，从不从源码发行版构建。

如果您的组织有自己的注册表，包就从那里获取。管理员在 **Admin → 包注册表** 中一次性填好 npm 注册表、PyPI 索引和 Git 主机，组织中的每个 Node 和 Python 数据源都通过它们安装。公共 npm 注册表和公共 PyPI 不需要填写。参见[包注册表](package-registries.md)。

安装按代码的内容哈希缓存，所以同一份代码只安装一次。

## 沙箱 {: #the-sandbox }

您的代码在独立的 worker 进程中、在沙箱里运行。它只能读取自己已安装的代码，磁盘上的其他内容一律不能读。它只能通过 Provisa 运行的代理访问部署出站允许列表中的主机，永远访问不到 Provisa 自己的端口。无法提供沙箱的主机会拒绝启动 worker，并说明缺少什么，而不是在没有约束的情况下运行代码。在 Windows 上，代码数据源会被拒绝。

## 密钥 {: #secrets }

调用付费 API 的代码需要它的密钥，而密钥应放在您组织的保管库中，而不是代码里。在数据源表单的 **Secrets** 下，选择代码可以读取的组织密钥。在配置文件中，把它们列在数据源的 mapping 里：

```yaml
mapping:
  secrets: [openrouter]
```

单独一个名称就是组织中同名的密钥。若要改为读取部署的环境变量，请给出引用：`secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`。这个列表是对保管库的引用，从不是值的副本，因此即使配置中提到的密钥还没有人存储，配置也照样加载。

您的代码会得到一个环境变量 `PROVISA_SERVICES`，需要时从 `$PROVISA_SERVICES/secrets/<name>` 读取密钥。Python 中：

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

TypeScript 中：

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

该请求与代码的其余流量经过同一个出站代理，由代理自己应答。通过这条路径无法访问 Provisa 的任何其他部分。地址中带有一个只有该数据源的 worker 持有的令牌；它只能读取数据源列出的名称，worker 停止后即失效。每次读取都在那一刻从保管库解析密钥，因此轮换后的密钥在下一次读取时生效。

数据源未列出的名称会被拒绝，无法解析的密钥也会被拒绝。拒绝信息会指出密钥名称，但从不包含其值。每次读取都会连同数据源、名称和时间记录到组织的审计日志中，任何值都不会写入日志。API 所在的主机仍须在出站允许列表中。

## 调用模型 {: #calling-a-model }

您的代码可以通过 Provisa 调用语言模型，而无需持有供应商密钥。`$PROVISA_SERVICES/llm/v1` 是兼容 OpenAI 的端点，因此只要把任何 OpenAI SDK 的基础 URL 指向那里即可使用。地址本身就是凭据，因此 SDK 要求的密钥可以是任意文本。Python 中：

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

TypeScript 中：

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

按角色指定模型，写成 `provisa:<role>`：您得到的是组织在 **Admin → AI Models** 下为该角色指定的模型，管理员更改时它会随之变化。当某个模型被指派给某个角色时，也可以直接写出它的名称。对于 embeddings，`$PROVISA_SERVICES/llm/v1/embeddings` 接受您组织的任何向量模型。其他任何模型都会按名称被拒绝。调用经由您组织自己的供应商、端点和密钥发出，发送的内容会先经过 Provisa 的脱敏规则。不提供流式输出：请发送默认值 `stream: false`。

每次调用都会为数据源记录模型、令牌数，以及按 **Polly model prices** 中该模型条目计算的费用，并写入组织的审计日志。要限制数据源的花费，请在其表单中设置 **LLM monthly cap**，单位为 USD。在配置文件中，它写在 mapping 里：

```yaml
mapping:
  llm: {cap_usd: 20}
```

设有上限的数据源只能调用在那里有价格的模型。一旦它当月（UTC）的花费达到上限，其调用就会被拒绝，直到下个月开始。达到上限时已在进行中的调用仍会完成，因此当月可能略微超出上限。

## Provisa 的服务 {: #provisas-services }

您的代码可以导入一个 Provisa 在任何运行位置都会提供的模块：Python 中为 `provisa_services`，Node.js、Bun 和 Deno 中为 `@provisa/services`。无需安装任何东西，它也不在任何包仓库中。在 Provisa 之外该模块并不存在，因此函数的测试会对它进行模拟。

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
| `secrets.get(name)` | 数据源声明的某个密钥，此刻从保管库读取。参见[密钥](#secrets)。 |
| `settings.get(name, default)` | 数据源的某项设置，来自其表单中的 **Settings**，steward 可以在不改代码的情况下修改。缺失且没有默认值时会抛出错误。 |
| `cache.get(key, default)`、`cache.set(key, value, ttl)`、`cache.delete(key)` | 数据源自己的缓存，由它的所有 worker 共享。值可以是 JSON 能容纳的任何内容，最大 1 MB，保留 `ttl` 秒或直到被替换。 |
| `vectors.upsert(items, model)`、`vectors.search(query, k, model)`、`vectors.embed(texts, model)`、`vectors.delete(ids)` | 数据源自己的向量集合，用于它自己的参考数据。每个条目有一个 `id`，以及一个 `text` 或 `vector`；文本由您组织的某个向量模型生成嵌入。它不能搜索您的受治理表：这些表的数据只以参数形式到达代码。 |
| `http.get(url)`、`http.post(url, ...)` | Provisa 代表代码发出的请求，经过部署的出站代理和证书颁发机构。主机必须在出站允许列表中，每次请求都会记录到审计日志。 |
| `llm.chat(model, messages)`、`llm.embeddings(model, input)`、`llm.base_url()`（JavaScript 中为 `llm.baseUrl()`） | Provisa 的 LLM 服务。参见[调用模型](#calling-a-model)。 |
| `log(message, level, fields)`、`metric(name, value, kind, attributes)` | 一行日志，以及一个名为 `code_source.<name>` 的指标点，附加到代码正在处理的请求上。 |

服务拒绝的调用会抛出 `ServiceError`，其中指出服务名称和原因。

这些服务应答在 Provisa 服务器上运行的代码。在远程主机上它们目前还无法访问，每次调用都会抛出错误。

对代码的每次调用都作为触发它的请求的一部分被追踪，包括函数、其类型、收到的参数个数（从不包括参数值）、返回的行数和批次、耗时以及 worker 的 CPU。每次服务调用也以同样方式追踪。在普通追踪详细级别下，这些信息位于请求的 span 上；在调试级别下，每次调用都是单独的 span。如果您的代码使用 OpenTelemetry，它的 span 会加入同一个 trace。

## 在远程主机上运行 {: #running-on-a-remote-host }

数据源默认在 Provisa 服务器上运行，这就是 dev 模式。当它的代码需要一台不同于服务器的机器时，例如 GPU 机器或位于另一个网络中的主机，就构建它的镜像并在那里运行。此时数据源以 built 模式运行，其表、授权和读取者保持不变。

**构建镜像。** 先在 dev 模式下保存数据源，使其代码被读取并固定。在其表单的 **Remote host** 下，选择架构（Linux x86-64 或 ARM64）以及镜像的去向：组织在 **Admin → Container registries** 下登记的某个容器仓库，或者下载。Provisa 自己组装镜像。镜像包含固定哈希的代码、通过您的包仓库按代码的 lockfile 为该平台安装的依赖、固定的运行时以及远程主机程序，基于一个最小的基础镜像。构建期间镜像中的任何内容都不会运行，也不涉及 Docker 守护进程。镜像的标签带有代码哈希和 Provisa 版本，其 label 标明运行时、代码哈希和导出项。

**运行镜像。** 表单会显示该镜像的 compose 服务。下载的镜像需先用 `docker load -i <file>` 加载。容器在内部监听 7443 端口，并把它的身份保存在卷上。主机端口由 `PROVISA_HOST_PORT` 指定；没有它，服务不会启动：

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

容器首次启动时，会在日志中输出一行以 `pairing code:` 开头的内容。

**连接镜像。** 回到表单，输入 Provisa 访问容器所用的主机、您发布的端口以及配对码。在共享的 Docker 网络或 Kubernetes 中，输入容器名或 Service 名以及 7443，无需发布任何端口。Provisa 通过双向 TLS 与容器配对。配对码只使用一次，此后双方只接受彼此。随后 Provisa 检查容器运行的是该数据源的代码并提供其导出项，然后把数据源切换到 built 模式。每个 Provisa 进程都与主机保持自己的连接，主机不再应答时会重新连接。在主机不应答期间，对该数据源表的读取会失败并指出该主机。

**如果代码发生变化。** 运行其他代码的容器重新连接时会被拒绝，并指出数据源固定的哈希和主机报告的哈希，直到 steward 审查代码并在表单中重新固定。要修改代码本身，请把数据源切回 dev，保存新代码，然后重新构建。

在容器中，其网络策略就是代码的出站边界；服务器的出站允许列表适用于 dev 模式。

## 读取表 {: #reading-a-table }

代码数据源的表会先落地，从不就地读取。Provisa 用读取的参数调用函数，按列检查每一行，并在表的缓存 TTL 内按这些参数保存这些行。之后，可见性、掩码和行规则会作用于落地的行，和任何表一样。

传参方式与任何表参数相同：在 SQL 中用 `_nf_` 条件，在 GraphQL 中用字段参数。

## 批量键：`loader` 参数 {: #batched-keys }

连接可以到达代码表：另一个表的行为它提供参数，例如把客户连接到他们的流失评分。默认情况下，Provisa 对连接行给出的每一组不同参数调用一次函数，并把所有这些调用放在一个请求中发给 worker。

带有名为 `loader` 参数的函数则改为每个批次调用一次，即 GraphQL 的 DataLoader 推广开来的模式。`loader` 是一个哈希：一个字典（在 TypeScript 中是对象），从每个键的稳定哈希映射到键本身，即一个 JSON 对象，存放连接行为函数其他参数提供的值。Provisa 会去除重复的键，因此每种不同的取值组合只出现一次。键是稀疏的：没有值的字段会被省略，而不是以 null 发送，于是函数自己的默认值生效，与单次调用时一样。

函数返回同样的形状：一个哈希，从它收到的每个键映射到该键的行，即表的普通行。Provisa 把每个键的行交给共享该键的每一个连接行，一个键可以对应零行或多行。按哈希而不是按行的取值匹配，能让稀疏键也保持匹配：函数会用默认值填补它缺失的字段，因此函数看到的值与连接行的值不同。哈希由 Provisa 计算；函数只需原样返回它收到的键。函数仍然只声明它的行类型，即行的列表，每个键的行都是这个类型。与 `loader` 并列的参数必须有默认值，因为 loader 调用只传入 `loader`。

函数可以限制批次大小，就像 DataLoader 的 `maxBatchSize`，方法是在三处之一声明 `loader N`：在 docstring 或 JSDoc 中 `@table` 旁边写一行 `@loader 100`；在 TypeScript 中紧挨 `loader` 参数之前写注释 `/* @loader 100 */`；或在 Python 中给该参数加一个 `Annotated` 字符串，`loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`。Provisa 随后把键拆分成该大小的批次，每个批次调用一次函数，并按哈希合并结果。不声明大小时，一个批次包含所有键。发现过程会按名称拒绝：在没有 `loader` 参数的函数上声明大小、大小不是正整数，以及一个函数声明了两个大小。

当一次处理多个键的调用比多次各处理一个键的调用更好时，使用 `loader`：

- 在一次向量化运算中为整个批次的模型打分。
- 把请求批量发送给一次可接受多个输入的 API 或语言模型。
- 每个批次只准备一次，例如加载模型或打开连接，而不是每个键一次。

这个 Python 函数一次为批次中的每个客户打分：

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

连接到客户表时，它以类似 `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}` 的 `loader` 被调用。第二个键只带有 `customer_id`，所以它的 plan 和 region 取默认值。函数返回 `{"9f2c...": [...], "41ab...": [...], ...}`，Provisa 把每个行列表交给共享该键的每一个客户行。

同一个函数的 TypeScript 写法：

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

## 示例 {: #an-example }

这个 Python 模块为一个账户流失的可能性打分：

```python
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RiskRow:
    account_id: str
    score: float
    computed_at: datetime
    reasons: list[str]


def churn_risk(account_id: str, as_of: datetime | None = None) -> list[RiskRow]:
    """Churn risk of one account.

    @table
    """
    ...
```

从它的 PyPI 包、Git 提交或上传将其注册为 `python` 数据源。发现功能随后会把 `churn_risk` 作为一张表提供，包含四列（`account_id`、`score`、`computed_at`、`reasons`）、一个必填参数 `account_id` 和一个可选参数 `as_of`。数据管家注册该表后，就可以用 SQL 读取：

```sql
SELECT score, reasons
FROM churn_risk
WHERE _nf_account_id = 'A-1042'
```

同一个函数用 TypeScript 写是这样的：

```typescript
export interface RiskRow {
  account_id: string;
  score: number;
  computed_at: Date;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(accountId: string, asOf?: Date): Promise<RiskRow[]> {
  // ...
}
```
