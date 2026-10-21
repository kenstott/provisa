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

## 读取表 {: #reading-a-table }

代码数据源的表会先落地，从不就地读取。Provisa 用读取的参数调用函数，按列检查每一行，并在表的缓存 TTL 内按这些参数保存这些行。之后，可见性、掩码和行规则会作用于落地的行，和任何表一样。

传参方式与任何表参数相同：在 SQL 中用 `_nf_` 条件，在 GraphQL 中用字段参数。

## 批量键：`loader` 参数 {: #batched-keys }

连接可以到达代码表：另一个表的行为它提供参数，例如把客户连接到他们的流失评分。默认情况下，Provisa 对连接行给出的每一组不同参数调用一次函数，并把所有这些调用放在一个请求中发给 worker。

带有名为 `loader` 参数的函数则改为每个批次调用一次，即 GraphQL 的 DataLoader 推广开来的模式。`loader` 是一个 JSON 对象列表，每个对象是一个键，存放连接行为函数其他参数提供的值。Provisa 会去除重复的键，因此每种不同的取值组合只出现一次。这个列表是稀疏的：没有值的字段会从键中省略，而不是以 null 发送，于是函数自己的默认值生效，与单次调用时一样。

函数返回普通的行，每行带有其键列，Provisa 按这些列把它们连接回连接行，从不按位置。行可以按任意顺序返回，一个键可以对应零行或多行。

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
    plan: str
    region: str
    score: float


def churn_score(
    customer_id: str, plan: str = "basic", region: str = "US", loader: list[CustomerKey] | None = None
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or [{"customer_id": customer_id, "plan": plan, "region": region}]
    rows = [{"plan": "basic", "region": "US", **key} for key in keys]
    scores = MODEL.predict(features(rows))  # one vectorized call for the whole batch
    return [Score(r["customer_id"], r["plan"], r["region"], s) for r, s in zip(rows, scores)]
```

连接到客户表时，它以 `loader=[{"customer_id": "637", "plan": "pro", "region": "EU"}, {"customer_id": "638"}, ...]` 被调用。第二个键只带有 `customer_id`，所以它的 plan 和 region 取默认值。返回的每一行都带有 `customer_id`、`plan` 和 `region`，Provisa 按这些列把它匹配到对应的客户。

同一个函数的 TypeScript 写法：

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  plan: string;
  region: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id: string,
  plan = "basic",
  region = "US",
  loader?: CustomerKey[],
): Promise<Score[]> {
  const keys = loader ?? [{ customer_id, plan, region }];
  const rows = keys.map((key) => ({ plan: "basic", region: "US", ...key }));
  const scores = await model.predict(rows); // one call for the whole batch
  return rows.map((row, i) => ({ ...row, score: scores[i] }) as Score);
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
