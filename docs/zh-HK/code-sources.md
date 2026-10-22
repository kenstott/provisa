# 代碼解析器

以 Python 或 Node 編寫的業務邏輯。

如果你寫過 GraphQL 伺服器，就會熟悉這種模式。代碼解析器是為一種物件類型作答的函數：在 GraphQL 中，標記了 `@table` 的函數*就是*其表類型的 resolver。Provisa 以查詢的參數呼叫它，並對返回的內容進行治理。

與手寫 resolver 的不同之處：

- **受治理。** 可見性、遮罩和行規則作用於函數返回的行，與任何表一樣。
- **批次處理。** 接收 `loader` 參數的函數在一次呼叫中取得一個 join 的所有鍵，即 DataLoader 模式。
- **下推。** 在你要求時，欄上的篩選條件會作為參數傳給你的函數。
- **隨處可用。** 同一個函數回答 GraphQL、SQL（透過 HTTP、pgwire 和 Arrow Flight）、Cypher（透過 HTTP 和 Bolt）、gRPC、REST、JSON:API 和 MCP。

## 第一個 resolver {: #an-example }

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

提供服務的步驟：

1. 新增一個 `python` 數據來源並指向程式碼：PyPI 套件、Git 提交、上載檔案或資料夾（見[程式碼從哪裡來](#where-the-code-comes-from)）。
2. 儲存。Provisa 安裝並讀取程式碼，把 `regions` 作為一張表提供出來，包含 `code` 和 `name` 兩欄。
3. 數據管家在一個網域（這裡是 `sales`）中註冊該表，並像其他表一樣授予存取權限。

然後在 GraphQL 中讀取它：

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

Node 的用法相同，使用 JSDoc 標籤和 TypeScript 類型：見 [Node](#node)。

## 構建 {: #build }

### 函數會變成甚麼 {: #what-a-function-becomes }

Provisa 讀取程式碼本身的宣告：Python 中的 docstring 和類型提示，Node 中的 JSDoc 標籤和 TypeScript 類型。

| 標籤 | 變成 |
|---|---|
| `@table` | 一張表。函數返回清單或可迭代物件，每個元素是一行。 |
| `@command` | 一個指令，按請求執行。 |
| `@stream` | 一張即時表。函數是產生器，每產出一行就傳送一行。 |

- 沒有標籤的函數會被忽略。
- Provisa 不認識的標籤會被拒絕，無法對應的類型也會被拒絕。拒絕訊息會一次列出所有問題，方便你一次改完。

行類型決定欄：

- 文字、數字、布林值、日期和時間戳記對應到相應的欄類型。清單保持為清單，巢狀物件變為具名類型。
- 在 Python 中，行類型可以是 dataclass、`TypedDict`、`NamedTuple`、Pydantic 模型或任何帶註解欄位的類別。Pydantic 模型的行按 `model_dump(by_alias=True)` 的結果傳送，因此帶別名的欄位會成為以別名命名的欄。
- 在 TypeScript 中，行類型可以是介面、類型別名或類別。
- 註冊表時，欄位本身的文件會成為其欄的描述：Pydantic 的 `Field(description=...)`，或 TypeScript 屬性上的文件註解。
- 寬鬆的行類型（Python 中的 `Any` 或 `dict`，TypeScript 中的 `any`、`unknown` 或 `Record`）沒有可讀取的欄。Provisa 會不帶參數呼叫函數一次，並根據返回的行建議欄。如果函數有必填參數，就無法這樣呼叫，你需要在註冊表時自行填寫欄類型。

### 參數 {: #parameters }

函數的參數成為其表的參數。下面這個函數為一個帳戶評分：

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

在 GraphQL 中，參數是欄位參數：

```graphql
{
  churnRisk(account_id: "A-1042") {
    score
    reasons
  }
}
```

在 SQL 中，它是一個 `_nf_` 條件：

```sql
SELECT score, reasons
FROM accounts.churn_risk
WHERE _nf_account_id = 'A-1042'
```

- 沒有預設值的參數是必填的。省略它的讀取會在程式碼執行前被拒絕。
- 有預設值的參數是選填的。省略時使用你的預設值。
- 數據管家仍可把選填參數設為必填。

### 函數能收到甚麼 {: #what-reaches-your-function }

一次呼叫只收到讀取的參數，請求中的其他內容一概收不到。每個參數的值來自欄位參數、`_nf_` 條件、下推給它的篩選條件或 join 的鍵。你的函數永遠看不到是誰在提問：呼叫者的身份和角色留在 Provisa，由它管治函數傳回的列。

下推正是建立在這一點之上。查詢中的篩選條件成為某個參數的值，因此函數可以只取所請求的列。

### 篩選下推 {: #pushdown }

對某個返回欄的篩選可以作為參數傳給你的函數，讓它只取得所需的行。用以下三種方式之一宣告由哪個參數接收：

- **按名稱。** 與某個返回欄同名的參數接收該欄上的相等條件。名為 `<op>_<column>` 的參數接收該運算子，例如 `gte_score` 或 `in_region`。
- **按標籤。** 在 docstring 或 JSDoc 中寫一行 `@<op>_<column> <parameter>`（或 `@<column> <parameter>`），或在 TypeScript 參數前寫行內的 `/* @<op>_<column> */`。
- **按 `Annotated`。** 在 Python 中，在參數的類型提示上寫字串 `"<op>_<column>"`（或 `"<column>"`）。

運算子：

| `op` | 接收 |
|---|---|
| `ne`、`gt`、`gte`、`lt`、`lte` | 與欄同類型的值 |
| `in` | 與欄同類型的清單 |
| `like` | 文字，作用於文字欄 |

- 欄上的範圍會填滿它的兩個參數：`score BETWEEN 0.5 AND 0.9` 得到 `gte_score` 和 `lte_score`。
- 欄名完全按你在行類型中宣告的樣子比對。只有在 Provisa 為讀者重新命名欄之後才相符的名稱不算相符。
- 函數類型無法接受的宣告會被拒絕。
- 篩選條件仍會作用於函數返回的行，因此忽略或放寬所給值的函數不會返回錯誤的行。

### 批次鍵：`loader` 參數 {: #batched-keys }

join 可以連到程式碼表，由另一張表的行提供其參數：例如把客戶與其流失評分關聯起來。預設情況下，Provisa 會為 join 行給出的每組不同參數呼叫一次函數，並把所有這些呼叫放在一個請求裡發給 worker。

接收名為 `loader` 參數的函數則改為每批呼叫一次，這是 GraphQL 的 DataLoader 推廣開來的模式。

- **接收甚麼。** `loader` 是一個雜湊：一個 dict（TypeScript 中是物件），從每個鍵的穩定雜湊對應到鍵本身。鍵是一個 JSON 物件，包含 join 行為函數其他參數給出的值。
- **去重。** 每種不同的取值組合只出現一次。
- **稀疏。** 沒有值的欄位不會出現在鍵中，因此會使用函數本身的預設值，與單次呼叫一樣。
- **返回甚麼。** 一個從收到的每個鍵對應到該鍵對應行的雜湊：表的普通行，可以沒有也可以很多。Provisa 把每個鍵的行交給共用該鍵的每個 join 行。
- **按雜湊比對。** 函數會用預設值填補稀疏鍵缺少的欄位，因此它看到的值與 join 行的值不同。雜湊由 Provisa 計算，函數只需原樣返回收到的鍵。
- **類型照常。** 函數仍只宣告其行類型，即行的清單。
- **需要預設值。** `loader` 以外的參數需要預設值，因為 loader 呼叫只傳 `loader`。
- **批次大小。** 像 DataLoader 的 `maxBatchSize` 一樣設上限，在以下三處之一寫 `loader N`：在 `@table` 旁寫一行 `@loader 100`，在 TypeScript 的 `loader` 參數前寫註解 `/* @loader 100 */`，或在 Python 中用 `Annotated` 字串，`loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`。Provisa 隨後把鍵按該大小分批，並按雜湊合併結果。不設大小時，一批包含所有鍵。
- **拒絕。** 在沒有 `loader` 參數的函數上設大小、大小不是正整數、一個函數上有兩個大小。

當一次處理多個鍵的呼叫比多次處理一個鍵的呼叫更好時，使用 `loader`：

- 在一次向量化運算中對整批數據執行模型評分。
- 把請求批次發給一次能接收多個輸入的 API 或語言模型。
- 每批只做一次準備工作，例如載入模型或開啟連線。

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

與客戶表 join 時，它會以類似 `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}` 的 `loader` 被呼叫。第二個鍵只帶 `customer_id`，因此其方案和區域取預設值。函數返回 `{"9f2c...": [...], "41ab...": [...], ...}`。

### 程式碼從哪裡來 {: #where-the-code-comes-from }

- **一個套件。** Node 用 npm 套件，Python 用 PyPI 套件，固定到確切版本。
- **一個 Git 儲存庫。** 完整的 40 字元提交，如果程式碼不在頂層則指定子目錄。
- **一次上載。** 程式碼的 `.zip`、`.tgz` 或 `.tar.gz`。
- **你電腦上的資料夾。** **選擇資料夾**會在瀏覽器中把資料夾壓縮成 zip（排除 `node_modules`、`.venv`、`__pycache__` 和 `.git`），並作為數據來源的封存檔上載。在能保留資料夾的瀏覽器（Chrome、Edge）中，**儲存**會重新壓縮並再次傳送；其他瀏覽器會再次要求你選擇資料夾。
- **伺服器上的目錄。** 在單一組織的安裝中，可以是營運人員允許的目錄之下的某個目錄。它在原處被讀取，因此修改會被下一次查詢讀到。多組織的伺服器會拒絕它。

除伺服器目錄外，每種來源都是固定的，因此數據來源執行的正是儲存時的程式碼。

- **入口模組。** 當 Python 程式碼有多個頂層模組時，指明從哪個進入。
- **在 VS Code 中開啟。** 伺服器目錄會在你安裝的 VS Code 中開啟；Git 來源會在 vscode.dev 上按其提交開啟。套件或上載沒有此連結。

### 安裝依賴項 {: #installing-dependencies }

有依賴項的程式碼需要鎖定檔，每次安裝都嚴格按它進行。沒有鎖定檔時，宣告了依賴項的套件會被拒絕，拒絕訊息會提示你加入一個。

| 執行環境 | 鎖定檔 |
|---|---|
| Node.js | `package-lock.json` 或 `npm-shrinkwrap.json` |
| Bun | `bun.lock` 或 `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock`（及其 `pyproject.toml`）、`pylock.toml`，或每行都帶 `--hash` 的 `requirements.txt` |

- **自帶或另給。** 自帶鎖定檔的程式碼從它安裝：帶鎖定檔的 Git 儲存庫、上載或資料夾，或作者發佈了 `npm-shrinkwrap.json` 的 npm 套件（npm 從不發佈 `package-lock.json`）。大多數 npm 套件和 PyPI 上的 Python 套件都不自帶，因此請在旁邊另給一個。
- **為甚麼重要。** 沒有鎖定檔時，同一份程式碼的兩次安裝可能不同，今天通過的測試明天可能因新的依賴項版本而失敗。
- **構建好的映像本身就是固定的。** 一旦你[構建數據來源的映像](#running-on-a-remote-host)，依賴項就在映像裡。鎖定檔對 dev 模式和每次新構建都有意義。
- **不執行安裝腳本。** npm 使用 `--ignore-scripts` 安裝，Python 只安裝 wheel，從不從原始碼發行版構建。
- **你的登錄庫。** 套件來自你所在組織自己的 npm 登錄庫、PyPI 索引和 Git 主機，只需在 **Admin → Package registries** 下填寫一次。公共 npm 登錄庫和 PyPI 無需填寫。見 [Package Registries](package-registries.md)。
- **快取。** 安裝按程式碼的內容雜湊快取，同一份程式碼只安裝一次。

### Node {: #node }

- Node 程式碼可運行在 Node.js、Bun 或 Deno 上，由你選擇。
- 標籤寫在 JSDoc 中，類型來自 TypeScript。

TypeScript 版的第一個 resolver：

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

TypeScript 版的批次 `churnScore`：

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

### dev 模式與 worker {: #workers }

數據來源預設運行在 Provisa 伺服器上，這就是 dev 模式。

- 你的程式碼運行在自己的 worker 程序中，每個 Provisa 程序裡每個數據來源及其程式碼版本各一個，在首次呼叫時啟動。
- 當機的 worker 會令進行中的呼叫失敗，並指明數據來源和函數。下一次呼叫會啟動新的 worker。
- 請求的截止時間一到，呼叫就會被取消：worker 會被告知停止該呼叫，等待中的請求會立即釋放。

### 讀取表 {: #reading-a-table }

讀取如何到達你的函數，取決於它還讀取甚麼：

- **只讀取一張程式碼表**時，直接交給你的函數。不會落地任何數據。Provisa 以讀取的參數和下推的篩選條件呼叫它，並把語句的其餘部分應用到返回的行上。
- **與其他表 join 的讀取**交給聯邦引擎。dev 模式數據來源的表會為此落地：這些行按對應參數保留，時長為表的快取 TTL。
- **已構建數據來源的表**在這種 join 中，會從其遠端主機即時讀取，引擎把該主機作為 Postgres 目錄掛接。以下情況則改為落地：其數據來源被複製或設了下限、該表被永久保留、它宣告了大於 0 的落地 TTL，或引擎無法掛接主機。

無論哪種情況，可見性、遮罩和行規則都作用於這些行，與任何表一樣。傳參方式與任何表參數相同：SQL 中用 `_nf_` 條件，GraphQL 中用欄位參數。

### 在遠端主機上運行 {: #running-on-a-remote-host }

當你的程式碼需要伺服器以外的機器，例如 GPU 主機或另一網絡中的主機時，構建它的映像並在那裡運行。數據來源隨即以構建模式運行，其表、授權和讀者保持不變。

**構建映像。**

- 先在 dev 模式下儲存數據來源，讓其程式碼被讀取並固定。
- 在表單的 **Remote host** 下，選擇架構（Linux x86-64 或 ARM64）以及映像的去處：你所在組織的某個容器登錄庫（**Admin → Container registries**），或下載。
- Provisa 自行在最小基礎映像上組裝映像，不需要 Docker 守護程序：固定雜湊的程式碼、透過你的套件登錄庫按其鎖定檔為該平台安裝的依賴項、固定的執行環境，以及遠端主機程式。構建期間映像中的任何東西都不會執行。
- 映像標籤包含程式碼雜湊和 Provisa 版本，其標籤說明了執行環境、程式碼雜湊和匯出項。

**運行它。** 表單會顯示該映像的 compose 服務。下載的映像需先用 `docker load -i <file>` 載入。容器在 7443 監聽，並把身份儲存在其卷上。主機連接埠用 `PROVISA_HOST_PORT` 選擇；沒有它服務不會啟動。

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

首次啟動時，容器會在日誌中列印一行以 `pairing code:` 開頭的內容。

**連接它。**

- 在表單中填寫 Provisa 存取容器所用的主機、你發佈的連接埠和配對碼。在共用的 Docker 網絡或 Kubernetes 中，填寫容器名稱或 Service 名稱以及 7443，無需發佈連接埠。
- Provisa 透過雙向 TLS 與容器配對。配對碼只用一次；之後雙方只接受對方。
- Provisa 檢查容器運行的是該數據來源的程式碼並提供其匯出項，然後把數據來源切換到構建模式。
- 每個 Provisa 程序都保持與主機的獨立連線，主機不再回應時會重新連線。在它不回應期間，對該數據來源表的讀取會失敗並指明主機。

**如果程式碼變了。**

- 回來時運行的是其他程式碼的容器會被拒絕，直到數據管家審核程式碼並在表單中重新固定。
- 要修改程式碼本身，把數據來源切回 dev，儲存新程式碼，再重新構建。

### Provisa 的服務 {: #provisas-services }

你的程式碼可以匯入一個 Provisa 在伺服器上一直提供的模組：Python 中是 `provisa_services`，Node.js、Bun 和 Deno 中是 `@provisa/services`。無需安裝，它也不在任何套件登錄庫中。在 Provisa 之外它不存在，因此你的測試需要模擬它。

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

| 呼叫 | 作用 |
|---|---|
| `secrets.get(name)` | 數據來源宣告的某個密鑰，當下從保管庫讀取。見[密鑰](#secrets)。 |
| `settings.get(name, default)`、`settings.all()` | 數據來源的設定，來自表單上的 **Settings**，數據管家無需改程式碼即可修改。缺少且無預設值時會拋出錯誤。 |
| `cache.get(key, default)`、`cache.set(key, value, ttl)`、`cache.delete(key)` | 數據來源自己的快取，由它的所有 worker 共用。值可以是 JSON 能容納的任何內容，最大 1 MB，保留 `ttl` 秒或直到被取代。 |
| `vectors.upsert(items, model)`、`vectors.search(query, k, model)`、`vectors.embed(texts, model)`、`vectors.delete(ids)` | 數據來源自己的向量集合，用於它自己的參考數據。一個項目有 `id` 以及 `text` 或 `vector`；文字由你所在組織的某個向量模型嵌入。它無法搜尋你受治理的表，那些數據只以參數形式到達程式碼。 |
| `http.get(url)`、`http.post(url, ...)` | Provisa 代表程式碼發出的請求，經過部署的出站代理和憑證機構。主機必須在出站允許清單中，每次請求都會記入稽核記錄。 |
| `llm.chat(model, messages)`、`llm.embeddings(model, input)`、`llm.base_url()`（JavaScript 中為 `llm.baseUrl()`） | Provisa 的 LLM 服務。見[呼叫模型](#calling-a-model)。 |
| `dq.dataset(name)`, `dq.product(id)` | 資料集（`domain.table` 或其資料表 id）或數據產品的數據質素，按元網域向您的程式碼所服務請求的角色所顯示的那樣傳回。對資料集：掃描它的檢查表，每個附帶描述、合約、所指派的數據產品和最近一次執行（結果、時間和失敗的檢查），以及它的剖析及每個剖析的最近一次執行。對數據產品：它的承諾、期望、合作方檢查和孤立檢查。 |
| `log(message, level, fields)`、`metric(name, value, kind, attributes)` | 一行日誌，以及名為 `code_source.<name>` 的指標點，附加到你的程式碼正在處理的請求上。 |

- 服務拒絕的呼叫會拋出 `ServiceError`，指明服務並說明原因。
- 這些服務只回應運行在 Provisa 伺服器上的程式碼。在遠端主機上，每次呼叫都會拋出錯誤。

#### 密鑰 {: #secrets }

付費 API 的密鑰應放在你所在組織的保管庫裡，而不是程式碼中。

- 在數據來源表單的 **Secrets** 下，選擇程式碼可以讀取的組織密鑰。在設定檔中，把它們列在數據來源的 mapping 裡：

    ```yaml
    mapping:
      secrets: [openrouter]
    ```

- 單獨的名稱指同名的組織密鑰。若要改為讀取部署的環境變數，請給出引用：`secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`。
- 該清單引用保管庫，從不複製值，因此即使設定中命名的密鑰尚無人儲存，設定仍能載入。

你的程式碼用 `secrets.get(name)` 讀取密鑰，或直接從 `$PROVISA_SERVICES/secrets/<name>` 讀取：

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

- `PROVISA_SERVICES` 是你的程式碼得到的唯一環境變數。該請求與程式碼的其他流量走同一個出站代理，由代理自行應答；Provisa 的其他部分都無法這樣存取。
- 該位址帶有只有該數據來源的 worker 持有的權杖。它只能讀取數據來源列出的名稱，worker 停止後即失效。
- 每次讀取都在當時從保管庫解析密鑰，因此輪換後的密鑰在下一次讀取時生效。
- 數據來源未列出的名稱會被拒絕，無法解析的密鑰也會被拒絕。拒絕訊息會指明密鑰，但絕不包含其值。
- 每次讀取都會連同數據來源、名稱和時間記入組織的稽核記錄。任何值都不會寫入日誌。
- API 所在的主機仍須在出站允許清單中。

#### 呼叫模型 {: #calling-a-model }

你的程式碼可以透過 Provisa 呼叫語言模型，而無需持有供應商密鑰。`$PROVISA_SERVICES/llm/v1` 是相容 OpenAI 的端點，因此任何 OpenAI SDK 只要把基礎 URL 指向這裡即可使用。該位址就是憑證，所以 SDK 要求的密鑰可以是任意文字。

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

- **按角色。** 把模型寫成 `provisa:<role>`，你會得到組織在 **Admin → AI Models** 下為該角色指定的模型；管理員更改選擇後它會隨之改變。當某個模型被指定了角色時，你也可以直接寫出該模型。
- **嵌入。** `$PROVISA_SERVICES/llm/v1/embeddings` 接受你所在組織的任何向量模型。
- 其他任何模型都會被拒絕。
- 呼叫透過你所在組織自己的供應商、端點和密鑰發出，傳送的內容會先經過 Provisa 的遮蔽規則。
- 不提供串流輸出。請傳送 `stream: false`，這也是預設值。

## 安全保障 {: #safeguards }

### 沙盒 {: #the-sandbox }

- 你的程式碼在自己的 worker 程序中、在沙盒裡運行。它能讀取自己已安裝的程式碼，磁碟上的其他內容一概不能讀取。
- 它只能透過 Provisa 運行的代理存取部署出站允許清單中的主機，且永遠無法存取 Provisa 自己的連接埠。
- 無法提供沙盒的主機會拒絕啟動 worker 並指明缺少甚麼；程式碼絕不會在無約束狀態下運行。在 Windows 上，程式碼數據來源會被拒絕。
- 在遠端主機上，容器的網絡政策就是程式碼的出站邊界。伺服器的出站允許清單適用於 dev 模式。

### 拒絕 {: #refusals }

以下每種情況都會被拒絕，能在程式碼執行前拒絕的都會在執行前拒絕：

- 未知標籤或 Provisa 無法對應的類型，一次列出一個函數的所有問題。
- 省略必填參數的讀取。
- 函數類型無法接受的下推或 `loader` 宣告。
- 有依賴項但沒有鎖定檔的套件。
- 不在允許範圍內的伺服器目錄，或在多組織伺服器上的伺服器目錄。
- 運行的程式碼與數據來源固定的程式碼不同的遠端主機。
- 數據來源未列出的密鑰、不允許的模型、服務不予回應的服務呼叫。

### 計量與上限 {: #metering }

- 每次模型呼叫都會為數據來源記錄模型、token 數，以及按 **Polly model prices** 中該模型條目計算的費用，並寫入組織的稽核記錄。
- 要限制數據來源的花費，在其表單上設定 **LLM monthly cap**，單位為美元。在設定檔中，它寫在 mapping 裡：

    ```yaml
    mapping:
      llm: {cap_usd: 20}
    ```

- 設了上限的數據來源只能呼叫在那裡有價格的模型。
- 一旦當月（UTC）花費達到上限，其呼叫會被拒絕，直到進入下個月。達到上限時已在進行的呼叫仍會完成，因此當月可能略為超出上限。

### 結果治理 {: #governance }

- 函數返回的行會按表的欄進行校驗。
- 隨後，可見性、遮罩和行規則會針對每個讀者生效，與任何表一樣。
- 你的程式碼只能以讀取所給參數的形式看到受治理表的數據。

### 追蹤 {: #tracing }

- 每次對你程式碼的呼叫都作為觸發它的請求的一部分被追蹤：函數、類型、收到的參數個數（從不記錄參數值）、返回的行數和批次數、耗時以及 worker 的 CPU。
- 每次服務呼叫也以同樣方式追蹤。
- 在一般追蹤級別下，這些資料記錄在請求的 span 上；在除錯級別下，每次呼叫是一個獨立的 span。
- 如果你的程式碼使用 OpenTelemetry，它的 span 會加入同一個 trace。
