# Node 及 Python 數據來源

有時您需要的數據藏在團隊已經寫好的程式碼後面：一個評分模型、一個內部服務的用戶端、一個整理匯出檔案的指令碼。Node 或 Python 數據來源讓您把這些程式碼作為資料表提供。您為想共用的函數加上標籤，告訴 Provisa 程式碼在哪裏，每個加了標籤的函數就成為一個資料表，每個角色都按與其他資料表相同的規則讀取它。

## 函數會變成甚麼 {: #what-a-function-becomes }

Provisa 讀取程式碼本身的宣告：Node 用 JSDoc 標籤及 TypeScript 類型，Python 用 docstring 及類型註解。有三個標籤起作用。

| 標籤 | 變成 |
|---|---|
| `@table` | 一個資料表。函數傳回清單或可迭代物件，每個元素是一列 |
| `@command` | 一個指令，按要求執行 |
| `@stream` | 一個即時資料表。函數是產生器，每產出一列就傳送一列 |

沒有標籤的函數會被略過。Provisa 不認識的標籤會被拒絕，無法對應的類型亦一樣。拒絕訊息會指明函數，並一次列出所有問題，讓您一次改好。

列類型決定欄位。文字、數字、布林值、日期及時間戳記對應到相應的欄位類型，清單仍是清單，巢狀物件成為具名類型。在 TypeScript 中，列類型是介面、類型別名或類別。在 Python 中，它是 dataclass、`TypedDict`、`NamedTuple`、Pydantic 模型或任何帶註解欄位的類別。Pydantic 模型的資料列按 `model_dump(by_alias=True)` 給出的形式傳送，因此帶別名的欄位會成為以別名命名的欄位。註冊資料表時，欄位本身的文件會成為其欄位的描述：TypeScript 屬性上的文件註解，或 Pydantic 的 `Field(description=...)`。當列類型很寬鬆時（TypeScript 中的 `any`、`unknown` 或 `Record`，Python 中的 `Any` 或 `dict`），Provisa 會不帶參數呼叫函數一次，並按傳回的資料列建議欄位。如函數有必填參數，就無法這樣呼叫，您需要在註冊資料表時自行指定欄位類型。

函數的參數成為資料表的參數。沒有預設值的參數是必填的，所以缺少它的讀取會在您的程式碼執行之前就被拒絕。有預設值的參數是可選的；省略它，就採用您的預設值。數據管家仍然可以把可選參數設為必填。

## 程式碼從哪裏來 {: #where-the-code-comes-from }

程式碼是固定的，所以數據來源執行的始終正是註冊時的那份程式碼：

- **套件**：Node 用 npm 套件，Python 用 PyPI 套件，指定確切版本。
- **Git 儲存庫**：完整的 40 位提交，如程式碼不在頂層，再加一個子目錄。
- **上載**：程式碼的 `.zip`、`.tgz` 或 `.tar.gz` 壓縮檔。

Node 程式碼在 Node.js、Bun 或 Deno 上執行，由您選擇。Python 程式碼在 Provisa 附帶的 Python 上執行。如程式碼有多於一個頂層 Python 模組，請在**入口模組**中指明從哪一個進入。

## 安裝相依套件 {: #installing-dependencies }

有相依套件的程式碼需要鎖定檔，每次安裝都嚴格按它進行。沒有鎖定檔時，宣告了相依套件的套件會被拒絕，拒絕訊息會提示您加入一個。

| 執行環境 | 鎖定檔 |
|---|---|
| Node.js | `package-lock.json` 或 `npm-shrinkwrap.json` |
| Bun | `bun.lock` 或 `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock`（連同其 `pyproject.toml`）、`pylock.toml`，或每行都附 `--hash` 的 `requirements.txt` |

鎖定檔是可選的，但沒有它，同一份程式碼的兩次安裝可能不同，今天通過的測試明天可能因某個依賴的新版本而失敗。要令每次安裝都完全相同，程式碼必須自帶鎖定檔，或者由您建立一個並在這裏提供。建置好的映像已經固定。一旦您建置了資料源的映像（[建置映像](#running-on-a-remote-host)），它的依賴就在映像裏；如果經過您的測試和 CI/CD 的正是這個映像，那麼運行的就是您測試過的映像。鎖定檔對 dev 模式和映像的每次新建置都很重要。自帶鎖定檔的程式碼按它安裝：帶鎖定檔的 Git 儲存庫或上載，或作者發佈了 `npm-shrinkwrap.json` 的 npm 套件（npm 從不發佈 `package-lock.json`）。沒有鎖定檔的程式碼安裝時依賴版本不固定：沒有鎖定檔的 Git 儲存庫或上載、大多數 npm 套件，以及來自 PyPI 的 Python 套件（wheel 中沒有放鎖定檔的位置）。請在這類程式碼旁邊提供一個鎖定檔，讓每次安裝都完全相同。有些程式碼不需要鎖定檔，因為它的分發方式已經固定了每個依賴。

任何套件都不能執行自己的安裝指令碼：npm 以 `--ignore-scripts` 安裝，Python 只安裝 wheel，從不從原始碼發行版建構。

如您的組織有自己的登錄庫，套件就從那裏取得。管理員在 **Admin → 套件登錄庫** 中一次過填好 npm 登錄庫、PyPI 索引及 Git 主機，組織中的每個 Node 及 Python 數據來源都透過它們安裝。公共 npm 登錄庫及公共 PyPI 不需要填寫。參見[套件登錄庫](package-registries.md)。

安裝按程式碼的內容雜湊快取，所以同一份程式碼只安裝一次。

## 沙盒 {: #the-sandbox }

您的程式碼在獨立的 worker 程序中、在沙盒裏執行。它只能讀取自己已安裝的程式碼，磁碟上的其他內容一律不能讀。它只能透過 Provisa 執行的代理伺服器連到部署對外允許清單中的主機，永遠連不到 Provisa 自己的連接埠。無法提供沙盒的主機會拒絕啟動 worker，並說明缺少甚麼，而不是在沒有約束的情況下執行程式碼。在 Windows 上，程式碼數據來源會被拒絕。

## 密鑰 {: #secrets }

調用付费 API 的程式碼需要它的密鑰，而密鑰應放在您組織的保管庫中，而不是程式碼裏。在資料源表單的 **Secrets** 下，選擇程式碼可以讀取的組織密鑰。在設定檔案中，把它們列在資料源的 mapping 裏：

```yaml
mapping:
  secrets: [openrouter]
```

單獨一個名稱就是組織中同名的密鑰。若要改為讀取部署的環境變數，請给出引用：`secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`。這個列表是對保管庫的引用，從不是值的副本，因此即使設定中提到的密鑰還沒有人儲存，設定也照樣載入。

您的程式碼會得到一個環境變數 `PROVISA_SERVICES`，需要時從 `$PROVISA_SERVICES/secrets/<name>` 讀取密鑰。Python 中：

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

TypeScript 中：

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

該請求與程式碼的其餘流量經過同一個出站代理，由代理自己應答。通過這條路徑無法訪問 Provisa 的任何其他部分。地址中帶有一個只有該資料源的 worker 持有的令牌；它只能讀取資料源列出的名稱，worker 停止後即失效。每次讀取都在那一刻從保管庫解析密鑰，因此輪換後的密鑰在下一次讀取時生效。

資料源未列出的名稱會被拒絕，無法解析的密鑰也會被拒絕。拒絕資訊會指出密鑰名稱，但從不包含其值。每次讀取都會連同資料源、名稱和時間記錄到組織的審計日誌中，任何值都不會寫入日誌。API 所在的主機仍須在出站允許列表中。

## 調用模型 {: #calling-a-model }

您的程式碼可以通過 Provisa 調用語言模型，而無需持有供應商密鑰。`$PROVISA_SERVICES/llm/v1` 是相容 OpenAI 的端點，因此只要把任何 OpenAI SDK 的基礎 URL 指向那裏即可使用。地址本身就是憑據，因此 SDK 要求的密鑰可以是任意文本。Python 中：

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

按角色指定模型，寫成 `provisa:<role>`：您得到的是組織在 **Admin → AI Models** 下為該角色指定的模型，管理員更改時它會隨之變化。當某個模型被指派給某個角色時，也可以直接寫出它的名稱。對於 embeddings，`$PROVISA_SERVICES/llm/v1/embeddings` 接受您組織的任何向量模型。其他任何模型都會按名稱被拒絕。調用經由您組織自己的供應商、端點和密鑰發出，發送的內容會先經過 Provisa 的脫敏規則。不提供串流輸出：請發送預設值 `stream: false`。

每次調用都會為資料源記錄模型、令牌數，以及按 **Polly model prices** 中該模型條目計算的費用，並寫入組織的審計日誌。要限制資料源的花費，請在其表單中設定 **LLM monthly cap**，單位為 USD。在設定檔案中，它写在 mapping 裏：

```yaml
mapping:
  llm: {cap_usd: 20}
```

設有上限的資料源只能調用在那裏有價格的模型。一旦它當月（UTC）的花費達到上限，其調用就會被拒絕，直到下個月開始。達到上限時已在進行中的調用仍會完成，因此當月可能略微超出上限。

## Provisa 的服務 {: #provisas-services }

您的程式碼可以匯入一個 Provisa 在任何運行位置都會提供的模組：Python 中為 `provisa_services`，Node.js、Bun 和 Deno 中為 `@provisa/services`。無需安裝任何東西，它也不在任何套件倉庫中。在 Provisa 之外該模組並不存在，因此函數的測試會對它進行模擬。

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

| 調用 | 作用 |
|---|---|
| `secrets.get(name)` | 資料源聲明的某個密鑰，此刻從保管庫讀取。參見[密鑰](#secrets)。 |
| `settings.get(name, default)` | 資料源的某項設定，來自其表單中的 **Settings**，steward 可以在不改程式碼的情況下修改。缺失且沒有預設值時會拋出錯誤。 |
| `cache.get(key, default)`、`cache.set(key, value, ttl)`、`cache.delete(key)` | 資料源自己的快取，由它的所有 worker 共享。值可以是 JSON 能容納的任何內容，最大 1 MB，保留 `ttl` 秒或直到被替換。 |
| `vectors.upsert(items, model)`、`vectors.search(query, k, model)`、`vectors.embed(texts, model)`、`vectors.delete(ids)` | 資料源自己的向量集合，用於它自己的參考數據。每個條目有一個 `id`，以及一個 `text` 或 `vector`；文本由您組織的某個向量模型生成嵌入。它不能搜尋您的受治理表：這些表的數據只以參數形式到達程式碼。 |
| `http.get(url)`、`http.post(url, ...)` | Provisa 代表程式碼發出的請求，經過部署的出站代理和憑證頒發機構。主機必須在出站允許列表中，每次請求都會記錄到審計日誌。 |
| `llm.chat(model, messages)`、`llm.embeddings(model, input)`、`llm.base_url()`（JavaScript 中為 `llm.baseUrl()`） | Provisa 的 LLM 服務。參見[調用模型](#calling-a-model)。 |
| `dq.dataset(name)`, `dq.product(id)` | 資料集（`domain.table` 或其資料表 id）或數據產品的數據質素，按元網域向您的程式碼所服務請求的角色所顯示的那樣傳回。對資料集：掃描它的檢查表，每個附帶描述、合約、所指派的數據產品和最近一次執行（結果、時間和失敗的檢查），以及它的剖析及每個剖析的最近一次執行。對數據產品：它的承諾、期望、合作方檢查和孤立檢查。 |
| `log(message, level, fields)`、`metric(name, value, kind, attributes)` | 一行日誌，以及一個名為 `code_source.<name>` 的指標點，附加到程式碼正在處理的請求上。 |

服務拒絕的調用會拋出 `ServiceError`，其中指出服務名稱和原因。

這些服務應答在 Provisa 伺服器上運行的程式碼。在遠程主機上它們目前還無法訪問，每次調用都會拋出錯誤。

對程式碼的每次調用都作為觸發它的請求的一部分被追蹤，包括函數、其類型、收到的參數個數（從不包括參數值）、返回的行數和批次、耗時以及 worker 的 CPU。每次服務調用也以同樣方式追蹤。在普通追蹤詳細級別下，這些資訊位於請求的 span 上；在除錯級別下，每次調用都是單獨的 span。如果您的程式碼使用 OpenTelemetry，它的 span 會加入同一個 trace。

## 在遠程主機上運行 {: #running-on-a-remote-host }

資料源預設在 Provisa 伺服器上運行，這就是 dev 模式。當它的程式碼需要一台不同於伺服器的機器時，例如 GPU 機器或位於另一個網絡中的主機，就建置它的映像並在那裏運行。此時資料源以 built 模式運行，其表、授權和讀取者保持不變。

**建置映像。** 先在 dev 模式下儲存資料源，使其程式碼被讀取並固定。在其表單的 **Remote host** 下，選擇架構（Linux x86-64 或 ARM64）以及映像的去向：組織在 **Admin → Container registries** 下登記的某個容器倉庫，或者下載。Provisa 自己組裝映像。映像包含固定雜湊的程式碼、通過您的套件倉庫按程式碼的 lockfile 為該平台安裝的依賴、固定的運行時以及遠程主機程式，基於一個最小的基礎映像。建置期間映像中的任何內容都不會運行，也不涉及 Docker 守護程序。映像的標籤帶有程式碼雜湊和 Provisa 版本，其 label 標明運行時、程式碼雜湊和匯出項。

**運行映像。** 表單會顯示該映像的 compose 服務。下載的映像需先用 `docker load -i <file>` 載入。容器在內部監聽 7443 埠，並把它的身份儲存在卷上。主機埠由 `PROVISA_HOST_PORT` 指定；沒有它，服務不會啟動：

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

容器首次啟動時，會在日誌中輸出一行以 `pairing code:` 開頭的內容。

**連接映像。** 回到表單，輸入 Provisa 訪問容器所用的主機、您發佈的埠以及配對碼。在共享的 Docker 網絡或 Kubernetes 中，輸入容器名或 Service 名以及 7443，無需發佈任何埠。Provisa 通過雙向 TLS 與容器配對。配對碼只使用一次，此後雙方只接受彼此。随後 Provisa 檢查容器運行的是該資料源的程式碼並提供其匯出項，然後把資料源切換到 built 模式。每個 Provisa 程序都與主機保持自己的連接，主機不再應答時會重新連接。在主機不應答期間，對該資料源表的讀取會失敗並指出該主機。

**如果程式碼發生變化。** 運行其他程式碼的容器重新連接時會被拒絕，並指出資料源固定的雜湊和主機報告的雜湊，直到 steward 審查程式碼並在表單中重新固定。要修改程式碼本身，請把資料源切回 dev，儲存新程式碼，然後重新建置。

在容器中，其網絡策略就是程式碼的出站邊界；伺服器的出站允許列表适用於 dev 模式。

## 讀取資料表 {: #reading-a-table }

程式碼數據來源的資料表會先落地，從不就地讀取。Provisa 以讀取的參數呼叫函數，按欄位檢查每一列，並在資料表的快取 TTL 內按這些參數保存這些列。之後，可見性、遮罩及行規則會作用於落地的資料列，與任何資料表一樣。

傳參方式與任何資料表參數相同：在 SQL 中用 `_nf_` 條件，在 GraphQL 中用欄位參數。

## 批量鍵：`loader` 參數 {: #batched-keys }

連接可以到達程式碼表：另一個表的行為它提供參數，例如把客戶連接到他們的流失評分。預設情況下，Provisa 對連接行給出的每一組不同參數調用一次函數，並把所有這些調用放在一個請求中發給 worker。

帶有名為 `loader` 參數的函數則改為每個批次調用一次，即 GraphQL 的 DataLoader 推廣開來的模式。`loader` 是一個雜湊：一個字典（在 TypeScript 中是物件），從每個鍵的穩定雜湊映射到鍵本身，即一個 JSON 物件，存放連接行為函數其他參數提供的值。Provisa 會去除重複的鍵，因此每種不同的取值組合只出現一次。鍵是稀疏的：沒有值的欄位會被省略，而不是以 null 發送，於是函數自己的預設值生效，與單次調用時一樣。

函數返回同樣的形狀：一個雜湊，從它收到的每個鍵映射到該鍵的行，即表的普通行。Provisa 把每個鍵的行交給共享該鍵的每一個連接行，一個鍵可以對應零行或多行。按雜湊而不是按行的取值匹配，能讓稀疏鍵也保持匹配：函數會用預設值填補它缺失的欄位，因此函數看到的值與連接行的值不同。雜湊由 Provisa 計算；函數只需原樣返回它收到的鍵。函數仍然只宣告它的行類型，即行的列表，每個鍵的行都是這個類型。與 `loader` 並列的參數必須有預設值，因為 loader 調用只傳入 `loader`。

函數可以限制批次大小，就像 DataLoader 的 `maxBatchSize`，方法是在三處之一宣告 `loader N`：在 docstring 或 JSDoc 中 `@table` 旁邊寫一行 `@loader 100`；在 TypeScript 中緊貼 `loader` 參數之前寫註解 `/* @loader 100 */`；或在 Python 中給該參數加一個 `Annotated` 字串，`loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`。Provisa 隨後把鍵拆分成該大小的批次，每個批次調用一次函數，並按雜湊合併結果。不宣告大小時，一個批次包含所有鍵。探索過程會按名稱拒絕：在沒有 `loader` 參數的函數上宣告大小、大小不是正整數，以及一個函數宣告了兩個大小。

當一次處理多個鍵的調用比多次各處理一個鍵的調用更好時，使用 `loader`：

- 在一次向量化運算中為整個批次的模型評分。
- 把請求批量發送給一次可接受多個輸入的 API 或語言模型。
- 每個批次只準備一次，例如載入模型或開啟連線，而不是每個鍵一次。

這個 Python 函數一次為批次中的每個客戶評分：

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

連接到客戶表時，它以類似 `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}` 的 `loader` 被調用。第二個鍵只帶有 `customer_id`，所以它的 plan 和 region 取預設值。函數返回 `{"9f2c...": [...], "41ab...": [...], ...}`，Provisa 把每個行列表交給共享該鍵的每一個客戶行。

同一個函數的 TypeScript 寫法：

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

這個 Python 模組為一個帳戶流失的可能性評分：

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

從它的 PyPI 套件、Git 提交或上載將其註冊為 `python` 數據來源。探索功能隨後會把 `churn_risk` 作為一個資料表提供，包含四個欄位（`account_id`、`score`、`computed_at`、`reasons`）、一個必填參數 `account_id` 及一個可選參數 `as_of`。數據管家註冊該資料表後，就可以用 SQL 讀取：

```sql
SELECT score, reasons
FROM churn_risk
WHERE _nf_account_id = 'A-1042'
```

同一個函數用 TypeScript 寫是這樣的：

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
