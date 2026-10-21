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

列類型決定欄位。文字、數字、布林值、日期及時間戳記對應到相應的欄位類型，清單仍是清單，巢狀物件成為具名類型。當列類型很寬鬆時（TypeScript 中的 `any`、`unknown` 或 `Record`，Python 中的 `Any` 或 `dict`），Provisa 會不帶參數呼叫函數一次，並按傳回的資料列建議欄位。如函數有必填參數，就無法這樣呼叫，您需要在註冊資料表時自行指定欄位類型。

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

## 讀取資料表 {: #reading-a-table }

程式碼數據來源的資料表會先落地，從不就地讀取。Provisa 以讀取的參數呼叫函數，按欄位檢查每一列，並在資料表的快取 TTL 內按這些參數保存這些列。之後，可見性、遮罩及行規則會作用於落地的資料列，與任何資料表一樣。

傳參方式與任何資料表參數相同：在 SQL 中用 `_nf_` 條件，在 GraphQL 中用欄位參數。

## 批量鍵：`loader` 參數 {: #batched-keys }

連接可以到達程式碼表：另一個表的行為它提供參數，例如把客戶連接到他們的流失評分。預設情況下，Provisa 對連接行給出的每一組不同參數調用一次函數，並把所有這些調用放在一個請求中發給 worker。

帶有名為 `loader` 參數的函數則改為每個批次調用一次，即 GraphQL 的 DataLoader 推廣開來的模式。`loader` 是一個雜湊：一個字典（在 TypeScript 中是物件），從每個鍵的穩定雜湊映射到鍵本身，即一個 JSON 物件，存放連接行為函數其他參數提供的值。Provisa 會去除重複的鍵，因此每種不同的取值組合只出現一次。鍵是稀疏的：沒有值的欄位會被省略，而不是以 null 發送，於是函數自己的預設值生效，與單次調用時一樣。

函數返回同樣的形狀：一個雜湊，從它收到的每個鍵映射到該鍵的行，即表的普通行。Provisa 把每個鍵的行交給共享該鍵的每一個連接行，一個鍵可以對應零行或多行。按雜湊而不是按行的取值匹配，能讓稀疏鍵也保持匹配：函數會用預設值填補它缺失的欄位，因此函數看到的值與連接行的值不同。雜湊由 Provisa 計算；函數只需原樣返回它收到的鍵。函數仍然只宣告它的行類型，即行的列表，每個鍵的行都是這個類型。與 `loader` 並列的參數必須有預設值，因為 loader 調用只傳入 `loader`。

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
