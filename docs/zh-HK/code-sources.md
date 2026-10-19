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

已發佈的套件很少附帶鎖定檔，所以表單容許在套件參照旁邊提供一個。

任何套件都不能執行自己的安裝指令碼：npm 以 `--ignore-scripts` 安裝，Python 只安裝 wheel，從不從原始碼發行版建構。

如您的組織有自己的登錄庫，套件就從那裏取得。管理員在 **Admin → 套件登錄庫** 中一次過填好 npm 登錄庫、PyPI 索引及 Git 主機，組織中的每個 Node 及 Python 數據來源都透過它們安裝。公共 npm 登錄庫及公共 PyPI 不需要填寫。參見[套件登錄庫](package-registries.md)。

安裝按程式碼的內容雜湊快取，所以同一份程式碼只安裝一次。

## 沙盒 {: #the-sandbox }

您的程式碼在獨立的 worker 程序中、在沙盒裏執行。它只能讀取自己已安裝的程式碼，磁碟上的其他內容一律不能讀。它只能透過 Provisa 執行的代理伺服器連到部署對外允許清單中的主機，永遠連不到 Provisa 自己的連接埠。無法提供沙盒的主機會拒絕啟動 worker，並說明缺少甚麼，而不是在沒有約束的情況下執行程式碼。在 Windows 上，程式碼數據來源會被拒絕。

## 讀取資料表 {: #reading-a-table }

程式碼數據來源的資料表會先落地，從不就地讀取。Provisa 以讀取的參數呼叫函數，按欄位檢查每一列，並在資料表的快取 TTL 內按這些參數保存這些列。之後，可見性、遮罩及行規則會作用於落地的資料列，與任何資料表一樣。

傳參方式與任何資料表參數相同：在 SQL 中用 `_nf_` 條件，在 GraphQL 中用欄位參數。

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
