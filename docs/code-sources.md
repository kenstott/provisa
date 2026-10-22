# Code Resolvers

Business logic in Python or Node.

If you have written a GraphQL server, you know this pattern. A code resolver is a function that answers for one object type: over GraphQL, a function tagged `@table` *is* the resolver for its table's type. Provisa calls it with the query's arguments and governs what comes back.

What differs from a resolver you write by hand:

- **Governed.** Visibility, masks and row rules apply to the rows your function returns, as they do for any table.
- **Batched.** A function that takes a `loader` parameter gets every key of a join in one call, the DataLoader pattern.
- **Pushed down.** A filter on a column reaches your function as a parameter when you ask for it.
- **Served everywhere.** The same function answers GraphQL, SQL (over HTTP, pgwire and Arrow Flight), Cypher (over HTTP and Bolt), gRPC, REST, JSON:API and MCP.

Code resolvers are one of three ways to write business logic in Provisa. See [Business Logic in Provisa](business-logic.md).

## A first resolver {: #an-example }

```python
from dataclasses import dataclass


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

To serve it:

1. Add a `python` source and point it at the code: a PyPI package, a Git commit, an upload or a folder (see [Where the code comes from](#where-the-code-comes-from)).
2. Save it. Provisa installs the code, reads it, and offers `churn_risk` as a table with the columns `score` and `reasons` and a required parameter, `account_id`.
3. A steward registers the table in a domain, here `accounts`, and grants it like any other.

Then read it. In GraphQL the parameter is a field argument:

```graphql
{
  churnRisk(account_id: "A-1042") {
    score
    reasons
  }
}
```

In SQL it is an `_nf_` condition:

```sql
SELECT score, reasons
FROM accounts.churn_risk
WHERE _nf_account_id = 'A-1042'
```

Node works the same way, with JSDoc tags and TypeScript types: see [Node](#node).

## Build {: #build }

### What a function becomes {: #what-a-function-becomes }

Provisa reads your code's own declarations: docstrings and type hints in Python, JSDoc tags and TypeScript types in Node.

| Tag | Becomes |
|---|---|
| `@table` | A table. The function returns a list or an iterable, and each element is a row. |
| `@command` | A command, run on request. |
| `@stream` | A live table. The function is a generator, and each row is sent as it is yielded. |

- A function without a tag is ignored.
- A tag Provisa doesn't know is refused, and so is a type it can't map. The refusal lists every problem at once, so you fix them in one pass.

The row type gives the columns:

- Text, numbers, booleans, dates and timestamps map to their column types. Lists stay lists, and nested objects become named types.
- In Python a row type is a dataclass, a `TypedDict`, a `NamedTuple`, a Pydantic model or any class with annotated fields. A Pydantic model's rows are sent as `model_dump(by_alias=True)` gives them, so a field with an alias becomes a column named by its alias.
- In TypeScript a row type is an interface, a type alias or a class.
- A field's own documentation becomes its column's description when you register the table: a Pydantic `Field(description=...)`, or a doc comment on a TypeScript property.
- A loose row type (`Any` or a `dict` in Python, `any`, `unknown` or a `Record` in TypeScript) has no columns to read. Provisa calls the function once with no arguments and suggests columns from the rows that come back. When the function has a required parameter it can't be called that way, so you type the columns yourself when you register the table.

The function's parameters become the table's parameters:

- A parameter without a default is required. A read that leaves it out is refused before your code runs.
- A parameter with a default is optional. Leave it out and your default applies.
- A steward can still make an optional one required.

### Pushdown {: #pushdown }

A filter on one of the returned columns can reach your function as an argument, so it fetches only the rows asked for. Declare which parameter takes it in any of three ways:

- **By name.** A parameter named exactly as a return column takes an equality on that column. One named `<op>_<column>` takes that operator, as in `gte_score` or `in_region`.
- **By tag.** A line `@<op>_<column> <parameter>` (or `@<column> <parameter>`) in the docstring or JSDoc, or an inline `/* @<op>_<column> */` before a TypeScript parameter.
- **By `Annotated`.** In Python, the string `"<op>_<column>"` (or `"<column>"`) on the parameter's type hint.

The operators:

| `op` | Takes |
|---|---|
| `ne`, `gt`, `gte`, `lt`, `lte` | A value of the column's type |
| `in` | A list of the column's type |
| `like` | Text, on a text column |

- A range on a column fills both of its parameters: `score BETWEEN 0.5 AND 0.9` gives `gte_score` and `lte_score`.
- Column names are matched exactly as you declared them in the row type. A name that matches only once Provisa renames the column for a reader is not a match.
- A declaration your function's types can't take is refused.
- The filter is still applied to the rows your function returns, so a function that ignores or loosens a value it is given can't return wrong rows.

### Batched keys: the `loader` parameter {: #batched-keys }

A join can reach a code table, with the rows of another table supplying its parameters: customers joined to their churn score, say. By default Provisa calls the function once for each distinct set of arguments the joining rows give, and sends all those calls to the worker in one request.

A function that takes a parameter named `loader` is called once per batch instead, the pattern GraphQL's DataLoader made familiar.

- **What it gets.** `loader` is a hash: a dict (an object in TypeScript) from a stable hash of each key to the key itself. A key is a JSON object holding the joining rows' values for the function's other parameters.
- **De-duplicated.** Each distinct combination of values appears once.
- **Sparse.** A field with no value is left out of the key, so the function's own default applies, as it does on a single call.
- **What it returns.** A hash from each key it was given to that key's rows: ordinary rows of the table, none or many. Provisa gives each key's rows to every joining row that shared the key.
- **Matched by hash.** The function fills a sparse key's missing fields with defaults, so the values it sees differ from the joining row's. Provisa computes the hash, and the function only echoes back the keys it was given.
- **Typed as usual.** The function still declares its row type alone, a list of rows.
- **Defaults needed.** Its parameters beside `loader` need defaults, because a loader call passes `loader` alone.
- **Batch size.** Cap it, as DataLoader's `maxBatchSize` does, with `loader N` in any one of three places: a `@loader 100` line beside `@table`, a `/* @loader 100 */` comment before the `loader` parameter in TypeScript, or an `Annotated` string in Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa then splits the keys into batches of that size and merges the answers by hash. Without a size, one batch holds every key.
- **Refused.** A size on a function with no `loader` parameter, a size that isn't a positive integer, and two sizes on one function.

Use `loader` when one call over many keys does better than many calls over one:

- Scoring a model over a whole batch in one vectorized pass.
- Batching requests to an API or a language model that accepts many inputs at once.
- Setting up once per batch, such as loading a model or opening a connection.

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

Joined to a table of customers, it is called with a `loader` such as `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. The second key carries only `customer_id`, so its plan and region take the defaults. The function returns `{"9f2c...": [...], "41ab...": [...], ...}`.

### Where the code comes from {: #where-the-code-comes-from }

- **A package.** An npm package for Node or a PyPI package for Python, at an exact version.
- **A Git repository.** A full 40-character commit, with a subdirectory if the code isn't at the top.
- **An upload.** A `.zip`, `.tgz` or `.tar.gz` of the code.
- **A folder on your machine.** **Choose folder** zips the folder in your browser, leaving out `node_modules`, `.venv`, `__pycache__` and `.git`, and uploads it as the source's archive. In a browser that can keep the folder (Chrome, Edge), **Save** zips and sends it again; another browser asks you for the folder again.
- **A directory on the server.** On a single-organization install, a directory under the ones the operator allows. It is read where it is, so an edit is read by the next query. A server of many organizations refuses it.

Every origin but the server directory is pinned, so the source runs exactly what was saved.

- **Entry module.** When Python code has more than one top-level module, name the one to enter by.
- **Open in VS Code.** A server directory opens in your installed VS Code; a Git origin opens at its commit on vscode.dev. A package or an upload has no such link.

### Installing dependencies {: #installing-dependencies }

Code with dependencies needs a lockfile, and every install follows it exactly. Without one, a package that declares dependencies is refused, and the refusal tells you to add one.

| Runtime | Lockfile |
|---|---|
| Node.js | `package-lock.json` or `npm-shrinkwrap.json` |
| Bun | `bun.lock` or `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (with its `pyproject.toml`), `pylock.toml`, or a `requirements.txt` with `--hash` on every line |

- **Carried or given.** Code that carries its own lockfile installs from it: a Git repository, upload or folder with one, or an npm package whose author published `npm-shrinkwrap.json` (npm never publishes `package-lock.json`). Most npm packages, and Python packages from PyPI, carry none, so give a lockfile beside them.
- **Why it matters.** Without one, two installs of the same code can differ, and a test that passes today can fail tomorrow on a new dependency version.
- **A built image is fixed already.** Once you [build the source's image](#running-on-a-remote-host), its dependencies are inside it. A lockfile matters for dev mode and for each new build.
- **No install scripts.** npm installs with `--ignore-scripts`, and Python installs wheels only, never building from a source distribution.
- **Your registries.** Packages come from your organization's own npm registries, PyPI indexes and Git hosts, entered once under **Admin → Package registries**. The public npm registry and PyPI need no entry. See [Package Registries](package-registries.md).
- **Cached.** An install is cached by the code's content hash, so the same code installs once.

### Node {: #node }

- Node code runs on Node.js, Bun or Deno, whichever you pick.
- Tags go in JSDoc, and types come from TypeScript.

The first resolver in TypeScript:

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

The batched `churnScore` in TypeScript:

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

## Deployment {: #deployment }

### Dev mode and workers {: #workers }

A source runs on the Provisa server by default; this is dev mode.

- Your code runs in a worker process of its own, one per source and version of its code in each Provisa process, started on its first call.
- A worker that crashes fails the calls in flight, naming the source and the function. The next call starts a new one.
- A call is cancelled when its request's deadline passes: the worker is told to stop it, and the waiting request is released at once.

### Reading a table {: #reading-a-table }

How a read reaches your function depends on what else it reads:

- **A read of one code table alone** goes straight to your function. Nothing is landed. Provisa calls it with the read's arguments and its pushed-down filters, and applies the rest of the statement to the rows it returns.
- **A read that joins other tables** is the federation engine's. A dev-mode source's table is landed for it: the rows are kept under those arguments for the table's cache TTL.
- **A built source's table** in such a join is read live from its remote host, which the engine attaches as a Postgres catalog. It is landed instead when its source is replicated or floored, when the table is kept for good, when it declares a landing TTL above 0, or on an engine that can't attach a host.

Either way, visibility, masks and row rules apply to the rows, as they do for any table. You pass arguments the way you pass any table parameter: `_nf_` conditions in SQL, field arguments in GraphQL.

### Running on a remote host {: #running-on-a-remote-host }

When your code needs a machine the server isn't, such as a GPU box or a host inside another network, build its image and run it there. The source then runs in built mode, and its tables, grants and readers stay the same.

**Build the image.**

- Save the source in dev mode first, so its code is read and pinned.
- Under **Remote host** on its form, choose the architecture (Linux x86-64 or ARM64) and where the image goes: one of your organization's container registries (**Admin → Container registries**), or a download.
- Provisa assembles the image itself, on a minimal base, with no Docker daemon: the code at its pinned hash, its dependencies installed for that platform from its lockfile through your package registries, the pinned runtime, and the remote host program. Nothing in the image runs while it is built.
- The image's tag carries the code hash and Provisa's version, and its labels name the runtime, the code hash and the exports.

**Run it.** The form shows a compose service for the image. Load a downloaded image first with `docker load -i <file>`. The container listens on 7443 and keeps its identity on its volume. You choose the host port with `PROVISA_HOST_PORT`; the service won't start without it.

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

On its first start the container prints a line beginning `pairing code:` in its log.

**Connect it.**

- On the form, enter the host Provisa reaches the container at, the port you published, and the pairing code. On a shared Docker network or in Kubernetes, enter the container or Service name and 7443, with nothing published.
- Provisa pairs with the container over mutual TLS. The code is used once; from then on each side accepts only the other.
- Provisa checks that the container runs the source's code and offers its exports, then switches the source to built mode.
- Every Provisa process keeps its own connection to the host and dials again if the host stops answering. While it doesn't answer, reads of the source's tables fail and name the host.

**If the code changes.**

- A container that comes back running other code is refused, naming the hash the source is pinned to and the one the host reports, until a steward reviews the code and re-pins it from the form.
- To change the code itself, switch the source back to dev, save the new code, and build again.

### Provisa's services {: #provisas-services }

Your code can import one module that Provisa always provides on the server: `provisa_services` in Python, `@provisa/services` in Node.js, Bun and Deno. Nothing needs installing, and it isn't on any package registry. Outside Provisa it doesn't exist, so your tests mock it.

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

| Call | What it does |
|---|---|
| `secrets.get(name)` | One of the source's declared secrets, read from the vault now. See [Secrets](#secrets). |
| `settings.get(name, default)`, `settings.all()` | The source's settings, from **Settings** on its form, which a steward can change without changing the code. A missing one with no default raises. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | The source's own cache, shared by every worker of it. A value is anything JSON can hold, up to 1 MB, kept for `ttl` seconds or until replaced. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | The source's own vector collection, for its own reference data. An item has an `id` and a `text` or a `vector`; a text is embedded by one of your organization's vector models. It can't search your governed tables, whose data reaches code only as parameters. |
| `http.get(url)`, `http.post(url, ...)` | A fetch Provisa makes for the code, through the deployment's outbound proxy and certificate authorities. The host must be on the egress allow-list, and each fetch is recorded in the audit trail. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` in JavaScript) | Provisa's LLM service. See [Calling a model](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | A log line, and a metric point named `code_source.<name>`, attached to the request your code is serving. |

- A call the service refuses raises a `ServiceError` that names the service and says why.
- The services answer code running on the Provisa server. On a remote host every call raises.

#### Secrets {: #secrets }

A key for a paid API belongs in your organization's vault, not in the code.

- Under **Secrets** on the source's form, pick the organization secrets the code may read. In a config file, list them in the source's mapping:

    ```yaml
    mapping:
      secrets: [openrouter]
    ```

- A name on its own is the organization secret of that name. To read a deployment environment variable instead, give the reference: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`.
- The list refers to the vault and never copies a value, so a config naming a secret nobody has stored yet still loads.

Your code reads a secret with `secrets.get(name)`, or straight from `$PROVISA_SERVICES/secrets/<name>`:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

- `PROVISA_SERVICES` is the one environment variable your code gets. The request goes through the same egress proxy as the rest of its traffic, and the proxy answers it itself; nothing else of Provisa is reachable that way.
- The address carries a token only this source's worker holds. It reads only the names the source lists, and stops working when the worker stops.
- Each read resolves the secret in the vault at that moment, so a rotated secret takes effect on the next read.
- A name the source doesn't list is refused, and so is a secret that can't be resolved. The refusal names the secret, never its value.
- Every read is recorded in the organization's audit trail with the source, the name and the time. No value is ever written to a log.
- The host the API is on still has to be on the egress allow-list.

#### Calling a model {: #calling-a-model }

Your code can call a language model through Provisa instead of holding a vendor key. `$PROVISA_SERVICES/llm/v1` is an OpenAI-compatible endpoint, so any OpenAI SDK works with its base URL pointed there. The address is the credential, so the key the SDK asks for can be any text.

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

- **By role.** Name a model as `provisa:<role>` and you get whichever model your organization assigns that role under **Admin → AI Models**; it follows that choice when an administrator changes it. You can also name a model directly when a role is assigned it.
- **Embeddings.** `$PROVISA_SERVICES/llm/v1/embeddings` takes any of your organization's vector models.
- Any other model is refused.
- The call goes out through your organization's own vendors, endpoints and keys, and what it sends passes through Provisa's redaction rules first.
- Streaming isn't offered. Send `stream: false`, which is the default.

## Safeguards {: #safeguards }

### The sandbox {: #the-sandbox }

- Your code runs in its own worker process, in a sandbox. It can read its own installed code and nothing else on disk.
- It can reach only the hosts on the deployment's egress allow-list, through a proxy Provisa runs, and never Provisa's own ports.
- A host that can't provide the sandbox refuses to start the worker and names what is missing; code never runs unconfined. On Windows, code sources are refused.
- On a remote host, the container's network policy is the code's egress boundary. The server's egress allow-list applies to dev mode.

### Refusals {: #refusals }

Each of these is refused, before your code runs where it can:

- An unknown tag or a type Provisa can't map, every problem of a function at once.
- A read that leaves out a required parameter.
- A pushdown or `loader` declaration the function's types can't take.
- A package with dependencies and no lockfile.
- A server directory outside the allowed ones, or on a server of many organizations.
- A remote host running other code than the source is pinned to.
- A secret the source doesn't list, a model that isn't allowed, a service call the service won't answer.

### Metering and caps {: #metering }

- Every model call is recorded for the source, with the model, its tokens and its cost at the model's entry in **Polly model prices**, and written to the organization's audit trail.
- To limit what a source spends, set **LLM monthly cap** on its form, in USD. In a config file it goes in the mapping:

    ```yaml
    mapping:
      llm: {cap_usd: 20}
    ```

- A source with a cap can call only models that have a price there.
- Once its spending for the month (UTC) reaches the cap, its calls are refused until the month turns. Calls already under way when the cap is reached still finish, so the month can end slightly over the cap.

### Governance on results {: #governance }

- Your function's rows are checked against the table's columns.
- Visibility, masks and row rules then apply to them for each reader, as they do for any table.
- Your code sees a governed table's data only as the parameters a read gives it.

### Tracing {: #tracing }

- Each call to your code is traced as part of the request that triggered it: the function, its kind, how many arguments it got (never their values), the rows and batches it returned, its duration and the worker's CPU.
- Each service call is traced the same way.
- At the normal trace detail these facts are on the request's span; at debug detail each call is a span of its own.
- If your code uses OpenTelemetry, its spans join the same trace.
