# Node and Python Sources

Sometimes the data you need sits behind code your team already wrote: a scoring model, a client for an internal service, a script that tidies an export. A Node or Python source lets you serve that code as tables. You tag the functions you want to share, point Provisa at the code, and each tagged function becomes a table that every role reads under the same rules as any other table.

## What a function becomes {: #what-a-function-becomes }

Provisa reads your code's own declarations: JSDoc tags and TypeScript types for Node, docstrings and type hints for Python. Three tags count.

| Tag | Becomes |
|---|---|
| `@table` | A table. The function returns a list or an iterable, and each element is a row |
| `@command` | A command, run on request |
| `@stream` | A live table. The function is a generator, and each row is sent as it is yielded |

A function without a tag is ignored. A tag Provisa doesn't know is refused, and so is a type it can't map. The refusal names the function and lists every problem at once, so you fix them in one pass.

The row type gives the columns. Text, numbers, booleans, dates and timestamps map to their column types, lists stay lists, and nested objects become named types. When a row's type is loose (`any`, `unknown` or a `Record` in TypeScript, `Any` or a `dict` in Python), Provisa calls the function once, with no arguments, and suggests columns from the rows that come back. If the function has a required parameter it can't be called that way, so you type the columns yourself when you register the table.

The function's parameters become the table's parameters. A parameter without a default is required, so a read that leaves it out is refused before your code runs. A parameter with a default is optional; leave it out and your default applies. A steward can still make an optional one required.

## Where the code comes from {: #where-the-code-comes-from }

The code is pinned, so a source always runs exactly what was registered:

- **A package**: an npm package for Node or a PyPI package for Python, at an exact version.
- **A Git repository**: a full 40-character commit, with a subdirectory if the code isn't at the top.
- **An upload**: a `.zip`, `.tgz` or `.tar.gz` of the code.

Node code runs on Node.js, Bun or Deno, whichever you pick. Python code runs on the Python Provisa ships. When the code has more than one top-level Python module, name the one to enter by in **Entry module**.

## Installing dependencies {: #installing-dependencies }

Code with dependencies needs a lockfile, and every install follows it exactly. Without one, a package that declares dependencies is refused, and the refusal tells you to add one.

| Runtime | Lockfile |
|---|---|
| Node.js | `package-lock.json` or `npm-shrinkwrap.json` |
| Bun | `bun.lock` or `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (with its `pyproject.toml`), `pylock.toml`, or a `requirements.txt` with `--hash` on every line |

A lockfile is optional, but without one two installs of the same code can differ, so a test that passes today can fail tomorrow on a new dependency version. For an identical install every time, the code must carry a lockfile, or you create one and give it here. A built image is fixed already. Once you build the source's image ([Build image](#running-on-a-remote-host)), its dependencies are inside it, and if that image is what goes through your testing and CI/CD, the image you tested is the one that runs. A lockfile matters for dev mode and for each new build of the image. Code that carries its own lockfile installs from it: a Git repo or upload with one, or an npm package whose author published `npm-shrinkwrap.json` (npm never publishes `package-lock.json`). Code without one installs with its dependencies floating: a Git repo or upload with no lockfile, most npm packages, and Python packages from PyPI, whose wheels have no place for one. Give a lockfile beside such code so every install is identical. Some code needs none, because the way it is distributed already fixes every dependency.

No package gets to run its own install scripts: npm installs with `--ignore-scripts`, and Python installs wheels only, never building from a source distribution.

Packages come from your organization's own registries when you have them. An administrator enters your npm registries, PyPI indexes and Git hosts once under **Admin → Package registries**, and every Node and Python source of the organization installs through them. The public npm registry and the public PyPI need no entry. See [Package Registries](package-registries.md).

An install is cached by the code's content hash, so the same code installs once.

## The sandbox {: #the-sandbox }

Your code runs in its own worker process, in a sandbox. It can read its own installed code and nothing else on disk. It can reach only the hosts on the deployment's egress allow-list, through a proxy Provisa runs, and never Provisa's own ports. A host that can't provide the sandbox refuses to start the worker and names what is missing, rather than running the code unconfined. On Windows, code sources are refused.

## Secrets {: #secrets }

Code that calls a paid API needs its key, and the key belongs in your organization's vault, not in the code. Under **Secrets** on the source's form, pick the organization secrets the code may read. In a config file, list them in the source's mapping:

```yaml
mapping:
  secrets: [openrouter]
```

A name on its own is the organization secret of that name. To read a deployment environment variable instead, give the reference: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`. The list is a reference to the vault, never a copy of a value, so a config that names a secret nobody has stored yet still loads.

Your code gets one environment variable, `PROVISA_SERVICES`, and reads a secret from `$PROVISA_SERVICES/secrets/<name>` when it needs it. In Python:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

In TypeScript:

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

The request goes through the same egress proxy as the rest of your code's traffic, and the proxy answers it itself. Nothing else of Provisa is reachable that way. The address carries a token that only this source's worker holds; it reads only the names the source lists, and it stops working when the worker stops. Each read resolves the secret in the vault at that moment, so a rotated secret takes effect on the next read.

A name the source doesn't list is refused, and so is a secret that can't be resolved. The refusal names the secret, never its value. Every read is recorded in the organization's audit trail with the source, the name and the time, and no value is ever written to a log. The host the API is on still has to be on the egress allow-list.

## Calling a model {: #calling-a-model }

Your code can call a language model through Provisa instead of holding a vendor key. `$PROVISA_SERVICES/llm/v1` is an OpenAI-compatible endpoint, so any OpenAI SDK works when you point its base URL there. The address is the credential, so the key the SDK asks for can be any text. In Python:

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

In TypeScript:

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

Name a model by its role, as `provisa:<role>`: you get whichever model your organization assigns that role under **Admin → AI Models**, and it follows that choice when an administrator changes it. You can also name a model directly when a role is assigned it. For embeddings, `$PROVISA_SERVICES/llm/v1/embeddings` takes any of your organization's vector models. Any other model is refused by name. The call goes out through your organization's own vendors, endpoints and keys, and what it sends passes through Provisa's redaction rules first. Streaming isn't offered: send `stream: false`, which is the default.

Every call is recorded for the source, with the model, its tokens and its cost at the model's entry in **Polly model prices**, and is written to the organization's audit trail. To limit what a source spends, set **LLM monthly cap** on its form, in USD. In a config file it goes in the mapping:

```yaml
mapping:
  llm: {cap_usd: 20}
```

A source with a cap can call only models that have a price there. Once its spending for the month (UTC) reaches the cap, its calls are refused until the month turns. Calls already under way when the cap is reached still finish, so the month can end slightly over the cap.

## Provisa's services {: #provisas-services }

Your code can import one module that Provisa always provides wherever it runs: `provisa_services` in Python, and `@provisa/services` in Node.js, Bun and Deno. Nothing needs installing, and it isn't on any package registry. Outside Provisa the module doesn't exist, so tests of your functions mock it.

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
| `settings.get(name, default)` | One of the source's settings, from **Settings** on its form, which a steward can change without changing the code. Missing with no default, it raises. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | The source's own cache, shared by every worker of it. A value is anything JSON can hold, up to 1 MB, kept for `ttl` seconds or until replaced. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | The source's own vector collection, for its own reference data. An item has an `id` and a `text` or a `vector`; a text is embedded by one of your organization's vector models. It can't search your governed tables: their data reaches code only as parameters. |
| `http.get(url)`, `http.post(url, ...)` | A fetch Provisa makes for the code, through the deployment's outbound proxy and certificate authorities. The host must be on the egress allow-list, and each fetch is recorded in the audit trail. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` in JavaScript) | Provisa's LLM service. See [Calling a model](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | A log line, and a metric point named `code_source.<name>`, attached to the request your code is serving. |

A call the service refuses raises a `ServiceError` that names the service and says why.

The services answer code running on the Provisa server. On a remote host they aren't reachable yet, and every call raises.

Each call to your code is traced as part of the request that triggered it, with the function, its kind, how many arguments it got (never their values), the rows and batches it returned, its duration and the worker's CPU. Each service call is traced the same way. At the normal trace detail these facts are on the request's span; at debug detail each call is a span of its own. If your code uses OpenTelemetry, its spans join the same trace.

## Running on a remote host {: #running-on-a-remote-host }

A source runs on the Provisa server by default; this is dev mode. When its code needs a machine the server isn't, such as a GPU box or a host inside another network, build its image and run it there. The source then runs in built mode, and its tables, grants and readers stay the same.

**Build the image.** Save the source in dev mode first, so its code is read and pinned. Under **Remote host** on its form, choose the architecture (Linux x86-64 or ARM64) and where the image goes: one of your organization's container registries, entered under **Admin → Container registries**, or a download. Provisa assembles the image itself. It holds the code at its pinned hash, its dependencies installed for that platform from the code's lockfile through your package registries, the pinned runtime, and the remote host program, on a minimal base image. Nothing in the image runs while it is built, and no Docker daemon is involved. The image's tag carries the code hash and Provisa's version, and its labels name the runtime, the code hash and the exports.

**Run it.** The form shows a compose service for the image. A downloaded image is loaded first with `docker load -i <file>`. The container listens on port 7443 inside and keeps its identity on the volume. You choose the host port with `PROVISA_HOST_PORT`; the service won't start without it:

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

**Connect it.** Back on the form, enter the host Provisa reaches the container at, the port you published, and the pairing code. On a shared Docker network or in Kubernetes, enter the container or Service name and 7443, with nothing published. Provisa pairs with the container over mutual TLS. The code is used once, and from then on each side accepts only the other. Provisa then checks that the container runs the source's code and offers its exports, and switches the source to built mode. Every Provisa process keeps its own connection to the host, and dials again if the host stops answering. While the host doesn't answer, reads of the source's tables fail and name the host.

**If the code changes.** A container that comes back running other code is refused, naming the hash the source is pinned to and the one the host reports, until a steward reviews the code and re-pins it from the form. To change the code itself, switch the source back to dev, save the new code, and build again.

In the container, its network policy is the code's egress boundary; the server's egress allow-list applies to dev mode.

## Reading a table {: #reading-a-table }

A code source's table is landed, never read in place. Provisa calls the function with the read's arguments, checks each row against the columns, and keeps the rows under those arguments for the table's cache TTL. Visibility, masks and row rules then apply to the landed rows, as they do for any table.

You pass arguments the way you pass any table parameter: as `_nf_` conditions in SQL and as field arguments in GraphQL.

## Batched keys: the `loader` parameter {: #batched-keys }

A join can reach a code table: the rows of another table supply its parameters, as in customers joined to their churn score. By default Provisa calls the function once for each distinct set of arguments the joining rows give, and sends all those calls to the worker in one request.

A function that takes a parameter named `loader` is called once per batch instead, the pattern GraphQL's DataLoader made familiar. `loader` is a hash: a dict (an object in TypeScript) from a stable hash of each key to the key itself, a JSON object holding the joining rows' values for the function's other parameters. Provisa de-duplicates the keys, so each distinct combination of values appears once. A key is sparse: a field with no value is left out rather than sent as null, so the function's own default applies, as it does on a single call.

The function returns the same shape: a hash from each key it was given to that key's rows, ordinary rows of the table. Provisa gives each key's rows to every joining row that shared the key, and a key can map to no rows or many. Matching by the hash, not by the rows' values, keeps a sparse key matched: the function fills its missing fields with defaults, so the values it sees differ from the joining row's. Provisa computes the hash; the function only echoes back the keys it was given. The function still declares its row type alone, a list of rows, and each key's rows are of that type. Its parameters beside `loader` need defaults, because a loader call passes `loader` alone.

A function can cap its batch size, as DataLoader's `maxBatchSize` does, by declaring `loader N` in any of three places: a `@loader 100` line beside `@table` in its docstring or JSDoc, a `/* @loader 100 */` comment just before the `loader` parameter in TypeScript, or an `Annotated` string on the parameter in Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. Provisa then splits the keys into batches of that size, calls the function once per batch, and merges the answers by hash. Without a size, one batch holds every key. Discovery refuses by name a size on a function with no `loader` parameter, a size that isn't a positive integer, and two sizes on one function.

Use `loader` when one call over many keys does better than many calls over one:

- Scoring a model over a whole batch in one vectorized pass.
- Batching requests to an API or a language model that accepts many inputs at once.
- Setting up once per batch, such as loading a model or opening a connection, rather than once per key.

This Python function scores every customer of a batch in one pass:

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

Joined to a table of customers, it is called with a `loader` such as `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. The second key carries only `customer_id`, so its plan and region take the defaults. The function returns `{"9f2c...": [...], "41ab...": [...], ...}`, and Provisa gives each list of rows to every customer row that shared that key.

The same function in TypeScript:

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

## An example {: #an-example }

This Python module scores how likely an account is to leave:

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

Register it as a `python` source from its PyPI package, Git commit or upload. Discovery then offers `churn_risk` as a table with four columns (`account_id`, `score`, `computed_at`, `reasons`), a required parameter `account_id` and an optional one, `as_of`. Once a steward registers the table, read it in SQL:

```sql
SELECT score, reasons
FROM churn_risk
WHERE _nf_account_id = 'A-1042'
```

The same function in TypeScript reads like this:

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
