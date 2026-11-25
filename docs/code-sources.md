=== "Docstring"

    

=== "SDK"

    # Code Resolvers

Business logic in Python, Node or Go.

If you have written a GraphQL server, you know this pattern. A code resolver is a function that answers for one object type. Over GraphQL, a function tagged `@table` *is* the resolver for its table's type. Provisa calls it with the query's arguments and governs what comes back.

What differs from a resolver you write by hand:

- **Governed.** Visibility, masks and row rules apply to the rows your function returns, as they do for any table.
- **Batched.** A `@loader` function gets every key of a join in one call, the DataLoader pattern.
- **Told what the read asks.** The read's filters, order, limit and columns reach your function as its read context, for it to use or ignore.
- **Served everywhere.** The same function answers GraphQL, SQL (over HTTP, pgwire and Arrow Flight), Cypher (over HTTP and Bolt), gRPC, REST, JSON:API and MCP.


Code resolvers are one of three ways to write business logic in Provisa. See [Business Logic in Provisa](business-logic.md).

## A first resolver {: #an-example }

=== "Docstring"

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

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import table


    @dataclass
    class Region:
        code: str
        name: str


    @table
    def regions() -> list[Region]:
        """Sales regions."""
        return [
            Region(code="emea", name="Europe, Middle East and Africa"),
            Region(code="amer", name="Americas"),
            Region(code="apac", name="Asia Pacific"),
        ]
    ```

To serve it:

1. Add a `python` source and point it at the code: a PyPI package, a Git commit, an upload or a folder (see [Where the code comes from](#where-the-code-comes-from)).
2. Save it. Provisa installs the code, reads it, and offers `regions` as a table with the columns `code` and `name`.
3. A steward registers the table in a domain, here `sales`, and grants it like any other.

Then read it in GraphQL:

```graphql
{
  regions {
    code
    name
  }
}
```

Or in SQL:

```sql
SELECT code, name
FROM sales.regions
```

Node works the same way, with JSDoc tags and TypeScript types; see [Node](#node).

Each example shows the tag in a docstring or JSDoc first. Its **SDK** tab shows the same function written with Provisa's optional SDK, `provisa-sdk` for Python, `@provisa/sdk` for TypeScript or `provisa.dev/sdk/go` for Go, where each tag is a decorator, a wrapper or a registration. Provisa reads the two as the same function. The SDK adds one thing: your type checker holds the function to its tag's rules while you write it, so a `@table` that returns one object or a `@loader` without a read context fails in your editor instead of when Provisa reads the code. Code that imports nothing of Provisa's keeps working, and the docstring form stays.

## Build {: #build }

### What a function becomes {: #what-a-function-becomes }

Provisa reads your code's own declarations: docstrings and type hints in Python, JSDoc tags and TypeScript types in Node.

| Tag | Becomes |
|---|---|
| `@table` | A table. The function returns a list or an iterable, and each element is a row. |
| `@command` | A command, run on request. |
| `@stream` | A live table. The function is a generator, and each row is sent as it is yielded. |
| `@columns` | A column transform: a batch of rows in, one result per row out. See [Column transforms](#column-transforms). |

- A function without a tag is ignored.
- A tag Provisa doesn't know is refused, and so is a type it can't map. The refusal lists every problem at once, so you fix them in one pass.

The row type gives the columns:

- Text, numbers, booleans, dates and timestamps map to their column types. Lists stay lists, and nested objects become named types.
- In Python a row type is a dataclass, a `TypedDict`, a `NamedTuple`, a Pydantic model or any class with annotated fields. A Pydantic model's rows are sent as `model_dump(by_alias=True)` gives them, so a field with an alias becomes a column named by its alias.
- In TypeScript a row type is an interface, a type alias or a class.
- A `@table` function can also return or yield Arrow instead of rows: a pyarrow `Table`, `RecordBatch` or `RecordBatchReader` in Python, or an apache-arrow `Table` or `RecordBatch` in Node. Arrow is sent as it is, its columns matched to the row type's by name, and any column the row type doesn't declare is dropped. Python casts each column to its declared type; in Node each column must already have it.
- A decimal column holds up to 29 whole digits and 9 decimal places. A value with more is refused by name and never rounded.
- A field's own documentation becomes its column's description when you register the table: a Pydantic `Field(description=...)`, or a doc comment on a TypeScript property.
- A loose row type (`Any` or a `dict` in Python, `any`, `unknown` or a `Record` in TypeScript) has no columns to read, so it is refused, naming the function. Declare a typed row whose fields are the columns. A single field of a typed row may be loose; it becomes a JSON column.

### Parameters {: #parameters }

A `@table` or `@stream` function takes up to two parameters, in this order:

- **Its inputs**: one object whose fields are the table's parameters. In Python that's a dataclass, a `TypedDict`, a `NamedTuple` or a Pydantic model; in TypeScript, an interface or a type alias.
- **Its read context**: a `ReadContext`, what the read asks of the rows (see [The read context](#read-context)).

Either can be left out. Any other signature is refused, naming the function. This one scores one account:

=== "Docstring"

    ```python
    from dataclasses import dataclass


    @dataclass
    class Risk:
        score: float
        reasons: list[str]


    @dataclass
    class RiskInputs:
        account_id: str


    def churn_risk(inputs: RiskInputs) -> list[Risk]:
        """Churn risk of one account.

        @table
        """
        return [Risk(score=0.82, reasons=["no logins in 30 days"])]
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import table


    @dataclass
    class Risk:
        score: float
        reasons: list[str]


    @dataclass
    class RiskInputs:
        account_id: str


    @table
    def churn_risk(inputs: RiskInputs) -> list[Risk]:
        """Churn risk of one account."""
        return [Risk(score=0.82, reasons=["no logins in 30 days"])]
    ```

Each field of the inputs object is a parameter. In GraphQL a parameter is a field argument:

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

- A field without a default is required. A read that leaves it out is refused before your code runs.
- A field with a default is optional. Leave it out and your default applies.
- A field's own documentation (a Pydantic `Field(description=...)`, a doc comment on a TypeScript property) becomes its parameter's description.
- A steward can still make an optional one required.
- A parameter is an input, never a filter. A field named like a return column, such as `region`, is the `_nf_region` input. A condition on the `region` column reaches your function through its read context.

Commands and `@columns` functions keep their own parameters, each its own argument.

### What reaches your function {: #what-reaches-your-function }

A call gets the read's inputs and its read context, and nothing else of the request. Your function never sees who is asking. The caller's identity and roles stay with Provisa, which governs the rows your function returns.

### The read context {: #read-context }

A function that declares a `ReadContext` parameter is told what the read asks of its rows:

| Field | Holds |
|---|---|
| `filters` | Each condition the read puts on a return column: `column`, `op` (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `like`) and `value` |
| `order_by` | The read's sort keys: `column`, `descending`, `nulls_first` |
| `limit` | The most rows the read keeps, its offset added in; none when every row counts |
| `columns` | The return columns the read names |
| `keys` | A `@loader`'s join keys (see [Batched keys](#batched-keys)); none otherwise |

=== "Docstring"

    ```python
    from collections.abc import Iterator
    from dataclasses import dataclass

    from provisa_services import ReadContext


    @dataclass
    class Order:
        id: int
        status: str
        total: float


    @dataclass
    class OrderInputs:
        region: str = "EU"


    def orders(inputs: OrderInputs, context: ReadContext) -> Iterator[Order]:
        """Orders of one region.

        @table
        """
        query = api.orders(region=inputs.region)
        for f in context.filters:
            if f.column == "status" and f.op == "eq":
                query = query.where(status=f.value)  # fetch only what the read keeps
        yield from query
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import Stream, table
    from provisa_services import ReadContext


    @dataclass
    class Order:
        id: int
        status: str
        total: float


    @dataclass
    class OrderInputs:
        region: str = "EU"


    @table
    def orders(inputs: OrderInputs, context: ReadContext) -> Stream[Order]:
        """Orders of one region."""
        query = api.orders(region=inputs.region)
        for f in context.filters:
            if f.column == "status" and f.op == "eq":
                query = query.where(status=f.value)  # fetch only what the read keeps
        yield from query
    ```

In TypeScript, `import type { ReadContext } from "@provisa/services"`.

Everything in the context is a hint. Provisa applies the whole statement to the rows your function returns, so a function can ignore any part of it, or loosen a filter, and still return the right answer. One rule holds for the limit:

- **If you apply the limit, apply every filter and the order first.** Otherwise ignore it. A function that cuts to the limit before a filter would leave Provisa too few rows.

Provisa gives a limit only when the read limits the table's own rows: a read of the table alone, with no grouping, aggregate, window or `DISTINCT`, where every condition is one of the filters and every sort key is a column. A filter is given when it compares a return column with a value, in a condition joined to the others by `AND`. A condition under `OR` or `NOT`, or on an expression, isn't given, and neither is a limit after it.

The context holds for a read that goes straight to your function, and for one an engine sends to a built source's host. A read that lands your table gives an empty context, and your function returns the whole table for its inputs (see [Reading a table](#reading-a-table)).

### Batched keys: `@loader` {: #batched-keys }

A join can reach a code table, with the rows of another table supplying its parameters: customers joined to their churn score, say. By default Provisa calls the function once for each distinct set of inputs the joining rows give, and sends all those calls to the worker in one request.

A function tagged `@loader` is called once per batch instead, the pattern GraphQL's DataLoader made familiar.

- **What it gets.** The batch's keys arrive in its read context, `context.keys`: a hash, a dict (an object in TypeScript) from a stable hash of each key to the key itself. A key is a JSON object holding the joining rows' values for the function's inputs. The inputs object itself is left at its defaults.
- **De-duplicated.** Each distinct combination of values appears once.
- **Sparse.** A field with no value is left out of the key, so the function's own default applies, as it does on a single call.
- **What it returns.** A hash from each key it was given to that key's rows: ordinary rows of the table, none or many. Provisa gives each key's rows to every joining row that shared the key.
- **Matched by hash.** The function fills a sparse key's missing fields with defaults, so the values it sees differ from the joining row's. Provisa computes the hash, and the function only echoes back the keys it was given.
- **Typed as usual.** The function still declares its row type alone, a list of rows.
- **Batch size.** Cap it, as DataLoader's `maxBatchSize` does, with `@loader 100` in place of `@loader`. Provisa then splits the keys into batches of that size and merges the answers by hash. Without a size, one batch holds every key.
- **Refused.** A `@loader` with no `ReadContext` parameter, a size that isn't a positive integer, two sizes on one function, and `@loader` on anything but a `@table`.

Use `@loader` when one call over many keys does better than many calls over one:

- Scoring a model over a whole batch in one vectorized pass.
- Batching requests to an API or a language model that accepts many inputs at once.
- Setting up once per batch, such as loading a model or opening a connection.

=== "Docstring"

    ```python
    from dataclasses import dataclass

    from provisa_services import ReadContext


    @dataclass
    class ScoreInputs:
        customer_id: str | None = None
        plan: str = "basic"
        region: str = "US"


    @dataclass
    class Score:
        customer_id: str
        score: float


    def churn_score(
        inputs: ScoreInputs, context: ReadContext
    ) -> list[Score] | dict[str, list[Score]]:
        """Churn score of each customer in the batch.

        @table
        @loader
        """
        given = {"customer_id": inputs.customer_id, "plan": inputs.plan, "region": inputs.region}
        keys = context.keys or {"": given}
        rows = {h: {"plan": "basic", "region": "US", **key} for h, key in keys.items()}
        scores = MODEL.predict(features(list(rows.values())))  # one vectorized call for the batch
        out = {h: [Score(r["customer_id"], s)] for (h, r), s in zip(rows.items(), scores)}
        return out if context.keys else out[""]  # a loader call returns the hash of key to rows
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import table
    from provisa_services import ReadContext


    @dataclass
    class ScoreInputs:
        customer_id: str | None = None
        plan: str = "basic"
        region: str = "US"


    @dataclass
    class Score:
        customer_id: str
        score: float


    @table(loader=True)
    def churn_score(
        inputs: ScoreInputs, context: ReadContext
    ) -> list[Score] | dict[str, list[Score]]:
        """Churn score of each customer in the batch."""
        given = {"customer_id": inputs.customer_id, "plan": inputs.plan, "region": inputs.region}
        keys = context.keys or {"": given}
        rows = {h: {"plan": "basic", "region": "US", **key} for h, key in keys.items()}
        scores = MODEL.predict(features(list(rows.values())))  # one vectorized call for the batch
        out = {h: [Score(r["customer_id"], s)] for (h, r), s in zip(rows.items(), scores)}
        # A loader call returns the hash of each key to its rows; a single call returns the rows.
        return out if context.keys else out[""]
    ```

    `@table(loader=100)` is `@loader 100`.

Joined to a table of customers, its context holds keys such as `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. The second key carries only `customer_id`, so its plan and region take the defaults. The function returns `{"9f2c...": [...], "41ab...": [...], ...}`.

### Column transforms: `@columns` {: #column-transforms }

A function tagged `@columns` computes new columns from the columns of rows you already have, such as splitting an address into its parts, classifying a description or enriching a name. It takes a batch of rows and returns one result per row, in the same order.

Each parameter is one input column, given as a list of that batch's values in row order. The function returns a list with one object per input row, and the object's fields are the output columns. In Python:

=== "Docstring"

    ```python
    from dataclasses import dataclass


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

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import Batch, Results, columns


    @dataclass
    class Address:
        street: str
        city: str
        country: str


    @columns
    def clean_address(raw_address: Batch[str]) -> Results[Address]:
        """Each raw address of the batch, as its parts."""
        ...
    ```

In TypeScript, declare it as `clean_address(raw_address: string[]): Promise<Address[]>` with `@columns` in its JSDoc.

You register it as a command, one at a time like any other. On the **Commands** page, pick the source, and its `@columns` functions are offered to pick from. The function's declaration supplies the input and output columns. Register it into a domain with the roles that may use it.

Apply it in a select list, and its output columns take the place of the call:

```sql
SELECT id, clean_address(raw_address) FROM suppliers
```

This answers `id, street, city, country`. Rename the outputs in an outer query if you need other names. The call has to be an item of the statement's own select list or of a CTE's select list (see [Column transforms in CTEs](#column-transforms-in-ctes) below). A call in a `WHERE`, a `JOIN`, a `GROUP BY` or inside another expression is refused, and so is an alias on the call. Each output column is a named step of the projection's lineage.

**Streaming.** Column transforms stream. The statement reads its input as Arrow record batches; each batch is cut to 1,000 rows before being sent to a worker, and the worker's output columns are passed on as soon as that batch is answered. The result is never materialized whole, so memory stays flat whatever the row count, and the first rows reach the caller before the last rows are read. Order is kept.

The workers run batches in parallel when you set more than one on the source's form (`workers` in its mapping, see [Dev mode and workers](#workers)); each added worker adds throughput. A function that keeps no state between calls benefits most. A batch answered with a different number of results than rows it was given is refused, naming the function.

The function can call Provisa's services, the LLM service among them, so one model call can clean, classify or enrich a whole batch. Each call is metered and capped against the source like any of its other calls ([Calling a model](#calling-a-model)).

A view that applies a column transform is landed, whether or not you declare it a materialized view. Provisa builds it as it builds a materialized view, calling the function on the view's rows in batches and storing the result, and the view is read from that stored table. A statement can then filter, join or group on the output columns like any other. The outputs are computed again each time the view is refreshed, as its freshness declares. They have no schedule of their own.

#### Column transforms in CTEs {: #column-transforms-in-ctes }

A column transform can also appear in a common table expression (CTE). The CTE becomes a staged step. Provisa runs it on its own first, reading its input, sending the rows to the function in 1,000-row batches and landing the enriched rows in a session temporary table. The rest of the statement then runs against that table.

```sql
WITH enriched AS (
    SELECT id, region, clean_address(raw_address) FROM suppliers
)
SELECT region, COUNT(*) AS n
FROM enriched
WHERE country = 'DE'
GROUP BY region
```

The `WHERE country = 'DE'` filter cannot push past the transform (the column is computed), so it runs after the CTE is staged. But conditions on input columns can push down into the CTE's read, and a join key from another part of the statement can restrict the CTE's rows before the function is called, so only the rows the statement actually uses are enriched.

Two pushdown rules apply:

- A condition on an input column whose value is known before the statement runs (a literal, a bound parameter, a session variable) is pushed into the CTE's read at once.
- When the CTE is inner-joined to another relation on its input columns, that side runs first and its keys restrict the CTE's read.

Each rule is applied only where it leaves the answer unchanged. A condition on a computed output column, a condition under `OR`, and an outer join do not trigger a pushdown.

Statements with no column transform are compiled and run exactly as they always were; the staging logic is entered only when a transform is called.

### Writing to a table: commands {: #writes }

SQL and GraphQL writes to a code table go to its source's commands. A `@command` is bound to a table's operation by its input type:

| Operation | The command takes |
|---|---|
| `INSERT` | `rows`: a list of the table's row type |
| `UPDATE` | `keys` and `changes`: a list of the table's primary-key values, then an object with some of the row type's fields, each optional |
| `DELETE` | `keys` |

A key value is an object of the table's key columns, or the value itself when the key is one column. The table needs a primary key for an update or a delete.

=== "Docstring"

    ```python
    from dataclasses import dataclass


    @dataclass
    class Order:
        id: int
        status: str
        total: float


    @dataclass
    class OrderChanges:
        status: str | None = None
        total: float | None = None


    def add_orders(rows: list[Order]) -> int:
        """@command"""
        return api.create(rows)


    def set_orders(keys: list[int], changes: OrderChanges) -> int:
        """@command"""
        return api.update(keys, changes)


    def remove_orders(keys: list[int]) -> int:
        """@command"""
        return api.delete(keys)
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import command


    @dataclass
    class Order:
        id: int
        status: str
        total: float


    @dataclass
    class OrderChanges:
        status: str | None = None
        total: float | None = None


    @command
    def add_orders(rows: list[Order]) -> int:
        return api.create(rows)


    @command
    def set_orders(keys: list[int], changes: OrderChanges) -> int:
        return api.update(keys, changes)


    @command
    def remove_orders(keys: list[int]) -> int:
        return api.delete(keys)
    ```

Writing works the way it does for any table:

- A write is governed as any write is: the role's write grant names the columns it may set, and its row rules are added to an update's or a delete's `WHERE`.
- An `INSERT`'s rows reach the insert command in batches.
- An `UPDATE` or a `DELETE` first reads the rows it touches: an ordinary read of the table's key columns over the statement's own `FROM` and `WHERE`, joins included, routed as any read is. The command is then called with those keys. An update whose `SET` is computed per row (`SET total = total * 2`) is called once for each distinct set of changes.
- The affected count is the number of rows written or keys given.
- A GraphQL mutation on the table goes the same way.
- The read and the call are not one transaction, so a row can change between the two.

When two commands fit one operation, tag the one it is with `@insert <table>`, `@update <table>` or `@delete <table>`, where `<table>` is the table's `@table` function. Two that fit with no tag are refused when you register the table, and so is a tag on a command that doesn't fit. A write to a table with no command bound for it is refused, naming the operation.

=== "Docstring"

    ```python
    from dataclasses import dataclass


    @dataclass
    class Order:
        id: int
        status: str
        total: float


    def import_orders(rows: list[Order]) -> int:
        """The insert of orders.

        @command
        @insert orders
        """
        return api.create(rows)
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import command


    @dataclass
    class Order:
        id: int
        status: str
        total: float


    @command(insert="orders")
    def import_orders(rows: list[Order]) -> int:
        """The insert of orders."""
        return api.create(rows)
    ```

    `@command(update="orders")` and `@command(delete="orders")` are the other two.

### Relating two tables: code resolvers {: #code-resolvers }

Sometimes equal column values don't tell you which rows of two tables belong together. A store sits inside a region, a document resembles another, or a name nearly matches. A code resolver decides it with your own algorithm. It is a `@query` that takes the source table and the target table as its two table parameters, in that order, and returns one row per match. Each row carries the key of each side and any other column you compute, such as a score.

=== "Docstring"

    ```python
    from dataclasses import dataclass

    from provisa_services import TableRef


    @dataclass
    class Store:
        id: int
        lat: float
        lon: float


    @dataclass
    class Region:
        id: int
        polygon: str


    @dataclass
    class Match:
        store_id: int
        region_id: int
        score: float


    def stores_in_regions(stores: TableRef[Store], regions: TableRef[Region]) -> list[Match]:
        """@query"""
        ...
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import query
    from provisa_services import TableRef


    @dataclass
    class Store:
        id: int
        lat: float
        lon: float


    @dataclass
    class Region:
        id: int
        polygon: str


    @dataclass
    class Match:
        store_id: int
        region_id: int
        score: float


    @query
    def stores_in_regions(stores: TableRef[Store], regions: TableRef[Region]) -> list[Match]:
        ...
    ```

Register the function as a command. Then, on the Relationships page, add a relationship whose target type is **Code resolver**. Pick the source table and its key, the target table and its key, the resolver, and the columns of its rows that hold each key. Provisa keeps the choice as objects of kinds it already has:

- a materialized view over the resolver's call, `SELECT * FROM stores_in_regions('retail.stores', 'geo.regions') AS m`, named by the id you give;
- `<id>`, a junction relationship from the source table to the target table through that view. Cypher serves it as one edge named by the id in upper case, with the match's other columns as its properties: `MATCH (s)-[m:STORE_REGIONS]->(r) RETURN s.id, m.score, r.name`. GraphQL serves it as a field named by the id that reaches the matched target rows: `stores { store_regions { name } }`. The score is not a GraphQL field; read it in Cypher. The view is not a Cypher node.

- **Landed.** The matches are kept as a table. Provisa works them out again when either table changes, and at the latest after the refresh interval. A view over a command call counts the tables the call is given as its inputs. The matches can't be computed each time they are read: Provisa runs a command call before it expands the views a statement reads, so a view held only as SQL would never call its resolver.
- **Governed.** The build reads both tables as the organisation's administrator, as any materialized view's build does. A column of the view is visible to the roles that see both keys. Following `<id>` reads the view and the target table under the reader's own row rules and column visibility.
- **Refused.** A command that isn't a code `@query`, doesn't take exactly two tables, takes any other argument, or whose rows lack either key is refused when you save the relationship, with the reason.
- **One thing.** The view and the relationship carry the set's id, and you change or retire them only as a set. The Relationships page offers **Change code resolver** and **Retire code resolver** on the relationship. Editing or deleting either on its own page is refused, naming the set. Retiring the set retires both, and their names stay taken.

#### Keeping the matches incrementally: `@change` {: #code-resolver-change }

By default every refresh runs the resolver again and replaces all the matches. When that costs too much, add a second function to the same source, tagged `@change`. Provisa then asks it only for the matches that changed since the last run.

=== "Docstring"

    ```python
    from dataclasses import dataclass

    from provisa_services import TableRef


    @dataclass
    class Store:
        id: int


    @dataclass
    class Region:
        id: int


    @dataclass
    class Changed:
        store_id: int
        region_id: int
        removed: bool
        seq: int


    def store_regions_changes(
        stores: TableRef[Store], regions: TableRef[Region], since: int | None = None
    ) -> list[Changed]:
        """Store-region matches changed since ``since``.

        @change table=store_regions
        @change key=store_id,region_id
        @change removed=removed
        @change cursor=seq
        """
        ...
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import change
    from provisa_services import TableRef


    @dataclass
    class Store:
        id: int


    @dataclass
    class Region:
        id: int


    @dataclass
    class Changed:
        store_id: int
        region_id: int
        removed: bool
        seq: int


    @change(table="store_regions", key=["store_id", "region_id"], removed="removed", cursor="seq")
    def store_regions_changes(
        stores: TableRef[Store], regions: TableRef[Region], since: int | None = None
    ) -> list[Changed]:
        """Store-region matches changed since ``since``."""
        ...
    ```

The function's own `Store` and `Region` name only the key columns it reads.

The four tag lines state what only you know.

| Line | States |
|---|---|
| `table=` | The landed table this function keeps fresh. For a code resolver that is the id you gave the set. |
| `key=` | The columns that identify one match, separated by commas. |
| `removed=` | The column that is true when a match no longer holds. |
| `cursor=` | The column whose value orders the changes. |

The function takes the resolver's two tables and one more parameter, the cursor. Provisa passes the largest cursor value it stored after the last run that succeeded. Your function returns the matches that changed after it. Provisa upserts each returned row by its key and deletes the key of a row whose removed column is true. The function never learns where the matches are stored.

On the first run and on every whole rebuild Provisa passes no cursor, and your function returns every match. One function serves both cases, so write the no-cursor branch to return the full result.

Turn it on in the code resolver's form on the Relationships page. Tick **Keep matches incrementally**, then set **Whole rebuild every (seconds)**. The interval is required. A whole rebuild corrects anything the changes missed, and Provisa won't save the setting without one. An administrator's full refresh of the view is also a whole rebuild.

A view you define yourself over a code command has the same setting on its **Load** tab, as **Incremental maintenance** with its own **Whole rebuild every (seconds)**.

- **Unavailable without `@change`.** When the resolver's source declares no `@change` function for the table, the tick is disabled and its help icon says so. A save that turns it on by another route, such as the API, is refused with the same reason.
- **Off means whole.** With the setting off, every refresh runs the resolver's own function and `@change` is never called.
- **Refused results.** A refresh fails, applies nothing and keeps the old cursor when a returned row lacks a key column, when a key appears twice in one result, or when a returned cursor is before the stored one. The view's last error names the function and the row's fault.

### Where the code comes from {: #where-the-code-comes-from }

- **A package.** An npm package for Node, a PyPI package for Python, or a Go module for Go, at an exact version (a Go module's is `v1.2.0`).
- **A Git repository.** A full 40-character commit, with a subdirectory if the code isn't at the top.
- **An upload.** A `.zip`, `.tgz` or `.tar.gz` of the code.
- **A folder on your machine.** **Choose folder** zips the folder in your browser, leaving out `node_modules`, `.venv`, `__pycache__` and `.git`, and uploads it as the source's archive. In a browser that can keep the folder (Chrome, Edge), **Save** zips and sends it again; another browser asks you for the folder again.
- **A directory on the server.** On a single-organization install, a directory under the ones the operator allows. It is read where it is, so an edit is read by the next query. A server of many organizations refuses it.

Every origin but the server directory is pinned, so the source runs exactly what was saved.

- **Entry module.** When Python code has more than one top-level module, name the one to enter by.
- **Package.** Go code is a module; its functions are those of one package, the module's root package unless you name another by its directory (`feeds`).
- **Open in VS Code.** A server directory opens in your installed VS Code; a Git origin opens at its commit on vscode.dev. A package or an upload has no such link.

### Installing dependencies {: #installing-dependencies }

Code with dependencies needs a lockfile, and every install follows it exactly. Without one, a package that declares dependencies is refused, and the refusal tells you to add one.

| Runtime | Lockfile |
|---|---|
| Node.js | `package-lock.json` or `npm-shrinkwrap.json` |
| Bun | `bun.lock` or `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (with its `pyproject.toml`), `pylock.toml`, or a `requirements.txt` with `--hash` on every line |
| Go | `go.sum` |

- **Carried or given.** Code that carries its own lockfile installs from it: a Git repository, upload or folder with one, or an npm package whose author published `npm-shrinkwrap.json` (npm never publishes `package-lock.json`). Most npm packages, and Python packages from PyPI, carry none, so give a lockfile beside them.
- **Why it matters.** Without one, two installs of the same code can differ, and a test that passes today can fail tomorrow on a new dependency version.
- **A built image is fixed already.** Once you [build the source's image](#running-on-a-remote-host), its dependencies are inside it. A lockfile matters for dev mode and for each new build.
- **No install scripts.** npm installs with `--ignore-scripts`, and Python installs wheels only, never building from a source distribution. Go runs no code while it fetches modules, and every module is checked against the code's `go.sum`; one it does not cover is refused by name.
- **Your registries.** Packages come from your organization's own npm registries, PyPI indexes, Go module proxies and Git hosts, entered once under **Admin → Package registries**. The public npm registry, PyPI and Go module proxy need no entry. See [Package Registries](package-registries.md).
- **Cached.** An install is cached by the code's content hash, so the same code installs once.

### Node {: #node }

- Node code runs on Node.js, Bun or Deno, whichever you pick.
- Tags go in JSDoc, and types come from TypeScript.

The scored account in TypeScript:

=== "JSDoc"

    ```typescript
    export interface Risk {
      score: number;
      reasons: string[];
    }

    export interface RiskInputs {
      account_id: string;
    }

    /** Churn risk of one account.
     * @table
     */
    export async function churnRisk(inputs: RiskInputs): Promise<Risk[]> {
      return [{ score: 0.82, reasons: ["no logins in 30 days"] }];
    }
    ```

=== "SDK"

    ```typescript
    import { table } from "@provisa/sdk";

    export interface Risk {
      score: number;
      reasons: string[];
    }

    export interface RiskInputs {
      account_id: string;
    }

    /** Churn risk of one account. */
    export const churnRisk = table(async function churnRisk(inputs: RiskInputs): Promise<Risk[]> {
      return [{ score: 0.82, reasons: ["no logins in 30 days"] }];
    });
    ```

    Each wrapper returns the function it is given. Its type carries the tag, which is all the declarations keep of it, so the export is a `const`.

The batched `churnScore` in TypeScript:

=== "JSDoc"

    ```typescript
    import type { ReadContext } from "@provisa/services";

    declare function predict(rows: object[]): Promise<number[]>; // your model client

    export interface ScoreInputs {
      customer_id?: string;
      plan?: string;
      region?: string;
    }

    export interface Score {
      customer_id: string;
      score: number;
    }

    /** Churn score of each customer in the batch.
     * @table
     * @loader
     */
    export async function churnScore(
      { customer_id = "", plan = "basic", region = "US" }: ScoreInputs,
      context: ReadContext,
    ): Promise<Score[] | Record<string, Score[]>> {
      const keys = context.keys ?? { "": { customer_id, plan, region } };
      const rows = Object.entries(keys).map(([h, key]) => ({
        h,
        customer_id: String(key.customer_id),
        plan: String(key.plan ?? "basic"),
        region: String(key.region ?? "US"),
      }));
      const scores = await predict(rows); // one call for the whole batch
      const out: Record<string, Score[]> = Object.fromEntries(
        rows.map((row, i) => [row.h, [{ customer_id: row.customer_id, score: scores[i] }]]),
      );
      // A loader call returns the hash of each key to its rows; a single call returns the rows.
      return context.keys ? out : out[""];
    }
    ```

=== "SDK"

    ```typescript
    import { table, type ReadContext } from "@provisa/sdk";

    declare function predict(rows: object[]): Promise<number[]>; // your model client

    export interface ScoreInputs {
      customer_id?: string;
      plan?: string;
      region?: string;
    }

    export interface Score {
      customer_id: string;
      score: number;
    }

    /** Churn score of each customer in the batch. */
    export const churnScore = table(
      async function churnScore(
        { customer_id = "", plan = "basic", region = "US" }: ScoreInputs,
        context: ReadContext,
      ): Promise<Score[] | Record<string, Score[]>> {
        const keys = context.keys ?? { "": { customer_id, plan, region } };
        const rows = Object.entries(keys).map(([h, key]) => ({
          h,
          customer_id: String(key.customer_id),
          plan: String(key.plan ?? "basic"),
          region: String(key.region ?? "US"),
        }));
        const scores = await predict(rows); // one call for the whole batch
        const out: Record<string, Score[]> = Object.fromEntries(
          rows.map((row, i) => [row.h, [{ customer_id: row.customer_id, score: scores[i] }]]),
        );
        // A loader call returns the hash of each key to its rows; a single call returns the rows.
        return context.keys ? out : out[""];
      },
      { loader: true },
    );
    ```

    `{ loader: 100 }` is `@loader 100`.

### Go {: #go }

- Go code runs as one program: Provisa compiles your package into its host once per version of the code, with the pinned Go toolchain (1.27.3). A `go.mod` that asks for a newer Go is refused.
- Tags go in the doc comment, each on a line of its own, and types come from the function's signature. An exported function is offered; a method or an unexported function is not.
- A function takes an optional `context.Context` first, cancelled when the read stops, then one struct of its inputs. A pointer field is an optional input; any other field is required.
- A field is named as `encoding/json` names it: its `json` tag, else its Go name.
- A `@table` returns `[]T`, `([]T, error)`, `iter.Seq[T]` or `iter.Seq2[T, error]`. A `@stream` returns `iter.Seq2[T, error]`; a row it yields with an error ends the stream with that error.
- A `string` type with constants of its own is an enum of them; `time.Time` is a timestamp.
- `@loader` and the read context are not yet offered for Go.

The scored account in Go:

=== "Doc comment"

    ```go
    package scoring

    import "context"

    type Risk struct {
    	Score   float64  `json:"score"`
    	Reasons []string `json:"reasons"`
    }

    type RiskInputs struct {
    	AccountID string `json:"account_id"`
    }

    // ChurnRisk is the churn risk of one account.
    //
    // @table
    func ChurnRisk(ctx context.Context, in RiskInputs) ([]Risk, error) {
    	return []Risk{{Score: 0.82, Reasons: []string{"no logins in 30 days"}}}, nil
    }
    ```

=== "SDK"

    ```go
    package scoring

    import (
    	"context"

    	provisa "provisa.dev/sdk/go"
    )

    type Risk struct {
    	Score   float64  `json:"score"`
    	Reasons []string `json:"reasons"`
    }

    type RiskInputs struct {
    	AccountID string `json:"account_id"`
    }

    // ChurnRisk is the churn risk of one account.
    func ChurnRisk(ctx context.Context, in RiskInputs) ([]Risk, error) {
    	return []Risk{{Score: 0.82, Reasons: []string{"no logins in 30 days"}}}, nil
    }

    var _ = provisa.Table[RiskInputs, Risk](ChurnRisk)
    ```

The SDK tab registers the function with `provisa.dev/sdk/go`, an ordinary module your `go.mod` requires and `go.sum` pins. A registration is a package-level `var _ =` whose value names an exported top-level function of the package; Provisa reads it as the tags it stands for, whatever name the import is given:

| Registration | Tag |
|---|---|
| `provisa.Table[I, R](F)`, `provisa.Stream[I, R](F)` | `@table`, `@stream` |
| `provisa.Command(F)`, `provisa.Query(F)`, `provisa.Columns(F)` | `@command`, `@query`, `@columns` |
| `provisa.OnStart(F)`, `provisa.OnStop(F)`, `provisa.Health(F)` | `@on-start`, `@on-stop`, `@health` |
| `provisa.Change(F, "t", []string{"k"}, "removed", "seq")` | `@change table=t`, `key=k`, `removed=removed`, `cursor=seq` |
| option `provisa.Requires("NAME")`, `provisa.Requires("NAME", provisa.Secret)` | `@requires NAME`, `@requires NAME:secret` |
| command option `provisa.Insert("t")`, `provisa.Update("t")`, `provisa.Delete("t")` | `@insert t`, `@update t`, `@delete t` |

- `I` is the inputs struct, `provisa.NoInputs` when the function takes none, and `R` is the row. Go infers neither through the SDK's constraints, so you write them.
- The compiler checks a `Table`, `Stream`, `OnStart`, `OnStop` or `Health` function's signature against its tag's rules. A command, query, column transform or change function can take any parameters, so Provisa checks those when it reads the code.
- Every option's arguments are constants. A registration naming a closure, a method or a variable, an option that isn't a constant, and an SDK call outside a package-level registration are refused, naming each.
- `provisa.TableRef[Row]` is a table a command or query takes. A doc-comment source declares its own generic `TableRef` type instead; Provisa knows either by its name.
- The column aliases `provisa.Text`, `BigInt`, `Double`, `Boolean`, `Timestamp` and `Json` are the Go types they name.

### Rust {: #rust }

- Rust code is a library crate. Provisa compiles your crate into its host once per version of the code, with the pinned Rust toolchain. A `rust-version` newer than that toolchain is refused.
- Tags go in the `///` doc comment, each on a line of its own, and types come from the function's signature. A `pub fn` at the crate's root is offered; a function in a module, or one that is not `pub`, is not.
- A `@table` or `@stream` function takes nothing, or one struct of its inputs. An `Option` field is an optional input; any other field is required. A command takes its parameters by value, never borrowed.
- A field is named as serde names it: `rename`, `rename_all`, `skip`, `flatten` and `default` are read.
- A `@table` returns a `Vec<T>` or an `impl Iterator<Item = T>`. A `@stream` returns an iterator; an item that is a `Result` ends the stream with its error. A trailing `Result<T, E>` is how a function fails.
- An enum of unit variants is an enum of their names. chrono's `DateTime` is a timestamp, `NaiveDate` a date, rust_decimal's `Decimal` a numeric and serde_json's `Value` json.
- A function is neither `async` nor generic.
- `@loader` and the read context are not offered for Rust.

The scored account in Rust:

=== "Doc comment"

    ```rust
    use serde::{Deserialize, Serialize};

    #[derive(Serialize)]
    pub struct Risk {
        pub score: f64,
        pub reasons: Vec<String>,
    }

    #[derive(Deserialize)]
    pub struct RiskInputs {
        pub account_id: String,
    }

    /// The churn risk of one account.
    ///
    /// @table
    pub fn churn_risk(inputs: RiskInputs) -> Result<Vec<Risk>, String> {
        let _ = inputs.account_id;
        Ok(vec![Risk { score: 0.82, reasons: vec!["no logins in 30 days".into()] }])
    }
    ```

=== "SDK"

    ```rust
    use serde::{Deserialize, Serialize};

    #[derive(Serialize)]
    pub struct Risk {
        pub score: f64,
        pub reasons: Vec<String>,
    }

    #[derive(Deserialize)]
    pub struct RiskInputs {
        pub account_id: String,
    }

    /// The churn risk of one account.
    pub fn churn_risk(inputs: RiskInputs) -> Result<Vec<Risk>, String> {
        let _ = inputs.account_id;
        Ok(vec![Risk { score: 0.82, reasons: vec!["no logins in 30 days".into()] }])
    }

    provisa_sdk::table!(churn_risk);
    ```

The SDK tab registers the function with the `provisa-sdk` crate, an ordinary dependency in your `Cargo.toml` with no dependencies of its own. A registration is a macro invoked at the crate's root, naming a `pub fn` at the crate's root. Provisa reads it as the tags it stands for:

| Registration | Tag |
|---|---|
| `table!(f)`, `stream!(f)` | `@table`, `@stream` |
| `command!(f)`, `query!(f)`, `columns!(f)` | `@command`, `@query`, `@columns` |
| `on_start!(f)`, `on_stop!(f)`, `health!(f)` | `@on-start`, `@on-stop`, `@health` |
| `change!(f, table = "t", key = ["k"], removed = "r", cursor = "c")` | `@change table=t`, `key=k`, `removed=r`, `cursor=c` |
| option `requires = "NAME"`, `secret = "NAME"` | `@requires NAME`, `@requires NAME:secret` |
| command option `insert = "t"`, `update = "t"`, `delete = "t"` | `@insert t`, `@update t`, `@delete t` |

- Provisa knows a registration by its path: `provisa_sdk::table!`, the name your `Cargo.toml` gives the dependency (`p = { package = "provisa-sdk", ... }` makes it `p::table!`), a `use provisa_sdk as p;` at the root, or a name a root `use provisa_sdk::{table}` or `use provisa_sdk::*` brings in. A module's own `use` is not followed.
- The compiler checks a `table!`, `stream!`, `columns!`, `on_start!`, `on_stop!` or `health!` function's signature against its tag's rules, and that each option is one its registration takes. A command, query or change function can take any parameters, so Provisa checks those when it reads the code.
- Every option's value is a literal. A registration naming anything but a function by its name, an option that isn't a literal, and an SDK macro invoked inside a module or a function are refused, naming each.
- The aliases `Rows<T>`, `Batch<T>` and `Results<T>` are `Vec<T>`, and `Stream<T>` is `Box<dyn Iterator<Item = T>>`. The column aliases `Text`, `BigInt`, `Double` and `Boolean` are `String`, `i64`, `f64` and `bool`.
- The SDK has no `TableRef`: the host decodes each argument with serde, and the SDK has no serde dependency. Declare your own generic `TableRef` type, as a doc-comment source does; Provisa knows it by its name.

## Deployment {: #deployment }

### Dev mode and workers {: #workers }

A source runs on the Provisa server by default; this is dev mode.

- Your code runs in a worker process of its own, one per source and version of its code in each Provisa process, started on its first call.
- A worker that crashes fails the calls in flight, naming the source and the function. The next call starts a new one.
- A call is cancelled when its request's deadline passes. The worker is told to stop it, and the waiting request is released at once.

### Reading a table {: #reading-a-table }

How a read reaches your function depends on what else it reads:

- **A read of one code source's tables alone** goes straight to your function. Nothing is landed. Provisa calls it with the read's inputs and its read context, and applies the whole statement to the rows it returns.
- **A read that joins other tables** is the federation engine's. A dev-mode source's table is landed for it. Your function gets the read's inputs and an empty context, and returns the whole table for them, which is kept under those inputs for the table's cache TTL.
- **A built source's table** in such a join is read live from its remote host, which the engine attaches as a Postgres catalog. The host gives your function the read context of the part of the statement the engine sends it. It is landed instead when its source is replicated or floored, when the table is kept for good, when it declares a landing TTL above 0, or on an engine that can't attach a host.

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
      - provisa-code-rates-spill:/var/lib/provisa-spill
volumes:
  provisa-code-rates-state: {}
  # Size this volume to codesource.duckdb_spill_cap_mb with the platform's quota
  # (Kubernetes ephemeral-storage or an emptyDir sizeLimit, an xfs project quota, or a
  # volume driver's size option). Without a quota the disk the image spills to is not
  # bounded.
  provisa-code-rates-spill: {}
```

On its first start the container prints a line beginning `pairing code:` in its log.

**Its memory and its disk.** The container answers each statement with an embedded DuckDB, bounded by three settings: `codesource.duckdb_memory_mb`, `codesource.duckdb_threads` and `codesource.duckdb_spill_cap_mb`. Provisa gives them to the container when it pairs and with every call, and the container keeps the last ones on its state volume, so it's bounded again after a restart. A statement that needs more memory spills to the spill volume. The cap holds only when you size that volume to it with your platform's quota. A statement that fills the volume is refused, and the refusal names `codesource.duckdb_spill_cap_mb`. A source in dev mode runs its statements in Provisa's own DuckDB, under the same memory and threads, and spills into the data directory with no bound.

**Connect it.**

- On the form, enter the host Provisa reaches the container at, the port you published, and the pairing code. On a shared Docker network or in Kubernetes, enter the container or Service name and 7443, with nothing published.
- Provisa pairs with the container over mutual TLS. The code is used once; from then on each side accepts only the other.
- Provisa checks that the container runs the source's code and offers its exports, then switches the source to built mode.
- Every Provisa process keeps its own 8 connections to the host, one for each call it runs at once, and dials again if the host stops answering. While it doesn't answer, reads of the source's tables fail and name the host.

**If the code changes.**

- A container that comes back running other code is refused until a steward reviews the code and re-pins it from the form.
- To change the code itself, switch the source back to dev, save the new code, and build again.

### Provisa's services {: #provisas-services }

Your code can import one module that Provisa always provides on the server: `provisa_services` in Python and Go, `@provisa/services` in Node.js, Bun and Deno. Nothing needs installing, and it isn't on any package registry. Outside Provisa it doesn't exist, so your tests mock it.

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

```go
import ps "provisa_services"

func Rates(ctx context.Context, in RatesInputs) ([]Rate, error) {
	region, err := ps.Settings.Get(ctx, "region")
	if err != nil {
		return nil, err
	}
	cached, err := ps.Cache.Get(ctx, "rates")
	// ...
	err = ps.Cache.Set(ctx, "rates", rates, 3600)
	// ...
}
```

In Go the calls have the same names, capitalized (`ps.Cache.Set`, `ps.HTTP.Get`, `ps.LLM.BaseURL`, `ps.DQ.Dataset`, `ps.Log`, `ps.Audit`), and each returns an `error` last. Every call but `ps.LLM.BaseURL` takes a `context.Context` first: give your function a leading `context.Context` and pass that one along, because it is what ties the call to the request your code is serving. Your `go.mod` doesn't need to name the module. To compile outside Provisa, point it at your own mock with a `replace provisa_services => ./mock`; on the server Provisa's module takes its place.

| Call | What it does |
|---|---|
| `secrets.get(name)` | One of the source's declared secrets, read from the vault now. See [Secrets](#secrets). |
| `settings.get(name, default)`, `settings.all()` | The source's settings, from **Settings** on its form, which a steward can change without changing the code. A missing one with no default raises. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | The source's own cache, shared by every worker of it. A value is anything JSON can hold, up to 1 MB, kept for `ttl` seconds or until replaced. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | The source's own vector collection, for its own reference data. An item has an `id` and a `text` or a `vector`; a text is embedded by one of your organization's vector models. It can't search your governed tables, whose data reaches code only as parameters. |
| `http.get(url)`, `http.post(url, ...)` | A fetch Provisa makes for the code, through the deployment's outbound proxy and certificate authorities. The host must be on the egress allow-list, and each fetch is recorded in the audit trail. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` in JavaScript) | Provisa's LLM service. See [Calling a model](#calling-a-model). |
| `dq.dataset(name)`, `dq.product(id)` | The data quality of a dataset (`domain.table` or its table id) or a data product, as the meta domain shows it to the role of the request your code is serving. For a dataset: the checker tables scanning it, each with its description, contract, assigned product and latest run (its outcome, time and failing checks), and its profiles with each one's latest run. For a product: its promises, expectations, partner checks and orphans. |
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
- A `@table` or `@stream` function whose parameters aren't `(inputs, context)`, and a `@loader` declaration it can't take.
- A write to a code table with no command bound for it, an update or a delete of one with no primary key, and two commands that fit one operation with no tag between them.
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

## Language reference {: #language-reference }

Each language's reference lists every annotation, decorator, macro and type its resolvers can use, with an example.

### Python reference {: #python-reference }

A Python resolver is a module whose functions carry docstring tags. Import nothing of Provisa's unless you want the read context, `TableRef` or the optional SDK.

#### Tags

Write each tag on its own line inside the function's docstring, or use the `provisa_sdk` decorator, which appends the same line to the docstring for you.

**`@table`**

The function returns a table's rows as any iterable: a list, a generator or an async iterable. Columns come from the return type's fields.

=== "Docstring"

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
        return [Region(code="emea", name="Europe, Middle East and Africa")]
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import table


    @dataclass
    class Region:
        code: str
        name: str


    @table
    def regions() -> list[Region]:
        """Sales regions."""
        return [Region(code="emea", name="Europe, Middle East and Africa")]
    ```

**`@loader` / `@loader N`**

Tags a `@table` function as a batch loader. Provisa calls it once per batch of join keys and passes the keys in the read context. With `@loader N`, each batch is at most `N` keys.

=== "Docstring"

    ```python
    from dataclasses import dataclass

    from provisa_services import ReadContext


    @dataclass
    class ScoreInputs:
        customer_id: str | None = None


    @dataclass
    class Score:
        customer_id: str
        score: float


    def churn_score(inputs: ScoreInputs, context: ReadContext) -> list[Score] | dict[str, list[Score]]:
        """@table
        @loader
        """
        ...
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import table
    from provisa_services import ReadContext


    @dataclass
    class ScoreInputs:
        customer_id: str | None = None


    @dataclass
    class Score:
        customer_id: str
        score: float


    @table(loader=True)   # or loader=50 for a batch of fifty
    def churn_score(inputs: ScoreInputs, context: ReadContext) -> list[Score] | dict[str, list[Score]]:
        ...
    ```

**`@stream`**

The function is a generator. Each row is delivered as it is yielded, on surfaces that serve subscriptions.

=== "Docstring"

    ```python
    from collections.abc import Iterator
    from dataclasses import dataclass


    @dataclass
    class AuditInputs:
        actor: str | None = None


    @dataclass
    class AuditEvent:
        actor: str
        action: str


    def audit_events(inputs: AuditInputs) -> Iterator[AuditEvent]:
        """@stream"""
        for event in tail_log():
            yield event
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import Stream, stream


    @dataclass
    class AuditInputs:
        actor: str | None = None


    @dataclass
    class AuditEvent:
        actor: str
        action: str


    @stream
    def audit_events(inputs: AuditInputs) -> Stream[AuditEvent]:
        for event in tail_log():
            yield event
    ```

**`@command`**

The function runs as a mutation.

=== "Docstring"

    ```python
    def cancel_order(order_id: int) -> bool:
        """@command"""
        return api.cancel(order_id)
    ```

=== "SDK"

    ```python
    from provisa_sdk import command


    @command
    def cancel_order(order_id: int) -> bool:
        return api.cancel(order_id)
    ```

**`@insert TABLE` / `@update TABLE` / `@delete TABLE`**

Binds a command to one kind of SQL write on `TABLE`. Use when two commands fit the same write signature and Provisa needs to know which is which.

=== "Docstring"

    ```python
    from dataclasses import dataclass


    @dataclass
    class Order:
        order_id: int
        amount: float


    def add_orders(rows: list[Order]) -> int:
        """@command
        @insert orders
        """
        ...
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import command


    @dataclass
    class Order:
        order_id: int
        amount: float


    @command(insert="orders")
    def add_orders(rows: list[Order]) -> int:
        ...
    ```

**`@query`**

A command whose kind is `query`. Used for code resolvers and for commands that return rows.

=== "Docstring"

    ```python
    from dataclasses import dataclass

    from provisa_services import TableRef


    @dataclass
    class Store:
        store_id: str
        country: str


    @dataclass
    class Region:
        region_id: str
        country: str


    @dataclass
    class Match:
        store_id: str
        region_id: str


    def stores_in_regions(stores: TableRef[Store], regions: TableRef[Region]) -> list[Match]:
        """@query"""
        ...
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import query
    from provisa_services import TableRef


    @dataclass
    class Store:
        store_id: str
        country: str


    @dataclass
    class Region:
        region_id: str
        country: str


    @dataclass
    class Match:
        store_id: str
        region_id: str


    @query
    def stores_in_regions(stores: TableRef[Store], regions: TableRef[Region]) -> list[Match]:
        ...
    ```

**`@columns`**

A batch column transform. Each parameter is one input column—a list of values in row order—and the function returns one result object per input row.

=== "Docstring"

    ```python
    from dataclasses import dataclass


    @dataclass
    class Address:
        street: str
        city: str


    def parse_address(raw: list[str]) -> list[Address]:
        """@columns"""
        ...
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import Batch, Results, columns


    @dataclass
    class Address:
        street: str
        city: str


    @columns
    def parse_address(raw: Batch[str]) -> Results[Address]:
        ...
    ```

**`@change table=T key=COLS removed=COL cursor=COL`**

Tags the function that keeps a code resolver's landed table fresh incrementally. Each argument goes on its own tag line.

=== "Docstring"

    ```python
    from dataclasses import dataclass

    from provisa_services import TableRef


    @dataclass
    class Store:
        store_id: str
        country: str


    @dataclass
    class Region:
        region_id: str
        country: str


    @dataclass
    class Changed:
        store_id: str
        region_id: str
        removed: bool
        seq: int


    def store_region_changes(
        stores: TableRef[Store], regions: TableRef[Region], since: int | None = None
    ) -> list[Changed]:
        """Store-region matches changed since ``since``.

        @change table=store_regions
        @change key=store_id,region_id
        @change removed=removed
        @change cursor=seq
        """
        ...
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import change
    from provisa_services import TableRef


    @dataclass
    class Store:
        store_id: str
        country: str


    @dataclass
    class Region:
        region_id: str
        country: str


    @dataclass
    class Changed:
        store_id: str
        region_id: str
        removed: bool
        seq: int


    @change(table="store_regions", key=["store_id", "region_id"], removed="removed", cursor="seq")
    def store_region_changes(
        stores: TableRef[Store], regions: TableRef[Region], since: int | None = None
    ) -> list[Changed]:
        """Store-region matches changed since ``since``."""
        ...
    ```

**`@on-start`**

Called once when the code starts. Takes nothing, or a mapping of the source's setup values.

=== "Docstring"

    ```python
    def setup(values: dict) -> None:
        """@on-start"""
        ...
    ```

=== "SDK"

    ```python
    from provisa_sdk import on_start


    @on_start
    def setup(values: dict) -> None:
        ...
    ```

**`@on-stop`**

Called once when the code stops. Takes nothing.

=== "Docstring"

    ```python
    def teardown() -> None:
        """@on-stop"""
        ...
    ```

=== "SDK"

    ```python
    from provisa_sdk import on_stop


    @on_stop
    def teardown() -> None:
        ...
    ```

**`@health`**

Returns the code's status as text. Called periodically to check the worker.

=== "Docstring"

    ```python
    def status() -> str:
        """@health"""
        return "ok"
    ```

=== "SDK"

    ```python
    from provisa_sdk import health


    @health
    def status() -> str:
        return "ok"
    ```

**`@requires NAME` / `@requires NAME:secret`**

Declares a setup value the code needs. Whoever adds the source fills it in. Stack one per value. With `:secret`, the value is treated as a credential and never shown in the UI.

=== "Docstring"

    ```python
    from dataclasses import dataclass


    @dataclass
    class Inputs:
        account: str


    @dataclass
    class Row:
        account: str
        balance: float


    def fetch(inputs: Inputs) -> list[Row]:
        """@table
        @requires REGION
        @requires API_KEY:secret
        """
        ...
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import requires, table


    @dataclass
    class Inputs:
        account: str


    @dataclass
    class Row:
        account: str
        balance: float


    @requires("API_KEY", secret=True)
    @requires("REGION")
    @table
    def fetch(inputs: Inputs) -> list[Row]:
        ...
    ```

**`@discover`**

A single function whose return value—a `provisa_services.Discovered`—generates tables, queries and commands at read time. The generated items share one handler function per kind.

The SDK has no `@discover` decorator: write it in the docstring.

```python
from provisa_services import Discovered


def discover_feeds() -> Discovered:
    """@discover"""
    ...
```

#### Parameter types

| Parameter | Type | Notes |
|---|---|---|
| Inputs | Dataclass, `TypedDict`, `NamedTuple`, Pydantic model, or any class with annotated fields | Each field is a table parameter. A field without a default is required. |
| Read context | `provisa_services.ReadContext` | Carries `filters`, `order_by`, `limit`, `columns` and `keys`. |
| Table argument | `provisa_services.TableRef[Row]` | For `@command` and `@query` only. The row type's fields declare the input columns. |

#### Column types

| Python type | Column type |
|---|---|
| `str` | text |
| `int` | bigint |
| `float` | double |
| `decimal.Decimal` | numeric |
| `datetime.datetime` | timestamp |
| `datetime.date` | date |
| `bool` | boolean |
| `Any` (single field only) | json |

#### Result type aliases

These are from `provisa_sdk`; each is an alias of its standard Python equivalent.

| Alias | Expands to | Use |
|---|---|---|
| `Rows[R]` | `Iterable[R]` | `@table` return type |
| `AsyncRows[R]` | `AsyncIterable[R]` | `@table` async return type |
| `Stream[R]` | `Iterator[R]` | `@stream` return type |
| `AsyncStream[R]` | `AsyncIterator[R]` | `@stream` async return type |
| `Batch[T]` | `list[T]` | `@columns` parameter type |
| `Results[R]` | `list[R]` | `@columns` return type |

---

### Node reference {: #node-reference }

A Node resolver is a TypeScript bundle. Tags go in JSDoc comments; types come from TypeScript. The optional `@provisa/sdk` package wraps each function with a branded type, so your type checker can hold it to its tag's rules before Provisa reads the code.

Node reads every tag Python does except `@discover`, which is read from a Python code source only.

#### Tags

**`@table`**

=== "JSDoc"

    ```typescript
    /** Sales regions.
     * @table
     */
    export async function regions(): Promise<Region[]> {
      return [{ code: "emea", name: "Europe, Middle East and Africa" }];
    }
    ```

=== "SDK"

    ```typescript
    import { table } from "@provisa/sdk";

    export const regions = table(async function regions(): Promise<Region[]> {
      return [{ code: "emea", name: "Europe, Middle East and Africa" }];
    });
    ```

**`@loader` / `@loader N`**

=== "JSDoc"

    ```typescript
    import type { ReadContext } from "@provisa/services";

    /** @table
     * @loader
     */
    export async function churnScore(
      inputs: ScoreInputs,
      context: ReadContext,
    ): Promise<Score[] | Record<string, Score[]>> {
      ...
    }
    ```

=== "SDK"

    ```typescript
    import { table } from "@provisa/sdk";

    export const churnScore = table(churnScoreFn, { loader: true }); // or { loader: 50 }
    ```

**`@stream`**

The function is an async generator.

=== "JSDoc"

    ```typescript
    /** @stream */
    export async function* auditEvents(inputs: AuditInputs): AsyncIterable<AuditEvent> {
      for await (const event of tailLog()) {
        yield event;
      }
    }
    ```

=== "SDK"

    ```typescript
    import { stream } from "@provisa/sdk";

    export const auditEvents = stream(async function* auditEvents(inputs: AuditInputs) { ... });
    ```

**`@command`**

=== "JSDoc"

    ```typescript
    /** @command */
    export async function cancelOrder(orderId: number): Promise<boolean> {
      return api.cancel(orderId);
    }
    ```

=== "SDK"

    ```typescript
    import { command } from "@provisa/sdk";

    export const cancelOrder = command(async function cancelOrder(orderId: number): Promise<boolean> {
      ...
    });
    ```

**`@insert TABLE` / `@update TABLE` / `@delete TABLE`**

=== "JSDoc"

    ```typescript
    /** @command
     * @insert orders
     */
    export async function addOrders(rows: Order[]): Promise<number> { ... }
    ```

=== "SDK"

    ```typescript
    import { command } from "@provisa/sdk";

    export const addOrders = command(addOrdersFn, { insert: "orders" });
    ```

**`@query`**

=== "JSDoc"

    ```typescript
    import type { TableRef } from "@provisa/services";

    /** @query */
    export async function storesInRegions(
      stores: TableRef<Store>,
      regions: TableRef<Region>,
    ): Promise<Match[]> { ... }
    ```

=== "SDK"

    ```typescript
    import { query } from "@provisa/sdk";

    export const storesInRegions = query(storesInRegionsFn);
    ```

**`@columns`**

=== "JSDoc"

    ```typescript
    /** @columns */
    export async function parseAddress(raw: string[]): Promise<Address[]> { ... }
    ```

=== "SDK"

    ```typescript
    import { columns } from "@provisa/sdk";

    export const parseAddress = columns(async function parseAddress(raw: string[]): Promise<Address[]> {
      ...
    });
    ```

**`@change table=T key=COLS removed=COL cursor=COL`**

Write each line in the JSDoc comment. No SDK wrapper.

```typescript
/** Store-region matches changed since `since`.
 * @change table=store_regions
 * @change key=store_id,region_id
 * @change removed=removed
 * @change cursor=seq
 */
export async function storeRegionChanges(
  stores: TableRef<Store>,
  regions: TableRef<Region>,
  since?: number,
): Promise<Changed[]> { ... }
```

**`@on-start`**

=== "JSDoc"

    ```typescript
    /** @on-start */
    export async function setup(values: Record<string, string>): Promise<void> { ... }
    ```

=== "SDK"

    ```typescript
    import { onStart } from "@provisa/sdk";

    export const setup = onStart(async function setup(values: Record<string, string>) { ... });
    ```

**`@on-stop`**

=== "JSDoc"

    ```typescript
    /** @on-stop */
    export async function teardown(): Promise<void> { ... }
    ```

=== "SDK"

    ```typescript
    import { onStop } from "@provisa/sdk";

    export const teardown = onStop(async function teardown() { ... });
    ```

**`@health`**

=== "JSDoc"

    ```typescript
    /** @health */
    export function status(): string {
      return "ok";
    }
    ```

=== "SDK"

    ```typescript
    import { health } from "@provisa/sdk";

    export const status = health(function status(): string { return "ok"; });
    ```

**`@requires NAME` / `@requires NAME:secret`**

=== "JSDoc"

    ```typescript
    /** @table
     * @requires REGION
     * @requires API_KEY:secret
     */
    export async function fetch(inputs: Inputs): Promise<Row[]> { ... }
    ```

=== "SDK"

    ```typescript
    import { table } from "@provisa/sdk";

    export const fetch = table(fetchFn, { requires: ["REGION", "API_KEY:secret"] });
    ```

#### Parameter types

| Parameter | Type | Notes |
|---|---|---|
| Inputs | Any TypeScript interface or type alias | Each property is a table parameter. An optional property (`?`) or one with a default may be left out. |
| Read context | `ReadContext` from `@provisa/services` | Carries `filters`, `order_by`, `limit`, `columns` and `keys`. |
| Table argument | `TableRef<Row>` from `@provisa/services` | For `@command` and `@query` only. |

#### Column type aliases

These are from `@provisa/sdk`; each is an alias of its TypeScript equivalent.

| Alias | TypeScript type | Column type |
|---|---|---|
| `Text` | `string` | text |
| `Double` | `number` | double |
| `BigInt` | `bigint` | bigint |
| `Boolean` | `boolean` | boolean |
| `Timestamp` | `Date` | timestamp |
| `Json` | `unknown` | json |

#### Result type aliases

| Alias | Expands to | Use |
|---|---|---|
| `Rows<R>` | `Iterable<R>` | `@table` return type |
| `AsyncRows<R>` | `AsyncIterable<R>` | `@table` async return type |
| `Batch<T>` | `T[]` | `@columns` parameter type |

---

### Go reference {: #go-reference }

A Go resolver is a package. Tags go in the doc comment; types come from the function's signature. Exported top-level functions are offered; methods and unexported functions are not. The optional `provisa.dev/sdk/go` module registers functions with their tags so the compiler can check signatures at build time.

Go does not offer `@loader` or the read context.

#### Tags

**`@table`**

A `@table` function takes an optional `context.Context` first, then its inputs struct; it returns `[]T`, `([]T, error)`, `iter.Seq[T]` or `iter.Seq2[T, error]`.

=== "Doc comment"

    ```go
    // ChurnRisk is the churn risk of one account.
    //
    // @table
    func ChurnRisk(ctx context.Context, in RiskInputs) ([]Risk, error) {
        return []Risk{{Score: 0.82}}, nil
    }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.Table[RiskInputs, Risk](ChurnRisk)
    ```

**`@stream`**

A `@stream` function returns `iter.Seq2[T, error]`. An item yielded with a non-nil error ends the stream.

=== "Doc comment"

    ```go
    // AuditEvents streams audit events.
    //
    // @stream
    func AuditEvents(ctx context.Context, in AuditInputs) iter.Seq2[AuditEvent, error] {
        return func(yield func(AuditEvent, error) bool) { ... }
    }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.Stream[AuditInputs, AuditEvent](AuditEvents)
    ```

**`@command`**

=== "Doc comment"

    ```go
    // CancelOrder cancels one order.
    //
    // @command
    func CancelOrder(orderID int) (bool, error) {
        return true, nil
    }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.Command(CancelOrder)
    ```

**`@insert TABLE` / `@update TABLE` / `@delete TABLE`**

=== "Doc comment"

    ```go
    // AddOrders inserts orders.
    //
    // @command
    // @insert orders
    func AddOrders(rows []Order) (int, error) { ... }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.Command(AddOrders, provisa.Insert("orders"))
    ```

**`@query`**

=== "Doc comment"

    ```go
    // StoresInRegions matches stores to regions.
    //
    // @query
    func StoresInRegions(stores TableRef[Store], regions TableRef[Region]) ([]Match, error) { ... }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.Query(StoresInRegions)
    ```

**`@columns`**

=== "Doc comment"

    ```go
    // ParseAddress parses raw addresses.
    //
    // @columns
    func ParseAddress(raw []string) ([]Address, error) { ... }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.Columns(ParseAddress)
    ```

**`@change table=T key=COLS removed=COL cursor=COL`**

=== "Doc comment"

    ```go
    // StoreRegionChanges returns matches changed since the cursor.
    //
    // @change table=store_regions
    // @change key=store_id,region_id
    // @change removed=removed
    // @change cursor=seq
    func StoreRegionChanges(stores TableRef[Store], regions TableRef[Region], since *int) ([]Changed, error) { ... }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.Change(StoreRegionChanges, "store_regions", []string{"store_id", "region_id"}, "removed", "seq")
    ```

**`@on-start`**

=== "Doc comment"

    ```go
    // Setup runs once on start.
    //
    // @on-start
    func Setup(ctx context.Context, values map[string]string) error { ... }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.OnStart(Setup)
    ```

**`@on-stop`**

=== "Doc comment"

    ```go
    // Teardown runs once on stop.
    //
    // @on-stop
    func Teardown(ctx context.Context) error { ... }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.OnStop(Teardown)
    ```

**`@health`**

=== "Doc comment"

    ```go
    // Status returns the code's health.
    //
    // @health
    func Status(ctx context.Context) (string, error) {
        return "ok", nil
    }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.Health(Status)
    ```

**`@requires NAME` / `@requires NAME:secret`**

=== "Doc comment"

    ```go
    // Fetch fetches rows.
    //
    // @table
    // @requires REGION
    // @requires API_KEY:secret
    func Fetch(ctx context.Context, in Inputs) ([]Row, error) { ... }
    ```

=== "SDK"

    ```go
    import provisa "provisa.dev/sdk/go"

    var _ = provisa.Table[Inputs, Row](Fetch, provisa.Requires("REGION"), provisa.Requires("API_KEY", provisa.Secret))
    ```

#### Field naming

Fields are named as `encoding/json` names them: the `json` struct tag, or the Go field name when there is none. A `string` type with named constants is an enum of those values. `time.Time` is a timestamp.

#### Column type aliases

These are from `provisa.dev/sdk/go`.

| Alias | Go type | Column type |
|---|---|---|
| `Text` | `string` | text |
| `BigInt` | `int64` | bigint |
| `Double` | `float64` | double |
| `Boolean` | `bool` | boolean |
| `Timestamp` | `time.Time` | timestamp |
| `Json` | `any` | json |

#### Special types

`provisa.NoInputs` stands for `I` when a `Table` or `Stream` function takes no inputs struct. `provisa.TableRef[Row]` is the type for a governed table argument; a doc-comment source declares its own generic `TableRef` type instead, which Provisa also knows by name.

---

### Rust reference {: #rust-reference }

A Rust resolver is a library crate. Tags go in `///` doc comments; types come from the function's signature. Only `pub fn` items at the crate's root are offered. The optional `provisa-sdk` crate registers functions with their tags as compile-time constants.

Rust does not offer `@loader` or the read context. Rust functions are neither `async` nor generic.

#### Tags

**`@table`**

A `@table` function takes nothing or one inputs struct. It returns `Vec<T>`, `impl Iterator<Item = T>`, or either inside `Result<..., E>`.

=== "Doc comment"

    ```rust
    /// The churn risk of one account.
    ///
    /// @table
    pub fn churn_risk(inputs: RiskInputs) -> Result<Vec<Risk>, String> {
        Ok(vec![Risk { score: 0.82, reasons: vec!["no logins".into()] }])
    }
    ```

=== "SDK"

    ```rust
    provisa_sdk::table!(churn_risk);
    ```

**`@stream`**

A `@stream` function returns an iterator. An item of `Result<T, E>` ends the stream on error.

=== "Doc comment"

    ```rust
    /// Audit events.
    ///
    /// @stream
    pub fn audit_events(inputs: AuditInputs) -> impl Iterator<Item = AuditEvent> {
        tail_log()
    }
    ```

=== "SDK"

    ```rust
    provisa_sdk::stream!(audit_events);
    ```

**`@command`**

=== "Doc comment"

    ```rust
    /// Cancel one order.
    ///
    /// @command
    pub fn cancel_order(order_id: i64) -> Result<bool, String> {
        Ok(true)
    }
    ```

=== "SDK"

    ```rust
    provisa_sdk::command!(cancel_order);
    ```

**`@insert TABLE` / `@update TABLE` / `@delete TABLE`**

=== "Doc comment"

    ```rust
    /// Insert orders.
    ///
    /// @command
    /// @insert orders
    pub fn add_orders(rows: Vec<Order>) -> Result<i64, String> { ... }
    ```

=== "SDK"

    ```rust
    provisa_sdk::command!(add_orders, insert = "orders");
    ```

**`@query`**

=== "Doc comment"

    ```rust
    /// Match stores to regions.
    ///
    /// @query
    pub fn stores_in_regions(stores: TableRef<Store>, regions: TableRef<Region>) -> Result<Vec<Match>, String> { ... }
    ```

=== "SDK"

    ```rust
    provisa_sdk::query!(stores_in_regions);
    ```

**`@columns`**

=== "Doc comment"

    ```rust
    /// Parse raw addresses.
    ///
    /// @columns
    pub fn parse_address(raw: Vec<String>) -> Result<Vec<Address>, String> { ... }
    ```

=== "SDK"

    ```rust
    provisa_sdk::columns!(parse_address);
    ```

**`@change table=T key=COLS removed=COL cursor=COL`**

=== "Doc comment"

    ```rust
    /// Matches changed since the cursor.
    ///
    /// @change table=store_regions
    /// @change key=store_id,region_id
    /// @change removed=removed
    /// @change cursor=seq
    pub fn store_region_changes(stores: TableRef<Store>, regions: TableRef<Region>, since: Option<i64>) -> Result<Vec<Changed>, String> { ... }
    ```

=== "SDK"

    ```rust
    provisa_sdk::change!(
        store_region_changes,
        table = "store_regions",
        key = ["store_id", "region_id"],
        removed = "removed",
        cursor = "seq",
    );
    ```

**`@on-start`**

=== "Doc comment"

    ```rust
    /// Called once on start.
    ///
    /// @on-start
    pub fn setup(values: HashMap<String, String>) -> Result<(), String> { ... }
    ```

=== "SDK"

    ```rust
    provisa_sdk::on_start!(setup);
    ```

**`@on-stop`**

=== "Doc comment"

    ```rust
    /// Called once on stop.
    ///
    /// @on-stop
    pub fn teardown() { ... }
    ```

=== "SDK"

    ```rust
    provisa_sdk::on_stop!(teardown);
    ```

**`@health`**

=== "Doc comment"

    ```rust
    /// Returns the code's status.
    ///
    /// @health
    pub fn status() -> &'static str {
        "ok"
    }
    ```

=== "SDK"

    ```rust
    provisa_sdk::health!(status);
    ```

**`@requires NAME` / `@requires NAME:secret`**

=== "Doc comment"

    ```rust
    /// Fetch rows.
    ///
    /// @table
    /// @requires REGION
    /// @requires API_KEY:secret
    pub fn fetch(inputs: Inputs) -> Result<Vec<Row>, String> { ... }
    ```

=== "SDK"

    ```rust
    provisa_sdk::table!(fetch, requires = "REGION", secret = "API_KEY");
    ```

#### Field naming

Fields are named as serde names them: `rename`, `rename_all`, `skip`, `flatten` and `default` are all read. An enum of unit variants becomes an enum of its variant names (or their `rename` values).

#### Column type aliases

These are from `provisa-sdk`.

| Alias | Rust type | Column type |
|---|---|---|
| `Text` | `String` | text |
| `BigInt` | `i64` | bigint |
| `Double` | `f64` | double |
| `Boolean` | `bool` | boolean |

For timestamp, date, numeric and json columns: use `chrono::DateTime`, `chrono::NaiveDate`, `rust_decimal::Decimal` and `serde_json::Value` directly.

#### Result and stream type aliases

| Alias | Expands to | Use |
|---|---|---|
| `Rows<T>` | `Vec<T>` | `@table` return type |
| `Stream<T>` | `Box<dyn Iterator<Item = T>>` | `@stream` return type |
| `Batch<T>` | `Vec<T>` | `@columns` parameter type |
| `Results<T>` | `Vec<T>` | `@columns` return type |

The SDK has no `TableRef` type. Declare your own generic `TableRef` struct; Provisa knows it by name.

---

### Spark reference {: #spark-reference }

A Spark script is a Python file. Tags go in docstrings, exactly as for any Python resolver. The script imports `provisa_spark` to reach its Spark cluster, and `provisa_services` for governed table arguments and the read context.

The script does not carry its own setup: Provisa reads the cluster URL and the script text from the Spark source's setup form.

#### Tags

All Python docstring tags are available. The ones most relevant to Spark work are:

**`@table`**

=== "Docstring"

    ```python
    from dataclasses import dataclass
    from provisa_spark import session


    @dataclass
    class SalesRow:
        region: str
        amount: float


    def sales() -> list[SalesRow]:
        """Daily sales by region.

        @table
        """
        df = session().sql("SELECT region, SUM(amount) AS amount FROM delta.sales GROUP BY region")
        return [SalesRow(r.region, r.amount) for r in df.collect()]
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass
    from provisa_sdk import table
    from provisa_spark import session


    @dataclass
    class SalesRow:
        region: str
        amount: float


    @table
    def sales() -> list[SalesRow]:
        """Daily sales by region."""
        df = session().sql("SELECT region, SUM(amount) AS amount FROM delta.sales GROUP BY region")
        return [SalesRow(r.region, r.amount) for r in df.collect()]
    ```

**`@command`**

=== "Docstring"

    ```python
    from provisa_spark import session


    def archive_orders(cutoff: str) -> int:
        """Archive orders older than cutoff.

        @command
        """
        df = session().sql(f"DELETE FROM delta.orders WHERE created < '{cutoff}'")
        return df.first()["num_affected_rows"]
    ```

=== "SDK"

    ```python
    from provisa_sdk import command
    from provisa_spark import session


    @command
    def archive_orders(cutoff: str) -> int:
        """Archive orders older than cutoff."""
        df = session().sql(f"DELETE FROM delta.orders WHERE created < '{cutoff}'")
        return df.first()["num_affected_rows"]
    ```

**`@query`**

A `@query` function can take governed Provisa tables as `TableRef` parameters, reading them through Arrow Flight SQL.

=== "Docstring"

    ```python
    from dataclasses import dataclass
    from provisa_services import TableRef
    from provisa_spark import dataframe


    @dataclass
    class Order:
        region: str
        amount: float


    @dataclass
    class Total:
        region: str
        amount: float


    def regional_totals(orders: TableRef[Order]) -> list[Total]:
        """Totals of governed orders by region.

        @query
        """
        frame = dataframe(orders).groupBy("region").sum("amount")
        return [Total(r[0], r[1]) for r in frame.collect()]
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass
    from provisa_sdk import query
    from provisa_services import TableRef
    from provisa_spark import dataframe


    @dataclass
    class Order:
        region: str
        amount: float


    @dataclass
    class Total:
        region: str
        amount: float


    @query
    def regional_totals(orders: TableRef[Order]) -> list[Total]:
        """Totals of governed orders by region."""
        frame = dataframe(orders).groupBy("region").sum("amount")
        return [Total(r[0], r[1]) for r in frame.collect()]
    ```

**`@on-start`** and **`@on-stop`**

=== "Docstring"

    ```python
    from provisa_spark import session


    def warmup(values: dict) -> None:
        """@on-start"""
        session()  # start the Spark Connect session once


    def shutdown() -> None:
        """@on-stop"""
        session().stop()
    ```

=== "SDK"

    ```python
    from provisa_sdk import on_start, on_stop
    from provisa_spark import session


    @on_start
    def warmup(values: dict) -> None:
        session()  # start the Spark Connect session once


    @on_stop
    def shutdown() -> None:
        session().stop()
    ```

**`@requires NAME` / `@requires NAME:secret`**

Declares a setup value beyond the built-in `script` and `cluster`.

=== "Docstring"

    ```python
    from dataclasses import dataclass


    @dataclass
    class Inputs:
        day: str


    @dataclass
    class Row:
        day: str
        amount: float


    def fetch(inputs: Inputs) -> list[Row]:
        """@table
        @requires DELTA_TABLE_PATH
        """
        ...
    ```

=== "SDK"

    ```python
    from dataclasses import dataclass

    from provisa_sdk import requires, table


    @dataclass
    class Inputs:
        day: str


    @dataclass
    class Row:
        day: str
        amount: float


    @requires("DELTA_TABLE_PATH")
    @table
    def fetch(inputs: Inputs) -> list[Row]:
        ...
    ```

#### Special imports

| Import | What it gives you |
|---|---|
| `from provisa_spark import session` | A `SparkSession` connected to the configured cluster, created once and shared. |
| `from provisa_spark import dataframe` | A governed `TableRef` argument as a Spark `DataFrame`, read through Arrow Flight SQL as the calling role. |
| `from provisa_services import ReadContext` | The read context (filters, order, limit, columns, keys). |
| `from provisa_services import TableRef` | The type annotation for a governed table argument. |

#### Column types

The same Python type mappings apply as for any Python resolver. See the [Python reference](#python-reference) column types table.

