# Spark source

A Spark source connects Provisa to a Spark cluster through Spark Connect. Provisa reads the cluster's catalog — its databases, tables and views with their column types and storage format — and you register tables from it the same way you register tables from any database source. No script is needed.

Add Spark in Sources the same way you add any other source: select Spark from the Code group, enter the cluster address, and save. The setup form takes one value:

| Value | What it is |
|---|---|
| `cluster` | The Spark Connect address, as structured fields (host, port, TLS, optional user id). Paste an `sc://` address and Provisa fills the fields. |

After you save, Provisa connects to the cluster and reads its catalog. Register Table then lists the databases and their tables and views for you to register.

## Registering tables

Register Table reads the cluster's catalog through Spark Connect. Each table and view appears with its column types. Select the ones you want and register them; they then participate in every Provisa protocol and join against tables from other sources.

With Provisa's predicates and limit pushed to the cluster, results are fetched as Arrow.

**Mutations.** A table's storage format determines what writes are served:

| Format | Supported mutations |
|---|---|
| Delta, Iceberg, Hudi | `INSERT`, `UPDATE`, `DELETE`, `MERGE` |
| Parquet, Hive | `INSERT` only |
| Views | None (read only) |

A write the format cannot perform is refused before it reaches the cluster, naming the table and its format.

**No DDL.** Tables and views are created in Spark itself; Provisa admits them from the catalog. A session's own temporary tables live in Provisa's serving engine and never reach the cluster.

## Python code sources over Spark

Logic the cluster does not hold belongs in an ordinary Python code source. Call `session()` from `provisa_spark` with the name of your Spark source, and it returns a Spark Connect session on that cluster.

```python
from dataclasses import dataclass

from provisa_services import ReadContext, TableRef
from provisa_spark import dataframe, session


@dataclass
class Order:
    id: int
    amount: float


def recent_orders(context: ReadContext) -> list[Order]:
    """Orders from the last day.

    @table
    """
    spark = session("my-spark")  # name of the Spark source
    df = spark.sql("SELECT id, amount FROM sales.orders WHERE day = current_date()")
    if context.limit is not None:
        df = df.limit(context.limit)
    return [Order(**row.asDict()) for row in df.collect()]
```

A `@table` function can also return the DataFrame directly, without a row class. Provisa applies the read context and converts through Arrow:

```python
from provisa_services import ReadContext
from provisa_spark import session


def recent_orders(context: ReadContext):
    """Orders from the last day.

    @table
    """
    return session("my-spark").sql(
        "SELECT id, amount FROM sales.orders WHERE day = current_date()"
    )
```

**Governed table inputs.** A `@query` function can take a registered Provisa table as a `TableRef` argument. `dataframe()` reads it over Arrow Flight as the calling role, so the job sees only what that role may read.

```python
from dataclasses import dataclass
from provisa_services import TableRef
from provisa_spark import dataframe, session


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

A `@query` over governed tables can be landed as a materialized view: define the view over the call, and it is built on schedule or when an input table changes.

## Replication and caching

The same performance choices available on any code source apply to Python code sources over Spark.

| Setup | What a query does | Performance | Timeliness |
|---|---|---|---|
| Replication on (the default) | Reads rows already landed in the replica. | Query time is decoupled from job run time. | As of the last refresh. Use [Freshness](replicas.md#freshness) to bound staleness. |
| Replication off | Runs the function live, held to the request timeout. | The query waits for the job. | Current as of the run. |
| Replication off with a cache TTL | Runs the function once per TTL. | First read waits; later reads are fast. | As of the last run. |

For a job that runs for minutes, keep replication on. For a function that answers in well under the timeout and where the reader needs current results, replication off is an option. Mind the cluster: each live query starts a Spark run.

## Streaming

The recommended pattern runs one Spark streaming job and serves every subscriber from Kafka. `@on-start` submits a streaming job writing to a Kafka topic; `@on-stop` stops it; `@health` reports whether it is running. A Provisa Kafka source on that topic then serves the subscriptions, so one Spark job covers every subscriber.

A `@stream` function also works: it runs one Spark streaming query per subscription, so size the cluster for the number of subscribers.

## Default cluster

An organization-level default Spark cluster is set under **Admin → Spark**. It holds the cluster address and a credential kept as a secret reference. A Python code source that calls `session()` with no argument uses it. A source's own cluster, when set, overrides the default for that source.

The demo and local installs start a one-node Spark cluster on demand, labelled a local test cluster, as the default. It downloads a pinned Spark distribution at first use. No Docker is required. On Windows the demo names Spark unavailable; a cluster can still be set in the organization's Spark tab.
