# Spark connector

The Spark connector runs one annotated Spark script against your cluster. The script's `@table` and `@command` functions are the source's tables and commands, so a new job needs no image build: you change the script.

Its image is an ordinary connector ([Code Resolvers](code-sources.md)). The operator runs it and enters its host, port and pairing code in Sources. Setup asks for two values:

| Value | What it is |
|---|---|
| `script` | The annotated Spark script, as multi-line text. |
| `cluster` | The Spark Connect address of the cluster, such as `sc://spark.example:15002`. |

At join, Provisa gives the host the script. The host reads the script's tags and answers with what it found. The image is proven by its digest and code hash, and the script by its own hash, recorded in the source's config as `script_hash`. A host whose script does not match that hash is refused by name, at join and on every later connection.

## Writing the script

Import `session()` from `provisa_spark`. It returns a Spark session on the configured cluster, so the script never names the cluster. Declare each table's row type: the table's columns come from it, as for any code source, so a table's shape changes only when the script does.

```python
from dataclasses import dataclass

from provisa_services import ReadContext
from provisa_spark import session


@dataclass
class Order:
    id: int
    amount: float


def orders(context: ReadContext) -> list[Order]:
    """Orders over the last day.

    @table
    """
    frame = session().sql("SELECT id, amount FROM sales.orders WHERE day = current_date()")
    if context.limit is not None:
        frame = frame.limit(context.limit)
    return [Order(**row.asDict()) for row in frame.collect()]


def rebuild_orders(day: str) -> str:
    """Rebuilds one day of orders.

    @command
    """
    session().sql(
        "INSERT OVERWRITE sales.orders SELECT * FROM staging.orders WHERE day = :day",
        args={"day": day},
    )
    return day
```

A `@table` is the frontier, like any connector's table: it brings data from outside, and lineage starts there. On a live read a function is given the statement's read context, a hint it may apply to its DataFrame; Provisa applies the filters, order and limit again. A `@command` with table inputs is covered in [Code Resolvers](code-sources.md).

## Small jobs, big jobs, replication and caching

Where a query's answer comes from decides what it costs and how fresh it is.

| Setup | What a query does | Performance | Timeliness |
|---|---|---|---|
| Replication on (the default) | Reads rows already landed in the replica. | The job's run time is paid when the replica refreshes, not by the query. Queries are fast whatever the job costs. | As of the last refresh. Use [Freshness](replicas.md#freshness) to say how old a read may be. |
| Replication off | Runs the script live, held to the request timeout. | The query waits for the job. Each query pays for a run on the cluster. | Current as of the run. |
| Replication off with a cache TTL | Runs the script once per TTL and answers from the held rows between runs. | The first read waits for the job; later reads are fast. | As of the last run, up to the TTL old. |

**Big jobs: keep replication on.** A job that runs for minutes or hours cannot answer within a request timeout, so a query must never have to run it. Replication builds and refreshes the table without waiting for a query, which is why it is the connector's default.

**Small jobs: replication off is an option.** When the script answers well within the request timeout and the reader wants the current answer, a steward can turn replication off on the source. Mind the cluster: every query is now a Spark run. If many readers ask the same thing, add a cache TTL or keep replication on.

### Transforms over governed tables

A `@query` that takes a governed table can transform it on the cluster. Read the argument with `dataframe()`. Its rows reach the cluster over Arrow Flight, read as the calling role, so the job sees only what that role may read.

```python
from provisa_services import TableRef
from provisa_spark import dataframe


def regional_totals(orders: TableRef[Order]) -> list[Total]:
    """@query"""
    frame = dataframe(orders).groupBy("region").sum("amount")
    return [Total(region=r[0], amount=r[1]) for r in frame.collect()]
```

You can call it live, `SELECT * FROM regional_totals('sales.orders')`, and the query waits for the job. For a transform that takes longer than a reader should wait, define a materialized view over the call:

```sql
SELECT t.region, t.amount FROM regional_totals('sales.orders') AS t
```

The view is landed like any other materialized view. A read answers from its stored rows and never starts a Spark job. The job runs when the view refreshes:

- on its refresh interval;
- when a table it is given changes, as that table's [change signal](replicas.md#freshness) reports it.

| Setup | What a query does | Performance | Timeliness |
|---|---|---|---|
| Live call | Runs the job on each query, held to the request timeout. | The query waits for the job. Each query pays for a run. | Current as of the run. |
| Materialized view over the call | Reads the rows the last refresh landed. | Fast, whatever the job costs. The cluster runs once per refresh. | As of the last refresh: its interval, or the last change to an input table. |

A role reading the view is held to the same rules as a live call. A role that cannot read a table the call is given is refused the view, naming that table, as it is refused the call. The job computes the rows once, from the whole table. A view that reads only the call's rows therefore refuses a role whose row rules narrow that table. A view that joins the call's rows back to the table is filtered by those rules instead.

## Streaming

The recommended pattern runs one Spark streaming job and serves every subscriber from Kafka:

- `@on-start` submits a streaming job that writes to a Kafka topic.
- `@on-stop` stops it.
- `@health` reports whether it is running, shown on the source in Sources.
- A Provisa Kafka source on that topic serves the subscriptions.

One Spark job then serves every subscriber, however many there are.

A `@stream` in the script also works: it runs one Spark streaming query per subscription, so size the cluster for the number of subscribers.
