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

## Streaming

The recommended pattern runs one Spark streaming job and serves every subscriber from Kafka:

- `@on-start` submits a streaming job that writes to a Kafka topic.
- `@on-stop` stops it.
- `@health` reports whether it is running, shown on the source in Sources.
- A Provisa Kafka source on that topic serves the subscriptions.

One Spark job then serves every subscriber, however many there are.

A `@stream` in the script also works: it runs one Spark streaming query per subscription, so size the cluster for the number of subscribers.
