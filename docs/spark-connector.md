# Spark connector

A Spark source is a connection to your Spark cluster through Spark Connect. It has no script. Provisa serves the cluster's own objects from its catalog. Logic the cluster does not hold is written as an ordinary [Python code source](code-sources.md#spark-reference) that opens the Spark source's session by name.

## Setting up a Spark source

Spark is a built-in source type, listed with the code sources in Sources. Setup asks for one value:

| Value | What it is |
|---|---|
| `cluster` | The Spark Connect address of the cluster, entered as its host, port, TLS switch and optional user id, with its token kept as a secret reference. **Paste address** splits a pasted `sc://` address into those fields. |

Left blank, the source uses the organization's default Spark cluster, which an administrator sets in the organization's Spark tab. The form names which cluster that is. Demo and local installs have a local test cluster as the default.

## Logic the cluster does not hold

Write it as a Python code source. Its functions open the Spark source's session with `provisa_services.spark.session("<spark source>")`, which connects to that source's cluster with its stored credential. A function given a governed table reads it on the cluster with `provisa_services.spark.dataframe(session, table)`: the rows reach the cluster over Arrow Flight, read as the calling role, so the job sees only what that role may read.

```python
from dataclasses import dataclass

import provisa_services as ps
from provisa_services import TableRef


@dataclass
class Order:
    region: str
    amount: float


@dataclass
class Total:
    region: str
    amount: float


def regional_totals(orders: TableRef[Order]) -> list[Total]:
    """@query"""
    spark = ps.spark.session("lakehouse")
    frame = ps.spark.dataframe(spark, orders).groupBy("region").sum("amount")
    return [Total(region=r[0], amount=r[1]) for r in frame.collect()]
```

The Python code source ships the Spark Connect client with its code, and the cluster's host must be on the egress allow-list. See the [Spark reference](code-sources.md#spark-reference).

## Small jobs, big jobs, replication and caching

Where a query's answer comes from decides what it costs and how fresh it is. This applies to a Python code source's tables that run on the cluster, as to any code source.

| Setup | What a query does | Performance | Timeliness |
|---|---|---|---|
| Replication on | Reads rows already landed in the replica. | The replica refresh pays the job's run time, so each query stays fast whatever the job costs. | As of the last refresh. Use [Freshness](replicas.md#freshness) to say how old a read may be. |
| Replication off | Runs the function live, held to the request timeout. | The query waits for the job. Each query pays for a run on the cluster. | Current as of the run. |
| Replication off with a cache TTL | Runs the function once per TTL and answers from the held rows between runs. | The first read waits for the job; later reads are fast. | As of the last run, up to the TTL old. |

**Big jobs: keep replication on.** A job that runs for minutes or hours cannot answer within a request timeout, so a query must never have to run it.

**Small jobs: replication off is an option.** When the function answers well within the request timeout and the reader wants the current answer, a steward can turn replication off. Every query is then a Spark run. If many readers ask the same thing, add a cache TTL or keep replication on.

### Transforms over governed tables

A `@query` that takes a governed table, like `regional_totals` above, can be called live, `SELECT * FROM regional_totals('sales.orders')`, and the query waits for the job. For a transform that takes longer than a reader should wait, define a materialized view over the call:

```sql
SELECT t.region, t.amount FROM regional_totals('sales.orders') AS t
```

The view is landed like any other materialized view. A read answers from its stored rows and never starts a Spark job. The job runs on the view's refresh interval, and again whenever a table it is given changes, as that table's [change signal](replicas.md#freshness) reports it.

A role reading the view is held to the same rules as a live call. A role that cannot read a table the call is given is refused the view, naming that table.
