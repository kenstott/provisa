# Spark data source

A Spark job reads a Provisa table as a DataFrame with the `provisa` data source. The read runs over Arrow Flight SQL as the principal whose credential you give it. Provisa applies that principal's row rules, masks and redaction before a row leaves the server, so they hold inside any Spark cluster, and the read is audited as that principal's.

## Install

```bash
pip install provisa-pyspark
```

The package needs Spark 4.0 or later, which a cluster brings with it. Where Spark is not installed yet, such as a laptop running local mode, `pip install "provisa-pyspark[spark]"` installs it too.

## Read a table

```python
from provisa_pyspark import ProvisaDataSource

spark.dataSource.register(ProvisaDataSource)

orders = (
    spark.read.format("provisa")
    .option("uri", "grpc+tls://provisa.example.com:8815")
    .option("token", personal_access_token)
    .load("sales.orders")
)
```

Register the data source once per Spark session. The name you pass to `load` is the table as Provisa's SQL names it. A part that contains a dot goes in double quotes, as in `'"sales.eu".orders'`.

| Option | Required | What it is |
|---|---|---|
| `uri` | yes | Provisa's Arrow Flight endpoint: `grpc://host:port`, or `grpc+tls://host:port` for a TLS listener. |
| `token` | on a secured deployment | The credential the read runs as: a personal access token, or a token from your sign-in provider. |
| `role` | no | A role to act as. Provisa honors it only when the principal holds that role. |
| `table` | instead of `load`'s path | The table name, for code that calls `load()` with no argument. |

Any other option is refused by name. A misspelt `token` fails the read instead of running it without a credential.

## Columns and types

The DataFrame's columns are the ones the principal is served, typed as the table declares them. A `numeric(10,2)` column is `decimal(10,2)` in Spark. When a declared type has no exact Spark equivalent, such as a decimal wider than 38 digits, the read is refused and names the column.

You can leave `.schema()` out. A schema you pass must match the declared one exactly.

## Partitions

Each Flight endpoint Provisa answers for the table becomes one Spark partition, read on an executor with the same credential. Every executor needs network access to the Flight port.
