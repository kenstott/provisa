# provisa-pyspark

A Spark data source for [Provisa](https://github.com/kenstott/provisa). Spark reads a governed Provisa table as a DataFrame over Arrow Flight SQL, as the principal whose credential you give it. Provisa applies that principal's row rules, masks and redaction before any row leaves the server, and audits the read.

## Install

```bash
pip install provisa-pyspark
```

It needs Spark 4.0 or later. A cluster brings its own; `pip install "provisa-pyspark[spark]"` installs pyspark where none is.

## Use

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

| Option | What it is |
|---|---|
| `uri` | Provisa's Arrow Flight endpoint, `grpc://host:port` or `grpc+tls://host:port`. Required. |
| `token` | The credential the read runs as (personal access token or provider token). |
| `role` | A role to act as; honored only when the principal holds it. |
| `table` | The table name, when `load()` is called without one. |

Other options are refused by name. The DataFrame's columns are the table's declared types; a type with no exact Spark equivalent is refused by column. Each Flight endpoint is one partition.
