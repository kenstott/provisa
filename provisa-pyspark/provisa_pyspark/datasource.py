# Copyright (c) 2026 Kenneth Stott
# Canary: 506f8d61-ce42-4d52-a6e8-ed48f65f74ad
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The ``provisa`` data source for Spark (REQ-2270): a governed Provisa table as a DataFrame.

    from provisa_pyspark import ProvisaDataSource

    spark.dataSource.register(ProvisaDataSource)
    orders = (
        spark.read.format("provisa")
        .option("uri", "grpc+tls://provisa.example.com:8815")
        .option("token", pat)
        .load("sales.orders")
    )

The DataFrame's schema is the table's declared types. Each Flight endpoint of the statement is one
partition, read on an executor as the same principal: Provisa applies the principal's row rules,
masks and redaction before a row leaves it, so they hold inside any cluster.
"""

# Requirements: REQ-2270

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.pandas.types import to_arrow_schema
from pyspark.sql.types import StructType

from provisa_pyspark import flight
from provisa_pyspark.options import ReadOptions
from provisa_pyspark.spark_types import spark_ddl, spark_simple


class ProvisaDataSource(DataSource):
    """``spark.read.format("provisa")``: reads one governed table over Arrow Flight SQL."""

    @classmethod
    def name(cls) -> str:
        return "provisa"

    def __init__(self, options: dict[str, str]) -> None:
        super().__init__(options)
        self._read = ReadOptions.parse(options)

    def schema(self) -> str:
        return spark_ddl(flight.declared_schema(self._read))

    def reader(self, schema: StructType) -> DataSourceReader:
        declared = flight.declared_schema(self._read)
        given = [(f.name, f.dataType.simpleString()) for f in schema.fields]
        if given != spark_simple(declared):
            raise ValueError(
                f"a Provisa table's columns are its declared types {spark_ddl(declared)}; a "
                "schema given to the read must be that one (or leave .schema() out)"
            )
        return ProvisaReader(self._read, declared, schema)


class ProvisaPartition(InputPartition):
    """One Flight endpoint of the statement."""

    def __init__(self, endpoint: flight.Endpoint) -> None:
        super().__init__(endpoint)
        self.endpoint = endpoint


class ProvisaReader(DataSourceReader):
    """Reads each endpoint's batches as the principal, in the Arrow layout Spark reads."""

    def __init__(self, read: ReadOptions, declared: pa.Schema, schema: StructType) -> None:
        self._read = read
        self._declared = declared
        # The layout Spark checks a yielded batch against (its data source read worker's own
        # conversion of the read's schema).
        self._target = to_arrow_schema(schema, timezone="UTC")

    def partitions(self) -> list[InputPartition]:
        return [ProvisaPartition(e) for e in flight.endpoints(self._read)]

    def read(self, partition: InputPartition) -> Iterator[pa.RecordBatch]:
        assert isinstance(partition, ProvisaPartition), partition
        for batch in flight.batches(self._read, partition.endpoint):
            yield flight.conformed(batch, self._declared, self._target)
