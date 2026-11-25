// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2270

package io.provisa.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The {@code provisa} data source for Spark (REQ-2270): a governed Provisa table as a DataFrame,
 * as provisa-pyspark's {@code ProvisaDataSource} reads it.
 *
 * <pre>
 *   spark.read.format("provisa")
 *       .option("uri", "grpc+tls://provisa.example.com:8815")
 *       .option("token", pat)
 *       .load("sales.orders")
 * </pre>
 *
 * <p>The DataFrame's schema is the table's declared types. Each Flight endpoint of the statement
 * is one partition, read on an executor as the same principal: Provisa applies the principal's row
 * rules, masks and redaction before a row leaves it, so they hold inside any cluster.
 */
public final class ProvisaDataSource implements TableProvider, DataSourceRegister {

  @Override
  public String shortName() {
    return "provisa";
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    ReadOptions read = ReadOptions.parse(options.asCaseSensitiveMap());
    return SparkTypes.sparkSchema(Flight.declaredSchema(read));
  }

  /** A schema given to the read is checked against the declared one rather than refused. */
  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> properties) {
    ReadOptions read = ReadOptions.parse(properties);
    Schema declared = Flight.declaredSchema(read);
    List<List<String>> given = new ArrayList<>();
    for (StructField field : schema.fields()) {
      given.add(List.of(field.name(), field.dataType().simpleString()));
    }
    if (!given.equals(SparkTypes.sparkSimple(declared))) {
      throw new IllegalArgumentException(
          "a Provisa table's columns are its declared types "
              + SparkTypes.sparkDdl(declared)
              + "; a schema given to the read must be that one (or leave .schema() out)");
    }
    return new ProvisaTable(read, declared, schema);
  }
}
