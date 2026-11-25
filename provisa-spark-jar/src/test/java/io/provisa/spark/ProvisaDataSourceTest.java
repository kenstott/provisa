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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * {@code spark.read.format("provisa")} in a local Spark against a stub Provisa Flight server: the
 * DataFrame is the declared types, its rows are the endpoint's (a column in another Arrow layout
 * read in Spark's), and every call carries the caller. The read against a real server, from the
 * shaded jar, is tests/integration/test_spark_reads_jar_e2e.py.
 */
class ProvisaDataSourceTest {
  static final Schema DECLARED =
      new Schema(
          List.of(
              Field.nullable("id", new ArrowType.Int(32, true)),
              Field.nullable("region", ArrowType.Utf8.INSTANCE),
              Field.nullable("amount", new ArrowType.Decimal(10, 2, 128))));
  static final Schema ROWS =
      new Schema(
          List.of(
              Field.nullable("id", new ArrowType.Int(32, true)),
              Field.nullable("region", ArrowType.LargeUtf8.INSTANCE),
              Field.nullable("amount", new ArrowType.Decimal(10, 2, 128))));

  static SparkSession spark;
  static StubProvisa provisa;

  @BeforeAll
  static void start() throws Exception {
    provisa =
        new StubProvisa(
            DECLARED,
            ROWS,
            root -> {
              IntVector id = (IntVector) root.getVector("id");
              LargeVarCharVector region = (LargeVarCharVector) root.getVector("region");
              DecimalVector amount = (DecimalVector) root.getVector("amount");
              id.setSafe(0, 1);
              region.setSafe(0, "east".getBytes(StandardCharsets.UTF_8));
              amount.setSafe(0, new BigDecimal("19.99"));
              id.setSafe(1, 3);
              region.setSafe(1, "east".getBytes(StandardCharsets.UTF_8));
              amount.setNull(1);
              root.setRowCount(2);
            });
    spark =
        SparkSession.builder()
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate();
  }

  @AfterAll
  static void stop() throws Exception {
    spark.stop();
    provisa.close();
  }

  @Test
  void aGovernedTableIsReadAsADataFrameOfItsDeclaredTypesAsTheCaller() {
    provisa.calls.clear();
    var frame =
        spark
            .read()
            .format("provisa")
            .option("uri", provisa.uri())
            .option("token", "pat-1")
            .option("role", "clerk")
            .load("sales.orders");
    List<List<String>> columns = new ArrayList<>();
    for (StructField f : frame.schema().fields()) {
      columns.add(List.of(f.name(), f.dataType().simpleString()));
    }
    assertEquals(
        List.of(
            List.of("id", "int"), List.of("region", "string"), List.of("amount", "decimal(10,2)")),
        columns);
    List<Row> rows = frame.orderBy("id").collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("east", rows.get(0).getString(1));
    assertEquals(new BigDecimal("19.99"), rows.get(0).getDecimal(2));
    assertEquals(3, rows.get(1).getInt(0));
    assertTrue(rows.get(1).isNullAt(2));
    String statement = "SELECT * FROM \"sales\".\"orders\"";
    for (StubProvisa.Call call : provisa.calls) {
      assertEquals("Bearer pat-1", call.authorization(), call.toString());
      assertEquals("clerk", call.role(), call.toString());
    }
    assertTrue(provisa.calls.contains(new StubProvisa.Call("info", statement, "Bearer pat-1", "clerk")));
    assertTrue(provisa.calls.contains(new StubProvisa.Call("rows", "rows", "Bearer pat-1", "clerk")));
  }

  @Test
  void aSchemaGivenToTheReadMustBeTheDeclaredOne() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                spark
                    .read()
                    .format("provisa")
                    .schema("id bigint, region string, amount decimal(10,2)")
                    .option("uri", provisa.uri())
                    .load("sales.orders"));
    assertEquals(
        "a Provisa table's columns are its declared types `id` int, `region` string, `amount`"
            + " decimal(10,2); a schema given to the read must be that one (or leave .schema()"
            + " out)",
        error.getMessage());
    long rows =
        spark
            .read()
            .format("provisa")
            .schema("id int, region string, amount decimal(10,2)")
            .option("uri", provisa.uri())
            .load("sales.orders")
            .count();
    assertEquals(2, rows);
  }

  @Test
  void aMisspeltOptionIsRefusedByName() {
    ReadOptions.OptionError error =
        assertThrows(
            ReadOptions.OptionError.class,
            () ->
                spark
                    .read()
                    .format("provisa")
                    .option("uri", provisa.uri())
                    .option("tokn", "pat-1")
                    .load("sales.orders"));
    assertTrue(error.getMessage().startsWith("unknown option(s) tokn;"), error.getMessage());
  }
}
