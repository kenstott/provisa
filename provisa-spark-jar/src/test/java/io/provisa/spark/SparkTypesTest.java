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

import java.util.List;
import java.util.stream.Stream;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** The Spark type of each declared Arrow type, as provisa-pyspark's spark_types maps it. */
class SparkTypesTest {
  static Field f(String name, ArrowType type, Field... children) {
    return new Field(name, FieldType.nullable(type), List.of(children));
  }

  static Field list(Field element) {
    return f("c", ArrowType.List.INSTANCE, element);
  }

  static Field map(Field key, Field value) {
    return f(
        "c",
        new ArrowType.Map(false),
        new Field(
            "entries",
            FieldType.notNullable(ArrowType.Struct.INSTANCE),
            List.of(new Field("key", FieldType.notNullable(key.getType()), key.getChildren()), value)));
  }

  static Stream<Arguments> mapped() {
    return Stream.of(
        Arguments.of(f("c", ArrowType.Bool.INSTANCE), "boolean"),
        Arguments.of(f("c", new ArrowType.Int(8, true)), "tinyint"),
        Arguments.of(f("c", new ArrowType.Int(16, true)), "smallint"),
        Arguments.of(f("c", new ArrowType.Int(32, true)), "int"),
        Arguments.of(f("c", new ArrowType.Int(64, true)), "bigint"),
        Arguments.of(f("c", new ArrowType.Int(8, false)), "smallint"),
        Arguments.of(f("c", new ArrowType.Int(16, false)), "int"),
        Arguments.of(f("c", new ArrowType.Int(32, false)), "bigint"),
        Arguments.of(f("c", new ArrowType.Int(64, false)), "decimal(20,0)"),
        Arguments.of(f("c", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)), "float"),
        Arguments.of(f("c", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), "float"),
        Arguments.of(f("c", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), "double"),
        Arguments.of(f("c", new ArrowType.Decimal(10, 2, 128)), "decimal(10,2)"),
        Arguments.of(f("c", new ArrowType.Decimal(38, 0, 128)), "decimal(38,0)"),
        Arguments.of(f("c", new ArrowType.Decimal(20, 4, 256)), "decimal(20,4)"),
        Arguments.of(f("c", ArrowType.Utf8.INSTANCE), "string"),
        Arguments.of(f("c", ArrowType.LargeUtf8.INSTANCE), "string"),
        Arguments.of(f("c", ArrowType.Utf8View.INSTANCE), "string"),
        Arguments.of(f("c", ArrowType.Binary.INSTANCE), "binary"),
        Arguments.of(f("c", ArrowType.LargeBinary.INSTANCE), "binary"),
        Arguments.of(f("c", ArrowType.BinaryView.INSTANCE), "binary"),
        Arguments.of(f("c", new ArrowType.FixedSizeBinary(16)), "binary"),
        Arguments.of(f("c", new ArrowType.Date(DateUnit.DAY)), "date"),
        Arguments.of(f("c", new ArrowType.Date(DateUnit.MILLISECOND)), "date"),
        Arguments.of(f("c", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")), "timestamp"),
        Arguments.of(
            f("c", new ArrowType.Timestamp(TimeUnit.NANOSECOND, "America/New_York")), "timestamp"),
        Arguments.of(f("c", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)), "timestamp_ntz"),
        Arguments.of(f("c", new ArrowType.Time(TimeUnit.MICROSECOND, 64)), "time(6)"),
        Arguments.of(f("c", new ArrowType.Time(TimeUnit.MILLISECOND, 32)), "time(6)"),
        Arguments.of(f("c", new ArrowType.Duration(TimeUnit.MICROSECOND)), "interval day to second"),
        Arguments.of(f("c", ArrowType.Null.INSTANCE), "void"),
        Arguments.of(list(f("element", new ArrowType.Int(32, true))), "array<int>"),
        Arguments.of(
            f("c", ArrowType.LargeList.INSTANCE, f("element", ArrowType.Utf8.INSTANCE)),
            "array<string>"),
        Arguments.of(
            f("c", new ArrowType.FixedSizeList(3), f("element", new ArrowType.Int(16, true))),
            "array<smallint>"),
        Arguments.of(
            map(f("key", ArrowType.Utf8.INSTANCE), f("value", new ArrowType.Int(64, true))),
            "map<string,bigint>"),
        Arguments.of(
            f(
                "c",
                ArrowType.Struct.INSTANCE,
                f("a", new ArrowType.Int(32, true)),
                f("b", ArrowType.Utf8.INSTANCE)),
            "struct<a:int,b:string>"));
  }

  @ParameterizedTest
  @MethodSource("mapped")
  void eachDeclaredTypeIsTheSparkTypeHoldingItExactly(Field field, String simple) {
    SparkTypes.SparkType type = SparkTypes.sparkType(field, "c");
    assertEquals(simple, type.simple());
    // The spelling is Spark's own: its simpleString, and DDL Spark parses to the same type.
    assertEquals(simple, type.dataType().simpleString());
    assertEquals(type.dataType(), DataType.fromDDL(type.ddl()));
  }

  @Test
  void theDdlQuotesEveryColumnNameAndStructMember() {
    Schema schema =
        new Schema(
            List.of(
                f("id", new ArrowType.Int(32, true)),
                f("odd `name`", new ArrowType.Decimal(10, 2, 128)),
                f("s", ArrowType.Struct.INSTANCE, f("x y", new ArrowType.Int(32, true)))));
    assertEquals(
        "`id` int, `odd ``name``` decimal(10,2), `s` struct<`x y`:int>",
        SparkTypes.sparkDdl(schema));
    assertEquals(
        List.of(
            List.of("id", "int"),
            List.of("odd `name`", "decimal(10,2)"),
            List.of("s", "struct<x y:int>")),
        SparkTypes.sparkSimple(schema));
    assertEquals(SparkTypes.sparkSchema(schema), DataType.fromDDL(SparkTypes.sparkDdl(schema)));
  }

  static Stream<Field> unmapped() {
    return Stream.of(
        f("amount", new ArrowType.Decimal(40, 2, 256)),
        list(f("element", new ArrowType.Decimal(50, 0, 256))),
        new Field(
            "amount",
            new FieldType(
                true,
                ArrowType.Utf8.INSTANCE,
                new DictionaryEncoding(0, false, new ArrowType.Int(32, true))),
            List.of()),
        f("amount", new ArrowType.Interval(org.apache.arrow.vector.types.IntervalUnit.YEAR_MONTH)),
        f(
            "amount",
            new ArrowType.Union(org.apache.arrow.vector.types.UnionMode.Sparse, new int[] {0}),
            f("a", new ArrowType.Int(32, true))));
  }

  @ParameterizedTest
  @MethodSource("unmapped")
  void aTypeNoSparkTypeHoldsIsRefusedNamingItsColumn(Field field) {
    SparkTypes.UnmappedType error =
        assertThrows(SparkTypes.UnmappedType.class, () -> SparkTypes.sparkType(field, "amount"));
    assertTrue(error.getMessage().startsWith("column 'amount' is declared "), error.getMessage());
    assertTrue(
        error.getMessage().endsWith(", which no Spark type holds exactly"), error.getMessage());
  }
}
