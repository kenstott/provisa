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
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * The Spark type of each Arrow type Provisa declares a column as (REQ-2270), the map of
 * provisa-pyspark's {@code spark_types.py}.
 *
 * <p>A table's declared types arrive as an Arrow schema (Flight SQL GetSchema). Each column is
 * given the Spark type that holds its values exactly; a type Spark has no exact equivalent for is
 * refused by column name, never widened or read as a string. Each type is also spelt as DDL
 * (column names quoted) and as Spark's {@code simpleString} (unquoted), the two spellings the
 * Python data source compares and reports.
 */
public final class SparkTypes {
  /** Spark's largest decimal precision. */
  private static final int MAX_DECIMAL_PRECISION = 38;

  private SparkTypes() {}

  /** An Arrow type no Spark type holds exactly. */
  public static final class UnmappedType extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    public UnmappedType(String message) {
      super(message);
    }
  }

  /** A Spark type, as DDL (column names quoted) and as Spark's {@code simpleString}. */
  public record SparkType(DataType dataType, String ddl, String simple) {}

  private static String quoted(String name) {
    return "`" + name.replace("`", "``") + "`";
  }

  private static SparkType same(DataType dataType, String spelling) {
    return new SparkType(dataType, spelling, spelling);
  }

  /** The Spark type of a scalar Arrow type, or null when it is not a scalar this maps. */
  private static SparkType scalar(ArrowType arrow) {
    if (arrow instanceof ArrowType.Bool) {
      return same(DataTypes.BooleanType, "boolean");
    }
    if (arrow instanceof ArrowType.Int i) {
      int bits = i.getBitWidth();
      boolean signed = i.getIsSigned();
      if (signed && bits == 8) {
        return same(DataTypes.ByteType, "tinyint");
      }
      if ((signed && bits == 16) || (!signed && bits == 8)) {
        return same(DataTypes.ShortType, "smallint");
      }
      if ((signed && bits == 32) || (!signed && bits == 16)) {
        return same(DataTypes.IntegerType, "int");
      }
      if ((signed && bits == 64) || (!signed && bits == 32)) {
        return same(DataTypes.LongType, "bigint");
      }
      if (!signed && bits == 64) {
        return same(DataTypes.createDecimalType(20, 0), "decimal(20,0)");
      }
      return null;
    }
    if (arrow instanceof ArrowType.FloatingPoint f) {
      if (f.getPrecision() == FloatingPointPrecision.DOUBLE) {
        return same(DataTypes.DoubleType, "double");
      }
      return same(DataTypes.FloatType, "float");
    }
    if (arrow instanceof ArrowType.Decimal d) {
      if (d.getPrecision() > MAX_DECIMAL_PRECISION) {
        return null;
      }
      return same(
          DataTypes.createDecimalType(d.getPrecision(), d.getScale()),
          "decimal(" + d.getPrecision() + "," + d.getScale() + ")");
    }
    if (arrow instanceof ArrowType.Utf8
        || arrow instanceof ArrowType.LargeUtf8
        || arrow instanceof ArrowType.Utf8View) {
      return same(DataTypes.StringType, "string");
    }
    if (arrow instanceof ArrowType.Binary
        || arrow instanceof ArrowType.LargeBinary
        || arrow instanceof ArrowType.BinaryView
        || arrow instanceof ArrowType.FixedSizeBinary) {
      return same(DataTypes.BinaryType, "binary");
    }
    if (arrow instanceof ArrowType.Date) {
      return same(DataTypes.DateType, "date");
    }
    if (arrow instanceof ArrowType.Timestamp t) {
      return t.getTimezone() != null
          ? same(DataTypes.TimestampType, "timestamp")
          : same(DataTypes.TimestampNTZType, "timestamp_ntz");
    }
    if (arrow instanceof ArrowType.Time) {
      // Parsed rather than constructed: TimeType is Spark 4.1's, and a reference to it here would
      // keep this class from loading on Spark 4.0, where only a time column is then refused.
      return same(DataType.fromDDL("time(6)"), "time(6)");
    }
    if (arrow instanceof ArrowType.Duration) {
      return same(
          DataTypes.createDayTimeIntervalType(DayTimeIntervalType.DAY(), DayTimeIntervalType.SECOND()),
          "interval day to second");
    }
    if (arrow instanceof ArrowType.Null) {
      return same(DataTypes.NullType, "void");
    }
    return null;
  }

  /**
   * The Spark type holding {@code field}'s values exactly; {@link UnmappedType} naming {@code
   * column} when there is none.
   */
  public static SparkType sparkType(Field field, String column) {
    ArrowType arrow = field.getType();
    if (field.getDictionary() == null) {
      SparkType scalar = scalar(arrow);
      if (scalar != null) {
        return scalar;
      }
      if (arrow instanceof ArrowType.List
          || arrow instanceof ArrowType.LargeList
          || arrow instanceof ArrowType.FixedSizeList) {
        SparkType element = sparkType(field.getChildren().get(0), column);
        return new SparkType(
            DataTypes.createArrayType(element.dataType(), true),
            "array<" + element.ddl() + ">",
            "array<" + element.simple() + ">");
      }
      if (arrow instanceof ArrowType.Map) {
        List<Field> entries = field.getChildren().get(0).getChildren();
        SparkType key = sparkType(entries.get(0), column);
        SparkType value = sparkType(entries.get(1), column);
        return new SparkType(
            DataTypes.createMapType(key.dataType(), value.dataType(), true),
            "map<" + key.ddl() + "," + value.ddl() + ">",
            "map<" + key.simple() + "," + value.simple() + ">");
      }
      if (arrow instanceof ArrowType.Struct) {
        List<StructField> fields = new ArrayList<>();
        List<String> ddl = new ArrayList<>();
        List<String> simple = new ArrayList<>();
        for (Field member : field.getChildren()) {
          SparkType t = sparkType(member, column);
          fields.add(DataTypes.createStructField(member.getName(), t.dataType(), true));
          ddl.add(quoted(member.getName()) + ":" + t.ddl());
          simple.add(member.getName() + ":" + t.simple());
        }
        return new SparkType(
            DataTypes.createStructType(fields),
            "struct<" + String.join(",", ddl) + ">",
            "struct<" + String.join(",", simple) + ">");
      }
    }
    throw new UnmappedType(
        "column "
            + PyRepr.of(column)
            + " is declared "
            + (field.getDictionary() == null ? arrow : "dictionary<" + arrow + ">")
            + ", which no Spark type holds exactly");
  }

  /** A column with its Spark type. */
  public record Column(String name, SparkType type) {}

  /** Each column of {@code schema} with its Spark type, in order. */
  public static List<Column> sparkColumns(Schema schema) {
    List<Column> columns = new ArrayList<>();
    for (Field field : schema.getFields()) {
      columns.add(new Column(field.getName(), sparkType(field, field.getName())));
    }
    return columns;
  }

  /** {@code schema} as DDL, as the Python data source's {@code schema()} answers it. */
  public static String sparkDdl(Schema schema) {
    List<String> parts = new ArrayList<>();
    for (Column c : sparkColumns(schema)) {
      parts.add(quoted(c.name()) + " " + c.type().ddl());
    }
    return String.join(", ", parts);
  }

  /** The Spark schema of {@code schema}: every column nullable, as a DDL-parsed one is. */
  public static StructType sparkSchema(Schema schema) {
    List<StructField> fields = new ArrayList<>();
    for (Column c : sparkColumns(schema)) {
      fields.add(DataTypes.createStructField(c.name(), c.type().dataType(), true));
    }
    return DataTypes.createStructType(fields);
  }

  /** Each column of {@code schema} with its Spark type's {@code simpleString}. */
  public static List<List<String>> sparkSimple(Schema schema) {
    List<List<String>> out = new ArrayList<>();
    for (Column c : sparkColumns(schema)) {
      out.add(List.of(c.name(), c.type().simple()));
    }
    return out;
  }
}
