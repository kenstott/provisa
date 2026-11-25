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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.Consumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampNanoTZVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A column in another Arrow layout of its declared type, copied into the layout Spark reads that
 * type as (Spark's own conversion of the column's Spark type), and read back through Spark's
 * ArrowColumnVector.
 */
class ConformTest {
  private final BufferAllocator allocator = new RootAllocator();

  @AfterEach
  void close() {
    allocator.close();
  }

  /** The column as Spark reads it, after {@code check} has looked at it. */
  private void read(FieldVector source, Consumer<ArrowColumnVector> check) {
    try (source) {
      Field declared = source.getField();
      Field target = Conform.target(declared);
      assertFalse(Conform.sameLayout(declared, target), declared + " is already " + target);
      try (FieldVector copy = Conform.copied(source, target, allocator)) {
        check.accept(new ArrowColumnVector(copy));
      }
    }
  }

  private static Field field(String name, ArrowType type, Field... children) {
    return new Field(name, FieldType.nullable(type), List.of(children));
  }

  @Test
  void aColumnAlreadyInSparksLayoutIsReadAsItArrives() {
    try (IntVector v = new IntVector("id", allocator)) {
      assertTrue(Conform.sameLayout(v.getField(), Conform.target(v.getField())));
    }
  }

  /** The types of a field and its children, names and nullability aside. */
  private static String types(Field field) {
    StringBuilder out = new StringBuilder(field.getType().toString());
    if (!field.getChildren().isEmpty()) {
      out.append('<');
      for (Field child : field.getChildren()) {
        out.append(types(child)).append(',');
      }
      out.append('>');
    }
    return out.toString();
  }

  @ParameterizedTest
  @MethodSource("io.provisa.spark.SparkTypesTest#mapped")
  void theTargetLayoutIsSparksOwnConversionOfTheColumnsSparkType(Field declared, String simple) {
    SparkTypes.SparkType type = SparkTypes.sparkType(declared, "c");
    StructType schema =
        DataTypes.createStructType(List.of(DataTypes.createStructField("c", type.dataType(), true)));
    Field spark = ArrowUtils.toArrowSchema(schema, "UTC", true, false, false).getFields().get(0);
    assertEquals(types(spark), types(Conform.target(declared)), simple);
  }

  @Test
  void aLargeStringIsReadAsAString() {
    LargeVarCharVector v = new LargeVarCharVector("region", allocator);
    v.allocateNew();
    v.setSafe(0, "east".getBytes(StandardCharsets.UTF_8));
    v.setNull(1);
    v.setValueCount(2);
    read(
        v,
        c -> {
          assertEquals("east", c.getUTF8String(0).toString());
          assertTrue(c.isNullAt(1));
        });
  }

  @Test
  void unsignedNumbersAreReadWidenedAndExact() {
    UInt1Vector small = new UInt1Vector("u", allocator);
    small.allocateNew();
    small.setSafe(0, 255);
    small.setValueCount(1);
    read(small, c -> assertEquals(255, c.getShort(0)));

    UInt8Vector big = new UInt8Vector("u", allocator);
    big.allocateNew();
    big.setSafe(0, -1L);
    big.setValueCount(1);
    read(
        big,
        c ->
            assertEquals(
                new BigDecimal("18446744073709551615"), c.getDecimal(0, 20, 0).toJavaBigDecimal()));
  }

  @Test
  void aTimestampInNanosecondsIsReadInMicrosecondsWhenExact() {
    TimeStampNanoTZVector v = new TimeStampNanoTZVector("at", allocator, "America/New_York");
    v.allocateNew();
    v.setSafe(0, 1_000_000_000L);
    v.setValueCount(1);
    read(v, c -> assertEquals(1_000_000L, c.getLong(0)));
  }

  @Test
  void aCastThatWouldLoseAValueRaisesRatherThanTruncate() {
    TimeStampNanoTZVector v = new TimeStampNanoTZVector("at", allocator, "UTC");
    v.allocateNew();
    v.setSafe(0, 1_000_000_001L);
    v.setValueCount(1);
    Conform.LossyValue error =
        assertThrows(Conform.LossyValue.class, () -> read(v, c -> {}));
    assertEquals(
        "column 'at': the value 1000000001 in NANOSECOND would lose its fraction as MICROSECOND",
        error.getMessage());

    DateMilliVector d = new DateMilliVector("day", allocator);
    d.allocateNew();
    d.setSafe(0, 1L);
    d.setValueCount(1);
    assertThrows(Conform.LossyValue.class, () -> read(d, c -> {}));
  }

  @Test
  void aDateInMillisecondsIsReadAsDays() {
    DateMilliVector v = new DateMilliVector("day", allocator);
    v.allocateNew();
    v.setSafe(0, 86_400_000L);
    v.setValueCount(1);
    read(v, c -> assertEquals(1, c.getInt(0)));
  }

  @Test
  void aTimeAndADurationAreReadInSparksUnits() {
    TimeMicroVector t = new TimeMicroVector("t", allocator);
    t.allocateNew();
    t.setSafe(0, 1_000_000L);
    t.setValueCount(1);
    // Spark holds a time as nanoseconds of the day.
    read(t, c -> assertEquals(1_000_000_000L, c.getLong(0)));

    DurationVector d =
        new DurationVector(
            field("d", new ArrowType.Duration(TimeUnit.SECOND)), allocator);
    d.allocateNew();
    d.setSafe(0, 2L);
    d.setValueCount(1);
    read(d, c -> assertEquals(2_000_000L, c.getLong(0)));
  }

  @Test
  void aLargeListOfLargeStringsIsReadAsAnArray() {
    LargeListVector v =
        (LargeListVector)
            field("tags", ArrowType.LargeList.INSTANCE, field("item", ArrowType.LargeUtf8.INSTANCE))
                .createVector(allocator);
    v.allocateNew();
    LargeVarCharVector items = (LargeVarCharVector) v.getDataVector();
    v.startNewValue(0);
    items.setSafe(0, "a".getBytes(StandardCharsets.UTF_8));
    items.setSafe(1, "b".getBytes(StandardCharsets.UTF_8));
    v.endValue(0, 2);
    v.setNull(1);
    items.setValueCount(2);
    v.setValueCount(2);
    read(
        v,
        c -> {
          assertEquals(2, c.getArray(0).numElements());
          assertEquals("b", c.getArray(0).getUTF8String(1).toString());
          assertTrue(c.isNullAt(1));
        });
  }

  @Test
  void aStructsMembersAreEachReadInSparksLayout() {
    StructVector v =
        (StructVector)
            field(
                    "s",
                    ArrowType.Struct.INSTANCE,
                    field("n", new ArrowType.Int(8, false)),
                    field("name", ArrowType.LargeUtf8.INSTANCE))
                .createVector(allocator);
    v.allocateNew();
    ((UInt1Vector) v.getChild("n")).setSafe(0, 200);
    ((LargeVarCharVector) v.getChild("name")).setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
    v.setIndexDefined(0);
    v.setNull(1);
    v.setValueCount(2);
    read(
        v,
        c -> {
          assertEquals(200, c.getStruct(0).getShort(0));
          assertEquals("x", c.getStruct(0).getUTF8String(1).toString());
          assertTrue(c.isNullAt(1));
        });
  }

  @Test
  void aDayDateIsAlreadySparksLayout() {
    Field day = field("d", new ArrowType.Date(DateUnit.DAY));
    assertTrue(Conform.sameLayout(day, Conform.target(day)));
  }
}
