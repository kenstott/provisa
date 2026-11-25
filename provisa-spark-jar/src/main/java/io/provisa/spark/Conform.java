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

import java.math.BigDecimal;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.DurationVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * A column in the Arrow layout Spark reads its declared type as (REQ-2270): provisa-pyspark's
 * {@code batch.cast(target, safe=True)} in {@code flight.conformed}.
 *
 * <p>The column's Spark type is already the declared one ({@link Flight#checkDeclared}), so the
 * values change only their layout: an unsigned or half-width number widened, a large or view
 * string or binary, a date in milliseconds, a timestamp, time or duration in another unit, a large
 * or fixed-size list. A value the target layout cannot hold exactly (a timestamp with nanoseconds
 * read as microseconds, a date64 inside a day) is refused, never truncated.
 */
final class Conform {
  private Conform() {}

  /** A value the target layout cannot hold without losing part of it. */
  static final class LossyValue extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    LossyValue(String message) {
      super(message);
    }
  }

  /**
   * The layout Spark reads a declared column's Spark type as: what Spark's own ArrowUtils gives
   * that type (strings and binaries not large; timestamps, durations in microseconds; a time in
   * nanoseconds; decimals 128 bits wide). Taken from the declared Arrow type rather than through
   * ArrowUtils, whose signatures differ between Spark 4 releases.
   */
  static Field target(Field declared) {
    ArrowType arrow = declared.getType();
    String name = declared.getName();
    if (arrow instanceof ArrowType.Int i && !i.getIsSigned()) {
      arrow =
          i.getBitWidth() == 64
              ? new ArrowType.Decimal(20, 0, 128)
              : new ArrowType.Int(i.getBitWidth() * 2, true);
    } else if (arrow instanceof ArrowType.FloatingPoint f
        && f.getPrecision() == FloatingPointPrecision.HALF) {
      arrow = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
    } else if (arrow instanceof ArrowType.Decimal d) {
      arrow = new ArrowType.Decimal(d.getPrecision(), d.getScale(), 128);
    } else if (arrow instanceof ArrowType.LargeUtf8 || arrow instanceof ArrowType.Utf8View) {
      arrow = ArrowType.Utf8.INSTANCE;
    } else if (arrow instanceof ArrowType.LargeBinary
        || arrow instanceof ArrowType.BinaryView
        || arrow instanceof ArrowType.FixedSizeBinary) {
      arrow = ArrowType.Binary.INSTANCE;
    } else if (arrow instanceof ArrowType.Date) {
      arrow = new ArrowType.Date(DateUnit.DAY);
    } else if (arrow instanceof ArrowType.Timestamp t) {
      arrow = new ArrowType.Timestamp(TimeUnit.MICROSECOND, t.getTimezone() == null ? null : "UTC");
    } else if (arrow instanceof ArrowType.Time) {
      arrow = new ArrowType.Time(TimeUnit.NANOSECOND, 64);
    } else if (arrow instanceof ArrowType.Duration) {
      arrow = new ArrowType.Duration(TimeUnit.MICROSECOND);
    } else if (arrow instanceof ArrowType.LargeList || arrow instanceof ArrowType.FixedSizeList
        || arrow instanceof ArrowType.List) {
      Field element = target(declared.getChildren().get(0));
      return new Field(
          name,
          FieldType.nullable(ArrowType.List.INSTANCE),
          List.of(new Field("element", element.getFieldType(), element.getChildren())));
    } else if (arrow instanceof ArrowType.Map) {
      List<Field> entries = declared.getChildren().get(0).getChildren();
      Field key = target(entries.get(0));
      Field value = target(entries.get(1));
      return new Field(
          name,
          FieldType.nullable(new ArrowType.Map(false)),
          List.of(
              new Field(
                  "entries",
                  FieldType.notNullable(ArrowType.Struct.INSTANCE),
                  List.of(
                      new Field("key", FieldType.notNullable(key.getType()), key.getChildren()),
                      new Field("value", value.getFieldType(), value.getChildren())))));
    } else if (arrow instanceof ArrowType.Struct) {
      List<Field> members = new java.util.ArrayList<>();
      for (Field member : declared.getChildren()) {
        members.add(target(member));
      }
      return new Field(name, FieldType.nullable(ArrowType.Struct.INSTANCE), members);
    }
    return new Field(name, FieldType.nullable(arrow), List.of());
  }

  /** Whether {@code got} is already laid out as {@code target}, its children included. */
  static boolean sameLayout(Field got, Field target) {
    if (Types.getMinorTypeForArrowType(got.getType())
        != Types.getMinorTypeForArrowType(target.getType())) {
      return false;
    }
    if (got.getType() instanceof ArrowType.Duration g
        && target.getType() instanceof ArrowType.Duration t
        && g.getUnit() != t.getUnit()) {
      return false;
    }
    List<Field> a = got.getChildren();
    List<Field> b = target.getChildren();
    if (a.size() != b.size()) {
      return false;
    }
    for (int i = 0; i < a.size(); i++) {
      if (!sameLayout(a.get(i), b.get(i))) {
        return false;
      }
    }
    return true;
  }

  /** {@code source}'s rows copied into a new vector laid out as {@code target}. */
  static FieldVector copied(FieldVector source, Field target, BufferAllocator allocator) {
    FieldVector out = target.createVector(allocator);
    try {
      out.allocateNew();
      int rows = source.getValueCount();
      for (int i = 0; i < rows; i++) {
        copy(source, i, out, i, target.getName());
      }
      out.setValueCount(rows);
      return out;
    } catch (RuntimeException e) {
      out.close();
      throw e;
    }
  }

  private static void copy(ValueVector from, int i, ValueVector to, int j, String column) {
    if (sameLayout(from.getField(), to.getField())) {
      to.copyFromSafe(i, j, from);
      return;
    }
    if (from.isNull(i)) {
      setNull(to, j);
      return;
    }
    if (to instanceof ListVector list) {
      // A map is a list of key/value structs: ListVector covers it.
      int at = list.startNewValue(j);
      ValueVector element = list.getDataVector();
      int[] range = range(from, i);
      ValueVector fromElement = elements(from);
      for (int k = range[0]; k < range[1]; k++) {
        copy(fromElement, k, element, at++, column);
      }
      list.endValue(j, range[1] - range[0]);
      return;
    }
    if (to instanceof StructVector struct) {
      StructVector fromStruct = (StructVector) from;
      struct.setIndexDefined(j);
      for (int k = 0; k < struct.size(); k++) {
        copy(fromStruct.getChildByOrdinal(k), i, struct.getChildByOrdinal(k), j, column);
      }
      return;
    }
    scalar(from, i, to, j, column);
  }

  private static ValueVector elements(ValueVector list) {
    if (list instanceof ListVector l) {
      return l.getDataVector();
    }
    if (list instanceof LargeListVector l) {
      return l.getDataVector();
    }
    if (list instanceof FixedSizeListVector l) {
      return l.getDataVector();
    }
    throw new IllegalStateException("not a list vector: " + list.getField());
  }

  private static int[] range(ValueVector list, int i) {
    if (list instanceof ListVector l) {
      return new int[] {l.getElementStartIndex(i), l.getElementEndIndex(i)};
    }
    if (list instanceof LargeListVector l) {
      return new int[] {
        Math.toIntExact(l.getElementStartIndex(i)), Math.toIntExact(l.getElementEndIndex(i))
      };
    }
    if (list instanceof FixedSizeListVector l) {
      int size = l.getListSize();
      return new int[] {i * size, i * size + size};
    }
    throw new IllegalStateException("not a list vector: " + list.getField());
  }

  private static void setNull(ValueVector to, int j) {
    if (to instanceof BaseFixedWidthVector v) {
      v.setNull(j);
    } else if (to instanceof BaseVariableWidthVector v) {
      v.setNull(j);
    } else if (to instanceof ListVector v) {
      v.setNull(j);
    } else if (to instanceof StructVector v) {
      v.setNull(j);
    } else if (!(to.getMinorType() == MinorType.NULL)) {
      throw new IllegalStateException("no null for " + to.getField());
    }
  }

  private static void scalar(ValueVector from, int i, ValueVector to, int j, String column) {
    switch (to.getMinorType()) {
      case TINYINT -> ((TinyIntVector) to).setSafe(j, (byte) integral(from, i));
      case SMALLINT -> ((SmallIntVector) to).setSafe(j, (short) integral(from, i));
      case INT -> ((IntVector) to).setSafe(j, (int) integral(from, i));
      case BIGINT -> ((BigIntVector) to).setSafe(j, integral(from, i));
      case FLOAT4 -> ((Float4Vector) to).setSafe(j, ((Float2Vector) from).getValueAsFloat(i));
      case DECIMAL -> ((DecimalVector) to).setSafe(j, decimal(from, i));
      case VARCHAR -> ((VarCharVector) to).setSafe(j, bytes(from, i));
      case VARBINARY -> ((VarBinaryVector) to).setSafe(j, bytes(from, i));
      case DATEDAY -> ((DateDayVector) to).setSafe(j, days(from, i, column));
      case TIMESTAMPSEC, TIMESTAMPMILLI, TIMESTAMPMICRO, TIMESTAMPNANO,
          TIMESTAMPSECTZ, TIMESTAMPMILLITZ, TIMESTAMPMICROTZ, TIMESTAMPNANOTZ -> {
        TimeStampVector source = (TimeStampVector) from;
        TimeUnit fromUnit = ((ArrowType.Timestamp) from.getField().getType()).getUnit();
        TimeUnit toUnit = ((ArrowType.Timestamp) to.getField().getType()).getUnit();
        ((TimeStampVector) to).setSafe(j, rescale(source.get(i), fromUnit, toUnit, column));
      }
      case TIMESEC, TIMEMILLI, TIMEMICRO, TIMENANO -> time(from, i, to, j, column);
      case DURATION -> {
        DurationVector source = (DurationVector) from;
        long value = DurationVector.get(source.getDataBuffer(), i);
        TimeUnit toUnit = ((ArrowType.Duration) to.getField().getType()).getUnit();
        ((DurationVector) to).setSafe(j, rescale(value, source.getUnit(), toUnit, column));
      }
      default ->
          throw new IllegalStateException(
              "column " + PyRepr.of(column) + ": no layout change from "
                  + from.getField().getType() + " to " + to.getField().getType());
    }
  }

  private static long integral(ValueVector from, int i) {
    return switch (from.getMinorType()) {
      case UINT1 -> UInt1Vector.getNoOverflow(((UInt1Vector) from).getDataBuffer(), i);
      case UINT2 -> ((UInt2Vector) from).get(i);
      case UINT4 -> UInt4Vector.getNoOverflow(((UInt4Vector) from).getDataBuffer(), i);
      default -> ((BaseIntVector) from).getValueAsLong(i);
    };
  }

  private static BigDecimal decimal(ValueVector from, int i) {
    return switch (from.getMinorType()) {
      case UINT8 -> new BigDecimal(UInt8Vector.getNoOverflow(((UInt8Vector) from).getDataBuffer(), i));
      case DECIMAL256 -> ((Decimal256Vector) from).getObject(i);
      default -> ((DecimalVector) from).getObject(i);
    };
  }

  private static byte[] bytes(ValueVector from, int i) {
    return switch (from.getMinorType()) {
      case LARGEVARCHAR -> ((LargeVarCharVector) from).get(i);
      case VIEWVARCHAR -> ((ViewVarCharVector) from).get(i);
      case LARGEVARBINARY -> ((LargeVarBinaryVector) from).get(i);
      case VIEWVARBINARY -> ((ViewVarBinaryVector) from).get(i);
      case FIXEDSIZEBINARY -> ((FixedSizeBinaryVector) from).get(i);
      case VARCHAR -> ((VarCharVector) from).get(i);
      default -> ((VarBinaryVector) from).get(i);
    };
  }

  private static final long MILLIS_PER_DAY = 86_400_000L;

  private static int days(ValueVector from, int i, String column) {
    if (from instanceof DateMilliVector milli) {
      long ms = milli.get(i);
      if (Math.floorMod(ms, MILLIS_PER_DAY) != 0) {
        throw new LossyValue(
            "column " + PyRepr.of(column) + ": the date64 value " + ms
                + " is not a whole day, which a date cannot hold");
      }
      return Math.toIntExact(Math.floorDiv(ms, MILLIS_PER_DAY));
    }
    return ((DateDayVector) from).get(i);
  }

  private static void time(ValueVector from, int i, ValueVector to, int j, String column) {
    long value;
    TimeUnit fromUnit = ((ArrowType.Time) from.getField().getType()).getUnit();
    if (from instanceof TimeSecVector v) {
      value = v.get(i);
    } else if (from instanceof TimeMilliVector v) {
      value = v.get(i);
    } else if (from instanceof TimeMicroVector v) {
      value = v.get(i);
    } else {
      value = ((TimeNanoVector) from).get(i);
    }
    TimeUnit toUnit = ((ArrowType.Time) to.getField().getType()).getUnit();
    long out = rescale(value, fromUnit, toUnit, column);
    if (to instanceof TimeSecVector v) {
      v.setSafe(j, Math.toIntExact(out));
    } else if (to instanceof TimeMilliVector v) {
      v.setSafe(j, Math.toIntExact(out));
    } else if (to instanceof TimeMicroVector v) {
      v.setSafe(j, out);
    } else {
      ((TimeNanoVector) to).setSafe(j, out);
    }
  }

  private static int exponent(TimeUnit unit) {
    return switch (unit) {
      case SECOND -> 0;
      case MILLISECOND -> 3;
      case MICROSECOND -> 6;
      case NANOSECOND -> 9;
    };
  }

  /** {@code value} in {@code from} units as {@code to} units, exactly or not at all. */
  static long rescale(long value, TimeUnit from, TimeUnit to, String column) {
    int shift = exponent(to) - exponent(from);
    long factor = 1;
    for (int k = 0; k < Math.abs(shift); k++) {
      factor *= 10;
    }
    if (shift >= 0) {
      try {
        return Math.multiplyExact(value, factor);
      } catch (ArithmeticException e) {
        throw new LossyValue(
            "column " + PyRepr.of(column) + ": the value " + value + " in " + from
                + " overflows as " + to);
      }
    }
    if (value % factor != 0) {
      throw new LossyValue(
          "column " + PyRepr.of(column) + ": the value " + value + " in " + from
              + " would lose its fraction as " + to);
    }
    return value / factor;
  }
}
