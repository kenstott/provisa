package io.provisa.jdbc;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * A list value from an Arrow Flight stream as a {@link java.sql.Array}.
 *
 * <p>Elements are handed over as plain Java values: text as {@code String}, numbers and booleans
 * as their boxed types, a nested list as an {@code Object[]}, and a nested struct or map as its
 * JSON text — never as Arrow's own collection or text classes.
 */
final class ArrowArray implements Array {

    private final Object[] elements;
    private final ArrowType elementType;

    ArrowArray(List<?> values, ArrowType elementType) {
        this.elements = plain(values);
        this.elementType = elementType;
    }

    private static Object[] plain(List<?> values) {
        Object[] out = new Object[values.size()];
        for (int i = 0; i < out.length; i++) {
            Object v = values.get(i);
            if (v instanceof Text) out[i] = v.toString();
            else if (v instanceof Map) out[i] = v.toString(); // Arrow renders a struct as JSON
            else if (v instanceof List<?> nested) out[i] = plain(nested);
            else out[i] = v;
        }
        return out;
    }

    @Override
    public String getBaseTypeName() {
        return switch (getBaseType()) {
            case Types.VARCHAR -> "VARCHAR";
            case Types.INTEGER -> "INTEGER";
            case Types.BIGINT -> "BIGINT";
            case Types.DOUBLE -> "DOUBLE";
            case Types.BOOLEAN -> "BOOLEAN";
            case Types.DECIMAL -> "DECIMAL";
            case Types.ARRAY -> "ARRAY";
            default -> "OTHER";
        };
    }

    @Override
    public int getBaseType() {
        if (elementType instanceof ArrowType.Utf8 || elementType instanceof ArrowType.LargeUtf8
                || elementType instanceof ArrowType.Struct || elementType instanceof ArrowType.Map) {
            return Types.VARCHAR;
        }
        if (elementType instanceof ArrowType.Int i) return i.getBitWidth() <= 32 ? Types.INTEGER : Types.BIGINT;
        if (elementType instanceof ArrowType.FloatingPoint) return Types.DOUBLE;
        if (elementType instanceof ArrowType.Bool) return Types.BOOLEAN;
        if (elementType instanceof ArrowType.Decimal) return Types.DECIMAL;
        if (elementType instanceof ArrowType.List || elementType instanceof ArrowType.LargeList
                || elementType instanceof ArrowType.FixedSizeList) {
            return Types.ARRAY;
        }
        return Types.OTHER;
    }

    @Override
    public Object getArray() {
        return elements.clone();
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        if (index < 1 || count < 0 || index - 1 + count > elements.length) {
            throw new SQLException(
                "Array slice out of range: index " + index + ", count " + count + ", length " + elements.length);
        }
        Object[] slice = new Object[count];
        System.arraycopy(elements, (int) index - 1, slice, 0, count);
        return slice;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getArray(index, count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLFeatureNotSupportedException("An array is read with getArray()");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }

    @Override
    public void free() {
        // The elements are plain Java values: nothing is held.
    }

    /** The elements as JSON-like text, which is what getString() answers for a list column. */
    @Override
    public String toString() {
        return java.util.Arrays.deepToString(elements);
    }
}
