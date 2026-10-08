package io.provisa.jdbc;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.sql.*;
import java.util.List;

/**
 * ResultSet metadata derived from Arrow schema — typed from the source.
 */
public class ArrowResultSetMetaData implements ResultSetMetaData {

    private final Schema schema;
    private final List<String> columnNames;

    ArrowResultSetMetaData(Schema schema, List<String> columnNames) {
        this.schema = schema;
        this.columnNames = columnNames;
    }

    @Override public int getColumnCount() { return columnNames.size(); }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnNames.get(column - 1);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        Field field = schema.getFields().get(column - 1);
        ArrowType type = field.getType();

        if (type instanceof ArrowType.Utf8) return Types.VARCHAR;
        if (type instanceof ArrowType.Int) {
            int bitWidth = ((ArrowType.Int) type).getBitWidth();
            if (bitWidth <= 32) return Types.INTEGER;
            return Types.BIGINT;
        }
        if (type instanceof ArrowType.FloatingPoint) {
            var fp = (ArrowType.FloatingPoint) type;
            if (fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)
                return Types.FLOAT;
            return Types.DOUBLE;
        }
        if (type instanceof ArrowType.Bool) return Types.BOOLEAN;
        if (type instanceof ArrowType.Decimal) return Types.DECIMAL;
        if (type instanceof ArrowType.Date) return Types.DATE;
        if (type instanceof ArrowType.Time) return Types.TIME;
        if (type instanceof ArrowType.Timestamp ts) {
            return ts.getTimezone() == null ? Types.TIMESTAMP : Types.TIMESTAMP_WITH_TIMEZONE;
        }
        if (type instanceof ArrowType.LargeUtf8) return Types.VARCHAR;
        if (type instanceof ArrowType.Map || type instanceof ArrowType.Struct) return Types.VARCHAR;
        if (type instanceof ArrowType.List || type instanceof ArrowType.LargeList
                || type instanceof ArrowType.FixedSizeList) return Types.ARRAY;
        if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary) return Types.BINARY;

        return Types.VARCHAR;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int type = getColumnType(column);
        return switch (type) {
            case Types.VARCHAR -> "VARCHAR";
            case Types.INTEGER -> "INTEGER";
            case Types.BIGINT -> "BIGINT";
            case Types.FLOAT -> "FLOAT";
            case Types.DOUBLE -> "DOUBLE";
            case Types.BOOLEAN -> "BOOLEAN";
            case Types.DECIMAL -> "DECIMAL";
            case Types.DATE -> "DATE";
            case Types.TIME -> "TIME";
            case Types.TIMESTAMP -> "TIMESTAMP";
            case Types.TIMESTAMP_WITH_TIMEZONE -> "TIMESTAMP WITH TIME ZONE";
            case Types.BINARY -> "BINARY";
            case Types.ARRAY -> "ARRAY";
            default -> "VARCHAR";
        };
    }

    @Override public String getSchemaName(int column) { return ""; }
    @Override public String getTableName(int column) { return ""; }
    @Override public String getCatalogName(int column) { return "provisa"; }
    @Override public int getColumnDisplaySize(int column) { return 256; }
    @Override
    public int getPrecision(int column) {
        return schema.getFields().get(column - 1).getType() instanceof ArrowType.Decimal d
            ? d.getPrecision() : 0;
    }

    @Override
    public int getScale(int column) {
        return schema.getFields().get(column - 1).getType() instanceof ArrowType.Decimal d
            ? d.getScale() : 0;
    }
    @Override public boolean isAutoIncrement(int column) { return false; }
    @Override public boolean isCaseSensitive(int column) { return true; }
    @Override public boolean isSearchable(int column) { return true; }
    @Override public boolean isCurrency(int column) { return false; }
    @Override
    public int isNullable(int column) {
        return schema.getFields().get(column - 1).isNullable() ? columnNullable : columnNoNulls;
    }
    @Override public boolean isSigned(int column) { return true; }
    @Override public boolean isReadOnly(int column) { return true; }
    @Override public boolean isWritable(int column) { return false; }
    @Override public boolean isDefinitelyWritable(int column) { return false; }
    /** The class {@code getObject} answers with for the column (FlightStreamResultSet.jdbcValue). */
    @Override
    public String getColumnClassName(int column) throws SQLException {
        Field field = schema.getFields().get(column - 1);
        return switch (getColumnType(column)) {
            case Types.VARCHAR -> field.getType() instanceof ArrowType.Utf8
                    || field.getType() instanceof ArrowType.LargeUtf8
                    || field.getType() instanceof ArrowType.Struct
                    || field.getType() instanceof ArrowType.Map
                ? String.class.getName() : Object.class.getName();
            case Types.ARRAY -> java.sql.Array.class.getName();
            case Types.INTEGER -> switch (((ArrowType.Int) field.getType()).getBitWidth()) {
                case 8 -> Byte.class.getName();
                case 16 -> Short.class.getName();
                default -> Integer.class.getName();
            };
            case Types.BIGINT -> Long.class.getName();
            case Types.FLOAT -> Float.class.getName();
            case Types.DOUBLE -> Double.class.getName();
            case Types.BOOLEAN -> Boolean.class.getName();
            case Types.DECIMAL -> java.math.BigDecimal.class.getName();
            case Types.DATE -> java.sql.Date.class.getName();
            case Types.TIME -> java.sql.Time.class.getName();
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> java.sql.Timestamp.class.getName();
            case Types.BINARY -> byte[].class.getName();
            default -> Object.class.getName();
        };
    }
    @Override public <T> T unwrap(Class<T> iface) { return null; }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }
}
