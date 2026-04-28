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
        if (type instanceof ArrowType.Timestamp) return Types.TIMESTAMP;
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
            case Types.TIMESTAMP -> "TIMESTAMP";
            case Types.BINARY -> "BINARY";
            default -> "VARCHAR";
        };
    }

    @Override public String getSchemaName(int column) { return ""; }
    @Override public String getTableName(int column) { return ""; }
    @Override public String getCatalogName(int column) { return "provisa"; }
    @Override public int getColumnDisplaySize(int column) { return 256; }
    @Override public int getPrecision(int column) { return 0; }
    @Override public int getScale(int column) { return 0; }
    @Override public boolean isAutoIncrement(int column) { return false; }
    @Override public boolean isCaseSensitive(int column) { return true; }
    @Override public boolean isSearchable(int column) { return true; }
    @Override public boolean isCurrency(int column) { return false; }
    @Override public int isNullable(int column) { return columnNullable; }
    @Override public boolean isSigned(int column) { return true; }
    @Override public boolean isReadOnly(int column) { return true; }
    @Override public boolean isWritable(int column) { return false; }
    @Override public boolean isDefinitelyWritable(int column) { return false; }
    @Override public String getColumnClassName(int column) { return Object.class.getName(); }
    @Override public <T> T unwrap(Class<T> iface) { return null; }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }
}
