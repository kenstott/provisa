package io.provisa.jdbc;

import java.sql.*;
import java.util.*;

/**
 * ResultSet metadata — infers column types from the data.
 */
public class ProvisaResultSetMetaData implements ResultSetMetaData {

    private final List<String> columns;
    private final List<Map<String, Object>> rows;

    ProvisaResultSetMetaData(List<String> columns, List<Map<String, Object>> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    @Override public int getColumnCount() { return columns.size(); }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columns.get(column - 1);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        // Infer from first non-null value
        String col = columns.get(column - 1);
        for (Map<String, Object> row : rows) {
            Object v = row.get(col);
            if (v == null) continue;
            if (v instanceof Boolean) return Types.BOOLEAN;
            if (v instanceof Integer) return Types.INTEGER;
            if (v instanceof Long) return Types.BIGINT;
            if (v instanceof Double || v instanceof Float) return Types.DOUBLE;
            if (v instanceof Number) return Types.NUMERIC;
            return Types.VARCHAR;
        }
        return Types.VARCHAR;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int type = getColumnType(column);
        return switch (type) {
            case Types.BOOLEAN -> "BOOLEAN";
            case Types.INTEGER -> "INTEGER";
            case Types.BIGINT -> "BIGINT";
            case Types.DOUBLE -> "DOUBLE";
            case Types.NUMERIC -> "NUMERIC";
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
