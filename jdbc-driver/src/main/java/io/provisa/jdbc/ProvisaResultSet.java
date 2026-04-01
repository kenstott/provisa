package io.provisa.jdbc;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * Provisa JDBC ResultSet.
 *
 * In-memory result set backed by a list of row maps.
 */
public class ProvisaResultSet extends AbstractResultSet {

    private final List<String> columns;
    private final List<Map<String, Object>> rows;
    private int cursor = -1;
    private boolean closed = false;

    ProvisaResultSet(List<String> columns, List<Map<String, Object>> rows) {
        this.columns = columns;
        this.rows = rows;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        cursor++;
        return cursor < rows.size();
    }

    @Override
    public void close() { closed = true; }

    @Override
    public boolean isClosed() { return closed; }

    private void checkClosed() throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
    }

    private Object getValue(int columnIndex) throws SQLException {
        checkClosed();
        if (cursor < 0 || cursor >= rows.size()) throw new SQLException("No current row");
        if (columnIndex < 1 || columnIndex > columns.size()) throw new SQLException("Invalid column index: " + columnIndex);
        return rows.get(cursor).get(columns.get(columnIndex - 1));
    }

    private Object getValue(String columnLabel) throws SQLException {
        checkClosed();
        if (cursor < 0 || cursor >= rows.size()) throw new SQLException("No current row");
        return rows.get(cursor).get(columnLabel);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object v = getValue(columnIndex);
        return v == null ? null : v.toString();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Object v = getValue(columnLabel);
        return v == null ? null : v.toString();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object v = getValue(columnIndex);
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).intValue();
        return Integer.parseInt(v.toString());
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        Object v = getValue(columnLabel);
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).intValue();
        return Integer.parseInt(v.toString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object v = getValue(columnIndex);
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).longValue();
        return Long.parseLong(v.toString());
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        Object v = getValue(columnLabel);
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).longValue();
        return Long.parseLong(v.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object v = getValue(columnIndex);
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).doubleValue();
        return Double.parseDouble(v.toString());
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        Object v = getValue(columnLabel);
        if (v == null) return 0;
        if (v instanceof Number) return ((Number) v).doubleValue();
        return Double.parseDouble(v.toString());
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object v = getValue(columnIndex);
        if (v == null) return null;
        return new BigDecimal(v.toString());
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object v = getValue(columnIndex);
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        return Boolean.parseBoolean(v.toString());
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Object v = getValue(columnLabel);
        if (v == null) return false;
        if (v instanceof Boolean) return (Boolean) v;
        return Boolean.parseBoolean(v.toString());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValue(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getValue(columnLabel);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false; // Simplified — proper impl would track last read
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new ProvisaResultSetMetaData(columns, rows);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int idx = columns.indexOf(columnLabel);
        if (idx < 0) throw new SQLException("Column not found: " + columnLabel);
        return idx + 1;
    }
}
