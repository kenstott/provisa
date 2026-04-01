package io.provisa.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * Abstract ResultSet with default unsupported implementations.
 */
public abstract class AbstractResultSet implements ResultSet {
    @Override public byte getByte(int i) throws SQLException { return (byte) getInt(i); }
    @Override public short getShort(int i) throws SQLException { return (short) getInt(i); }
    @Override public float getFloat(int i) throws SQLException { return (float) getDouble(i); }
    @Override public byte[] getBytes(int i) throws SQLException { String s = getString(i); return s == null ? null : s.getBytes(); }
    @Override public Date getDate(int i) throws SQLException { String s = getString(i); return s == null ? null : Date.valueOf(s); }
    @Override public Time getTime(int i) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(int i) throws SQLException { String s = getString(i); return s == null ? null : Timestamp.valueOf(s); }
    @Override public InputStream getAsciiStream(int i) throws SQLException { return null; }
    @Override public InputStream getUnicodeStream(int i) throws SQLException { return null; }
    @Override public InputStream getBinaryStream(int i) throws SQLException { return null; }
    @Override public byte getByte(String s) throws SQLException { return (byte) getInt(s); }
    @Override public short getShort(String s) throws SQLException { return (short) getInt(s); }
    @Override public float getFloat(String s) throws SQLException { return (float) getDouble(s); }
    @Override public byte[] getBytes(String s) throws SQLException { String v = getString(s); return v == null ? null : v.getBytes(); }
    @Override public Date getDate(String s) throws SQLException { String v = getString(s); return v == null ? null : Date.valueOf(v); }
    @Override public Time getTime(String s) throws SQLException { return null; }
    @Override public Timestamp getTimestamp(String s) throws SQLException { String v = getString(s); return v == null ? null : Timestamp.valueOf(v); }
    @Override public InputStream getAsciiStream(String s) throws SQLException { return null; }
    @Override public InputStream getUnicodeStream(String s) throws SQLException { return null; }
    @Override public InputStream getBinaryStream(String s) throws SQLException { return null; }
    @Override public SQLWarning getWarnings() { return null; }
    @Override public void clearWarnings() {}
    @Override public String getCursorName() { return null; }
    @Override public Reader getCharacterStream(int i) throws SQLException { return null; }
    @Override public Reader getCharacterStream(String s) throws SQLException { return null; }
    @Override public BigDecimal getBigDecimal(String s) throws SQLException { Object v = getObject(s); return v == null ? null : new BigDecimal(v.toString()); }
    @Override public BigDecimal getBigDecimal(int i, int scale) throws SQLException { return getBigDecimal(i); }
    @Override public BigDecimal getBigDecimal(String s, int scale) throws SQLException { return getBigDecimal(s); }
    @Override public boolean isBeforeFirst() { return false; }
    @Override public boolean isAfterLast() { return false; }
    @Override public boolean isFirst() { return false; }
    @Override public boolean isLast() { return false; }
    @Override public void beforeFirst() {}
    @Override public void afterLast() {}
    @Override public boolean first() { return false; }
    @Override public boolean last() { return false; }
    @Override public int getRow() { return 0; }
    @Override public boolean absolute(int row) { return false; }
    @Override public boolean relative(int rows) { return false; }
    @Override public boolean previous() { return false; }
    @Override public void setFetchDirection(int direction) {}
    @Override public int getFetchDirection() { return FETCH_FORWARD; }
    @Override public void setFetchSize(int rows) {}
    @Override public int getFetchSize() { return 0; }
    @Override public int getType() { return TYPE_FORWARD_ONLY; }
    @Override public int getConcurrency() { return CONCUR_READ_ONLY; }
    @Override public boolean rowUpdated() { return false; }
    @Override public boolean rowInserted() { return false; }
    @Override public boolean rowDeleted() { return false; }
    @Override public void updateNull(int i) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(int i, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(int i, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(int i, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(int i, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(int i, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(int i, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(int i, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(int i, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(int i, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(int i, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(int i, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(int i, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(int i, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int i, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int i, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int i, Reader x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int i, Object x, int scaleOrLength) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(int i, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNull(String s) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBoolean(String s, boolean x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateByte(String s, byte x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateShort(String s, short x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateInt(String s, int x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateLong(String s, long x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateFloat(String s, float x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDouble(String s, double x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBigDecimal(String s, BigDecimal x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateString(String s, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBytes(String s, byte[] x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateDate(String s, Date x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTime(String s, Time x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateTimestamp(String s, Timestamp x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String s, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String s, InputStream x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String s, Reader x, int length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String s, Object x, int scaleOrLength) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateObject(String s, Object x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void insertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void deleteRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void refreshRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void cancelRowUpdates() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void moveToInsertRow() throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void moveToCurrentRow() {}
    @Override public Statement getStatement() { return null; }
    @Override public Object getObject(int i, Map<String, Class<?>> map) throws SQLException { return getObject(i); }
    @Override public Ref getRef(int i) throws SQLException { return null; }
    @Override public Blob getBlob(int i) throws SQLException { return null; }
    @Override public Clob getClob(int i) throws SQLException { return null; }
    @Override public Array getArray(int i) throws SQLException { return null; }
    @Override public Object getObject(String s, Map<String, Class<?>> map) throws SQLException { return getObject(s); }
    @Override public Ref getRef(String s) throws SQLException { return null; }
    @Override public Blob getBlob(String s) throws SQLException { return null; }
    @Override public Clob getClob(String s) throws SQLException { return null; }
    @Override public Array getArray(String s) throws SQLException { return null; }
    @Override public Date getDate(int i, Calendar cal) throws SQLException { return getDate(i); }
    @Override public Date getDate(String s, Calendar cal) throws SQLException { return getDate(s); }
    @Override public Time getTime(int i, Calendar cal) throws SQLException { return getTime(i); }
    @Override public Time getTime(String s, Calendar cal) throws SQLException { return getTime(s); }
    @Override public Timestamp getTimestamp(int i, Calendar cal) throws SQLException { return getTimestamp(i); }
    @Override public Timestamp getTimestamp(String s, Calendar cal) throws SQLException { return getTimestamp(s); }
    @Override public URL getURL(int i) throws SQLException { return null; }
    @Override public URL getURL(String s) throws SQLException { return null; }
    @Override public void updateRef(int i, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRef(String s, Ref x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int i, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String s, Blob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int i, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String s, Clob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(int i, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateArray(String s, Array x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public RowId getRowId(int i) throws SQLException { return null; }
    @Override public RowId getRowId(String s) throws SQLException { return null; }
    @Override public void updateRowId(int i, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateRowId(String s, RowId x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public int getHoldability() { return HOLD_CURSORS_OVER_COMMIT; }
    @Override public void updateNString(int i, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNString(String s, String x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int i, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String s, NClob x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public NClob getNClob(int i) throws SQLException { return null; }
    @Override public NClob getNClob(String s) throws SQLException { return null; }
    @Override public SQLXML getSQLXML(int i) throws SQLException { return null; }
    @Override public SQLXML getSQLXML(String s) throws SQLException { return null; }
    @Override public void updateSQLXML(int i, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateSQLXML(String s, SQLXML x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public String getNString(int i) throws SQLException { return getString(i); }
    @Override public String getNString(String s) throws SQLException { return getString(s); }
    @Override public Reader getNCharacterStream(int i) throws SQLException { return null; }
    @Override public Reader getNCharacterStream(String s) throws SQLException { return null; }
    @Override public void updateNCharacterStream(int i, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(String s, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int i, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int i, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int i, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String s, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String s, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String s, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int i, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String s, InputStream x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int i, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String s, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int i, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String s, Reader x, long length) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNCharacterStream(String s, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateAsciiStream(String s, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBinaryStream(String s, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateCharacterStream(String s, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(int i, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateBlob(String s, InputStream x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateClob(String s, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(int i, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public void updateNClob(String s, Reader x) throws SQLException { throw new SQLFeatureNotSupportedException(); }
    @Override public <T> T getObject(int i, Class<T> type) throws SQLException { return type.cast(getObject(i)); }
    @Override public <T> T getObject(String s, Class<T> type) throws SQLException { return type.cast(getObject(s)); }
    @Override public <T> T unwrap(Class<T> iface) { return null; }
    @Override public boolean isWrapperFor(Class<?> iface) { return false; }
}
