package io.provisa.jdbc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * Streaming Arrow IPC ResultSet.
 *
 * Reads record batches lazily from an Arrow IPC stream. Memory usage is
 * bounded to one batch at a time (typically 1K-10K rows).
 */
public class ArrowStreamResultSet extends AbstractResultSet {

    private final InputStream inputStream;
    private final BufferAllocator allocator;
    private final ArrowStreamReader reader;
    private final Schema schema;
    private final List<String> columnNames;

    private VectorSchemaRoot currentBatch;
    private int rowInBatch = -1;
    private int batchRowCount = 0;
    private boolean finished = false;
    private boolean closed = false;

    // REQ-690: columns flagged encrypted via Arrow field metadata (provisa_encrypted=true),
    // decrypted client-side by the connection's EncryptionService.
    private final Set<String> encryptedColumns = new HashSet<>();
    private EnvelopeDecryptor encryptionService;

    ArrowStreamResultSet(InputStream stream) throws SQLException {
        this(stream, null);
    }

    ArrowStreamResultSet(InputStream stream, EnvelopeDecryptor encryptionService) throws SQLException {
        this.inputStream = stream;
        this.encryptionService = encryptionService;
        this.allocator = new RootAllocator();
        try {
            this.reader = new ArrowStreamReader(stream, allocator);
            this.schema = reader.getVectorSchemaRoot().getSchema();
            this.columnNames = new ArrayList<>();
            for (Field field : schema.getFields()) {
                columnNames.add(field.getName());
                Map<String, String> meta = field.getMetadata();
                if (meta != null && "true".equals(meta.get("provisa_encrypted"))) {
                    encryptedColumns.add(field.getName());
                }
            }
            this.currentBatch = reader.getVectorSchemaRoot();
        } catch (Exception e) {
            allocator.close();
            throw new SQLException("Failed to open Arrow stream: " + e.getMessage(), e);
        }
    }

    private String maybeDecrypt(String column, String raw) throws SQLException {
        if (raw == null || !encryptedColumns.contains(column)) {
            return raw;
        }
        if (encryptionService == null) {
            throw new DecryptionException(
                "column '" + column + "' is encrypted but no kms_provider/kms_key_arn "
                + "was configured on this connection (REQ-690)");
        }
        return encryptionService.decryptField(raw);
    }

    @Override
    public boolean next() throws SQLException {
        if (closed || finished) return false;

        rowInBatch++;

        // Still within current batch
        if (rowInBatch < batchRowCount) {
            return true;
        }

        // Load next batch
        try {
            if (reader.loadNextBatch()) {
                batchRowCount = currentBatch.getRowCount();
                rowInBatch = 0;
                return batchRowCount > 0;
            } else {
                finished = true;
                return false;
            }
        } catch (Exception e) {
            throw new SQLException("Error reading Arrow batch: " + e.getMessage(), e);
        }
    }

    private FieldVector getVector(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.size()) {
            throw new SQLException("Invalid column index: " + columnIndex);
        }
        return currentBatch.getVector(columnIndex - 1);
    }

    private FieldVector getVector(String columnLabel) throws SQLException {
        int idx = columnNames.indexOf(columnLabel);
        if (idx < 0) throw new SQLException("Column not found: " + columnLabel);
        return currentBatch.getVector(idx);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        FieldVector vec = getVector(columnIndex);
        if (vec.isNull(rowInBatch)) return null;
        return maybeDecrypt(columnNames.get(columnIndex - 1), vec.getObject(rowInBatch).toString());
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        FieldVector vec = getVector(columnLabel);
        if (vec.isNull(rowInBatch)) return null;
        return maybeDecrypt(columnLabel, vec.getObject(rowInBatch).toString());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        FieldVector vec = getVector(columnIndex);
        if (vec.isNull(rowInBatch)) return 0;
        Object val = vec.getObject(rowInBatch);
        if (val instanceof Number) return ((Number) val).intValue();
        return Integer.parseInt(val.toString());
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        FieldVector vec = getVector(columnLabel);
        if (vec.isNull(rowInBatch)) return 0;
        Object val = vec.getObject(rowInBatch);
        if (val instanceof Number) return ((Number) val).intValue();
        return Integer.parseInt(val.toString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        FieldVector vec = getVector(columnIndex);
        if (vec.isNull(rowInBatch)) return 0;
        Object val = vec.getObject(rowInBatch);
        if (val instanceof Number) return ((Number) val).longValue();
        return Long.parseLong(val.toString());
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        FieldVector vec = getVector(columnLabel);
        if (vec.isNull(rowInBatch)) return 0;
        Object val = vec.getObject(rowInBatch);
        if (val instanceof Number) return ((Number) val).longValue();
        return Long.parseLong(val.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        FieldVector vec = getVector(columnIndex);
        if (vec.isNull(rowInBatch)) return 0;
        Object val = vec.getObject(rowInBatch);
        if (val instanceof Number) return ((Number) val).doubleValue();
        return Double.parseDouble(val.toString());
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        FieldVector vec = getVector(columnLabel);
        if (vec.isNull(rowInBatch)) return 0;
        Object val = vec.getObject(rowInBatch);
        if (val instanceof Number) return ((Number) val).doubleValue();
        return Double.parseDouble(val.toString());
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        FieldVector vec = getVector(columnIndex);
        if (vec.isNull(rowInBatch)) return null;
        return new BigDecimal(vec.getObject(rowInBatch).toString());
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        FieldVector vec = getVector(columnIndex);
        if (vec.isNull(rowInBatch)) return false;
        Object val = vec.getObject(rowInBatch);
        if (val instanceof Boolean) return (Boolean) val;
        return Boolean.parseBoolean(val.toString());
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        FieldVector vec = getVector(columnLabel);
        if (vec.isNull(rowInBatch)) return false;
        Object val = vec.getObject(rowInBatch);
        if (val instanceof Boolean) return (Boolean) val;
        return Boolean.parseBoolean(val.toString());
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        FieldVector vec = getVector(columnIndex);
        if (vec.isNull(rowInBatch)) return null;
        String col = columnNames.get(columnIndex - 1);
        if (encryptedColumns.contains(col)) {
            return maybeDecrypt(col, vec.getObject(rowInBatch).toString());
        }
        return vec.getObject(rowInBatch);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        FieldVector vec = getVector(columnLabel);
        if (vec.isNull(rowInBatch)) return null;
        if (encryptedColumns.contains(columnLabel)) {
            return maybeDecrypt(columnLabel, vec.getObject(rowInBatch).toString());
        }
        return vec.getObject(rowInBatch);
    }

    @Override
    public boolean wasNull() {
        return false; // Simplified
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return new ArrowResultSetMetaData(schema, columnNames);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        int idx = columnNames.indexOf(columnLabel);
        if (idx < 0) throw new SQLException("Column not found: " + columnLabel);
        return idx + 1;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try { reader.close(); } catch (Exception ignored) {}
        allocator.close();
        try { inputStream.close(); } catch (Exception ignored) {}
    }

    @Override
    public boolean isClosed() { return closed; }
}
