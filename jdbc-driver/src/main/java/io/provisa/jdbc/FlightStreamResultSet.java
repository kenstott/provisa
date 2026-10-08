package io.provisa.jdbc;

import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * JDBC ResultSet backed by an Arrow Flight stream.
 *
 * <p>Reads record batches lazily from a FlightStream; memory is bounded to one batch at a time.
 * Every value is handed over as the JDBC type for its column ({@link #jdbcValue}): text as
 * {@code String}, dates as {@code java.sql.Date}, timestamps as {@code java.sql.Timestamp},
 * decimals as {@code BigDecimal}; a SQL NULL is {@code null} from the object getters and the
 * JDBC zero value from the primitive ones, with {@link #wasNull()} telling them apart.
 */
public class FlightStreamResultSet extends AbstractResultSet {

    private final FlightStream stream;
    private final Schema schema;
    private final List<String> columnNames;
    // REQ-690: columns flagged encrypted via Arrow field metadata (provisa_encrypted=true),
    // decrypted client-side by the connection's decryptor.
    private final Set<String> encryptedColumns = new HashSet<>();
    private final EnvelopeDecryptor decryptor;

    private VectorSchemaRoot currentBatch;
    private int rowInBatch = -1;
    private int batchRowCount = 0;
    private boolean finished = false;
    private boolean closed = false;
    private boolean lastWasNull = false;

    /**
     * @throws FlightRuntimeException when the server answered the ticket with an error: the
     *         schema is the first thing a stream delivers, so a refusal surfaces here
     */
    FlightStreamResultSet(FlightStream stream, EnvelopeDecryptor decryptor) {
        this.stream = stream;
        this.decryptor = decryptor;
        this.schema = stream.getSchema();
        this.columnNames = new ArrayList<>();
        for (Field field : schema.getFields()) {
            columnNames.add(field.getName());
            Map<String, String> meta = field.getMetadata();
            if (meta != null && "true".equals(meta.get("provisa_encrypted"))) {
                encryptedColumns.add(field.getName());
            }
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (closed || finished) return false;
        rowInBatch++;
        if (rowInBatch < batchRowCount) {
            return true;
        }
        // Load the next batch that has rows; a stream may carry empty batches.
        try {
            while (stream.next()) {
                currentBatch = stream.getRoot();
                batchRowCount = currentBatch.getRowCount();
                rowInBatch = 0;
                if (batchRowCount > 0) return true;
            }
            finished = true;
            return false;
        } catch (FlightRuntimeException e) {
            throw FlightTransport.refusal("Query failed while streaming", e);
        }
    }

    // ── Values ──

    private int index(String columnLabel) throws SQLException {
        int idx = columnNames.indexOf(columnLabel);
        if (idx < 0) throw new SQLException("Column not found: " + columnLabel);
        return idx + 1;
    }

    /** The current row's value in a column as its JDBC type, or null for SQL NULL. */
    private Object value(int columnIndex) throws SQLException {
        if (closed) throw new SQLException("ResultSet is closed");
        if (currentBatch == null || finished) throw new SQLException("No current row");
        if (columnIndex < 1 || columnIndex > columnNames.size()) {
            throw new SQLException("Invalid column index: " + columnIndex);
        }
        FieldVector vector = currentBatch.getVector(columnIndex - 1);
        Object value = jdbcValue(vector, rowInBatch);
        lastWasNull = value == null;
        String column = columnNames.get(columnIndex - 1);
        if (value != null && encryptedColumns.contains(column)) {
            if (decryptor == null) {
                throw new DecryptionException(
                    "column '" + column + "' is encrypted but no kms_provider/kms_key_arn "
                    + "was configured on this connection (REQ-690)");
            }
            return decryptor.decryptField(value.toString());
        }
        return value;
    }

    /**
     * One Arrow value as the JDBC type of its column. Arrow's own {@code getObject} answers with
     * Arrow's types (a {@code Text}, a day count, a {@code LocalDateTime}, a raw epoch); JDBC
     * callers expect {@code String}, {@code java.sql.Date}, {@code java.sql.Timestamp} and
     * {@code java.sql.Time}.
     */
    static Object jdbcValue(FieldVector vector, int row) {
        if (vector.isNull(row)) return null;
        if (vector instanceof DateDayVector days) {
            return Date.valueOf(LocalDate.ofEpochDay(days.get(row)));
        }
        if (vector instanceof DateMilliVector millis) {
            return Date.valueOf(
                LocalDateTime.ofEpochSecond(Math.floorDiv(millis.get(row), 1000L), 0, ZoneOffset.UTC)
                    .toLocalDate());
        }
        if (vector instanceof TimeStampVector stamps) {
            ArrowType.Timestamp type = (ArrowType.Timestamp) vector.getField().getType();
            Instant instant = instant(stamps.get(row), type.getUnit());
            if (type.getTimezone() == null) {
                // A zoneless timestamp is a wall-clock reading: the same fields, whatever the
                // JVM's zone.
                return Timestamp.valueOf(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
            }
            return Timestamp.from(instant);
        }
        if (vector instanceof TimeSecVector seconds) {
            return Time.valueOf(LocalTime.ofSecondOfDay(seconds.get(row)));
        }
        if (vector instanceof TimeMilliVector millis) {
            return Time.valueOf(LocalTime.ofNanoOfDay(millis.get(row) * 1_000_000L));
        }
        if (vector instanceof TimeMicroVector micros) {
            return Time.valueOf(LocalTime.ofNanoOfDay(micros.get(row) * 1_000L));
        }
        if (vector instanceof TimeNanoVector nanos) {
            return Time.valueOf(LocalTime.ofNanoOfDay(nanos.get(row)));
        }
        Object value = vector.getObject(row);
        return value instanceof Text ? value.toString() : value;
    }

    private static Instant instant(long epoch, TimeUnit unit) {
        return switch (unit) {
            case SECOND -> Instant.ofEpochSecond(epoch);
            case MILLISECOND -> Instant.ofEpochMilli(epoch);
            case MICROSECOND -> Instant.ofEpochSecond(
                Math.floorDiv(epoch, 1_000_000L), Math.floorMod(epoch, 1_000_000L) * 1_000L);
            case NANOSECOND -> Instant.ofEpochSecond(
                Math.floorDiv(epoch, 1_000_000_000L), Math.floorMod(epoch, 1_000_000_000L));
        };
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return value(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return value(index(columnLabel));
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return null;
        if (v instanceof BigDecimal decimal) return decimal.toPlainString();
        if (v instanceof byte[] bytes) return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        return v.toString();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(index(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return 0;
        return v instanceof Number n ? n.intValue() : Integer.parseInt(v.toString());
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(index(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return 0;
        return v instanceof Number n ? n.longValue() : Long.parseLong(v.toString());
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(index(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return 0;
        return v instanceof Number n ? n.doubleValue() : Double.parseDouble(v.toString());
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(index(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return null;
        return v instanceof BigDecimal decimal ? decimal : new BigDecimal(v.toString());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(index(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return false;
        return v instanceof Boolean b ? b : Boolean.parseBoolean(v.toString());
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(index(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return null;
        if (v instanceof Date date) return date;
        if (v instanceof Timestamp stamp) return Date.valueOf(stamp.toLocalDateTime().toLocalDate());
        return Date.valueOf(v.toString());
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(index(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return null;
        if (v instanceof Timestamp stamp) return stamp;
        if (v instanceof Date date) return Timestamp.valueOf(date.toLocalDate().atStartOfDay());
        return Timestamp.valueOf(v.toString());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(index(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return null;
        if (v instanceof Time time) return time;
        if (v instanceof Timestamp stamp) return Time.valueOf(stamp.toLocalDateTime().toLocalTime());
        return Time.valueOf(v.toString());
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(index(columnLabel));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object v = value(columnIndex);
        if (v == null) return null;
        return v instanceof byte[] bytes
            ? bytes
            : v.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(index(columnLabel));
    }

    @Override
    public boolean wasNull() {
        return lastWasNull;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return new ArrowResultSetMetaData(schema, columnNames);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return index(columnLabel);
    }

    @Override
    public void close() throws SQLException {
        if (closed) return;
        closed = true;
        try {
            stream.close();
        } catch (FlightRuntimeException e) {
            throw FlightTransport.refusal("Closing the Flight stream failed", e);
        } catch (Exception e) {
            throw new SQLException("Closing the Flight stream failed: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
