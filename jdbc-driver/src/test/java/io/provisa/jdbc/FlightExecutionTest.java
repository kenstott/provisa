package io.provisa.jdbc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Queries run over Arrow Flight (REQ-293): the ticket carries the credential the server reads, a
 * refusal is raised with the server's message and never retried over HTTP, HTTP is used only when
 * the Flight port was unreachable at connect, and every value comes back as its JDBC type.
 *
 * <p>Both servers are stubs inside this JVM: a Flight server whose producer answers a ticket as
 * the Provisa Flight server does (a typed stream, or an error status), and an HTTP server
 * answering {@code /auth/login} and {@code /data/sql}.
 */
class FlightExecutionTest {

    private static final LocalDateTime WALL_CLOCK = LocalDateTime.of(2026, 3, 4, 5, 6, 7, 890_000_000);
    private static final Instant INSTANT = Instant.parse("2026-03-04T05:06:07.890Z");

    private BufferAllocator allocator;
    private FlightServer flight;
    private HttpServer http;
    private final List<JsonObject> tickets = new ArrayList<>();
    private final List<String> httpPaths = new ArrayList<>();
    /** What the Flight producer answers the next ticket with; null streams the typed rows. */
    private CallStatus flightError;
    private boolean emptyResult;
    /** What the Flight producer answers the connect probe with; null answers it. */
    private CallStatus probeError;

    @BeforeEach
    void start() throws IOException {
        allocator = new RootAllocator();
        flight = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), new NoOpFlightProducer() {
            @Override
            public void listActions(CallContext context, StreamListener<org.apache.arrow.flight.ActionType> listener) {
                // The connect probe: the Provisa server answers it without a credential.
                if (probeError != null) {
                    listener.onError(probeError.toRuntimeException());
                    return;
                }
                listener.onCompleted();
            }

            @Override
            public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                tickets.add(JsonParser.parseString(new String(ticket.getBytes(), StandardCharsets.UTF_8))
                    .getAsJsonObject());
                if (flightError != null) {
                    listener.error(flightError.toRuntimeException());
                    return;
                }
                try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
                    listener.start(root);
                    if (!emptyResult) {
                        fill(root);
                        listener.putNext();
                    }
                    listener.completed();
                }
            }
        }).build();
        flight.start();

        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/", exchange -> {
            httpPaths.add(exchange.getRequestURI().getPath());
            exchange.getRequestBody().readAllBytes();
            String body = exchange.getRequestURI().getPath().equals("/auth/login")
                ? "{\"access_token\": \"tok-1\", \"token_type\": \"bearer\"}"
                : "{\"data\": {\"sql\": [{\"id\": 7}]}}";
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        http.start();
    }

    @AfterEach
    void stop() throws Exception {
        http.stop(0);
        flight.close();
        allocator.close();
    }

    // ── the rows the Flight stub streams: one of every type, then a row of NULLs ──

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private static final Schema SCHEMA = new Schema(List.of(
        new Field("id", FieldType.notNullable(new ArrowType.Int(32, true)), null),
        field("big", new ArrowType.Int(64, true)),
        field("ratio", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
        field("amount", new ArrowType.Decimal(12, 2, 128)),
        field("name", new ArrowType.Utf8()),
        field("active", new ArrowType.Bool()),
        field("day", new ArrowType.Date(DateUnit.DAY)),
        field("seen", new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
        field("seen_utc", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "UTC")),
        field("blob", new ArrowType.Binary())));

    private static void fill(VectorSchemaRoot root) {
        root.allocateNew();
        ((IntVector) root.getVector("id")).setSafe(0, 7);
        ((BigIntVector) root.getVector("big")).setSafe(0, 9_000_000_000L);
        ((Float8Vector) root.getVector("ratio")).setSafe(0, 0.25);
        ((DecimalVector) root.getVector("amount")).setSafe(0, new BigDecimal("1234.50"));
        ((VarCharVector) root.getVector("name")).setSafe(0, "Ada".getBytes(StandardCharsets.UTF_8));
        ((BitVector) root.getVector("active")).setSafe(0, 1);
        ((DateDayVector) root.getVector("day")).setSafe(0, (int) LocalDate.of(2026, 3, 4).toEpochDay());
        long micros = INSTANT.getEpochSecond() * 1_000_000L + INSTANT.getNano() / 1_000L;
        ((TimeStampMicroVector) root.getVector("seen")).setSafe(0, micros);
        ((TimeStampMicroTZVector) root.getVector("seen_utc")).setSafe(0, micros);
        ((VarBinaryVector) root.getVector("blob")).setSafe(0, new byte[]{1, 2, 3});
        // Row 1: NULL everywhere a NULL is allowed.
        ((IntVector) root.getVector("id")).setSafe(1, 8);
        for (String column : List.of("big", "ratio", "amount", "name", "active", "day", "seen", "seen_utc", "blob")) {
            root.getVector(column).setNull(1);
        }
        root.setRowCount(2);
    }

    private Connection connect(String role, boolean flightReachable) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "alice");
        props.setProperty("password", "secret");
        if (role != null) props.setProperty("role", role);
        // A port nothing listens on, when the test is about an unreachable Flight port.
        props.setProperty("flight_port", String.valueOf(flightReachable ? flight.getPort() : 1));
        return new ProvisaDriver().connect("jdbc:provisa://127.0.0.1:" + http.getAddress().getPort(), props);
    }

    // ── transport selection and the credential ──

    @Test
    void aQueryRunsOverFlightWithTheSessionTokenInItsTicket() throws SQLException {
        try (Connection conn = connect(null, true);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM sales.orders")) {
            assertInstanceOf(FlightStreamResultSet.class, rs);
            assertTrue(rs.next());
        }
        JsonObject ticket = tickets.get(0);
        assertEquals("SELECT * FROM sales.orders", ticket.get("query").getAsString());
        assertEquals("tok-1", ticket.get("token").getAsString());
        assertFalse(ticket.has("role"), "no role is requested unless the connection asks for one");
        assertEquals(List.of("/auth/login"), httpPaths, "the query did not go over HTTP");
    }

    @Test
    void aRequestedRoleRidesTheTicket() throws SQLException {
        try (Connection conn = connect("analyst,auditor", true);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
        assertEquals("analyst,auditor", tickets.get(0).get("role").getAsString());
    }

    @Test
    void aRefusedCredentialIsRaisedWithTheServersMessageAndNotRetriedOverHttp() throws SQLException {
        flightError = CallStatus.UNAUTHENTICATED.withDescription("credential rejected");
        try (Connection conn = connect(null, true); Statement stmt = conn.createStatement()) {
            SQLException refused = assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1"));
            assertTrue(refused.getMessage().contains("credential rejected"), refused.getMessage());
            assertEquals("28000", refused.getSQLState());
        }
        assertEquals(1, tickets.size());
        assertEquals(List.of("/auth/login"), httpPaths, "a Flight refusal is never retried over HTTP");
    }

    @Test
    void aRoleTheUserDoesNotHoldIsRaisedByName() throws SQLException {
        flightError = CallStatus.UNAUTHENTICATED
            .withDescription("role 'steward' is not assigned to this identity");
        try (Connection conn = connect("steward", true); Statement stmt = conn.createStatement()) {
            SQLException refused = assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1"));
            assertTrue(refused.getMessage().contains("role 'steward' is not assigned"), refused.getMessage());
        }
        assertEquals(List.of("/auth/login"), httpPaths);
    }

    @Test
    void anyOtherServerErrorIsRaisedTooAndNotRetriedOverHttp() throws SQLException {
        flightError = CallStatus.INTERNAL.withDescription("Unknown table sales.nope");
        try (Connection conn = connect(null, true); Statement stmt = conn.createStatement()) {
            SQLException failed = assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1"));
            assertTrue(failed.getMessage().contains("Unknown table sales.nope"), failed.getMessage());
        }
        assertEquals(List.of("/auth/login"), httpPaths);
    }

    @Test
    void httpIsUsedOnlyWhenTheFlightPortWasUnreachableAtConnect() throws SQLException {
        try (Connection conn = connect(null, false);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM sales.orders")) {
            assertInstanceOf(ProvisaResultSet.class, rs);
            assertTrue(rs.next());
            assertEquals(7, rs.getInt("id"));
        }
        assertEquals(List.of("/auth/login", "/data/sql"), httpPaths);
        assertTrue(tickets.isEmpty());
    }

    @Test
    void aFlightPortThatAnswersWithAnErrorIsNotTreatedAsUnreachable() {
        probeError = CallStatus.UNAUTHENTICATED.withDescription("a client certificate is required");
        SQLException refused = assertThrows(SQLException.class, () -> connect(null, true));
        assertTrue(refused.getMessage().contains("a client certificate is required"), refused.getMessage());
        assertEquals("28000", refused.getSQLState());
    }

    // ── result fidelity ──

    @Test
    void everyValueComesBackAsItsJdbcType() throws SQLException {
        try (Connection conn = connect(null, true);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt("id"));
            assertEquals(Integer.valueOf(7), rs.getObject("id"));
            assertEquals(9_000_000_000L, rs.getLong("big"));
            assertEquals(Long.valueOf(9_000_000_000L), rs.getObject("big"));
            assertEquals(0.25, rs.getDouble("ratio"));
            assertEquals(Double.valueOf(0.25), rs.getObject("ratio"));
            // A decimal keeps its scale: 1234.50, not 1234.5 and not a binary float.
            assertEquals(new BigDecimal("1234.50"), rs.getBigDecimal("amount"));
            assertEquals(new BigDecimal("1234.50"), rs.getObject("amount"));
            assertEquals("1234.50", rs.getString("amount"));
            assertEquals("Ada", rs.getString("name"));
            assertEquals("Ada", rs.getObject("name"), "text is a String, not Arrow's Text");
            assertTrue(rs.getBoolean("active"));
            assertEquals(Boolean.TRUE, rs.getObject("active"));
            assertEquals(Date.valueOf("2026-03-04"), rs.getDate("day"));
            assertEquals(Date.valueOf("2026-03-04"), rs.getObject("day"));
            assertEquals("2026-03-04", rs.getString("day"));
            // A zoneless timestamp is its wall-clock reading in any JVM zone.
            assertEquals(Timestamp.valueOf(WALL_CLOCK), rs.getTimestamp("seen"));
            assertEquals(Timestamp.valueOf(WALL_CLOCK), rs.getObject("seen"));
            assertEquals("2026-03-04 05:06:07.89", rs.getString("seen"));
            assertEquals(Date.valueOf("2026-03-04"), rs.getDate("seen"));
            // A zoned timestamp is its instant.
            assertEquals(Timestamp.from(INSTANT), rs.getTimestamp("seen_utc"));
            assertArrayEquals(new byte[]{1, 2, 3}, rs.getBytes("blob"));
            assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) rs.getObject("blob"));
            assertFalse(rs.wasNull());
            // By index as by label.
            assertEquals(7, rs.getInt(1));
            assertEquals("Ada", rs.getString(5));
            assertEquals(5, rs.findColumn("name"));
            assertTrue(rs.next());
            assertFalse(rs.next());
        }
    }

    @Test
    void aSqlNullIsNullOrTheZeroValueAndWasNullSaysSo() throws SQLException {
        try (Connection conn = connect(null, true);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
            assertTrue(rs.next());
            assertTrue(rs.next());
            assertEquals(8, rs.getInt("id"));
            assertFalse(rs.wasNull());
            for (String column : List.of("big", "ratio", "amount", "name", "active", "day", "seen", "seen_utc", "blob")) {
                assertNull(rs.getObject(column), column);
                assertTrue(rs.wasNull(), column);
                assertNull(rs.getString(column), column);
            }
            assertEquals(0L, rs.getLong("big"));
            assertTrue(rs.wasNull());
            assertEquals(0.0, rs.getDouble("ratio"));
            assertFalse(rs.getBoolean("active"));
            assertNull(rs.getBigDecimal("amount"));
            assertNull(rs.getDate("day"));
            assertNull(rs.getTimestamp("seen"));
            assertNull(rs.getBytes("blob"));
        }
    }

    @Test
    void theMetadataIsTheStreamsSchema() throws SQLException {
        try (Connection conn = connect(null, true);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
            ResultSetMetaData meta = rs.getMetaData();
            assertEquals(10, meta.getColumnCount());
            assertEquals("id", meta.getColumnName(1));
            int[] types = new int[meta.getColumnCount()];
            for (int i = 0; i < types.length; i++) types[i] = meta.getColumnType(i + 1);
            assertArrayEquals(new int[]{
                Types.INTEGER, Types.BIGINT, Types.DOUBLE, Types.DECIMAL, Types.VARCHAR, Types.BOOLEAN,
                Types.DATE, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, Types.BINARY}, types);
            assertEquals(12, meta.getPrecision(4));
            assertEquals(2, meta.getScale(4));
            assertEquals(ResultSetMetaData.columnNoNulls, meta.isNullable(1));
            assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(2));
            assertEquals("java.math.BigDecimal", meta.getColumnClassName(4));
            assertEquals("java.lang.String", meta.getColumnClassName(5));
            assertEquals("java.sql.Date", meta.getColumnClassName(7));
            assertEquals("java.sql.Timestamp", meta.getColumnClassName(8));
        }
    }

    @Test
    void anEmptyResultStillHasItsColumns() throws SQLException {
        emptyResult = true;
        try (Connection conn = connect(null, true);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t WHERE 1 = 0")) {
            assertFalse(rs.next());
            assertEquals(10, rs.getMetaData().getColumnCount());
            assertEquals(Types.DECIMAL, rs.getMetaData().getColumnType(4));
            assertEquals(5, rs.findColumn("name"));
        }
    }
}
