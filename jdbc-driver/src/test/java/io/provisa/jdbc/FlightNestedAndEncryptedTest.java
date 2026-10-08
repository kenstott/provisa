package io.provisa.jdbc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Over Arrow Flight: nested values come back as JDBC values, not Arrow's collection classes, and
 * a column the server flags encrypted is decrypted with the connection's key (REQ-690, REQ-693).
 *
 * <p>The Flight and HTTP servers are stubs inside this JVM.
 */
class FlightNestedAndEncryptedTest {

    private static final SecureRandom RNG = new SecureRandom();
    private static final byte[] MASTER_KEY = new byte[32];
    private static final byte[] OTHER_KEY = new byte[32];

    static {
        RNG.nextBytes(MASTER_KEY);
        RNG.nextBytes(OTHER_KEY);
    }

    private static final Schema SCHEMA = new Schema(List.of(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("tags", FieldType.nullable(new ArrowType.List()),
            List.of(new Field("item", FieldType.nullable(new ArrowType.Utf8()), null))),
        new Field("scores", FieldType.nullable(new ArrowType.List()),
            List.of(new Field("item", FieldType.nullable(new ArrowType.Int(32, true)), null))),
        new Field("address", FieldType.nullable(new ArrowType.Struct()),
            List.of(
                new Field("city", FieldType.nullable(new ArrowType.Utf8()), null),
                new Field("zip", FieldType.nullable(new ArrowType.Int(32, true)), null))),
        new Field("ssn", new FieldType(true, new ArrowType.Utf8(), null, Map.of("provisa_encrypted", "true")),
            null),
        new Field("note", FieldType.nullable(new ArrowType.Utf8()), null)));

    private BufferAllocator allocator;
    private FlightServer flight;
    private HttpServer http;
    private final List<JsonObject> tickets = new ArrayList<>();
    private String encrypted;

    @BeforeEach
    void start() throws Exception {
        encrypted = envelopeFor("123-45-6789");
        allocator = new RootAllocator();
        flight = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), new NoOpFlightProducer() {
            @Override
            public void listActions(CallContext context, StreamListener<org.apache.arrow.flight.ActionType> listener) {
                listener.onCompleted();
            }

            @Override
            public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                tickets.add(JsonParser.parseString(new String(ticket.getBytes(), StandardCharsets.UTF_8))
                    .getAsJsonObject());
                try (VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator)) {
                    listener.start(root);
                    fill(root);
                    listener.putNext();
                    listener.completed();
                }
            }
        }).build();
        flight.start();
        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] bytes = "{\"access_token\": \"tok-1\", \"token_type\": \"bearer\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        http.start();
    }

    @AfterEach
    void stop() throws Exception {
        http.stop(0);
        // Every call has ended before the stub's buffers are counted: a client that closes its
        // result set early cancels the call, and the server releases what it had queued then.
        flight.shutdown();
        flight.awaitTermination(30, java.util.concurrent.TimeUnit.SECONDS);
        flight.close();
        allocator.close();
    }

    private void fill(VectorSchemaRoot root) {
        root.allocateNew();
        ((IntVector) root.getVector("id")).setSafe(0, 7);

        UnionListWriter tags = ((ListVector) root.getVector("tags")).getWriter();
        tags.setPosition(0);
        tags.startList();
        tags.writeVarChar("red");
        tags.writeVarChar("blue");
        tags.endList();

        UnionListWriter scores = ((ListVector) root.getVector("scores")).getWriter();
        scores.setPosition(0);
        scores.startList();
        scores.writeInt(3);
        scores.writeInt(5);
        scores.endList();

        StructVector address = (StructVector) root.getVector("address");
        ((VarCharVector) address.getChild("city")).setSafe(0, "Oslo".getBytes(StandardCharsets.UTF_8));
        ((IntVector) address.getChild("zip")).setSafe(0, 150);
        address.setIndexDefined(0);

        ((VarCharVector) root.getVector("ssn")).setSafe(0, encrypted.getBytes(StandardCharsets.UTF_8));
        // Not flagged: it happens to hold the same ciphertext text and must come back untouched.
        ((VarCharVector) root.getVector("note")).setSafe(0, encrypted.getBytes(StandardCharsets.UTF_8));
        root.setRowCount(1);
    }

    // ── the envelope the server writes for an encrypted column (EnvelopeDecryptor's format) ──

    private static byte[] aesGcm(byte[] key, byte[] nonce, byte[] plaintext) throws Exception {
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, nonce));
        return c.doFinal(plaintext);
    }

    private static String envelopeFor(String plaintext) throws Exception {
        byte[] dek = new byte[32];
        byte[] wrapNonce = new byte[12];
        byte[] iv = new byte[12];
        RNG.nextBytes(dek);
        RNG.nextBytes(wrapNonce);
        RNG.nextBytes(iv);
        ByteArrayOutputStream wrapped = new ByteArrayOutputStream();
        wrapped.write(wrapNonce);
        wrapped.write(aesGcm(MASTER_KEY, wrapNonce, dek));
        byte[] ciphertext = aesGcm(dek, iv, plaintext.getBytes(StandardCharsets.UTF_8));
        ByteBuffer buf = ByteBuffer.allocate(6 + wrapped.size() + iv.length + ciphertext.length);
        buf.put((byte) 0xE1).put((byte) 1).putInt(wrapped.size());
        buf.put(wrapped.toByteArray()).put(iv).put(ciphertext);
        return Base64.getEncoder().encodeToString(buf.array());
    }

    private Connection connect(byte[] masterKey) throws SQLException, IOException {
        Properties props = new Properties();
        props.setProperty("user", "alice");
        props.setProperty("password", "secret");
        props.setProperty("flight_port", String.valueOf(flight.getPort()));
        if (masterKey != null) {
            props.setProperty("kms_provider", "local");
            props.setProperty("kms_key_arn", "local-key-1");
            props.setProperty("kms_master_key", Base64.getEncoder().encodeToString(masterKey));
        }
        return new ProvisaDriver().connect("jdbc:provisa://127.0.0.1:" + http.getAddress().getPort(), props);
    }

    // ── nested values ──

    @Test
    void aListIsAJdbcArrayOfPlainValues() throws Exception {
        try (Connection conn = connect(null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
            assertTrue(rs.next());
            Array tags = rs.getArray("tags");
            assertArrayEquals(new Object[]{"red", "blue"}, (Object[]) tags.getArray());
            assertEquals(Types.VARCHAR, tags.getBaseType());
            assertInstanceOf(Array.class, rs.getObject("tags"), "not Arrow's list class");
            assertArrayEquals(new Object[]{3, 5}, (Object[]) rs.getArray("scores").getArray());
            assertEquals(Types.INTEGER, rs.getArray("scores").getBaseType());
            assertArrayEquals(new Object[]{5}, (Object[]) rs.getArray("scores").getArray(2, 1));
            assertEquals("[red, blue]", rs.getString("tags"));
            SQLException notAnArray = assertThrows(SQLException.class, () -> rs.getArray("id"));
            assertTrue(notAnArray.getMessage().contains("is not an array"), notAnArray.getMessage());
        }
    }

    @Test
    void aStructIsItsJsonText() throws Exception {
        try (Connection conn = connect(null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
            assertTrue(rs.next());
            Object address = rs.getObject("address");
            assertInstanceOf(String.class, address, "not Arrow's map class");
            JsonObject json = JsonParser.parseString((String) address).getAsJsonObject();
            assertEquals("Oslo", json.get("city").getAsString());
            assertEquals(150, json.get("zip").getAsInt());
            assertEquals(address, rs.getString("address"));
        }
    }

    @Test
    void theMetadataNamesTheJdbcTypes() throws Exception {
        try (Connection conn = connect(null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
            ResultSetMetaData meta = rs.getMetaData();
            // Read the row: the stub server counts its buffers at teardown, and a call the client
            // abandons before its first batch leaves that batch queued on the server side.
            assertTrue(rs.next());
            assertEquals(Types.ARRAY, meta.getColumnType(2));
            assertEquals("ARRAY", meta.getColumnTypeName(2));
            assertEquals("java.sql.Array", meta.getColumnClassName(2));
            assertEquals(Types.VARCHAR, meta.getColumnType(4));
            assertEquals("java.lang.String", meta.getColumnClassName(4));
        }
    }

    // ── client-side decryption ──

    @Test
    void theTicketNamesTheClientSideKeyOnlyWhenOneIsConfigured() throws Exception {
        try (Connection conn = connect(MASTER_KEY);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
        assertEquals("local-key-1", tickets.get(0).get("kms_key").getAsString());
        try (Connection conn = connect(null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT 1")) {
            assertTrue(rs.next());
        }
        assertFalse(tickets.get(1).has("kms_key"));
    }

    @Test
    void aFlaggedColumnIsDecryptedAndAnUnflaggedOneIsNot() throws Exception {
        try (Connection conn = connect(MASTER_KEY);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
            assertTrue(rs.next());
            assertEquals("123-45-6789", rs.getString("ssn"));
            assertEquals("123-45-6789", rs.getObject("ssn"));
            assertEquals(encrypted, rs.getString("note"), "an unflagged column is returned as it is");
        }
    }

    @Test
    void theWrongKeyRaisesAndNeverReturnsCiphertextAsData() throws Exception {
        try (Connection conn = connect(OTHER_KEY);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
            assertTrue(rs.next());
            assertThrows(DecryptionException.class, () -> rs.getString("ssn"));
            assertThrows(DecryptionException.class, () -> rs.getObject("ssn"));
            assertEquals(7, rs.getInt("id"), "the other columns of the row are still readable");
        }
    }

    @Test
    void anEncryptedColumnWithNoKeyConfiguredRaises() throws Exception {
        try (Connection conn = connect(null);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM t")) {
            assertTrue(rs.next());
            DecryptionException refused = assertThrows(DecryptionException.class, () -> rs.getString("ssn"));
            assertTrue(refused.getMessage().contains("ssn"), refused.getMessage());
        }
    }
}
