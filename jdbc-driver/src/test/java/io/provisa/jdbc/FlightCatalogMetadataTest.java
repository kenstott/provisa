package io.provisa.jdbc;

import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.RequestContext;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catalog metadata comes from the server's role-narrowed Arrow Flight catalog (REQ-128): the
 * driver lists flights with the session token and the requested role, and getTables, getColumns
 * and the key metadata are what that listing holds — nothing is read from anywhere else.
 *
 * <p>The Flight and HTTP servers are stubs inside this JVM. The Flight stub lists a different
 * catalog per requested role, as the server does.
 */
class FlightCatalogMetadataTest {

    private static Field column(String name, Map<String, String> metadata) {
        return new Field(name, new FieldType(true, new ArrowType.Utf8(), null, metadata), null);
    }

    private static FlightInfo table(String domain, String name, String description, Field... columns) {
        Schema schema = new Schema(List.of(columns), Map.of("description", description, "domain", domain));
        return new FlightInfo(schema, FlightDescriptor.path(domain, name), List.of(), -1, -1);
    }

    private static final FlightInfo ORDERS = table("sales", "orders", "Customer orders",
        column("id", Map.of("primary_key", "true", "description", "Order id")),
        column("customer_id", Map.of(
            "references", "{\"domain\": \"sales\", \"table\": \"customers\", \"column\": \"id\"}")),
        column("region", Map.of()));
    private static final FlightInfo CUSTOMERS = table("sales", "customers", "Customer accounts",
        column("id", Map.of("primary_key", "true")),
        column("name", Map.of()));
    private static final FlightInfo STAFF = table("hr", "staff", "Staff",
        column("id", Map.of("primary_key", "true")));
    /** A command: listed beside tables at a three-part path, and not a table. */
    private static final FlightInfo COMMAND = new FlightInfo(
        new Schema(List.of()), FlightDescriptor.path("commands", "sales", "order_count"), List.of(), -1, -1);

    private BufferAllocator allocator;
    private FlightServer flight;
    private HttpServer http;
    private final List<Map<String, String>> listCalls = new ArrayList<>();
    private final List<String> httpPaths = new ArrayList<>();
    private CallStatus listError;
    private int specStatus = 200;
    private String specAuthorization;
    private String specRole;

    /** The role's REST OpenAPI document, in the shape the server generates it. */
    private static final String REST_SPEC = """
        {"openapi": "3.1.0",
         "paths": {
           "/sales/orders": {"get": {"parameters": [
             {"name": "limit", "in": "query", "schema": {"type": "integer"}},
             {"name": "fields", "in": "query",
              "schema": {"type": "array", "items": {"$ref": "#/components/schemas/OrdersField"}}}]}},
           "/sales/orders/{id}": {"get": {"parameters": []}}},
         "components": {"schemas": {
           "Error": {"type": "object", "properties": {"detail": {"type": "string"}}},
           "Orders": {"type": "object", "description": "Customer orders", "properties": {
             "id": {"type": "integer", "description": "Order id"},
             "region": {"type": "string"}}},
           "OrdersField": {"type": "string", "enum": ["id", "region"]}}}}
        """;

    /** Keeps the headers each call arrived with, for the producer to read. */
    private static final FlightServerMiddleware.Key<Headers> HEADERS = FlightServerMiddleware.Key.of("headers");

    private record Headers(CallHeaders incoming) implements FlightServerMiddleware {
        @Override public void onBeforeSendingHeaders(CallHeaders outgoing) { }
        @Override public void onCallCompleted(CallStatus status) { }
        @Override public void onCallErrored(Throwable err) { }
    }

    @BeforeEach
    void start() throws IOException {
        allocator = new RootAllocator();
        flight = FlightServer.builder(allocator, Location.forGrpcInsecure("127.0.0.1", 0), new NoOpFlightProducer() {
            @Override
            public void listActions(CallContext context, StreamListener<org.apache.arrow.flight.ActionType> listener) {
                listener.onCompleted();
            }

            @Override
            public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
                CallHeaders incoming = context.getMiddleware(HEADERS).incoming();
                String role = incoming.get("x-provisa-role");
                Map<String, String> seen = new java.util.HashMap<>();
                seen.put("authorization", incoming.get("authorization"));
                seen.put("x-provisa-role", role);
                listCalls.add(seen);
                if (listError != null) {
                    listener.onError(listError.toRuntimeException());
                    return;
                }
                // The catalog is the role's own: hr_reader is served staff, everyone else sales.
                List<FlightInfo> served = "hr_reader".equals(role)
                    ? List.of(STAFF) : List.of(ORDERS, CUSTOMERS, COMMAND);
                served.forEach(listener::onNext);
                listener.onCompleted();
            }
        }).middleware(HEADERS, (CallInfo info, CallHeaders incoming, RequestContext context) -> new Headers(incoming))
            .build();
        flight.start();

        http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            httpPaths.add(path);
            exchange.getRequestBody().readAllBytes();
            String body = "{\"access_token\": \"tok-1\", \"token_type\": \"bearer\"}";
            int status = 200;
            if (path.equals("/data/rest/openapi.json")) {
                specAuthorization = exchange.getRequestHeaders().getFirst("Authorization");
                specRole = exchange.getRequestHeaders().getFirst("X-Provisa-Role");
                status = specStatus;
                body = status == 200 ? REST_SPEC : "{\"detail\": \"Role 'steward' is not assigned to this user\"}";
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
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

    private Connection connect(String role, boolean flightReachable) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "alice");
        props.setProperty("password", "secret");
        if (role != null) props.setProperty("role", role);
        props.setProperty("flight_port", String.valueOf(flightReachable ? flight.getPort() : 1));
        return new ProvisaDriver().connect("jdbc:provisa://127.0.0.1:" + http.getAddress().getPort(), props);
    }

    private static List<String> column(ResultSet rs, String name) throws SQLException {
        List<String> out = new ArrayList<>();
        while (rs.next()) out.add(rs.getString(name));
        return out;
    }

    @Test
    void theCatalogIsListedWithTheSessionTokenAndTheRequestedRole() throws SQLException {
        try (Connection conn = connect("analyst,auditor", true)) {
            conn.getMetaData().getTables(null, null, "%", null).close();
        }
        assertEquals("Bearer tok-1", listCalls.get(0).get("authorization"));
        assertEquals("analyst,auditor", listCalls.get(0).get("x-provisa-role"));
        try (Connection conn = connect(null, true)) {
            conn.getMetaData().getTables(null, null, "%", null).close();
        }
        assertNull(listCalls.get(1).get("x-provisa-role"), "no role is requested unless the connection asks");
        assertEquals(List.of("/auth/login", "/auth/login"), httpPaths, "nothing is read over HTTP");
    }

    @Test
    void getTablesIsWhatTheRoleIsServed() throws SQLException {
        try (Connection conn = connect(null, true);
             ResultSet rs = conn.getMetaData().getTables(null, null, "%", null)) {
            List<String> names = new ArrayList<>();
            List<String> remarks = new ArrayList<>();
            while (rs.next()) {
                names.add(rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"));
                remarks.add(rs.getString("REMARKS"));
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
            }
            assertEquals(List.of("sales.orders", "sales.customers"), names, "the command is not a table");
            assertEquals(List.of("Customer orders", "Customer accounts"), remarks);
        }
        // Another role's connection is listed that role's catalog, and nothing of this one's.
        try (Connection conn = connect("hr_reader", true);
             ResultSet rs = conn.getMetaData().getTables(null, null, "%", null)) {
            assertEquals(List.of("staff"), column(rs, "TABLE_NAME"));
        }
        try (Connection conn = connect("hr_reader", true);
             ResultSet rs = conn.getMetaData().getColumns(null, null, "orders", null)) {
            assertFalse(rs.next(), "a table the role is not served has no columns to list");
        }
    }

    @Test
    void getColumnsIsTheTablesServedColumns() throws SQLException {
        try (Connection conn = connect(null, true);
             ResultSet rs = conn.getMetaData().getColumns(null, null, "orders", null)) {
            List<String> names = new ArrayList<>();
            String firstRemark = null;
            while (rs.next()) {
                if (names.isEmpty()) firstRemark = rs.getString("REMARKS");
                names.add(rs.getString("COLUMN_NAME"));
            }
            assertEquals(List.of("id", "customer_id", "region"), names);
            assertEquals("Order id", firstRemark);
        }
    }

    @Test
    void theKeysAreTheOnesTheCatalogDeclares() throws SQLException {
        try (Connection conn = connect(null, true)) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet pk = meta.getPrimaryKeys(null, null, "orders")) {
                assertEquals(List.of("id"), column(pk, "COLUMN_NAME"));
            }
            try (ResultSet fk = meta.getImportedKeys(null, null, "orders")) {
                assertTrue(fk.next());
                assertEquals("customers", fk.getString("PKTABLE_NAME"));
                assertEquals("id", fk.getString("PKCOLUMN_NAME"));
                assertEquals("orders", fk.getString("FKTABLE_NAME"));
                assertEquals("customer_id", fk.getString("FKCOLUMN_NAME"));
                assertFalse(fk.next());
            }
            try (ResultSet exported = meta.getExportedKeys(null, null, "customers")) {
                assertEquals(List.of("orders"), column(exported, "FKTABLE_NAME"));
            }
            try (ResultSet none = meta.getImportedKeys(null, null, "customers")) {
                assertFalse(none.next());
            }
        }
    }

    @Test
    void aRefusedCatalogReadIsRaisedWithTheServersReason() throws SQLException {
        listError = CallStatus.UNAUTHENTICATED.withDescription("role 'steward' is not assigned to this identity");
        try (Connection conn = connect("steward", true)) {
            SQLException refused = assertThrows(SQLException.class,
                () -> conn.getMetaData().getTables(null, null, "%", null));
            assertTrue(refused.getMessage().contains("role 'steward' is not assigned"), refused.getMessage());
            assertEquals("28000", refused.getSQLState());
        }
        assertEquals(List.of("/auth/login"), httpPaths, "it is not read from somewhere else instead");
    }

    @Test
    void withTheFlightPortUnreachableTheCatalogIsTheRolesRestDocument() throws SQLException {
        try (Connection conn = connect("analyst", false)) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, null, "%", null)) {
                List<String> names = new ArrayList<>();
                while (rs.next()) {
                    names.add(rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"));
                    assertEquals("Customer orders", rs.getString("REMARKS"));
                }
                assertEquals(List.of("sales.orders"), names);
            }
            try (ResultSet rs = meta.getColumns(null, null, "orders", null)) {
                assertTrue(rs.next());
                assertEquals("id", rs.getString("COLUMN_NAME"));
                assertEquals(java.sql.Types.INTEGER, rs.getInt("DATA_TYPE"));
                assertEquals("INTEGER", rs.getString("TYPE_NAME"));
                assertEquals("Order id", rs.getString("REMARKS"));
                assertTrue(rs.next());
                assertEquals("region", rs.getString("COLUMN_NAME"));
                assertEquals(java.sql.Types.VARCHAR, rs.getInt("DATA_TYPE"));
                assertFalse(rs.next());
            }
            // That document declares no keys: none are invented.
            try (ResultSet pk = meta.getPrimaryKeys(null, null, "orders")) {
                assertFalse(pk.next());
            }
            try (ResultSet fk = meta.getImportedKeys(null, null, "orders")) {
                assertFalse(fk.next());
            }
        }
        assertTrue(listCalls.isEmpty());
        assertTrue(httpPaths.contains("/data/rest/openapi.json"));
        assertTrue(httpPaths.stream().noneMatch(p -> p.startsWith("/admin")), "the admin API is never asked: " + httpPaths);
        // The document is asked for as the connection's identity: its token and requested role.
        assertEquals("Bearer tok-1", specAuthorization);
        assertEquals("analyst", specRole);
    }

    @Test
    void aRefusedRestDocumentIsRaisedWithTheServersReason() throws SQLException {
        specStatus = 403;
        try (Connection conn = connect("steward", false)) {
            SQLException refused = assertThrows(SQLException.class,
                () -> conn.getMetaData().getTables(null, null, "%", null));
            assertTrue(refused.getMessage().contains("403"), refused.getMessage());
            assertTrue(refused.getMessage().contains("is not assigned to this user"), refused.getMessage());
            assertEquals("28000", refused.getSQLState());
        }
    }
}
