package io.provisa.jdbc;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sign-in: the driver exchanges the user and password for the server's session token, sends it
 * on every request, and never opens a connection the server did not authenticate.
 *
 * <p>The server is a stub inside this JVM answering as the real routes do: {@code /auth/login}
 * with {@code {"access_token", "token_type"}} and {@code /data/sql} with {@code {"data": {"sql":
 * [...]}}}.
 */
class ProvisaConnectionSignInTest {

    /** One request the stub received. */
    record Seen(String path, String authorization, String role, JsonObject body) {}

    private HttpServer server;
    private final List<Seen> seen = new ArrayList<>();
    private int loginStatus = 200;
    private String loginBody = "{\"access_token\": \"tok-1\", \"token_type\": \"bearer\"}";

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/auth/login", exchange -> answer(exchange, loginStatus, loginBody));
        server.createContext("/data/sql", exchange ->
            answer(exchange, 200, "{\"data\": {\"sql\": [{\"id\": 7}]}}"));
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private void answer(HttpExchange exchange, int status, String body) throws IOException {
        String sent = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        seen.add(new Seen(
            exchange.getRequestURI().getPath(),
            exchange.getRequestHeaders().getFirst("Authorization"),
            exchange.getRequestHeaders().getFirst("X-Provisa-Role"),
            sent.isEmpty() ? new JsonObject() : JsonParser.parseString(sent).getAsJsonObject()));
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private Connection connect(String user, String password, String role) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        if (role != null) props.setProperty("role", role);
        return new ProvisaDriver().connect(
            "jdbc:provisa://127.0.0.1:" + server.getAddress().getPort(), props);
    }

    private Seen query(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM sales.orders")) {
            assertTrue(rs.next());
            assertEquals(7, rs.getInt("id"));
        }
        return seen.get(seen.size() - 1);
    }

    @Test
    void theSessionTokenTheServerIssuesIsSentOnEveryRequest() throws SQLException {
        try (Connection conn = connect("alice", "secret", null)) {
            Seen login = seen.get(0);
            assertEquals("/auth/login", login.path());
            assertEquals("alice", login.body().get("username").getAsString());
            assertEquals("secret", login.body().get("password").getAsString());

            Seen sql = query(conn);
            assertEquals("/data/sql", sql.path());
            assertEquals("Bearer tok-1", sql.authorization());
        }
    }

    @Test
    void noRoleIsInventedFromTheUserName() throws SQLException {
        try (Connection conn = connect("alice", "secret", null)) {
            Seen sql = query(conn);
            assertNull(sql.role(), "the server derives the role from the identity");
            assertFalse(sql.body().has("role"), "the role never travels in the body");
            assertEquals("alice", conn.getMetaData().getUserName());
        }
    }

    @Test
    void aRequestedRoleIsSentAsTheRoleHeader() throws SQLException {
        try (Connection conn = connect("alice", "secret", "analyst,auditor")) {
            Seen sql = query(conn);
            assertEquals("analyst,auditor", sql.role());
            assertFalse(sql.body().has("role"));
        }
    }

    @Test
    void aRoleInTheUrlIsTheRequestedRole() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "alice");
        props.setProperty("password", "secret");
        try (Connection conn = new ProvisaDriver().connect(
                "jdbc:provisa://127.0.0.1:" + server.getAddress().getPort() + "?role=analyst%2Cauditor",
                props)) {
            assertEquals("analyst,auditor", query(conn).role());
        }
    }

    @Test
    void aRefusedSignInIsRaisedWithTheServersStatusAndReason() {
        loginStatus = 401;
        loginBody = "{\"detail\": \"Invalid username or password\", \"code\": \"auth.invalid_credentials\"}";
        SQLException refused = assertThrows(SQLException.class, () -> connect("alice", "wrong", null));
        assertTrue(refused.getMessage().contains("401"), refused.getMessage());
        assertTrue(refused.getMessage().contains("Invalid username or password"), refused.getMessage());
        assertEquals("28000", refused.getSQLState());
        assertEquals(1, seen.size(), "nothing is sent after a refused sign-in");
    }

    @Test
    void aLockedOutOrFailingServerIsRaisedToo() {
        loginStatus = 503;
        loginBody = "upstream unavailable";
        SQLException refused = assertThrows(SQLException.class, () -> connect("alice", "secret", null));
        assertTrue(refused.getMessage().contains("503"), refused.getMessage());
        assertTrue(refused.getMessage().contains("upstream unavailable"), refused.getMessage());
    }

    @Test
    void aSignInAnsweredWithoutATokenIsRefused() {
        loginBody = "{\"token\": \"old-shape\", \"role\": \"admin\"}";
        SQLException refused = assertThrows(SQLException.class, () -> connect("alice", "secret", null));
        assertTrue(refused.getMessage().contains("access_token"), refused.getMessage());
    }

    @Test
    void anUnreachableServerIsRaised() {
        server.stop(0);
        SQLException refused = assertThrows(SQLException.class, () -> connect("alice", "secret", null));
        assertTrue(refused.getMessage().contains("cannot reach"), refused.getMessage());
    }

    @Test
    void aServerWithNoPasswordSignInTakesTheUserNameAsTheRequestedRole() throws SQLException {
        loginStatus = 404;
        loginBody = "{\"detail\": \"Not Found\"}";
        try (Connection conn = connect("analyst", "", null)) {
            Seen sql = query(conn);
            assertNull(sql.authorization());
            assertEquals("analyst", sql.role());
        }
        // A role property still decides when one is given.
        try (Connection conn = connect("anyone", "", "steward")) {
            assertEquals("steward", query(conn).role());
        }
    }
}
