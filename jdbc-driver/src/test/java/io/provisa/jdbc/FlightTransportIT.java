package io.provisa.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The driver against a live Provisa server whose Flight port answers (REQ-293), with or without
 * an auth provider.
 *
 * <p>System properties: {@code provisa.url} (jdbc:provisa://host:port), {@code provisa.user},
 * {@code provisa.password}, {@code provisa.sql} (a statement the user may run and that returns at
 * least one row), {@code provisa.unheldRole} (a role the user does not hold; with an auth
 * provider). Run via {@code mvn verify}.
 */
class FlightTransportIT {

    static final String BASE_URL = System.getProperty("provisa.url", "jdbc:provisa://localhost:8001");
    static final String USER = System.getProperty("provisa.user", "admin");
    static final String PASSWORD = System.getProperty("provisa.password", "");
    static final String SQL = System.getProperty("provisa.sql", "SELECT 1 AS one");
    static final String UNHELD_ROLE = System.getProperty("provisa.unheldRole");

    private static Properties props(String password, String role) {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", password);
        if (role != null) props.setProperty("role", role);
        return props;
    }

    @Test
    void aQueryRunsOverFlightAndStreamsTypedRows() throws SQLException {
        try (var conn = (ProvisaConnection) DriverManager.getConnection(BASE_URL, props(PASSWORD, null))) {
            assertNotNull(conn.flightTransport, "the Flight port must answer for this test");
            try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(SQL)) {
                assertInstanceOf(FlightStreamResultSet.class, rs);
                assertTrue(rs.getMetaData().getColumnCount() > 0);
                assertTrue(rs.next());
            }
        }
    }

    @Test
    void theFlightTransportIsClosedWithTheConnection() throws SQLException {
        var conn = (ProvisaConnection) DriverManager.getConnection(BASE_URL, props(PASSWORD, null));
        conn.close();
        assertTrue(conn.isClosed());
        assertNull(conn.flightTransport);
    }

    @Test
    void aRoleTheUserDoesNotHoldIsRefusedWithTheServersReason() throws SQLException {
        org.junit.jupiter.api.Assumptions.assumeTrue(
            UNHELD_ROLE != null, "provisa.unheldRole names a role the user does not hold");
        try (Connection conn = DriverManager.getConnection(BASE_URL, props(PASSWORD, UNHELD_ROLE));
             Statement stmt = conn.createStatement()) {
            SQLException refused = assertThrows(SQLException.class, () -> stmt.executeQuery(SQL));
            assertTrue(refused.getMessage().contains(UNHELD_ROLE), refused.getMessage());
        }
    }
}
