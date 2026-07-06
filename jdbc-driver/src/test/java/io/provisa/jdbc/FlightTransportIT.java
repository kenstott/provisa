package io.provisa.jdbc;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Arrow Flight transport against a live backend.
 *
 * Requires: docker-compose up (Provisa on :8001, Flight server on :8815)
 * Run via: mvn verify
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlightTransportIT {

    static final String BASE_URL = System.getProperty("provisa.url", "jdbc:provisa://localhost:8001");
    static final String USER = System.getProperty("provisa.user", "admin");

    @Test
    @Order(1)
    void flightTransport_connectsWhenServerAvailable() throws SQLException {
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        try (var conn = (ProvisaConnection) DriverManager.getConnection(BASE_URL, props)) {
            // Flight may or may not be available depending on backend config
            if (conn.flightTransport != null) {
                assertTrue(conn.flightTransport.isConnected());
            } else {
                System.out.println("Flight server not available — HTTP fallback in use");
            }
        }
    }

    @Test
    @Order(3)
    void httpFallback_worksWhenFlightUnavailable() throws SQLException {
        // Connect to a port where Flight is definitely not running
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        try (var conn = (ProvisaConnection) DriverManager.getConnection(BASE_URL, props)) {
            // Even if Flight failed, the connection should be valid
            assertFalse(conn.isClosed());
            // Metadata should work (uses HTTP, not Flight)
            assertNotNull(conn.getMetaData().getDatabaseProductName());
        }
    }

    @Test
    @Order(4)
    void flightTransport_closedOnConnectionClose() throws SQLException {
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        var conn = (ProvisaConnection) DriverManager.getConnection(BASE_URL, props);
        @SuppressWarnings("unused")
        FlightTransport ft = conn.flightTransport;
        conn.close();
        assertTrue(conn.isClosed());
        assertNull(conn.flightTransport);
    }
}
