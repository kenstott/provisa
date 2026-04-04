package io.provisa.jdbc;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Arrow Flight client wrapper for Provisa query execution.
 *
 * Connects to the Provisa Flight server (grpc://host:8815), builds a ticket
 * containing the GraphQL query + role + variables, and streams Arrow record
 * batches back via doGet.
 */
class FlightTransport implements AutoCloseable {

    private static final Logger log = Logger.getLogger(FlightTransport.class.getName());
    private static final int DEFAULT_FLIGHT_PORT = 8815;

    private final FlightClient client;
    private final BufferAllocator allocator;
    private final String role;

    private FlightTransport(FlightClient client, BufferAllocator allocator, String role) {
        this.client = client;
        this.allocator = allocator;
        this.role = role;
    }

    /**
     * Attempt to connect to the Flight server. Returns null if unavailable.
     */
    static FlightTransport tryConnect(String httpHost, int httpPort, String role) {
        int flightPort = deriveFlightPort(httpPort);
        String location = "grpc://" + httpHost + ":" + flightPort;
        BufferAllocator allocator = new RootAllocator();
        try {
            FlightClient client = FlightClient.builder(allocator,
                Location.forGrpcInsecure(httpHost, flightPort))
                .build();
            // Probe the connection with a handshake or action
            client.listActions().forEach(a -> {}); // quick connectivity check
            log.info("Flight transport connected: " + location);
            return new FlightTransport(client, allocator, role);
        } catch (Exception e) {
            log.log(Level.FINE, "Flight transport unavailable at " + location + ", using HTTP fallback", e);
            allocator.close();
            return null;
        }
    }

    /**
     * Execute a query via Flight doGet and return a streaming ResultSet.
     */
    FlightStreamResultSet execute(String queryText, Map<String, Object> variables) throws SQLException {
        JsonObject ticket = new JsonObject();
        ticket.addProperty("query", queryText);
        ticket.addProperty("role", role);
        if (variables != null && !variables.isEmpty()) {
            ticket.add("variables", new Gson().toJsonTree(variables));
        }

        byte[] ticketBytes = ticket.toString().getBytes(StandardCharsets.UTF_8);

        try {
            FlightStream stream = client.getStream(new Ticket(ticketBytes));
            return new FlightStreamResultSet(stream);
        } catch (Exception e) {
            throw new SQLException("Flight query execution failed: " + e.getMessage(), e);
        }
    }

    /**
     * Build ticket JSON bytes for a query (exposed for testing).
     */
    static byte[] buildTicket(String queryText, String role, Map<String, Object> variables) {
        JsonObject ticket = new JsonObject();
        ticket.addProperty("query", queryText);
        ticket.addProperty("role", role);
        if (variables != null && !variables.isEmpty()) {
            ticket.add("variables", new Gson().toJsonTree(variables));
        }
        return ticket.toString().getBytes(StandardCharsets.UTF_8);
    }

    boolean isConnected() {
        return client != null;
    }

    @Override
    public void close() {
        try { client.close(); } catch (Exception ignored) {}
        allocator.close();
    }

    private static int deriveFlightPort(int httpPort) {
        // Convention: Flight port = 8815 for default HTTP port 8001
        // For other ports, offset by the same delta
        if (httpPort == 8001) return DEFAULT_FLIGHT_PORT;
        return httpPort + (DEFAULT_FLIGHT_PORT - 8001);
    }
}
