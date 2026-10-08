package io.provisa.jdbc;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStatusCode;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Arrow Flight transport for query execution (REQ-293).
 *
 * <p>Connects to the Provisa Flight server (grpc://host:8815). A query is one {@code doGet}
 * whose ticket is JSON: the query text, the session token as {@code token} (the one credential
 * form the Flight server reads, REQ-1263), the requested {@code role} when the connection
 * requests one, and {@code kms_key} when client-side decryption is configured (REQ-693).
 *
 * <p>Flight is used whenever its port answered when the connection was opened. HTTP is used only
 * when the port was unreachable then. Once Flight is the transport, whatever the server answers
 * a query with — a refused credential, a refused statement, any error — is raised to the caller
 * with the server's message; it is never retried over HTTP.
 */
class FlightTransport implements AutoCloseable {

    private static final Logger log = Logger.getLogger(FlightTransport.class.getName());
    private static final int DEFAULT_FLIGHT_PORT = 8815;
    private static final int DEFAULT_HTTP_PORT = 8001;

    private final FlightClient client;
    private final BufferAllocator allocator;

    private FlightTransport(FlightClient client, BufferAllocator allocator) {
        this.client = client;
        this.allocator = allocator;
    }

    /**
     * Connect to the Flight server.
     *
     * @return the transport, or null when the Flight port is UNREACHABLE (nothing listening, or
     *         the host does not resolve) — the one case in which queries go over HTTP
     * @throws SQLException when the port answered and the probe failed for any other reason; an
     *         answer of any kind is not "unavailable"
     */
    static FlightTransport connect(String host, int flightPort) throws SQLException {
        String location = "grpc://" + host + ":" + flightPort;
        BufferAllocator allocator = new RootAllocator();
        FlightClient client = null;
        try {
            client = FlightClient.builder(allocator, Location.forGrpcInsecure(host, flightPort)).build();
            // The probe: an RPC that needs no credential, so only reachability decides it.
            client.listActions().forEach(a -> { });
            log.info("Flight transport connected: " + location);
            return new FlightTransport(client, allocator);
        } catch (FlightRuntimeException e) {
            closeQuietly(client, allocator);
            if (e.status().code() == FlightStatusCode.UNAVAILABLE) {
                log.info("Flight is unreachable at " + location + "; queries on this connection use HTTP");
                return null;
            }
            throw refusal("Flight connection to " + location + " failed", e);
        } catch (RuntimeException e) {
            closeQuietly(client, allocator);
            throw new SQLException("Flight connection to " + location + " failed: " + e.getMessage(), e);
        }
    }

    /**
     * Execute a query via Flight doGet and return a streaming ResultSet.
     *
     * @throws SQLException with the server's message when the server refuses or fails the query
     */
    FlightStreamResultSet execute(
            String queryText, String token, String role, String kmsKey, EnvelopeDecryptor decryptor)
            throws SQLException {
        byte[] ticket = buildTicket(queryText, token, role, kmsKey, null);
        FlightStream stream = null;
        try {
            stream = client.getStream(new Ticket(ticket));
            // The server's answer to the ticket arrives with the schema: read it here so a
            // refusal is raised by executeQuery, not by the first next().
            return new FlightStreamResultSet(stream, decryptor);
        } catch (FlightRuntimeException e) {
            closeStream(stream);
            throw refusal("Query failed", e);
        }
    }

    /**
     * The SQLException for a Flight error: the server's own message, with SQLState 28000 for a
     * refused credential and 42501 for a refused permission.
     */
    static SQLException refusal(String what, FlightRuntimeException e) {
        CallStatus status = e.status();
        String reason = status.description() != null ? status.description() : e.getMessage();
        String state = switch (status.code()) {
            case UNAUTHENTICATED -> "28000";
            case UNAUTHORIZED -> "42501";
            default -> null;
        };
        return new SQLException(what + " (" + status.code() + "): " + reason, state, e);
    }

    /**
     * The ticket for a query: JSON the Flight server reads. {@code token}, {@code role} and
     * {@code kmsKey} are each sent only when the connection has one.
     */
    static byte[] buildTicket(
            String queryText, String token, String role, String kmsKey, Map<String, Object> variables) {
        JsonObject ticket = new JsonObject();
        ticket.addProperty("query", queryText);
        if (token != null) {
            ticket.addProperty("token", token);
        }
        if (role != null) {
            ticket.addProperty("role", role);
        }
        if (kmsKey != null) {
            ticket.addProperty("kms_key", kmsKey);
        }
        if (variables != null && !variables.isEmpty()) {
            ticket.add("variables", new Gson().toJsonTree(variables));
        }
        return ticket.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        closeQuietly(client, allocator);
    }

    private static void closeStream(FlightStream stream) {
        if (stream == null) return;
        try {
            stream.close();
        } catch (Exception closing) {
            log.fine("closing a failed Flight stream: " + closing);
        }
    }

    private static void closeQuietly(FlightClient client, BufferAllocator allocator) {
        try {
            if (client != null) client.close();
        } catch (Exception closing) {
            log.fine("closing the Flight client: " + closing);
        }
        try {
            allocator.close();
        } catch (RuntimeException closing) {
            // Arrow reports buffers still held by a stream the caller has not closed yet.
            log.fine("closing the Flight allocator: " + closing);
        }
    }

    /** The Flight port that goes with an HTTP port: 8815 beside 8001, offset alike elsewhere. */
    static int deriveFlightPort(int httpPort) {
        return httpPort + (DEFAULT_FLIGHT_PORT - DEFAULT_HTTP_PORT);
    }
}
