package io.provisa.jdbc;

import com.google.gson.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/**
 * Provisa JDBC Connection.
 *
 * Authenticates against Provisa, discovers registered tables, and executes
 * SQL via the HTTP API.
 *
 * Mode:
 *   catalog — exposes registered tables for schema discovery and routes SQL
 *             through the /data/sql governance endpoint.
 */
public class ProvisaConnection extends AbstractConnection {

    String baseUrl;
    String user;
    String role; // the role this connection REQUESTS; null = the server derives it from the identity
    String mode; // "catalog"
    String authToken; // the session token sign-in returned; null on a server with no password sign-in
    FlightTransport flightTransport; // null when the Flight port was unreachable at connect
    EnvelopeDecryptor encryptionService; // REQ-690: client-side column decrypt (null = disabled)
    String kmsKeyArn; // REQ-693: proof-of-client-decrypt sent to the high-security gate
    private boolean closed = false;

    /**
     * Sign in and open the connection.
     *
     * <p>The user and password are exchanged at {@code /auth/login} for a session token, which
     * every later request carries. A refused sign-in is raised with the server's status and
     * reason: a connection is never opened as someone the server did not authenticate.
     *
     * <p>{@code requestedRole} (the {@code role} connection property) asks to act as one role the
     * user holds, or a comma-separated set of them; with none the server derives the role from
     * the identity. On a server with no password sign-in ({@code /auth/login} answers 404: no
     * auth provider is configured) there is no identity to derive it from, and the user name is
     * the requested role, as that server takes every role at face value (REQ-131).
     *
     * <p>{@code flightPort} (the {@code flight_port} connection property) names the Flight port
     * when it is not the conventional one beside the HTTP port (8815 beside 8001).
     */
    ProvisaConnection(
            String baseUrl, String user, String password, String mode, String requestedRole,
            Integer flightPort) throws SQLException {
        this.baseUrl = baseUrl;
        this.mode = mode != null ? mode : "catalog";
        this.user = user;
        this.authToken = signIn(user, password);
        if (requestedRole != null && !requestedRole.isEmpty()) {
            this.role = requestedRole;
        } else if (this.authToken == null && user != null && !user.isEmpty()) {
            this.role = user;
        } else {
            this.role = null;
        }

        // REQ-293: queries run over Flight when its port answers now, over HTTP only when it
        // is unreachable now (logged at INFO by FlightTransport.connect).
        URI base = URI.create(baseUrl);
        int resolvedFlightPort = flightPort != null
            ? flightPort
            : FlightTransport.deriveFlightPort(base.getPort() == -1 ? 8001 : base.getPort());
        this.flightTransport = FlightTransport.connect(base.getHost(), resolvedFlightPort);
    }

    /**
     * Configure client-side decryption from connection params (REQ-690, REQ-694).
     *
     * <p>{@code kms_provider}=local uses a base64 {@code kms_master_key} (tests / local). The
     * cloud CMK providers (aws/azure/gcp — REQ-694) unwrap the DEK via their cloud KMS Decrypt
     * API and require their SDK on the runtime classpath; {@link KmsProviders} fails loud if the
     * named provider's SDK is absent, and an unknown provider fails closed rather than silently
     * disabling decryption. Documented in {@code docs/arch/jdbc-client-side-encryption.md}.
     */
    public void configureEncryption(String kmsProvider, String kmsKeyArn, String kmsMasterKeyB64)
            throws SQLException {
        if (kmsProvider == null && kmsKeyArn == null) {
            return; // decryption not requested
        }
        this.kmsKeyArn = kmsKeyArn;
        Map<String, String> params = new HashMap<>();
        if (kmsMasterKeyB64 != null) {
            params.put("kms_master_key", kmsMasterKeyB64);
        }
        KmsProvider provider = KmsProviders.forName(kmsProvider, kmsKeyArn, params);
        this.encryptionService = new EnvelopeDecryptor(provider, 300);
    }

    /**
     * Exchange the user and password for a session token at {@code /auth/login}.
     *
     * @return the token, or null when the server has no password sign-in (404)
     * @throws SQLException when the server refuses the sign-in, cannot be reached, or answers
     *         without a token
     */
    private String signIn(String user, String password) throws SQLException {
        JsonObject body = new JsonObject();
        body.addProperty("username", user);
        body.addProperty("password", password);

        int status;
        String response;
        try {
            HttpURLConnection conn =
                (HttpURLConnection) URI.create(baseUrl + "/auth/login").toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.getOutputStream().write(body.toString().getBytes(StandardCharsets.UTF_8));
            status = conn.getResponseCode();
            java.io.InputStream stream = status < 400 ? conn.getInputStream() : conn.getErrorStream();
            response = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            throw new SQLException("Sign-in failed: cannot reach " + baseUrl + ": " + e.getMessage(), e);
        }

        if (status == 404) {
            return null;
        }
        if (status != 200) {
            // 28000: invalid authorization specification
            throw new SQLException(
                "Sign-in failed (HTTP " + status + "): " + serverReason(response), "28000");
        }
        JsonElement token = JsonParser.parseString(response).getAsJsonObject().get("access_token");
        if (token == null || token.isJsonNull()) {
            throw new SQLException("Sign-in failed: the server answered without an access_token");
        }
        return token.getAsString();
    }

    /** The reason in a server error body: its {@code detail} when it is the JSON error shape, else the text. */
    static String serverReason(String body) {
        try {
            JsonElement parsed = JsonParser.parseString(body);
            if (parsed.isJsonObject() && parsed.getAsJsonObject().has("detail")) {
                JsonElement detail = parsed.getAsJsonObject().get("detail");
                return detail.isJsonPrimitive() ? detail.getAsString() : detail.toString();
            }
        } catch (JsonSyntaxException e) {
            // not JSON: the text is the reason
        }
        return body;
    }

    /** The headers every request carries: the requested role (when one is) and the session token. */
    private void identify(HttpURLConnection conn) {
        if (role != null) {
            conn.setRequestProperty("X-Provisa-Role", role);
        }
        if (authToken != null) {
            conn.setRequestProperty("Authorization", "Bearer " + authToken);
        }
    }

    // ── Registered tables (mode=catalog) ──

    /**
     * Fetch registered tables with columns, aliases, and descriptions.
     */
    List<RegisteredTable> fetchRegisteredTables() throws SQLException {
        try {
            // Only fields the admin schema has and this driver reads
            // (tests/unit/test_jdbc_driver_admin_queries.py validates the text against it).
            String gql = "{ tables { id domainId tableName alias description " +
                    "columns { columnName alias description } } }";
            JsonObject result = executeGraphQL(baseUrl + "/admin/graphql", gql);
            JsonArray tablesArr = result.getAsJsonObject("data").getAsJsonArray("tables");

            List<RegisteredTable> tables = new ArrayList<>();
            for (JsonElement el : tablesArr) {
                JsonObject t = el.getAsJsonObject();
                List<RegisteredColumn> cols = new ArrayList<>();
                for (JsonElement colEl : t.getAsJsonArray("columns")) {
                    JsonObject c = colEl.getAsJsonObject();
                    cols.add(new RegisteredColumn(
                        c.get("columnName").getAsString(),
                        c.has("alias") && !c.get("alias").isJsonNull() ? c.get("alias").getAsString() : null,
                        c.has("description") && !c.get("description").isJsonNull() ? c.get("description").getAsString() : null
                    ));
                }
                tables.add(new RegisteredTable(
                    t.get("id").getAsInt(),
                    t.get("domainId").getAsString(),
                    t.get("tableName").getAsString(),
                    t.has("alias") && !t.get("alias").isJsonNull() ? t.get("alias").getAsString() : null,
                    t.has("description") && !t.get("description").isJsonNull() ? t.get("description").getAsString() : null,
                    cols
                ));
            }
            return tables;
        } catch (Exception e) {
            throw new SQLException("Failed to fetch registered tables: " + e.getMessage(), e);
        }
    }

    /**
     * Fetch semantic relationships for PK/FK metadata.
     */
    List<Relationship> fetchRelationships() throws SQLException {
        try {
            String gql = "{ relationships { id sourceTableId targetTableId " +
                    "sourceTableName targetTableName sourceColumn targetColumn cardinality } }";
            JsonObject result = executeGraphQL(baseUrl + "/admin/graphql", gql);
            JsonArray relsArr = result.getAsJsonObject("data").getAsJsonArray("relationships");

            List<Relationship> rels = new ArrayList<>();
            for (JsonElement el : relsArr) {
                JsonObject r = el.getAsJsonObject();
                // A relationship with no target table or column (one defined by a condition,
                // not a key pair) is not a foreign key and has no place in key metadata.
                if (r.get("targetTableId").isJsonNull() || r.get("targetColumn").isJsonNull()) {
                    continue;
                }
                rels.add(new Relationship(
                    r.get("id").getAsString(),
                    r.get("sourceTableId").getAsInt(),
                    r.get("targetTableId").getAsInt(),
                    r.get("sourceTableName").getAsString(),
                    r.get("targetTableName").getAsString(),
                    r.get("sourceColumn").getAsString(),
                    r.get("targetColumn").getAsString(),
                    r.get("cardinality").getAsString()
                ));
            }
            return rels;
        } catch (Exception e) {
            throw new SQLException("Failed to fetch relationships: " + e.getMessage(), e);
        }
    }

    // ── HTTP helpers ──

    private JsonObject executeGraphQL(String endpoint, String query) throws Exception {
        JsonObject body = new JsonObject();
        body.addProperty("query", query);
        return executeGraphQL(endpoint, body);
    }

    private JsonObject executeGraphQL(String endpoint, JsonObject body) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) URI.create(endpoint).toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        identify(conn);
        conn.setDoOutput(true);
        conn.getOutputStream().write(body.toString().getBytes(StandardCharsets.UTF_8));

        if (conn.getResponseCode() != 200) {
            String error = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            throw new SQLException("HTTP " + conn.getResponseCode() + ": " + error);
        }

        String response = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return JsonParser.parseString(response).getAsJsonObject();
    }

    // ── Connection methods ──

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new ProvisaStatement(this);
    }

    /**
     * Execute raw SQL through the /data/sql Stage 2 governance endpoint.
     * Used by catalog mode to route arbitrary SQL through RLS, masking, and visibility.
     *
     * @return list of rows, each as a map of column name → value
     */
    List<Map<String, Object>> executeSqlEndpoint(String sql) throws SQLException {
        try {
            JsonObject body = new JsonObject();
            // The role travels in X-Provisa-Role only: the server refuses a body role that
            // differs from the role the request runs as.
            body.addProperty("sql", sql);

            HttpURLConnection conn = (HttpURLConnection)
                URI.create(baseUrl + "/data/sql").toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            identify(conn);
            conn.setDoOutput(true);
            conn.getOutputStream().write(body.toString().getBytes(StandardCharsets.UTF_8));

            if (conn.getResponseCode() != 200) {
                String error = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
                throw new SQLException("HTTP " + conn.getResponseCode() + ": " + serverReason(error));
            }

            String response = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            JsonObject result = JsonParser.parseString(response).getAsJsonObject();
            JsonArray rows = result.getAsJsonObject("data").getAsJsonArray("sql");

            List<Map<String, Object>> out = new ArrayList<>();
            for (JsonElement el : rows) {
                JsonObject row = el.getAsJsonObject();
                Map<String, Object> map = new LinkedHashMap<>();
                for (String key : row.keySet()) {
                    JsonElement val = row.get(key);
                    if (val == null || val.isJsonNull()) {
                        map.put(key, null);
                    } else if (val.isJsonPrimitive()) {
                        JsonPrimitive p = val.getAsJsonPrimitive();
                        if (p.isNumber()) map.put(key, p.getAsNumber());
                        else if (p.isBoolean()) map.put(key, p.getAsBoolean());
                        else map.put(key, p.getAsString());
                    } else {
                        map.put(key, val.toString());
                    }
                }
                out.add(map);
            }
            return out;
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("SQL endpoint execution failed: " + e.getMessage(), e);
        }
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new ProvisaDatabaseMetaData(this);
    }

    @Override public void close() {
        closed = true;
        if (flightTransport != null) {
            flightTransport.close();
            flightTransport = null;
        }
    }
    @Override public boolean isClosed() { return closed; }
    @Override public String getSchema() { return mode; }

    void checkClosed() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
    }

    // ── Data classes ──

    static class RegisteredTable {
        final int id;
        final String domainId;
        final String tableName;
        final String alias;
        final String description;
        final List<RegisteredColumn> columns;

        RegisteredTable(int id, String domainId, String tableName, String alias,
                       String description, List<RegisteredColumn> columns) {
            this.id = id;
            this.domainId = domainId;
            this.tableName = tableName;
            this.alias = alias;
            this.description = description;
            this.columns = columns;
        }

        /** Display name: alias if set, otherwise raw table name. */
        String displayName() { return alias != null ? alias : tableName; }
    }

    static class RegisteredColumn {
        final String columnName;
        final String alias;
        final String description;

        RegisteredColumn(String columnName, String alias, String description) {
            this.columnName = columnName;
            this.alias = alias;
            this.description = description;
        }

        /** Display name: alias if set, otherwise raw column name. */
        String displayName() { return alias != null ? alias : columnName; }
    }

    static class Relationship {
        final String id;
        final int sourceTableId;
        final int targetTableId;
        final String sourceTableName;
        final String targetTableName;
        final String sourceColumn;
        final String targetColumn;
        final String cardinality;

        Relationship(String id, int sourceTableId, int targetTableId,
                    String sourceTableName, String targetTableName,
                    String sourceColumn, String targetColumn, String cardinality) {
            this.id = id;
            this.sourceTableId = sourceTableId;
            this.targetTableId = targetTableId;
            this.sourceTableName = sourceTableName;
            this.targetTableName = targetTableName;
            this.sourceColumn = sourceColumn;
            this.targetColumn = targetColumn;
            this.cardinality = cardinality;
        }
    }
}
