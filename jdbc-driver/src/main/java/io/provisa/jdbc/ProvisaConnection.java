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
    String role;
    String mode; // "catalog"
    String authToken;
    FlightTransport flightTransport; // null if Flight unavailable
    EnvelopeDecryptor encryptionService; // REQ-690: client-side column decrypt (null = disabled)
    String kmsKeyArn; // REQ-693: proof-of-client-decrypt sent to the high-security gate
    private boolean closed = false;

    ProvisaConnection(String baseUrl, String user, String password, String mode) throws SQLException {
        this.baseUrl = baseUrl;
        this.mode = mode != null ? mode : "catalog";

        String resolvedRole = user;
        String resolvedToken = null;
        try {
            JsonObject authResult = authenticate(user, password);
            resolvedRole = authResult.has("role") ? authResult.get("role").getAsString() : user;
            resolvedToken = authResult.has("token") ? authResult.get("token").getAsString() : null;
        } catch (Exception e) {
            // Fall back to using username as role (test mode)
        }
        this.role = resolvedRole;
        this.authToken = resolvedToken;

        // Attempt Flight connection (silent fallback to HTTP if unavailable)
        String host = baseUrl.replaceFirst("^https?://", "").split(":")[0];
        int port = 8001;
        try {
            port = Integer.parseInt(baseUrl.replaceFirst("^https?://", "").split(":")[1].split("/")[0]);
        } catch (Exception ignored) {}
        this.flightTransport = FlightTransport.tryConnect(host, port, this.role);
    }

    /**
     * Configure client-side decryption from connection params (REQ-690, REQ-694).
     *
     * <p>{@code kms_provider}=local uses a base64 {@code kms_master_key} (tests / local). The
     * cloud CMK providers (aws/azure/gcp — REQ-694) require their SDK on the classpath and are
     * documented in {@code docs/arch/jdbc-client-side-encryption.md}; an unknown provider fails
     * closed rather than silently disabling decryption.
     */
    public void configureEncryption(String kmsProvider, String kmsKeyArn, String kmsMasterKeyB64)
            throws SQLException {
        if (kmsProvider == null && kmsKeyArn == null) {
            return; // decryption not requested
        }
        this.kmsKeyArn = kmsKeyArn;
        String p = kmsProvider == null ? "" : kmsProvider.toLowerCase();
        if ("local".equals(p)) {
            if (kmsMasterKeyB64 == null) {
                throw new SQLException("kms_provider=local requires kms_master_key (base64 32-byte key)");
            }
            byte[] master = Base64.getDecoder().decode(kmsMasterKeyB64);
            this.encryptionService = new EnvelopeDecryptor(new LocalKmsProvider(master), 300);
        } else if ("aws".equals(p) || "azure".equals(p) || "gcp".equals(p)) {
            throw new SQLException(
                "kms_provider=" + p + " requires the cloud SDK on the classpath; see "
                + "docs/arch/jdbc-client-side-encryption.md (REQ-690 e2e, target 2027-Q1)");
        } else {
            throw new SQLException("Unknown kms_provider: " + kmsProvider);
        }
    }

    private JsonObject authenticate(String user, String password) throws Exception {
        JsonObject body = new JsonObject();
        body.addProperty("username", user);
        body.addProperty("password", password);

        HttpURLConnection conn = (HttpURLConnection) URI.create(baseUrl + "/auth/login").toURL().openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        conn.getOutputStream().write(body.toString().getBytes(StandardCharsets.UTF_8));

        if (conn.getResponseCode() != 200) {
            throw new SQLException("Authentication failed: " + conn.getResponseCode());
        }

        String response = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return JsonParser.parseString(response).getAsJsonObject();
    }

    // ── Registered tables (mode=catalog) ──

    /**
     * Fetch registered tables with columns, aliases, and descriptions.
     */
    List<RegisteredTable> fetchRegisteredTables() throws SQLException {
        try {
            String gql = "{ tables { id sourceId domainId schemaName tableName governance " +
                    "alias description columns { id columnName visibleTo writableBy " +
                    "unmaskedTo maskType alias description } } }";
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
        conn.setRequestProperty("X-Provisa-Role", role);
        if (authToken != null) {
            conn.setRequestProperty("Authorization", "Bearer " + authToken);
        }
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
            body.addProperty("sql", sql);
            body.addProperty("role", role);

            HttpURLConnection conn = (HttpURLConnection)
                URI.create(baseUrl + "/data/sql").toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("X-Provisa-Role", role);
            if (authToken != null) {
                conn.setRequestProperty("Authorization", "Bearer " + authToken);
            }
            conn.setDoOutput(true);
            conn.getOutputStream().write(body.toString().getBytes(StandardCharsets.UTF_8));

            if (conn.getResponseCode() != 200) {
                String error = new String(conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
                throw new SQLException("HTTP " + conn.getResponseCode() + ": " + error);
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
