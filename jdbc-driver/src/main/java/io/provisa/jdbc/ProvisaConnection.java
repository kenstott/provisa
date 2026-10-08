package io.provisa.jdbc;

import com.google.gson.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

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

    // ── The catalog (mode=catalog) ──

    /**
     * The tables this connection's role is served, with their columns and keys (REQ-128).
     *
     * <p>Read from the server's role-narrowed catalog, which lists what the signed-in role may
     * see and nothing else: the Arrow Flight listing, or on a connection whose Flight port was
     * unreachable when it was opened the same listing over HTTP ({@link #httpCatalog()}). Both
     * are one catalog from one builder, so the metadata does not depend on which port answered.
     */
    List<RegisteredTable> fetchRegisteredTables() throws SQLException {
        List<FlightTransport.CatalogTable> listed =
            flightTransport != null ? flightTransport.catalog(authToken, role) : httpCatalog();
        List<RegisteredTable> tables = new ArrayList<>();
        int id = 1;
        for (FlightTransport.CatalogTable t : listed) {
            List<RegisteredColumn> cols = new ArrayList<>();
            for (FlightTransport.CatalogColumn col : t.columns()) {
                cols.add(new RegisteredColumn(
                    col.name(), null, col.description(), col.primaryKey(),
                    col.referencesTable(), col.referencesColumn(), col.sqlType()));
            }
            tables.add(new RegisteredTable(id++, t.domain(), t.table(), null, t.description(), cols));
        }
        return tables;
    }

    /**
     * The role's catalog over HTTP ({@code /data/catalog}): the same listing the Flight port
     * gives, each table as its path and its serialized Arrow schema, read by the one function
     * that reads the Flight listing — so a connection shows the same names, types and keys
     * whichever port answered.
     */
    private List<FlightTransport.CatalogTable> httpCatalog() throws SQLException {
        JsonObject listing;
        try {
            HttpURLConnection conn =
                (HttpURLConnection) URI.create(baseUrl + "/data/catalog").toURL().openConnection();
            conn.setRequestMethod("GET");
            identify(conn);
            int status = conn.getResponseCode();
            java.io.InputStream stream = status < 400 ? conn.getInputStream() : conn.getErrorStream();
            String body = stream == null ? "" : new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            if (status != 200) {
                throw new SQLException(
                    "Reading the catalog failed (HTTP " + status + "): " + serverReason(body),
                    status == 401 || status == 403 ? "28000" : null);
            }
            listing = JsonParser.parseString(body).getAsJsonObject();
        } catch (java.io.IOException e) {
            throw new SQLException("Reading the catalog failed: " + e.getMessage(), e);
        }
        List<FlightTransport.CatalogTable> tables = new ArrayList<>();
        for (JsonElement entry : listing.getAsJsonArray("tables")) {
            JsonObject table = entry.getAsJsonObject();
            JsonArray path = table.getAsJsonArray("path");
            byte[] serialized = java.util.Base64.getDecoder().decode(table.get("schema").getAsString());
            Schema schema;
            try {
                schema = MessageSerializer.deserializeSchema(new ReadChannel(
                    java.nio.channels.Channels.newChannel(new java.io.ByteArrayInputStream(serialized))));
            } catch (java.io.IOException e) {
                throw new SQLException("The catalog entry " + path + " carries no readable schema", e);
            }
            tables.add(FlightTransport.catalogTable(
                path.get(0).getAsString(), path.get(1).getAsString(), schema));
        }
        return tables;
    }

    /**
     * The to-one relationships between the tables this role is served, read off the catalog's
     * key metadata: a column that refers to another table's column.
     */
    List<Relationship> fetchRelationships() throws SQLException {
        List<RegisteredTable> tables = fetchRegisteredTables();
        Map<String, Integer> ids = new HashMap<>();
        for (RegisteredTable t : tables) ids.put(t.tableName, t.id);
        List<Relationship> rels = new ArrayList<>();
        for (RegisteredTable t : tables) {
            for (RegisteredColumn col : t.columns) {
                if (col.referencesTable == null) continue;
                Integer target = ids.get(col.referencesTable);
                if (target == null) continue; // the catalog lists a reference only with its target
                rels.add(new Relationship(
                    t.tableName + "." + col.columnName, t.id, target, t.tableName,
                    col.referencesTable, col.columnName, col.referencesColumn, "many-to-one"));
            }
        }
        return rels;
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
        final boolean primaryKey;
        final String referencesTable; // null unless the column refers to another table's column
        final String referencesColumn;
        final int sqlType; // java.sql.Types

        RegisteredColumn(String columnName, String alias, String description) {
            this(columnName, alias, description, false, null, null);
        }

        RegisteredColumn(String columnName, String alias, String description, boolean primaryKey,
                         String referencesTable, String referencesColumn) {
            this(columnName, alias, description, primaryKey, referencesTable, referencesColumn, Types.VARCHAR);
        }

        RegisteredColumn(String columnName, String alias, String description, boolean primaryKey,
                         String referencesTable, String referencesColumn, int sqlType) {
            this.sqlType = sqlType;
            this.columnName = columnName;
            this.alias = alias;
            this.description = description;
            this.primaryKey = primaryKey;
            this.referencesTable = referencesTable;
            this.referencesColumn = referencesColumn;
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
