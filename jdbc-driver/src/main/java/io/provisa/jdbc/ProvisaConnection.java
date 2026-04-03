package io.provisa.jdbc;

import com.google.gson.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;

/**
 * Provisa JDBC Connection.
 *
 * Authenticates against Provisa, discovers approved queries or registered tables,
 * and executes them via the HTTP API.
 *
 * Modes:
 *   approved — exposes approved queries as virtual views (default)
 *   catalog  — exposes registered tables for schema discovery (no query execution)
 */
public class ProvisaConnection extends AbstractConnection {

    String baseUrl;
    String role;
    String mode; // "approved" or "catalog"
    String authToken;
    private boolean closed = false;

    ProvisaConnection(String baseUrl, String user, String password, String mode) throws SQLException {
        this.baseUrl = baseUrl;
        this.mode = mode != null ? mode : "approved";

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
    }

    private JsonObject authenticate(String user, String password) throws Exception {
        JsonObject body = new JsonObject();
        body.addProperty("username", user);
        body.addProperty("password", password);

        HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + "/auth/login").openConnection();
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

    // ── Approved queries (mode=approved) ──

    /**
     * Fetch approved queries visible to this role.
     */
    List<ApprovedQuery> fetchApprovedQueries() throws SQLException {
        try {
            String gql = "{ persistedQueries { id stableId queryText status compiledSql } }";
            JsonObject result = executeGraphQL(baseUrl + "/admin/graphql", gql);
            JsonArray queries = result.getAsJsonObject("data")
                    .getAsJsonArray("persistedQueries");

            List<ApprovedQuery> approved = new ArrayList<>();
            for (JsonElement el : queries) {
                JsonObject q = el.getAsJsonObject();
                if (!"approved".equals(q.get("status").getAsString())) continue;
                String stableId = q.has("stableId") && !q.get("stableId").isJsonNull()
                        ? q.get("stableId").getAsString() : null;
                if (stableId == null) continue;
                approved.add(new ApprovedQuery(
                    stableId,
                    q.get("queryText").getAsString(),
                    q.has("compiledSql") ? q.get("compiledSql").getAsString() : ""
                ));
            }
            return approved;
        } catch (Exception e) {
            throw new SQLException("Failed to fetch approved queries: " + e.getMessage(), e);
        }
    }

    /**
     * Resolve root field names for a query via /data/compile.
     * Returns list of root field names (includes domain prefix).
     */
    List<String> resolveRootFields(String queryText) throws SQLException {
        try {
            JsonObject compiled = compileQuery(queryText);
            List<String> fields = new ArrayList<>();

            // Multi-root returns {"queries": [...]}
            if (compiled.has("queries")) {
                JsonArray queries = compiled.getAsJsonArray("queries");
                for (JsonElement el : queries) {
                    fields.add(el.getAsJsonObject().get("root_field").getAsString());
                }
            } else if (compiled.has("root_field")) {
                // Single root
                fields.add(compiled.get("root_field").getAsString());
            }
            return fields;
        } catch (Exception e) {
            throw new SQLException("Failed to resolve root fields: " + e.getMessage(), e);
        }
    }

    /**
     * Execute an approved query by stable ID, returning JSON results.
     */
    JsonObject executeApprovedQuery(String stableId, Map<String, Object> variables) throws SQLException {
        List<ApprovedQuery> queries = fetchApprovedQueries();
        ApprovedQuery match = null;
        for (ApprovedQuery q : queries) {
            if (q.stableId.equals(stableId)) {
                match = q;
                break;
            }
        }
        if (match == null) {
            throw new SQLException("Approved query not found: " + stableId);
        }

        try {
            JsonObject body = new JsonObject();
            body.addProperty("query", match.queryText);
            if (variables != null && !variables.isEmpty()) {
                body.add("variables", new Gson().toJsonTree(variables));
            }
            return executeGraphQL(baseUrl + "/data/graphql", body);
        } catch (Exception e) {
            throw new SQLException("Query execution failed: " + e.getMessage(), e);
        }
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

    // ── Compile ──

    /**
     * Compile a query to see its output columns and root fields.
     */
    JsonObject compileQuery(String queryText) throws SQLException {
        try {
            JsonObject body = new JsonObject();
            body.addProperty("query", queryText);

            HttpURLConnection conn = (HttpURLConnection) new URL(baseUrl + "/data/compile").openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("X-Provisa-Role", role);
            if (authToken != null) {
                conn.setRequestProperty("Authorization", "Bearer " + authToken);
            }
            conn.setDoOutput(true);
            conn.getOutputStream().write(body.toString().getBytes(StandardCharsets.UTF_8));

            String response = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            return JsonParser.parseString(response).getAsJsonObject();
        } catch (Exception e) {
            throw new SQLException("Compile failed: " + e.getMessage(), e);
        }
    }

    // ── HTTP helpers ──

    private JsonObject executeGraphQL(String endpoint, String query) throws Exception {
        JsonObject body = new JsonObject();
        body.addProperty("query", query);
        return executeGraphQL(endpoint, body);
    }

    private JsonObject executeGraphQL(String endpoint, JsonObject body) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(endpoint).openConnection();
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
        if ("catalog".equals(mode)) {
            throw new SQLException("mode=catalog is metadata-only; query execution is not supported");
        }
        return new ProvisaStatement(this);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new ProvisaDatabaseMetaData(this);
    }

    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }
    @Override public String getSchema() { return mode; }

    void checkClosed() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
    }

    // ── Data classes ──

    static class ApprovedQuery {
        final String stableId;
        final String queryText;
        final String compiledSql;

        ApprovedQuery(String stableId, String queryText, String compiledSql) {
            this.stableId = stableId;
            this.queryText = queryText;
            this.compiledSql = compiledSql;
        }
    }

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
