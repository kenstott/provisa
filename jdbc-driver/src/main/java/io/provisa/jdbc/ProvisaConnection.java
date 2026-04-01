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
 * Authenticates against Provisa, discovers approved queries as tables,
 * and executes them via the HTTP API.
 */
public class ProvisaConnection extends AbstractConnection {

    final String baseUrl;
    final String role;
    String authToken;
    private boolean closed = false;

    ProvisaConnection(String baseUrl, String user, String password) throws SQLException {
        this.baseUrl = baseUrl;

        // Authenticate to get role and token
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
     * Execute an approved query by stable ID, returning JSON results.
     */
    JsonObject executeApprovedQuery(String stableId, Map<String, Object> variables) throws SQLException {
        // Look up the query text by stable ID
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

    /**
     * Compile a query to see its output columns (for metadata).
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

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new ProvisaStatement(this);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new ProvisaDatabaseMetaData(this);
    }

    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }

    void checkClosed() throws SQLException {
        if (closed) throw new SQLException("Connection is closed");
    }

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
}
