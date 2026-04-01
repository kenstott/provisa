package io.provisa.jdbc;

import com.google.gson.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.regex.*;

/**
 * Provisa JDBC Statement.
 *
 * Executes approved queries via redirect to Arrow IPC, streaming results
 * batch-by-batch without loading the full result into memory.
 *
 * SQL format: SELECT * FROM <stable_id> [WHERE col = 'val' [AND ...]]
 */
public class ProvisaStatement extends AbstractStatement {

    private final ProvisaConnection conn;
    private ResultSet currentResultSet;
    private boolean closed = false;

    private static final Pattern SQL_PATTERN = Pattern.compile(
        "SELECT\\s+(.+?)\\s+FROM\\s+([\\w\\-]+)(?:\\s+WHERE\\s+(.+))?",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    private static final Pattern WHERE_CLAUSE = Pattern.compile(
        "(\\w+)\\s*=\\s*'([^']*)'|(\\w+)\\s*=\\s*(\\S+)"
    );

    ProvisaStatement(ProvisaConnection conn) {
        this.conn = conn;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (closed) throw new SQLException("Statement is closed");

        Matcher m = SQL_PATTERN.matcher(sql.trim());
        if (!m.matches()) {
            throw new SQLException(
                "Unsupported SQL syntax. Use: SELECT * FROM <query_stable_id> [WHERE col = 'val']"
            );
        }

        String stableId = m.group(2);
        String whereClause = m.group(3);

        Map<String, Object> variables = new HashMap<>();
        if (whereClause != null) {
            Matcher wm = WHERE_CLAUSE.matcher(whereClause);
            while (wm.find()) {
                String col = wm.group(1) != null ? wm.group(1) : wm.group(3);
                String val = wm.group(2) != null ? wm.group(2) : wm.group(4);
                variables.put(col, val);
            }
        }

        // Look up the approved query
        List<ProvisaConnection.ApprovedQuery> queries = conn.fetchApprovedQueries();
        ProvisaConnection.ApprovedQuery match = null;
        for (ProvisaConnection.ApprovedQuery q : queries) {
            if (q.stableId.equals(stableId)) {
                match = q;
                break;
            }
        }
        if (match == null) {
            throw new SQLException("Approved query not found: " + stableId);
        }

        // Try Arrow IPC redirect first, fall back to JSON
        try {
            currentResultSet = executeWithArrowRedirect(match, variables);
        } catch (Exception e) {
            // Fall back to JSON inline
            currentResultSet = executeWithJson(match, variables);
        }

        return currentResultSet;
    }

    /**
     * Execute via Arrow IPC redirect — streaming, unbounded.
     */
    private ResultSet executeWithArrowRedirect(
        ProvisaConnection.ApprovedQuery query, Map<String, Object> variables
    ) throws Exception {
        // Request redirect to Arrow IPC
        JsonObject body = new JsonObject();
        body.addProperty("query", query.queryText);
        if (variables != null && !variables.isEmpty()) {
            body.add("variables", new Gson().toJsonTree(variables));
        }

        HttpURLConnection http = (HttpURLConnection)
            new URL(conn.baseUrl + "/data/graphql").openConnection();
        http.setRequestMethod("POST");
        http.setRequestProperty("Content-Type", "application/json");
        http.setRequestProperty("X-Provisa-Role", conn.role);
        http.setRequestProperty("X-Provisa-Redirect-Format",
            "application/vnd.apache.arrow.stream");
        http.setRequestProperty("X-Provisa-Redirect-Threshold", "0");
        if (conn.authToken != null) {
            http.setRequestProperty("Authorization", "Bearer " + conn.authToken);
        }
        http.setDoOutput(true);
        http.getOutputStream().write(body.toString().getBytes(StandardCharsets.UTF_8));

        if (http.getResponseCode() != 200) {
            throw new SQLException("HTTP " + http.getResponseCode());
        }

        String response = new String(http.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        JsonObject result = JsonParser.parseString(response).getAsJsonObject();

        // Check for redirect URL
        if (!result.has("redirect")) {
            throw new Exception("No redirect — fall back to JSON");
        }

        String redirectUrl = result.getAsJsonObject("redirect")
            .get("redirect_url").getAsString();

        // Open HTTP stream to the Arrow IPC file
        HttpURLConnection arrowConn = (HttpURLConnection) new URL(redirectUrl).openConnection();
        arrowConn.setRequestMethod("GET");

        if (arrowConn.getResponseCode() != 200) {
            throw new SQLException("Failed to fetch Arrow data: " + arrowConn.getResponseCode());
        }

        InputStream arrowStream = arrowConn.getInputStream();
        return new ArrowStreamResultSet(arrowStream);
    }

    /**
     * Fallback: execute via JSON inline (loads full result into memory).
     */
    private ResultSet executeWithJson(
        ProvisaConnection.ApprovedQuery query, Map<String, Object> variables
    ) throws SQLException {
        JsonObject result = conn.executeApprovedQuery(query.stableId, variables);
        JsonObject data = result.getAsJsonObject("data");
        if (data == null) {
            throw new SQLException("No data in response");
        }

        String rootField = data.keySet().iterator().next();
        JsonElement rootData = data.get(rootField);

        if (rootData == null || rootData.isJsonNull() || !rootData.isJsonArray()) {
            return new ProvisaResultSet(new ArrayList<>(), new ArrayList<>());
        }

        JsonArray rows = rootData.getAsJsonArray();
        List<String> columns = new ArrayList<>();
        if (rows.size() > 0) {
            columns.addAll(rows.get(0).getAsJsonObject().keySet());
        }

        List<Map<String, Object>> data2 = new ArrayList<>();
        for (JsonElement el : rows) {
            JsonObject row = el.getAsJsonObject();
            Map<String, Object> map = new LinkedHashMap<>();
            for (String col : columns) {
                JsonElement val = row.get(col);
                if (val == null || val.isJsonNull()) map.put(col, null);
                else if (val.isJsonPrimitive()) {
                    JsonPrimitive p = val.getAsJsonPrimitive();
                    if (p.isNumber()) map.put(col, p.getAsNumber());
                    else if (p.isBoolean()) map.put(col, p.getAsBoolean());
                    else map.put(col, p.getAsString());
                } else map.put(col, val.toString());
            }
            data2.add(map);
        }

        return new ProvisaResultSet(columns, data2);
    }

    @Override public ResultSet getResultSet() { return currentResultSet; }
    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }
    @Override public Connection getConnection() { return conn; }
}
