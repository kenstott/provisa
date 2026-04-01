package io.provisa.jdbc;

import java.sql.*;
import java.util.*;

/**
 * Database metadata — exposes approved queries as virtual tables.
 */
public class ProvisaDatabaseMetaData extends AbstractDatabaseMetaData {

    private final ProvisaConnection conn;

    ProvisaDatabaseMetaData(ProvisaConnection conn) {
        this.conn = conn;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        List<ProvisaConnection.ApprovedQuery> queries = conn.fetchApprovedQueries();

        List<String> columns = Arrays.asList(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"
        );
        List<Map<String, Object>> rows = new ArrayList<>();
        for (ProvisaConnection.ApprovedQuery q : queries) {
            if (tableNamePattern != null && !tableNamePattern.equals("%")) {
                if (!q.stableId.contains(tableNamePattern)) continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("TABLE_CAT", "provisa");
            row.put("TABLE_SCHEM", "approved");
            row.put("TABLE_NAME", q.stableId);
            row.put("TABLE_TYPE", "VIEW");
            row.put("REMARKS", "Approved query: " + extractName(q.queryText));
            rows.add(row);
        }
        return new ProvisaResultSet(columns, rows);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        // Find the approved query
        List<ProvisaConnection.ApprovedQuery> queries = conn.fetchApprovedQueries();
        ProvisaConnection.ApprovedQuery match = null;
        for (ProvisaConnection.ApprovedQuery q : queries) {
            if (q.stableId.equals(tableNamePattern)) {
                match = q;
                break;
            }
        }

        List<String> columns = Arrays.asList(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
            "DATA_TYPE", "TYPE_NAME", "ORDINAL_POSITION"
        );
        List<Map<String, Object>> rows = new ArrayList<>();

        if (match != null) {
            // Execute a compile to get column metadata
            try {
                com.google.gson.JsonObject compiled = conn.compileQuery(match.queryText);
                // The compile endpoint doesn't return column types directly,
                // so we execute with LIMIT 0 to get the schema from the response
                com.google.gson.JsonObject body = new com.google.gson.JsonObject();
                body.addProperty("query", match.queryText);
                // For now, infer columns from the query text (field names)
                String sql = compiled.has("sql") ? compiled.get("sql").getAsString() : "";
                // Extract SELECT columns from compiled SQL
                if (sql.startsWith("SELECT ")) {
                    String selectPart = sql.substring(7, sql.indexOf(" FROM "));
                    String[] cols = selectPart.split(",\\s*");
                    int ordinal = 1;
                    for (String col : cols) {
                        // Strip aliases and quotes
                        String colName = col.trim()
                            .replaceAll("\"[^\"]*\"\\.", "")  // remove table alias
                            .replaceAll("\"", "")              // remove quotes
                            .replaceAll(".*\\.", "");          // remove remaining prefix
                        Map<String, Object> row = new LinkedHashMap<>();
                        row.put("TABLE_CAT", "provisa");
                        row.put("TABLE_SCHEM", "approved");
                        row.put("TABLE_NAME", match.stableId);
                        row.put("COLUMN_NAME", colName);
                        row.put("DATA_TYPE", Types.VARCHAR);
                        row.put("TYPE_NAME", "VARCHAR");
                        row.put("ORDINAL_POSITION", ordinal++);
                        rows.add(row);
                    }
                }
            } catch (Exception e) {
                // If compile fails, return empty columns
            }
        }

        return new ProvisaResultSet(columns, rows);
    }

    private String extractName(String queryText) {
        // Extract operation name from "query MyName { ... }"
        if (queryText == null) return "";
        java.util.regex.Matcher m = java.util.regex.Pattern
            .compile("(?:query|mutation)\\s+(\\w+)")
            .matcher(queryText);
        return m.find() ? m.group(1) : "";
    }

    @Override public String getDatabaseProductName() { return "Provisa"; }
    @Override public String getDatabaseProductVersion() { return "0.1.0"; }
    @Override public String getDriverName() { return "Provisa JDBC Driver"; }
    @Override public String getDriverVersion() { return "0.1.0"; }
    @Override public int getDriverMajorVersion() { return 0; }
    @Override public int getDriverMinorVersion() { return 1; }
    @Override public String getURL() { return conn.baseUrl; }
    @Override public String getUserName() { return conn.role; }
    @Override public boolean isReadOnly() { return true; }
    @Override public Connection getConnection() { return conn; }
}
