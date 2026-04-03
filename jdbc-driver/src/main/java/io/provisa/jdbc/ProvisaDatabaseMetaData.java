package io.provisa.jdbc;

import java.sql.*;
import java.util.*;

/**
 * Database metadata — exposes tables/views depending on connection mode.
 *
 * mode=approved: Approved queries as virtual views, named {stableId}__{rootField}.
 *   Each root field in a multi-root query becomes its own view.
 *   Column metadata from /data/compile with aliases and descriptions.
 *
 * mode=catalog: Registered tables with aliases, descriptions, and domain schemas.
 *   Metadata-only — no query execution.
 *
 * Both modes: PK/FK relationships materialized from semantic relationships.
 */
public class ProvisaDatabaseMetaData extends AbstractDatabaseMetaData {

    private final ProvisaConnection conn;

    ProvisaDatabaseMetaData(ProvisaConnection conn) {
        this.conn = conn;
    }

    // ── getTables ──

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        if ("catalog".equals(conn.mode)) {
            return getCatalogTables(tableNamePattern);
        }
        return getApprovedTables(tableNamePattern);
    }

    private ResultSet getApprovedTables(String tableNamePattern) throws SQLException {
        List<ProvisaConnection.ApprovedQuery> queries = conn.fetchApprovedQueries();

        List<String> columns = Arrays.asList(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"
        );
        List<Map<String, Object>> rows = new ArrayList<>();
        for (ProvisaConnection.ApprovedQuery q : queries) {
            List<String> rootFields;
            try {
                rootFields = conn.resolveRootFields(q.queryText);
            } catch (Exception e) {
                rootFields = Collections.singletonList("unknown");
            }
            String operationName = extractName(q.queryText);

            for (String rootField : rootFields) {
                String viewName = q.stableId + "__" + rootField;
                if (tableNamePattern != null && !tableNamePattern.equals("%")) {
                    if (!viewName.contains(tableNamePattern)) continue;
                }
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("TABLE_CAT", "provisa");
                row.put("TABLE_SCHEM", "approved");
                row.put("TABLE_NAME", viewName);
                row.put("TABLE_TYPE", "VIEW");
                row.put("REMARKS", operationName.isEmpty() ? "Approved query" : "Approved query: " + operationName);
                rows.add(row);
            }
        }
        return new ProvisaResultSet(columns, rows);
    }

    private ResultSet getCatalogTables(String tableNamePattern) throws SQLException {
        List<ProvisaConnection.RegisteredTable> tables = conn.fetchRegisteredTables();

        List<String> columns = Arrays.asList(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"
        );
        List<Map<String, Object>> rows = new ArrayList<>();
        for (ProvisaConnection.RegisteredTable t : tables) {
            String displayName = t.displayName();
            if (tableNamePattern != null && !tableNamePattern.equals("%")) {
                if (!displayName.contains(tableNamePattern)) continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("TABLE_CAT", "provisa");
            row.put("TABLE_SCHEM", t.domainId);
            row.put("TABLE_NAME", displayName);
            row.put("TABLE_TYPE", "TABLE");
            row.put("REMARKS", t.description != null ? t.description : "");
            rows.add(row);
        }
        return new ProvisaResultSet(columns, rows);
    }

    // ── getColumns ──

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        if ("catalog".equals(conn.mode)) {
            return getCatalogColumns(tableNamePattern);
        }
        return getApprovedColumns(tableNamePattern);
    }

    private ResultSet getApprovedColumns(String viewName) throws SQLException {
        // Parse stableId and rootField from viewName: stableId__rootField
        String stableId = null;
        String targetRootField = null;
        if (viewName != null) {
            int sep = viewName.indexOf("__");
            if (sep > 0) {
                stableId = viewName.substring(0, sep);
                targetRootField = viewName.substring(sep + 2);
            }
        }

        // Find the matching approved query
        List<ProvisaConnection.ApprovedQuery> queries = conn.fetchApprovedQueries();
        ProvisaConnection.ApprovedQuery match = null;
        if (stableId != null) {
            for (ProvisaConnection.ApprovedQuery q : queries) {
                if (q.stableId.equals(stableId)) {
                    match = q;
                    break;
                }
            }
        }

        List<String> columns = Arrays.asList(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
            "DATA_TYPE", "TYPE_NAME", "ORDINAL_POSITION", "REMARKS"
        );
        List<Map<String, Object>> rows = new ArrayList<>();

        if (match != null) {
            try {
                com.google.gson.JsonObject compiled = conn.compileQuery(match.queryText);
                // Find the compile result matching the target root field
                com.google.gson.JsonObject fieldResult = null;
                if (compiled.has("queries") && targetRootField != null) {
                    for (com.google.gson.JsonElement el : compiled.getAsJsonArray("queries")) {
                        com.google.gson.JsonObject qr = el.getAsJsonObject();
                        if (targetRootField.equals(qr.get("root_field").getAsString())) {
                            fieldResult = qr;
                            break;
                        }
                    }
                } else {
                    fieldResult = compiled;
                }

                if (fieldResult != null) {
                    String sql = fieldResult.has("sql") ? fieldResult.get("sql").getAsString() : "";
                    if (sql.toUpperCase().contains("SELECT ") && sql.toUpperCase().contains(" FROM ")) {
                        String selectPart = sql.substring(
                            sql.toUpperCase().indexOf("SELECT ") + 7,
                            sql.toUpperCase().indexOf(" FROM ")
                        );
                        String[] cols = selectPart.split(",\\s*");
                        int ordinal = 1;
                        for (String col : cols) {
                            String colName = col.trim()
                                .replaceAll("\"[^\"]*\"\\.", "")
                                .replaceAll("\"", "")
                                .replaceAll(".*\\.", "")
                                .replaceAll("(?i)\\s+AS\\s+", " AS ")
                                .replaceAll(".*\\sAS\\s+", "");
                            // Look up alias and description from registered table metadata
                            String displayName = colName;
                            String remarks = "";
                            List<ProvisaConnection.RegisteredTable> regTables = conn.fetchRegisteredTables();
                            for (ProvisaConnection.RegisteredTable rt : regTables) {
                                for (ProvisaConnection.RegisteredColumn rc : rt.columns) {
                                    if (rc.columnName.equals(colName)) {
                                        if (rc.alias != null) displayName = rc.alias;
                                        if (rc.description != null) remarks = rc.description;
                                        break;
                                    }
                                }
                            }
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("TABLE_CAT", "provisa");
                            row.put("TABLE_SCHEM", "approved");
                            row.put("TABLE_NAME", viewName);
                            row.put("COLUMN_NAME", displayName);
                            row.put("DATA_TYPE", Types.VARCHAR);
                            row.put("TYPE_NAME", "VARCHAR");
                            row.put("ORDINAL_POSITION", ordinal++);
                            row.put("REMARKS", remarks);
                            rows.add(row);
                        }
                    }
                }
            } catch (Exception e) {
                // If compile fails, return empty columns
            }
        }
        return new ProvisaResultSet(columns, rows);
    }

    private ResultSet getCatalogColumns(String tableNamePattern) throws SQLException {
        List<ProvisaConnection.RegisteredTable> tables = conn.fetchRegisteredTables();

        List<String> columns = Arrays.asList(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
            "DATA_TYPE", "TYPE_NAME", "ORDINAL_POSITION", "REMARKS"
        );
        List<Map<String, Object>> rows = new ArrayList<>();

        for (ProvisaConnection.RegisteredTable t : tables) {
            String displayName = t.displayName();
            if (tableNamePattern != null && !tableNamePattern.equals("%") && !displayName.equals(tableNamePattern)) {
                continue;
            }
            int ordinal = 1;
            for (ProvisaConnection.RegisteredColumn c : t.columns) {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("TABLE_CAT", "provisa");
                row.put("TABLE_SCHEM", t.domainId);
                row.put("TABLE_NAME", displayName);
                row.put("COLUMN_NAME", c.displayName());
                row.put("DATA_TYPE", Types.VARCHAR);
                row.put("TYPE_NAME", "VARCHAR");
                row.put("ORDINAL_POSITION", ordinal++);
                row.put("REMARKS", c.description != null ? c.description : "");
                rows.add(row);
            }
        }
        return new ProvisaResultSet(columns, rows);
    }

    // ── PK/FK from semantic relationships (#4) ──

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        // Derive PKs: if a table is the target of a many-to-one, targetColumn is the PK
        List<ProvisaConnection.Relationship> rels = conn.fetchRelationships();
        List<String> columns = Arrays.asList(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"
        );
        List<Map<String, Object>> rows = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (ProvisaConnection.Relationship r : rels) {
            if ("many-to-one".equals(r.cardinality) && r.targetTableName.equals(table)) {
                String key = r.targetColumn;
                if (seen.add(key)) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("TABLE_CAT", "provisa");
                    row.put("TABLE_SCHEM", schema);
                    row.put("TABLE_NAME", table);
                    row.put("COLUMN_NAME", r.targetColumn);
                    row.put("KEY_SEQ", seen.size());
                    row.put("PK_NAME", "pk_" + table + "_" + r.targetColumn);
                    rows.add(row);
                }
            }
        }
        return new ProvisaResultSet(columns, rows);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        // FK side: this table references another
        return getForeignKeys(table, true);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        // PK side: another table references this one
        return getForeignKeys(table, false);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable)
            throws SQLException {
        List<ProvisaConnection.Relationship> rels = conn.fetchRelationships();
        List<String> columns = fkColumns();
        List<Map<String, Object>> rows = new ArrayList<>();
        int seq = 1;
        for (ProvisaConnection.Relationship r : rels) {
            if ("many-to-one".equals(r.cardinality)
                && r.targetTableName.equals(parentTable)
                && r.sourceTableName.equals(foreignTable)) {
                rows.add(buildFkRow(r, seq++));
            }
        }
        return new ProvisaResultSet(columns, rows);
    }

    private ResultSet getForeignKeys(String table, boolean imported) throws SQLException {
        List<ProvisaConnection.Relationship> rels = conn.fetchRelationships();
        List<String> columns = fkColumns();
        List<Map<String, Object>> rows = new ArrayList<>();
        int seq = 1;
        for (ProvisaConnection.Relationship r : rels) {
            if (!"many-to-one".equals(r.cardinality)) continue;
            boolean match = imported
                ? r.sourceTableName.equals(table)  // this table has the FK
                : r.targetTableName.equals(table);  // this table is the PK target
            if (match) {
                rows.add(buildFkRow(r, seq++));
            }
        }
        return new ProvisaResultSet(columns, rows);
    }

    private static List<String> fkColumns() {
        return Arrays.asList(
            "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "PKCOLUMN_NAME",
            "FKTABLE_CAT", "FKTABLE_SCHEM", "FKTABLE_NAME", "FKCOLUMN_NAME",
            "KEY_SEQ", "UPDATE_RULE", "DELETE_RULE", "FK_NAME", "PK_NAME"
        );
    }

    private Map<String, Object> buildFkRow(ProvisaConnection.Relationship r, int seq) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("PKTABLE_CAT", "provisa");
        row.put("PKTABLE_SCHEM", null);
        row.put("PKTABLE_NAME", r.targetTableName);
        row.put("PKCOLUMN_NAME", r.targetColumn);
        row.put("FKTABLE_CAT", "provisa");
        row.put("FKTABLE_SCHEM", null);
        row.put("FKTABLE_NAME", r.sourceTableName);
        row.put("FKCOLUMN_NAME", r.sourceColumn);
        row.put("KEY_SEQ", seq);
        row.put("UPDATE_RULE", DatabaseMetaData.importedKeyNoAction);
        row.put("DELETE_RULE", DatabaseMetaData.importedKeyNoAction);
        row.put("FK_NAME", "fk_" + r.sourceTableName + "_" + r.sourceColumn);
        row.put("PK_NAME", "pk_" + r.targetTableName + "_" + r.targetColumn);
        return row;
    }

    // ── Helpers ──

    private String extractName(String queryText) {
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
