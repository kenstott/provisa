package io.provisa.jdbc;

import java.sql.*;
import java.util.*;

/**
 * Database metadata — exposes registered tables for schema discovery.
 *
 * Registered tables carry aliases, descriptions, and domain schemas.
 * PK/FK relationships are materialized from semantic relationships.
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
        return getCatalogTables(tableNamePattern);
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
        return getCatalogColumns(tableNamePattern);
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
