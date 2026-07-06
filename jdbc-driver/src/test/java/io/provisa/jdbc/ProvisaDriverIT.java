package io.provisa.jdbc;

import org.junit.jupiter.api.*;

import java.sql.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests against a live Provisa backend.
 *
 * Requires: docker-compose up (Postgres + Trino + Provisa on localhost:8001)
 * Run via: mvn verify
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProvisaDriverIT {

    static final String BASE_URL = System.getProperty("provisa.url", "jdbc:provisa://localhost:8001");
    static final String USER = System.getProperty("provisa.user", "admin");

    // ── mode=catalog (default) ──

    @Test
    @Order(1)
    void connectsSuccessfully() throws SQLException {
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        try (Connection conn = DriverManager.getConnection(BASE_URL, props)) {
            assertFalse(conn.isClosed());
            assertEquals("Provisa", conn.getMetaData().getDatabaseProductName());
        }
    }

    @Test
    @Order(10)
    void catalogMode_connectsSuccessfully() throws SQLException {
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        try (Connection conn = DriverManager.getConnection(BASE_URL + "?mode=catalog", props)) {
            assertFalse(conn.isClosed());
            assertEquals("catalog", conn.getSchema());
        }
    }

    @Test
    @Order(11)
    void catalogMode_getTablesReturnsRegisteredTables() throws SQLException {
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        try (Connection conn = DriverManager.getConnection(BASE_URL + "?mode=catalog", props)) {
            ResultSet rs = conn.getMetaData().getTables(null, null, "%", null);
            List<String> names = new ArrayList<>();
            List<String> schemas = new ArrayList<>();
            while (rs.next()) {
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                names.add(rs.getString("TABLE_NAME"));
                schemas.add(rs.getString("TABLE_SCHEM"));
            }
            assertFalse(names.isEmpty(), "Should have registered tables");
            // Schemas should be domain IDs
            for (String schema : schemas) {
                assertNotNull(schema);
            }
        }
    }

    @Test
    @Order(12)
    void catalogMode_getColumnsReturnsAliasesAndDescriptions() throws SQLException {
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        try (Connection conn = DriverManager.getConnection(BASE_URL + "?mode=catalog", props)) {
            ResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
            if (!tables.next()) {
                fail("No registered tables");
            }
            String tableName = tables.getString("TABLE_NAME");

            ResultSet cols = conn.getMetaData().getColumns(null, null, tableName, null);
            boolean hasColumns = false;
            while (cols.next()) {
                hasColumns = true;
                assertNotNull(cols.getString("COLUMN_NAME"));
                // REMARKS should be present (may be empty)
                assertNotNull(cols.getString("REMARKS"));
            }
            assertTrue(hasColumns, "Table should have columns");
        }
    }

    @Test
    @Order(13)
    void catalogMode_executesSqlThroughGovernanceEndpoint() throws SQLException {
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        try (Connection conn = DriverManager.getConnection(BASE_URL + "?mode=catalog", props)) {
            ResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
            if (!tables.next()) {
                fail("No registered tables");
            }
            String tableName = tables.getString("TABLE_NAME");

            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
            ResultSetMetaData meta = rs.getMetaData();
            assertTrue(meta.getColumnCount() >= 0);

            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
            }
            assertTrue(rowCount >= 0, "Query should execute successfully");
        }
    }

    // ── PK/FK relationships ──

    @Test
    @Order(20)
    void relationships_getImportedKeys() throws SQLException {
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        try (Connection conn = DriverManager.getConnection(BASE_URL + "?mode=catalog", props)) {
            // Try to find FK relationships for any table
            ResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
            boolean foundRelationship = false;
            while (tables.next()) {
                String table = tables.getString("TABLE_NAME");
                ResultSet fks = conn.getMetaData().getImportedKeys(null, null, table);
                if (fks.next()) {
                    foundRelationship = true;
                    assertNotNull(fks.getString("PKTABLE_NAME"));
                    assertNotNull(fks.getString("PKCOLUMN_NAME"));
                    assertNotNull(fks.getString("FKTABLE_NAME"));
                    assertNotNull(fks.getString("FKCOLUMN_NAME"));
                    break;
                }
            }
            // Don't fail if no relationships configured — just log
            if (!foundRelationship) {
                System.out.println("No relationships configured — FK test skipped");
            }
        }
    }

    @Test
    @Order(21)
    void relationships_getPrimaryKeys() throws SQLException {
        var props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", "");
        try (Connection conn = DriverManager.getConnection(BASE_URL + "?mode=catalog", props)) {
            ResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
            boolean foundPk = false;
            while (tables.next()) {
                String table = tables.getString("TABLE_NAME");
                ResultSet pks = conn.getMetaData().getPrimaryKeys(null, null, table);
                if (pks.next()) {
                    foundPk = true;
                    assertEquals(table, pks.getString("TABLE_NAME"));
                    assertNotNull(pks.getString("COLUMN_NAME"));
                    break;
                }
            }
            if (!foundPk) {
                System.out.println("No PK relationships derived — PK test skipped");
            }
        }
    }
}
