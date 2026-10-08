package io.provisa.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Catalog metadata and a query through the driver against a live Provisa server with auth
 * enforced (REQ-126, REQ-128, REQ-131).
 *
 * <p>System properties: {@code provisa.url}; {@code provisa.adminUser} /
 * {@code provisa.adminPassword} / {@code provisa.adminRole} — a user who may read the registered
 * catalog, and the role it acts as; {@code provisa.table} (a registered table) and
 * {@code provisa.columns} (its columns, comma-separated); {@code provisa.sql}. Run via
 * {@code mvn verify}; tests/integration/test_jdbc_driver_e2e.py starts the server and passes
 * these.
 */
class ProvisaDriverIT {

    static final String BASE_URL = System.getProperty("provisa.url", "jdbc:provisa://localhost:8001");
    static final String TABLE = System.getProperty("provisa.table", "orders");
    static final List<String> COLUMNS = List.of(System.getProperty("provisa.columns", "id,region").split(","));
    static final String SQL = System.getProperty("provisa.sql", "SELECT id, region FROM sales.orders");

    private static Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", System.getProperty("provisa.adminUser", "admin"));
        props.setProperty("password", System.getProperty("provisa.adminPassword", ""));
        String role = System.getProperty("provisa.adminRole");
        if (role != null) props.setProperty("role", role);
        return DriverManager.getConnection(BASE_URL, props);
    }

    @Test
    void theConnectionReportsTheProductAndTheSignedInUser() throws SQLException {
        try (Connection conn = connect()) {
            assertFalse(conn.isClosed());
            DatabaseMetaData meta = conn.getMetaData();
            assertEquals("Provisa", meta.getDatabaseProductName());
            assertEquals(System.getProperty("provisa.adminUser", "admin"), meta.getUserName());
            assertEquals("catalog", conn.getSchema());
        }
    }

    @Test
    void getTablesListsTheRegisteredTable() throws SQLException {
        try (Connection conn = connect();
             ResultSet rs = conn.getMetaData().getTables(null, null, "%", null)) {
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                assertEquals("TABLE", rs.getString("TABLE_TYPE"));
                assertNotNull(rs.getString("TABLE_SCHEM"));
                names.add(rs.getString("TABLE_NAME"));
            }
            assertTrue(names.contains(TABLE), "registered tables: " + names);
        }
    }

    @Test
    void getColumnsListsTheTablesColumns() throws SQLException {
        try (Connection conn = connect();
             ResultSet rs = conn.getMetaData().getColumns(null, null, TABLE, null)) {
            List<String> names = new ArrayList<>();
            while (rs.next()) {
                names.add(rs.getString("COLUMN_NAME"));
            }
            assertTrue(names.containsAll(COLUMNS), TABLE + " columns: " + names);
        }
    }

    @Test
    void keyMetadataAnswersForATableWithNoRelationships() throws SQLException {
        try (Connection conn = connect()) {
            try (ResultSet fks = conn.getMetaData().getImportedKeys(null, null, TABLE)) {
                assertFalse(fks.next(), "the test model declares no relationship");
            }
            try (ResultSet pks = conn.getMetaData().getPrimaryKeys(null, null, TABLE)) {
                assertFalse(pks.next());
            }
        }
    }

    @Test
    void aQueryReturnsTheTablesRowsWithTheirColumns() throws SQLException {
        try (Connection conn = connect();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(SQL)) {
            assertEquals(COLUMNS.size(), rs.getMetaData().getColumnCount());
            for (int i = 0; i < COLUMNS.size(); i++) {
                assertEquals(COLUMNS.get(i), rs.getMetaData().getColumnName(i + 1));
            }
            int rows = 0;
            while (rs.next()) {
                assertNotNull(rs.getObject(COLUMNS.get(0)));
                rows++;
            }
            assertTrue(rows > 0, "the table has rows");
        }
    }
}
