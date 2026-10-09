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
 * <p>The metadata is the signed-in role's own catalog (REQ-128): a second user whose role is not
 * served a table does not see it.
 *
 * <p>System properties: {@code provisa.url}; {@code provisa.user} / {@code provisa.password} — a
 * plain user; {@code provisa.hiddenTable} — a table that user's role is not served;
 * {@code provisa.adminUser} /
 * {@code provisa.adminPassword} / {@code provisa.adminRole} — a user who may read the registered
 * catalog, and the role it acts as; {@code provisa.table} (a registered table) and
 * {@code provisa.columns} (its columns, comma-separated) and {@code provisa.primaryKey} (the
 * column its source declares as primary key); {@code provisa.sql}. Run via
 * {@code mvn verify}; tests/integration/test_jdbc_driver_e2e.py starts the server and passes
 * these.
 */
class ProvisaDriverIT {

    static final String BASE_URL = System.getProperty("provisa.url", "jdbc:provisa://localhost:8001");
    static final String TABLE = System.getProperty("provisa.table", "orders");
    static final List<String> COLUMNS = List.of(System.getProperty("provisa.columns", "id,region").split(","));
    static final String SQL = System.getProperty("provisa.sql", "SELECT id, region FROM sales.orders");

    /** The column the registered table's source declares as its primary key. */
    static final String PRIMARY_KEY = System.getProperty("provisa.primaryKey", "id");

    /** A table the plain user's role is NOT served (the admin user's role is). */
    static final String HIDDEN_TABLE = System.getProperty("provisa.hiddenTable");

    private static Connection connect() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", System.getProperty("provisa.adminUser", "admin"));
        props.setProperty("password", System.getProperty("provisa.adminPassword", ""));
        String role = System.getProperty("provisa.adminRole");
        if (role != null) props.setProperty("role", role);
        return DriverManager.getConnection(BASE_URL, props);
    }

    /** The plain user ({@code provisa.user}), whose role is served {@code provisa.table} only. */
    private static Connection connectAsPlainUser() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", System.getProperty("provisa.user", "admin"));
        props.setProperty("password", System.getProperty("provisa.password", ""));
        return DriverManager.getConnection(BASE_URL, props);
    }

    private static List<String> tableNames(Connection conn) throws SQLException {
        List<String> names = new ArrayList<>();
        try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", null)) {
            while (rs.next()) names.add(rs.getString("TABLE_NAME"));
        }
        return names;
    }

    @Test
    void aTableTheRoleIsNotServedIsInNeitherGetTablesNorGetColumns() throws SQLException {
        org.junit.jupiter.api.Assumptions.assumeTrue(
            HIDDEN_TABLE != null, "provisa.hiddenTable names a table the plain user's role is not served");
        try (Connection admin = connect()) {
            assertTrue(tableNames(admin).contains(HIDDEN_TABLE), "the role that is served it lists it");
        }
        try (Connection plain = connectAsPlainUser()) {
            List<String> names = tableNames(plain);
            assertTrue(names.contains(TABLE), "its own table: " + names);
            assertFalse(names.contains(HIDDEN_TABLE), "listed to a role that is not served it: " + names);
            try (ResultSet cols = plain.getMetaData().getColumns(null, null, HIDDEN_TABLE, null)) {
                assertFalse(cols.next());
            }
        }
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
            // The key the SOURCE declares: registration records a source table's primary key, so
            // the catalog lists it and the driver reports it. (provisa.primaryKey names it.)
            try (ResultSet pks = conn.getMetaData().getPrimaryKeys(null, null, TABLE)) {
                assertTrue(pks.next(), "the source table declares a primary key");
                assertEquals(PRIMARY_KEY, pks.getString("COLUMN_NAME"));
                assertEquals(TABLE, pks.getString("TABLE_NAME"));
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
