package io.provisa.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProvisaDatabaseMetaDataTest {

    @Mock
    ProvisaConnection conn;

    // ── mode=approved: getTables ──

    @Test
    void approvedMode_getTablesReturnsViewsWithRootFieldSuffix() throws SQLException {
        conn.mode = "approved";
        when(conn.fetchApprovedQueries()).thenReturn(List.of(
            new ProvisaConnection.ApprovedQuery("my-report", "{ users { id } orders { id } }", "")
        ));
        when(conn.resolveRootFields("{ users { id } orders { id } }"))
            .thenReturn(List.of("sales__users", "sales__orders"));

        var meta = new ProvisaDatabaseMetaData(conn);
        ResultSet rs = meta.getTables(null, null, "%", null);

        List<String> names = new ArrayList<>();
        while (rs.next()) names.add(rs.getString("TABLE_NAME"));

        assertEquals(2, names.size());
        assertTrue(names.contains("my-report__sales__users"));
        assertTrue(names.contains("my-report__sales__orders"));
    }

    @Test
    void approvedMode_singleRootStillGetsSuffix() throws SQLException {
        conn.mode = "approved";
        when(conn.fetchApprovedQueries()).thenReturn(List.of(
            new ProvisaConnection.ApprovedQuery("get-users", "{ users { id } }", "")
        ));
        when(conn.resolveRootFields("{ users { id } }"))
            .thenReturn(List.of("sales__users"));

        var meta = new ProvisaDatabaseMetaData(conn);
        ResultSet rs = meta.getTables(null, null, "%", null);

        assertTrue(rs.next());
        assertEquals("get-users__sales__users", rs.getString("TABLE_NAME"));
        assertEquals("VIEW", rs.getString("TABLE_TYPE"));
        assertFalse(rs.next());
    }

    // ── mode=catalog: getTables ──

    @Test
    void catalogMode_getTablesReturnsRegisteredTablesWithAliases() throws SQLException {
        conn.mode = "catalog";
        when(conn.fetchRegisteredTables()).thenReturn(List.of(
            new ProvisaConnection.RegisteredTable(1, "sales", "orders", null, "Customer orders", List.of()),
            new ProvisaConnection.RegisteredTable(2, "sales", "customers", "clients", "Customer accounts", List.of())
        ));

        var meta = new ProvisaDatabaseMetaData(conn);
        ResultSet rs = meta.getTables(null, null, "%", null);

        assertTrue(rs.next());
        assertEquals("orders", rs.getString("TABLE_NAME"));
        assertEquals("sales", rs.getString("TABLE_SCHEM"));
        assertEquals("Customer orders", rs.getString("REMARKS"));
        assertEquals("TABLE", rs.getString("TABLE_TYPE"));

        assertTrue(rs.next());
        assertEquals("clients", rs.getString("TABLE_NAME")); // alias used
        assertEquals("Customer accounts", rs.getString("REMARKS"));

        assertFalse(rs.next());
    }

    // ── mode=catalog: getColumns ──

    @Test
    void catalogMode_getColumnsReturnsAliasesAndDescriptions() throws SQLException {
        conn.mode = "catalog";
        when(conn.fetchRegisteredTables()).thenReturn(List.of(
            new ProvisaConnection.RegisteredTable(1, "sales", "orders", null, null, List.of(
                new ProvisaConnection.RegisteredColumn("id", null, "Primary key"),
                new ProvisaConnection.RegisteredColumn("customer_id", "cust_id", "FK to customers"),
                new ProvisaConnection.RegisteredColumn("total", "order_total", null)
            ))
        ));

        var meta = new ProvisaDatabaseMetaData(conn);
        ResultSet rs = meta.getColumns(null, null, "orders", null);

        assertTrue(rs.next());
        assertEquals("id", rs.getString("COLUMN_NAME"));
        assertEquals("Primary key", rs.getString("REMARKS"));

        assertTrue(rs.next());
        assertEquals("cust_id", rs.getString("COLUMN_NAME")); // alias used
        assertEquals("FK to customers", rs.getString("REMARKS"));

        assertTrue(rs.next());
        assertEquals("order_total", rs.getString("COLUMN_NAME")); // alias used
        assertEquals("", rs.getString("REMARKS")); // null description → empty

        assertFalse(rs.next());
    }

    // ── PK/FK from relationships (#4) ──

    @Test
    void getPrimaryKeys_derivesFromRelationships() throws SQLException {
        when(conn.fetchRelationships()).thenReturn(List.of(
            new ProvisaConnection.Relationship("r1", 1, 2, "orders", "customers",
                "customer_id", "id", "many-to-one")
        ));

        var meta = new ProvisaDatabaseMetaData(conn);
        ResultSet rs = meta.getPrimaryKeys(null, null, "customers");

        assertTrue(rs.next());
        assertEquals("customers", rs.getString("TABLE_NAME"));
        assertEquals("id", rs.getString("COLUMN_NAME"));
        assertFalse(rs.next());
    }

    @Test
    void getImportedKeys_returnsForeignKeysForTable() throws SQLException {
        when(conn.fetchRelationships()).thenReturn(List.of(
            new ProvisaConnection.Relationship("r1", 1, 2, "orders", "customers",
                "customer_id", "id", "many-to-one")
        ));

        var meta = new ProvisaDatabaseMetaData(conn);
        ResultSet rs = meta.getImportedKeys(null, null, "orders");

        assertTrue(rs.next());
        assertEquals("customers", rs.getString("PKTABLE_NAME"));
        assertEquals("id", rs.getString("PKCOLUMN_NAME"));
        assertEquals("orders", rs.getString("FKTABLE_NAME"));
        assertEquals("customer_id", rs.getString("FKCOLUMN_NAME"));
        assertFalse(rs.next());
    }

    @Test
    void getExportedKeys_returnsReferencingTables() throws SQLException {
        when(conn.fetchRelationships()).thenReturn(List.of(
            new ProvisaConnection.Relationship("r1", 1, 2, "orders", "customers",
                "customer_id", "id", "many-to-one")
        ));

        var meta = new ProvisaDatabaseMetaData(conn);
        ResultSet rs = meta.getExportedKeys(null, null, "customers");

        assertTrue(rs.next());
        assertEquals("customers", rs.getString("PKTABLE_NAME"));
        assertEquals("orders", rs.getString("FKTABLE_NAME"));
        assertFalse(rs.next());
    }

    @Test
    void getCrossReference_returnsRelationshipBetweenTables() throws SQLException {
        when(conn.fetchRelationships()).thenReturn(List.of(
            new ProvisaConnection.Relationship("r1", 1, 2, "orders", "customers",
                "customer_id", "id", "many-to-one")
        ));

        var meta = new ProvisaDatabaseMetaData(conn);
        ResultSet rs = meta.getCrossReference(null, null, "customers", null, null, "orders");

        assertTrue(rs.next());
        assertEquals("id", rs.getString("PKCOLUMN_NAME"));
        assertEquals("customer_id", rs.getString("FKCOLUMN_NAME"));
        assertFalse(rs.next());
    }

    @Test
    void noRelationships_returnsEmptyPkFk() throws SQLException {
        when(conn.fetchRelationships()).thenReturn(List.of());

        var meta = new ProvisaDatabaseMetaData(conn);
        assertFalse(meta.getPrimaryKeys(null, null, "orders").next());
        assertFalse(meta.getImportedKeys(null, null, "orders").next());
        assertFalse(meta.getExportedKeys(null, null, "orders").next());
    }
}
