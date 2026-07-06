package io.provisa.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.doCallRealMethod;

@ExtendWith(MockitoExtension.class)
class ProvisaStatementTest {

    @Mock
    ProvisaConnection conn;

    @Test
    void allowsStatementCreation() throws SQLException {
        conn.mode = "catalog";
        when(conn.createStatement()).thenCallRealMethod();
        doCallRealMethod().when(conn).checkClosed();
        assertDoesNotThrow(() -> conn.createStatement());
    }

    @Test
    void routesToSqlEndpoint() throws SQLException {
        conn.mode = "catalog";
        conn.baseUrl = "http://localhost:8001";
        conn.role = "admin";

        List<Map<String, Object>> rows = new ArrayList<>();
        Map<String, Object> row1 = new LinkedHashMap<>();
        row1.put("id", 1);
        row1.put("name", "Alice");
        rows.add(row1);
        when(conn.executeSqlEndpoint(anyString())).thenReturn(rows);

        try (var stmt = new ProvisaStatement(conn);
             ResultSet rs = stmt.executeQuery("SELECT id, name FROM users WHERE region = 'us-east'")) {
            assertTrue(rs.next());
            assertEquals("1", rs.getString("id"));
            assertEquals("Alice", rs.getString("name"));
            assertFalse(rs.next());
        }

        verify(conn).executeSqlEndpoint("SELECT id, name FROM users WHERE region = 'us-east'");
    }

    @Test
    void emptyResultSet() throws SQLException {
        conn.mode = "catalog";
        conn.baseUrl = "http://localhost:8001";
        conn.role = "admin";

        when(conn.executeSqlEndpoint(anyString())).thenReturn(new ArrayList<>());

        try (var stmt = new ProvisaStatement(conn);
             ResultSet rs = stmt.executeQuery("SELECT * FROM orders")) {
            assertFalse(rs.next());
        }
        verify(conn).executeSqlEndpoint("SELECT * FROM orders");
    }
}
