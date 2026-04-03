package io.provisa.jdbc;

import com.google.gson.*;
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
    void rejectsInvalidSql() {
        conn.mode = "approved";
        var stmt = new ProvisaStatement(conn);
        assertThrows(SQLException.class, () -> stmt.executeQuery("DROP TABLE users"));
    }

    @Test
    void parsesStableIdAndRootFieldFromViewName() throws SQLException {
        conn.mode = "approved";
        conn.baseUrl = "http://localhost:8001";
        conn.role = "admin";

        // Return a matching approved query
        when(conn.fetchApprovedQueries()).thenReturn(List.of(
            new ProvisaConnection.ApprovedQuery("my-report", "{ users { id name } }", "")
        ));

        // Mock executeApprovedQuery to return multi-root response
        JsonObject response = new JsonObject();
        JsonObject data = new JsonObject();
        JsonArray users = new JsonArray();
        JsonObject user1 = new JsonObject();
        user1.addProperty("id", 1);
        user1.addProperty("name", "Alice");
        users.add(user1);
        data.add("sales__users", users);
        response.add("data", data);
        when(conn.executeApprovedQuery(eq("my-report"), any())).thenReturn(response);

        var stmt = new ProvisaStatement(conn);
        ResultSet rs = stmt.executeQuery("SELECT * FROM my-report__sales__users");

        assertTrue(rs.next());
        assertEquals("1", rs.getString("id"));
        assertEquals("Alice", rs.getString("name"));
        assertFalse(rs.next());
    }

    @Test
    void legacyFormatWithoutRootFieldSuffix() throws SQLException {
        conn.mode = "approved";
        conn.baseUrl = "http://localhost:8001";
        conn.role = "admin";

        when(conn.fetchApprovedQueries()).thenReturn(List.of(
            new ProvisaConnection.ApprovedQuery("get-users", "{ users { id } }", "")
        ));

        JsonObject response = new JsonObject();
        JsonObject data = new JsonObject();
        JsonArray users = new JsonArray();
        JsonObject user1 = new JsonObject();
        user1.addProperty("id", 42);
        users.add(user1);
        data.add("users", users);
        response.add("data", data);
        when(conn.executeApprovedQuery(eq("get-users"), any())).thenReturn(response);

        var stmt = new ProvisaStatement(conn);
        ResultSet rs = stmt.executeQuery("SELECT * FROM get-users");

        assertTrue(rs.next());
        assertEquals("42", rs.getString("id"));
        assertFalse(rs.next());
    }

    @Test
    void throwsOnUnknownStableId() throws SQLException {
        conn.mode = "approved";
        when(conn.fetchApprovedQueries()).thenReturn(List.of());

        var stmt = new ProvisaStatement(conn);
        assertThrows(SQLException.class,
            () -> stmt.executeQuery("SELECT * FROM nonexistent__field"));
    }

    @Test
    void parsesWhereClauseIntoVariables() throws SQLException {
        conn.mode = "approved";
        conn.baseUrl = "http://localhost:8001";
        conn.role = "admin";

        when(conn.fetchApprovedQueries()).thenReturn(List.of(
            new ProvisaConnection.ApprovedQuery("q1", "{ users { id } }", "")
        ));

        JsonObject response = new JsonObject();
        JsonObject data = new JsonObject();
        data.add("users", new JsonArray());
        response.add("data", data);
        when(conn.executeApprovedQuery(eq("q1"), any())).thenReturn(response);

        var stmt = new ProvisaStatement(conn);
        stmt.executeQuery("SELECT * FROM q1__users WHERE region = 'us-east' AND status = active");

        // Verify executeApprovedQuery was called (variables parsed from WHERE)
        verify(conn).executeApprovedQuery(eq("q1"), argThat(vars ->
            "us-east".equals(vars.get("region")) && "active".equals(vars.get("status"))
        ));
    }

    @Test
    void catalogModeRejectsExecution() throws SQLException {
        conn.mode = "catalog";
        when(conn.createStatement()).thenCallRealMethod();
        doCallRealMethod().when(conn).checkClosed();
        assertThrows(SQLException.class, () -> conn.createStatement());
    }
}
