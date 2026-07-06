package io.provisa.jdbc;

import java.sql.*;
import java.util.*;

/**
 * Provisa JDBC Statement.
 *
 * Routes SQL through the /data/sql governance endpoint (RLS, masking, and
 * visibility) and returns the rows as a {@link ProvisaResultSet}.
 */
public class ProvisaStatement extends AbstractStatement {

    private final ProvisaConnection conn;
    private ResultSet currentResultSet;
    private boolean closed = false;

    ProvisaStatement(ProvisaConnection conn) {
        this.conn = conn;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (closed) throw new SQLException("Statement is closed");

        // Route raw SQL through /data/sql governance endpoint
        List<Map<String, Object>> rows = conn.executeSqlEndpoint(sql);
        List<String> columns = rows.isEmpty()
            ? new ArrayList<>()
            : new ArrayList<>(rows.get(0).keySet());
        currentResultSet = new ProvisaResultSet(columns, rows);
        return currentResultSet;
    }

    @Override public ResultSet getResultSet() { return currentResultSet; }
    @Override public void close() { closed = true; }
    @Override public boolean isClosed() { return closed; }
    @Override public Connection getConnection() { return conn; }
}
