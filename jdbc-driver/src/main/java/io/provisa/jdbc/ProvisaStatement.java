package io.provisa.jdbc;

import java.sql.*;
import java.util.*;

/**
 * Provisa JDBC Statement.
 *
 * A query runs through the governed pipeline (RLS, masking, visibility) over Arrow Flight when
 * the connection has a Flight transport (REQ-293), streamed as a {@link FlightStreamResultSet};
 * and through the /data/sql endpoint, as a {@link ProvisaResultSet}, only on a connection whose
 * Flight port was unreachable when it was opened. A Flight error is raised, never retried over
 * HTTP.
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

        if (currentResultSet != null) currentResultSet.close();
        if (conn.flightTransport != null) {
            currentResultSet = conn.flightTransport.execute(
                sql, conn.authToken, conn.role, conn.kmsKeyArn, conn.encryptionService);
            return currentResultSet;
        }

        // The Flight port was unreachable at connect: /data/sql
        List<Map<String, Object>> rows = conn.executeSqlEndpoint(sql);
        List<String> columns = rows.isEmpty()
            ? new ArrayList<>()
            : new ArrayList<>(rows.get(0).keySet());
        currentResultSet = new ProvisaResultSet(columns, rows);
        return currentResultSet;
    }

    @Override public ResultSet getResultSet() { return currentResultSet; }
    @Override
    public void close() throws SQLException {
        closed = true;
        if (currentResultSet != null) currentResultSet.close();
    }
    @Override public boolean isClosed() { return closed; }
    @Override public Connection getConnection() { return conn; }
}
