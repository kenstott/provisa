package io.provisa.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Provisa JDBC Driver.
 *
 * Connection URL format: jdbc:provisa://host:port
 * Properties: user, password
 */
public class ProvisaDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new ProvisaDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register Provisa JDBC driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;

        String hostPort = url.substring("jdbc:provisa://".length());
        String[] parts = hostPort.split("/", 2);
        String baseUrl = "http://" + parts[0];

        String user = info.getProperty("user", "");
        String password = info.getProperty("password", "");

        return new ProvisaConnection(baseUrl, user, password);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:provisa://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[]{
            new DriverPropertyInfo("user", info.getProperty("user")),
            new DriverPropertyInfo("password", info.getProperty("password")),
        };
    }

    @Override public int getMajorVersion() { return 0; }
    @Override public int getMinorVersion() { return 1; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public Logger getParentLogger() { return Logger.getLogger("io.provisa.jdbc"); }
}
