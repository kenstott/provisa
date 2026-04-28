package io.provisa.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Provisa JDBC Driver.
 *
 * Connection URL format: jdbc:provisa://host:port[?mode=catalog|approved]
 * Properties: user, password, mode
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

        String remainder = url.substring("jdbc:provisa://".length());

        // Split off query string: host:port?mode=catalog
        String hostPort = remainder;
        String mode = info.getProperty("mode", "approved");
        int qIdx = remainder.indexOf('?');
        if (qIdx >= 0) {
            hostPort = remainder.substring(0, qIdx);
            String query = remainder.substring(qIdx + 1);
            for (String param : query.split("&")) {
                String[] kv = param.split("=", 2);
                if (kv.length == 2 && "mode".equals(kv[0])) {
                    mode = kv[1];
                }
            }
        }

        // Strip trailing path segments
        int slashIdx = hostPort.indexOf('/');
        if (slashIdx >= 0) hostPort = hostPort.substring(0, slashIdx);

        String baseUrl = "http://" + hostPort;
        String user = info.getProperty("user", "");
        String password = info.getProperty("password", "");

        return new ProvisaConnection(baseUrl, user, password, mode);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:provisa://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo modeProp = new DriverPropertyInfo("mode", info.getProperty("mode", "approved"));
        modeProp.description = "Connection mode: 'approved' (query approved queries) or 'catalog' (schema discovery)";
        modeProp.choices = new String[]{"approved", "catalog"};
        return new DriverPropertyInfo[]{
            new DriverPropertyInfo("user", info.getProperty("user")),
            new DriverPropertyInfo("password", info.getProperty("password")),
            modeProp,
        };
    }

    @Override public int getMajorVersion() { return 0; }
    @Override public int getMinorVersion() { return 1; }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public Logger getParentLogger() { return Logger.getLogger("io.provisa.jdbc"); }
}
