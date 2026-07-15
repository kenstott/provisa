package io.provisa.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Provisa JDBC Driver.
 *
 * Connection URL format: jdbc:provisa://host:port[?mode=catalog]
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
        String mode = info.getProperty("mode", "catalog");
        // REQ-690/REQ-694: client-side decryption params (connection URL or Properties).
        String kmsProvider = info.getProperty("kms_provider");
        String kmsKeyArn = info.getProperty("kms_key_arn");
        String kmsMasterKey = info.getProperty("kms_master_key"); // base64, local provider only
        int qIdx = remainder.indexOf('?');
        if (qIdx >= 0) {
            hostPort = remainder.substring(0, qIdx);
            String query = remainder.substring(qIdx + 1);
            for (String param : query.split("&")) {
                String[] kv = param.split("=", 2);
                if (kv.length != 2) continue;
                switch (kv[0]) {
                    case "mode": mode = kv[1]; break;
                    case "kms_provider": kmsProvider = kv[1]; break;
                    case "kms_key_arn": kmsKeyArn = kv[1]; break;
                    case "kms_master_key": kmsMasterKey = kv[1]; break;
                    default: break;
                }
            }
        }

        // Strip trailing path segments
        int slashIdx = hostPort.indexOf('/');
        if (slashIdx >= 0) hostPort = hostPort.substring(0, slashIdx);

        String baseUrl = "http://" + hostPort;
        String user = info.getProperty("user", "");
        String password = info.getProperty("password", "");

        ProvisaConnection conn = new ProvisaConnection(baseUrl, user, password, mode);
        conn.configureEncryption(kmsProvider, kmsKeyArn, kmsMasterKey);
        return conn;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:provisa://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        DriverPropertyInfo modeProp = new DriverPropertyInfo("mode", info.getProperty("mode", "catalog"));
        modeProp.description = "Connection mode: 'catalog' (schema discovery and SQL execution)";
        modeProp.choices = new String[]{"catalog"};
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
