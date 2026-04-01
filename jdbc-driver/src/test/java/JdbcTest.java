import java.sql.*;

/**
 * End-to-end JDBC driver test:
 * 1. Connect to Provisa
 * 2. List approved queries as tables
 * 3. Execute an approved query and print results
 */
public class JdbcTest {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:provisa://localhost:8001";
        var props = new java.util.Properties();
        props.setProperty("user", "admin");
        props.setProperty("password", "");

        System.out.println("=== Connecting to Provisa via JDBC ===");
        Connection conn = DriverManager.getConnection(url, props);
        System.out.println("Connected. Role: " + conn.getMetaData().getUserName());

        System.out.println("\n=== Listing approved queries (getTables) ===");
        ResultSet tables = conn.getMetaData().getTables(null, null, "%", null);
        String stableId = null;
        while (tables.next()) {
            String name = tables.getString("TABLE_NAME");
            String remarks = tables.getString("REMARKS");
            System.out.println("  TABLE: " + name + "  (" + remarks + ")");
            stableId = name; // Use last approved query
        }
        tables.close();

        if (stableId == null) {
            System.out.println("No approved queries found!");
            conn.close();
            return;
        }

        System.out.println("\n=== Querying approved query: " + stableId + " ===");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + stableId);

        // Print column headers
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();
        StringBuilder header = new StringBuilder();
        for (int i = 1; i <= colCount; i++) {
            if (i > 1) header.append(" | ");
            header.append(String.format("%-15s", meta.getColumnName(i)));
        }
        System.out.println(header);
        System.out.println("-".repeat(header.length()));

        // Print rows
        int rowCount = 0;
        while (rs.next()) {
            StringBuilder row = new StringBuilder();
            for (int i = 1; i <= colCount; i++) {
                if (i > 1) row.append(" | ");
                String val = rs.getString(i);
                row.append(String.format("%-15s", val != null ? val : "NULL"));
            }
            System.out.println(row);
            rowCount++;
        }
        System.out.println("\n" + rowCount + " rows returned.");

        rs.close();
        stmt.close();
        conn.close();
        System.out.println("Connection closed.");
    }
}
