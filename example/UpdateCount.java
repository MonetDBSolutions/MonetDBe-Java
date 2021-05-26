import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

public class UpdateCount {
    private static void updateCount(String connectionUrl, int loop) {
        try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("CREATE TABLE updateCount (b BOOLEAN, i INTEGER, s STRING);");

                int i = 0;
                while (i < loop) {
                    try (PreparedStatement p = conn.prepareStatement("INSERT INTO updateCount VALUES (?, ?, ?);")) {
                        p.setBoolean(1, true);
                        p.setInt(2, 10);
                        p.setString(3, "Hello world");
                        p.executeUpdate();
                    }
                    /*try (Statement s = conn.createStatement()) {
                        s.executeUpdate("INSERT INTO updateCount VALUES (false, 15, 'hello');");
                    }*/
                    i++;
                }
                System.out.println("Inserted " + i + " non-null values");

                i=0;

                while (i < loop) {
                    try (PreparedStatement p = conn.prepareStatement("INSERT INTO updateCount VALUES (?, ?, ?);")) {
                        p.setNull(1, Types.BOOLEAN);
                        p.setNull(2, Types.INTEGER);
                        p.setNull(3, Types.VARCHAR);
                        p.executeUpdate();
                    }
                    i++;
                }
                System.out.println("Inserted " + i + " null values");

                // Clean up
                int result = statement.executeUpdate("DROP TABLE updateCount;");
                System.out.println("Affected rows (should be " + loop*2 + "): " + result);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        updateCount("jdbc:monetdb:memory:", 5);
        updateCount("jdbc:monetdb:file:" + System.getProperty("user.dir") + "/testdata/localdb", 5);
    }
}