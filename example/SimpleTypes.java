import java.sql.*;
import java.util.Properties;

public class InsertAndQuerySimpleTypes {
    private static void createAndInsert(Connection c) {
        try {
            Statement s = c.createStatement();

            System.out.println("Create table");
            s.execute("CREATE TABLE simple (b boolean, s smallint, i int, l bigint, r real, f float);");

            System.out.println("Insert into\n");
            s.execute("INSERT INTO simple VALUES " +
                    "(true, 2, 3, 5, 1.0, 1.66), " +
                    "(true, 4, 6, 10, 2.5, 3.643), " +
                    "(false, 8, 12, 20, 25.25, 372.325), " +
                    "(false, 16, 24, 40, 255.255, 2434.432)," +
                    "(false, null, 1, 1, 1, null);");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void dropTable(Connection c) {
        try {
            Statement s = c.createStatement();
            s.execute("DROP TABLE simple;");
            System.out.println("Drop count: " + s.getUpdateCount());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDB(Connection c) {
        try {
            Statement s = c.createStatement();
            s.executeQuery("SELECT * FROM simple;");
            ResultSet rs = s.getResultSet();
            System.out.println("Select resultSet: ");
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("Bool: " + rs.getBoolean(1));
                System.out.println("Short: " + rs.getShort(2));
                System.out.println("Int: " + rs.getInt(3));
                System.out.println("Long: " + rs.getLong(4));
                System.out.println("Float: " + rs.getFloat(5));
                System.out.println("Double: " + rs.getDouble(6));
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            Connection c = DriverManager.getConnection("jdbc:monetdb://:memory:", new Properties());

            System.out.println("Opened connection");

            createAndInsert(c);
            queryDB(c);
            dropTable(c);

            c.close();
            System.out.println("Closed connection");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}