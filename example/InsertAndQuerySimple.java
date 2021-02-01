import java.sql.*;
import java.util.Properties;

public class InsertAndQuerySimple {
    private static void createAndInsert(Connection c) {
        try {
            Statement s = c.createStatement();

            System.out.println("Create table");
            s.execute("CREATE TABLE a (b boolean, s smallint, i int, l bigint, r real, f float, st string, da date, t time, ts timestamp);");

            System.out.println("Insert into\n");
            s.execute("INSERT INTO a VALUES " +
                    "(true, 2, 3, 5, 1.0, 1.66,'hey1',str_to_date('23-09-1987', '%d-%m-%Y'),str_to_time('11:40:30', '%H:%M:%S'),str_to_timestamp('23-09-1987 11:40', '%d-%m-%Y %H:%M')), " +
                    "(true, 4, 6, 10, 2.5, 3.643,'hey2',str_to_date('23-09-1990', '%d-%m-%Y'),str_to_time('11:40:35', '%H:%M:%S'),str_to_timestamp('23-09-1990 11:40', '%d-%m-%Y %H:%M')), " +
                    "(false, 8, 12, 20, 25.25, 372.325,'hey3',str_to_date('24-09-2020', '%d-%m-%Y'),str_to_time('12:01:59', '%H:%M:%S'),str_to_timestamp('24-09-2007 12:01', '%d-%m-%Y %H:%M')), " +
                    "(false, 16, 24, 40, 255.255, 2434.432,'hey4',current_date,current_time,current_timestamp)," +
                    "(false, null, 1, 1, 1, null,'hey5',current_date,current_time,current_timestamp);");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void dropTable(Connection c) {
        try {
            Statement s = c.createStatement();
            s.execute("DROP TABLE a;");
            System.out.println("Drop count: " + s.getUpdateCount());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBStatement (Connection c) {
        try {
            Statement s = c.createStatement();
            s.executeQuery("SELECT * FROM a;");
            ResultSet rs = s.getResultSet();
            System.out.println("Select resultSet: ");
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("Bool: " + rs.getBoolean(0));
                System.out.println("Short: " + rs.getShort(1));
                System.out.println("Int: " + rs.getInt(2));
                System.out.println("Long: " + rs.getLong(3));
                System.out.println("Float: " + rs.getFloat(4));
                System.out.println("Double: " + rs.getDouble(5));
                System.out.println("String: " + rs.getString(6));
                System.out.println("Date: " + rs.getDate(7));
                System.out.println("Time: " + rs.getTime(8));
                System.out.println("Timestamp: " + rs.getTimestamp(9));
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            Connection c = DriverManager.getConnection("jdbc:monetdb://:memory:", new Properties());

            if (c != null) {
                System.out.println("Opened connection");
                
                createAndInsert(c);
                queryDBStatement(c);
                dropTable(c);

                c.close();
                System.out.println("Closed connection");
            } else {
                System.out.println("No connection was made");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}