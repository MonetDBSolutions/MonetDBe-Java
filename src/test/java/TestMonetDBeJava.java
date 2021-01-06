import nl.cwi.monetdb.monetdbe.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class TestMonetDBeJava {
    static {
        try {
            Class.forName("nl.cwi.monetdb.monetdbe.MonetDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void dropDB (MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            s.execute("DROP TABLE a;");
            System.out.println("Drop update count: " + s.getUpdateCount());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBPreparedStatement (MonetConnection c) {
        try {
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareCall("SELECT st FROM a WHERE i < ? AND r < ?");
            ps.setInt(1,8);
            ps.setFloat(2,2.2f);
            ps.execute();
            MonetResultSet rs = (MonetResultSet) ps.getResultSet();

            System.out.println("Select resultSet: ");
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("String: " + rs.getString(6));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBStatement (MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            s.execute("SELECT * FROM a;");
            MonetResultSet rs = (MonetResultSet) s.getResultSet();
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

    private static void populateDB(MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();

            System.out.println("Create table");
            s.execute("CREATE TABLE a (b boolean, s smallint, i int, l bigint, r real, f float, st string, da date, t time, ts timestamp);");

            System.out.println("Insert into");
            s.execute("INSERT INTO a VALUES " +
                    "(true, 2, 3, 5, 1.0, 1.66,'hey1',str_to_date('23-09-1987', '%d-%m-%Y'),str_to_time('11:40:30', '%H:%M:%S'),str_to_timestamp('23-09-1987 11:40', '%d-%m-%Y %H:%M')), " +
                    "(true, 4, 6, 10, 2.5, 3.643,'hey2',str_to_date('23-09-1990', '%d-%m-%Y'),str_to_time('11:40:35', '%H:%M:%S'),str_to_timestamp('23-09-1990 11:40', '%d-%m-%Y %H:%M')), " +
                    "(false, 8, 12, 20, 25.25, 372.325,'hey3',str_to_date('24-09-2007', '%d-%m-%Y'),str_to_time('12:01:59', '%H:%M:%S'),str_to_timestamp('24-09-2007 12:01', '%d-%m-%Y %H:%M')), " +
                    "(false, 16, 24, 40, 255.255, 2434.432,'hey4',str_to_date('24-12-2007', '%d-%m-%Y'),str_to_time('14:11:29', '%H:%M:%S'),str_to_timestamp('24-12-2007 14:11', '%d-%m-%Y %H:%M'));");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            Properties info = null;
            String url = "jdbc:monetdb://:memory:";

            Connection conn = DriverManager.getConnection(url, info);
            MonetConnection c = (MonetConnection) conn;

            if (c != null) {
                System.out.println("Opened connection @ " + url.substring(15));
                populateDB(c);
                queryDBStatement(c);
                queryDBPreparedStatement(c);
                dropDB(c);
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
