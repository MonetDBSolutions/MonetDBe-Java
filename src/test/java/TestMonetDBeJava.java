import nl.cwi.monetdb.monetdbe.*;

import java.sql.*;
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
            System.out.println("Drop count: " + s.getUpdateCount());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertDBPreparedStatementDate (MonetConnection c) {
        try {
            System.out.println("Preparing statement (Insert query)");
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareCall("INSERT INTO a VALUES (false, 32, 243, 49, 2235.255, 243434.432,'hey5',?,?,?)");
            Date da = Date.valueOf("2015-10-31");
            ps.setDate(1,da);
            Time t = Time.valueOf("14:11:29");
            ps.setTime(2,t);
            Timestamp ts = Timestamp.valueOf("2007-12-24 14:11:40");
            ps.setTimestamp(3,ts);
            ps.executeUpdate();
            int affected_rows = ps.getUpdateCount();
            System.out.println("Prepared statement update count: " + affected_rows + "\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBPreparedStatementDate (MonetConnection c) {
        try {
            System.out.println("Preparing statement (Date query)");
            //MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareCall("SELECT da, t, ts FROM a WHERE da = ? AND t = ? AND ts = ?");
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareCall("SELECT da, t, ts FROM a WHERE da <> ? AND t <> ? AND ts <> ?");
            Date da = Date.valueOf("2015-10-31");
            ps.setDate(1,da);
            Time t = Time.valueOf("14:11:29");
            ps.setTime(2,t);
            Timestamp ts = Timestamp.valueOf("2007-12-24 14:11:40");
            ps.setTimestamp(3,ts);
            ps.executeQuery();
            MonetResultSet rs = (MonetResultSet) ps.getResultSet();

            rs.beforeFirst();
            System.out.println("\nPrepared statement resultSet:");
            while (rs.next()) {
                System.out.println("Date: " + rs.getDate(0));
                System.out.println("Time: " + rs.getTime(1));
                System.out.println("Timestamp: " + rs.getTimestamp(2));
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBPreparedStatement (MonetConnection c) {
        try {
            System.out.println("Preparing statement (Normal query)");
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareCall("SELECT st, i, r FROM a WHERE i < ? AND r < ? AND st <> ?");
            ps.setInt(1,20);
            ps.setFloat(2,30.2f);
            ps.setString(3,"hey2");
            ps.executeQuery();
            MonetResultSet rs = (MonetResultSet) ps.getResultSet();

            rs.beforeFirst();
            System.out.println("\nPrepared statement resultSet:");
            while (rs.next()) {
                System.out.println("String: " + rs.getString(0));
                System.out.println("Int: " + rs.getInt(1));
                System.out.println("Float: " + rs.getFloat(2));
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBStatement (MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            s.executeQuery("SELECT * FROM a WHERE i < 6000;");
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

    private static void queryDBLongQuery (MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            s.executeQuery("SELECT sql_mul(i,s), radians(degrees(radians(i))), tan(i) FROM a;");
            MonetResultSet rs = (MonetResultSet) s.getResultSet();
            System.out.println("Select resultSet: ");
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("Int: " + rs.getLong(0));
                System.out.println("Double: " + rs.getDouble(1));
                System.out.println("Double: " + rs.getDouble(2));
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

            System.out.println("Insert into\n");
            s.execute("INSERT INTO a VALUES " +
                    "(true, 2, 3, 5, 1.0, 1.66,'hey1',str_to_date('23-09-1987', '%d-%m-%Y'),str_to_time('11:40:30', '%H:%M:%S'),str_to_timestamp('23-09-1987 11:40', '%d-%m-%Y %H:%M')), " +
                    "(true, 4, 6, 10, 2.5, 3.643,'hey2',str_to_date('23-09-1990', '%d-%m-%Y'),str_to_time('11:40:35', '%H:%M:%S'),str_to_timestamp('23-09-1990 11:40', '%d-%m-%Y %H:%M')), " +
                    "(false, 8, 12, 20, 25.25, 372.325,'hey3',str_to_date('24-09-2020', '%d-%m-%Y'),str_to_time('12:01:59', '%H:%M:%S'),str_to_timestamp('24-09-2007 12:01', '%d-%m-%Y %H:%M')), " +
                    "(false, 16, 24, 40, 255.255, 2434.432,'hey4',str_to_date('31-10-2015', '%d-%m-%Y'),str_to_time('14:11:29', '%H:%M:%S'),str_to_timestamp('24-12-2007 14:11:40', '%d-%m-%Y %H:%M:%S'));");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            Properties info = new Properties();
            String url = "jdbc:monetdb://:memory:";
            info.setProperty("querytimeout","1");

            Connection conn = DriverManager.getConnection(url, info);
            MonetConnection c = (MonetConnection) conn;

            if (c != null) {
                System.out.println("Opened connection @ " + url.substring(15));
                System.out.println("Query timeout is " + c.getClientInfo("querytimeout"));
                populateDB(c);
                //insertDBPreparedStatementDate(c);
                queryDBStatement(c);
                queryDBPreparedStatement(c);
                //queryDBLongQuery(c);
                //queryDBPreparedStatementDate(c);
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
