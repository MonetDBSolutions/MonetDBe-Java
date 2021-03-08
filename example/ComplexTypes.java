import org.monetdb.monetdbe.MonetBlob;

import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

public class ComplexTypes {
    private static void createAndInsert(Connection c) {
        try {
            Statement s = c.createStatement();

            System.out.println("Create table");
            s.execute("CREATE TABLE complex (bd numeric, s string, b blob, d date, t time, ts timestamp);");

            System.out.println("Insert into\n");
            s.execute("INSERT INTO complex VALUES " +
                    "(34589.54,'hello','12ff803F',current_date,current_time,current_timestamp), " +
                    "(34012933.888,'world','0000803F',str_to_date('23-09-1987', '%d-%m-%Y'),str_to_time('11:40:30', '%H:%M:%S'),str_to_timestamp('23-09-1987 11:40', '%d-%m-%Y %H:%M')), " +
                    "(666.666,'bye','ffffffff',str_to_date('23-09-1990', '%d-%m-%Y'),str_to_time('11:40:35', '%H:%M:%S'),str_to_timestamp('23-09-1990 11:40', '%d-%m-%Y %H:%M'))," +
                    "(NULL,NULL,NULL,NULL,NULL,NULL);");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertNulls (Connection c) {
        try {
            System.out.println("Inserting NULL values");

            PreparedStatement p = c.prepareStatement("INSERT INTO complex VALUES (?,?,?,?,?,?);");
            p.setNull(1,Types.NUMERIC);
            p.setNull(2,Types.VARCHAR);
            p.setNull(3,Types.BLOB);
            p.setNull(4,Types.DATE);
            p.setNull(5,Types.TIME);
            p.setNull(6,Types.TIMESTAMP);

            p.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDB(Connection c) {
        try {
            System.out.println("Querying complex types");
            Statement s = c.createStatement();
            s.executeQuery("SELECT * FROM complex;");
            ResultSet rs = s.getResultSet();
            System.out.println("Select resultSet: ");
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("BigDecimal " + rs.getBigDecimal(1));
                System.out.println("String: " + rs.getString(2));
                Blob b = rs.getBlob(3);
                if (b.length() > 0)
                    System.out.println("Blob: " + Arrays.toString(b.getBytes(1,(int)b.length())));
                else
                    System.out.println("Blob: null");
                System.out.println("Date: " + rs.getDate(4));
                System.out.println("Time: " + rs.getTime(5));
                System.out.println("Timestamp: " + rs.getTimestamp(6));
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
            //insertNulls(c);
            queryDB(c);

            c.close();
            System.out.println("Closed connection");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
