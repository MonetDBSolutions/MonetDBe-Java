import java.sql.*;
import java.util.Properties;

public class PreparedQueries {

    private static void preparedInsert (Connection c) throws SQLException {
        System.out.println("Preparing insert statement");

        //PreparedStatement ps = c.prepareStatement("INSERT INTO p VALUES (?,?,?,?,?,?,?,?);");
        PreparedStatement ps = c.prepareStatement("INSERT INTO p VALUES (?,?,?,?,?,'2020-10-31','15:16:59','2007-12-24 14:11:40');");

        ps.setInt(1,1000);
        ps.setLong(2,1000000);
        ps.setFloat(3,3.5f);
        ps.setDouble(4,3.5);
        ps.setString(5,"bye world");
        /*Date da = Date.valueOf("2020-10-31");
        ps.setDate(6,da);
        Time t = Time.valueOf("15:16:59");
        ps.setTime(7,t);
        Timestamp ts = Timestamp.valueOf("2007-12-24 14:11:40");
        ps.setTimestamp(8,ts);*/

        System.out.println("Executing prepared insert");
        ps.execute();
    }

    private static void preparedNullInsert (Connection c) throws SQLException {
        System.out.println("Inserting NULL values");

        //PreparedStatement p = c.prepareStatement("INSERT INTO p VALUES (?,?,?,?,?,?,?,?);");
        PreparedStatement p = c.prepareStatement("INSERT INTO p VALUES (?,?,?,?,?,NULL,NULL,NULL);");
        p.setNull(1,Types.INTEGER);
        p.setNull(2,Types.BIGINT);
        p.setNull(3,Types.REAL);
        p.setNull(4,Types.DOUBLE);
        p.setNull(5,Types.VARCHAR);
        /*p.setNull(6,Types.DATE);
        p.setNull(7,Types.TIME);
        p.setNull(8,Types.TIMESTAMP);*/
        p.execute();
    }

    public static void main(String[] args) {
        try {
            Connection c = DriverManager.getConnection("jdbc:monetdb:memory:", new Properties());

            Statement s = c.createStatement();
            s.executeUpdate("CREATE TABLE p (i INTEGER, l BIGINT, f REAL, df FLOAT, s STRING, d DATE, t TIME, ts TIMESTAMP)");

            preparedInsert(c);
            preparedNullInsert(c);

            System.out.println("Querying inserted data");
            ResultSet rs = s.executeQuery("SELECT * FROM p;");

            while(rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("Int: " + rs.getInt(1));
                System.out.println("Long: " + rs.getLong(2));
                System.out.println("Float: " + rs.getFloat(3));
                System.out.println("Double: " + rs.getDouble(4));
                System.out.println("String: " + rs.getString(5));
                System.out.println("Date: " + rs.getDate(6));
                System.out.println("Time: " + rs.getTime(7));
                System.out.println("Timestamp: " + rs.getTimestamp(8));
                System.out.println();
            }
            c.close();
            System.out.println("Closed connection");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
