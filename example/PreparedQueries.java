import java.sql.*;
import java.util.Properties;

public class PreparedQueries {

    private static void preparedInsert (Connection c) throws SQLException {
        System.out.println("Preparing insert statement");
        PreparedStatement ps = c.prepareStatement("INSERT INTO p VALUES (?,?,?,?,?,?);");

        ps.setInt(1,1000);
        //ps.setLong(2,100);
        ps.setInt(2,100);
        ps.setString(3,"bye world");
        Date da = Date.valueOf("2020-10-31");
        ps.setDate(4,da);
        Time t = Time.valueOf("15:16:59");
        ps.setTime(5,t);
        Timestamp ts = Timestamp.valueOf("2007-12-24 14:11:40");
        ps.setTimestamp(6,ts);

        System.out.println("Executing prepared insert");
        ps.execute();
    }

    private static void preparedNullInsert (Connection c) throws SQLException {
        System.out.println("Inserting NULL values");

        PreparedStatement p = c.prepareStatement("INSERT INTO p VALUES (?,?,?,?,?,?);");
        p.setNull(1,Types.INTEGER);
        p.setNull(2,Types.BIGINT);
        p.setNull(3,Types.VARCHAR);
        p.setNull(4,Types.DATE);
        p.setNull(5,Types.TIME);
        p.setNull(6,Types.TIMESTAMP);
        p.execute();
    }

    public static void main(String[] args) {
        try {
            Connection c = DriverManager.getConnection("jdbc:monetdb://:memory:", new Properties());

            Statement s = c.createStatement();
            s.executeUpdate("CREATE TABLE p (i INTEGER, l BIGINT, s STRING, d DATE, t TIME, ts TIMESTAMP)");

            preparedInsert(c);
            preparedNullInsert(c);

            System.out.println("Querying inserted data");
            ResultSet rs = s.executeQuery("SELECT * FROM p;");

            while(rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("Int: " + rs.getInt(1));
                System.out.println("Long: " + rs.getLong(2));
                System.out.println("String: " + rs.getString(3));
                System.out.println("Date: " + rs.getDate(4));
                System.out.println("Time: " + rs.getTime(5));
                System.out.println("Timestamp: " + rs.getTimestamp(6));
                System.out.println();
            }
            c.close();
            System.out.println("Closed connection");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
