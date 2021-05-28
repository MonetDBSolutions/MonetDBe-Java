import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;
import org.monetdb.monetdbe.MonetBlob;

public class PreparedQueries {
    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    private static void create (Statement s) throws SQLException {
        s.executeUpdate("CREATE TABLE p (i INTEGER, l BIGINT, f REAL, df FLOAT, s STRING, b BLOB, d DATE, t TIME, ts TIMESTAMP, bd NUMERIC)");
    }

    private static void preparedInsert (Connection c) throws SQLException {
        System.out.println("PreparedStatement: Inserting values");

        PreparedStatement ps = c.prepareStatement("INSERT INTO p VALUES (?,?,?,?,?,?,?,?,?,?);");

        ps.setInt(1,1000);
        ps.setLong(2,1000000);
        ps.setFloat(3,3.5f);
        ps.setDouble(4,3.5);
        ps.setString(5,"bye world");
        ps.setBlob(6,new ByteArrayInputStream("Hello world".getBytes()));
        Date da = Date.valueOf("2020-10-31");
        ps.setDate(7,da);
        Time t = Time.valueOf("15:16:59");
        ps.setTime(8,t);
        Timestamp ts = Timestamp.valueOf("2007-12-24 14:11:40");
        ps.setTimestamp(9,ts);

        //ps.setBigDecimal(10,BigDecimal.TEN);
        ps.setInt(10,10);

        /*System.out.println("Clear parameters");
        ps.clearParameters();
        System.out.println("Set Int");
        ps.setInt(1,2000);*/
        System.out.println("Executing prepared insert");
        ps.execute();
    }

    private static void preparedNullInsert (Connection c) throws SQLException {
        System.out.println("PreparedStatement: Inserting NULL values");

        PreparedStatement p = c.prepareStatement("INSERT INTO p VALUES (?,?,?,?,?,?,?,?,?,?);");
        p.setNull(1,Types.INTEGER);
        p.setNull(2,Types.BIGINT);
        p.setNull(3,Types.REAL);
        p.setNull(4,Types.DOUBLE);
        p.setNull(5,Types.VARCHAR);
        p.setNull(6,Types.BLOB);
        p.setNull(7,Types.DATE);
        p.setNull(8,Types.TIME);
        p.setNull(9,Types.TIMESTAMP);
        p.setNull(10,Types.NUMERIC);
        p.execute();
    }

    private static void query (Statement s) throws SQLException {
        System.out.println("Querying inserted data");
        ResultSet rs = s.executeQuery("SELECT * FROM p;");

        while(rs.next()) {
            System.out.println("Row " + rs.getRow());
            System.out.println("Int: " + rs.getInt(1));
            System.out.println("Long: " + rs.getLong(2));
            System.out.println("Float: " + rs.getFloat(3));
            System.out.println("Double: " + rs.getDouble(4));
            System.out.println("String: " + rs.getString(5));

            MonetBlob b = (MonetBlob) rs.getBlob(6);
            System.out.print("Blob: ");
            System.out.println(b == null ? b : bytesToHex(b.getBytes(1,(int) b.length())));

            System.out.println("Date: " + rs.getDate(7));
            System.out.println("Time: " + rs.getTime(8));
            System.out.println("Timestamp: " + rs.getTimestamp(9));
            System.out.println("BigDecimal: " + rs.getBigDecimal(10));
            System.out.println();
        }
    }

    public static void main(String[] args) {
        try {
            Connection c = DriverManager.getConnection("jdbc:monetdb:memory:", new Properties());
            Statement s = c.createStatement();

            create(s);
            preparedInsert(c);
            preparedNullInsert(c);
            query(s);

            c.close();
            System.out.println("Closed connection");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
