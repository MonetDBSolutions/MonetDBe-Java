import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;
import org.monetdb.monetdbe.MonetBlob;
import org.monetdb.monetdbe.MonetParameterMetaData;
import org.monetdb.monetdbe.MonetResultSet;
import org.monetdb.monetdbe.MonetResultSetMetaData;

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
        s.executeUpdate("CREATE TABLE p (bo BOOL, ti TINYINT, sh SMALLINT, i INTEGER, l BIGINT, f REAL, df FLOAT, s STRING, b BLOB, d DATE, t TIME, ts TIMESTAMP, bd NUMERIC)");
    }

    private static void preparedMetadata (PreparedStatement ps) {
        MonetResultSetMetaData rsMeta;
        MonetParameterMetaData pMeta;
        try {
            rsMeta = (MonetResultSetMetaData) ps.getMetaData();
            pMeta = (MonetParameterMetaData) ps.getParameterMetaData();
            if (pMeta != null) {
                System.out.println("Input Metadata:");
                for (int i = 1; i < pMeta.getParameterCount()+1; i++) {
                    System.out.println("Param " + i);
                    System.out.println("Monet type: " + pMeta.getParameterTypeName(i));
                    System.out.println("SQL type: " + pMeta.getParameterType(i));
                    System.out.println();
                }
                System.out.println();
            }
            else {
                System.out.println("No input metadata\n");
            }
            if (rsMeta != null) {
                System.out.println("Output Metadata:");
                for (int i = 1; i < rsMeta.getColumnCount()+1; i++) {
                    System.out.println("Output " + i);
                    System.out.println("Column Name: " + rsMeta.getColumnName(i));
                    System.out.println("Monet type: " + rsMeta.getColumnTypeName(i));
                    System.out.println("SQL type: " + rsMeta.getColumnType(i));
                    System.out.println();
                }
                System.out.println();
            }
            else {
                System.out.println("No output metadata\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void preparedInsert (PreparedStatement ps) throws SQLException {
        System.out.println("PreparedStatement: Inserting values");

        ps.setBoolean(1,false);
        ps.setByte(2,(byte)2);
        ps.setShort(3,(short)390);
        ps.setInt(4,1000);
        ps.setLong(5,1000000);
        ps.setFloat(6,3.5f);
        ps.setDouble(7,3.5);
        ps.setString(8,"bye world");
        ps.setBlob(9,new ByteArrayInputStream("Hello world".getBytes()));
        Date da = Date.valueOf("2020-10-31");
        ps.setDate(10,da);
        Time t = Time.valueOf("15:16:59");
        ps.setTime(11,t);
        Timestamp ts = Timestamp.valueOf("2007-12-24 14:11:40");
        ps.setTimestamp(12,ts);

        //ps.setBigDecimal(13,BigDecimal.TEN);
        ps.setInt(13,10);

        /*System.out.println("Clear parameters");
        ps.clearParameters();
        System.out.println("Set Int");
        ps.setInt(1,2000);*/

        preparedMetadata(ps);

        System.out.println("Executing prepared insert");
        ps.execute();
    }

    private static void preparedNullInsert (PreparedStatement ps) throws SQLException {
        System.out.println("PreparedStatement: Inserting NULL values");

        ps.setNull(1,Types.BOOLEAN);
        ps.setNull(2,Types.TINYINT);
        ps.setNull(3,Types.SMALLINT);
        ps.setNull(4,Types.INTEGER);
        ps.setNull(5,Types.BIGINT);
        ps.setNull(6,Types.REAL);
        ps.setNull(7,Types.DOUBLE);
        ps.setNull(8,Types.VARCHAR);
        ps.setNull(9,Types.BLOB);
        ps.setNull(10,Types.DATE);
        ps.setNull(11,Types.TIME);
        ps.setNull(12,Types.TIMESTAMP);
        ps.setNull(13,Types.NUMERIC);
        preparedMetadata(ps);
        ps.execute();
    }


    private static void query (Statement s) throws SQLException {
        System.out.println("Querying inserted data");
        ResultSet rs = s.executeQuery("SELECT * FROM p;");

        while(rs.next()) {
            System.out.println("Row " + rs.getRow());
            System.out.println("Bool: " + rs.getByte(1));
            System.out.println("Tiny Int: " + rs.getByte(2));
            System.out.println("Short: " + rs.getShort(3));
            System.out.println("Int: " + rs.getInt(4));
            System.out.println("Long: " + rs.getLong(5));
            System.out.println("Float: " + rs.getFloat(6));
            System.out.println("Double: " + rs.getDouble(7));
            System.out.println("String: " + rs.getString(8));

            MonetBlob b = (MonetBlob) rs.getBlob(9);
            System.out.print("Blob: ");
            System.out.println(b == null ? b : bytesToHex(b.getBytes(1,(int) b.length())));

            System.out.println("Date: " + rs.getDate(10));
            System.out.println("Time: " + rs.getTime(11));
            System.out.println("Timestamp: " + rs.getTimestamp(12));
            System.out.println("BigDecimal: " + rs.getBigDecimal(13));
            System.out.println();
        }
    }

    private static void queryPrepared (Connection c) throws SQLException {
        System.out.println("Prepared Query over inserted data");
        PreparedStatement ps = c.prepareStatement("SELECT bo, i, df, s FROM p WHERE sh = ?");
        preparedMetadata(ps);
        ps.setShort(1,(short)390);
        ps.execute();
        ResultSet rs = ps.getResultSet();
        while(rs.next()) {
            System.out.println("Row " + rs.getRow());
            System.out.println("Bool: " + rs.getByte(1));
            System.out.println("Int: " + rs.getInt(2));
            System.out.println("Float: " + rs.getFloat(3));
            System.out.println("String: " + rs.getString(4));
            System.out.println();
        }
    }

    public static void main(String[] args) {
        try {
            Connection c = DriverManager.getConnection("jdbc:monetdb:memory:", new Properties());
            Statement s = c.createStatement();

            create(s);
            PreparedStatement ps = c.prepareStatement("INSERT INTO p VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);");
            preparedInsert(ps);
            preparedNullInsert(ps);
            queryPrepared(c);
            query(s);

            c.close();
            System.out.println("Closed connection");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
