package test;

import org.junit.Test;
import org.monetdb.monetdbe.MonetBlob;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class Test_22_GetObject {
    @Test
    public void getObject() {
        Stream.of(AllTests.CONNECTIONS).forEach(this::getObject);
    }

    public void getObject(String connectionUrl) {
        try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {
            Statement s = conn.createStatement();
            s.executeUpdate("CREATE TABLE test22 (b BOOLEAN, ti TINYINT, si SMALLINT, i INTEGER, l BIGINT, r REAL, f FLOAT,de DECIMAL(32,20), h HUGEINT, s STRING, bl BLOB, d DATE, t TIME, ts TIMESTAMP)");
            PreparedStatement ps = conn.prepareStatement("INSERT INTO test22 VALUES (?,?,?,?,?,?,?,1237831.123879879,9223372036854776800,?,?,?,?,?)");
            long instant = System.currentTimeMillis();
            Date d = new Date(instant);
            Time t = new Time(instant);
            Timestamp ts = new Timestamp(instant);
            BigInteger bigint = BigInteger.valueOf(9223372036854775800L);
            bigint = bigint.add(BigInteger.valueOf(1000));
            BigDecimal bigdec = BigDecimal.valueOf(1237831.123879879);
            long lng = 3000000L;
            float fl = 3287.3289f;
            double db = 328732.328129;

            ps.setObject(1,false);
            ps.setObject(2,1);
            ps.setObject(3,20);
            ps.setObject(4,50000);
            ps.setObject(5,lng);
            ps.setObject(6,fl);
            ps.setObject(7,db);
            //TODO Set BigDec and BigInt are not yet supported (Jan2022)
            //ps.setObject(8,bigdec);
            //ps.setObject(9,bigint);
            ps.setObject(8,"string");
            ps.setObject(9,new MonetBlob("12ff803F"));
            ps.setObject(10,d);
            ps.setObject(11,t);
            ps.setTimestamp(12,ts);

            assertEquals(1,ps.executeUpdate());

            ResultSet rs = s.executeQuery("SELECT * FROM test22");
            assertTrue(rs.next());

            assertEquals(false,rs.getObject(1));
            assertEquals((short)1,rs.getObject(2));
            assertEquals((short) 20,rs.getObject(3));
            assertEquals((int) 50000,rs.getObject(4));
            assertEquals(lng,rs.getObject(5));
            assertEquals(fl,rs.getObject(6));
            assertEquals(db,rs.getObject(7));
            //TODO Change
            assertEquals(bigdec,rs.getBigDecimal(8).stripTrailingZeros());
            assertEquals(bigint,rs.getObject(9));
            assertEquals("string",rs.getObject(10));
            assertEquals(MonetBlob.class,rs.getObject(11).getClass());
            assertEquals(d.toString(),rs.getObject(12).toString());
            assertEquals(t.toString(),rs.getObject(13).toString());
            assertEquals(ts,rs.getObject(14));
            assertFalse(rs.next());

            s.execute("DROP TABLE test22");
        } catch (SQLException e) {
            fail(e.getLocalizedMessage());
        }
    }
}
