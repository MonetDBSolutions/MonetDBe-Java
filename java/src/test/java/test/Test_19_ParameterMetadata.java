package test;

import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class Test_19_ParameterMetadata {
    @Test
    public void parameterMetadata() {
        Stream.of(AllTests.CONNECTIONS).forEach(this::parameterMetadata);
    }

    private void parameterMetadata(String connectionUrl) {
        try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

            assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
            assertFalse(conn.isClosed());
            Statement s = conn.createStatement();
            s.execute("CREATE TABLE test19 (i int, l bigint, f real, d double, bd NUMERIC(36,18), s STRING, b BLOB, da DATE);");

            // Create table and insert values
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test19 VALUES (?,?,?,?,?,?,?,?)")) {
                ParameterMetaData meta = ps.getParameterMetaData();
                assertEquals(8,meta.getParameterCount());
                
                //Check Precision and Scale
                assertEquals(32,meta.getPrecision(1));
                assertEquals(0,meta.getScale(1));
                assertEquals(64,meta.getPrecision(2));
                assertEquals(0,meta.getScale(2));

                //TODO Why 24 and 53 instead of 32 and 64?
                assertEquals(24,meta.getPrecision(3));
                assertEquals(0,meta.getScale(3));
                assertEquals(53,meta.getPrecision(4));
                assertEquals(0,meta.getScale(4));

                assertEquals(36,meta.getPrecision(5));
                assertEquals(18,meta.getScale(5));
                assertEquals(0,meta.getPrecision(6));
                assertEquals(0,meta.getScale(6));
                assertEquals(0,meta.getPrecision(7));
                assertEquals(0,meta.getScale(7));
                assertEquals(0,meta.getPrecision(8));
                assertEquals(0,meta.getScale(8));

                //Check types (sql, monetdbe, java)
                assertEquals(Types.INTEGER,meta.getParameterType(1));
                assertEquals("monetdbe_int32_t",meta.getParameterTypeName(1));
                assertEquals(Integer.class.getName(),meta.getParameterClassName(1));
                assertEquals(Types.BIGINT,meta.getParameterType(2));
                assertEquals("monetdbe_int64_t",meta.getParameterTypeName(2));
                assertEquals(Long.class.getName(),meta.getParameterClassName(2));
                assertEquals(Types.REAL,meta.getParameterType(3));
                assertEquals("monetdbe_float",meta.getParameterTypeName(3));
                assertEquals(Float.class.getName(),meta.getParameterClassName(3));
                assertEquals(Types.DOUBLE,meta.getParameterType(4));
                assertEquals("monetdbe_double",meta.getParameterTypeName(4));
                assertEquals(Double.class.getName(),meta.getParameterClassName(4));
                assertEquals(Types.NUMERIC,meta.getParameterType(5));
                assertEquals("monetdbe_int128_t",meta.getParameterTypeName(5));
                assertEquals(BigDecimal.class.getName(),meta.getParameterClassName(5));
                assertEquals(Types.VARCHAR,meta.getParameterType(6));
                assertEquals("monetdbe_str",meta.getParameterTypeName(6));
                assertEquals(String.class.getName(),meta.getParameterClassName(6));
                assertEquals(Types.BLOB,meta.getParameterType(7));
                assertEquals("monetdbe_blob",meta.getParameterTypeName(7));
                assertEquals(Blob.class.getName(),meta.getParameterClassName(7));
                assertEquals(Types.DATE,meta.getParameterType(8));
                assertEquals("monetdbe_date",meta.getParameterTypeName(8));
                assertEquals(Date.class.getName(),meta.getParameterClassName(8));

            }
            s.execute("DROP TABLE test19");

        } catch (SQLException e) {
            fail(e.toString());
        }
    }
}
