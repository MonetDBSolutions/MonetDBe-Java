package test;

import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

import java.math.BigDecimal;
import java.sql.*;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class Test_20_PreparedResultMetadata {

    @Test
    public void preparedResultMetadata() {
        Stream.of(AllTests.CONNECTIONS).forEach(this::preparedResultMetadata);
    }

    private void preparedResultMetadata(String connectionUrl) {
        try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {
            assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
            assertFalse(conn.isClosed());

            Statement s = conn.createStatement();
            s.execute("CREATE TABLE test20 (i int, l bigint, f real, d double, bd NUMERIC(36,18), s STRING, b BLOB, da DATE);");
            s.execute("INSERT INTO test20 VALUES (20,60000,20.4321,20934.43029,4398574389.5983798,'string','12ff803F',current_date)");

            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM test20 WHERE i = ?")) {
                //Jul2021 doesn't support PreparedStatement output ResultMetadata
                //TODO Find better strategy to do different tests for older versions
                if (ps.getMetaData() == null) {
                    s.execute("DROP TABLE test20");
                    return;
                }
                ResultSetMetaData meta = ps.getMetaData();
                assertEquals(8,meta.getColumnCount());

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
                assertEquals(Types.INTEGER,meta.getColumnType(1));
                assertEquals("monetdbe_int32_t",meta.getColumnTypeName(1));
                assertEquals(Integer.class.getName(),meta.getColumnClassName(1));
                assertEquals(Types.BIGINT,meta.getColumnType(2));
                assertEquals("monetdbe_int64_t",meta.getColumnTypeName(2));
                assertEquals(Long.class.getName(),meta.getColumnClassName(2));
                assertEquals(Types.REAL,meta.getColumnType(3));
                assertEquals("monetdbe_float",meta.getColumnTypeName(3));
                assertEquals(Float.class.getName(),meta.getColumnClassName(3));
                assertEquals(Types.DOUBLE,meta.getColumnType(4));
                assertEquals("monetdbe_double",meta.getColumnTypeName(4));
                assertEquals(Double.class.getName(),meta.getColumnClassName(4));
                assertEquals(Types.NUMERIC,meta.getColumnType(5));
                //TODO MonetDBe type should return monetdbe_int64 (NUMERIC is not necessarily a int128)
                assertEquals("monetdbe_int128_t",meta.getColumnTypeName(5));
                assertEquals(BigDecimal.class.getName(),meta.getColumnClassName(5));
                assertEquals(Types.VARCHAR,meta.getColumnType(6));
                assertEquals("monetdbe_str",meta.getColumnTypeName(6));
                assertEquals(String.class.getName(),meta.getColumnClassName(6));
                assertEquals(Types.BLOB,meta.getColumnType(7));
                assertEquals("monetdbe_blob",meta.getColumnTypeName(7));
                assertEquals(Blob.class.getName(),meta.getColumnClassName(7));
                assertEquals(Types.DATE,meta.getColumnType(8));
                assertEquals("monetdbe_date",meta.getColumnTypeName(8));
                assertEquals(Date.class.getName(),meta.getColumnClassName(8));

                //Names
                assertEquals("i",meta.getColumnName(1));
                assertEquals("l",meta.getColumnName(2));
                assertEquals("f",meta.getColumnName(3));
                assertEquals("d",meta.getColumnName(4));
                assertEquals("bd",meta.getColumnName(5));
                assertEquals("s",meta.getColumnName(6));
                assertEquals("b",meta.getColumnName(7));
                assertEquals("da",meta.getColumnName(8));
            }
            s.execute("DROP TABLE test20");

        } catch (SQLException e) {
            fail(e.toString());
        }
    }
    public static void main (String[] args) {
        Test_20_PreparedResultMetadata t = new Test_20_PreparedResultMetadata();
        Stream.of(AllTests.CONNECTIONS).forEach(t::preparedResultMetadata);
    }
}
