package test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.stream.Stream;

import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

public class Test_06_ComplexInsertAndQueryStatements {

	@Test
	public void basicInsertAndQueryStatements() {
		Stream.of(Configuration.CONNECTIONS).forEach(x -> basicInsertAndQueryStatements(x));
	}

	private void basicInsertAndQueryStatements(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			// Create table and insert values
			try (Statement statement = conn.createStatement()) {
				statement.executeUpdate(
						"CREATE TABLE test06 (bd NUMERIC, s STRING, b BLOB, d DATE, t TIME, ts TIMESTAMP);");
				statement.executeUpdate("INSERT INTO test06 VALUES "
						+ "(34589.54, 'hello', '12ff803F', current_date, current_time, current_timestamp), "
						+ "(34012933.888, 'world', '0000803F', str_to_date('23-09-1987', '%d-%m-%Y'), str_to_time('11:40:30', '%H:%M:%S'), str_to_timestamp('23-09-1987 11:40', '%d-%m-%Y %H:%M')), "
						+ "(666.666, 'bye', 'ffffffff', str_to_date('23-09-1990', '%d-%m-%Y'), str_to_time('11:40:35', '%H:%M:%S'), str_to_timestamp('23-09-1990 11:40', '%d-%m-%Y %H:%M')), "
						+ "(NULL, NULL, NULL, NULL, NULL, NULL);");

				// Query table
				try (ResultSet rs = statement.executeQuery("SELECT * FROM test06;")) {
					assertEquals(4, ((MonetResultSet) rs).getRowsNumber());
					assertEquals(6, ((MonetResultSet) rs).getColumnsNumber());

					rs.next();
					assertEquals(1, rs.getRow());
					assertEquals(new BigDecimal("34589.540").toString(), rs.getBigDecimal(1).toString());
					assertEquals("hello", rs.getString(2));
					assertArrayEquals(new byte[] { 0x12, (byte) 0xFF, (byte) 0x80, 0x3F }, rs.getBlob(3).getBytes(1, (int)rs.getBlob(3).length()));
					assertEquals(new Date(Calendar.getInstance().getTimeInMillis()).toString(),
							rs.getDate(4).toString());
					// assertEquals(new Time(Calendar.getInstance().getTimeInMillis()), rs.getTime(5));
					// assertEquals(new Timestamp(Calendar.getInstance().getTimeInMillis()), rs.getTimestamp(6));

					rs.next();
					assertEquals(2, rs.getRow());
					assertEquals(new BigDecimal("34012933.888").toString(), rs.getBigDecimal(1).toString());
					assertEquals("world", rs.getString(2));
					assertArrayEquals(new byte[] { 0x0, 0x0, (byte) 0x80, 0x3F }, rs.getBlob(3).getBytes(1, (int)rs.getBlob(3).length()));
					assertEquals(Date.valueOf("1987-09-23"), rs.getDate(4));
					assertEquals(Time.valueOf("11:40:30"), rs.getTime(5));
					assertEquals(Timestamp.valueOf("1987-09-23 11:40:00"), rs.getTimestamp(6));

					rs.next();
					assertEquals(3, rs.getRow());
					assertEquals(new BigDecimal("666.666").toString(), rs.getBigDecimal(1).toString());
					assertEquals("bye", rs.getString(2));
					assertArrayEquals(new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF }, rs.getBlob(3).getBytes(1, (int)rs.getBlob(3).length()));
					assertEquals(Date.valueOf("1990-09-23"), rs.getDate(4));
					assertEquals(Time.valueOf("11:40:35"), rs.getTime(5));
					assertEquals(Timestamp.valueOf("1990-09-23 11:40:00"), rs.getTimestamp(6));

					rs.next();
					assertEquals(4, rs.getRow());
					assumeTrue("BigDecimal should be null, not " + rs.getBigDecimal(1), rs.getBigDecimal(1) == null);
					assertNull(rs.getString(2));
					assertNull(rs.getBlob(3));
					assertNull(rs.getDate(4));
					assertNull(rs.getTime(5));
					assertNull(rs.getTimestamp(6));

					assertFalse(rs.next());
				}

				// Clean up
				statement.executeUpdate("DROP TABLE test06;");

				assertEquals(4, statement.getUpdateCount()); // 4: because we've dropped a table with 4 records
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}