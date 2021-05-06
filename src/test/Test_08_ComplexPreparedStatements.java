package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.stream.Stream;

import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

public class Test_08_ComplexPreparedStatements {

	@Test
	public void complexPreparedStatements() {
		Stream.of(AllTests.CONNECTIONS).forEach(x -> complexPreparedStatements(x));
	}

	private void complexPreparedStatements(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());
			
			// TODO: Support BLOB datatype
			/*
			// Create first table and insert values
			try (Statement statement = conn.createStatement()) {
				statement.executeUpdate(
						"CREATE TABLE test08a (bd NUMERIC, s STRING, b BLOB, d DATE, t TIME, ts TIMESTAMP);");

				try (PreparedStatement p = conn.prepareStatement("INSERT INTO test08a VALUES (?, ?, ?, ?, ?, ?);")) {
					p.setBigDecimal(1, BigDecimal.TEN);
					p.setString(2, "Hello world");
					// TODO: implement PreparedStatement.setBlob()
					// p.setBlob(3, new ByteArrayInputStream("Hello world".getBytes()));
					// TODO: and allow setting BLOB to null
					p.setNull(3, Types.BLOB);
					p.setDate(4, Date.valueOf("1975-10-25"));
					p.setTime(5, Time.valueOf("12:24:36"));
					p.setTimestamp(6, Timestamp.valueOf("1975-10-25 12:24:36"));
					assertEquals(1, p.executeUpdate());
				}

				try (PreparedStatement p = conn.prepareStatement("INSERT INTO test08a VALUES (?, ?, ?, ?, ?, ?);")) {
					p.setNull(1, Types.NUMERIC);
					p.setNull(2, Types.VARCHAR);
					p.setNull(3, Types.BLOB);
					p.setNull(4, Types.DATE);
					p.setNull(5, Types.TIME);
					p.setNull(6, Types.TIMESTAMP);
					assertEquals(1, p.executeUpdate());
				}

				// Query table
				try (ResultSet rs = statement.executeQuery("SELECT * FROM test08a;")) {
					assertEquals(1, ((MonetResultSet) rs).getRowsNumber());
					assertEquals(6, ((MonetResultSet) rs).getColumnsNumber());

					rs.next();
					assertEquals(1, rs.getRow());
					assertNull(rs.getBigDecimal(1));
					assertNull(rs.getString(2));
					assertNull(rs.getBlob(3));
					assertNull(rs.getDate(4));
					assertNull(rs.getTime(5));
					assertNull(rs.getTimestamp(6));

					assertFalse(rs.next());
				}

				// Clean up
				statement.executeUpdate("DROP TABLE test08a;");

				assertEquals(2, statement.getUpdateCount()); // 2: because we've dropped a table with 2 records
			}
			*/
			
			// Create second table and insert values
			try (Statement statement = conn.createStatement()) {
				statement.executeUpdate(
						"CREATE TABLE test08b (i INTEGER, l BIGINT, f REAL, df FLOAT, s STRING, d DATE, t TIME, ts TIMESTAMP);");

				try (PreparedStatement p = conn.prepareStatement("INSERT INTO test08b VALUES (?,?,?,?,?,'2020-10-31','15:16:59','2007-12-24 14:11:40');")) {
					p.setInt(1, 1000);
			        p.setLong(2, 1000000);
			        p.setFloat(3, 3.5f);
			        p.setDouble(4, 3.5d);
			        p.setString(5, "bye world");
					assertEquals(1, p.executeUpdate());
				}

				try (PreparedStatement p = conn.prepareStatement("INSERT INTO test08b VALUES (?,?,?,?,?,NULL,NULL,NULL);")) {
					p.setNull(1, Types.INTEGER);
			        p.setNull(2, Types.BIGINT);
			        p.setNull(3, Types.REAL);
			        p.setNull(4, Types.DOUBLE);
			        p.setNull(5, Types.VARCHAR);
					assertEquals(1, p.executeUpdate());
				}

				// Query table
				try (ResultSet rs = statement.executeQuery("SELECT * FROM test08b;")) {
					assertEquals(2, ((MonetResultSet) rs).getRowsNumber());
					assertEquals(8, ((MonetResultSet) rs).getColumnsNumber());

					rs.next();
					assertEquals(1, rs.getRow());
					assertEquals(1000, rs.getInt(1));
					assertEquals(1000000, rs.getInt(2));
					assertEquals(3.5f, rs.getFloat(3), .01f);
					assertEquals(3.5d, rs.getDouble(4), 0.1d);
					assertEquals("bye world", rs.getString(5));
					assertEquals(Date.valueOf("2020-10-31"), rs.getDate(6));
					assertEquals(Time.valueOf("15:16:59"), rs.getTime(7));
					assertEquals(Timestamp.valueOf("2007-12-24 14:11:40"), rs.getTimestamp(8));
					
					rs.next();
					assertEquals(2, rs.getRow());
					assertEquals(0, rs.getInt(1));
					assertEquals(0, rs.getInt(2));
					assertEquals(0.0f, rs.getFloat(3), .01f);
					assertEquals(0.0d, rs.getDouble(4), 0.1d);
					// TODO: Fix obtaining null String should return null instead of ''
					// assertNull(rs.getString(5));
					assertNull(rs.getDate(6));
					assertNull(rs.getTime(7));
					assertNull(rs.getTimestamp(8));

					assertFalse(rs.next());
				}

				// Clean up
				statement.executeUpdate("DROP TABLE test08b;");

				assertEquals(1, statement.getUpdateCount()); // 1: why?
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}