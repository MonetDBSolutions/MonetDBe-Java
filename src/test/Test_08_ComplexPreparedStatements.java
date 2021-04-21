package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
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
	public void preparedStatements() {
		Stream.of(Configuration.CONNECTIONS).forEach(x -> preparedStatements(x));
	}

	private void preparedStatements(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			// Create table and insert values
			try (Statement statement = conn.createStatement()) {
				statement.executeUpdate(
						"CREATE TABLE test08 (bd NUMERIC, s STRING, b BLOB, d DATE, t TIME, ts TIMESTAMP);");

				try (PreparedStatement p = conn.prepareStatement("INSERT INTO test08 VALUES (?, ?, ?, ?, ?, ?);")) {
					p.setBigDecimal(1, BigDecimal.TEN);
					p.setString(2, "Hello world");
					// TODO: implement PreparedStatement.setBlob()
					// p.setBlob(3, new ByteArrayInputStream("Hello world".getBytes()));
					p.setNull(3, Types.BLOB);
					p.setDate(4, Date.valueOf("1975-10-25"));
					p.setTime(5, Time.valueOf("12:24:36"));
					p.setTimestamp(6, Timestamp.valueOf("1975-10-25 12:24:36"));
					assertEquals(1, p.executeUpdate());
				}

				try (PreparedStatement p = conn.prepareStatement("INSERT INTO test08 VALUES (?, ?, ?, ?, ?, ?);")) {
					p.setNull(1, Types.NUMERIC);
					p.setNull(2, Types.VARCHAR);
					p.setNull(3, Types.BLOB);
					p.setNull(4, Types.DATE);
					p.setNull(5, Types.TIME);
					p.setNull(6, Types.TIMESTAMP);
					assertEquals(1, p.executeUpdate());
				}

				// Query table
				try (ResultSet rs = statement.executeQuery("SELECT * FROM test08;")) {
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
				statement.executeUpdate("DROP TABLE test08;");

				assertEquals(2, statement.getUpdateCount()); // 2: because we've dropped a table with 2 records
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}