package test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

import static org.junit.Assert.*;

public class Test_07_SimplePreparedStatements {

	@Test
	public void simplePreparedStatements() {
		Stream.of(AllTests.CONNECTIONS).forEach(x -> simplePreparedStatements(x));
	}

	private void simplePreparedStatements(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			// Create table and insert values
			try (Statement statement = conn.createStatement()) {
				statement.executeUpdate("CREATE TABLE test07 (b BOOLEAN, i INTEGER, s STRING);");

				try (PreparedStatement p = conn.prepareStatement("INSERT INTO test07 VALUES (?, ?, ?);")) {
					p.setBoolean(1, true);
					p.setInt(2, 10);
					p.setString(3, "Hello world");
					assertEquals(1, p.executeUpdate());
				}

				try (PreparedStatement p = conn.prepareStatement("INSERT INTO test07 VALUES (?, ?, ?);")) {
					p.setNull(1, Types.BOOLEAN);
					p.setNull(2, Types.INTEGER);
					p.setNull(3, Types.VARCHAR);
					assertEquals(1, p.executeUpdate());
				}

				// Query table
				try (ResultSet rs = statement.executeQuery("SELECT * FROM test07;")) {
					assertEquals(2, ((MonetResultSet) rs).getRowsNumber());
					assertEquals(3, ((MonetResultSet) rs).getColumnsNumber());

					rs.next();
					assertEquals(1, rs.getRow());
					assertEquals(true, rs.getBoolean(1));
					assertEquals(10, rs.getInt(2));
					assertEquals("Hello world", rs.getString(3));

					rs.next();
					assertEquals(2, rs.getRow());
					assertEquals(false, rs.getBoolean(1));
					assertEquals(0, rs.getInt(2));
					assertNull(rs.getString(3));

					assertFalse(rs.next());
				}

				// Clean up
				statement.executeUpdate("DROP TABLE test07;");

				assertEquals(1, statement.getUpdateCount()); // TODO: explain this '1'
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}