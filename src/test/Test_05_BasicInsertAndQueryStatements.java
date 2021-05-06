package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

public class Test_05_BasicInsertAndQueryStatements {

	@Test
	public void basicInsertAndQueryStatements() {
		Stream.of(AllTests.CONNECTIONS).forEach(x -> basicInsertAndQueryStatements(x));
	}

	private void basicInsertAndQueryStatements(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			// Create table and insert values
			try (Statement statement = conn.createStatement()) {
				statement.executeUpdate(
						"CREATE TABLE test05 (b BOOLEAN, s SMALLINT, i INT, l BIGINT, r REAL, f FLOAT);");
				statement.executeUpdate(
						"INSERT INTO test05 VALUES " + "(true, 2, 3, 5, 1.0, 1.66), (true, 4, 6, 10, 2.5, 3.643), "
								+ "(false, 8, 12, 20, 25.25, 372.325), (false, 16, 24, 40, 255.255, 2434.432), "
								+ "(false, null, 1, 1, 1, null);");

				// Query table
				try (ResultSet rs = statement.executeQuery("SELECT * FROM test05;")) {
					assertEquals(5, ((MonetResultSet) rs).getRowsNumber());
					assertEquals(6, ((MonetResultSet) rs).getColumnsNumber());

					rs.next();
					assertEquals(1, rs.getRow());
					assertEquals(true, rs.getBoolean(1));
					assertEquals(2, rs.getShort(2));
					assertEquals(3, rs.getInt(3));
					assertEquals(5, rs.getLong(4));
					assertEquals(1.0f, rs.getFloat(5), .01d);
					assertEquals(1.66d, rs.getDouble(6), .01d);

					rs.next();
					assertEquals(2, rs.getRow());
					assertEquals(true, rs.getBoolean(1));
					assertEquals(4, rs.getShort(2));
					assertEquals(6, rs.getInt(3));
					assertEquals(10, rs.getLong(4));
					assertEquals(2.5f, rs.getFloat(5), .01d);
					assertEquals(3.643d, rs.getDouble(6), .01d);

					rs.next();
					assertEquals(3, rs.getRow());
					assertEquals(false, rs.getBoolean(1));
					assertEquals(8, rs.getShort(2));
					assertEquals(12, rs.getInt(3));
					assertEquals(20, rs.getLong(4));
					assertEquals(25.25f, rs.getFloat(5), .01d);
					assertEquals(372.325d, rs.getDouble(6), .01d);

					rs.next();
					assertEquals(4, rs.getRow());
					assertEquals(false, rs.getBoolean(1));
					assertEquals(16, rs.getShort(2));
					assertEquals(24, rs.getInt(3));
					assertEquals(40, rs.getLong(4));
					assertEquals(255.255f, rs.getFloat(5), .01d);
					assertEquals(2434.432d, rs.getDouble(6), .01d);

					rs.next();
					assertEquals(5, rs.getRow());
					assertEquals(false, rs.getBoolean(1));
					assertEquals(0, rs.getShort(2));
					assertEquals(1, rs.getInt(3));
					assertEquals(1, rs.getLong(4));
					assertEquals(1.0f, rs.getFloat(5), .01d);
					assertEquals(0d, rs.getDouble(6), .01d);

					assertFalse(rs.next());
				}

				// Clean up
				statement.executeUpdate("DROP TABLE test05;");

				assertEquals(5, statement.getUpdateCount()); // 5: because we've dropped a table with 5 records
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}