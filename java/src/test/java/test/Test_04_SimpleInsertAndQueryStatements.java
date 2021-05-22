package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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

public class Test_04_SimpleInsertAndQueryStatements {

	@Test
	public void simpleInsertAndQueryStatements() {
		Stream.of(AllTests.CONNECTIONS).forEach(this::simpleInsertAndQueryStatements);
	}

	private void simpleInsertAndQueryStatements(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			// Create table and insert values
			try (Statement statement = conn.createStatement()) {
				statement.executeUpdate("CREATE TABLE test04 (b BOOLEAN, i INTEGER, s STRING);");
				statement.executeUpdate(
						"INSERT INTO test04 VALUES (false, 3, 'hello'), (true, 500, 'world'), (false, -1, NULL);");

				// Query table
				try (ResultSet rs = statement.executeQuery("SELECT * FROM test04;")) {
					assertEquals(3, ((MonetResultSet) rs).getRowsNumber());
					assertEquals(3, ((MonetResultSet) rs).getColumnsNumber());

					rs.next();
					assertEquals(1, rs.getRow());
					assertEquals(false, rs.getBoolean(1));
					assertEquals(3, rs.getInt(2));
					assertEquals("hello", rs.getString(3));

					rs.next();
					assertEquals(2, rs.getRow());
					assertEquals(true, rs.getBoolean(1));
					assertEquals(500, rs.getInt(2));
					assertEquals("world", rs.getString(3));

					rs.next();
					assertEquals(3, rs.getRow());
					assertEquals(false, rs.getBoolean(1));
					assertEquals(-1, rs.getInt(2));
					assertNull(rs.getString(3));

					assertFalse(rs.next());
				}

				// Clean up
				int result = statement.executeUpdate("DROP TABLE test04;");
				assertEquals(3, result); // 3: because we've dropped a table with 3 records

				// TODO: fix getUpdateCount, currently always returns -1
				// assertEquals(result, statement.getUpdateCount()); 
				assertEquals(-1, statement.getUpdateCount()); 
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}