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

public class Test_17_QueryInThread {

	AssertionError exception_;

	private class QueryThread extends Thread {

		private Connection conn_;

		public QueryThread(Connection conn) {
			conn_ = conn;
		}

		@Override
		public void run() {
			try (Statement statement = conn_.createStatement()) {
				try (ResultSet rs = statement.executeQuery("SELECT ID, NAME FROM test17;")) {
					assertTrue(rs.next());
					assertEquals(1, rs.getRow());
					assertEquals(1, rs.getInt(1));
					assertEquals("A", rs.getString(2));
					assertTrue(rs.next());
					assertEquals(2, rs.getRow());
					assertEquals(2, rs.getInt(1));
					assertEquals("B", rs.getString(2));
				}
			} catch (SQLException e) {
				exception_ = new AssertionError("Thread exception: " + e.toString(), e);
				fail(e.toString());
			}
		}
	}

	@Test
	public void queryInThread() {
		Stream.of(AllTests.CONNECTIONS).forEach(x -> queryInThread(x));
	}

	private void queryInThread(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			try (Statement statement = conn.createStatement()) {

				// Set up some data
				statement.executeUpdate("CREATE TABLE test17 (ID INTEGER, NAME VARCHAR(256));");
				statement.executeUpdate("INSERT INTO test17 VALUES (1, 'A');");
				statement.executeUpdate("INSERT INTO test17 VALUES (2, 'B');");

				// Simple query
				try (ResultSet rs = statement.executeQuery("SELECT ID, NAME FROM test17;")) {
					assertTrue(rs.next());
					assertEquals(1, rs.getRow());
					assertEquals(1, rs.getInt(1));
					assertEquals("A", rs.getString(2));
					assertTrue(rs.next());
					assertEquals(2, rs.getRow());
					assertEquals(2, rs.getInt(1));
					assertEquals("B", rs.getString(2));
				}

				// Now perform the same query in a separate thread
				Thread t = new QueryThread(conn);
				t.start();
				t.join(3000);
				if (exception_ != null)
					throw exception_;

				// Clean up
				statement.executeUpdate("DROP TABLE test17;");

				assertEquals(1, statement.getUpdateCount()); // 1: because we've dropped a table with 1 record
			}

		} catch (SQLException | InterruptedException e) {

			fail(e.toString());

		}
	}
}