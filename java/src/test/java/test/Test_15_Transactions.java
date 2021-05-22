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

public class Test_15_Transactions {

	@Test
	public void transactions() {
		Stream.of(AllTests.CONNECTIONS).forEach(this::transactions);
	}

	private void transactions(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			try (Statement statement = conn.createStatement()) {
	            statement.executeUpdate("CREATE TABLE test15 (i int);");

	            // Begin transaction
	            conn.setAutoCommit(false);
	            assertFalse(conn.getAutoCommit());

	            statement.executeUpdate("INSERT INTO test15 VALUES (12345);");
	            conn.rollback();

	            try (ResultSet rs = statement.executeQuery("SELECT * FROM test15;")) {
	            	assertFalse(rs.next());
	            }

	            statement.executeUpdate("INSERT INTO test15 VALUES (23456);");
	            conn.commit();

	            try (ResultSet rs = statement.executeQuery("SELECT * FROM test15;")) {
	            	assertTrue(rs.next());
	            	assertEquals(23456, rs.getInt(1));
	            }
	            
	            // Finish transaction
	            conn.setAutoCommit(true);
	            assertTrue(conn.getAutoCommit());
	            
	            // Clean up
				int result = statement.executeUpdate("DROP TABLE test15;");
				assertEquals(1, result); // 1: because we've dropped a table with 1 record

				assertEquals(-1, statement.getUpdateCount());
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}