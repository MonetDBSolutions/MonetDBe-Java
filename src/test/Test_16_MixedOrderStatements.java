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

public class Test_16_MixedOrderStatements {

	@Test
	public void mixedOrderStatements() {
		Stream.of(AllTests.CONNECTIONS).forEach(x -> mixedOrderStatements(x));
	}

	private void mixedOrderStatements(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			try (Statement statement1 = conn.createStatement();
					Statement statement2 = conn.createStatement();
					Statement statement3 = conn.createStatement()) {
	            statement1.executeUpdate("CREATE TABLE test16 (ID INTEGER GENERATED ALWAYS AS IDENTITY(START WITH 0) NOT NULL PRIMARY KEY,NAME VARCHAR(256) NOT NULL);");

	            statement3.executeUpdate("INSERT INTO test16 (NAME) VALUES ('A');");

	            try (ResultSet rs = statement2.executeQuery("SELECT ID, NAME FROM test16;")) {
	            	assertTrue(rs.next());
	            }

	            statement2.executeUpdate("INSERT INTO test16 (NAME) VALUES ('B');");

	            try (ResultSet rs = statement1.executeQuery("SELECT ID, NAME FROM test16;")) {
	            	assertTrue(rs.next());
	            	assertEquals(0, rs.getInt(1));
	            	assertEquals("A", rs.getString(2));
	            	assertTrue(rs.next());
	            	assertEquals(1, rs.getInt(1));
	            	assertEquals("B", rs.getString(2));
	            }
	            
	            // Clean up
				statement2.executeUpdate("DROP TABLE test16;");

				assertEquals(1, statement2.getUpdateCount()); // 1: because we've dropped a table with 1 record
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}