package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import org.junit.Test;
import org.monetdb.monetdbe.MonetConnection;

public class Test_13_Schema {

	@Test
	public void schema() {
		Stream.of(AllTests.CONNECTIONS).forEach(x -> schema(x));
	}

	private void schema(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

            assertEquals("sys", conn.getSchema());
            assertEquals("monetdb", ((MonetConnection) conn).getUserName());
            
			// Create table and insert values
			try (Statement statement = conn.createStatement()) {
				statement.execute("CREATE SCHEMA test13_voc;");
				
	            conn.setSchema("test13_voc");
	            assertEquals("test13_voc", conn.getSchema());
	            assertEquals("monetdb", ((MonetConnection) conn).getUserName());
	            
	            conn.setSchema("sys");
	            assertEquals("sys", conn.getSchema());
	            assertEquals("monetdb", ((MonetConnection) conn).getUserName());
			}
			
			try (Statement statement = conn.createStatement()) {
				// Clean up
				statement.execute("DROP SCHEMA test13_voc;");

				assertEquals(0, statement.getUpdateCount()); // 0: drop succeeded
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}