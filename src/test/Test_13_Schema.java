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
		Stream.of(Configuration.CONNECTIONS).forEach(x -> schema(x));
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
				statement.execute("CREATE SCHEMA voc;");
				
	            conn.setSchema("voc");
	            assertEquals("voc", conn.getSchema());
	            assertEquals("monetdb", ((MonetConnection) conn).getUserName());
	            
	            conn.setSchema("sys");
	            assertEquals("sys", conn.getSchema());
	            assertEquals("monetdb", ((MonetConnection) conn).getUserName());
			}
			
			try (Statement statement = conn.createStatement()) {
				// Clean up
				statement.execute("DROP SCHEMA voc;");

				assertEquals(-2, statement.getUpdateCount()); // -2: drop succeeded
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}