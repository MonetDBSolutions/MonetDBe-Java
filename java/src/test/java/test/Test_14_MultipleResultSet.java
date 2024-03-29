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

// TODO: Multiple ResultSets are not supported with the current monetdbe API, future feature
// TODO: Test is disabled in AllTests.java, enable when this feature is implemented
public class Test_14_MultipleResultSet {

	@Test
	public void multipleResultSet() {
		Stream.of(AllTests.CONNECTIONS).forEach(this::multipleResultSet);
	}

	private void multipleResultSet(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			try (Statement statement = conn.createStatement()) {
				// TODO: support multiple resultsets with statement.getMoreResults()
	            assertTrue(statement.execute("SELECT 1; SELECT 2; SELECT 3;"));
	            try (ResultSet rs = statement.getResultSet()) {
		            rs.next();
		            assertEquals((short)1, rs.getObject(1));
		            assertTrue(statement.getMoreResults());
		            rs.next();
		            assertEquals((short)2, rs.getObject(1));
		            assertTrue(statement.getMoreResults());
		            rs.next();
		            assertEquals((short)3, rs.getObject(1));
		            assertFalse(statement.getMoreResults());
	            }
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}